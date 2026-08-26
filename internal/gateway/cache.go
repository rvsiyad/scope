package gateway

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Exact-match response caching. Every hit is a provider call not made —
// tokens (and with paid providers, dollars) not spent, and seconds of
// generation latency not waited for — so the cache keeps savings counters,
// not just a hit rate.
//
// Two policy decisions worth stating up front:
//
//   - Only deterministic requests (temperature explicitly 0) are cached. At
//     temperature > 0 the provider is *supposed* to answer differently each
//     time; serving a memorized reply would silently change that behavior.
//     Callers opt in to caching by opting out of randomness.
//   - Tenants never share entries. Completions can echo prompt content, and
//     tenant A's prompts must not leak to tenant B through a cache key
//     collision, so the tenant is part of the key.

// ResponseCache is a bounded, TTL'd, exact-match cache of completed chat
// responses. LRU eviction keeps it within maxEntries; TTL bounds staleness
// (models get swapped and re-quantized under the same name, so entries must
// age out even if popular). Nil-receiver safe, like Reservation: a Server
// built without a cache calls it unconditionally and everything misses.
type ResponseCache struct {
	maxEntries int
	ttl        time.Duration
	// now is injectable so tests drive time instead of sleeping.
	now func() time.Time

	mu    sync.Mutex
	order *list.List               // front = most recently used
	items map[string]*list.Element // key -> element whose Value is *cacheEntry

	hits        atomic.Uint64
	misses      atomic.Uint64
	tokensSaved atomic.Uint64
}

type cacheEntry struct {
	key       string
	resp      ChatResponse
	expiresAt time.Time
}

func NewResponseCache(maxEntries int, ttl time.Duration) *ResponseCache {
	return &ResponseCache{
		maxEntries: maxEntries,
		ttl:        ttl,
		now:        time.Now,
		order:      list.New(),
		items:      make(map[string]*list.Element),
	}
}

// Cacheable reports whether a request is eligible for the cache at all:
// only sampling pinned to temperature 0 qualifies (see the policy note
// above). The stream flag is deliberately not consulted — a completion is
// the same completion whether it was delivered as JSON or SSE, so a
// non-streamed response can later serve a streamed request and vice versa.
func Cacheable(req ChatRequest) bool {
	return req.Temperature != nil && *req.Temperature == 0
}

// Get returns the cached response for an equivalent prior request from the
// same tenant. Expired entries are evicted lazily here rather than by a
// background sweeper — an expired entry costs nothing until someone asks
// for it. Callers must check Cacheable first; Get counts every call as a
// hit or a miss.
func (c *ResponseCache) Get(tenantName string, req ChatRequest) (ChatResponse, bool) {
	if c == nil {
		return ChatResponse{}, false
	}
	key := cacheKey(tenantName, req)
	c.mu.Lock()
	el, ok := c.items[key]
	if ok && c.now().After(el.Value.(*cacheEntry).expiresAt) {
		c.removeLocked(el)
		ok = false
	}
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return ChatResponse{}, false
	}
	c.order.MoveToFront(el)
	resp := el.Value.(*cacheEntry).resp
	c.mu.Unlock()

	c.hits.Add(1)
	c.tokensSaved.Add(uint64(savedTokens(resp)))
	return resp, true
}

// Put stores a completed response. When full, the least-recently-used entry
// is evicted — a popular entry surviving over a stale one is the whole
// point of bounding by recency instead of insertion order.
func (c *ResponseCache) Put(tenantName string, req ChatRequest, resp ChatResponse) {
	if c == nil || !Cacheable(req) {
		return
	}
	key := cacheKey(tenantName, req)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value.(*cacheEntry).resp = resp
		el.Value.(*cacheEntry).expiresAt = c.now().Add(c.ttl)
		c.order.MoveToFront(el)
		return
	}
	for c.order.Len() >= c.maxEntries {
		c.removeLocked(c.order.Back())
	}
	entry := &cacheEntry{key: key, resp: resp, expiresAt: c.now().Add(c.ttl)}
	c.items[key] = c.order.PushFront(entry)
}

// removeLocked drops one entry. Caller holds c.mu.
func (c *ResponseCache) removeLocked(el *list.Element) {
	c.order.Remove(el)
	delete(c.items, el.Value.(*cacheEntry).key)
}

// savedTokens is what a hit avoided re-buying: the provider's own count when
// the stored response carries one, otherwise metered content length.
func savedTokens(resp ChatResponse) int {
	if resp.Usage != nil {
		return resp.Usage.TotalTokens
	}
	n := 0
	for _, ch := range resp.Choices {
		n += meterText(len(ch.Message.Content))
	}
	return n
}

// cacheKey hashes the parts of a request that determine the provider's
// answer: tenant (isolation), model, sampling parameters, and the messages.
// Hashing the decoded struct rather than the request body is what
// "normalized" means here — two byte-different JSON bodies (key order,
// whitespace, duplicate-field games) that decode to the same request hash
// the same. Every field is length- or tag-delimited so adjacent values
// can't collide by concatenation ("ab"+"c" vs "a"+"bc").
func cacheKey(tenantName string, req ChatRequest) string {
	h := sha256.New()
	writeField(h, tenantName)
	writeField(h, req.Model)
	// nil means "provider default", which is not the same request as any
	// explicit value — the sentinel keeps them distinct.
	if req.Temperature != nil {
		writeField(h, strconv.FormatFloat(*req.Temperature, 'g', -1, 64))
	} else {
		writeField(h, "\x00default")
	}
	if req.MaxTokens != nil {
		writeField(h, strconv.Itoa(*req.MaxTokens))
	} else {
		writeField(h, "\x00default")
	}
	for _, m := range req.Messages {
		writeField(h, m.Role)
		writeField(h, m.Content)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeField(w io.Writer, s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	w.Write(n[:])
	io.WriteString(w, s)
}

// CacheStatus is the cache's scoreboard as reported by /healthz. TokensSaved
// is the dollar story in token units: every one is a token the provider was
// not asked to generate again.
type CacheStatus struct {
	Entries     int    `json:"entries"`
	Hits        uint64 `json:"hits"`
	Misses      uint64 `json:"misses"`
	TokensSaved uint64 `json:"tokens_saved"`
}

func (c *ResponseCache) Status() *CacheStatus {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	entries := c.order.Len()
	c.mu.Unlock()
	return &CacheStatus{
		Entries:     entries,
		Hits:        c.hits.Load(),
		Misses:      c.misses.Load(),
		TokensSaved: c.tokensSaved.Load(),
	}
}
