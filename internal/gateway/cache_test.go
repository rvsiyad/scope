package gateway

import (
	"fmt"
	"testing"
	"time"
)

func newTestCache(maxEntries int, ttl time.Duration) (*ResponseCache, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	c := NewResponseCache(maxEntries, ttl)
	c.now = clock.now
	return c, clock
}

func f64(v float64) *float64 { return &v }

func cacheableReq(prompt string) ChatRequest {
	return ChatRequest{
		Model:       "llama3.2:1b",
		Temperature: f64(0),
		Messages:    []Message{{Role: "user", Content: prompt}},
	}
}

func respWithUsage(content string, total int) ChatResponse {
	return ChatResponse{
		Choices: []Choice{{Message: Message{Role: "assistant", Content: content}}},
		Usage:   &Usage{TotalTokens: total},
	}
}

func TestCacheHitOnEquivalentRequest(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	req := cacheableReq("what is a WAL?")

	if _, ok := c.Get("acme", req); ok {
		t.Fatal("empty cache must miss")
	}
	c.Put("acme", req, respWithUsage("a log", 42))

	got, ok := c.Get("acme", req)
	if !ok {
		t.Fatal("equivalent request must hit")
	}
	if got.Choices[0].Message.Content != "a log" {
		t.Fatalf("hit returned %q, want %q", got.Choices[0].Message.Content, "a log")
	}
}

func TestCacheMissAcrossKeyFields(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	base := cacheableReq("hello")
	c.Put("acme", base, respWithUsage("hi", 5))

	otherModel := base
	otherModel.Model = "llama3.2:3b"

	otherMax := base
	otherMax.MaxTokens = new(int)
	*otherMax.MaxTokens = 100

	otherPrompt := base
	otherPrompt.Messages = []Message{{Role: "user", Content: "hello!"}}

	cases := []struct {
		name   string
		tenant string
		req    ChatRequest
	}{
		{"different tenant", "globex", base},
		{"different model", "acme", otherModel},
		{"explicit max_tokens vs default", "acme", otherMax},
		{"different prompt", "acme", otherPrompt},
	}
	for _, tc := range cases {
		if _, ok := c.Get(tc.tenant, tc.req); ok {
			t.Errorf("%s must miss", tc.name)
		}
	}
}

// The key must not let adjacent fields collide by concatenation: two
// messages "ab"+"c" are a different conversation than "a"+"bc".
func TestCacheKeyFieldBoundaries(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	req1 := cacheableReq("")
	req1.Messages = []Message{{Role: "user", Content: "ab"}, {Role: "user", Content: "c"}}
	req2 := cacheableReq("")
	req2.Messages = []Message{{Role: "user", Content: "a"}, {Role: "user", Content: "bc"}}

	c.Put("acme", req1, respWithUsage("first", 1))
	if _, ok := c.Get("acme", req2); ok {
		t.Fatal("concatenation-colliding messages must not share an entry")
	}
}

func TestCacheStreamFlagSharesEntry(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	req := cacheableReq("hello")
	c.Put("acme", req, respWithUsage("hi", 5))

	streamed := req
	streamed.Stream = true
	if _, ok := c.Get("acme", streamed); !ok {
		t.Fatal("stream flag must not change the cache key")
	}
}

func TestCacheablePolicy(t *testing.T) {
	if Cacheable(ChatRequest{}) {
		t.Error("default (nil) temperature must not be cacheable")
	}
	if Cacheable(ChatRequest{Temperature: f64(0.7)}) {
		t.Error("temperature 0.7 must not be cacheable")
	}
	if !Cacheable(ChatRequest{Temperature: f64(0)}) {
		t.Error("temperature 0 must be cacheable")
	}

	// Put on a non-cacheable request must be a no-op even if called.
	c, _ := newTestCache(8, time.Minute)
	req := ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "x"}}}
	c.Put("acme", req, respWithUsage("y", 1))
	if st := c.Status(); st.Entries != 0 {
		t.Fatalf("non-cacheable Put stored an entry: %d", st.Entries)
	}
}

func TestCacheTTLExpiry(t *testing.T) {
	c, clock := newTestCache(8, time.Minute)
	req := cacheableReq("hello")
	c.Put("acme", req, respWithUsage("hi", 5))

	clock.advance(59 * time.Second)
	if _, ok := c.Get("acme", req); !ok {
		t.Fatal("entry inside TTL must hit")
	}

	clock.advance(2 * time.Second)
	if _, ok := c.Get("acme", req); ok {
		t.Fatal("entry past TTL must miss")
	}
	if st := c.Status(); st.Entries != 0 {
		t.Fatalf("expired entry not evicted: %d entries", st.Entries)
	}
}

func TestCacheLRUEviction(t *testing.T) {
	c, _ := newTestCache(2, time.Minute)
	reqA, reqB, reqC := cacheableReq("a"), cacheableReq("b"), cacheableReq("c")

	c.Put("acme", reqA, respWithUsage("A", 1))
	c.Put("acme", reqB, respWithUsage("B", 1))
	// Touch A so B becomes the least recently used.
	c.Get("acme", reqA)
	c.Put("acme", reqC, respWithUsage("C", 1))

	if _, ok := c.Get("acme", reqB); ok {
		t.Fatal("least-recently-used entry must have been evicted")
	}
	if _, ok := c.Get("acme", reqA); !ok {
		t.Fatal("recently-used entry must survive eviction")
	}
	if _, ok := c.Get("acme", reqC); !ok {
		t.Fatal("newest entry must survive eviction")
	}
}

func TestCacheCounters(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	req := cacheableReq("hello")

	c.Get("acme", req) // miss
	c.Put("acme", req, respWithUsage("hi", 40))
	c.Get("acme", req) // hit, saves 40
	c.Get("acme", req) // hit, saves 40

	st := c.Status()
	if st.Hits != 2 || st.Misses != 1 {
		t.Fatalf("hits/misses = %d/%d, want 2/1", st.Hits, st.Misses)
	}
	if st.TokensSaved != 80 {
		t.Fatalf("tokens saved = %d, want 80", st.TokensSaved)
	}
}

func TestCacheSavedTokensMetersWithoutUsage(t *testing.T) {
	c, _ := newTestCache(8, time.Minute)
	req := cacheableReq("hello")
	resp := ChatResponse{Choices: []Choice{{Message: Message{Role: "assistant", Content: "12345678"}}}}
	c.Put("acme", req, resp)

	c.Get("acme", req)
	if st := c.Status(); st.TokensSaved != 2 { // 8 chars / 4 chars-per-token
		t.Fatalf("tokens saved = %d, want metered 2", st.TokensSaved)
	}
}

func TestCacheNilReceiverIsSafe(t *testing.T) {
	var c *ResponseCache
	req := cacheableReq("hello")
	if _, ok := c.Get("acme", req); ok {
		t.Fatal("nil cache must miss")
	}
	c.Put("acme", req, respWithUsage("hi", 1))
	if st := c.Status(); st != nil {
		t.Fatal("nil cache status must be nil")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c, _ := newTestCache(16, time.Minute)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				req := cacheableReq(fmt.Sprintf("prompt-%d", j%20))
				c.Get("acme", req)
				c.Put("acme", req, respWithUsage("r", 1))
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
