package gateway

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// counterSet keeps the gateway's cumulative counters: requests, tokens,
// and dollars, per label combination. The _total metrics used to emit each
// request's own value (76 tokens, 1 request), which reads naturally in a
// log but is useless to rate() — a temperature-0 benchmark sending the
// same request forever produces a constant series with zero growth. A
// counter accumulates instead, which is the contract the _total suffix
// promises and the shape rate()'s counter-reset math expects. The totals
// live in process memory, so a gateway restart legitimately resets them
// to zero — exactly the reset the query engine's rate()/increase() heal.
//
// Label combinations are bounded by the emission discipline (tenant,
// model, outcome, cache — never ids), so the map is small by the same
// cardinality rule the tsdb enforces downstream.
type counterSet struct {
	mu     sync.Mutex
	totals map[string]float64
}

func newCounterSet() *counterSet {
	return &counterSet{totals: map[string]float64{}}
}

// add bumps one series' total and returns the new cumulative value plus
// the timestamp to stamp on the emitted sample. Total and timestamp are
// taken under one lock so concurrent requests observe consistent pairs: a
// later (greater) total never carries an earlier timestamp. Same-
// millisecond bursts can still emit equal timestamps, which the tsdb
// drops as duplicates — losing an intermediate cumulative point costs
// rate() resolution, never correctness, because the surviving later
// sample already contains the dropped one's growth.
func (c *counterSet) add(name string, labels map[string]string, delta float64) (total float64, ts int64) {
	key := counterKey(name, labels)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totals[key] += delta
	return c.totals[key], time.Now().UnixMilli()
}

// counterKey is the series identity: name plus sorted label pairs, the
// same identity rule the tsdb applies.
func counterKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)
	var sb strings.Builder
	sb.WriteString(name)
	for _, k := range names {
		sb.WriteByte(0)
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
	}
	return sb.String()
}
