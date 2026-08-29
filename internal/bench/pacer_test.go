package bench

import (
	"sync/atomic"
	"testing"
	"time"
)

// A healthy system: service time well under the arrival interval. Every
// request should complete near its service time, and every scheduled
// request must actually run.
func TestPacerKeepsUpWhenServiceIsFast(t *testing.T) {
	var calls atomic.Int64
	h := Run(100, 500*time.Millisecond, 8, func() {
		calls.Add(1)
		time.Sleep(time.Millisecond)
	})
	if calls.Load() != 50 {
		t.Fatalf("ran %d of 50 scheduled requests", calls.Load())
	}
	if p50 := h.Quantile(0.5); p50 < time.Millisecond || p50 > 20*time.Millisecond {
		t.Errorf("p50 %v, want around the 1ms service time", p50)
	}
}

// The coordinated-omission case: one worker, service time far above the
// arrival interval. A closed-loop generator would report every request at
// the ~25ms service time; an honest open-loop one must show the backlog —
// the tail carries the queueing delay, many multiples of the service time.
func TestPacerExposesQueueingDelay(t *testing.T) {
	service := 25 * time.Millisecond
	h := Run(100, 400*time.Millisecond, 1, func() {
		time.Sleep(service)
	})
	if h.Count() != 40 {
		t.Fatalf("ran %d of 40 scheduled requests", h.Count())
	}
	// 40 arrivals at 10ms spacing served serially at 25ms: the last
	// arrival waits roughly 40*(25-10)ms = 600ms. Anything under ~3x the
	// service time would mean latencies were measured from send time —
	// the exact lie this harness exists to prevent.
	if p99 := h.Quantile(0.99); p99 < 3*service {
		t.Errorf("p99 %v does not show queueing delay (service %v)", p99, service)
	}
}
