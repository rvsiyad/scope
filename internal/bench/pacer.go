package bench

import (
	"sync"
	"time"
)

// Run drives fn at a fixed open-loop rate for the given duration and
// returns the latency histogram. Open-loop is the honest shape for a
// service benchmark: arrivals are scheduled on a clock the system under
// test cannot slow down, the way real clients arrive. Two rules make it
// coordinated-omission-proof:
//
//   - Every latency is measured from the request's SCHEDULED start, not
//     from when a worker got around to sending it. If the system stalls
//     for a second, the requests queued behind that stall each carry the
//     stall in their number instead of politely restarting their clocks.
//   - No scheduled request is ever skipped. A slow system faces a growing
//     backlog — exactly what its real clients would become — rather than
//     a load generator that courteously backs off.
//
// workers bounds in-flight concurrency; size it well above the expected
// rate x service time so the pacer itself never becomes the bottleneck
// (the backlog channel absorbs the difference when it briefly is).
func Run(rate float64, d time.Duration, workers int, fn func()) *Hist {
	h := &Hist{}
	n := int(rate * d.Seconds())
	interval := time.Duration(float64(time.Second) / rate)
	schedule := make(chan time.Time, n)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for intended := range schedule {
				if wait := time.Until(intended); wait > 0 {
					time.Sleep(wait)
				}
				fn()
				h.Record(time.Since(intended))
			}
		}()
	}

	// The full schedule is computed up front from one start instant —
	// arrival i at start + i*interval — so a lagging run can never
	// stretch the plan. The buffered channel is the backlog.
	start := time.Now().Add(10 * time.Millisecond)
	for i := 0; i < n; i++ {
		schedule <- start.Add(time.Duration(i) * interval)
	}
	close(schedule)
	wg.Wait()
	return h
}
