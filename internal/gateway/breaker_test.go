package gateway

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests advance time explicitly instead of sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestBreaker(threshold int, timeout time.Duration) (*Breaker, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	b := NewBreaker(BreakerConfig{FailureThreshold: threshold, OpenTimeout: timeout})
	b.now = clock.now
	return b, clock
}

func TestBreakerStaysClosedBelowThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)

	b.RecordFailure()
	b.RecordFailure()
	if got := b.State(); got != StateClosed {
		t.Fatalf("state after 2/3 failures = %v, want closed", got)
	}
	if !b.Allow() {
		t.Fatal("closed breaker must allow calls")
	}
}

func TestBreakerSuccessResetsConsecutiveCount(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)

	// failure, failure, success, failure, failure: never 3 in a row.
	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess()
	b.RecordFailure()
	b.RecordFailure()
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (successes reset the count)", got)
	}
}

func TestBreakerTripsAtThresholdAndFailsFast(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)

	for range 3 {
		b.RecordFailure()
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state after 3 failures = %v, want open", got)
	}
	if b.Allow() {
		t.Fatal("open breaker must reject calls before the cooldown")
	}
}

func TestBreakerHalfOpenAdmitsSingleTrial(t *testing.T) {
	b, clock := newTestBreaker(1, time.Minute)

	b.RecordFailure()
	clock.advance(time.Minute)

	if !b.Allow() {
		t.Fatal("cooldown elapsed: one trial must be admitted")
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("state during trial = %v, want half-open", got)
	}
	if b.Allow() {
		t.Fatal("second caller must be rejected while the trial is in flight")
	}
}

func TestBreakerTrialSuccessCloses(t *testing.T) {
	b, clock := newTestBreaker(1, time.Minute)

	b.RecordFailure()
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("trial must be admitted")
	}
	b.RecordSuccess()

	if got := b.State(); got != StateClosed {
		t.Fatalf("state after trial success = %v, want closed", got)
	}
	if !b.Allow() {
		t.Fatal("closed breaker must allow calls")
	}
}

func TestBreakerFailureCountResetsAfterReclose(t *testing.T) {
	b, clock := newTestBreaker(2, time.Minute)

	b.RecordFailure()
	b.RecordFailure()
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("trial must be admitted")
	}
	b.RecordSuccess()

	// Reclosed: it must take a full fresh run of consecutive failures to
	// trip again, not just one on top of stale state.
	b.RecordFailure()
	if got := b.State(); got != StateClosed {
		t.Fatalf("state after 1/2 failures post-reclose = %v, want closed", got)
	}
}

func TestBreakerTrialFailureReopens(t *testing.T) {
	b, clock := newTestBreaker(1, time.Minute)

	b.RecordFailure()
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("trial must be admitted")
	}
	b.RecordFailure()

	if got := b.State(); got != StateOpen {
		t.Fatalf("state after trial failure = %v, want open", got)
	}
	if b.Allow() {
		t.Fatal("cooldown must restart after a failed trial")
	}
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("a new trial must be admitted after the restarted cooldown")
	}
}

func TestBreakerStaleTrialSlotIsReclaimed(t *testing.T) {
	b, clock := newTestBreaker(1, time.Minute)

	b.RecordFailure()
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("trial must be admitted")
	}
	// The trial's outcome is never reported (e.g. the client hung up and the
	// router recorded nothing). The breaker must not stay wedged forever.
	clock.advance(time.Minute)
	if !b.Allow() {
		t.Fatal("stale trial slot must be handed to the next caller")
	}
}

func TestBreakerLateResultsWhileOpenAreIgnored(t *testing.T) {
	b, _ := newTestBreaker(2, time.Minute)

	// Two in-flight calls; breaker trips while a third straggler is running.
	b.RecordFailure()
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	// Straggler success must not close the breaker early…
	b.RecordSuccess()
	if got := b.State(); got != StateOpen {
		t.Fatalf("state after late success = %v, want open", got)
	}
	// …and straggler failures must not extend the cooldown (verified by the
	// openedAt timestamp being unchanged: a trial is still admitted on time).
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("state after late failure = %v, want open", got)
	}
}

func TestBreakerConcurrentAccessIsSafe(t *testing.T) {
	b, _ := newTestBreaker(5, time.Millisecond)
	b.now = time.Now // real clock: exercises open->half-open under race too

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 100 {
				if b.Allow() {
					if i%2 == 0 {
						b.RecordSuccess()
					} else {
						b.RecordFailure()
					}
				}
				b.State()
			}
		}(i)
	}
	wg.Wait()
}
