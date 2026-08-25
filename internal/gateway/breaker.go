package gateway

import (
	"sync"
	"time"
)

// BreakerState is one of the three circuit breaker states. The state machine:
//
//	Closed --(N consecutive failures)--> Open
//	Open --(cooldown elapses)--> HalfOpen (one trial call allowed)
//	HalfOpen --(trial succeeds)--> Closed
//	HalfOpen --(trial fails)--> Open (cooldown restarts)
//
// The point of Open is fail-fast: while a provider is down, callers get an
// immediate rejection instead of stacking up requests that each burn a full
// timeout against a dead upstream (and turn one outage into a retry storm).
type BreakerState int

const (
	StateClosed BreakerState = iota
	StateOpen
	StateHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

type BreakerConfig struct {
	// FailureThreshold is how many consecutive failures trip the breaker.
	// Consecutive (not a failure rate) keeps the machine explainable: any
	// success proves the provider alive and resets the count.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before allowing a
	// half-open trial. It also bounds how long a half-open trial may stay
	// unresolved before another caller gets a turn.
	OpenTimeout time.Duration
}

func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{FailureThreshold: 3, OpenTimeout: 10 * time.Second}
}

// Breaker is a hand-rolled circuit breaker. Callers ask Allow() before each
// upstream call and report the outcome with RecordSuccess/RecordFailure.
// Outcomes that say nothing about provider health (e.g. the client hanging
// up) must simply not be recorded — the breaker self-heals from the trial
// slot leaking via the staleness rule in Allow.
type Breaker struct {
	cfg BreakerConfig
	// now is injectable so tests drive time instead of sleeping.
	now func() time.Time

	mu             sync.Mutex
	state          BreakerState
	failures       int
	openedAt       time.Time
	trialInFlight  bool
	trialStartedAt time.Time
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultBreakerConfig().FailureThreshold
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = DefaultBreakerConfig().OpenTimeout
	}
	return &Breaker{cfg: cfg, now: time.Now}
}

// Allow reports whether a call may proceed right now. In HalfOpen only one
// trial call is admitted at a time, so a recovering provider sees a single
// probe rather than the full backed-up load.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.cfg.OpenTimeout {
			return false
		}
		b.state = StateHalfOpen
		b.trialInFlight = true
		b.trialStartedAt = b.now()
		return true
	case StateHalfOpen:
		// If the current trial's outcome was never reported (caller crashed,
		// client cancelled), don't stay wedged: after OpenTimeout the slot is
		// considered stale and the next caller becomes the trial.
		if b.trialInFlight && b.now().Sub(b.trialStartedAt) < b.cfg.OpenTimeout {
			return false
		}
		b.trialInFlight = true
		b.trialStartedAt = b.now()
		return true
	}
	return false
}

func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		b.failures = 0
	case StateHalfOpen:
		b.state = StateClosed
		b.failures = 0
		b.trialInFlight = false
	case StateOpen:
		// A call admitted before the trip finished late and succeeded. The
		// open timer is the authority on when to retest; a straggler's good
		// news doesn't reopen the floodgates early.
	}
}

func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.trip()
		}
	case StateHalfOpen:
		b.trip()
	case StateOpen:
		// Already open; don't refresh openedAt, or stragglers from the
		// original outage could postpone the half-open retest indefinitely.
	}
}

// trip moves to Open and restarts the cooldown. Caller holds b.mu.
func (b *Breaker) trip() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.failures = 0
	b.trialInFlight = false
}

// State reports the current state without advancing it: an Open breaker
// whose cooldown has elapsed still reads Open until an Allow admits a trial.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
