package gateway

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Token-budget rate limiting. Budgets are denominated in LLM tokens, not
// requests, because one request can cost 5 tokens or 5000 — request-count
// limits don't protect anything that bills by the token. The catch: a
// request's true cost is unknown until the stream finishes, which breaks the
// classic token bucket (it wants the cost up front). So admission happens in
// three steps:
//
//	estimate  guess the cost before calling the provider
//	reserve   debit the guess from the tenant's bucket, or reject with 429
//	settle    when the true cost is known, refund the overestimate — or
//	          charge the overrun
//
// An overrun drives the bucket into debt (a negative balance) on purpose:
// tokens that already streamed can't be un-streamed, so the tenant's future
// admissions wait for refill to cover what was actually spent.

// TenantBudget is one tenant's token bucket, extended with reservations.
// Burst capacity equals one minute of refill: a tenant may spend a full
// minute of budget at once, then is paced by the refill rate.
type TenantBudget struct {
	capacity     float64
	refillPerSec float64
	// now is injectable so tests drive time instead of sleeping.
	now func() time.Time

	mu         sync.Mutex
	available  float64
	lastRefill time.Time
}

func NewTenantBudget(tokensPerMinute int) *TenantBudget {
	b := &TenantBudget{
		capacity:     float64(tokensPerMinute),
		refillPerSec: float64(tokensPerMinute) / 60,
		now:          time.Now,
	}
	b.available = b.capacity
	b.lastRefill = b.now()
	return b
}

// ErrOverCapacity rejects a request whose estimate exceeds the bucket's
// capacity: no amount of waiting would ever admit it, so there is no honest
// Retry-After to send. The client must shrink the request (usually
// max_tokens) or the tenant needs a bigger budget.
var ErrOverCapacity = errors.New("estimated cost exceeds budget capacity")

// BudgetExhaustedError rejects a request that refill will eventually make
// admittable; RetryAfter says when.
type BudgetExhaustedError struct {
	RetryAfter time.Duration
}

func (e *BudgetExhaustedError) Error() string {
	return fmt.Sprintf("token budget exhausted, retry after %s", e.RetryAfter)
}

// Reserve debits the estimated cost and returns a reservation to settle once
// the true cost is known. Rejections are *BudgetExhaustedError (come back
// after refill) or ErrOverCapacity (never admittable).
func (b *TenantBudget) Reserve(estimate int) (*Reservation, error) {
	if float64(estimate) > b.capacity {
		return nil, ErrOverCapacity
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if float64(estimate) > b.available {
		return nil, &BudgetExhaustedError{RetryAfter: b.timeUntil(float64(estimate))}
	}
	b.available -= float64(estimate)
	return &Reservation{budget: b, estimate: estimate}, nil
}

// Available reports the current balance in whole tokens; negative while the
// tenant is in debt from an overrun.
func (b *TenantBudget) Available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return int(math.Floor(b.available))
}

// Capacity reports the bucket's burst capacity in whole tokens.
func (b *TenantBudget) Capacity() int { return int(b.capacity) }

// refill advances the bucket to now. Caller holds b.mu.
func (b *TenantBudget) refill() {
	now := b.now()
	if elapsed := now.Sub(b.lastRefill).Seconds(); elapsed > 0 {
		b.available = math.Min(b.capacity, b.available+elapsed*b.refillPerSec)
	}
	b.lastRefill = now
}

// timeUntil reports how long refill needs to cover `want` tokens, rounded up
// to whole seconds because Retry-After is an integer header. Caller holds
// b.mu with refill already applied.
func (b *TenantBudget) timeUntil(want float64) time.Duration {
	secs := math.Ceil((want - b.available) / b.refillPerSec)
	if secs < 1 {
		secs = 1
	}
	return time.Duration(secs) * time.Second
}

// Reservation is an in-flight claim on a tenant's budget.
type Reservation struct {
	budget   *TenantBudget
	estimate int
	once     sync.Once
	// onSettle, when set, observes the actual cost the reservation settled
	// at (fires exactly once) — the hook admission metrics hang off without
	// the budget knowing about them.
	onSettle func(actual int)
}

// Settle reports the request's actual token cost and adjusts the bucket by
// the difference: refund if the estimate was high, extra charge — possibly
// into debt — if it was low. Idempotent, because the handler settles on
// every exit path (clean EOF, provider error, client disconnect) and only
// the first outcome counts. Safe on a nil receiver so callers running
// without a budget can settle unconditionally.
func (r *Reservation) Settle(actual int) {
	if r == nil {
		return
	}
	r.once.Do(func() {
		b := r.budget
		b.mu.Lock()
		b.refill()
		b.available = math.Min(b.capacity, b.available+float64(r.estimate-actual))
		b.mu.Unlock()
		if r.onSettle != nil {
			r.onSettle(actual)
		}
	})
}
