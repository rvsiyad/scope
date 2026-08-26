package gateway

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// newTestBudget reuses breaker_test.go's fakeClock so budget tests advance
// time explicitly instead of sleeping.
func newTestBudget(tokensPerMinute int) (*TenantBudget, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	b := NewTenantBudget(tokensPerMinute)
	b.now = clock.now
	b.lastRefill = clock.now()
	return b, clock
}

func TestReserveDebitsAndSettleRefunds(t *testing.T) {
	b, _ := newTestBudget(600)

	res, err := b.Reserve(200)
	if err != nil {
		t.Fatalf("Reserve(200) on a full bucket: %v", err)
	}
	if got := b.Available(); got != 400 {
		t.Fatalf("after reserve, available = %d, want 400", got)
	}

	res.Settle(50) // actual cost far below the estimate: refund 150
	if got := b.Available(); got != 550 {
		t.Fatalf("after settle, available = %d, want 550", got)
	}
}

func TestSettleOverrunGoesIntoDebt(t *testing.T) {
	b, clock := newTestBudget(600)

	res, err := b.Reserve(100)
	if err != nil {
		t.Fatal(err)
	}
	res.Settle(800) // stream blew through the estimate

	if got := b.Available(); got >= 0 {
		t.Fatalf("after overrun, available = %d, want negative (in debt)", got)
	}
	if _, err := b.Reserve(1); err == nil {
		t.Fatal("reserve while in debt must be rejected")
	}

	// Refill must first pay the debt, then rebuild balance: 600/min refills
	// the -200 debt to +400 after one minute.
	clock.advance(time.Minute)
	if got := b.Available(); got != 400 {
		t.Fatalf("one minute after overrun, available = %d, want 400", got)
	}
}

func TestRejectionCarriesRetryAfter(t *testing.T) {
	b, _ := newTestBudget(600) // 10 tokens/sec refill

	if _, err := b.Reserve(600); err != nil {
		t.Fatal(err)
	}
	_, err := b.Reserve(100)
	var exhausted *BudgetExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("err = %v, want *BudgetExhaustedError", err)
	}
	// 100 tokens at 10 tokens/sec: honest earliest retry is 10s.
	if exhausted.RetryAfter != 10*time.Second {
		t.Fatalf("RetryAfter = %s, want 10s", exhausted.RetryAfter)
	}
}

func TestOverCapacityIsNeverAdmittable(t *testing.T) {
	b, _ := newTestBudget(600)
	if _, err := b.Reserve(601); !errors.Is(err, ErrOverCapacity) {
		t.Fatalf("err = %v, want ErrOverCapacity", err)
	}
}

func TestRefillPacesAdmission(t *testing.T) {
	b, clock := newTestBudget(600)

	if _, err := b.Reserve(600); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(100); err == nil {
		t.Fatal("empty bucket must reject")
	}

	clock.advance(10 * time.Second) // 10 tokens/sec -> 100 back
	if _, err := b.Reserve(100); err != nil {
		t.Fatalf("after refill, Reserve(100): %v", err)
	}

	// Refill caps at capacity: an idle tenant does not accumulate hours of
	// burst.
	clock.advance(time.Hour)
	if got := b.Available(); got != 600 {
		t.Fatalf("after an idle hour, available = %d, want capacity 600", got)
	}
}

func TestSettleIsIdempotent(t *testing.T) {
	b, _ := newTestBudget(600)

	res, err := b.Reserve(100)
	if err != nil {
		t.Fatal(err)
	}
	res.Settle(40)
	res.Settle(0) // second settle (e.g. deferred cleanup) must not double-refund
	if got := b.Available(); got != 560 {
		t.Fatalf("after double settle, available = %d, want 560", got)
	}
}

func TestNilReservationSettleIsSafe(t *testing.T) {
	var res *Reservation
	res.Settle(100) // must not panic: rate limiting disabled -> nil reservation
}

func TestRefundNeverOverflowsCapacity(t *testing.T) {
	b, clock := newTestBudget(600)

	res, err := b.Reserve(300)
	if err != nil {
		t.Fatal(err)
	}
	// By settle time refill has already restored the bucket; the refund must
	// clamp at capacity, not mint extra budget.
	clock.advance(time.Hour)
	res.Settle(0)
	if got := b.Available(); got != 600 {
		t.Fatalf("available = %d, want capacity 600", got)
	}
}

func TestConcurrentReservesNeverOverrunBudget(t *testing.T) {
	b, _ := newTestBudget(1000)

	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.Reserve(100); err == nil {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if admitted != 10 {
		t.Fatalf("admitted = %d greedy reserves of 100 against 1000, want exactly 10", admitted)
	}
	if got := b.Available(); got < 0 {
		t.Fatalf("available = %d, races drove the bucket negative without an overrun", got)
	}
}
