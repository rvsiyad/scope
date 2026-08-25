package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

// checkableProvider is a fakeProvider with a scripted health check.
type checkableProvider struct {
	fakeProvider
	healthErr    error
	healthChecks int
}

func (p *checkableProvider) CheckHealth(ctx context.Context) error {
	p.healthChecks++
	return p.healthErr
}

func trip(t *testing.T, r *Router, p *fakeProvider) {
	t.Helper()
	r.Chat(context.Background(), ChatRequest{})
	if got := r.chain[0].breaker.State(); got != StateOpen {
		t.Fatalf("setup: breaker = %v, want open", got)
	}
}

func TestProbeClosesBreakerOnRecovery(t *testing.T) {
	p := &checkableProvider{fakeProvider: fakeProvider{name: "p", err: errors.New("boom")}}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)
	clock := routerClock(r)
	trip(t, r, &p.fakeProvider)

	// Provider recovers with zero user traffic: the probe alone must detect
	// it and close the breaker.
	p.healthErr = nil
	clock.advance(time.Minute)
	r.probeOnce(context.Background())

	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("breaker after successful probe = %v, want closed", got)
	}
	if p.healthChecks != 1 {
		t.Fatalf("health checks = %d, want 1", p.healthChecks)
	}
}

func TestProbeFailureRestartsCooldown(t *testing.T) {
	p := &checkableProvider{fakeProvider: fakeProvider{name: "p", err: errors.New("boom")}, healthErr: errors.New("still down")}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)
	clock := routerClock(r)
	trip(t, r, &p.fakeProvider)

	clock.advance(time.Minute)
	r.probeOnce(context.Background())
	if got := r.chain[0].breaker.State(); got != StateOpen {
		t.Fatalf("breaker after failed probe = %v, want open", got)
	}

	// The failed probe restarted the cooldown: the next sweep, before the
	// cooldown elapses again, must not spend another health check.
	r.probeOnce(context.Background())
	if p.healthChecks != 1 {
		t.Fatalf("health checks = %d, want 1 (cooldown not elapsed)", p.healthChecks)
	}

	clock.advance(time.Minute)
	r.probeOnce(context.Background())
	if p.healthChecks != 2 {
		t.Fatalf("health checks = %d, want 2 after cooldown", p.healthChecks)
	}
}

func TestProbeLeavesHealthyProvidersAlone(t *testing.T) {
	p := &checkableProvider{fakeProvider: fakeProvider{name: "p"}, healthErr: errors.New("flaky health endpoint")}
	r := NewRouter(DefaultBreakerConfig(), p)

	// Closed breaker: real traffic is the ground truth, so even a failing
	// health endpoint must be ignored — no probe runs at all.
	r.probeOnce(context.Background())
	if p.healthChecks != 0 {
		t.Fatalf("health checks = %d, want 0 for a closed breaker", p.healthChecks)
	}
	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("breaker = %v, want closed", got)
	}
}

func TestProbeSkipsProvidersWithoutHealthCheck(t *testing.T) {
	p := &fakeProvider{name: "p", err: errors.New("boom")}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)
	clock := routerClock(r)
	trip(t, r, p)

	clock.advance(time.Minute)
	r.probeOnce(context.Background()) // must not panic or consume the trial slot

	// The half-open trial slot is still available to user traffic.
	if !r.chain[0].breaker.Allow() {
		t.Fatal("trial slot must remain available when no health check exists")
	}
}

func TestOllamaCheckHealth(t *testing.T) {
	ollama := httptest.NewServer(nil) // 404s: unhealthy
	defer ollama.Close()

	p := NewOllamaProvider(ollama.URL)
	if err := p.CheckHealth(context.Background()); err == nil {
		t.Fatal("want error for non-200 health response")
	}
	if err := NewOllamaProvider("http://127.0.0.1:1").CheckHealth(context.Background()); err == nil {
		t.Fatal("want error for unreachable ollama")
	}
}

func TestHealthzReportsBreakerStates(t *testing.T) {
	p := &fakeProvider{name: "primary", err: errors.New("boom")}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)
	trip(t, r, p)

	srv := NewWithProvider(Config{OllamaURLs: []string{"http://127.0.0.1:1"}, PostgresAddr: "127.0.0.1:1"}, r)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	var st healthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Provider) != 1 || st.Provider[0].Name != "primary" || st.Provider[0].Breaker != "open" {
		t.Fatalf("providers = %+v, want primary/open", st.Provider)
	}
}
