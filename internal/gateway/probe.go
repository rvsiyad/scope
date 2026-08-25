package gateway

import (
	"context"
	"log"
	"time"
)

// HealthChecker is implemented by providers that expose a cheap liveness
// check (for Ollama, GET /api/version). Providers without one still work —
// their recovery is only detected by half-open trials on user traffic.
type HealthChecker interface {
	CheckHealth(ctx context.Context) error
}

const defaultProbeInterval = 5 * time.Second
const probeTimeout = 3 * time.Second

// StartProbes runs a background loop that health-checks unhealthy providers
// so recovery is detected without burning a user request. Probes only run
// while a breaker is not closed: for a healthy provider, real traffic is the
// ground truth, and a flaky health endpoint must not be able to trip a
// breaker that live requests keep proving wrong.
func (r *Router) StartProbes(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.probeOnce(ctx)
			}
		}
	}()
}

func (r *Router) probeOnce(ctx context.Context) {
	for _, rp := range r.chain {
		hc, ok := rp.provider.(HealthChecker)
		if !ok || rp.breaker.State() == StateClosed {
			continue
		}
		// Allow() makes the probe an ordinary half-open trial: it respects
		// the cooldown and competes for the same single trial slot as user
		// traffic, so the recovering provider still sees one caller at a time.
		if !rp.breaker.Allow() {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		err := hc.CheckHealth(probeCtx)
		cancel()
		if err != nil {
			rp.breaker.RecordFailure()
			log.Printf("probe: provider %s still unhealthy: %v", rp.provider.Name(), err)
			continue
		}
		rp.breaker.RecordSuccess()
		log.Printf("probe: provider %s recovered, breaker closed", rp.provider.Name())
	}
}

// ProviderStatus is one provider's entry in /healthz.
type ProviderStatus struct {
	Name    string `json:"name"`
	Breaker string `json:"breaker"`
}

func (r *Router) Status() []ProviderStatus {
	statuses := make([]ProviderStatus, 0, len(r.chain))
	for _, rp := range r.chain {
		statuses = append(statuses, ProviderStatus{
			Name:    rp.provider.Name(),
			Breaker: rp.breaker.State().String(),
		})
	}
	return statuses
}
