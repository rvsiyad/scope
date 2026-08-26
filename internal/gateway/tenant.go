package gateway

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// TenantConfig declares one tenant: who they are, the API key that
// identifies them, and their token budget.
type TenantConfig struct {
	Name            string
	APIKey          string
	TokensPerMinute int
}

// ParseTenants parses the SCOPE_TENANTS format — comma-separated
// name:api-key:tokens-per-minute entries:
//
//	acme:sk-acme-123:6000,globex:sk-globex-456:1200
//
// An empty spec means no tenants: the gateway runs open (auth and rate
// limiting off), which keeps the zero-config demo mode working.
func ParseTenants(spec string) ([]TenantConfig, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	var out []TenantConfig
	seen := map[string]bool{}
	for _, entry := range strings.Split(spec, ",") {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("tenant entry %q: want name:api-key:tokens-per-minute", entry)
		}
		name, key := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		tpm, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if name == "" || key == "" {
			return nil, fmt.Errorf("tenant entry %q: empty name or api key", entry)
		}
		if err != nil || tpm <= 0 {
			return nil, fmt.Errorf("tenant entry %q: tokens-per-minute must be a positive integer", entry)
		}
		if seen[key] {
			return nil, fmt.Errorf("tenant entry %q: duplicate api key", entry)
		}
		seen[key] = true
		out = append(out, TenantConfig{Name: name, APIKey: key, TokensPerMinute: tpm})
	}
	return out, nil
}

// tenant is the runtime state behind one API key. The counters are the
// admission story per tenant: how many requests got in, how many were turned
// away, and what the admitted ones actually cost once settled.
type tenant struct {
	name   string
	budget *TenantBudget

	admitted      atomic.Uint64
	rejected      atomic.Uint64
	tokensCharged atomic.Uint64
}

func buildTenants(configs []TenantConfig) map[string]*tenant {
	if len(configs) == 0 {
		return nil
	}
	out := make(map[string]*tenant, len(configs))
	for _, c := range configs {
		out[c.APIKey] = &tenant{name: c.Name, budget: NewTenantBudget(c.TokensPerMinute)}
	}
	return out
}

// TenantStatus is one tenant's admission picture as reported by /healthz:
// the live budget balance plus the counters. Until the self-built metrics
// backend lands (phase B), this is the gateway's admission dashboard.
type TenantStatus struct {
	Name string `json:"name"`
	// Available is the current balance; negative means the tenant is in
	// debt from a stream that overran its estimate.
	Available     int    `json:"available_tokens"`
	Capacity      int    `json:"capacity_tokens"`
	Admitted      uint64 `json:"admitted"`
	Rejected      uint64 `json:"rejected"`
	TokensCharged uint64 `json:"tokens_charged"`
}

func (s *Server) tenantStatus() []TenantStatus {
	if len(s.tenants) == 0 {
		return nil
	}
	out := make([]TenantStatus, 0, len(s.tenants))
	for _, t := range s.tenants {
		out = append(out, TenantStatus{
			Name:          t.name,
			Available:     t.budget.Available(),
			Capacity:      t.budget.Capacity(),
			Admitted:      t.admitted.Load(),
			Rejected:      t.rejected.Load(),
			TokensCharged: t.tokensCharged.Load(),
		})
	}
	// The map's iteration order would make healthz flap between calls.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// authTenant resolves the request's tenant from its Bearer key, writing the
// 401 itself on failure. With no tenants configured the gateway runs open
// and every request is anonymous (nil tenant, nothing rate limited);
// configuring any tenant turns auth on for everyone.
func (s *Server) authTenant(w http.ResponseWriter, r *http.Request) (*tenant, bool) {
	if len(s.tenants) == 0 {
		return nil, true
	}
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || strings.TrimSpace(key) == "" {
		writeAPIErrorCode(w, http.StatusUnauthorized, "invalid_request_error", "invalid_api_key",
			"missing API key: pass it as 'Authorization: Bearer <key>'")
		return nil, false
	}
	t, found := s.tenants[strings.TrimSpace(key)]
	if !found {
		writeAPIErrorCode(w, http.StatusUnauthorized, "invalid_request_error", "invalid_api_key",
			"unknown API key")
		return nil, false
	}
	return t, true
}
