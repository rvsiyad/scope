// Package gateway implements the OpenAI-compatible LLM gateway: the HTTP
// surface clients point their SDKs at, plus everything that sits between a
// client request and a provider call.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Config struct {
	// Addr is the listen address, e.g. ":8080".
	Addr string
	// OllamaURLs is the failover chain, tried in order: first entry is the
	// primary, e.g. ["http://localhost:11434", "http://localhost:11435"].
	OllamaURLs []string
	// PostgresAddr is the host:port of Postgres, health-checked over TCP only
	// until the gateway actually stores tenants there.
	PostgresAddr string
	// Tenants are the API keys and token budgets the gateway enforces.
	// Empty means open mode: no auth, no rate limiting.
	Tenants []TenantConfig
	// CacheEntries and CacheTTL bound the response cache; zero values take
	// the defaults below. The cache has no off switch because it only ever
	// applies to requests that pinned temperature to 0 (see Cacheable).
	CacheEntries int
	CacheTTL     time.Duration
}

const (
	defaultCacheEntries = 1024
	defaultCacheTTL     = 5 * time.Minute
)

// Server is the gateway's HTTP handler.
type Server struct {
	cfg      Config
	mux      *http.ServeMux
	health   *http.Client
	provider Provider
	cache    *ResponseCache
	// tenants maps API key -> tenant state; nil in open mode.
	tenants map[string]*tenant
}

func New(cfg Config) *Server {
	// Even a single provider goes through the router: its breaker turns a
	// dead upstream into fast 503s instead of a pile-up of hung requests.
	providers := make([]Provider, 0, len(cfg.OllamaURLs))
	for i, url := range cfg.OllamaURLs {
		name := "ollama"
		if len(cfg.OllamaURLs) > 1 {
			name = fmt.Sprintf("ollama-%d", i+1)
		}
		providers = append(providers, NewNamedOllamaProvider(name, url))
	}
	return NewWithProvider(cfg, NewRouter(DefaultBreakerConfig(), providers...))
}

// NewWithProvider lets tests (and later, the router) inject the provider.
func NewWithProvider(cfg Config, p Provider) *Server {
	if cfg.CacheEntries == 0 {
		cfg.CacheEntries = defaultCacheEntries
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = defaultCacheTTL
	}
	s := &Server{
		cfg:      cfg,
		mux:      http.NewServeMux(),
		health:   &http.Client{Timeout: 5 * time.Second},
		provider: p,
		cache:    NewResponseCache(cfg.CacheEntries, cfg.CacheTTL),
		tenants:  buildTenants(cfg.Tenants),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Start launches the server's background work (currently the recovery
// probes) and returns immediately. ctx cancellation stops it.
func (s *Server) Start(ctx context.Context) {
	if router, ok := s.provider.(*Router); ok {
		router.StartProbes(ctx, defaultProbeInterval)
	}
}

type healthStatus struct {
	Status   string           `json:"status"`
	Ollama   string           `json:"ollama"`
	Postgres string           `json:"postgres"`
	Provider []ProviderStatus `json:"providers,omitempty"`
	Budgets  []TenantStatus   `json:"budgets,omitempty"`
	Cache    *CacheStatus     `json:"cache,omitempty"`
}

// handleHealthz reports the gateway's own liveness plus reachability of its
// dependencies, so `curl /healthz` after `docker compose up` tells you
// exactly which piece of the stack is missing.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	st := healthStatus{Status: "ok", Ollama: "ok", Postgres: "ok"}
	if router, ok := s.provider.(*Router); ok {
		st.Provider = router.Status()
	}
	st.Budgets = s.tenantStatus()
	st.Cache = s.cache.Status()

	if len(s.cfg.OllamaURLs) > 0 {
		resp, err := s.health.Get(s.cfg.OllamaURLs[0] + "/api/version")
		if err != nil {
			st.Ollama = "unreachable: " + err.Error()
		} else {
			resp.Body.Close()
		}
	}

	conn, err := net.DialTimeout("tcp", s.cfg.PostgresAddr, 2*time.Second)
	if err != nil {
		st.Postgres = "unreachable: " + err.Error()
	} else {
		conn.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	if st.Ollama != "ok" || st.Postgres != "ok" {
		st.Status = "degraded"
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(st)
}
