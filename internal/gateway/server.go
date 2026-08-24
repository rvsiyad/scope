// Package gateway implements the OpenAI-compatible LLM gateway: the HTTP
// surface clients point their SDKs at, plus everything that sits between a
// client request and a provider call.
package gateway

import (
	"encoding/json"
	"net"
	"net/http"
	"time"
)

type Config struct {
	// Addr is the listen address, e.g. ":8080".
	Addr string
	// OllamaURL is the base URL of the Ollama server, e.g. "http://localhost:11434".
	OllamaURL string
	// PostgresAddr is the host:port of Postgres, health-checked over TCP only
	// until the gateway actually stores tenants there.
	PostgresAddr string
}

// Server is the gateway's HTTP handler.
type Server struct {
	cfg      Config
	mux      *http.ServeMux
	health   *http.Client
	provider Provider
}

func New(cfg Config) *Server {
	return NewWithProvider(cfg, NewOllamaProvider(cfg.OllamaURL))
}

// NewWithProvider lets tests (and later, the router) inject the provider.
func NewWithProvider(cfg Config, p Provider) *Server {
	s := &Server{
		cfg:      cfg,
		mux:      http.NewServeMux(),
		health:   &http.Client{Timeout: 5 * time.Second},
		provider: p,
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type healthStatus struct {
	Status   string `json:"status"`
	Ollama   string `json:"ollama"`
	Postgres string `json:"postgres"`
}

// handleHealthz reports the gateway's own liveness plus reachability of its
// dependencies, so `curl /healthz` after `docker compose up` tells you
// exactly which piece of the stack is missing.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	st := healthStatus{Status: "ok", Ollama: "ok", Postgres: "ok"}

	resp, err := s.health.Get(s.cfg.OllamaURL + "/api/version")
	if err != nil {
		st.Ollama = "unreachable: " + err.Error()
	} else {
		resp.Body.Close()
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
