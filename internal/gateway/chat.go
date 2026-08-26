package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	tr := s.startTrace()
	defer tr.finish()

	endAuth := tr.span("auth")
	tenant, ok := s.authTenant(w, r)
	endAuth(nil)
	if !ok {
		tr.setAttr("outcome", "unauthorized")
		return
	}
	tr.setAttr("tenant", tenantName(tenant))

	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		tr.setAttr("outcome", "invalid_request")
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON: "+err.Error())
		return
	}
	if req.Model == "" {
		tr.setAttr("outcome", "invalid_request")
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "'model' is required")
		return
	}
	if len(req.Messages) == 0 {
		tr.setAttr("outcome", "invalid_request")
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "'messages' must be non-empty")
		return
	}
	tr.setAttr("model", req.Model)
	tr.setAttr("stream", strconv.FormatBool(req.Stream))

	// Cache lookup sits after auth (the key needs the tenant) but before
	// admission, and a hit is free: budgets exist because provider tokens
	// cost money, and a hit consumes zero of them. Charging for a hit would
	// bill the tenant for nothing; 429ing one would refuse a response that
	// costs nothing to serve.
	if Cacheable(req) {
		endLookup := tr.span("cache_lookup")
		cached, hit := s.cache.Get(tenantName(tenant), req)
		if hit {
			endLookup(map[string]string{"result": "hit"})
			tr.setAttr("cache", "hit")
			tr.setAttr("outcome", "ok")
			tr.charge(0, s.cfg.PricePerMTokens) // a hit buys zero provider tokens
			if req.Stream {
				s.replayCachedStream(w, req, cached)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(cached)
			return
		}
		endLookup(map[string]string{"result": "miss"})
		tr.setAttr("cache", "miss")
	} else {
		tr.setAttr("cache", "bypass")
	}

	endReserve := tr.span("reserve")
	res, ok := s.admit(w, tenant, req)
	endReserve(nil)
	if !ok {
		tr.setAttr("outcome", "rejected")
		return
	}

	if req.Stream {
		s.streamChat(w, r, tenantName(tenant), req, res, tr)
		return
	}

	endProvider := tr.span("provider")
	providerStart := time.Now()
	resp, err := s.provider.Chat(r.Context(), req)
	if err != nil {
		endProvider(map[string]string{"provider": s.provider.Name(), "error": err.Error()})
		s.settleTraced(tr, res, 0) // the call never produced tokens; full refund
		tr.setAttr("outcome", "provider_error")
		log.Printf("provider %s error: %v", s.provider.Name(), err)
		writeProviderError(w, err)
		return
	}
	// For a non-streaming call the first byte is the whole response, so the
	// call duration is both latency and TTFT.
	ttft := time.Since(providerStart)
	actual := actualTokens(resp, req)
	endProvider(map[string]string{
		"provider": s.provider.Name(),
		"ttft_ms":  msString(ttft),
	})
	tr.metric("gateway_ttft_ms", float64(ttft)/float64(time.Millisecond), nil)

	s.settleTraced(tr, res, actual)
	tr.charge(actual, s.cfg.PricePerMTokens)
	tr.setAttr("outcome", "ok")
	s.cache.Put(tenantName(tenant), req, resp)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// settleTraced wraps Settle in its own span: it is microseconds of in-memory
// work, but it is a real phase of the request's story — the waterfall should
// show the budget being trued up, not leave the trace ending at the provider.
func (s *Server) settleTraced(tr *reqTrace, res *Reservation, actual int) {
	endSettle := tr.span("settle")
	res.Settle(actual)
	endSettle(map[string]string{"tokens_charged": strconv.Itoa(actual)})
}

// admit reserves the request's estimated cost against the tenant's budget,
// writing the rejection itself. A 429 for a temporarily exhausted budget
// carries Retry-After — the honest instant refill will cover the estimate —
// while an estimate bigger than the whole budget gets no Retry-After,
// because no amount of waiting would admit it.
func (s *Server) admit(w http.ResponseWriter, t *tenant, req ChatRequest) (*Reservation, bool) {
	if t == nil {
		return nil, true // open mode: nothing to reserve, nil settles safely
	}
	res, err := t.budget.Reserve(estimateTokens(req))
	if err == nil {
		t.admitted.Add(1)
		res.onSettle = func(actual int) { t.tokensCharged.Add(uint64(actual)) }
		return res, true
	}
	t.rejected.Add(1)
	var exhausted *BudgetExhaustedError
	if errors.As(err, &exhausted) {
		secs := int(exhausted.RetryAfter.Seconds())
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeAPIErrorCode(w, http.StatusTooManyRequests, "rate_limit_error", "token_budget_exhausted",
			fmt.Sprintf("token budget for tenant %q exhausted; retry after %ds", t.name, secs))
	} else {
		writeAPIErrorCode(w, http.StatusTooManyRequests, "rate_limit_error", "request_exceeds_budget",
			"estimated cost exceeds the tenant's entire budget; lower max_tokens or shorten the prompt")
	}
	return nil, false
}

// actualTokens is what a non-streaming response really cost: the provider's
// own count when it reports one, otherwise the prompt estimate plus metered
// response text.
func actualTokens(resp ChatResponse, req ChatRequest) int {
	if resp.Usage != nil {
		return resp.Usage.TotalTokens
	}
	n := estimatePromptTokens(req)
	for _, c := range resp.Choices {
		n += meterText(len(c.Message.Content))
	}
	return n
}

// streamChat relays a provider stream to the client as SSE. The invariant
// that makes the gateway a real streaming proxy: every chunk is written AND
// flushed before the next Recv — nothing above one chunk is ever buffered.
// (The cache-fill accumulator below is a copy of what already streamed, not
// a buffer between provider and client.)
func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, cacheTenant string, req ChatRequest, res *Reservation, tr *reqTrace) {
	endProvider := tr.span("provider")
	providerStart := time.Now()
	stream, err := s.provider.ChatStream(r.Context(), req)
	if err != nil {
		endProvider(map[string]string{"provider": s.provider.Name(), "error": err.Error()})
		s.settleTraced(tr, res, 0) // stream never started; full refund
		tr.setAttr("outcome", "provider_error")
		log.Printf("provider %s error: %v", s.provider.Name(), err)
		writeProviderError(w, err)
		return
	}
	defer stream.Close()

	// Settle on every way out of this function. Best case the final chunk
	// carried the provider's real usage; otherwise (disconnect, mid-stream
	// error) charge the prompt estimate plus the content actually relayed —
	// tokens the tenant consumed even if nobody saw the end of the stream.
	// The provider span ends here too: for a stream, the provider phase
	// lasts until the last byte, wherever the stream stopped.
	meteredChars := 0
	var usage *Usage
	var ttft time.Duration
	defer func() {
		attrs := map[string]string{"provider": s.provider.Name()}
		if ttft > 0 {
			attrs["ttft_ms"] = msString(ttft)
		}
		endProvider(attrs)
		actual := estimatePromptTokens(req) + meterText(meteredChars)
		if usage != nil {
			actual = usage.TotalTokens
		}
		s.settleTraced(tr, res, actual)
		tr.charge(actual, s.cfg.PricePerMTokens)
	}()
	// If the client disconnects, its request context cancels; closing the
	// upstream stream then unblocks a pending Recv, so we stop paying for
	// tokens nobody is reading.
	stop := context.AfterFunc(r.Context(), func() { stream.Close() })
	defer stop()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "api_error", "streaming unsupported by server")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	// Send headers immediately: SSE clients (and their HTTP libraries) wait
	// on response headers before reading events, so holding them back until
	// the first token would stall every client for the provider's TTFT.
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	id := newCompletionID()
	created := time.Now().Unix()
	first := true

	// A stream is cache-fillable only once it ends cleanly (finish reason
	// seen, EOF reached): caching a half-delivered completion would replay
	// the truncation to every later hit.
	fill := Cacheable(req)
	var content strings.Builder
	finishReason, model := "", req.Model

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Status and headers are already sent; all we can do is stop.
			// The missing [DONE] tells the client the stream ended abnormally.
			tr.setAttr("outcome", "provider_error_midstream")
			log.Printf("provider %s mid-stream error: %v", s.provider.Name(), err)
			return
		}
		if ttft == 0 {
			// The TTFT marker: how long the client stared at a spinner
			// before the first token arrived — the latency LLM users
			// actually feel, and the headline number on the dashboard.
			ttft = time.Since(providerStart)
			tr.metric("gateway_ttft_ms", float64(ttft)/float64(time.Millisecond), nil)
		}
		meteredChars += len(chunk.Content)
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
		if fill {
			content.WriteString(chunk.Content)
			if chunk.FinishReason != "" {
				finishReason = chunk.FinishReason
			}
			if chunk.Model != "" {
				model = chunk.Model
			}
		}

		event := ChatChunkResponse{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   chunk.Model,
			Choices: []ChunkChoice{{Delta: Delta{Content: chunk.Content}}},
		}
		if event.Model == "" {
			event.Model = req.Model
		}
		if first {
			event.Choices[0].Delta.Role = "assistant"
			first = false
		}
		if chunk.FinishReason != "" {
			fr := chunk.FinishReason
			event.Choices[0].FinishReason = &fr
		}

		if err := writeSSE(w, event); err != nil {
			// Client went away mid-stream; Close() (deferred) tears down the
			// upstream call so we stop paying for tokens nobody reads.
			tr.setAttr("outcome", "client_disconnect")
			log.Printf("client disconnected mid-stream: %v", err)
			return
		}
		flusher.Flush()
	}
	tr.setAttr("outcome", "ok")

	if fill && finishReason != "" {
		s.cache.Put(cacheTenant, req, ChatResponse{
			ID:      id,
			Object:  "chat.completion",
			Created: created,
			Model:   model,
			Choices: []Choice{{
				Message:      Message{Role: "assistant", Content: content.String()},
				FinishReason: finishReason,
			}},
			Usage: usage,
		})
	}

	io.WriteString(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// replayCachedStream serves a cache hit to a client that asked for SSE. The
// completion already exists in full — there is no provider TTFT to hide —
// so it replays as a minimal, well-formed stream: one chunk carrying the
// whole message and the finish reason, then [DONE].
func (s *Server) replayCachedStream(w http.ResponseWriter, req ChatRequest, resp ChatResponse) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "api_error", "streaming unsupported by server")
		return
	}
	if len(resp.Choices) == 0 {
		writeAPIError(w, http.StatusInternalServerError, "api_error", "cached response has no choices")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	choice := resp.Choices[0]
	fr := choice.FinishReason
	if fr == "" {
		fr = "stop"
	}
	event := ChatChunkResponse{
		ID:      resp.ID,
		Object:  "chat.completion.chunk",
		Created: resp.Created,
		Model:   resp.Model,
		Choices: []ChunkChoice{{
			Delta:        Delta{Role: "assistant", Content: choice.Message.Content},
			FinishReason: &fr,
		}},
	}
	if event.Model == "" {
		event.Model = req.Model
	}
	if err := writeSSE(w, event); err != nil {
		return
	}
	io.WriteString(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// writeProviderError distinguishes "every provider is down or circuit-broken"
// (503 — our capacity problem, safe to retry elsewhere/later) from a single
// upstream call failing (502).
func writeProviderError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNoHealthyProvider) {
		writeAPIError(w, http.StatusServiceUnavailable, "api_error", "no healthy upstream provider")
		return
	}
	writeAPIError(w, http.StatusBadGateway, "api_error", "upstream provider error")
}

func writeSSE(w io.Writer, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, "data: "); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err = io.WriteString(w, "\n\n")
	return err
}
