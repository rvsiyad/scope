package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON: "+err.Error())
		return
	}
	if req.Model == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "'model' is required")
		return
	}
	if len(req.Messages) == 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "'messages' must be non-empty")
		return
	}

	if req.Stream {
		s.streamChat(w, r, req)
		return
	}

	resp, err := s.provider.Chat(r.Context(), req)
	if err != nil {
		log.Printf("provider %s error: %v", s.provider.Name(), err)
		writeProviderError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// streamChat relays a provider stream to the client as SSE. The invariant
// that makes the gateway a real streaming proxy: every chunk is written AND
// flushed before the next Recv — nothing above one chunk is ever buffered.
func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, req ChatRequest) {
	stream, err := s.provider.ChatStream(r.Context(), req)
	if err != nil {
		log.Printf("provider %s error: %v", s.provider.Name(), err)
		writeProviderError(w, err)
		return
	}
	defer stream.Close()
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

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Status and headers are already sent; all we can do is stop.
			// The missing [DONE] tells the client the stream ended abnormally.
			log.Printf("provider %s mid-stream error: %v", s.provider.Name(), err)
			return
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
			log.Printf("client disconnected mid-stream: %v", err)
			return
		}
		flusher.Flush()
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
