package gateway

import (
	"encoding/json"
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
		writeAPIError(w, http.StatusNotImplemented, "invalid_request_error", "streaming is not implemented yet")
		return
	}

	oresp, err := s.ollamaChat(r.Context(), req)
	if err != nil {
		log.Printf("provider error: %v", err)
		writeAPIError(w, http.StatusBadGateway, "api_error", "upstream provider error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(oresp.toOpenAI(time.Now().Unix()))
}
