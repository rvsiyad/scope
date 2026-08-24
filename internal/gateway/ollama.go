package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Ollama's native /api/chat dialect. We deliberately target the native API
// rather than Ollama's own OpenAI-compat endpoint: the translation layer is
// the point — it is exactly what the Anthropic adapter will need for real.

type ollamaChatRequest struct {
	Model    string         `json:"model"`
	Messages []Message      `json:"messages"`
	Stream   bool           `json:"stream"`
	Options  map[string]any `json:"options,omitempty"`
}

type ollamaChatResponse struct {
	Model           string  `json:"model"`
	Message         Message `json:"message"`
	Done            bool    `json:"done"`
	DoneReason      string  `json:"done_reason"`
	PromptEvalCount int     `json:"prompt_eval_count"`
	EvalCount       int     `json:"eval_count"`
}

func (s *Server) ollamaChat(ctx context.Context, req ChatRequest) (*ollamaChatResponse, error) {
	oreq := ollamaChatRequest{Model: req.Model, Messages: req.Messages}
	if req.Temperature != nil || req.MaxTokens != nil {
		oreq.Options = map[string]any{}
		if req.Temperature != nil {
			oreq.Options["temperature"] = *req.Temperature
		}
		if req.MaxTokens != nil {
			oreq.Options["num_predict"] = *req.MaxTokens
		}
	}

	body, err := json.Marshal(oreq)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.cfg.OllamaURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.llm.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, msg)
	}

	var oresp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&oresp); err != nil {
		return nil, fmt.Errorf("decoding ollama response: %w", err)
	}
	return &oresp, nil
}

// toOpenAI translates Ollama's response into the OpenAI shape clients expect.
func (o *ollamaChatResponse) toOpenAI(created int64) ChatResponse {
	finish := "stop"
	if o.DoneReason == "length" {
		finish = "length"
	}
	return ChatResponse{
		ID:      newCompletionID(),
		Object:  "chat.completion",
		Created: created,
		Model:   o.Model,
		Choices: []Choice{{Index: 0, Message: o.Message, FinishReason: finish}},
		Usage: &Usage{
			PromptTokens:     o.PromptEvalCount,
			CompletionTokens: o.EvalCount,
			TotalTokens:      o.PromptEvalCount + o.EvalCount,
		},
	}
}
