package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// OllamaProvider adapts Ollama's native /api/chat dialect to the Provider
// contract. We deliberately target the native API rather than Ollama's own
// OpenAI-compat endpoint: the translation layer is the point — it is exactly
// what the Anthropic adapter will need for real.
type OllamaProvider struct {
	baseURL string
	// No overall timeout: a completion legitimately takes as long as the
	// model takes. Cancellation comes from the request context.
	http *http.Client
}

func NewOllamaProvider(baseURL string) *OllamaProvider {
	return &OllamaProvider{baseURL: baseURL, http: &http.Client{}}
}

func (p *OllamaProvider) Name() string { return "ollama" }

// CheckHealth satisfies HealthChecker with Ollama's cheapest endpoint.
func (p *OllamaProvider) CheckHealth(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", p.baseURL+"/api/version", nil)
	if err != nil {
		return err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama version endpoint returned %d", resp.StatusCode)
	}
	return nil
}

type ollamaChatRequest struct {
	Model    string         `json:"model"`
	Messages []Message      `json:"messages"`
	Stream   bool           `json:"stream"`
	Options  map[string]any `json:"options,omitempty"`
}

// ollamaChatResponse is both the non-streaming response body and, in
// streaming mode, the shape of each NDJSON line.
type ollamaChatResponse struct {
	Model           string  `json:"model"`
	Message         Message `json:"message"`
	Done            bool    `json:"done"`
	DoneReason      string  `json:"done_reason"`
	PromptEvalCount int     `json:"prompt_eval_count"`
	EvalCount       int     `json:"eval_count"`
}

func toOllama(req ChatRequest, stream bool) ollamaChatRequest {
	oreq := ollamaChatRequest{Model: req.Model, Messages: req.Messages, Stream: stream}
	if req.Temperature != nil || req.MaxTokens != nil {
		oreq.Options = map[string]any{}
		if req.Temperature != nil {
			oreq.Options["temperature"] = *req.Temperature
		}
		if req.MaxTokens != nil {
			oreq.Options["num_predict"] = *req.MaxTokens
		}
	}
	return oreq
}

func (p *OllamaProvider) post(ctx context.Context, req ChatRequest, stream bool) (*http.Response, error) {
	body, err := json.Marshal(toOllama(req, stream))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", p.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, msg)
	}
	return resp, nil
}

func (p *OllamaProvider) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	resp, err := p.post(ctx, req, false)
	if err != nil {
		return ChatResponse{}, err
	}
	defer resp.Body.Close()

	var oresp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&oresp); err != nil {
		return ChatResponse{}, fmt.Errorf("decoding ollama response: %w", err)
	}
	return ChatResponse{
		ID:      newCompletionID(),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   oresp.Model,
		Choices: []Choice{{Index: 0, Message: oresp.Message, FinishReason: finishReason(oresp.DoneReason)}},
		Usage: &Usage{
			PromptTokens:     oresp.PromptEvalCount,
			CompletionTokens: oresp.EvalCount,
			TotalTokens:      oresp.PromptEvalCount + oresp.EvalCount,
		},
	}, nil
}

func (p *OllamaProvider) ChatStream(ctx context.Context, req ChatRequest) (ChunkStream, error) {
	resp, err := p.post(ctx, req, true)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &ollamaStream{body: resp.Body, sc: sc}, nil
}

// ollamaStream turns Ollama's NDJSON stream (one JSON object per line, final
// line has done=true) into normalized chunks.
type ollamaStream struct {
	body io.ReadCloser
	sc   *bufio.Scanner
	done bool
	// closeOnce: Close is called both by the handler's defer and by the
	// client-disconnect watcher, possibly concurrently.
	closeOnce sync.Once
}

func (s *ollamaStream) Recv() (StreamChunk, error) {
	if s.done {
		return StreamChunk{}, io.EOF
	}
	for s.sc.Scan() {
		line := bytes.TrimSpace(s.sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var oresp ollamaChatResponse
		if err := json.Unmarshal(line, &oresp); err != nil {
			return StreamChunk{}, fmt.Errorf("decoding ollama stream line: %w", err)
		}
		chunk := StreamChunk{Content: oresp.Message.Content, Model: oresp.Model}
		if oresp.Done {
			s.done = true
			chunk.FinishReason = finishReason(oresp.DoneReason)
			chunk.Usage = &Usage{
				PromptTokens:     oresp.PromptEvalCount,
				CompletionTokens: oresp.EvalCount,
				TotalTokens:      oresp.PromptEvalCount + oresp.EvalCount,
			}
		}
		return chunk, nil
	}
	if err := s.sc.Err(); err != nil {
		return StreamChunk{}, err
	}
	// Upstream closed without a done=true line: surface as a clean EOF.
	return StreamChunk{}, io.EOF
}

func (s *ollamaStream) Close() error {
	s.closeOnce.Do(func() { s.body.Close() })
	return nil
}

func finishReason(doneReason string) string {
	if doneReason == "length" {
		return "length"
	}
	return "stop"
}
