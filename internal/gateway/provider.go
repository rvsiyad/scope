package gateway

import "context"

// Provider is the gateway's internal contract with an upstream LLM. The core
// speaks only normalized types (ChatRequest, StreamChunk); each provider gets
// one adapter translating its dialect in and out. Adding a provider must
// never touch the core.
type Provider interface {
	Name() string
	// Chat performs a full, non-streaming completion.
	Chat(ctx context.Context, req ChatRequest) (ChatResponse, error)
	// ChatStream starts a streaming completion. The caller must drain the
	// stream until io.EOF or Close it early (e.g. client disconnect).
	ChatStream(ctx context.Context, req ChatRequest) (ChunkStream, error)
}

// ChunkStream yields normalized streaming chunks. Recv blocks until the next
// chunk is available and returns io.EOF after the final chunk.
type ChunkStream interface {
	Recv() (StreamChunk, error)
	Close() error
}

// StreamChunk is one normalized increment of a streaming completion.
type StreamChunk struct {
	// Content is the text delta (may be empty on the final chunk).
	Content string
	// FinishReason is "" until the final chunk ("stop", "length", ...).
	FinishReason string
	// Usage is non-nil only on the final chunk, when token counts are known.
	Usage *Usage
	Model string
}
