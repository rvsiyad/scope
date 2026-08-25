package gateway

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
)

// ErrNoHealthyProvider means every provider in the chain was either skipped
// (breaker open) or failed. The handler maps it to 503: the outage is ours
// to report honestly, not to disguise as a slow error.
var ErrNoHealthyProvider = errors.New("no healthy provider available")

// Router is a Provider that fronts an ordered failover chain. Each upstream
// gets its own circuit breaker: requests go to the first provider whose
// breaker admits them, failures fall through to the next, and an open
// breaker is skipped without spending a call on it.
type Router struct {
	chain []*routedProvider
}

type routedProvider struct {
	provider Provider
	breaker  *Breaker
}

func NewRouter(cfg BreakerConfig, providers ...Provider) *Router {
	r := &Router{}
	for _, p := range providers {
		r.chain = append(r.chain, &routedProvider{provider: p, breaker: NewBreaker(cfg)})
	}
	return r
}

func (r *Router) Name() string { return "router" }

func (r *Router) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	for _, rp := range r.chain {
		if !rp.breaker.Allow() {
			continue
		}
		resp, err := rp.provider.Chat(ctx, req)
		if err == nil {
			rp.breaker.RecordSuccess()
			return resp, nil
		}
		if ctx.Err() != nil {
			// The client gave up (disconnect or deadline). That says nothing
			// about provider health — record nothing — and there is nobody
			// left to fail over for.
			return ChatResponse{}, err
		}
		rp.breaker.RecordFailure()
		log.Printf("provider %s failed, trying next: %v", rp.provider.Name(), err)
	}
	return ChatResponse{}, ErrNoHealthyProvider
}

// ChatStream fails over only while it is safe to do so: before the first
// byte. Once a provider has started streaming, chunks have already reached
// the client, and replaying the request elsewhere would duplicate content —
// so a mid-stream failure is recorded against the breaker but not retried.
func (r *Router) ChatStream(ctx context.Context, req ChatRequest) (ChunkStream, error) {
	for _, rp := range r.chain {
		if !rp.breaker.Allow() {
			continue
		}
		stream, err := rp.provider.ChatStream(ctx, req)
		if err == nil {
			return &breakerStream{ChunkStream: stream, ctx: ctx, rp: rp}, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		rp.breaker.RecordFailure()
		log.Printf("provider %s failed, trying next: %v", rp.provider.Name(), err)
	}
	return nil, ErrNoHealthyProvider
}

// breakerStream reports a stream's ultimate outcome to its provider's
// breaker: clean EOF is a success, a mid-stream error with the client still
// attached is a failure, and anything after client disconnect is noise.
type breakerStream struct {
	ChunkStream
	ctx  context.Context
	rp   *routedProvider
	once sync.Once
}

func (s *breakerStream) Recv() (StreamChunk, error) {
	chunk, err := s.ChunkStream.Recv()
	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		s.once.Do(s.rp.breaker.RecordSuccess)
	case s.ctx.Err() != nil:
		// Client disconnected; the error is fallout from our own Close.
	default:
		s.once.Do(s.rp.breaker.RecordFailure)
	}
	return chunk, err
}
