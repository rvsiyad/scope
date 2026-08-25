package gateway

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// fakeProvider scripts outcomes and counts calls, so tests can assert both
// where a request landed and that open breakers cost zero upstream calls.
type fakeProvider struct {
	name        string
	err         error
	streamErr   error         // error from ChatStream itself (pre-first-byte)
	chunks      []StreamChunk // replayed by the stream, then EOF
	recvErr     error         // returned after chunks are exhausted, instead of EOF
	chatCalls   int
	streamCalls int
}

func (p *fakeProvider) Name() string { return p.name }

func (p *fakeProvider) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	p.chatCalls++
	if p.err != nil {
		return ChatResponse{}, p.err
	}
	return ChatResponse{Model: p.name}, nil
}

func (p *fakeProvider) ChatStream(ctx context.Context, req ChatRequest) (ChunkStream, error) {
	p.streamCalls++
	if p.streamErr != nil {
		return nil, p.streamErr
	}
	return &fakeStream{p: p}, nil
}

type fakeStream struct {
	p   *fakeProvider
	pos int
}

func (s *fakeStream) Recv() (StreamChunk, error) {
	if s.pos < len(s.p.chunks) {
		chunk := s.p.chunks[s.pos]
		s.pos++
		return chunk, nil
	}
	if s.p.recvErr != nil {
		return StreamChunk{}, s.p.recvErr
	}
	return StreamChunk{}, io.EOF
}

func (s *fakeStream) Close() error { return nil }

func routerClock(r *Router) *fakeClock {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	for _, rp := range r.chain {
		rp.breaker.now = clock.now
	}
	return clock
}

func TestRouterFailsOverToNextProvider(t *testing.T) {
	primary := &fakeProvider{name: "primary", err: errors.New("boom")}
	backup := &fakeProvider{name: "backup"}
	r := NewRouter(DefaultBreakerConfig(), primary, backup)

	resp, err := r.Chat(context.Background(), ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "backup" {
		t.Fatalf("served by %q, want backup", resp.Model)
	}
	if primary.chatCalls != 1 || backup.chatCalls != 1 {
		t.Fatalf("calls = primary %d backup %d, want 1 and 1", primary.chatCalls, backup.chatCalls)
	}
}

func TestRouterSkipsOpenBreakerWithoutCalling(t *testing.T) {
	primary := &fakeProvider{name: "primary", err: errors.New("boom")}
	backup := &fakeProvider{name: "backup"}
	r := NewRouter(BreakerConfig{FailureThreshold: 2, OpenTimeout: time.Minute}, primary, backup)
	routerClock(r)

	for range 3 {
		if _, err := r.Chat(context.Background(), ChatRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	// Two failures trip primary's breaker; the third request must not have
	// touched primary at all — that's the fail-fast the breaker buys.
	if primary.chatCalls != 2 {
		t.Fatalf("primary called %d times, want 2 (skipped once open)", primary.chatCalls)
	}
	if backup.chatCalls != 3 {
		t.Fatalf("backup called %d times, want 3", backup.chatCalls)
	}
}

func TestRouterAllDownFailsFast(t *testing.T) {
	only := &fakeProvider{name: "only", err: errors.New("boom")}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, only)
	routerClock(r)

	if _, err := r.Chat(context.Background(), ChatRequest{}); err == nil {
		t.Fatal("want error from failing provider")
	}
	_, err := r.Chat(context.Background(), ChatRequest{})
	if !errors.Is(err, ErrNoHealthyProvider) {
		t.Fatalf("err = %v, want ErrNoHealthyProvider", err)
	}
	if only.chatCalls != 1 {
		t.Fatalf("provider called %d times, want 1 (breaker open)", only.chatCalls)
	}
}

func TestRouterTrafficReturnsAfterRecovery(t *testing.T) {
	primary := &fakeProvider{name: "primary", err: errors.New("boom")}
	backup := &fakeProvider{name: "backup"}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, primary, backup)
	clock := routerClock(r)

	if _, err := r.Chat(context.Background(), ChatRequest{}); err != nil {
		t.Fatal(err)
	}

	// Provider comes back; after the cooldown the half-open trial goes to
	// primary, succeeds, and traffic returns there.
	primary.err = nil
	clock.advance(time.Minute)

	resp, err := r.Chat(context.Background(), ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "primary" {
		t.Fatalf("served by %q, want primary (recovered)", resp.Model)
	}
	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("primary breaker = %v, want closed", got)
	}
}

func TestRouterClientCancelIsNotProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	primary := &fakeProvider{name: "primary", err: ctx.Err()}
	backup := &fakeProvider{name: "backup"}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, primary, backup)

	if _, err := r.Chat(ctx, ChatRequest{}); err == nil {
		t.Fatal("want the cancellation error to surface")
	}
	if backup.chatCalls != 0 {
		t.Fatal("must not fail over for a client that already left")
	}
	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("primary breaker = %v, want closed (cancel is not a failure)", got)
	}
}

func TestRouterStreamFailsOverBeforeFirstByte(t *testing.T) {
	primary := &fakeProvider{name: "primary", streamErr: errors.New("connect refused")}
	backup := &fakeProvider{name: "backup", chunks: []StreamChunk{{Content: "hi", Model: "backup"}}}
	r := NewRouter(DefaultBreakerConfig(), primary, backup)

	stream, err := r.ChatStream(context.Background(), ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Model != "backup" {
		t.Fatalf("stream served by %q, want backup", chunk.Model)
	}
}

func TestRouterStreamCleanEOFCountsAsSuccess(t *testing.T) {
	p := &fakeProvider{name: "p", chunks: []StreamChunk{{Content: "hi"}}}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)

	stream, err := r.ChatStream(context.Background(), ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("breaker = %v, want closed", got)
	}
}

func TestRouterMidStreamErrorRecordsFailureWithoutRetry(t *testing.T) {
	primary := &fakeProvider{
		name:    "primary",
		chunks:  []StreamChunk{{Content: "par"}},
		recvErr: errors.New("connection reset"),
	}
	backup := &fakeProvider{name: "backup"}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, primary, backup)

	stream, err := r.ChatStream(context.Background(), ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	// The first byte reached the client, so this failure must surface as-is:
	// replaying the request on backup would duplicate the streamed content.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("want the mid-stream error to surface")
	}
	if backup.streamCalls != 0 {
		t.Fatal("must not retry a stream that already delivered content")
	}
	if got := r.chain[0].breaker.State(); got != StateOpen {
		t.Fatalf("breaker = %v, want open (mid-stream failure recorded)", got)
	}
}

func TestRouterStreamDisconnectFalloutNotRecorded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &fakeProvider{name: "p", chunks: []StreamChunk{{Content: "hi"}}, recvErr: io.ErrClosedPipe}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, p)

	stream, err := r.ChatStream(ctx, ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	// Client hangs up; the handler closes the upstream stream and the pending
	// Recv errors out. That error is our own doing, not the provider's.
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("want the close fallout error to surface")
	}
	if got := r.chain[0].breaker.State(); got != StateClosed {
		t.Fatalf("breaker = %v, want closed (disconnect fallout ignored)", got)
	}
}

func TestServerReturns503WhenAllProvidersDown(t *testing.T) {
	only := &fakeProvider{name: "only", err: errors.New("boom")}
	r := NewRouter(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}, only)
	routerClock(r)

	// Trip the breaker, then hit the handler: clients must see a clean 503.
	r.Chat(context.Background(), ChatRequest{})

	srv := NewWithProvider(Config{}, r)
	rec := postChat(t, srv, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != 503 {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
