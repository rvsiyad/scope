package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// The emitter's one law: telemetry must never hurt the traffic it observes.
// Record* calls are a non-blocking send onto a bounded buffer — microseconds,
// no I/O, no lock shared with the flusher's network calls. A background
// goroutine drains the buffer and ships batches. When the collector is slow
// or down there are only three possible policies: block the hot path (turns
// an observability outage into a gateway outage), queue unboundedly (a slow
// OOM — the same outage, later), or drop and count what was dropped. This
// emitter drops. Lost telemetry is a bounded, measured loss; a lost user
// request is not.

// Config configures an Emitter. Zero values take the defaults below.
type Config struct {
	// CollectorURL is the ingest endpoint, e.g. "http://localhost:8091".
	// The emitter POSTs batches to CollectorURL + "/v1/ingest".
	CollectorURL string
	// BatchSize triggers a flush when this many records are pending.
	// Batching amortizes HTTP overhead; FlushInterval bounds how stale a
	// quiet period leaves the dashboards.
	BatchSize     int
	FlushInterval time.Duration
	// BufferSize bounds the hot-path buffer; records beyond it are dropped.
	BufferSize int
}

const (
	defaultBatchSize     = 64
	defaultFlushInterval = time.Second
	defaultBufferSize    = 4096
)

// record is one buffered item; exactly one field is set.
type record struct {
	span   *Span
	metric *MetricPoint
}

// Emitter is the fire-and-forget facade the gateway records telemetry
// through. Nil-receiver safe: a gateway with no collector configured holds
// a nil *Emitter and every call is a no-op.
type Emitter struct {
	cfg    Config
	client *http.Client
	in     chan record

	// The scoreboard. Dropped counts records the buffer refused; flush
	// errors count whole batches lost to a dead collector. Both are the
	// honesty half of the drop policy: losing telemetry is allowed,
	// losing it silently is not.
	spansEmitted   atomic.Uint64
	metricsEmitted atomic.Uint64
	dropped        atomic.Uint64
	batchesSent    atomic.Uint64
	flushErrors    atomic.Uint64
}

func NewEmitter(cfg Config) *Emitter {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultBufferSize
	}
	return &Emitter{
		cfg: cfg,
		// The timeout doubles as the flusher's backpressure bound: one
		// in-flight POST can stall the drain loop for at most this long
		// before the buffer starts dropping — which is the design.
		client: &http.Client{Timeout: 2 * time.Second},
		in:     make(chan record, cfg.BufferSize),
	}
}

// RecordSpan hands a finished span to the emitter. Never blocks: a full
// buffer means the flusher is behind (collector slow or down) and the span
// is dropped and counted instead of making the caller wait.
func (e *Emitter) RecordSpan(s Span) {
	if e == nil {
		return
	}
	select {
	case e.in <- record{span: &s}:
	default:
		e.dropped.Add(1)
	}
}

// RecordMetric hands one sample to the emitter. Same contract as RecordSpan.
func (e *Emitter) RecordMetric(m MetricPoint) {
	if e == nil {
		return
	}
	select {
	case e.in <- record{metric: &m}:
	default:
		e.dropped.Add(1)
	}
}

// Start launches the drain loop and returns immediately. ctx cancellation
// stops it after one final best-effort flush of whatever is pending.
func (e *Emitter) Start(ctx context.Context) {
	if e == nil {
		return
	}
	go e.run(ctx)
}

func (e *Emitter) run(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	var batch Batch
	pending := 0
	flush := func() {
		if pending == 0 {
			return
		}
		e.send(batch)
		batch = Batch{}
		pending = 0
	}

	for {
		select {
		case rec := <-e.in:
			if rec.span != nil {
				batch.Spans = append(batch.Spans, *rec.span)
			} else {
				batch.Metrics = append(batch.Metrics, *rec.metric)
			}
			if pending++; pending >= e.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain what already made it into the buffer, then flush once.
			for {
				select {
				case rec := <-e.in:
					if rec.span != nil {
						batch.Spans = append(batch.Spans, *rec.span)
					} else {
						batch.Metrics = append(batch.Metrics, *rec.metric)
					}
					pending++
				default:
					flush()
					return
				}
			}
		}
	}
}

// send ships one batch. A failed batch is dropped, not retried: the buffer
// keeps filling while a retry loop courts a dead collector, so retrying
// only defers the drop to the buffer — with interest. Recovery is the next
// batch's problem; the flush-error counter records the loss.
func (e *Emitter) send(b Batch) {
	payload, err := json.Marshal(b)
	if err != nil {
		e.flushErrors.Add(1)
		return
	}
	resp, err := e.client.Post(e.cfg.CollectorURL+"/v1/ingest", "application/json", bytes.NewReader(payload))
	if err != nil {
		e.flushErrors.Add(1)
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		e.flushErrors.Add(1)
		return
	}
	e.batchesSent.Add(1)
	e.spansEmitted.Add(uint64(len(b.Spans)))
	e.metricsEmitted.Add(uint64(len(b.Metrics)))
}

// EmitterStatus is the emitter's scoreboard as reported by /healthz.
type EmitterStatus struct {
	SpansEmitted   uint64 `json:"spans_emitted"`
	MetricsEmitted uint64 `json:"metrics_emitted"`
	BatchesSent    uint64 `json:"batches_sent"`
	Dropped        uint64 `json:"dropped"`
	FlushErrors    uint64 `json:"flush_errors"`
}

func (e *Emitter) Status() *EmitterStatus {
	if e == nil {
		return nil
	}
	return &EmitterStatus{
		SpansEmitted:   e.spansEmitted.Load(),
		MetricsEmitted: e.metricsEmitted.Load(),
		BatchesSent:    e.batchesSent.Load(),
		Dropped:        e.dropped.Load(),
		FlushErrors:    e.flushErrors.Load(),
	}
}
