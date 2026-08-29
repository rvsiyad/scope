# ADR 0001: Build the observability backend instead of using Prometheus/ClickHouse

Status: accepted

## Context

The gateway needs metrics and traces. The industry answer is a config
file: scrape with Prometheus, or ship spans to ClickHouse behind an OTLP
collector, and be done in an afternoon. Both are excellent software and
either would have been the right call for a product team on a deadline.

This is not a product team on a deadline. The project exists to learn and
demonstrate how storage engines, query engines, and telemetry pipelines
actually work — the layer that is ~70% of production AI-infrastructure
work and almost never present in portfolios. Importing Prometheus would
have outsourced exactly the part that is the point.

## Decision

Build the backend from scratch, component by canonical component: a
write-ahead log with CRC records and torn-write recovery, Gorilla
compression from the paper, a head-block/immutable-segment storage engine
with an inverted label index, compaction with retention, a trace store
with id-keyed indexes, a PromQL-lite query engine, and dashboards that
are a pure client of the query API. Use the same *architecture* as the
real systems (Prometheus's TSDB, Facebook's Gorilla) so every design
conversation transfers, and measure everything so the miniature earns
numbers instead of hand-waves.

Postgres stays for what it is genuinely right for (tenants and API keys —
relational, low-volume, transactional); building a relational database
teaches nothing this project needs.

## Consequences

- Every claim about the stack is inspectable and measured: 3.07
  bytes/sample compression on real telemetry, ~600k points/s sustained
  ingest with relaxed fsync on a laptop, single-digit-ms dashboard
  queries, kill -9 recovery proven through the query path
  (docs/benchmarks.md).
- The system is deliberately smaller than what it imitates: no
  distributed anything, no downsampling, no histogram buckets on the wire
  (quantiles come from raw samples — exact for what is kept, and the
  documented upgrade path is pre-bucketed histograms for cross-instance
  aggregation).
- In production we would run Prometheus (or Mimir/VictoriaMetrics at
  scale) and an OTLP pipeline without hesitation — and after building the
  miniature, we can say precisely what those systems are doing for us and
  what their configuration knobs actually trade.
