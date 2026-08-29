# ADR 0005: Cardinality is a budget, enforced at the door

Status: accepted

## Context

In a labeled TSDB, cost is not per sample — it is per *series*. Every
distinct label combination mints a live Gorilla stream in the head, an
entry in the series registry, and posting-list slots in the inverted
index, forever. Samples for an existing series are nearly free; a new
series is a permanent allocation.

That makes one class of label lethal: unbounded values. A `request_id`,
`user_id`, or raw URL label turns every request into a new series, and
the head grows without limit until the process OOMs. This is the classic
way real TSDBs die — "cardinality explosion" is the incident category —
and it is always a well-intentioned label ("we wanted to filter by user")
rather than an attack.

## Decision

Two lines of defense:

- **Schema discipline at the emitter.** The gateway's metric labels are a
  closed set of bounded dimensions — `tenant`, `model`, `provider`,
  `outcome`, `cache` — each with a small, operator-controlled value
  space. High-cardinality identifiers (trace ids, request ids) belong to
  the *trace store*, which is keyed for exactly that shape; the waterfall
  links a metric's world to a request's world, so nothing is lost by
  keeping ids out of labels.
- **A hard limit at the store.** The head enforces a maximum series count
  (`SCOPE_TSDB_MAX_SERIES`, default 10k): a sample that would mint a new
  series past the cap is rejected with a counted, visible error, while
  samples for known series always pass. The guard survives head flushes —
  the cap carries forward, because a flush emptying the head must not
  reopen the door.

## Consequences

- A cardinality incident becomes a rejection counter climbing on
  `/healthz` — diagnosable at the door, at the moment of the mistake —
  instead of an OOM hours later with the evidence inside the corpse.
- The cap is honest about being a circuit breaker, not a fix: when it
  trips, the remedy is fixing the offending label, and the rejection
  count says exactly when it started.
- The two-store split (ADR 0006) is half of cardinality control: metrics
  stay aggregate because per-request identity has somewhere better
  to live.
