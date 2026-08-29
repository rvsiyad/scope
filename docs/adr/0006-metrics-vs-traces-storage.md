# ADR 0006: Metrics and traces get different stores — layout follows read pattern

Status: accepted

## Context

Both stores ingest the same way (validated batch → WAL → in-memory head →
immutable segments → retention) and even share the WAL machinery. The
temptation is to finish the thought: one store for all telemetry.

But storage layout is determined by the *read* pattern, not the write
pattern, and the two shapes read nothing alike:

- **Metrics** are read by *label matcher over a time range*: "all series
  with `tenant=acme` for the last 15 minutes, aligned to steps." Nobody
  asks for one sample by identity.
- **Traces** are read by *identity*: "trace `ab12…`, whole, as a tree."
  Nobody scans all spans in a window except to list recent requests.

## Decision

Same skeleton, deliberately different organs:

- The **tsdb** organizes around series: a label → posting-list inverted
  index answers matcher queries by set intersection, samples live in
  per-series compressed streams, and reads merge head + segments by
  time range.
- The **trace store** organizes around traces: spans are grouped per
  trace id, an id-keyed index answers "fetch this trace" directly, and a
  secondary time index serves only the request-log listing. Sampling is
  decided per trace id (hash-based keep ratio) so a kept trace is always
  *whole* — a sampled-out span tree's other half is worthless.
- What genuinely generalized was reused: the WAL, the flush-to-immutable-
  segment lifecycle, and retention enforcement are the same pattern in
  both; the divergence is the indexes and the unit of storage.

## Consequences

- Each store is simple because it answers one question shape. The "one
  store for all telemetry" design would need both index families anyway,
  plus a compromise record layout serving neither read well — the trap is
  that the merge saves nothing but a directory name.
- The split is also the cardinality strategy (ADR 0005): identity-shaped
  data (request ids) lives in the id-keyed store; aggregate-shaped data
  (bounded labels) lives in the matcher-keyed store. Each store gets data
  shaped like its index.
- Logs, the third telemetry shape (needle-in-haystack text search), are
  out of scope — and would be a third layout (term indexes over bulk
  text), which is the lesson generalizing, not an omission hidden.
