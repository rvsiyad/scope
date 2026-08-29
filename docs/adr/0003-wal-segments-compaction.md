# ADR 0003: WAL + immutable segments + compaction (LSM-lite, not B-tree)

Status: accepted

## Context

Telemetry ingest is relentlessly write-heavy, almost append-only, and
read by time range. A B-tree pays for random in-place updates we never
do; the log-structured family (WAL in front, immutable sorted files
behind, background merges) matches the workload exactly — it is the shape
of Prometheus, LevelDB/RocksDB, and Cassandra for the same reason.

Every storage design lives inside the amplification triangle: write
amplification (bytes written per byte ingested), read amplification
(files consulted per query), space amplification (bytes held per byte of
data). You pick which two to be good at, per workload.

## Decision

Three layers, each with one job:

- **WAL first** (`internal/wal`): every accepted batch is appended as a
  length-prefixed, CRC-framed record before the ack. The fsync policy is
  the ack's meaning: `always` means a 204 survives power loss; `interval`
  means a bounded window rides on the page cache. Recovery replays to the
  first corrupt/torn record and truncates — a torn tail is expected
  crash debris, not an error.
- **Head block → immutable segments**: live series accumulate in memory
  (as Gorilla streams); a periodic flush writes a time-partitioned
  segment file that is never modified afterward. Immutability is what
  makes reads simple (merge head + segments), crashes safe (a
  half-written segment is simply not the published one), and backups
  trivial.
- **Compaction + retention**: a background pass merges small segments and
  drops those wholly past retention, using write-new → fsync → atomic
  rename so a crash mid-compaction leaves either the old set or the new
  set, never a mixture. Compaction bounds read amplification (segment
  count) at the price of re-writing data (write amplification) — the
  triangle, paid deliberately.

## Consequences

- The durability dial is measured, not asserted: fsync=`always` ceilings
  at ~25k points/s on the dev machine (each ack waits a serialized ~4 ms
  fsync) while `interval` sustains ~600k points/s — docs/benchmarks.md
  prices both, including the honest seconds-of-backlog acks past
  saturation.
- Crash recovery is a composition, not a feature: segments (immutable) +
  WAL replay into the head = the kill -9 demo, verified through the
  query path.
- The trade accepted: everything is append-ordered. Out-of-order samples
  are rejected at the door, and there is no update/delete path at all —
  correct for telemetry, disqualifying for OLTP, which is the point of
  saying "layout follows workload" out loud.
