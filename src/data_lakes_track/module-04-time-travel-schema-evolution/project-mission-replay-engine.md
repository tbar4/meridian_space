# Capstone — Mission Replay Engine

**Module:** Data Lakes — M04: Time Travel and Schema Evolution
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%). The Module 2 (table format) and Module 3 (partition + clustering) capstones are the substrate.

---

## Mission Briefing

### From: SDA Investigations Lead

```
ARCHIVE BRIEFING — RC-2026-04-DL-004
SUBJECT: Mission Replay Engine — read-only query service for
         reconstructing operational state at past timestamps.
PRIORITY: P1 — required for Q3 accident-review process.
```

When something goes wrong on orbit, the investigation team needs to see the table as the operators saw it then — not as we see it now. The current archive lets us pin a snapshot, but we don't have a unified entry point that takes a timestamp, resolves to a snapshot, projects against the right schema, and returns the data. That's the engine you're building.

The replay engine is read-only. It does not commit; it does not modify; it does not affect the cold archive's data path. It is a service that accepts a `(table, target_timestamp_ms, predicate)` query and returns Arrow record batches. The full-mission replay (one mission of data over six months) must complete in under ten minutes against the live cold archive on the standard worker pool.

The engine is the user-facing tool for the accident-investigation workflow. Get it right and the investigators have what they need; get it wrong and they fall back to manual file-grepping against the data lake, which they have been doing for six years and have promised to stop.

---

## What You're Building

A Rust crate, `artemis-mission-replay`, exposing:

- A `ReplayEngine` struct constructed from a catalog reference and a table name, holding a `ReplaySession` per active query.
- A `replay_at(timestamp_ms, predicate) -> RecordBatchStream` method that resolves the timestamp to a snapshot, projects against the snapshot-time schema, applies the predicate, and streams batches.
- A `replay_at_snapshot(snapshot_id, predicate) -> RecordBatchStream` method for snapshot-ID-addressed replays.
- A `replay_diff(t_old, t_new) -> ChangeStream` method that emits the (added, removed) rows between the two times.
- A CLI binary, `artemis-replay`, with subcommands `query` (point-in-time read), `diff` (CDF between two times), and `inspect` (snapshot history, table metadata).
- Observability: every replay records the pinned snapshot ID, the schema ID used, the planning summary (manifests/files/row groups scanned vs total), and the elapsed time. The structured logs are consumed by the existing Artemis observability stack.

The engine handles schema changes that occurred between the replay target and the present: a query for data from six months ago, when the table had three fewer columns, returns the three-column-fewer schema (snapshot-time mode) rather than back-filling nulls (current mode).

---

## Functional Requirements

1. **Timestamp-to-snapshot resolution.** Implements the resolution rule from Lesson 2: the most recent snapshot whose `commit_timestamp_ms <= target_ms`. Resolution against a timestamp outside the retention window returns a clear "snapshot expired" error.
2. **Snapshot-time schema projection.** The default mode reads files projected against the schema that was current at the pinned snapshot's commit time. The `mode = current` parameter switches to projection against the current schema.
3. **Per-snapshot partition spec.** The planner uses the partition spec recorded in the pinned snapshot, not the table's current spec. Reads against pre-spec-change snapshots use the old spec; reads against post-spec-change snapshots use the new.
4. **Predicate lifting per snapshot's spec.** The predicate is lifted through the pinned snapshot's partition spec (Module 3 Lesson 3's lifting rules). Different spec → different lifting → potentially different pruning result.
5. **Streaming reads.** The result is a stream of `RecordBatch`es, not a materialized vector. Long replays must not blow up memory; the consumer pulls batches as it processes them.
6. **Schema-aware projection.** A data file written before a column was added is read with the missing column null-filled; a file written before a column was renamed is read with the rename transparently applied via field-ID matching.
7. **Change Data Feed.** `replay_diff(t_old, t_new)` returns two streams (`added`, `removed`) of `RecordBatch`es, computed by walking the snapshot chain between the two times and collecting Added/Deleted manifest entries.

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] A replay at `target_ms = T` against a table with snapshots committed at `T-5min`, `T-1min`, `T+1min` pins the snapshot committed at `T-1min`.
- [x] A replay at a `target_ms` older than the retention window returns a structured `SnapshotExpired` error containing the oldest available snapshot's timestamp.
- [x] A replay at `T` against a table whose schema changed at `T+1day` (a column added after the replay target) produces a record batch stream with the schema-at-T (no new column). The same replay with `mode = current` produces a stream with the new column null-filled.
- [x] A replay at `T` against a table whose partition spec changed at `T+1day` uses the partition spec that was current at T for predicate lifting. The pruning result is deterministically reproducible.
- [x] A replay of a column-renamed table (`voltage` → `panel_voltage` at `T+1day`) reads the column correctly under either projection mode: at `T` with `mode = snapshot_time` the column appears as `voltage`; with `mode = current` it appears as `panel_voltage` with the same values (field-ID matching makes the rename transparent).
- [x] A full-mission replay over six months of data (~2 TB compressed) completes in under 10 minutes on the standard worker pool, measured against the integration test bench.
- [x] `replay_diff(t_old, t_new)` against two snapshots that differ by one append commit returns the appended files' rows in the `added` stream and an empty `removed` stream.
- [x] A long-running replay's memory consumption (resident set, measured via `jemalloc-ctl::stats::resident`) stays under 2 GB regardless of the result size, demonstrating streaming behavior.

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The retention-window error handling is documented in `docs/expired-snapshot.md`. The doc describes what the engine returns when the target is expired, how the investigator escalates to the long-retention archive tier, and why the engine does not auto-fall-through.
- [ ] *(self-assessed)* The projection-mode choice is documented in `docs/projection-modes.md`. The doc justifies snapshot-time as the default for accident investigation and explains when current mode is the right choice.
- [ ] *(self-assessed)* The streaming-memory discipline is documented in `docs/streaming-memory.md`. The doc explains how the engine bounds memory regardless of result size and what the failure mode is if the consumer stalls.
- [ ] *(self-assessed)* The observability output is documented in `docs/observability.md` with the schema of the structured log lines and the metric names the engine emits.

---

## Architecture Notes

A reasonable module layout, building on the Module 2 and Module 3 crates:

```text
artemis-mission-replay/
├── src/
│   ├── lib.rs              # ReplayEngine, ReplaySession
│   ├── resolve.rs          # timestamp -> snapshot resolution
│   ├── projection.rs       # snapshot-time vs current schema projection
│   ├── stream.rs           # streaming RecordBatch reader
│   ├── diff.rs             # snapshot-diff CDF
│   └── bin/artemis_replay.rs
├── tests/
│   ├── resolve.rs
│   ├── projection.rs
│   ├── schema_evolution.rs
│   ├── partition_evolution.rs
│   ├── streaming_memory.rs
│   └── full_mission_bench.rs  # ignored by default; the perf bench
└── docs/
    ├── expired-snapshot.md
    ├── projection-modes.md
    ├── streaming-memory.md
    └── observability.md
```

The engine is fundamentally a thin layer over the Module 2/3 read path. The new work is:

- Resolving timestamps to snapshots via the snapshot history.
- Applying the right schema to projections.
- Wrapping the result as a `Stream<Item = Result<RecordBatch>>` (an `async_stream::stream!` macro is the cleanest pattern).
- Computing snapshot diffs via the manifest-entry-status walk.

The CLI is a small `clap`-driven binary; the heavy lifting is in the library.

---

## Hints

<details>

<summary>Hint 1 — Streaming with `async_stream`</summary>

The `async_stream` crate's `stream!` macro lets you yield batches lazily from an async function. The structure:

```rust,ignore
use async_stream::stream;

pub fn replay_at(
    &self,
    target_ms: i64,
    predicate: Predicate,
) -> impl Stream<Item = Result<RecordBatch>> {
    stream! {
        let pinned = self.resolve_timestamp(target_ms).await?;
        let plan = self.plan_against(&pinned, &predicate).await?;
        for file_scan in plan.files {
            for batch in self.read_file(&file_scan).await? {
                yield Ok(batch?);
            }
        }
    }
}
```

The consumer pulls batches as it processes them; the engine reads files as needed. Memory stays bounded by the current batch size.

</details>

<details>

<summary>Hint 2 — Field-ID-based projection in practice</summary>

The Parquet reader's projection mask supports projecting by Parquet column index, but the column-ID mapping you need lives in the Parquet schema's field metadata (the `field_id` annotation Iceberg adds). The `parquet` crate exposes this via `SchemaDescriptor::column_with_path` and the column's metadata. The pattern: walk the target schema, for each field look up the column in the Parquet file whose field_id matches, build a `ProjectionMask` of those indices, and the reader will return the columns in the target schema's order.

</details>

<details>

<summary>Hint 3 — Schema-time projection's null-fill</summary>

Columns in the target schema with no matching field_id in the data file need to be null-filled. The cleanest way: after the Parquet reader produces a batch with only the present columns, post-process the batch to add null columns for the missing target fields. `arrow::array::new_null_array(data_type, batch.num_rows())` produces a null array of the right type and length. Insert these into the batch at the correct schema positions.

</details>

<details>

<summary>Hint 4 — The full-mission benchmark</summary>

The 10-minute SLA for a full-mission replay (~2 TB) requires the engine to parallelize file reads. A `tokio::stream::FuturesUnordered` with a concurrency cap of ~64 (configurable) issues that many concurrent S3 GETs at a time. Each GET is a file read; the file's batches are decoded and yielded into the result stream. The order of batches in the output may not match the order in the metadata — the consumer must not assume ordering. The Mission Replay Engine documents this; the analyst tooling that consumes it does its own ordering when needed (typically by `sample_timestamp_ns`).

</details>

<details>

<summary>Hint 5 — CDF and the schema-change subtlety</summary>

A snapshot diff that spans a schema change has an interesting edge case: the Added files use the new schema; the Removed files use the old schema. The consumer of the CDF may see "added rows under schema B, removed rows under schema A." The engine's CDF API documents this and projects both streams against a user-specified target schema (snapshot-time-of-old, snapshot-time-of-new, or current), with the same field-ID-matching machinery the time-travel read uses. The tests should exercise the case where a column was added between the two diff endpoints.

</details>

---

## References

- *Designing Data-Intensive Applications* (Kleppmann & Riccomini), Chapter 5 — "Encoding and Evolution"; Chapter 7 — "Snapshot Isolation"
- Apache Iceberg specification — "Scan Planning" and "Schema Evolution"
- `async_stream` crate documentation
- `parquet` crate's field-ID/Iceberg-compatibility documentation

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI, the four self-assessed docs are written, and the full-mission replay benchmark hits the 10-minute SLA. The Module 5 capstone will plug a query engine on top of the read path; your `ReplayEngine` is the substrate that engine consumes for time-travel queries.
