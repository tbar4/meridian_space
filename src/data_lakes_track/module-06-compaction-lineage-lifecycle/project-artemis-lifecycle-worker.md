# Capstone — Artemis Archive Lifecycle Worker

**Module:** Data Lakes — M06: Compaction, Lineage, and Lifecycle
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%). The Module 2-5 capstones are the substrate.

---

## Mission Briefing

### From: Cold Archive Operations Lead

```
ARCHIVE BRIEFING — RC-2026-04-DL-006
SUBJECT: Archive Lifecycle Worker — background maintenance service
         for the Artemis cold archive.
PRIORITY: P1 — required to bring the archive into long-term
         operational steady state.
```

The cold archive's read and write paths work. The analyst portal is in production. What we don't have is the maintenance discipline that keeps the archive working over years. We've been deferring the small-file problem for six months; query latencies are creeping up. We've never run snapshot expiration, so the metadata footprint keeps growing. Orphan files from failed commits accumulate at maybe 0.5% per week — we estimate 200-300 GB of orphans in the archive already. The audit team has started asking for lineage queries we can't currently answer because the lineage discipline isn't enforced uniformly.

The job: build the Archive Lifecycle Worker. It's a Rust service that runs in the cold-archive infrastructure, performs the four maintenance jobs (compaction, snapshot expiration, orphan cleanup, lineage compaction) on a continuous schedule, reports metrics to the observability stack, and respects the live workload's bandwidth budget so analyst queries are not affected.

The worker doesn't need to be flashy. It needs to be correct, observable, and safe. Production workloads will run against this for years; getting the maintenance discipline right now is what makes the archive durable over the long term.

---

## What You're Building

A Rust crate, `artemis-lifecycle-worker`, exposing a long-running binary that runs the four maintenance jobs continuously. Components:

- A **scheduler** that runs each job on its configured schedule (daily, weekly, monthly, or continuous) with respect to bandwidth budgets and quiet-window policies.
- A **compaction engine** implementing bin-packing, sort-based, and Z-order rewrite strategies (Lesson 1), choosing per-partition based on the table's clustering spec.
- A **snapshot expiration engine** implementing the two-phase deletion (Lesson 2): reachability calculation, expiration plan, storage-tier handoff to S3 Glacier IR, Phase-2 physical delete.
- An **orphan cleanup engine** implementing the age- and active-writer-guarded reconciliation scan (Lesson 3).
- A **lineage validation hook** that the Module 2 commit path calls before the CAS to reject commits missing required lineage fields.
- A **manifest-compaction job** (Lesson 1) that runs weekly to consolidate per-commit manifests.
- A **schema-history-compaction job** (Lesson 3) that runs monthly.
- A **structured-logging integration** that emits metrics for every job (files compacted, snapshots expired, orphans cleaned, lineage validations) to the observability stack via the `tracing` crate.
- A small **operator CLI**, `artemis-lifecycle-cli`, for manual job invocation (`artemis-lifecycle-cli compact --table sda_observations --partition 'mission_id=apollo-7,day=2024-03-15'`) and for inspecting the worker's state.

The worker must run for at least 30 days against the production archive without producing analyst-visible query-latency degradation, measured by the existing observability stack.

---

## Functional Requirements

1. **Compaction strategy selection.** For each partition selected for compaction, the engine consults the table's sort-order metadata: tables with no sort order use bin-packing; tables with a linear sort order use sort-based compaction; tables with a Z-order cluster spec use Z-order compaction.
2. **Compaction scope.** The engine compacts partitions whose small-file count exceeds a threshold (default 50 files below 32 MB). Partitions below the threshold are not touched.
3. **Compaction throughput limit.** The engine consumes at most 30% of the configured object-store bandwidth budget. The bandwidth limit is enforced at the read level (using a token bucket on the Parquet reader) and at the write level.
4. **Quiet-window scheduling.** Compaction is paused between 09:00 and 17:00 UTC daily; expiration runs only between 00:00 and 06:00 UTC.
5. **Two-phase deletion.** Expiration's Phase 1 (metadata commit) runs in a daily batch; Phase 2 (physical delete) runs 24+ hours later. The two phases are decoupled; a Phase 2 run uses the journal of files queued by Phase 1.
6. **Storage-tier handoff.** Before Phase 2 deletes, expired snapshots' data and metadata files are copied to the cold-tier object store. The cold tier's catalog is updated to reference them.
7. **Orphan detection with safety guards.** Files younger than 7 days OR in the active-writer set are not classified as orphans. The active-writer set is read from the catalog's writer heartbeat table.
8. **Lineage validation.** The Module 2 commit path calls the validation hook before its CAS. Commits without the required fields (operation, writer.id, writer.commit_hash, writer.invocation_id) are rejected with a clear error message.
9. **Operator manual triggers.** The CLI supports manual invocation of any job, scoped to a specific table or partition where applicable. Manual invocations bypass the quiet-window schedule but still respect bandwidth limits.
10. **Resumability.** Every job is resumable from its last consistent state. A worker crash mid-job is recoverable by re-running the same job, which picks up where the crash left off (compaction reads its plan from the catalog; orphan cleanup reads its journal; etc.).

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] A bin-packing compaction reduces a partition's data-file count from N small files (each 1-10 MB) to ceil(N × avg_size / 128 MB) compacted files, with one overwrite commit. The compacted partition has fewer total files; the data row count is preserved exactly.
- [x] A Z-order compaction against a partition with random `payload_id` and `sensor_kind` ordering produces output files whose per-column statistics for both columns are tight (max-min span less than 30% of the column's full range, averaged across files).
- [x] A snapshot expiration against a table with 1000 snapshots and a 30-day retention window classifies snapshots older than 30 days as expired (modulo tagged snapshots) and removes their metadata from the snapshot-history-log.
- [x] An orphan cleanup against an object store containing 100 files of mixed ages and reachability classifications correctly identifies and deletes only files that are (a) not reachable, (b) older than 7 days, and (c) not in the active-writer set.
- [x] An attempted commit with `snapshot.summary` missing the required lineage fields is rejected at pre-commit validation; the catalog pointer is not advanced; the writer receives a structured error.
- [x] A worker process killed mid-compaction with `SIGKILL` and then restarted resumes correctly: the next run reads the catalog to identify which compaction was in flight (no commit yet) and re-plans. No data is lost; no double-deletions occur.
- [x] A 24-hour soak test against a synthetic high-load environment (1 writer committing every 30s; 50 analyst queries/minute; lifecycle worker running all jobs) shows no analyst-query-latency degradation beyond the workload's own variation (measured as p95/p99 latency change < 10%).
- [x] The worker's metrics-exposed endpoints (Prometheus-format `/metrics`) include files_compacted_total, snapshots_expired_total, orphans_cleaned_total, lineage_validation_failures_total, with per-table labels.

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The bandwidth-budget tuning is documented in `docs/bandwidth-tuning.md`. The doc describes how the 30% budget was chosen against the live workload, the failure modes if it is set too low (compaction falls behind) or too high (analyst impact), and the observable signals that indicate the budget needs adjusting.
- [ ] *(self-assessed)* The retention-window choice is documented in `docs/retention-tuning.md` per table. The doc lists each table, its retention window, and the analytic argument that justifies the choice.
- [ ] *(self-assessed)* The schedule (compaction continuous, expiration 00:00-06:00 UTC, etc.) is documented in `docs/schedule.md` with the analyst-team time zone constraint that drives the choice and the alternative schedules that were considered.
- [ ] *(self-assessed)* The resumability properties are documented in `docs/resumability.md`. The doc enumerates each job's crash-recovery behavior and the catalog state used to resume each one.

---

## Architecture Notes

A reasonable module layout:

```text
artemis-lifecycle-worker/
├── src/
│   ├── lib.rs
│   ├── scheduler.rs           # Schedule { Daily, Weekly, Monthly, Continuous }
│   ├── bandwidth.rs           # Token-bucket rate limiting for IO
│   ├── compaction.rs          # bin-packing, sort-based, Z-order rewrite
│   ├── expiration.rs          # reachability calc, two-phase deletion
│   ├── tier_handoff.rs        # S3 Glacier IR copy + cold-tier catalog
│   ├── orphan_cleanup.rs      # detection scan + deletion with journal
│   ├── lineage.rs             # validation hook + manifest compaction
│   ├── metrics.rs             # Prometheus metrics
│   ├── bin/artemis_lifecycle_worker.rs
│   └── bin/artemis_lifecycle_cli.rs
├── tests/
│   ├── compaction.rs
│   ├── expiration_two_phase.rs
│   ├── orphan_detection.rs
│   ├── lineage_validation.rs
│   ├── resumability.rs
│   └── soak.rs               # ignored by default; the 24-hour soak
└── docs/
    ├── bandwidth-tuning.md
    ├── retention-tuning.md
    ├── schedule.md
    └── resumability.md
```

The token-bucket rate-limiter for bandwidth budgeting is a standard pattern; the `governor` crate or a hand-rolled `tokio::sync::Semaphore`-based variant both work. The metrics-exposing endpoint uses `prometheus` or `metrics-exporter-prometheus` integrated into a small `axum` HTTP server.

The active-writer set is read from a Postgres table that the M2 commit code maintains as a heartbeat (writer ID, host, started_at, last_heartbeat_at, in_progress_paths). The lifecycle worker reads this table on every orphan-cleanup scan. The Module 2 capstone's writer code may need a small extension to populate the heartbeat table; this is expected work.

---

## Hints

<details>

<summary>Hint 1 — Compaction's per-partition serialization</summary>

The capstone's compaction engine must avoid compacting the same partition from two workers simultaneously. The discipline: take a per-partition lock from the catalog (a `compaction_in_progress` table keyed by (table, partition_tuple), with a `started_at` timestamp). The lock has a TTL (e.g., 1 hour); a worker that crashes leaves the lock; the next worker observes the expired lock and reclaims it. This is the table-format-layer analog of the lease pattern; it's simpler than implementing it in code because Postgres already supports the primitive.

</details>

<details>

<summary>Hint 2 — The reachable-set cache</summary>

The reachable-set calculation (Lesson 2) is expensive for tables with many snapshots — it reads every snapshot's metadata. The capstone should cache the result with a TTL of a few hours; the cache is invalidated by any new commit (the lifecycle worker reads the catalog to detect this). Caching reduces the daily expiration job from hours to minutes.

</details>

<details>

<summary>Hint 3 — The Z-order rank-normalization</summary>

Z-order compaction requires the cluster columns to be normalized to the same u32 scale (Module 3 Lesson 2). The standard approach: compute the rank of each column's value distribution within the partition being compacted (use `arrow::compute::rank` or a per-column quantile sketch), and use the rank as the Z-order input. The ranks are stable within a single compaction run; different runs may produce different ranks for the same values (depending on data distribution shifts) — this is fine because clustering is per-file, not global.

</details>

<details>

<summary>Hint 4 — Resumability via the catalog</summary>

Resumability means every job's working state lives in the catalog (or another durable store), not in the worker's memory. Compaction writes its plan to the catalog before executing it; expiration writes its plan; orphan cleanup writes its journal. A restarted worker reads the persistent state and continues. The discipline is "no in-memory state lives across restarts"; if you find yourself wanting to remember something between job invocations, write it to a Postgres table.

</details>

<details>

<summary>Hint 5 — The soak-test environment</summary>

The 24-hour soak test (acceptance criterion) requires a synthetic environment that produces the right load shape. A reasonable setup: a writer that commits every 30s with mock data; 50 query workers that each issue a typical analyst query every minute against random table partitions; the lifecycle worker running all four jobs at production schedule. The soak test runs in CI on a dedicated test cluster; failure means analyst-latency regression beyond 10%. The Artemis platform has this test as part of the canary deployment pipeline for any lifecycle-worker change.

</details>

---

## References

- Apache Iceberg specification — "Snapshot Properties", "Maintenance", "Tagging"
- `governor` crate documentation — token-bucket rate limiting
- `prometheus` crate documentation — metrics export
- AWS S3 Glacier Instant Retrieval documentation — for the cold-tier handoff

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI, the four self-assessed docs are written, and the 24-hour soak test passes consistently. The Data Lakes track is complete at this point — the cold archive has a transactional table format, good partitioning and clustering, time travel, a SQL surface, and the operational discipline to keep all of it working over years. The next track (Distributed Systems) builds on this foundation for the constellation-scale workloads that span multiple ground stations and orbital assets.
