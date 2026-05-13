# Lesson 1 — Snapshot Isolation on Object Storage

**Module:** Data Lakes — M04: Time Travel and Schema Evolution
**Position:** Lesson 1 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 7 ("Transactions — Snapshot Isolation and Repeatable Read", "Indexes and Snapshot Isolation"). Apache Iceberg specification, "Scan Planning" and "Snapshot Retention" sections.

---

## Context

Module 2 introduced snapshots as the unit of immutable table-version metadata. Module 3 introduced read planning against the current snapshot. This lesson develops the **isolation contract** that snapshots provide to readers — what guarantees a query holds about the state of the table during its execution, and what the writer side has to do to preserve those guarantees.

The right frame is the one DDIA (Ch. 7) develops for traditional databases: snapshot isolation is the property that "each transaction reads from a consistent snapshot of the database — that is, the transaction sees all the data that was committed in the database at the start of the transaction. Even if the data is subsequently changed by another transaction, each transaction sees only the old data from that particular point in time." In the lakehouse the same property holds, structurally, because the storage substrate is immutable: every snapshot file remains on disk after subsequent commits, so a reader that pins a snapshot ID at query start continues to read against that snapshot for the query's full duration, regardless of how many other commits arrive in the meantime.

This lesson makes the structural property explicit: the read-side protocol that pins a snapshot, the implications for long-running queries, the interaction with snapshot expiration that determines how long a snapshot remains readable, and the specific guarantees and non-guarantees the model provides. The capstone's mission-replay engine depends on this protocol — replay queries are by definition long-running reads against past snapshots, and the snapshot-expiration retention window directly bounds how far back replay can reach.

---

## Core Concepts

### Snapshot Isolation as a Free Consequence of Immutability

DDIA (Ch. 7) describes snapshot isolation as a database engine's deliberate machinery — multiple object versions, garbage collection of old versions, careful read-time visibility rules. The lakehouse gets the same property essentially for free. Each commit produces *new* metadata files; old metadata files are *not modified*; the catalog pointer changes atomically. A reader that records the current snapshot ID at query start has a stable handle: the metadata files referenced by that snapshot ID remain on disk, unmodified, fully readable, regardless of how many commits arrive after.

The corollary that matters operationally: **a long-running query does not block writers, and writers do not block readers**. The Artemis ingestion pipeline can commit a new snapshot every thirty seconds while an analyst query that takes ten minutes runs against the snapshot from before the query started. The analyst sees consistent data; the writers make progress; no coordination is needed beyond the catalog CAS that the writers use among themselves.

DDIA (Ch. 7) calls out the same property for traditional MVCC: "Snapshot isolation is a boon for long-running, read-only queries such as backups and analytics. It is very hard to reason about the meaning of a query if the data on which it operates is changing at the same time as the query is executing." The lakehouse case is identical except cheaper — the immutability is structural, not stored as a per-row version chain that the engine garbage-collects, so the per-row overhead is zero.

### The Pin Protocol

A query against a lakehouse table pins a snapshot by capturing the snapshot ID at planning time. The protocol:

1. The planner reads the catalog's current snapshot for the table. Call this `S`.
2. The planner reads the snapshot metadata file for `S` from the object store.
3. The planner reads `S`'s manifest list, then its referenced manifests, then plans the data file reads (Module 3's three-pass pruning).
4. The reader executes the plan against the data files. Every file read uses paths from `S`'s manifest entries.

After step 1, the query never re-reads the catalog. The catalog can advance to `S+1`, `S+2`, … while the query runs; the query continues to use `S` as its snapshot. Other queries against the same table started after the catalog advances will read the new snapshot; long-running queries hold their original pin.

A subtlety: the pin must be **explicit** in the query's state. The Artemis read planner returns a `ScanPlan` that includes the pinned `snapshot_id`; the reader includes it in its log lines and observability. If a query takes longer than expected, the operations team can correlate the query's snapshot ID with the catalog history and determine exactly which version of the table the query is reading. This is the lakehouse equivalent of a database's "transaction start timestamp" — same diagnostic shape, same operational value.

### Long-Running Queries and the Expiration Window

The pin protocol holds the snapshot's metadata files (snapshot, manifest list, manifests) and data files in their referenced state for the query's duration. The hidden requirement: those files must continue to exist on disk while the query runs. Snapshot expiration (Module 6) is the maintenance job that physically deletes snapshot files older than a retention window; a query that pins a snapshot that is *expired* during its execution sees its in-flight reads fail with "object not found."

The Artemis archive sets the retention window to 30 days. Any query that completes in under 30 days against any snapshot from the last 30 days is safe. Replay queries against snapshots older than 30 days are explicitly unsupported by the live read path; the data is available in the cold-archive backup (immutable snapshots replicated to an object-locked bucket) and accessed through a separate read path with longer SLAs.

The interaction worth understanding. **The retention window is the lakehouse's analog of the database's MVCC garbage-collection horizon**. DDIA (Ch. 7, "Indexes and snapshot isolation") makes the same point for traditional systems: long-running queries hold snapshots; the system cannot reclaim space until the queries finish; the operations team has to set a horizon past which it gives up on long-running queries to avoid running out of space. The Artemis archive's 30-day window is the equivalent setting; query timeouts are configured well within it (the default planner sets a 6-hour query timeout) to avoid the operational case where a forgotten query holds an old snapshot indefinitely.

### What Snapshot Isolation Guarantees, and What It Does Not

The guarantee is precise: every row read by a query reflects the table's state as of the pinned snapshot's commit time. No row reflects a later commit; no row is missing because of a later commit. The query produces the result it would have produced if no writer had run during the query.

The non-guarantees, also precise:

- **Snapshot isolation is not serializable isolation.** Two read-only queries that pin different snapshots can produce results that no serial execution of all transactions could produce; the lakehouse does not order queries among themselves except by their pin. For typical analytical queries this is irrelevant — the queries are independent — but it matters for any workflow that runs two queries and relies on them seeing the same state. The fix is to share a snapshot ID across the related queries; the Artemis tooling supports a `--pin-snapshot` flag for this case.
- **Snapshot isolation does not prevent write skew.** DDIA (Ch. 7, "Write Skew and Phantoms") describes the anomaly: two writers each read a state, each decide to modify a different row, each commit, neither observes the other's changes. The lakehouse's optimistic concurrency control (Module 2) detects writer-writer conflicts at the CAS but does not detect read-modify-write conflicts where the read and the write target different rows. For append-only workloads this is irrelevant; for overwrite workloads the application layer must structure its commits to avoid the pattern.
- **Snapshot isolation does not bound staleness across regions.** If the object store and the catalog are geographically distributed, a query in region A may see a snapshot that lags behind region B's writers by the inter-region replication lag. The Artemis archive runs a single catalog and a single primary object store region, with backup replication to a second region — this avoids the cross-region staleness problem at the cost of higher write latency for that one region. Multi-region lakehouses generally accept staleness in trade for write availability.

The guarantee that *is* given is the one that matters for analytical workloads: a query operates on a consistent, point-in-time view of the table. This is what makes the lakehouse reliable for the long-running, read-only analyses that dominate its workload.

### Implications for Operational Pipelines

The pin protocol has several operational implications worth knowing.

**Reader retries are cheap.** A query that fails partway through (network error, transient object-store fault) can be retried by replanning against the same pinned snapshot. The retry produces identical results because the snapshot is unchanged. The Artemis reader does this automatically — any query failure with a recognized-transient error code triggers a retry against the original pinned snapshot, up to a configurable retry budget.

**Tail-latency analysis is meaningful.** A query that takes 10 minutes against a stable snapshot is a query against a known input. The same query at the same configuration produces the same input bytes the next time it runs (the snapshot is immutable). This makes performance regressions observable: a query that took 10 minutes last week and takes 20 minutes this week, against the same snapshot, has a real regression in either the planner or the storage layer — not a workload shift, because the workload (the snapshot) is unchanged.

**The snapshot ID is the right caching key.** Computed results that depend on a snapshot's data can be cached by snapshot ID; cache invalidation is trivial because a new snapshot has a new ID and a new cache entry. The Artemis dashboard caches frequently-computed aggregations this way; the cache hit rate at steady state is over 99% because snapshots typically change every few minutes while dashboard queries refresh every few seconds.

---

## Core Mechanics in Code

### Pinning a Snapshot for a Query

The minimal read-side protocol: capture the snapshot once at the start of the query, use it for all subsequent reads.

```rust,ignore
use anyhow::Result;

pub struct PinnedQuery {
    pub table: String,
    pub snapshot_id: SnapshotId,
    pub snapshot_metadata_path: String,
}

/// Pin the current snapshot of the table. The returned PinnedQuery
/// captures the snapshot ID and metadata path; subsequent reads use
/// these directly without consulting the catalog again.
pub async fn pin_current(catalog: &PostgresCatalog, table: &str) -> Result<PinnedQuery> {
    let entry = catalog.get_current(table).await?;
    Ok(PinnedQuery {
        table: table.to_string(),
        snapshot_id: entry.current_snapshot_id,
        snapshot_metadata_path: entry.metadata_path,
    })
}

/// Read the table at a previously-pinned snapshot. This function does
/// not consult the catalog; it goes straight to the snapshot's metadata
/// path. If the snapshot has been expired (Module 6) and its metadata
/// removed, the metadata read fails with NotFound and the caller must
/// handle the case.
pub async fn read_pinned(
    query: &PinnedQuery,
    predicate: &Predicate,
) -> Result<ScanPlan> {
    let snapshot: Snapshot = read_metadata_file(&query.snapshot_metadata_path).await?;
    plan_scan_against_snapshot(&snapshot, predicate).await
}
```

The discipline. The catalog is consulted once, at pin time. After that, the query is independent of any subsequent commits. The query's results are determined entirely by the snapshot ID it captured; two queries with the same pin produce identical results.

### Pinning a Past Snapshot

Time travel (Lesson 2 develops this in depth) is just pinning a snapshot other than the current one. The mechanics are the same.

```rust,ignore
use anyhow::Result;

/// Pin a specific past snapshot by ID. The snapshot history is recorded
/// in the table's snapshot-history metadata; this function consults the
/// history to find the metadata path for the requested snapshot ID.
pub async fn pin_by_id(
    catalog: &PostgresCatalog,
    table: &str,
    snapshot_id: SnapshotId,
) -> Result<PinnedQuery> {
    let history = read_snapshot_history(catalog, table).await?;
    let entry = history.find(|s| s.snapshot_id == snapshot_id)
        .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} not in history"))?;
    Ok(PinnedQuery {
        table: table.to_string(),
        snapshot_id,
        snapshot_metadata_path: entry.metadata_path.clone(),
    })
}

/// Pin the snapshot that was current at the given UTC timestamp. The
/// implementation walks the snapshot history backward from the current
/// snapshot until it finds one whose commit timestamp is <= the target.
pub async fn pin_at_time(
    catalog: &PostgresCatalog,
    table: &str,
    timestamp_ms: i64,
) -> Result<PinnedQuery> {
    let history = read_snapshot_history(catalog, table).await?;

    // Walk history backward; the first snapshot with timestamp <= target
    // is the one that was current at the target time.
    let entry = history.iter()
        .rev()
        .find(|s| s.timestamp_ms <= timestamp_ms)
        .ok_or_else(|| anyhow::anyhow!("no snapshot before {timestamp_ms}"))?;

    Ok(PinnedQuery {
        table: table.to_string(),
        snapshot_id: entry.snapshot_id,
        snapshot_metadata_path: entry.metadata_path.clone(),
    })
}
```

What to notice. Time travel does not require any new mechanism — it is the same pin protocol applied to a different snapshot ID. The retrieval mechanism (consult the snapshot-history metadata) is the only new piece; the read path after the pin is unchanged. The snapshot-history metadata is itself the Iceberg `metadata_log` field, a small append-only list of `(snapshot_id, commit_timestamp_ms, metadata_path)` triples. Maintaining it is a per-commit responsibility of the writer; reading it is the only catalog dependency for time-travel queries.

---

## Key Takeaways

- **Snapshot isolation in a lakehouse is a structural consequence of the immutable-snapshot model**, not a deliberate engine mechanism. Each commit produces new metadata; old metadata is never modified; readers pin a snapshot and use it for the query's duration regardless of subsequent commits.
- **The pin protocol is one catalog read at query start**, captured in the query's state, and used for every subsequent read. The catalog can advance freely during the query; the pin holds the query's view stable.
- **Snapshot retention bounds the time-travel reach.** The Module 6 snapshot-expiration job deletes old snapshot files after a configurable retention window; queries against expired snapshots fail. The Artemis archive uses a 30-day window; query timeouts are configured well inside it.
- **Snapshot isolation guarantees per-query consistency, not cross-query serializability.** Two related queries can pin different snapshots and see different states; the fix is to share a pin across them. Write skew is not prevented; the optimistic-CAS layer (Module 2) detects writer-writer conflicts but not read-modify-write conflicts on disjoint rows.
- **The snapshot ID is the right cache key, the right log correlation ID, and the right unit for performance analysis.** Snapshots are immutable inputs; two runs against the same snapshot are two runs against the same input. Performance regressions and result divergences are diagnosable in terms of snapshot IDs.

{{#quiz quiz-01.toml}}
