# Lesson 3 — Atomic Commits via Optimistic Concurrency

**Module:** Data Lakes — M02: Open Table Formats
**Position:** Lesson 3 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 7 ("Transactions — Pessimistic versus optimistic concurrency control", "Serializable Snapshot Isolation"). Apache Iceberg specification, "Commit Process". Database Internals — Alex Petrov, Chapter 5 ("Concurrency Control").

> **Source note:** The transaction-theory framing is grounded in DDIA and *Database Internals*. The commit-protocol details follow the Iceberg spec.

---

## Context

Lesson 2 ended with a writer that produced new metadata files for a commit and was about to ask the catalog for an atomic pointer swap. This lesson is about the pointer swap. The pointer swap is the single piece of the table format that needs transactional semantics; everything else is decoupled work that can be retried, abandoned, or run concurrently without affecting readers. Getting the pointer swap right is what makes the entire metadata hierarchy work; getting it wrong is how lakehouses lose data.

The right primitive is **optimistic concurrency control**. The writer reads the current pointer value, does its work assuming nothing else will change the pointer in the meantime, then attempts to swap the pointer from the value it read to its new value. If the swap succeeds, the commit is durable. If the swap fails — because another writer beat it to the punch — the writer aborts and retries from the start: read the new pointer, rebuild the commit on top of it, try again. DDIA (Ch. 7) introduces this as the optimistic alternative to pessimistic locking: "instead of blocking if something potentially dangerous happens, transactions continue anyway, in the hope that everything will turn out all right. When a transaction wants to commit, the database checks whether anything bad happened; if so, the transaction is aborted and has to be retried."

The lesson develops the commit protocol end to end. The compare-and-swap primitive the catalog must provide. The retry loop that handles conflicts. The narrower class of conflicts that retries cannot fix safely (and what to do about them). The throughput characteristics of optimistic concurrency under contention. By the end of this lesson the engineer has a full mental model of the commit path and can implement it; the capstone project does.

---

## Core Concepts

### The Compare-and-Swap Primitive

The catalog must provide, atomically, the operation:

```
compare_and_swap(table, expected_old_snapshot_id, new_snapshot_id):
    if catalog[table].current == expected_old_snapshot_id:
        catalog[table].current = new_snapshot_id
        return Ok
    else:
        return Err(CommitConflict { actual: catalog[table].current })
```

The "atomically" is doing all the work. DDIA (Ch. 7, "Single-object writes") calls this the conditional write or compare-and-set operation, and notes that it is "similar to a compare-and-set or compare-and-swap (CAS) operation in shared-memory concurrency." For our purposes, the property the writer relies on is that no two writers can both observe `expected_old` as the current value and both have their writes succeed. Exactly one writer's CAS succeeds; the other writer's CAS fails with the actual current value, which the failed writer can use to retry.

In practice the CAS is implemented in whatever transactional primitive the catalog exposes. The Artemis archive's Postgres-backed catalog uses a single SQL `UPDATE ... WHERE current_snapshot_id = $expected_old`, which is atomic against Postgres's row locks. Hive Metastore uses its own transaction protocol. Nessie uses Git-like reference updates. DynamoDB uses a conditional `UpdateItem`. The mechanism differs; the contract is the same: linearizable CAS on a single small piece of state.

The CAS is also where Iceberg's design and Delta's design converge in spirit and diverge in mechanism. Iceberg names the operation directly: catalog CAS, with the catalog providing the primitive. Delta's transaction log puts the equivalent operation at the storage layer: "atomically create a new log file with sequence number N+1," which only works on filesystems that provide atomic create-if-not-exists. On S3 (which does not, except recently with conditional writes), Delta layers a coordination service (DynamoDB) on top to provide the missing primitive. The same primitive in a different shape.

### The Commit Loop

The full commit protocol, written as the writer sees it, is a loop:

```text
loop:
    1. Read the current snapshot from the catalog. This is the base.
    2. Build the new snapshot on top of the base (Lesson 2).
    3. Write the new metadata files (snapshot, manifest list, manifest).
    4. CAS the catalog from base.id to new.id.
       Success → commit is durable; return.
       Conflict → another writer committed in the meantime.
       Retry: go to step 1.
```

The retry path is what makes optimistic concurrency robust. When the CAS fails, the writer learns the actual current snapshot ID. The writer then re-reads the new current snapshot, re-bases the commit on it, writes new metadata files, and retries the CAS. The retry is *not* a re-run of the same commit — it is a fresh commit built on top of whatever the table is now. The new metadata files written in step 3 of the failed attempt are orphaned (left in the object store but referenced by nothing); the next maintenance run (Module 6's orphan-file cleanup) will delete them. The data files themselves are *not* orphaned if the same set of new rows is being added; the rebased commit references them again.

Two subtleties the production loop must handle. First, the data files written before the loop began should not be rewritten on each retry — they are valid Parquet, content-addressed by path, and remain valid regardless of how many CAS attempts the metadata commit takes. Only the metadata files need to be regenerated per attempt. Second, the retry must bound: an unbounded loop under high contention is a livelock waiting to happen. The Artemis writer caps retries at sixteen attempts with exponential backoff plus jitter; commits that exhaust the retry budget fail loudly to the caller, who decides whether to wait longer or escalate.

### Conflict Detection: Append vs Overwrite

Not all commit conflicts are equal. The CAS-based detection treats every concurrent commit as a conflict, but in the optimistic-concurrency literature there is a richer notion: did the two concurrent commits actually conflict on the *data*, or did they happen to commit at the same time without touching the same rows?

The richest version of the question is the same one DDIA (Ch. 7) develops for serializable snapshot isolation: did one transaction's read overlap another transaction's write? In the lakehouse case the question specializes. **Append-only commits don't conflict with each other on data** — two writers each adding new data files to disjoint partitions can in principle both succeed, because neither one invalidates the other's view of the table. **Overwrite commits conflict with everything** — a commit that rewrites file F invalidates any concurrent commit that was based on a snapshot containing F, because the rebased commit would need to be aware that F is gone.

Iceberg's commit protocol detects this distinction at retry time. When an append-only commit's CAS fails, the writer re-reads the new current snapshot, observes that the conflicting commit was append-only and added disjoint files, and constructs the rebased commit as "the previous new files plus the conflicting commit's new files." No data files need to be re-written. The retry is cheap and almost always succeeds.

When an overwrite commit's CAS fails — or when an append-only commit conflicts with an intervening overwrite — the rebase has to do real work. The writer must re-read the table's current file set, determine which files its overwrite would now affect (potentially a different set than originally planned), and produce a new commit. In the worst case, the overwrite is no longer applicable and the writer must reject the commit and surface the conflict to the application layer. This is the same shape as DDIA's "decisions based on an outdated premise" pattern: the writer's intent was framed in terms of a table state that no longer exists, and re-framing the intent in terms of the current state requires application-level judgment.

The Artemis ingestion writer is almost entirely append-only (new downlinks add new files; old files are not modified). Conflict resolution is straightforward and cheap. The correction-job writer that runs occasionally to fix bad ingests is overwrite-based; its conflict path requires re-evaluating which files to rewrite against the current snapshot. The two writers compose: an in-flight correction blocks ingest only briefly during the CAS, never during data file writing.

### Throughput Under Contention

Optimistic concurrency's well-known weakness, as DDIA (Ch. 7) puts it, is that it "performs badly if there is high contention." When many writers attempt to commit concurrently against the same table, most CAS attempts fail and most writers retry, producing the classic optimistic livelock pattern: more retries → more contention → more retries.

For a typical lakehouse the contention rate is low — most tables have a single writer (the ingestion pipeline) and many concurrent readers. The Artemis cold archive has exactly one writer per table during steady-state operation; commit conflicts arise only during maintenance windows when multiple jobs run concurrently. Throughput is dictated by the catalog's CAS latency (a few milliseconds for Postgres on the same host as the writer) and the metadata-write cost (tens to hundreds of milliseconds depending on manifest list size). Commit rates of one per second are easy; tens per second are achievable; hundreds per second require care.

When contention does become a real bottleneck, the typical interventions are: (1) batch many ingest events into one commit (the Artemis writer accumulates downlinks for thirty seconds before committing, capping commit rate at ~2 per minute even under heavy downlink); (2) shard the table by some partition key so concurrent writers commit against different table identities (the Artemis archive has one Iceberg table per mission rather than one global table); (3) consolidate writers into a single committer service that orders incoming commits and applies them sequentially (the Delta Lake reference deployment uses this pattern). All three approaches reduce the per-table commit rate to something CAS-friendly. None of them works around the fundamental property that linearizable CAS bounds throughput; they shift the work onto a different scope.

### Why Not Pessimistic Locking?

A natural alternative to optimistic CAS is pessimistic locking: the writer takes a table-level lock, holds it during the metadata write, releases it after the catalog update. This trades the retry cost of optimistic concurrency for the contention cost of locking — but the cost it really trades is the cost of unbounded lock holds when writers fail or stall.

Lakehouse writes are slow in absolute terms (writing twelve Parquet files of 128 MB each takes seconds to tens of seconds). A pessimistic lock held for the duration of a commit is a lock held for thirty seconds at a time. A crashed writer holding such a lock requires a separate lock-revocation mechanism with its own correctness concerns: lock leases, fencing tokens, the whole machinery DDIA (Ch. 8) develops for the distributed-lock problem. Optimistic concurrency avoids this entirely by making the only contended primitive a millisecond-scale CAS; concurrent writers do their slow work in parallel and only coordinate at the very end. The cost is that some writers' work is thrown away on conflict.

For workloads that are predominantly append-only against tables with single writers, optimistic concurrency is strictly the right choice. For workloads with high overwrite contention, a single-writer-per-table discipline (intervention 3 above) eliminates contention without paying for distributed locking. The lakehouse community has converged on this pattern; locking-based table formats are rare.

---

## Code Examples

### A Postgres-Backed Catalog with CAS

The Artemis archive's catalog is a single Postgres row per table. The CAS is a single `UPDATE`, atomic against Postgres's per-row lock.

```rust,ignore
use anyhow::{anyhow, Result};
use sqlx::PgPool;

#[derive(Debug, Clone, Copy)]
pub struct CommitConflict {
    pub expected_old: SnapshotId,
    pub actual_current: SnapshotId,
}

#[derive(Debug)]
pub struct PostgresCatalog {
    pool: PgPool,
}

impl PostgresCatalog {
    pub async fn get_current(&self, table: &str) -> Result<CatalogEntry> {
        let row = sqlx::query!(
            "SELECT current_snapshot_id, metadata_path
             FROM iceberg_tables
             WHERE table_name = $1",
            table,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(CatalogEntry {
            table_name: table.to_string(),
            current_snapshot_id: row.current_snapshot_id as u64,
            metadata_path: row.metadata_path,
        })
    }

    /// Atomically swap the table's current_snapshot_id from
    /// `expected_old` to `new`. Returns Ok(()) on success;
    /// Err(CommitConflict { .. }) if the row's current value is not
    /// `expected_old` at the time the UPDATE runs.
    pub async fn compare_and_swap(
        &self,
        table: &str,
        expected_old: SnapshotId,
        new: SnapshotId,
        new_metadata_path: &str,
    ) -> Result<(), CommitError> {
        // The UPDATE is atomic on a single row. Postgres's MVCC ensures
        // that concurrent UPDATEs against the same row are serialized;
        // the loser sees rows_affected = 0 because its WHERE predicate
        // matched a no-longer-current row.
        let result = sqlx::query!(
            "UPDATE iceberg_tables
             SET current_snapshot_id = $1, metadata_path = $2
             WHERE table_name = $3
               AND current_snapshot_id = $4",
            new as i64,
            new_metadata_path,
            table,
            expected_old as i64,
        )
        .execute(&self.pool)
        .await
        .map_err(CommitError::Database)?;

        if result.rows_affected() == 1 {
            return Ok(());
        }

        // CAS failed. Read the current value to give the caller a
        // useful error.
        let current = self.get_current(table).await
            .map_err(CommitError::Database)?;
        Err(CommitError::Conflict(CommitConflict {
            expected_old,
            actual_current: current.current_snapshot_id,
        }))
    }
}

#[derive(Debug)]
pub enum CommitError {
    Conflict(CommitConflict),
    Database(anyhow::Error),
}
```

What to notice. The CAS is one SQL statement. Postgres's per-row MVCC is what provides the atomicity; no application-level locking is needed. The cost of a CAS attempt is a single round-trip to the database — typically under 2 ms on the same network. The contention behavior is Postgres's normal row-contention behavior: concurrent writers against the same row queue briefly at the lock, see the `UPDATE` succeed for one of them and fail for the others. The `rows_affected = 0` is the conflict signal; the followup read produces the actual current value for the retry path.

### The Full Commit Retry Loop

The retry loop puts the pieces from the previous lessons together: build a commit, attempt the CAS, rebase on conflict, retry. The example uses a fixed retry budget with exponential backoff and jitter.

```rust,ignore
use anyhow::{Context, Result};
use rand::Rng;
use std::time::Duration;

const MAX_RETRIES: u32 = 16;
const INITIAL_BACKOFF_MS: u64 = 50;

/// Append `new_data_files` to `table` with optimistic-concurrency retries.
/// The data files have already been written to the object store; this
/// function manages only the metadata commit.
pub async fn commit_append(
    catalog: &PostgresCatalog,
    table: &str,
    metadata_dir: &str,
    new_data_files: &[DataFile],
) -> Result<SnapshotId> {
    for attempt in 0..MAX_RETRIES {
        // 1. Read the current state.
        let entry = catalog.get_current(table).await?;
        let base_snapshot: Snapshot = read_metadata_file(&entry.metadata_path).await?;
        let base_manifest_list: ManifestList =
            read_metadata_file(&base_snapshot.manifest_list_path).await?;

        // 2. Build the new snapshot on top of the current state.
        let (new_snapshot, new_snapshot_path) = build_append_snapshot(
            &base_snapshot,
            &base_manifest_list,
            new_data_files.to_vec(),
            metadata_dir,
        )
        .await
        .context("building append snapshot")?;

        // 3. Attempt the CAS.
        match catalog
            .compare_and_swap(
                table,
                base_snapshot.snapshot_id,
                new_snapshot.snapshot_id,
                &new_snapshot_path,
            )
            .await
        {
            Ok(()) => {
                // Commit durable. The metadata files we wrote in step 2
                // are now referenced by the catalog; no cleanup needed.
                return Ok(new_snapshot.snapshot_id);
            }
            Err(CommitError::Conflict(c)) => {
                // Another writer committed between our read of the base
                // and our CAS attempt. Log the conflict, back off, retry.
                tracing::warn!(
                    table = %table,
                    expected = c.expected_old,
                    actual = c.actual_current,
                    attempt,
                    "commit conflict; retrying"
                );

                // The metadata files we wrote in step 2 are orphans now.
                // Orphan-file cleanup (Module 6) handles them; we don't
                // need to delete them here. The data files are *not*
                // orphans — they'll be referenced by our next attempt's
                // commit.

                // Exponential backoff with jitter to avoid lockstep
                // retries when many writers are contending.
                let base_ms = INITIAL_BACKOFF_MS * 2u64.pow(attempt.min(8));
                let jitter_ms = rand::thread_rng().gen_range(0..=base_ms);
                tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
            }
            Err(CommitError::Database(e)) => {
                // Real database error, not a conflict. Surface it.
                return Err(e.context("catalog CAS"));
            }
        }
    }

    Err(anyhow::anyhow!(
        "commit failed after {} retries on table {}",
        MAX_RETRIES,
        table,
    ))
}
```

The discipline. **Data files are written once, before the retry loop, outside this function.** The retry loop's per-iteration work is just the metadata: read base, build new metadata, attempt CAS. A retry costs the metadata files (some tens of KB written), the catalog read (one DB round-trip), the catalog write (one DB round-trip), and the backoff sleep. A typical retry under low contention completes in under 200 ms. Bounded retry plus exponential-backoff-with-jitter keeps the system stable even under bursts of contention.

The structured-log line on conflict (`tracing::warn!`) is critical for operational visibility. Sustained conflict rates above a threshold are an indicator that two writers are racing against a table that should be partitioned; the Artemis observability stack alerts on `commit_conflict_total` rate.

### A Test That Exercises the CAS Under Concurrent Writers

The integration test that validates the CAS does what it claims: two concurrent writers each attempt a commit; exactly one succeeds, the other observes the conflict and either retries or fails per its policy.

```rust,ignore
use anyhow::Result;
use tokio::sync::Barrier;
use std::sync::Arc;

/// Spawn two concurrent committers against the same table. Assert that
/// exactly one's first attempt succeeds, the other's first attempt sees
/// a CommitError::Conflict, and the second writer's retry then succeeds
/// against the new base. The final table state has both commits applied
/// in sequence.
#[tokio::test]
async fn cas_serializes_concurrent_writers() -> Result<()> {
    let catalog = test_catalog().await?;
    let metadata_dir = test_metadata_dir();
    let table = "telemetry_test";
    setup_initial_table(&catalog, table, &metadata_dir).await?;

    // A barrier ensures both writers reach the CAS at the same time;
    // without it the test is racy in the wrong direction (timing-based
    // rather than CAS-based).
    let barrier = Arc::new(Barrier::new(2));

    let cat1 = catalog.clone();
    let cat2 = catalog.clone();
    let dir1 = metadata_dir.clone();
    let dir2 = metadata_dir.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    let writer_a = tokio::spawn(async move {
        let files = make_test_files("A", 3).await?;
        // Read the base before the barrier so both writers have the same base.
        let entry = cat1.get_current("telemetry_test").await?;
        let base = read_snapshot(&entry.metadata_path).await?;
        let base_ml = read_manifest_list(&base.manifest_list_path).await?;
        b1.wait().await; // line up at the CAS
        let (new, path) = build_append_snapshot(&base, &base_ml, files, &dir1).await?;
        cat1.compare_and_swap("telemetry_test", base.snapshot_id, new.snapshot_id, &path).await
    });

    let writer_b = tokio::spawn(async move {
        let files = make_test_files("B", 3).await?;
        let entry = cat2.get_current("telemetry_test").await?;
        let base = read_snapshot(&entry.metadata_path).await?;
        let base_ml = read_manifest_list(&base.manifest_list_path).await?;
        b2.wait().await;
        let (new, path) = build_append_snapshot(&base, &base_ml, files, &dir2).await?;
        cat2.compare_and_swap("telemetry_test", base.snapshot_id, new.snapshot_id, &path).await
    });

    let result_a = writer_a.await?;
    let result_b = writer_b.await?;

    // Exactly one of the two should have succeeded; the other should
    // have observed a CommitConflict.
    let successes = [&result_a, &result_b].iter().filter(|r| r.is_ok()).count();
    let conflicts = [&result_a, &result_b]
        .iter()
        .filter(|r| matches!(r, Err(CommitError::Conflict(_))))
        .count();

    assert_eq!(successes, 1, "exactly one writer should succeed");
    assert_eq!(conflicts, 1, "exactly one writer should see a conflict");

    Ok(())
}
```

The test asserts the property that matters: exactly one writer's CAS succeeds, exactly one observes the conflict. The retry-then-succeed behavior is tested separately in a longer-running test that exercises the full `commit_append` retry loop under a steady stream of concurrent commits. Both tests are in the capstone's integration suite.

---

## Key Takeaways

- The table format's only transactionally-significant operation is the **catalog CAS** that swaps the current snapshot pointer. Everything else (data files, manifests, snapshot metadata) is decoupled work that can be retried or abandoned without affecting readers.
- **Optimistic concurrency control** is the right primitive for lakehouse commits: writers do their slow work in parallel, coordinate only at the very end, and retry on conflict. The alternative — pessimistic locking — would hold locks for the duration of multi-file writes, which is incompatible with object-store write latencies and the failure modes of crashed lock holders.
- **Conflicts are detected by CAS but resolved by rebase.** A conflicting commit reads the new current snapshot, rebuilds its metadata on top of the new state, and retries. For append-only commits this is cheap and almost always succeeds. For overwrite commits the rebase may require re-evaluating which files to rewrite.
- **Bounded retries with exponential backoff and jitter** keep the system stable under bursts of contention. Sustained conflict rates indicate that two writers are racing against a table that should be split into smaller tables or fed through a single-writer coordinator.
- **Throughput under contention is bounded by catalog CAS rate**, not by data write rate. For workloads that need hundreds of commits per second per table, the typical fix is batching, table-level sharding, or a single-writer committer — not faster locking.

{{#quiz quiz-03.toml}}
