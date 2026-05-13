# Lesson 2 — Snapshot Expiration and Storage Tiering

**Module:** Data Lakes — M06: Compaction, Lineage, and Lifecycle
**Position:** Lesson 2 of 3
**Source:** Apache Iceberg specification, "Snapshot Expiration" section. *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 7 ("Snapshot Isolation — Indexes and Snapshot Isolation") for the analogous MVCC-garbage-collection framing.

> **Source note:** The snapshot-expiration protocol is well-specified in Iceberg; the storage-tiering pattern is operational practice in the Artemis cold archive and may differ at other deployments.

---

## Context

Every commit produces new metadata files. Every overwrite or compaction commit produces metadata files that reference fewer data files than the previous snapshot. Module 2's design choices — immutable snapshots, append-only metadata, no in-place updates — mean that nothing is ever deleted by the commit path itself. The storage grows monotonically. After two years of ingest plus daily compaction, the Artemis archive has roughly 200 GB of metadata files (snapshots, manifest lists, manifests) and 50 TB of data files; perhaps 5 TB of the data files are unreferenced by the current snapshot (compaction sources, replaced files). Without a cleanup discipline, this only grows.

**Snapshot expiration** is the discipline that reclaims storage by removing the metadata and data files no longer reachable from any kept snapshot. The job is conceptually simple — find what's unreferenced, delete it — but the safety properties are subtle. The expiration must coexist with concurrent readers (pinned snapshots from Module 4 Lesson 1); with the retention window contract (queries against snapshots within the window must work); with storage-tier handoffs (snapshots aged beyond the live window may be archived to a longer-retention tier rather than deleted outright); with the audit trail requirement (some snapshots may need to be preserved permanently for regulatory reasons).

This lesson develops the expiration protocol end to end. The reachability calculation that identifies what's safe to delete. The retention guards that keep live queries working. The storage-tier handoff pattern that moves data to a cheaper tier rather than deleting it. The capstone's lifecycle worker runs expiration continuously; the lesson is the design.

---

## Core Concepts

### The Reachability Calculation

A snapshot is **reachable** if some currently-supported read path can still see it. The Artemis archive's reachability rules:

- The **current snapshot** is reachable.
- Any snapshot **within the retention window** (30 days for the Artemis archive) is reachable. Time-travel queries with timestamps in this window must succeed.
- Any snapshot **explicitly tagged** for permanent retention is reachable. The audit-trail discipline (Lesson 3) preserves certain snapshots beyond the standard window.
- All other snapshots are **expired** and eligible for deletion.

The retention window is the dominant criterion in normal operation. The Artemis archive's 30-day window is set against the longest-supported live replay duration; queries against snapshots older than 30 days are routed to the cold-archive backup tier through a separate read path (the tier handoff described below).

A **data file** is reachable if any reachable snapshot's manifests reference it. A data file may be referenced by multiple snapshots — typically the case during the retention window, when older snapshots still reference data files that have since been replaced by compaction. The data file is reachable as long as *any* reachable snapshot still references it.

The expiration computation: walk every reachable snapshot, collect the set of data files referenced. The union is the reachable file set. Every data file not in this set is unreachable and eligible for deletion. The same logic applies to manifests and manifest lists.

```text
Reachable set:
  - Current snapshot: 1 entry
  - Snapshots within retention window: hundreds to tens of thousands of entries
  - Tagged-for-retention snapshots: a few entries

For each reachable snapshot, walk:
  - Snapshot metadata: 1 file
  - Manifest list: 1 file (referenced by the snapshot)
  - Manifests: thousands of files (referenced by the manifest list)
  - Data files: millions of paths (referenced by manifest entries)

Union of all data file paths across all reachable snapshots = reachable file set.
```

The Artemis archive's reachable file set as of a typical day: 30 days × ~80k commits per day = ~2.4 million snapshots, but most of them reference common data files (long-tail compaction sources). The deduplicated reachable file set is around 1.8 million data files. The unreachable set is around 200k files (compaction sources from outside the window, replaced files, and so on). The expiration job's job is to delete those 200k files plus the metadata that referenced them.

### The Two-Phase Deletion

A delete that immediately removes the file races against in-flight queries (Module 4 Lesson 1). The lakehouse discipline is **two-phase deletion**: mark a file as eligible for deletion at the metadata layer (it's no longer in any reachable snapshot's manifests), then physically delete it after a grace period long enough that no in-flight query can still reference it.

The two phases:

**Phase 1: Metadata-level decommissioning.** The compaction commit (Lesson 1) or any overwrite commit removes the file from the new snapshot. The file's `EntryStatus` in the most recent commit's manifest is `Deleted`. Reachable snapshots from before the commit still reference the file via their own manifests. The file remains on disk; readers pinned to older snapshots continue to read it.

**Phase 2: Physical deletion.** Some time later — when the retention window has advanced past every snapshot that referenced the file — the expiration job runs the reachability calculation. The file is no longer in any reachable snapshot's manifest set. The physical deletion is safe: no reader can pin a snapshot that references the file because every such snapshot has been expired.

The time between Phase 1 and Phase 2 is the **retention window** plus the **expiration scheduling delay**. For the Artemis archive with a 30-day window and daily expiration, the delay is 30-31 days. The expiration's safety margin is intentionally large; the cost of an extra few days of unreachable-file storage is small compared to the cost of breaking an in-flight query.

This is the analog of MVCC garbage collection in row-store databases. DDIA (Ch. 7, "Indexes and snapshot isolation"): "When a transaction commits, the database engine cannot immediately delete old versions, because they may still be needed by another transaction." The lakehouse case is the same; the units are bigger (whole files instead of row versions); the safety properties are the same.

### The Expiration Job Structure

The expiration job runs on a schedule (daily for the Artemis archive). The job is read-mostly against the metadata, write-mostly against the deletion stage. The structure:

1. **Snapshot the catalog.** Read the table's current snapshot and the snapshot history. The expiration runs against this point-in-time view; concurrent commits during the job are handled by the CAS at the end.

2. **Compute the reachable snapshot set.** Apply the reachability rules from above: current snapshot, snapshots within retention window, tagged snapshots. The result is a vec of snapshot IDs to keep.

3. **Identify expired snapshots.** Every snapshot in the history but not in the reachable set is expired.

4. **Compute the reachable file set.** For each reachable snapshot, read its manifest list, read each manifest, collect the data file paths. Union across reachable snapshots. (Optimization: cache the per-snapshot file sets so re-running this job doesn't re-read every manifest. The Artemis worker caches in Redis with a TTL bounded by the retention window.)

5. **Compute the unreachable file set.** Compare the reachable file set against the object store's actual contents. The difference is the unreachable files. (Important: the comparison must use a snapshot of the object store contents from *before* step 2, not after, to avoid race conditions with concurrent ingest writes.)

6. **Schedule deletions.** Each unreachable file is queued for deletion. The deletion is batched and rate-limited to avoid impacting concurrent reads' bandwidth.

7. **Commit the expiration.** A single metadata commit that records the expiration: the snapshots removed from the snapshot history, the data files queued for deletion. The commit is informational; the actual deletions proceed in the background.

The job is safe to re-run; if it fails partway through, the next run picks up where the previous left off. The metadata commits at the end are idempotent — committing an expiration that removes already-removed snapshots is a no-op.

### Snapshot Expiration and the Retention Window

The retention window is the operational lever that bounds time-travel reach. The choice is a tradeoff:

- **Long window** (say 90 days): supports longer time-travel queries; stores more historical metadata and replaced data files; expiration runs less aggressively; storage costs are higher.
- **Short window** (say 3 days): supports only recent time-travel queries; stores minimal historical metadata; expiration runs aggressively; storage costs are lower.

The Artemis archive's 30-day window is the result of measurement against the actual replay-query workload. The investigation team's typical replay span is 7-14 days post-anomaly; the 30-day window covers this with margin. Replays older than 30 days are rare enough that the cold-archive backup tier is the right answer for them.

The window is per-table; not every table needs the same window. The orbital-object-registry table has a 30-day window. The ground-station-telemetry table has a 7-day window (replays don't matter; only the current state is queried). The mission-archive-config table has a 365-day window (used by the audit team).

### Storage-Tier Handoff

A snapshot aging out of the live retention window doesn't have to be deleted outright. The lakehouse community's pattern for long-retention archives is **storage-tier handoff**: copy the snapshot's metadata and data files to a cheaper storage tier before deleting from the live tier. Queries against the long-retention tier go through a separate read path with longer SLAs.

The Artemis archive's tier structure:

- **Live tier**: AWS S3 Standard. 30-day retention. Sub-second access latency. Used by all online queries.
- **Cold tier**: AWS S3 Glacier Instant Retrieval. 7-year retention. Sub-second access latency, ~3× the cost per GB-month of Standard. Used by audit and accident-investigation queries against older history.
- **Deep archive**: AWS S3 Glacier Deep Archive. Permanent retention. Hours-to-days access latency, ~1/8 the cost of Standard. Used for legal compliance retention; queried only through the formal investigation process.

The handoff job runs as a background task between Phase 1 and Phase 2 of the expiration. Before a snapshot's data files are deleted from the live tier, they are copied to the cold tier. The cold tier maintains its own catalog (a separate Iceberg table whose snapshots are the live tier's expired snapshots); analyst queries against old history go through the cold-tier catalog and read from the cold-tier object store.

The complexity worth understanding: **the cold tier's catalog is read-only**. Snapshots in it are immutable; no commits modify them; the catalog's CAS protocol is unused. The cold-tier read path is simpler than the live-tier read path because there's no concurrent writer to worry about. The audit-trail use case fits this perfectly: the data is what it was at the time it aged out of the live tier; nothing modifies it later.

### Coordinating with the Live Workload

The expiration job's work — reachability calculation, file listing, metadata commits — competes with the live query workload for object-store bandwidth and the catalog's read budget. The discipline matches Lesson 1's compaction pacing:

- Read budget capped at 20% of available object-store IOPS.
- Expiration runs during the daily quiet window (overnight UTC for the Artemis archive's analyst-team time zone).
- Catalog reads use a separate connection pool from the ingest writers to avoid lock contention.
- The metadata-commit at the end of the job retries on CAS conflict with the ingest commits, using the same retry-with-jitter pattern as the ingest path.

The result is that expiration completes within the daily quiet window without impacting analyst-visible performance. The work fits comfortably; the live tier's data file count stays bounded; storage growth is dominated by genuine new ingest, not by unreclaimed history.

### Tagged Retention: The Audit Exception

Most snapshots expire on the standard schedule. Some don't, by explicit operator decision. The Artemis archive supports **tagged retention** through the Iceberg tag mechanism:

```text
-- Operator-side: tag a snapshot for permanent retention.
TAG snapshot 4729 AS 'incident-2024-03-15-conjunction-alert' WITH RETENTION FOREVER;
```

Tagged snapshots are exempt from the expiration reachability rules. The expiration job treats them as reachable; their data files and metadata stay on disk indefinitely (or until the tag is explicitly removed). Tags also serve as named bookmarks for queries: `SELECT ... FROM table FOR TAG 'incident-2024-03-15-conjunction-alert'` reads against the tagged snapshot directly without timestamp resolution.

The tagged-retention pattern handles the audit and regulatory cases without complicating the standard expiration logic. The retention exception is one entry in the reachable set; everything else operates as if it weren't there. The Artemis archive has roughly 50 tags at any time — one per significant orbital event over the past two years — adding a few GB of preserved metadata and around 200 GB of preserved data files. Manageable cost; full forensic reach.

---

## Core Mechanics in Code

### The Reachability Walk

The core of the expiration job: walk the reachable snapshots, collect every file path they reference.

```rust,ignore
use anyhow::Result;
use std::collections::HashSet;

pub struct ReachableSet {
    pub snapshot_ids: HashSet<SnapshotId>,
    pub data_file_paths: HashSet<String>,
    pub manifest_paths: HashSet<String>,
    pub manifest_list_paths: HashSet<String>,
}

/// Walk the reachable snapshots and collect every file they reference.
/// The set returned is the complement of the deletable files: anything
/// in the object store not in this set is unreachable.
pub async fn compute_reachable_set(
    catalog: &PostgresCatalog,
    table: &str,
    retention_window_ms: i64,
    tagged_snapshot_ids: &HashSet<SnapshotId>,
) -> Result<ReachableSet> {
    let history = read_snapshot_history(catalog, table).await?;
    let now_ms = current_unix_ms();

    // 1. Identify reachable snapshots:
    //    - the current snapshot (the last entry in history)
    //    - any snapshot within the retention window
    //    - any tagged snapshot
    let mut reachable_snapshots: HashSet<SnapshotId> = HashSet::new();
    if let Some(current) = history.last() {
        reachable_snapshots.insert(current.snapshot_id);
    }
    for entry in &history {
        if now_ms - entry.timestamp_ms < retention_window_ms {
            reachable_snapshots.insert(entry.snapshot_id);
        }
        if tagged_snapshot_ids.contains(&entry.snapshot_id) {
            reachable_snapshots.insert(entry.snapshot_id);
        }
    }

    // 2. For each reachable snapshot, collect referenced files.
    let mut data_paths: HashSet<String> = HashSet::new();
    let mut manifest_paths: HashSet<String> = HashSet::new();
    let mut manifest_list_paths: HashSet<String> = HashSet::new();

    for entry in &history {
        if !reachable_snapshots.contains(&entry.snapshot_id) {
            continue;
        }
        let snapshot: Snapshot = read_metadata_file(&entry.metadata_path).await?;
        manifest_list_paths.insert(snapshot.manifest_list_path.clone());

        let manifest_list = read_manifest_list(&snapshot.manifest_list_path).await?;
        for ml_entry in &manifest_list.manifests {
            manifest_paths.insert(ml_entry.manifest_path.clone());
            let manifest = read_manifest(&ml_entry.manifest_path).await?;
            for me in manifest.entries {
                if matches!(me.status, EntryStatus::Existing | EntryStatus::Added) {
                    data_paths.insert(me.data_file.path);
                }
            }
        }
    }

    Ok(ReachableSet {
        snapshot_ids: reachable_snapshots,
        data_file_paths: data_paths,
        manifest_paths,
        manifest_list_paths,
    })
}
```

The walk is the expensive part. For the Artemis archive with ~80k commits per day and a 30-day window, the walk reads ~2.4M snapshot metadata files (small; tens of KB each), ~2.4M manifest lists (the same), and several million distinct manifests (one per commit, but deduplicated across snapshots since each manifest is committed in exactly one snapshot). The data file paths are collected as the manifest read proceeds. The total work is a few hours of mostly-parallel I/O against the metadata store; the lifecycle worker parallelizes the per-snapshot walks at a configurable concurrency.

### The Expiration Decision

Given the reachable set, identify what to delete:

```rust,ignore
use anyhow::Result;
use std::collections::HashSet;

pub struct ExpirationPlan {
    pub expired_snapshots: Vec<SnapshotId>,
    pub deletable_data_files: Vec<String>,
    pub deletable_manifests: Vec<String>,
    pub deletable_manifest_lists: Vec<String>,
}

/// Plan the expiration: identify snapshots to remove from history and
/// files (data, manifest, manifest list) to mark for physical deletion.
pub async fn plan_expiration(
    catalog: &PostgresCatalog,
    table: &str,
    reachable: &ReachableSet,
    object_store: &dyn ObjectStore,
) -> Result<ExpirationPlan> {
    let history = read_snapshot_history(catalog, table).await?;

    // Snapshots to remove from history.
    let expired_snapshots: Vec<SnapshotId> = history
        .iter()
        .filter(|entry| !reachable.snapshot_ids.contains(&entry.snapshot_id))
        .map(|entry| entry.snapshot_id)
        .collect();

    // Enumerate the metadata directory and identify metadata files not
    // in the reachable set.
    let all_data_files = list_object_store_dir(object_store, "data/").await?;
    let all_manifests = list_object_store_dir(object_store, "metadata/m").await?;
    let all_manifest_lists = list_object_store_dir(object_store, "metadata/ml").await?;

    let deletable_data_files: Vec<String> = all_data_files
        .into_iter()
        .filter(|path| !reachable.data_file_paths.contains(path))
        .collect();
    let deletable_manifests: Vec<String> = all_manifests
        .into_iter()
        .filter(|path| !reachable.manifest_paths.contains(path))
        .collect();
    let deletable_manifest_lists: Vec<String> = all_manifest_lists
        .into_iter()
        .filter(|path| !reachable.manifest_list_paths.contains(path))
        .collect();

    Ok(ExpirationPlan {
        expired_snapshots,
        deletable_data_files,
        deletable_manifests,
        deletable_manifest_lists,
    })
}
```

The pattern. The plan is the set of files to delete and the set of snapshots to remove from history. The plan is committed first via a metadata commit that updates the snapshot history; the physical deletes proceed in the background after the commit. If the worker crashes between the commit and the deletions, the next run re-plans and resumes — the metadata is the source of truth and the deletes are idempotent.

### The Storage-Tier Handoff (Sketch)

Before deletion from the live tier, copy data files to the cold tier:

```rust,ignore
use anyhow::Result;

pub async fn handoff_to_cold_tier(
    plan: &ExpirationPlan,
    live_store: &dyn ObjectStore,
    cold_store: &dyn ObjectStore,
    cold_catalog: &PostgresCatalog,
) -> Result<()> {
    // 1. Copy each data file from live to cold. The copy is streamed
    // to bound memory; the destination path mirrors the source path
    // (the cold tier uses the same path layout for simplicity).
    for src in &plan.deletable_data_files {
        let dst = src.clone();
        copy_streamed(live_store, src, cold_store, &dst).await?;
    }

    // 2. Copy the expired snapshots' metadata to the cold tier.
    // The cold tier catalog will reference these by path.
    for snapshot_id in &plan.expired_snapshots {
        let entry = find_history_entry(snapshot_id)?;
        let snapshot: Snapshot = read_metadata_file(&entry.metadata_path).await?;
        // Copy snapshot metadata, manifest list, and manifests.
        copy_streamed(live_store, &entry.metadata_path, cold_store, &entry.metadata_path).await?;
        copy_streamed(live_store, &snapshot.manifest_list_path,
                      cold_store, &snapshot.manifest_list_path).await?;
        let manifest_list = read_manifest_list(&snapshot.manifest_list_path).await?;
        for ml_entry in manifest_list.manifests {
            copy_streamed(live_store, &ml_entry.manifest_path,
                          cold_store, &ml_entry.manifest_path).await?;
        }
    }

    // 3. Commit a new snapshot to the cold-tier catalog referencing
    // the just-copied snapshots. The cold tier is append-only at this
    // point; old cold-tier snapshots are preserved.
    cold_catalog.append_snapshots_to_history(plan.expired_snapshots.clone()).await?;

    Ok(())
}
```

The pattern. The cold tier accumulates the live tier's expired snapshots. Audit and accident-investigation queries route to the cold tier through a separate read path; the cold tier's data is immutable, the catalog read-only, the operational profile much simpler than the live tier. The 7-year retention budget is the cost of keeping the cold tier; the Artemis archive's actual cold-tier size after two years of operation is around 35 TB — manageable, and a small fraction of the live tier's 50 TB working set.

---

## Key Takeaways

- **Snapshot expiration reclaims storage** by removing metadata and data files that no reachable snapshot references. The job is essential — without it the storage grows monotonically — but it must coexist safely with concurrent reads.
- **Two-phase deletion** decouples visibility from physical deletion. Phase 1 (metadata commit) removes the file from the new snapshot's manifest; Phase 2 (physical delete) happens later, after the retention window guarantees no reader can still need the file.
- **The reachability calculation** is the heart of expiration. Walk every reachable snapshot (current + within retention window + tagged), union the file references, compare against the object store contents. The complement is the deletable set.
- **Storage-tier handoff** moves aged-out data to a cheaper tier rather than deleting outright. The Artemis archive's three-tier structure (S3 Standard / Glacier IR / Deep Archive) balances access latency against cost; audit and investigation use cases against old history go through a separate read path against the cheaper tiers.
- **Tagged retention** is the audit exception. Specific snapshots can be preserved indefinitely via tags; the reachability calculation treats them as reachable; the expiration logic stays clean.

{{#quiz quiz-02.toml}}
