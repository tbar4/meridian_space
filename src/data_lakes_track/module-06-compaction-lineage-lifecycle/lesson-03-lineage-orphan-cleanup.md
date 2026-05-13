# Lesson 3 — Lineage, Audit Trails, and Orphan Cleanup

**Module:** Data Lakes — M06: Compaction, Lineage, and Lifecycle
**Position:** Lesson 3 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 11 ("Stream Processing — Event Sourcing"). Apache Iceberg specification, "Snapshot Properties" and "Maintenance" sections.

> **Source note:** Lineage practice varies widely across deployments; the Artemis discipline described here is one specific pattern. The orphan-cleanup mechanic is well-specified by Iceberg's `RemoveOrphanFiles` action; verification against the current version is recommended.

---

## Context

The previous two lessons handled the *what* of lifecycle work — consolidating small files and expiring old snapshots. This lesson handles the *why* and the cleanup of incidents the other jobs leave behind. **Lineage** is the audit trail that records what produced each commit: which job, which input snapshots, which transformation, which operator triggered it. **Orphan cleanup** is the reconciliation pass that finds files in the object store that no metadata references — debris from failed commits, interrupted compactions, or aborted maintenance jobs.

The two subjects pair because they share a common discipline: **the metadata is the source of truth**. Lineage records the metadata's history; orphan cleanup reconciles the object store against the metadata. Both jobs sit downstream of the commit path and serve the operations team rather than the analyst workload. A lakehouse without lineage is one no one can audit; a lakehouse without orphan cleanup is one whose object-store costs grow with the failure rate of every maintenance job.

This lesson develops both. The lineage schema that records what produced each commit with enough fidelity for forensic analysis. The schema-history compaction pattern that keeps the lineage's own metadata bounded. The orphan-detection scan that walks the object store and compares against metadata. The safety properties that prevent orphan cleanup from accidentally deleting an in-flight commit. The capstone's lifecycle worker runs both as part of its daily maintenance pass.

---

## Core Concepts

### What the Lineage Records

A commit's lineage answers questions like: which writer made this commit? Which input data did it transform? Which downstream consumers depend on it? At what timestamp did it happen? Was this a routine ingest, a backfill, a compaction, or an incident response? The lineage's job is to record enough context that the operations team can answer these questions months or years later.

The Artemis archive's commit-lineage schema, attached to each snapshot via Iceberg's `snapshot.summary` field (key-value metadata; the Iceberg spec leaves the keys to the operator):

```text
snapshot.summary:
  operation: "append" | "overwrite" | "compaction" | "expiration"
  writer.id: "ingest-pipeline-prod-1" | "compaction-worker-2" | ...
  writer.host: "ground-station-3.artemis.internal"
  writer.commit_hash: "a1b2c3d..."             // git SHA of the writer binary
  writer.invocation_id: "run-2024-03-15-1027"  // unique per writer invocation
  input.snapshot_ids: [4727, 4728]             // for compaction/replay commits
  input.row_count: 12000                       // rows ingested or processed
  input.bytes_compressed: 24831044
  trigger.type: "schedule" | "manual" | "incident-response"
  trigger.operator: "j.smith"                  // if trigger.type = manual
  trigger.ticket: "ARTEMIS-2024-03-15-3"       // if incident-response
```

The fields are operational. **writer.id** identifies the producing service. **commit_hash** ties the data to a specific binary version, which the audit team can use to read the transformation logic at the time. **invocation_id** distinguishes between multiple runs of the same writer (e.g., two backfill jobs). **input.snapshot_ids** records the upstream data — for compaction, the source snapshot whose files were consolidated; for replay-derived data, the snapshot the replay was run against. **trigger.type** records why the commit happened — a routine schedule run, an operator-initiated change, an incident-response action. **trigger.ticket** ties incident-response commits to the originating issue.

The discipline. Every commit, no exceptions, writes these fields. The writer code is the only place that knows what it's doing; the lineage must be recorded at commit time, not reconstructed later. The Artemis archive's ingest writer, compaction worker, and replay tooling all populate the lineage fields as part of their normal commit path; a commit that omits required lineage fields is rejected by a pre-commit validation hook (added to the table format library; it inspects `snapshot.summary` against the table's required-keys configuration).

DDIA (Ch. 11, "Event Sourcing") makes the same point in the stream-processing context: "Event sourcing has the property that the application state is determined by the sequence of events. If you keep the entire history of events, you have an audit log of everything that has ever happened." The lakehouse's snapshot history is exactly this event log; the lineage metadata is what makes the events meaningful for audit.

### The Audit Use Cases

The Artemis archive's audit team has three recurring queries against the lineage. Each one motivates a specific aspect of the schema.

**"When was this data ingested, and by what version of the ingest pipeline?"** This is the post-mortem query when an analyst discovers a data quality issue. The answer requires `writer.id`, `writer.commit_hash`, and `timestamp_ms` (recorded by the snapshot itself). The audit team correlates `commit_hash` against the ingest-pipeline source repository to see exactly what transformation produced the suspect data.

**"What snapshots are downstream of this incident?"** When an incident reveals that a specific input was bad (a sensor mis-calibrated, a ground-station feed corrupted), the audit team needs to know every snapshot that consumed the bad input. The lineage's `input.snapshot_ids` field is what enables this: the audit team finds the snapshot that ingested the bad input, then queries for every later snapshot that listed that one as an input (recursively). The result is the bounded blast radius of the incident.

**"Who triggered this overwrite commit?"** The Artemis archive's overwrite path is restricted — only the correction-job tooling produces overwrites, and only operators with specific permissions can trigger it. The lineage's `trigger.type`, `trigger.operator`, and `trigger.ticket` fields are the audit trail for this: every overwrite is traced to a specific human operator and a specific incident ticket. A commit without these fields fails pre-commit validation; the discipline is enforced at the commit boundary.

All three queries are answered by reading the snapshot history and inspecting the summary fields. The cost is one metadata read per snapshot in the audit window; for the typical audit query against ~1000 snapshots, this is a few seconds of wall-clock time.

### Schema-History Compaction

The lineage data is stored in the snapshot summaries. Over time the snapshot history accumulates; for tables with hourly commits over a 30-day window, the history has ~720 entries, each with the summary metadata. The history's size grows linearly. For the Artemis archive's millions-of-commits-per-mission scale, the schema history is a non-trivial metadata payload.

The accumulation isn't a correctness problem — every entry is independently useful for audit. But the metadata read cost is real: reading the snapshot-history-log file to plan a time-travel query touches every entry. For tables with many millions of snapshots in history, the planning latency degrades.

The mitigation is **schema-history compaction**: a separate metadata commit that rewrites the snapshot-history-log to a compacted form. Old entries (typically those outside the active time-travel window) are summarized rather than enumerated — a single entry says "snapshots 4000-50000 occurred between T1 and T2, all routine ingest, all from writer ingest-pipeline-prod-1" rather than 46000 individual entries. The full per-snapshot detail remains in the underlying snapshot metadata files (which are themselves preserved or archived by Lesson 2's expiration); the compacted log is just the index.

The compaction runs monthly. The size of the snapshot-history-log file drops from megabytes to kilobytes; metadata-read latency drops by orders of magnitude. Audit queries that need per-snapshot detail still work — they read the snapshot file directly through the compacted log's pointer.

The Iceberg specification's metadata-log-compaction support is the primitive; the Artemis archive uses it on the Iceberg-standard schedule.

### Orphan Files: The Failure Debris

An **orphan file** is an object-store file that exists physically but is not referenced by any reachable snapshot's metadata. Orphans come from a few sources:

- **Failed commits.** Module 2 Lesson 3's retry loop writes new metadata files speculatively; on conflict, the writer rebases and retries, leaving the previous attempt's metadata files behind. These are orphan manifests and orphan snapshot files. The associated data files are *not* orphans because the next attempt's commit references them.
- **Aborted compactions.** A compaction that wrote its consolidated output file but failed to commit (perhaps because the source files were no longer reachable; Lesson 1's sanity check) leaves the consolidated file behind. It is referenced by nothing.
- **Interrupted writes.** A writer that crashed partway through a multi-file commit may have written some data files before the crash. If the crash prevented the commit from completing, the partial data files are orphans.
- **Manual operations.** Sometimes operators copy files around for testing or debugging; those files are typically not in any commit's manifests and become orphans by definition.

Orphans aren't a correctness issue — they don't affect query results. They are a cost issue: every orphan is occupied storage paid for with no operational benefit. A poorly-maintained lakehouse can accumulate substantial orphan debris; the Artemis archive's first month of operation, before orphan-cleanup was in place, accumulated ~5% of its total storage in orphans. After the cleanup discipline was operational, the orphan rate dropped to a steady ~0.1% (mostly in-flight writes during the cleanup scan, cleaned up on the next pass).

### Orphan Detection: The Scan Discipline

Orphan detection is the inverse of the reachability calculation from Lesson 2. The reachability calculation produces the *reachable* file set. Orphan detection produces the *unreferenced and not-currently-being-written* file set.

The discipline that's important for safety: **a file that is currently being written must not be classified as an orphan**, because deleting an in-flight file breaks the writer that's writing it. The fix is a **minimum age threshold**: only files older than some safety window are eligible for orphan classification. The Artemis archive uses a 7-day minimum age. A file younger than 7 days is assumed to be possibly in-flight; the next cleanup pass picks it up if it's still orphaned then.

The detection algorithm:

1. Snapshot the reachable file set (Lesson 2's calculation; cached or re-run).
2. List the object store's data and metadata directories.
3. For each listed file, check three conditions: (a) it is not in the reachable set, (b) it is older than the minimum-age threshold, (c) it is older than the in-flight-write window for currently-active writers (the Artemis writers heartbeat to the catalog; the cleanup job consults the heartbeats to identify which writers are active and what files they could be writing).
4. Files meeting all three conditions are orphans. Delete them.

The third condition is the safety guard that distinguishes in-flight from genuinely-orphan files. A writer that started a multi-file commit 3 hours ago hasn't yet finished; its in-progress data files are not yet in any manifest but they will be soon. The minimum-age threshold gives the writer time to finish; the active-writer heartbeat gives additional defense in case the writer is slow.

### Idempotency and Resumability

Both lineage compaction and orphan cleanup are batch jobs with potentially-large work. A crashed job must resume cleanly on the next run. The discipline is **idempotency**: every step is safe to repeat.

For lineage compaction: the compacted snapshot-history-log is written atomically (the rename pattern; an in-progress write produces a `.tmp` file that doesn't replace the current log until the rename). A crash mid-write leaves the in-progress file behind (an orphan, cleaned up by the next orphan-cleanup pass); the current log is unchanged.

For orphan cleanup: each file's deletion is independent. The job logs deletions to a journal as it runs; on resume, it reads the journal, skips files already deleted (a tombstone in the journal), continues from where it left off. The journal is itself a small file in the metadata directory; the lifecycle worker maintains it as part of its working state.

Both jobs commit no shared state until they complete. The CAS-on-the-catalog protocol used by ingest and compaction commits is *not* used by maintenance jobs — they don't change the table's data state, only the metadata structure. The result is that maintenance failures don't interfere with the live workload; failed maintenance is just incomplete maintenance, retryable next time.

### The Lifecycle Worker's Scheduling

The capstone's lifecycle worker runs all four jobs on a continuous schedule. The interactions matter; getting the ordering wrong produces wasted work or cascading conflicts.

The Artemis worker's daily schedule:

- **00:00 UTC**: Snapshot expiration (Lesson 2). Reachable-set calculation, file enumeration, deletion scheduling.
- **02:00 UTC**: Storage-tier handoff (Lesson 2). Aged-out snapshots copied to the cold tier.
- **04:00 UTC**: Physical deletion (Lesson 2 Phase 2). The expiration plan's deletable files are removed from the live tier.
- **06:00 UTC**: Orphan cleanup (Lesson 3). The post-expiration scan picks up debris from the day's expiration plus any other accumulated orphans.
- **Throughout the day, with bandwidth budget**: Compaction (Lesson 1). Runs continuously, target-sized to consume at most 30% of available bandwidth.
- **Weekly (Sunday 02:00 UTC)**: Manifest compaction (Lesson 1). Consolidates the week's per-commit manifests.
- **Monthly (1st of month)**: Schema-history compaction (Lesson 3). Compacts the snapshot-history-log.

The ordering matters. Expiration runs before orphan cleanup because expiration's Phase 2 deletes produce a clean state for orphan cleanup to operate against. Compaction runs continuously rather than in a window because it benefits from spreading the work; expiration runs in a single window because its reachability calculation is most efficient when run all at once. Schema-history compaction runs least often because its benefit is amortized over many time-travel queries.

The worker's metrics — files compacted, snapshots expired, orphans cleaned, lineage compactions per period — are reported to the observability stack alongside the live query metrics. The operations team sees the lifecycle health as a first-class observable, not as background noise.

---

## Core Mechanics in Code

### The Lineage Validation Hook

Every commit validates its lineage fields before the CAS:

```rust,ignore
use anyhow::{anyhow, Result};
use std::collections::HashMap;

pub struct LineageRequirements {
    pub required_keys: Vec<String>,
    pub allowed_operations: Vec<String>,
    pub trigger_validators: HashMap<String, Box<dyn Fn(&HashMap<String, String>) -> Result<()>>>,
}

/// Validate the lineage fields in a snapshot's summary against the
/// table's lineage requirements. Returns Err if any required field
/// is missing or invalid.
pub fn validate_lineage(
    summary: &HashMap<String, String>,
    requirements: &LineageRequirements,
) -> Result<()> {
    // 1. Every required key must be present.
    for key in &requirements.required_keys {
        if !summary.contains_key(key) {
            return Err(anyhow!("required lineage key missing: {}", key));
        }
    }

    // 2. The operation must be one of the allowed values.
    let operation = summary.get("operation")
        .ok_or_else(|| anyhow!("operation key missing"))?;
    if !requirements.allowed_operations.contains(operation) {
        return Err(anyhow!("operation '{}' not allowed", operation));
    }

    // 3. Trigger-specific validation: if trigger.type = manual, then
    //    trigger.operator must be present; if trigger.type = incident-
    //    response, then trigger.ticket must be present.
    if let Some(trigger_type) = summary.get("trigger.type") {
        if let Some(validator) = requirements.trigger_validators.get(trigger_type) {
            validator(summary)?;
        }
    }

    Ok(())
}
```

The pattern. The validation is per-commit; the requirements are per-table. Tables with different audit needs configure different required keys. The validation runs synchronously in the commit path — before the CAS — and rejects malformed commits before they advance the snapshot pointer. The cost is microseconds; the safety benefit is significant.

### The Orphan Detection Scan

The reconciliation pass that finds orphan files:

```rust,ignore
use anyhow::Result;
use std::collections::HashSet;
use std::time::{SystemTime, Duration};

const MIN_ORPHAN_AGE: Duration = Duration::from_secs(7 * 24 * 3600); // 7 days

pub struct OrphanScanResult {
    pub orphan_data_files: Vec<String>,
    pub orphan_manifests: Vec<String>,
    pub skipped_in_flight: u32,
}

/// Scan the object store for files unreferenced by any reachable snapshot.
/// Filters out files younger than the min-age threshold (potentially in
/// flight) and files associated with currently-active writers.
pub async fn detect_orphans(
    table: &Table,
    reachable: &ReachableSet,
    object_store: &dyn ObjectStore,
    active_writer_paths: &HashSet<String>,
) -> Result<OrphanScanResult> {
    let now = SystemTime::now();
    let mut orphan_data: Vec<String> = Vec::new();
    let mut orphan_manifests: Vec<String> = Vec::new();
    let mut skipped: u32 = 0;

    // Scan the data directory.
    let data_files = object_store.list_dir("data/").await?;
    for file in data_files {
        if reachable.data_file_paths.contains(&file.path) {
            continue;
        }
        if active_writer_paths.contains(&file.path) {
            skipped += 1;
            continue;
        }
        let age = now.duration_since(file.last_modified).unwrap_or(Duration::ZERO);
        if age < MIN_ORPHAN_AGE {
            skipped += 1;
            continue;
        }
        orphan_data.push(file.path);
    }

    // Same for manifests directory.
    let manifest_files = object_store.list_dir("metadata/").await?;
    for file in manifest_files {
        if reachable.manifest_paths.contains(&file.path)
            || reachable.manifest_list_paths.contains(&file.path)
        {
            continue;
        }
        let age = now.duration_since(file.last_modified).unwrap_or(Duration::ZERO);
        if age < MIN_ORPHAN_AGE {
            skipped += 1;
            continue;
        }
        orphan_manifests.push(file.path);
    }

    Ok(OrphanScanResult {
        orphan_data_files: orphan_data,
        orphan_manifests,
        skipped_in_flight: skipped,
    })
}
```

What to notice. The age threshold is the primary safety guard. The active-writer-paths set is the secondary guard for writers whose work pre-dates the threshold (e.g., a backfill that has been running for two weeks). The two together ensure no in-flight write is misclassified as an orphan. The `skipped_in_flight` count is reported as a metric; sustained nonzero values indicate either many slow writers or a misconfiguration of the active-writer tracking — both worth surfacing to the operations team.

### The Deletion Loop with Journaling

The deletion side, with a journal that makes the job resumable:

```rust,ignore
use anyhow::Result;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub async fn delete_orphans(
    scan_result: &OrphanScanResult,
    object_store: &dyn ObjectStore,
    journal_path: &PathBuf,
) -> Result<u32> {
    // Open the journal in append mode. The journal records every
    // successful deletion; on a restart, the worker reads the journal
    // to skip files already deleted.
    let mut journal = OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)
        .await?;

    let already_deleted = read_journal_entries(journal_path).await?;
    let mut deleted_count: u32 = 0;

    for path in scan_result.orphan_data_files.iter()
        .chain(scan_result.orphan_manifests.iter())
    {
        if already_deleted.contains(path) {
            continue;
        }
        match object_store.delete(path).await {
            Ok(()) => {
                journal.write_all(format!("{path}\n").as_bytes()).await?;
                deleted_count += 1;
            }
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "failed to delete orphan; will retry next pass");
                // Don't journal the failure; the next pass picks it up.
            }
        }
    }

    journal.flush().await?;
    Ok(deleted_count)
}
```

The discipline. Every successful deletion is logged to the journal before the deletion is considered durable. On restart the journal is read and already-deleted paths are skipped. Failed deletions are not journaled — the next pass retries them. The pattern is the standard write-ahead-log pattern from the Database Internals track, applied at the lifecycle-job layer.

---

## Key Takeaways

- **Lineage is the audit trail** that records what produced each commit. The schema must include enough fields (writer ID, commit hash, input snapshots, trigger metadata) to answer audit questions months or years later.
- **Lineage validation runs at commit time.** The writer is the only place that knows what it's doing; the commit-path validation hook rejects malformed commits before they advance the snapshot pointer.
- **Orphan files come from failure modes** of the commit and maintenance paths. They are a cost issue rather than a correctness issue, but accumulated debris can be substantial; the cleanup discipline keeps orphan storage to single-digit percentages.
- **Orphan detection requires age-based safety guards.** Files younger than the minimum-age threshold are assumed to be possibly in flight and are skipped. Active-writer tracking handles writers whose work pre-dates the threshold.
- **The lifecycle worker schedules all four jobs** (compaction, expiration, orphan cleanup, lineage compaction) with explicit ordering and bandwidth budgets. The ordering avoids cascading conflicts; the bandwidth budget keeps maintenance from impacting live queries.

{{#quiz quiz-03.toml}}
