# Lesson 4 — Dead Letter Queues

**Module:** Data Pipelines — M05: Delivery Guarantees and Fault Tolerance
**Position:** Lesson 4 of 4
**Source:** *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 (Error Handling and Dead-Letter Queues); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 9 (Failure Handling and Reprocessing)

---

## Context

The retry policy from Module 2 L3 covers *transient* errors — the network blip, the broker leader election, the partner API hiccup. The idempotency machinery from M5 L2 absorbs the duplicates that retries inevitably produce. The checkpoint machinery from L3 lets the pipeline recover its state across restart. The combination handles every recoverable failure mode the SDA pipeline encounters.

It does not handle *permanent* failures. Some events cannot be processed regardless of how many times they are retried. A frame from the radar source whose binary payload cannot be deserialized — the wire format does not match the protocol the operator was built against, possibly because the radar's firmware was upgraded without coordination. An observation referencing a satellite catalog ID that has been decommissioned for orbit burn — the record is internally consistent but references state that no longer exists. An ISL beacon's state vector with an impossible-physics value (radius below Earth's surface, velocity above c) that violates the operator's input invariants. Retrying these does not help; every retry produces the same error, and the operator's retry-budget eventually exhausts. Dropping them silently violates the audit requirement that every observation is accounted for.

The third path is the **dead-letter queue** — a separate sink, distinct from the main pipeline, that receives events the operator cannot process. Each entry carries the original event plus enough metadata for an engineer to investigate: the error kind, the operator name, the timestamp, the retry attempts that were tried. The DLQ is not a discard bucket; it is a *debug tool* and a *re-processing source*. Engineers inspect it during incident response; engineers re-inject from it when the underlying issue is fixed. The pattern is universal in production streaming systems; this lesson develops it for SDA's pipeline.

---

## Core Concepts

### The Retry-Disposition Decision Tree

Every error from an operator's hot path classifies into one of three buckets.

**Transient (Retry).** Network errors, 5xx responses, timeouts, broker leader-elections. Will resolve on retry given enough time. The retry wrapper from M2 L3 handles these with decorrelated-jitter backoff. The operator's retry budget bounds the time spent retrying.

**Permanent (DLQ).** Deserialization errors, schema-mismatch errors, validation failures, references to non-existent state. Will NOT resolve on retry — every attempt produces the same error. Routes to the DLQ for human-investigable handling. Does not consume retry budget.

**Discardable (drop).** Invariant violations that should be dropped silently without operational attention. The radar frame whose wire format declares a length larger than the maximum permitted — a clear bug at the source, not worth investigating, not worth re-processing. Drops with a metric increment.

The classification is the operator's responsibility. M2 L3 introduced `RetryDisposition::{Retry, Permanent, Discard}`; this lesson extends `Permanent` to mean "DLQ" — the operator hands the event to the DLQ sink rather than just propagating the error. The lesson's discipline: every operator's error path classifies explicitly, every classification is documented in code, the default for unknown errors is `Permanent` (DLQ) so they surface for operational attention rather than getting silently retried or dropped.

### DLQ Metadata

The DLQ entry is the event plus context. The context is what makes the DLQ a debug tool rather than a dump.

```rust,ignore
pub struct DlqEntry {
    /// Wall-clock time when the error occurred.
    pub timestamp: SystemTime,
    /// The operator that produced the error.
    pub operator: String,
    /// The kind of error (deserialization, validation, processing exception).
    pub error_kind: DlqErrorKind,
    /// Free-form error message for human investigators.
    pub error_message: String,
    /// Number of retry attempts before giving up (typically 0 for
    /// permanent errors, > 0 for transient errors that exceeded the
    /// retry budget).
    pub retry_count: u32,
    /// The original event. Stored as the raw bytes (or the deserialized
    /// envelope when available) so re-processing can replay it.
    pub original_payload: Vec<u8>,
    /// Schema version of the metadata format itself. Important for
    /// re-processing tools that span DLQ entries from different
    /// pipeline versions.
    pub schema_version: u32,
}

pub enum DlqErrorKind {
    Deserialization,
    SchemaMismatch,
    ValidationFailed,
    ProcessingException,
    RetryBudgetExhausted,
}
```

The schema_version is the often-overlooked field. The DLQ accumulates entries over a long time horizon; the metadata format will evolve as the pipeline evolves. A re-processing tool reading entries from six months ago needs to know what fields the entry carried when it was written. The version field is the migration mechanism — a future tool reads `schema_version=1` entries with the v1 format and `schema_version=2` entries with the v2 format. Without the version, future migrations require either guessing or losing entries.

### Poison Pills

A poison pill is an event that causes errors *every* retry. The poison-pill scenario is what distinguishes DLQ-bearing pipelines from drop-only ones. Without a DLQ, a poison pill blocks the pipeline: the operator retries, fails, retries, fails, exhausts its retry budget, the supervisor restarts the operator, the operator reads the same poison pill from the consumer offset, fails again. The pipeline makes no progress past the poison pill.

With a DLQ, the poison pill is quarantined. The operator's first retry attempt classifies the error as Permanent, hands it to the DLQ, and continues with the next event. The pipeline stays healthy; the poison pill is in the DLQ for investigation. The operational discipline: a metric for `dlq_entries_total{error_kind}` and an alert when the rate exceeds a threshold. A spike in DLQ entries is a signal — a partner's API change, a schema migration that did not roll out everywhere, a bug in the operator's deserializer.

The threshold for the alert is tuned per error kind. A handful of `Deserialization` errors per day during steady-state is normal (occasional malformed wire packets). A spike to hundreds per minute signals a partner change or a rollback. `RetryBudgetExhausted` errors should be rare; a spike means a downstream is degraded longer than the retry budget covers, and operations should investigate.

### The DLQ as Re-processing Source

The DLQ is *also* a stream. Once an underlying issue is fixed (a partner's API change is reverted, a schema migration completes, a bug fix deploys), the DLQ's entries can be re-processed through the pipeline. A re-processing tool reads from the DLQ, reconstructs the original events, and pushes them back into the pipeline's input topic. The operators process them as if they were freshly arrived, the dedup logic from L2 absorbs the duplicates from any prior partial processing, and the events end up in the right downstream state.

The re-processing tool is its own piece of code, separate from the live pipeline. It has access to the DLQ's stored entries, knows the schema_version migration story, can filter by error_kind or operator or time range. The tool is an operational lever: when an issue is fixed, ops runs the tool against the affected DLQ window to recover the affected events. Without the tool, the events are stuck in the DLQ forever; with the tool, the DLQ's role is "temporarily holding events while we figure out what to do," which is exactly the right framing.

The tool's correctness depends on the pipeline's idempotency. Re-injecting from the DLQ produces the same events the pipeline processed before; without idempotency, re-processing produces duplicate effects. The L2 machinery (sink-side dedup, idempotent UPSERT, Idempotency-Key headers) absorbs the re-injection; the re-processing is safe by construction. This is the L2-L4 composition the capstone exercises end-to-end.

### The Discard-Bucket Anti-Pattern

A DLQ that nobody reads is worse than no DLQ. Entries pile up; ops loses track of what they mean; a real incident produces a small spike that gets lost in the noise of historical entries. This is the anti-pattern the lesson identifies and the discipline to prevent it.

Three operational practices distinguish a useful DLQ from a discard bucket.

**Alert on growth rate.** A DLQ growing at >10x its baseline rate for >5 minutes fires an alert. The alert text names the dominant error_kind in the recent window so on-call has an immediate hypothesis to investigate.

**Periodic review.** The DLQ has a weekly review cadence. Ops walks through new entries since last review, classifies the dominant patterns, decides on remediation per error_kind. Some patterns become permanent fixes (the operator's classifier is updated to handle the case). Some become re-processing tasks (the underlying issue was fixed; re-inject the affected window). Some become explicit non-events (the entry represents a known-and-accepted failure mode).

**Bounded retention.** The DLQ does not grow forever. Entries older than the retention window (typically 30 days for SDA) are evicted. The retention is a forcing function for operational discipline — the team cannot ignore the DLQ indefinitely; entries must be addressed before they age out. The retention is documented and the eviction is logged so the team knows what they're losing.

The DLQ's value is proportional to the discipline applied to it. A well-managed DLQ catches partner API changes within minutes; a discard bucket catches nothing in particular.

---

## Code Examples

### A DLQ Sink with Structured Metadata

The DLQ sink writes JSON-Lines to disk in the SDA pipeline's local filesystem; production deployments write to a Kafka topic with longer retention so re-processing tools can read from there. The local-disk version is sufficient for the lesson's purposes and matches the M3 L4 retract-aware sink's storage choice.

```rust,ignore
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DlqErrorKind {
    Deserialization,
    SchemaMismatch,
    ValidationFailed,
    ProcessingException,
    RetryBudgetExhausted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub schema_version: u32,
    pub timestamp_unix_ms: u64,
    pub operator: String,
    pub error_kind: DlqErrorKind,
    pub error_message: String,
    pub retry_count: u32,
    pub original_payload: Vec<u8>,
}

pub struct DlqSink {
    file_path: PathBuf,
    operator_name: String,
}

impl DlqSink {
    pub fn new(file_path: PathBuf, operator_name: impl Into<String>) -> Self {
        Self { file_path, operator_name: operator_name.into() }
    }

    /// Write a DLQ entry. Each entry is one JSON-Lines record.
    /// Append-only; never overwrites existing entries.
    pub async fn write(
        &self,
        kind: DlqErrorKind,
        error_message: impl Into<String>,
        retry_count: u32,
        original_payload: Vec<u8>,
    ) -> Result<()> {
        let entry = DlqEntry {
            schema_version: 1,
            timestamp_unix_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            operator: self.operator_name.clone(),
            error_kind: kind,
            error_message: error_message.into(),
            retry_count,
            original_payload,
        };
        let mut line = serde_json::to_vec(&entry)?;
        line.push(b'\n');

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .await?;
        file.write_all(&line).await?;
        // For SDA's reliability: sync after each write. Production with
        // higher DLQ rates batches multiple writes per fsync.
        file.sync_data().await?;
        Ok(())
    }
}
```

The append-only + per-write fsync is the durability discipline. A DLQ entry that is buffered in the kernel's page cache and not yet on disk is at risk if the process crashes. For the SDA pipeline's DLQ rates (single-digit entries per minute under steady state) the per-write sync cost is negligible. For higher-rate DLQs the cost matters and batching is appropriate; the standard pattern is "sync at most every N entries or every M milliseconds, whichever first." JSON-Lines as the format makes the file human-inspectable (`tail -f /path/to/dlq.jsonl | jq`) and machine-readable in one pass — useful for both ad-hoc investigation and the re-processing tool.

### An Operator That Routes to DLQ Based on RetryDisposition

The operator's error path. M2 L3's retry wrapper is extended: `RetryDisposition::Permanent(e)` triggers a DLQ write before the error propagates; `Discard` drops with a counter; `Retry` goes through the wrapper's backoff machinery as before.

```rust,ignore
use anyhow::Result;

pub async fn run_operator_with_dlq<F, Fut, T>(
    mut op: F,
    dlq: &DlqSink,
    policy: RetryPolicy,
    payload: Vec<u8>,  // raw payload bytes for DLQ
) -> Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RetryDisposition<T>>,
{
    use std::time::Duration;
    use tokio::time::sleep;

    let mut attempt = 0u32;
    let mut prev_delay = policy.initial;
    loop {
        attempt += 1;
        match op().await {
            RetryDisposition::Ok(v) => return Ok(Some(v)),
            RetryDisposition::Discard => {
                metrics::counter!("operator_discards_total").increment(1);
                return Ok(None);
            }
            RetryDisposition::Permanent(e) => {
                // Permanent — route to DLQ.
                dlq.write(
                    DlqErrorKind::ValidationFailed,
                    e.to_string(),
                    attempt - 1,
                    payload,
                ).await?;
                return Ok(None);  // operator continues to next event
            }
            RetryDisposition::Retry(e) if attempt >= policy.max_attempts => {
                // Retry budget exhausted — also DLQ.
                dlq.write(
                    DlqErrorKind::RetryBudgetExhausted,
                    e.to_string(),
                    attempt,
                    payload,
                ).await?;
                return Ok(None);
            }
            RetryDisposition::Retry(_) => {
                // Decorrelated jitter from M2 L3.
                let upper = (prev_delay.as_millis() as u64).saturating_mul(3)
                    .max(policy.initial.as_millis() as u64);
                let delay = Duration::from_millis(upper).min(policy.cap);
                prev_delay = delay;
                sleep(delay).await;
            }
        }
    }
}
```

Two things to notice. The `Permanent` arm and the `RetryBudgetExhausted` arm both DLQ but with different `error_kind` labels — the DLQ entry distinguishes "this is a permanent error type" from "this was transient but we couldn't make it work." Operations dashboards split on the label to prioritize different remediation patterns. The operator's `Result<Option<T>>` return makes "go to next event" explicit at the type level: `Ok(None)` means "this event was discarded or DLQ'd, move on"; `Ok(Some(v))` means "process this value"; `Err(e)` means "operator-level error, propagate to supervisor." The structure makes the operator's hot loop easy to read: `match op_result { Some(v) => process(v), None => continue }`.

### A Re-Processing Tool

The CLI tool that reads from the DLQ and re-injects events into the pipeline's input topic. It is a separate binary from the pipeline itself, designed for operational use after an underlying issue has been fixed.

```rust,ignore
use anyhow::Result;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn reprocess(
    dlq_path: PathBuf,
    pipeline_input_topic: &str,
    producer: &FutureProducer,
    filter: ReprocessFilter,
) -> Result<ReprocessReport> {
    let mut report = ReprocessReport::default();
    let file = File::open(&dlq_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        let entry: DlqEntry = serde_json::from_str(&line)?;
        if !filter.matches(&entry) {
            report.skipped += 1;
            continue;
        }
        // Push the original payload back into the pipeline's input.
        // The pipeline's idempotency machinery from L2 absorbs any
        // duplicates from prior partial processing.
        producer.send(
            FutureRecord::to(pipeline_input_topic)
                .payload(&entry.original_payload),
            std::time::Duration::from_secs(30),
        ).await
            .map_err(|(e, _)| anyhow::anyhow!("send failed: {e}"))?;
        report.reprocessed += 1;
    }
    Ok(report)
}

#[derive(Debug)]
pub struct ReprocessFilter {
    pub error_kinds: Vec<DlqErrorKind>,
    pub operators: Vec<String>,
    pub since_unix_ms: Option<u64>,
    pub until_unix_ms: Option<u64>,
}

impl ReprocessFilter {
    pub fn matches(&self, entry: &DlqEntry) -> bool {
        // Filter on error_kind, operator, time range. Empty filters
        // match everything.
        let kind_ok = self.error_kinds.is_empty() ||
            self.error_kinds.iter().any(|k| matches!((k, &entry.error_kind),
                (DlqErrorKind::Deserialization, DlqErrorKind::Deserialization) |
                (DlqErrorKind::SchemaMismatch, DlqErrorKind::SchemaMismatch) |
                (DlqErrorKind::ValidationFailed, DlqErrorKind::ValidationFailed) |
                (DlqErrorKind::ProcessingException, DlqErrorKind::ProcessingException) |
                (DlqErrorKind::RetryBudgetExhausted, DlqErrorKind::RetryBudgetExhausted)
            ));
        let op_ok = self.operators.is_empty() || self.operators.contains(&entry.operator);
        let since_ok = self.since_unix_ms.map_or(true, |s| entry.timestamp_unix_ms >= s);
        let until_ok = self.until_unix_ms.map_or(true, |u| entry.timestamp_unix_ms <= u);
        kind_ok && op_ok && since_ok && until_ok
    }
}

#[derive(Debug, Default)]
pub struct ReprocessReport {
    pub reprocessed: usize,
    pub skipped: usize,
}
```

The filter API matters operationally. A typical re-processing run targets "all `Deserialization` errors from operator `radar-ingest` between 14:00 and 15:30 yesterday" — the time when the partner's wire format change rolled out, and the time it was reverted. The filter narrows the re-processing to the affected window, avoiding re-injecting unrelated DLQ entries that might now succeed and produce unwanted side effects. The L2 idempotency is the safety net that makes the re-injection correct under any duplicate; the filter is the operational discipline that limits re-injection to what is intended.

---

## Key Takeaways

* **Three error categories**: transient (retry with backoff), permanent (DLQ), discardable (drop with metric). The classification is the operator's responsibility; the default for unknown errors is permanent (DLQ) so they surface for operational attention.
* **DLQ entries carry metadata**: timestamp, operator, error_kind, error_message, retry_count, original_payload, schema_version. The schema_version field is the migration mechanism for future re-processing tools that span DLQ entries from different pipeline versions.
* **Poison pills are the case the DLQ exists for**. Without a DLQ, a poison pill blocks the pipeline; the operator retries forever and makes no progress. With a DLQ, the pill is quarantined and the pipeline stays healthy.
* The **DLQ is also a re-processing source**. After an underlying issue is fixed, a re-processing tool reads from the DLQ and re-injects events into the pipeline. The L2 idempotency machinery absorbs any duplicates from prior partial processing.
* **The discard-bucket anti-pattern** is what makes a DLQ useless. The operational disciplines that prevent it: alert on growth rate, weekly review cadence, bounded retention. A well-managed DLQ catches partner API changes within minutes; a discard bucket catches nothing.

---

{{#quiz lesson-04-quiz.toml}}
