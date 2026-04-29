# Capstone Project — Exactly-Once Conjunction Alert Pipeline

**Module:** Data Pipelines — M05: Delivery Guarantees and Fault Tolerance
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All four lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0207 / Phase 5 Implementation**
> **Classification:** RESTART-SAFETY HARDENING
>
> Two months ago, a maintenance window required restarting the SDA
> pipeline to apply a security patch. The orchestrator's graceful-drain
> logic worked correctly — every operator drained its incoming channel
> before exiting — but the alert subscriber had already received
> fourteen alerts that the new pipeline did not know about, and the
> new pipeline emitted six alerts that the subscriber had already acted
> on. Two false-positive collision-avoidance maneuvers were executed
> as a consequence. The postmortem identified two missing pieces:
> durable state on the producer side (so restart resumes from where
> the previous instance left off), and idempotent processing on the
> consumer side (so duplicate deliveries do not produce duplicate
> effects).
>
> Phase 5 installs both. The windowed correlator from M3 becomes
> crash-safe via periodic checkpoints. The alert-emit path becomes
> idempotent end-to-end via the M5 L2 dedup machinery extended to the
> subscriber boundary. Permanent errors route to a DLQ with metadata
> sufficient for re-processing after underlying issues are fixed.
>
> Success criteria for Phase 5: a deliberate kill -9 of the pipeline
> at three different points (mid-process, mid-checkpoint, mid-emit)
> followed by restart produces an alert log with every alert exactly
> once. The 30-second alert SLO is held throughout the test. The DLQ
> captures permanent errors with full metadata; the re-processing
> tool re-injects DLQ entries without producing duplicate alerts.

---

## What You're Building

Make the M4 hardened pipeline crash-safe and exactly-once-effective.

1. The windowed correlator from M3 becomes a `CheckpointingOperator` (L3 pattern): periodic checkpoints write its sliding-window state plus the consumer offset to local NVMe, async-replicated to S3
2. The alert sink uses the L2 `DedupSet` keyed on alert_id with a 5-minute window and 100K capacity bound
3. The Kafka consumer is configured for at-least-once (L1: `acks=all`, `enable.idempotence=true`, `enable.auto.commit=false`, process-then-commit)
4. The alert subscriber boundary stores recently-seen alert_ids in a small embedded SQLite (durable across the subscriber's own restarts)
5. Operators classify errors per L4: transient → retry, permanent → DLQ, invariant-violation → discard
6. A DLQ sink writes JSON-Lines to local disk with the L4 metadata schema
7. A re-processing CLI tool (`sda-reprocess`) reads from the DLQ and re-injects events into the pipeline's input topic

The orchestrator from M2, the windowed correlator from M3, and the priority-aware shedding from M4 are all unchanged in structure. The new operators wrap or extend the existing pieces; the operator graph declaration grows by a few nodes (DLQ sink, alert subscriber boundary state).

---

## Suggested Architecture

```
                  ┌─────────────────────┐
                  │  Kafka input topic  │
                  │  (consumer offset   │
                  │   committed         │
                  │   process-then-     │
                  │   commit)           │
                  └──────────┬──────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │ Source operators (radar,     │
              │ optical, ISL) wrapped with   │
              │ retry + DLQ classifier       │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │ Normalize fan-in (M3 L1)     │
              └──────────────┬───────────────┘
                             │ ──── credit channel from L2
                             ▼
              ┌──────────────────────────────┐
              │ Windowed Correlator          │
              │ (M3) wrapped as              │
              │ CheckpointingOperator        │
              │ - state: sliding windows     │
              │ - offset: consumer commit    │
              │ - cadence: 30s               │
              │ - storage: local + S3        │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │ Alert Sink (M2 L3 dedup +   │
              │ M3 L4 retract-aware)        │
              │ + 5-min DedupSet 100K bound  │
              └──────────────┬───────────────┘
                             │ alerts
                             ▼
              ┌──────────────────────────────┐
              │ Alert Subscriber Boundary    │
              │ (embedded SQLite seen-set)   │
              └──────────────────────────────┘

   Side paths:                                  Out-of-band:
   ┌──────────┐    ┌──────────┐                 ┌──────────────┐
   │   DLQ    │◄───│ Operator │                 │ sda-reprocess│
   │  Sink    │    │ classifier│                │  CLI tool    │
   └──────────┘    └──────────┘                 └──────┬───────┘
                                                       │
                                                       ▼
                                              re-inject into Kafka
                                              input topic
```

---

## Acceptance Criteria

### Functional Requirements

- [x] Kafka consumer configured per L1: `enable.auto.commit=false`, `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5` (idempotent producer makes higher in-flight safe)
- [x] Process-then-commit ordering with explicit `commit_message(..., CommitMode::Sync)` after each batch
- [x] Source-side internal log: every observation is durably recorded in a per-source append-only file with its consumer offset before being emitted to downstream
- [x] Sink-side `DedupSet` (5-minute time bound, 100K capacity bound) on alert_id; duplicates absorbed silently
- [x] `CheckpointingOperator` wrapping the windowed correlator: 30-second cadence, atomic temp-file + rename writes, local NVMe primary + S3 async replicate
- [x] On restart: load latest local checkpoint if present; fall back to S3 if local is missing; fall back to fresh state if neither
- [x] DLQ sink with the L4 schema (`schema_version=1`); per-error-kind classification by every operator
- [x] `sda-reprocess` CLI tool with filter args (`--error-kind`, `--operator`, `--since`, `--until`) and a dry-run mode that prints what would be re-injected without sending

### Quality Requirements

- [x] **Three crash tests** in the integration test suite, each with `kill -9` at a different point: (a) mid-process (between consumer recv and sink write), (b) mid-checkpoint (during the checkpoint flush), (c) mid-emit (after sink write but before commit). Each test asserts the post-restart alert log contains every alert exactly once.
- [x] Checkpoint pause duration measured per snapshot; the histogram's P99 is below 200ms (the SLO budget). Performance test asserts this on representative load.
- [x] DLQ schema versioned at write; the re-processing tool dispatches on `schema_version` and supports the historical versions documented in the codebase
- [x] No `.unwrap()` or `.expect()` in non-startup code paths; all errors propagate to the operator's classifier

### Operational Requirements

- [x] `/metrics` extends M4's with: `checkpoint_age_seconds` (gauge per operator), `checkpoint_size_bytes` (gauge), `checkpoint_pause_duration_ms` (histogram), `dlq_entries_total{operator, error_kind}` (counter), `recovery_path_total{path}` (counter for local/remote/fresh on each startup)
- [x] Alert when `checkpoint_age_seconds > 2 × cadence` (a stalled checkpoint indicates a problem)
- [x] Alert when `dlq_entries_total{error_kind="Deserialization"}` rate > 10× baseline for >5 minutes (partner schema change canary)
- [x] DLQ runbook: per-error-kind playbook documenting investigation steps and remediation patterns (entry, hypothesis, validation, fix, re-processing decision)

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* Recovery time from a 100MB checkpoint is under 5 seconds end-to-end (load + deserialize + resume + first event emitted)
- [ ] *(self-assessed)* The re-processing tool handles 10K events without producing any new alerts (per the L4 lesson's exemplary "zero new effects" outcome). Demonstrated via a synthetic DLQ window from the integration tests.
- [ ] *(self-assessed)* The pipeline's restart-recovery test runs as a chaos-engineering integration test in CI, killing the pipeline at random points across 100 iterations; asserts every iteration ends with a consistent alert log

---

## Hints

<details>

<summary>How do I serialize the windowed correlator's per-key sliding windows efficiently?</summary>

The natural representation is a `BTreeMap<KeyType, VecDeque<Observation>>`. `bincode::serialize` on this produces a compact binary format suitable for the checkpoint write. Pre-allocate the temp-file with a reasonable size hint to avoid re-allocation during the write. For the 30-second window at SDA's load (~10K observations/sec), the serialized state is in the tens of megabytes — well within the 200ms pause budget when written to NVMe.

```rust,ignore
#[derive(Serialize, Deserialize)]
struct CorrelatorState {
    windows: BTreeMap<ObjectIdPair, VecDeque<Observation>>,
    last_offset: u64,
}

let bytes = bincode::serialize(&state)?;  // typically <50MB
fs::write(&tmp_path, bytes).await?;
fs::rename(&tmp_path, &final_path).await?;
```

The serialization can be parallelized for very large states: split the `BTreeMap` into chunks, serialize each chunk in parallel via `rayon`, concatenate. For SDA's scale this is unnecessary; the bincode serialize is single-digit microseconds per MB.

</details>

<details>

<summary>How do I inject crash points deterministically in tests?</summary>

The L1 test harness pattern with the `CrashingSink` is the foundation. Extend it: the operator's hot loop has a `#[cfg(test)]`-gated `crash_after: Option<u32>` field that panics after N successful events. The integration test sets `crash_after = Some(N)`, runs the pipeline, asserts the panic was caught by the supervisor, then runs a second instance from the same Kafka topic and asserts the recovery completed correctly.

```rust,ignore
#[cfg(test)]
async fn process_event(&mut self, ev: Event) -> Result<()> {
    self.events_processed += 1;
    if let Some(crash_at) = self.crash_after {
        if self.events_processed == crash_at {
            panic!("test-injected crash at event {}", self.events_processed);
        }
    }
    // ... actual processing
}
```

Combine with `tokio::time::pause()` (M2 L4 pattern) so the test runs in fast-forward without real wall-clock delays. The whole crash-recover-verify cycle should run in under a second for CI suitability.

</details>

<details>

<summary>How do I version the DLQ schema with serde tagged unions?</summary>

The simplest pattern is to use serde's `#[serde(tag = "schema_version")]` with a sum-type enum that wraps each version's struct. The reader dispatches on the tag automatically:

```rust,ignore
#[derive(Serialize, Deserialize)]
#[serde(tag = "schema_version")]
enum DlqEntryAnyVersion {
    #[serde(rename = "1")]
    V1(DlqEntryV1),
    #[serde(rename = "2")]
    V2(DlqEntryV2),
    // ... future versions
}
```

The writer always emits the latest version. The reader's `serde_json::from_str::<DlqEntryAnyVersion>(line)` automatically dispatches based on the `schema_version` field in the JSON. New versions add a variant; old code that doesn't know about the new version returns an error on deserialization, which can be handled gracefully (skip with metric).

The `DlqEntryV2` struct evolves backwards-compatibility — keep field names where possible, add new ones as `Option<T>` for graceful upgrade. Production tooling should provide a migrate-old-to-new tool that reads V1 entries and writes V2 entries; the re-processing tool reads either.

</details>

<details>

<summary>How do I size the dedup window and capacity for the alert sink?</summary>

The window must exceed the maximum re-delivery window. For SDA's pipeline, the dominant re-delivery source is checkpoint replay during restart: a checkpoint at cadence 30s means the pipeline can replay up to 30s of events (the time between the latest checkpoint and the crash). Add a safety factor — 5 minutes is comfortable. The capacity bound is the safety valve for bursts; during the 10x burst test from M4, the sink's incoming rate peaks at ~50K alerts/sec briefly, so a 100K capacity covers a 2-second peak comfortably.

The numbers are operational: tune based on actual observed re-delivery rates and burst characteristics. Document the chosen values with a `BurstProfile`-style comment in the topology declaration.

</details>

<details>

<summary>How do I test the re-processor without polluting the live pipeline?</summary>

A `--target-topic test-mode-input` argument that lets the tool point at a test topic instead of production. The integration test uses this flag; production runs use the default (the live input topic). The test asserts events landed in the test topic (via a test-mode consumer) without affecting the production state.

For dry-run validation in production, the `--dry-run` flag has the tool print what it would re-inject without actually sending. Operations uses dry-run before any production re-injection to confirm the filter is targeting the right window.

</details>

---

## Getting Started

Recommended order:

1. **Source-side internal log.** A per-source append-only file that records every observation with its Kafka offset before emitting downstream. The recovery story works only when this is durable.
2. **Sink-side `DedupSet` on alert_id.** L2's pattern; double-bound (time + count). Verify with a test that injects the same alert_id twice and asserts only one downstream emit.
3. **Kafka consumer reconfiguration**. Switch from auto-commit to process-then-commit. Verify with the L1 crash test: kill between process and commit, restart, observe the redelivery.
4. **`CheckpointingOperator` wrapping the windowed correlator.** L3's pattern; atomic temp-file + rename; 30-second cadence to start.
5. **Recovery routine on startup.** Local-first, S3-second, fresh-third hierarchy.
6. **DLQ sink + per-operator classifier.** L4's pattern; classify each error explicitly; route to DLQ with metadata.
7. **`sda-reprocess` CLI tool.** Read from DLQ, filter, re-inject. Test against a synthetic DLQ.
8. **Three crash tests in the integration suite.** Mid-process, mid-checkpoint, mid-emit. Assert every alert lands exactly once.

Aim for the first crash test passing by day 5 (consumer reconfig + checkpoint + recovery). The DLQ and re-processor land in the second week along with the operational runbook. The chaos-engineering stretch goal is an end-of-week-2 polish if time permits.

---

## What This Module Sets Up

In Module 6 you will surface the new metrics — checkpoint_age, dlq_entries_total, recovery_path — as the operational dashboard's resilience panels. The runbook discipline you establish here (per-error-kind playbooks, re-processing protocols) becomes part of the on-call rotation's standard procedure. The audit script from M4 extends to verify every operator has a documented retry-disposition classifier and DLQ wiring.

The pipeline at the end of this module is correct under load (M4), correct across restart (M5), correct in event time (M3), and correctly orchestrated (M2). It produces output that downstream subscribers can trust to be exactly-once-effective. M6's work is making that correctness *visible* to operations — the dashboards, lineage, distributed tracing, and SLO monitoring that turn a correct pipeline into an operationally-legible one.

This is the module where the SDA Fusion Service crosses from "works under happy paths" to "works through real production failure modes." The patterns generalize beyond SDA to any streaming system that must survive restarts: at-least-once + idempotent + checkpointed + DLQ'd is the canonical streaming-pipeline reliability stack.
