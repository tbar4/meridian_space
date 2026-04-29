# Module 05 — Delivery Guarantees and Fault Tolerance

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 5 of 6
**Source material:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 7 (Reliable Data Delivery), Chapter 8 (Exactly-Once Semantics), Chapter 9 (Failure Handling and Reprocessing); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Fault Tolerance, Microbatching and Checkpointing); *Database Internals* — Alex Petrov, Chapter 5 (Checkpointing in Recovery); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 (Error Handling and Dead-Letter Queues, Late-Arriving Data)
**Quiz pass threshold:** 70% on all four lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0207**
> **Classification:** RESTART-SAFETY HARDENING
> **Subject:** Make the windowed correlator crash-safe and the alert path exactly-once-effective
>
> Two months ago, a maintenance window required restarting the pipeline to apply a security patch. The orchestrator's graceful-drain logic worked correctly — every operator drained its incoming channel before exiting — but the alert subscriber had already received fourteen alerts that the new pipeline did not know about, and the new pipeline emitted six alerts that the subscriber had already acted on. Two false-positive collision-avoidance maneuvers were executed as a consequence. The postmortem identified two missing pieces: durable state on the producer side (so restart resumes from where the previous instance left off), and idempotent processing on the consumer side (so duplicate deliveries do not produce duplicate effects). This module installs both.

The pipeline at the start of this module handles steady-state load (M4), produces correct event-time results (M3), and is correctly orchestrated and supervised (M2). It has one structural blind spot: it loses data on a process restart. The windowed correlator's state is in process memory; the supervisor restarts a panicked operator with a fresh empty state; in-flight observations between operators are buffered in tokio channels that do not survive a process exit. Every restart loses some non-trivial amount of work, and the SDA-2026-0207 incident's failure mode is what happens when that lost work straddles a real-world action boundary like an alert subscriber.

Module 5 is the response. **At-least-once delivery** at the transport layer (Kafka producer with `acks=all` + retries; consumer with process-then-commit) gives the property "every observation reaches the consumer at least once, with duplicates as the operational cost." **Idempotency at the application layer** (sink-side dedup keyed on observation_id, idempotent SQL UPSERT, Kafka's idempotent producer) gives the property "duplicate deliveries produce identical effects on the world." The two together are *effective exactly-once* — the pipeline's net effect is exactly-once even though the underlying transport admits duplicates. **Checkpointing** captures the windowed correlator's state durably so restart resumes from a saved snapshot rather than rebuilding from scratch. **Dead-letter queues** route permanent errors to a separate sink with metadata so engineers can investigate and re-inject after fixes.

The mental model the module installs is the four-piece reliability discipline: (1) at-least-once at every transport boundary, (2) idempotency at every state-modifying boundary, (3) checkpointed state on every stateful operator, (4) DLQ for every permanent-error path with explicit re-processing tooling. Every streaming pipeline in production combines these four; the module's specifics are where the patterns meet SDA's actual workload.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Distinguish at-most-once, at-least-once, and exactly-once delivery semantics rigorously, and configure the Kafka producer/consumer pair for at-least-once
2. Compose at-least-once delivery with application-layer idempotency to produce effective exactly-once processing, and recognize where the guarantee holds versus where boundary owners must implement their own dedup
3. Implement bounded sliding-window dedup sets keyed on natural or derived idempotency keys, with the production-safety double-bound (time AND count)
4. Configure Kafka's idempotent producer (`enable.idempotence=true`) and reason about its partition-scoped guarantee versus transactional Kafka's cross-partition guarantee
5. Implement checkpointing of stateful operators with the State+Offset recovery contract, atomic temp-file + rename writes, and the pause-snapshot-resume protocol via credit-withholding
6. Choose between aligned (Flink-style barriers) and per-operator checkpoints based on the pipeline's idempotency machinery and operational tradeoffs
7. Classify operator errors into transient/permanent/discardable and route each to the appropriate destination (retry/DLQ/drop with counter), with DLQ entries carrying schema-versioned metadata for re-processing tools
8. Recognize the discard-bucket anti-pattern and apply the operational disciplines (alert on growth rate, periodic review, bounded retention) that prevent it

---

## Lesson Summary

### Lesson 1 — At-Least-Once Delivery

The three levels of delivery guarantee (at-most-once, at-least-once, exactly-once) and the operational tradeoffs each implies. Producer-side at-least-once via `acks=all` + retries + `max.in.flight` (with the ordering caveat). Consumer-side at-least-once via process-then-commit and `enable.auto.commit=false`. Three sources of duplicates (producer retries after partial success, consumer crashes between process and commit, rebalances during processing). The cost of duplicates is proportional to the duplicate rate × per-event downstream cost; for SDA's alert-triggering subscriber, this drives the discipline.

*Key question:* A consumer is configured with `enable.auto.commit=true`. The application crashes after auto-commit fired but before the application processed the messages whose offsets it committed. What happens, and what is the lesson's recommended fix?

### Lesson 2 — Exactly-Once via Idempotency

Idempotency as the application-layer property that composes with at-least-once delivery into effective exactly-once. Natural keys (observation_id) versus derived keys (sorted content-addressable hash). Bounded dedup sets with the double-bound (time AND count) for production safety. Kafka's `enable.idempotence=true` as broker-side PID + sequence dedup, partition-scoped. Where the guarantee holds (within the pipeline, at idempotent SQL/Kafka/HTTP-with-key boundaries) versus where boundary owners must implement dedup themselves (alerts that trigger external actions).

*Key question:* The pipeline emits `ConjunctionRisk` events to a non-idempotent webhook that triggers an email to a human operator. Does the at-least-once + idempotency composition give effective exactly-once at the email side, and what is the framework for thinking about it?

### Lesson 3 — Checkpointing

Checkpointing as the durable-state mechanism that lets a restarted operator resume from a saved State+Offset rather than rebuilding from scratch. The pause-snapshot-resume protocol via credit-withholding from M4 L2. Aligned (Flink barriers) versus per-operator checkpoints — SDA uses per-operator with idempotency-driven recovery for global consistency. Storage tiers: local fast NVMe primary, remote object-storage durable replicate, hybrid as production default. Atomic temp-file + rename for all-or-nothing durability.

*Key question:* The teammate proposes storing only the operator's serialized state in the checkpoint, omitting the offset on the grounds that 'the consumer's committed offset is already durable.' Why does the lesson reject this and insist on the State+Offset pair?

### Lesson 4 — Dead Letter Queues

The DLQ pattern. Three error categories (transient/retry, permanent/DLQ, discardable/drop). DLQ metadata as the debug-tool foundation: timestamp, operator, error_kind, error_message, retry_count, original_payload, schema_version. Poison pills as the canonical case the DLQ exists for. The DLQ as a re-processing source after underlying issues are fixed, absorbed by L2's idempotency machinery. Three operational disciplines that prevent the discard-bucket anti-pattern: alert on growth rate, periodic review cadence, bounded retention.

*Key question:* Operations sees a sudden 40x spike in `dlq_entries_total{error_kind="Deserialization"}` from a single operator. What does the DLQ's schema design support as a first-look diagnosis, and what is the typical resolution path?

---

## Capstone Project — Exactly-Once Conjunction Alert Pipeline

Make the M4 hardened pipeline crash-safe and exactly-once-effective. The windowed correlator becomes a `CheckpointingOperator` with 30-second cadence, atomic writes, and local-first/remote-second/fresh-third recovery. The alert sink uses the L2 `DedupSet` keyed on alert_id. Kafka consumer reconfigures for process-then-commit. Permanent errors route to a DLQ with the L4 schema; an `sda-reprocess` CLI tool re-injects DLQ entries with filter and dry-run support. Three crash tests (mid-process, mid-checkpoint, mid-emit) assert post-restart correctness. Acceptance criteria, suggested architecture, deterministic crash-test patterns, and the full project brief are in `project-exactly-once-alerts.md`.

The orchestrator from M2, the windowed correlator from M3, and the priority-aware shedding from M4 are all unchanged in structure. The new operators wrap or extend the existing pieces; the operator graph grows by a few nodes (DLQ sink, alert subscriber boundary state).

---

## File Index

```
module-05-delivery-guarantees-and-fault-tolerance/
├── README.md                                         ← this file
├── lesson-01-at-least-once.md                        ← At-least-once delivery
├── lesson-01-quiz.toml                               ← Quiz (5 questions)
├── lesson-02-exactly-once-idempotency.md             ← Exactly-once via idempotency
├── lesson-02-quiz.toml                               ← Quiz (5 questions)
├── lesson-03-checkpointing.md                        ← Checkpointing
├── lesson-03-quiz.toml                               ← Quiz (5 questions)
├── lesson-04-dead-letter-queues.md                   ← Dead letter queues
├── lesson-04-quiz.toml                               ← Quiz (5 questions)
└── project-exactly-once-alerts.md                    ← Capstone project brief
```

---

## Prerequisites

* Modules 1 through 4 completed — the `Observation` envelope, the `OperatorGraph` with supervisor, the watermark-aware windowed correlator, and the per-edge `FlowPolicy` discipline are all assumed
* Foundation Track completed — async Rust, channels, runtime intuitions
* Familiarity with `tokio::sync::mpsc`, `tokio::fs` (for atomic-rename writes), `serde` (for state serialization), `bincode` (for compact binary checkpoint format), and the `rdkafka` Rust client's producer/consumer APIs
* Comfort reading Kafka's producer configuration documentation (`acks`, `retries`, `enable.idempotence`, `max.in.flight.requests.per.connection`) and consumer configuration (`enable.auto.commit`, commit modes)

## What Comes Next

Module 6 (Observability and Lineage) makes the pipeline's correctness *visible* to operations. The new metrics from this module — `checkpoint_age_seconds`, `dlq_entries_total`, `recovery_path_total` — become panels on the resilience dashboard. The runbook discipline established here (per-error-kind playbooks, re-processing protocols) becomes part of the on-call rotation's standard procedure. The patterns this module installed — at-least-once + idempotent + checkpointed + DLQ'd — generalize beyond SDA to any streaming system that must survive restarts; M6 develops the observability stack that makes those patterns operationally legible.

The pipeline at the end of this module is correct under load, correct across restart, correct in event time, and correctly orchestrated. Module 6 turns "correct" into "correct AND visible," which is the difference between a system that works and a system that operations can trust.
