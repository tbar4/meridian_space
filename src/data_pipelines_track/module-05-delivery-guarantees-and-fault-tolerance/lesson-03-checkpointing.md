# Lesson 3 — Checkpointing

**Module:** Data Pipelines — M05: Delivery Guarantees and Fault Tolerance
**Position:** Lesson 3 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Fault Tolerance — Microbatching and Checkpointing); *Database Internals* — Alex Petrov, Chapter 5 (Checkpointing in Recovery — the same conceptual machinery applied to streaming state)

---

## Context

Lessons 1 and 2 produced a pipeline that delivers messages at-least-once and processes them with effective exactly-once semantics via idempotency. Both rely on durable state at the *boundary* — the SQL UPSERT, the Kafka idempotent producer's PID+sequence state, the consumer's committed offset. The state *inside* the pipeline — the windowed correlator's per-key sliding windows, the M3 retract-aware sink's in-flight retained windows, the M2 supervisor's restart history — lives in process memory and disappears on restart.

For most operators that does not matter. A normalize stateless map operator restarts cleanly and resumes by reading from the consumer offset. The orchestrator's supervisor restarts a panicked operator with a fresh Task, which means a fresh empty state, which is exactly right for stateless operators because their state IS empty by definition. For *stateful* operators — the windowed correlator is the canonical case — restart means losing the current windows in flight. The pipeline restarts; the input replay from the consumer offset begins; the windows that had been accumulating before the crash are gone, and the replay rebuilds them from scratch.

The cost is real. A 30-second window's worth of in-flight events takes 30 seconds of replay to reconstruct. During that 30 seconds, the pipeline is producing alerts whose source data is incomplete — the correlator has not yet seen all the observations that should be in its window because they are in the past, before the consumer offset's restart point. Module 5's mission framing (the SDA-2026-0207 incident) had exactly this shape: the restart's replay window straddled an active conjunction event, the windows were rebuilt without the early observations of that event, and the resulting alerts were emitted with degraded confidence.

This lesson installs **checkpointing** — the durable-state mechanism that lets a restarted operator resume from a saved state plus the input offset that produced it, without replaying from scratch. Database engines use checkpointing in WAL recovery (Petrov Chapter 5); Spark and Flink use it for streaming-state recovery; the SDA pipeline uses it for the same purpose at the operator level. The pattern is the same in every system: pause the operator briefly, snapshot its state to durable storage, record the input offset that the snapshot reflects, resume. On restart, load the latest snapshot, set the input offset, resume processing. The window of data lost on restart shrinks from "since process start" to "since last checkpoint."

---

## Core Concepts

### State + Offset = Checkpoint

A checkpoint is the durable record of an operator's recoverable progress. It has two components.

**State**: the operator's in-memory state at the moment the checkpoint was taken. For the windowed correlator, this is the per-key sliding windows. For the retract-aware sink, this is the recently-emitted (window_id, sequence) records. For a stateful aggregator, this is the running aggregations. The state is whatever the operator needs to resume processing without losing data.

**Offset**: the position in the input stream that the checkpointed state reflects. For a Kafka consumer, this is the broker offset of the last fully-processed message. For a file-based source, this is the byte offset. The offset is the link between the state and the input — it answers "if I restart with this state, where do I start reading from?"

The two together are the recovery contract: load the state, set the offset, resume. Either one alone is insufficient. A state without an offset is a snapshot of indeterminate vintage; resuming from it produces incorrect output because the input position is unknown. An offset without a state is a position to resume reading from, but the operator's running aggregations are gone — the pipeline replays correctly only if the operator is stateless. The capstone enforces the State+Offset invariant structurally: every checkpoint write atomically includes both, and every checkpoint read returns both or fails.

### Pause-Snapshot-Resume Protocol

The simplest checkpoint protocol: pause the operator's input for the duration of the checkpoint, write the state to durable storage along with the current offset, resume input. The pause is what guarantees the State+Offset pair is consistent — no new input arrives during the snapshot, so the state and the offset reflect the same point in the stream.

The credit-based-flow primitive from Module 4 L2 is the natural mechanism for the pause. The operator withholds credits from its upstream during the snapshot; the upstream's local credit counter drains; the upstream stops producing without occupying any in-flight slot. The snapshot completes; the operator returns credits; the upstream resumes. The duration of the pause is the snapshot's wall-clock cost, typically 50-200ms for SDA-scale operator state.

The cost of the pause is end-to-end latency. During the pause, no observations flow through the operator; downstream operators see a brief gap in their input. For SDA's 30-second alert SLO, a 200ms pause is 0.7% of the budget — acceptable. For tighter SLOs the pause must be tighter; production checkpoints write to local fast storage (NVMe, RAM disk) to keep the pause sub-millisecond. The discipline: choose checkpoint frequency and pause duration to fit the SLO budget.

### Aligned Checkpoints (Flink's Approach)

For a single-operator checkpoint the pause-snapshot-resume protocol is sufficient. For a multi-operator pipeline, the operators' checkpoints must be *aligned* — the saved state across operators must reflect the same point in the input stream — or the recovery produces inconsistent results (operator A resumes from input position X, operator B resumes from input position X+10, the pipeline's overall state is incoherent).

Flink's solution is **barrier alignment**. The orchestrator injects a special *checkpoint barrier* marker into the source streams. The barrier flows through the pipeline as a normal event; when an operator receives a barrier on every input, it snapshots its current state and forwards the barrier to its downstream. The barrier reaches the next operator, which waits until it has the barrier on every input, snapshots, forwards. The structure produces a *consistent cut* across the pipeline: every operator's snapshot reflects the same set of input events.

The cost is operationally real. The slowest input's barrier delay determines the alignment time — operators wait at the barrier until every input has reported. A slow input stalls the entire checkpoint. For SDA's pipeline with three sources at very different rates (radar 5000/sec, ISL 100/sec, optical 50/sec) the alignment is dominated by the slow inputs. Flink offers an *unaligned* mode that trades consistency for speed; the SDA pipeline uses aligned checkpoints because the consistency property is what the recovery contract relies on.

### Per-Operator (Unaligned) Checkpoints

The simpler alternative: each operator checkpoints on its own schedule, with no cross-operator alignment. Each operator's checkpoint reflects only its own state and the position in *its* input stream. Recovery is then per-operator: each operator independently loads its latest checkpoint and resumes from its checkpointed offset.

The simplification has a real cost: cross-operator state is no longer consistent. If operator A checkpointed at position X and operator B (downstream of A) checkpointed at position Y > X, then after a restart, A starts at X but B starts at Y. A re-emits messages in (X, Y]; B has already processed them and dedups via idempotency. This works correctly under the at-least-once + idempotent composition from Lessons 1 and 2 — duplicates are absorbed.

For SDA's pipeline, per-operator checkpointing is the right shape because the idempotency machinery is already in place. The pipeline does not need the strong consistency that aligned checkpoints provide; it needs each operator to recover its own state, and the global consistency is recovered via dedup at every boundary. The capstone uses per-operator checkpoints; the lesson covers aligned checkpoints for completeness because they show up in production streaming systems and the framing matters when those systems are introduced.

### Checkpoint Storage

The state must go somewhere durable. Three tiers, each with its own cost profile.

**Local disk (NVMe / SSD).** Fastest write latency (~100µs for small writes). The natural choice for the checkpoint's hot-path destination. Limitation: a host failure loses the checkpoint along with the host. For pipeline restart-without-host-loss (the common case), local-disk checkpoints suffice.

**Remote object storage (S3, GCS).** Slower write latency (10-100ms). Survives host loss because the storage is redundant across availability zones. The natural choice for *durable* checkpoints that must survive worst-case failures.

**Hybrid: local-then-async-replicated-to-remote.** The standard production pattern. Write to local first (fast, on the hot path); replicate asynchronously to remote (durable, off the hot path). The pause-snapshot-resume cost is bounded by the local write; the remote-replication catches up in the background. On restart, prefer the local checkpoint if it exists (fast recovery); fall back to remote if it doesn't (host loss recovery).

The choice is operational. For SDA's pipeline, the hybrid approach with a 1-second local checkpoint cadence and 60-second remote replication is the production default — adjustable per operator based on the state size and the recovery time budget. The capstone exposes the cadence and replication parameters per operator; ops tuning happens in the orchestrator's startup configuration rather than in code.

---

## Code Examples

### A Periodic-Checkpoint Operator with Local Disk

The simplest implementation: every N seconds, pause the input via credit-withholding, serialize the state, write to disk, resume. On startup, look for the latest checkpoint and restore.

```rust,ignore
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorCheckpoint<S> {
    /// The operator's serialized state at checkpoint time.
    pub state: S,
    /// The input-stream offset that the state reflects.
    pub offset: u64,
    /// When the checkpoint was taken (for diagnostics).
    pub taken_at_unix_ms: u64,
}

/// A checkpointing wrapper around any stateful operator. The operator
/// implements a Checkpointable trait that exposes serialize/restore
/// hooks; the wrapper handles the pause-snapshot-resume protocol and
/// the disk I/O.
pub struct CheckpointingOperator<S> {
    state: S,
    last_offset: u64,
    interval: Duration,
    checkpoint_dir: PathBuf,
    operator_name: String,
}

impl<S> CheckpointingOperator<S>
where
    S: Serialize + for<'de> Deserialize<'de> + Default,
{
    /// Build a fresh operator OR restore from the latest checkpoint
    /// if one exists. The constructor does the recovery.
    pub async fn new_or_restore(
        operator_name: impl Into<String>,
        interval: Duration,
        checkpoint_dir: PathBuf,
    ) -> Result<Self> {
        let operator_name = operator_name.into();
        let path = checkpoint_dir.join(format!("{operator_name}.bin"));
        let (state, last_offset) = if path.exists() {
            let bytes = fs::read(&path).await?;
            let cp: OperatorCheckpoint<S> = bincode::deserialize(&bytes)?;
            tracing::info!(
                operator = %operator_name,
                offset = cp.offset,
                "restored from checkpoint"
            );
            (cp.state, cp.offset)
        } else {
            tracing::info!(operator = %operator_name, "no checkpoint found; starting fresh");
            (S::default(), 0)
        };
        Ok(Self {
            state,
            last_offset,
            interval,
            checkpoint_dir,
            operator_name,
        })
    }

    /// Take a checkpoint NOW. Caller is responsible for pausing input
    /// (e.g., via credit-withholding) before invocation; this method
    /// is just the snapshot and write.
    pub async fn checkpoint(&self) -> Result<()> {
        let path = self.checkpoint_dir.join(format!("{}.bin", self.operator_name));
        let cp = OperatorCheckpoint {
            state: &self.state,
            offset: self.last_offset,
            taken_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        // Write to a temp file first, then atomically rename. The rename
        // is the durability barrier — the file appears at its final path
        // only when the write is complete, so a crash during write
        // leaves the previous (consistent) checkpoint intact.
        let tmp_path = path.with_extension("bin.tmp");
        let bytes = bincode::serialize(&cp)?;
        fs::write(&tmp_path, bytes).await?;
        fs::rename(&tmp_path, &path).await?;
        Ok(())
    }
}
```

The atomic-rename pattern is the durability discipline. Writing directly to the final path leaves the file in a partial state if the process crashes mid-write; the next startup reads a corrupt file and either fails to deserialize or (worse) deserializes an inconsistent State+Offset pair. The rename is atomic at the filesystem level (POSIX `rename` is atomic for files in the same directory), so the file is either the previous good checkpoint or the new good checkpoint, never a partial state. Production code uses `fs::sync_data` or `fsync` before the rename to ensure the write is durable past a power failure; we elide that for clarity.

### A Barrier-Based Coordinator (Aligned Checkpoint)

For aligned checkpoints, the orchestrator injects a barrier marker into source streams. Each operator forwards the barrier after snapshotting; the coordinator waits for all operators' barrier acknowledgments to declare the checkpoint complete.

```rust,ignore
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// A barrier marker that flows through the pipeline as a control
/// item. Operators forward it after taking their checkpoint.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointBarrier(pub u64); // checkpoint id

/// The orchestrator's coordinator. Tracks barrier acknowledgments from
/// every operator; the checkpoint is complete when every operator has
/// acked the barrier with the same checkpoint id.
pub struct CheckpointCoordinator {
    expected_acks: usize,
    received_acks: Arc<Mutex<HashMap<u64, HashSet<String>>>>,
    completion: Arc<Notify>,
}

impl CheckpointCoordinator {
    /// Inject a barrier into the source streams and wait for all
    /// operators to ack.
    pub async fn run_checkpoint(
        &self,
        cp_id: u64,
        sources: &[mpsc::Sender<SourceItem>],
    ) -> Result<()> {
        // Reset ack state for this checkpoint.
        self.received_acks.lock().unwrap().entry(cp_id).or_default();
        // Inject the barrier into every source stream.
        for src in sources {
            src.send(SourceItem::Barrier(cp_id)).await?;
        }
        // Wait for all operators to ack.
        loop {
            self.completion.notified().await;
            let acks = self.received_acks.lock().unwrap();
            if let Some(set) = acks.get(&cp_id) {
                if set.len() >= self.expected_acks {
                    return Ok(());
                }
            }
        }
    }

    /// Operator-side ack: called when an operator has snapshotted in
    /// response to a barrier and is forwarding it downstream.
    pub fn ack(&self, cp_id: u64, operator_name: &str) {
        let mut acks = self.received_acks.lock().unwrap();
        acks.entry(cp_id).or_default().insert(operator_name.to_string());
        drop(acks);
        self.completion.notify_waiters();
    }
}
```

The barrier mechanism is more complex than the per-operator pattern but produces the consistent-cut property the framework guarantees. The cost is operational: the coordinator is a centralized component that the supervisor must keep alive, the barrier protocol must be implemented in every operator, and the slowest input stalls the entire checkpoint. Production systems that use aligned checkpointing (Flink's defaults) accept this complexity in exchange for cross-operator consistency. The lesson covers it for completeness; SDA's pipeline uses the simpler per-operator approach.

### Recovery from a Checkpoint at Startup

The startup path that calls `new_or_restore` for every checkpointing operator. The orchestrator's bootstrap logic looks for local checkpoints first, falls back to remote, defaults to fresh state if neither exists.

```rust,ignore
pub async fn bootstrap_pipeline(
    config: &PipelineConfig,
) -> Result<RunningPipeline> {
    let local_dir = &config.checkpoint_dir;
    let remote_dir = &config.remote_checkpoint_uri;
    let mut operators: Vec<CheckpointingOperator<_>> = Vec::new();

    for op_spec in &config.operators {
        // Try local first. If nothing exists locally, try remote.
        // If nothing exists in either, start fresh.
        let local_path = local_dir.join(format!("{}.bin", op_spec.name));
        let restored = if local_path.exists() {
            CheckpointingOperator::new_or_restore(
                &op_spec.name,
                op_spec.checkpoint_interval,
                local_dir.clone(),
            ).await?
        } else if remote_exists(remote_dir, &op_spec.name).await? {
            tracing::info!(operator = %op_spec.name, "local missing; pulling from remote");
            pull_remote_to_local(remote_dir, local_dir, &op_spec.name).await?;
            CheckpointingOperator::new_or_restore(
                &op_spec.name,
                op_spec.checkpoint_interval,
                local_dir.clone(),
            ).await?
        } else {
            tracing::warn!(operator = %op_spec.name, "no checkpoint anywhere; starting fresh");
            CheckpointingOperator::new_or_restore(
                &op_spec.name,
                op_spec.checkpoint_interval,
                local_dir.clone(),
            ).await?
        };
        operators.push(restored);
    }

    Ok(RunningPipeline { operators, /* ... */ })
}
```

The local-then-remote-then-fresh hierarchy is what gives the pipeline different recovery profiles for different failure modes. A normal restart (process exit, redeploy) recovers from local and is fast (sub-second). A host-failure recovery (the host went down) recovers from remote and is slower (tens of seconds for the pull, plus the deserialize time). A first-time start has no checkpoint and starts fresh. Each path is logged at the structured-log level the orchestrator's monitoring expects; the recovery route taken is itself a metric (`pipeline_recovery_path{path="local|remote|fresh"}` counter) that surfaces interesting events for ops to diagnose.

---

## Key Takeaways

* A **checkpoint is State + Offset**: the operator's serialized state plus the input-stream offset that the state reflects. Either alone is insufficient; both together are the recovery contract. The atomic-rename pattern ensures the file on disk is either the previous good checkpoint or the new good checkpoint, never a partial state.
* The **pause-snapshot-resume protocol** is the simplest implementation: withhold credits from upstream, serialize, write to disk, return credits. The pause duration is end-to-end latency cost during the snapshot — typically 50-200ms for SDA-scale state, tunable via storage choice.
* **Aligned checkpoints** (Flink-style barriers) produce consistent cuts across operators at the cost of slowest-input stall time. **Per-operator checkpoints** trade the consistency property for simplicity and rely on at-least-once + idempotency to recover global consistency via dedup. SDA uses per-operator.
* **Storage tiers** are local disk (fast, host-bound), remote object storage (slower, durable across host loss), and the **hybrid** (local-first, async-replicated-to-remote) which is the standard production pattern. The hot-path cost is bounded by local; the durability comes from remote.
* **Recovery hierarchy** is local → remote → fresh. Normal restart recovers from local in sub-second; host failure pulls from remote in tens of seconds; cold start has no checkpoint and starts at offset 0. Each path is logged as a structured metric so ops can see the recovery profile of every restart.

---

{{#quiz lesson-03-quiz.toml}}
