# Project — Multi-Source Telemetry Aggregator

**Module:** Foundation — M03: Message Passing Patterns  
**Prerequisite:** All three module quizzes passed (≥70%)

---
<!-- toc -->
---
## Mission Brief

**TO:** Platform Engineering  
**FROM:** Mission Control Systems Lead  
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL  
**SUBJECT:** RFC-0047 — Telemetry Aggregation Pipeline

---

The control plane currently receives telemetry from 48 LEO satellite uplinks and two archived replay feeds simultaneously during mission replay operations. Each source produces frames at independent rates. Emergency command frames from any source must be processed before routine telemetry. Downstream analytics consumers need every frame; a monitoring dashboard needs only the latest pipeline statistics.

Your task is to build the telemetry aggregation pipeline that connects these sources to their consumers. The pipeline must: fan-in all sources into a priority-ordered stream, fan-out to a downstream frame processor and to a monitoring dashboard, apply backpressure so fast sources cannot overwhelm the processor, and shut down cleanly when signalled.

---

## System Specification

### Frame Types

```rust,edition2021
#[derive(Debug, Clone)]
pub enum FramePriority {
    Emergency,  // SAFE_MODE, ABORT commands
    Routine,    // Standard telemetry
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub source_id: u32,
    pub source_kind: SourceKind,
    pub priority: FramePriority,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum SourceKind {
    LiveUplink,
    ArchivedReplay,
}
```

### Pipeline Architecture

```
[Uplink 0..48]  ──┐
                  ├─► [Router Actor] ─► [Priority Fan-In] ─► [Frame Processor]
[Replay 0..2]   ──┘         │                                        │
                             └──────────────────────────────► [Broadcast: all frames]
                                                                      │
                                                              [Dashboard] [Archive]

[watch: shutdown] ──────────────────────────────────────► All tasks
[watch: stats]    ◄──────────────────── Frame Processor (updates atomically)
```

### Behavioural Requirements

**Fan-in:** Frames from live uplinks and archived replays are merged via a router actor that supports dynamic source registration. Emergency frames must be prioritised over routine frames when both are available simultaneously.

**Backpressure:** The frame processor has a bounded input channel (capacity 64). When the processor is saturated, backpressure propagates up to the priority fan-in, which in turn applies pressure to the router's internal channel. Routine sources are slowed; emergency frames still make progress due to priority ordering.

**Fan-out:** Every processed frame is sent over a `broadcast` channel to all downstream consumers. The monitoring dashboard subscribes; an archive writer task subscribes. The dashboard is allowed to lag and handles `RecvError::Lagged` gracefully.

**Stats:** The pipeline maintains three `AtomicU64` counters: `frames_routed`, `frames_processed`, `emergency_count`. These are exposed via a `watch` channel as a `PipelineStats` snapshot, updated by the frame processor after each frame.

**Shutdown:** A `watch<bool>` shutdown signal is distributed to all tasks. On signal: (1) stop accepting new frames from sources, (2) drain the priority fan-in channel, (3) close the broadcast channel, (4) all tasks exit within 5 seconds.

---

## Expected Output

A binary that:
1. Starts a router actor accepting dynamic source registration
2. Registers 4 live uplink sources (each sending 10 frames) and 1 replay source (sending 5 frames)
3. 2 of the live uplink frames per source are marked `Emergency`
4. Runs a frame processor that logs each frame with its priority and source
5. Runs a monitoring task that reads `watch<PipelineStats>` every 50ms and prints stats
6. Runs a downstream archive task subscribed to the broadcast channel
7. Sends shutdown signal after all sources finish; all tasks exit cleanly

The output should clearly show emergency frames being processed before routine frames from the same batch.

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | Emergency frames processed before queued routine frames from the same source | Yes — log order |
| 2 | New sources can be registered at runtime via the router control channel | Yes — sources registered mid-run |
| 3 | Frame processor channel capacity is enforced — producers yield when full | Yes — add `tokio::time::sleep` in processor and verify producers do not drop frames |
| 4 | All downstream consumers receive every processed frame via `broadcast` | Yes — counts match between processor and archive consumer |
| 5 | Stats `watch` channel provides latest snapshot without acquiring any lock | Yes — code review: only atomic loads in stats read path |
| 6 | Shutdown drains the fan-in channel before exiting | Yes — no frames lost after shutdown signal |
| 7 | Lagged broadcast receivers log a warning and continue — they do not crash | Yes — introduce a slow archive task and verify `Lagged` is handled |

---

## Hints

<details>
<summary>Hint 1 — Priority fan-in with biased select!</summary>

Use two channels from the router: one for emergency frames, one for routine. The priority fan-in selects with `biased`:

```rust,edition2021
async fn priority_fan_in(
    mut emergency_rx: tokio::sync::mpsc::Receiver<Frame>,
    mut routine_rx: tokio::sync::mpsc::Receiver<Frame>,
    out_tx: tokio::sync::mpsc::Sender<Frame>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut shutdown = shutdown;
    loop {
        tokio::select! {
            biased;
            Some(f) = emergency_rx.recv() => {
                if out_tx.send(f).await.is_err() { break; }
            }
            Some(f) = routine_rx.recv() => {
                if out_tx.send(f).await.is_err() { break; }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            else => break,
        }
    }
}
```

</details>

<details>
<summary>Hint 2 — Router actor with dynamic registration</summary>

The router forwards all sources to two internal channels split by priority. Each registered source gets a forwarding task:

```rust,edition2021
enum RouterMsg {
    AddSource {
        source_id: u32,
        source_kind: SourceKind,
        feed: tokio::sync::mpsc::Receiver<Frame>,
    },
    RemoveSource { source_id: u32 },
}
```

The forwarding task reads from the feed and sends to the appropriate internal channel based on `frame.priority`.

</details>

<details>
<summary>Hint 3 — Stats snapshot with watch + atomics</summary>

The frame processor updates atomic counters after each frame, then sends a snapshot to the watch channel:

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct PipelineStats {
    pub frames_processed: u64,
    pub emergency_count: u64,
}

struct StatsTracker {
    frames_processed: AtomicU64,
    emergency_count: AtomicU64,
    tx: tokio::sync::watch::Sender<PipelineStats>,
}

impl StatsTracker {
    fn record(&self, is_emergency: bool) {
        self.frames_processed.fetch_add(1, Relaxed);
        if is_emergency {
            self.emergency_count.fetch_add(1, Relaxed);
        }
        // Publish a snapshot — receivers always see the latest.
        let _ = self.tx.send(PipelineStats {
            frames_processed: self.frames_processed.load(Relaxed),
            emergency_count: self.emergency_count.load(Relaxed),
        });
    }
}
```

</details>

<details>
<summary>Hint 4 — Broadcast fan-out with lagged handling</summary>

```rust,edition2021
async fn archive_consumer(
    mut rx: tokio::sync::broadcast::Receiver<Frame>,
) {
    let mut archived = 0u64;
    loop {
        match rx.recv().await {
            Ok(frame) => {
                archived += 1;
                tracing::debug!(
                    source = frame.source_id,
                    seq = frame.sequence,
                    "archived"
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                // Archive fell behind — note the gap and continue.
                tracing::warn!(missed = n, "archive lagged");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!(total = archived, "archive consumer done");
                break;
            }
        }
    }
}
```

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation</summary>

```rust,edition2021
// This reference implementation is intentionally condensed.
// A production implementation would split into modules.
use tokio::sync::{broadcast, mpsc, watch};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
pub enum FramePriority { Emergency, Routine }

#[derive(Debug, Clone)]
pub enum SourceKind { LiveUplink, ArchivedReplay }

#[derive(Debug, Clone)]
pub struct Frame {
    pub source_id: u32,
    pub source_kind: SourceKind,
    pub priority: FramePriority,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct PipelineStats {
    pub frames_processed: u64,
    pub emergency_count: u64,
}

enum RouterMsg {
    AddSource {
        source_id: u32,
        feed: mpsc::Receiver<Frame>,
    },
}

async fn router_actor(
    mut ctrl: mpsc::Receiver<RouterMsg>,
    emergency_tx: mpsc::Sender<Frame>,
    routine_tx: mpsc::Sender<Frame>,
) {
    let (internal_tx, mut internal_rx) = mpsc::channel::<Frame>(512);
    let mut handles: HashMap<u32, tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        tokio::select! {
            Some(msg) = ctrl.recv() => {
                match msg {
                    RouterMsg::AddSource { source_id, mut feed } => {
                        let fwd = internal_tx.clone();
                        let h = tokio::spawn(async move {
                            while let Some(frame) = feed.recv().await {
                                if fwd.send(frame).await.is_err() { break; }
                            }
                        });
                        handles.insert(source_id, h);
                    }
                }
            }
            Some(frame) = internal_rx.recv() => {
                let dest = match frame.priority {
                    FramePriority::Emergency => &emergency_tx,
                    FramePriority::Routine   => &routine_tx,
                };
                if dest.send(frame).await.is_err() { break; }
            }
            else => break,
        }
    }
}

async fn priority_fan_in(
    mut emerg_rx: mpsc::Receiver<Frame>,
    mut routine_rx: mpsc::Receiver<Frame>,
    out_tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            Some(f) = emerg_rx.recv()  => { if out_tx.send(f).await.is_err() { break; } }
            Some(f) = routine_rx.recv() => { if out_tx.send(f).await.is_err() { break; } }
            Ok(()) = shutdown.changed() => { if *shutdown.borrow() { break; } }
            else => break,
        }
    }
}

async fn frame_processor(
    mut rx: mpsc::Receiver<Frame>,
    bcast_tx: broadcast::Sender<Frame>,
    stats_tx: watch::Sender<PipelineStats>,
    processed: Arc<AtomicU64>,
    emergency: Arc<AtomicU64>,
) {
    while let Some(frame) = rx.recv().await {
        let is_emerg = matches!(frame.priority, FramePriority::Emergency);
        tracing::info!(
            source = frame.source_id, seq = frame.sequence,
            priority = if is_emerg { "EMERGENCY" } else { "routine" },
            "processed"
        );
        processed.fetch_add(1, Relaxed);
        if is_emerg { emergency.fetch_add(1, Relaxed); }
        let _ = stats_tx.send(PipelineStats {
            frames_processed: processed.load(Relaxed),
            emergency_count: emergency.load(Relaxed),
        });
        let _ = bcast_tx.send(frame);
    }
}

async fn archive_consumer(mut rx: broadcast::Receiver<Frame>) {
    let mut count = 0u64;
    loop {
        match rx.recv().await {
            Ok(_) => count += 1,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "archive lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::info!(total = count, "archive done");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (stats_tx, mut stats_rx) = watch::channel(PipelineStats::default());
    let (bcast_tx, _) = broadcast::channel::<Frame>(128);
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<RouterMsg>(8);
    let (emerg_tx, emerg_rx) = mpsc::channel::<Frame>(64);
    let (routine_tx, routine_rx) = mpsc::channel::<Frame>(256);
    let (proc_tx, proc_rx) = mpsc::channel::<Frame>(64);

    let processed = Arc::new(AtomicU64::new(0));
    let emergency = Arc::new(AtomicU64::new(0));

    // Start pipeline tasks.
    tokio::spawn(router_actor(ctrl_rx, emerg_tx, routine_tx));
    tokio::spawn(priority_fan_in(emerg_rx, routine_rx, proc_tx, shutdown_rx.clone()));
    tokio::spawn(frame_processor(
        proc_rx, bcast_tx.clone(), stats_tx,
        Arc::clone(&processed), Arc::clone(&emergency),
    ));
    tokio::spawn(archive_consumer(bcast_tx.subscribe()));

    // Register 4 live uplink sources.
    for sat_id in 0..4u32 {
        let (feed_tx, feed_rx) = mpsc::channel::<Frame>(32);
        ctrl_tx.send(RouterMsg::AddSource { source_id: sat_id, feed: feed_rx }).await.unwrap();
        tokio::spawn(async move {
            for seq in 0u64..10 {
                let priority = if seq < 2 { FramePriority::Emergency } else { FramePriority::Routine };
                feed_tx.send(Frame {
                    source_id: sat_id, source_kind: SourceKind::LiveUplink,
                    priority, sequence: seq, payload: vec![sat_id as u8; 32],
                }).await.unwrap();
                sleep(Duration::from_millis(5)).await;
            }
        });
    }

    // Stats monitor.
    tokio::spawn(async move {
        for _ in 0..4 {
            sleep(Duration::from_millis(50)).await;
            stats_rx.changed().await.ok();
            let s = stats_rx.borrow().clone();
            println!("stats: processed={} emergency={}", s.frames_processed, s.emergency_count);
        }
    });

    sleep(Duration::from_millis(300)).await;
    println!("sending shutdown");
    shutdown_tx.send(true).unwrap();
    sleep(Duration::from_millis(100)).await;
    println!("final: processed={} emergency={}",
        processed.load(Relaxed), emergency.load(Relaxed));
}
```

</details>

---

## Reflection

This project assembles the full message-passing toolkit from Module 3. The router actor provides dynamic fan-in with independent source lifecycle management. The priority fan-in ensures emergency frames are never delayed by routine traffic. The broadcast channel distributes every processed frame to all downstream consumers. The watch channel distributes state — shutdown signal and pipeline stats — without requiring consumers to hold any lock.

The pattern here — router → priority queue → processor → broadcast — recurs throughout Meridian's data pipeline architecture. In Module 4 (Network Programming), the router actor gains TCP listener integration, turning it into a full ground station connection broker.