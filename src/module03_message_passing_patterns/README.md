# Module 03 — Message Passing Patterns

**Track:** Foundation — Mission Control Platform  
**Position:** Module 3 of 6  
**Source material:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 3, 6, 7, 8  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

Module 2 built shared-state concurrency: `Mutex`, `RwLock`, atomics. Those primitives protect data that multiple actors need to touch. This module takes the complementary approach: instead of sharing data, pass ownership through channels. Producers and consumers are decoupled — each owns its state exclusively, communicating only through typed messages.

For the Meridian control plane, message passing is the primary architecture for the telemetry pipeline. 48 satellite uplinks funnel frames into a priority-ordered aggregator. TLE catalog updates fan out to every active session simultaneously. The shutdown signal propagates to all tasks through a watched value. None of these require shared mutable state — they compose entirely from channel primitives.

---

## What You Will Learn

By the end of this module you will be able to:

- Create bounded `mpsc` channels, size them for backpressure, clone senders for multiple concurrent producers, and design consumer loops that terminate cleanly when all senders drop
- Implement the actor pattern: an async task that owns its state exclusively and exposes all operations as messages, using `oneshot` channels for request-response within the message protocol
- Distribute events to all subscribers using `broadcast`, handle `RecvError::Lagged` correctly, and size the broadcast capacity for the slowest realistic consumer
- Distribute current state to many readers using `watch`, understand the difference between event distribution and state distribution, and apply `Arc<T>` inside a watch for cheap config reads
- Merge multiple independent async sources into one stream using shared-sender MPSC (uniform sources), `select! { biased; }` (priority sources), and a router actor (dynamic sources)
- Choose between `mpsc`, `broadcast`, `watch`, and `oneshot` given a fan-in or fan-out requirement

---

## Lessons

### Lesson 1 — `tokio::mpsc`: Bounded Channels, Backpressure, and Sender Cloning

Covers `mpsc::channel(capacity)`, `Sender::clone` for multiple producers, `send().await` as the backpressure mechanism, `try_send` for non-blocking producers, the consumer loop termination on sender drop, `oneshot` for request-response, and the actor pattern as the structural idiom that emerges from MPSC channels.

**Key question this lesson answers:** How do you safely move work between concurrent async tasks without shared state, and what ensures slow consumers are not overwhelmed by fast producers?

→ `lesson-01-mpsc.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — Broadcast and Watch Channels: Fan-Out Patterns

Covers `broadcast::channel(capacity)` for event fan-out (every subscriber gets every message), `RecvError::Lagged` handling, `watch::channel(initial)` for state fan-out (latest value, change notification), `borrow()` for lock-free reads, and the decision matrix for choosing between `mpsc`, `broadcast`, and `watch`.

**Key question this lesson answers:** How do you distribute one event or one value to many concurrent tasks, and when does missing an intermediate update matter?

→ `lesson-02-broadcast-watch.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — Fan-In Aggregation: Merging Streams from Multiple Satellite Feeds

Covers shared-sender MPSC for uniform fan-in, `select! { biased; }` for priority fan-in with two priority levels, message tagging with typed source identifiers, and the router actor for dynamic fan-in (sources registered and removed at runtime).

**Key question this lesson answers:** How do you merge many independent async sources into one stream with control over priority, fairness, and dynamic source registration?

→ `lesson-03-fan-in.md` / `lesson-03-quiz.toml`

---

## Capstone Project — Multi-Source Telemetry Aggregator

Build the full telemetry aggregation pipeline: a router actor with dynamic source registration, a priority fan-in that ensures emergency frames are never delayed behind routine telemetry, a bounded frame processor with backpressure, a broadcast fan-out to downstream consumers, atomic pipeline statistics exposed through a watch channel, and a clean shutdown sequence.

Acceptance is against 7 verifiable criteria including emergency frame priority, dynamic source registration, backpressure enforcement, lossless shutdown drain, and lagged broadcast handling.

→ `project-telemetry-aggregator.md`

---

## Prerequisites

Modules 1 and 2 must be complete. Module 1 established how async tasks are scheduled and why they cooperatively yield — essential for understanding why bounded channel backpressure works without blocking threads. Module 2 established the shared-state model that message passing replaces — understanding both models is necessary to choose the right one for a given problem.

## What Comes Next

**Module 4 — Network Programming** connects the message-passing pipeline to the network. The telemetry aggregator from this module gains a TCP listener front-end, turning the router actor into a full ground station connection broker that accepts connections from the 12 Meridian ground station sites.