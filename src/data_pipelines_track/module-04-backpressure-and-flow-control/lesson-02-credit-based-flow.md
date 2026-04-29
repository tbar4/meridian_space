# Lesson 2 — Credit-Based Flow Control

**Module:** Data Pipelines — M04: Backpressure and Flow Control
**Position:** Lesson 2 of 3
**Source:** *Network Programming with Rust* — Abhishek Chanda, the chapter on application-level flow control over TCP and the AMQP/HTTP-2 patterns; *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 3 (`max.in.flight.requests.per.connection` as a degenerate single-credit scheme); *Async Rust* — Maxwell Flitton & Caroline Morton, advanced channel patterns

---

## Context

Lesson 1 established `send().await` on a bounded channel as the foundation of backpressure. The mechanism is *implicit credit*: the channel's capacity is the credit pool; every successful send consumes one slot; the consumer's `recv` returns a slot to the pool. The sender does not manage credits explicitly; it just calls `send` and gets backpressure for free. This is the right shape for almost every operator-pair edge in the SDA pipeline and the right default for nearly all dataflow systems.

There is one shape it does not fit well. When the receiver wants to *pause ingestion entirely without holding the in-flight item in a buffer*. The canonical case is a checkpoint flush — the receiver needs to durably persist its current state before accepting new events, and during that window it cannot afford to even buffer the next event because the in-flight buffer is exactly what the checkpoint is trying to capture. The bounded-channel-plus-await design has no clean way to express "stop sending, but don't fill the slot you'd have used." The receiver can just not call `recv` for a while, but the next item the sender produces fills the channel, occupies a slot, and is now part of the in-flight state the checkpoint must serialize.

Credit-based flow control is the alternative shape. The receiver issues *credits* explicitly; the sender consumes credits on each send and refuses to send when out of credits. The credit return path is a *separate channel* from the data path, which means the receiver can pause issuing credits without touching the data channel at all — the sender's credit counter goes to zero, the sender stops, no in-flight item is created. This decoupling is what makes credit-based flow control the right primitive for pause-without-buffer-fill operations and for pipelines where data and control are physically separated (HTTP/2 stream flow control; AMQP prefetch; Kafka's `max.in.flight.requests.per.connection` as a single-credit degenerate case).

This lesson develops credit-based flow control as the operational alternative to bounded-channel-plus-await, identifies precisely when each fits, and develops the implementation pattern as a wrapper around the M2 channel primitives. The capstone uses credit-based flow control upstream of the windowed correlator's checkpoint flush in Module 5; this lesson installs the machinery now so that work has the primitive available.

---

## Core Concepts

### Credits, Defined

A *credit* is permission to send one item. The receiver has a finite pool of credits at any moment; the sender consumes one credit per send; sending without a credit is forbidden. The receiver replenishes the pool by sending *credit-return* messages — back to the sender, indicating that some prior items have been consumed and N new credits are available. The sender's local credit counter starts at the receiver's initial grant, decrements on each send, increments on each credit-return.

When credits = 0 the sender stops. It does not buffer. It does not block on a channel send. It returns to its caller (or sits at its own await point waiting for credit returns) without producing the next item. This is the mechanism that makes the receiver's pause real: by withholding credits, the receiver creates an upstream stop without occupying a single channel slot.

The shape resembles backpressure-via-await in the steady state — when credits flow normally, the sender produces at the receiver's rate, just like it would on a bounded channel — but differs in two structural ways. First, the credit signal is on a *separate channel*, decoupled from the data path. Second, the sender's behavior on "out of credits" is its own decision (block, drop, route elsewhere) rather than the channel's.

### Credit-Based vs Backpressure-via-Await

Both produce upstream slowdowns under sustained downstream pressure. The differences are operational.

**Backpressure-via-await** binds the credit signal to the data channel — the channel's capacity IS the credit pool. To pause the sender, the receiver must not call `recv`, which means the channel fills, which means the next in-flight item occupies a slot. The receiver cannot pause without buffering at least one more item.

**Credit-based** decouples them. The receiver pauses by withholding credits on the side channel; the data channel remains empty (or at whatever steady state it was in). The sender's local credit counter goes to zero; the sender stops without producing the next item.

For most pipeline edges this difference does not matter. The data channel filling with one extra item before the sender suspends is not a problem; it does not change the pipeline's correctness. For the checkpoint case it does matter — the in-flight buffer being empty is the property the checkpoint depends on. Credit-based is the right primitive when the receiver needs that empty-buffer property.

There is a secondary difference. Credit-based supports *batched grants*: the receiver can issue 100 credits at once, and the sender can fire 100 sends without coordination. Backpressure-via-await supports the same pattern only via the channel's capacity, which couples the burst size to the persistent slack. Credits let burst size and steady-state slack be independent, which is occasionally useful (a receiver that wants 10 in-flight messages steady-state but allows occasional 100-message bursts).

### The Credit Return Path

Two channels: data and credit-return. The data channel is the same `mpsc::Sender<T>` / `mpsc::Receiver<T>` pair we have used throughout. The credit-return channel is `mpsc::Sender<u32>` / `mpsc::Receiver<u32>`, where each message is a credit count being returned. The receiver's per-event behavior:

1. `recv` an item from the data channel.
2. Process the item.
3. Send `1` (or some larger batch count) on the credit-return channel.

The sender's per-event behavior:

1. Drain the credit-return channel of any pending returns; increment the local counter by the sum.
2. If the local counter is 0, await a credit-return on the credit-return channel.
3. Decrement the counter; send the item on the data channel.

The data channel itself does not need to be bounded — credits bound the in-flight count, which is what the bounded channel was for. In practice, the data channel is given a small bound (matching the maximum credit grant) to keep the implementation defensive against credit-counter bugs.

### Where Production Uses Credits

**HTTP/2** has per-stream and per-connection flow-control windows (RFC 7540 Section 5.2). The receiver's `WINDOW_UPDATE` frame is exactly a credit-return: it tells the sender how many more bytes it may send on a given stream. The use case is multiplexing many streams on a single TCP connection — each stream needs its own backpressure that does not affect the others, and credits on a side channel give that.

**AMQP (RabbitMQ, ActiveMQ)** uses per-channel *prefetch* limits. The consumer declares its prefetch count; the broker delivers up to that many unacknowledged messages; the consumer's ack returns a credit. The mechanism is identical to the lesson's pattern, just with the broker and consumer in the producer/consumer roles.

**Kafka's `max.in.flight.requests.per.connection`** is a degenerate single-credit case. The producer can have at most N in-flight requests to a given broker; each completed request returns one credit. With N=1 (the strongest setting), the producer is effectively serial-pipelined. With N=5 (the default), small bursts are allowed.

The pattern is widespread in production protocols. It is less commonly used in single-process pipelines because backpressure-via-await is sufficient most of the time; credit-based shows up specifically where the additional decoupling is needed.

### When to Reach for Credits in SDA

Three concrete cases.

**Checkpoint flush (Module 5).** The windowed correlator's state is being durably written to disk. During the write, the operator cannot afford to even buffer the next event. The flush operator pauses ingestion by withholding credits; the upstream correlator sender stops cleanly without occupying a slot.

**Cross-runtime edges (advanced bulkheading from M2 L4).** When an operator in one runtime sends to an operator in a different runtime (the propagator pool versus the main pipeline), the bounded channel between them does not propagate backpressure cleanly because the runtimes have independent schedulers. Credit-based flow gives an explicit mechanism that the receiving runtime controls.

**Operator handoff during a graceful drain.** During shutdown, the orchestrator wants the upstream to stop producing while in-flight items finish flowing. The orchestrator (or a control-plane operator) withholds credits on the affected edges; the upstream halts; downstream drains; the pipeline shuts down cleanly without losing in-flight items.

For everything else, prefer the simpler `send().await` pattern from Lesson 1. Credit-based flow is a heavier mechanism with more coordination overhead, and using it where it is not needed adds operational surface area.

---

## Code Examples

### A `CreditChannel` Wrapper

The wrapper is two channels and a small bookkeeping struct. The sender side checks credits before each send; the receiver side issues credit returns after each successful processing.

```rust,ignore
use anyhow::Result;
use tokio::sync::mpsc;

/// One end of a credit-based flow channel. The sender consumes credits
/// from its local counter on each send; when the counter is zero, it
/// awaits a credit return.
pub struct CreditSender<T> {
    data_tx: mpsc::Sender<T>,
    credit_rx: mpsc::Receiver<u32>,
    local_credits: u32,
}

impl<T> CreditSender<T> {
    pub async fn send(&mut self, item: T) -> Result<()> {
        // Drain any pending credit returns first.
        while let Ok(returned) = self.credit_rx.try_recv() {
            self.local_credits = self.local_credits.saturating_add(returned);
        }
        // If we are out of credits, block on a return.
        while self.local_credits == 0 {
            match self.credit_rx.recv().await {
                Some(returned) => {
                    self.local_credits = self.local_credits.saturating_add(returned);
                }
                None => return Err(anyhow::anyhow!("credit return channel closed")),
            }
        }
        self.local_credits -= 1;
        self.data_tx.send(item).await
            .map_err(|_| anyhow::anyhow!("data channel receiver dropped"))?;
        Ok(())
    }

    /// Current local credit count — useful for diagnostics.
    pub fn credits(&self) -> u32 { self.local_credits }
}

/// The receiver end. Reading an item produces a `CreditHandle` that
/// MUST be returned (via return_credit) after the item is processed.
/// Forgetting to return credits is the canonical credit-leak bug.
pub struct CreditReceiver<T> {
    data_rx: mpsc::Receiver<T>,
    credit_tx: mpsc::Sender<u32>,
}

pub struct CreditHandle<'a> {
    credit_tx: &'a mpsc::Sender<u32>,
    returned: bool,
}

impl<T> CreditReceiver<T> {
    pub async fn recv(&mut self) -> Option<(T, CreditHandle<'_>)> {
        let item = self.data_rx.recv().await?;
        let handle = CreditHandle {
            credit_tx: &self.credit_tx,
            returned: false,
        };
        Some((item, handle))
    }
}

impl<'a> CreditHandle<'a> {
    /// Return one credit to the sender. Should be called after the
    /// associated item has been processed.
    pub async fn return_credit(mut self) -> Result<()> {
        self.returned = true;
        self.credit_tx.send(1).await
            .map_err(|_| anyhow::anyhow!("credit return channel sender dropped"))?;
        Ok(())
    }
}

impl<'a> Drop for CreditHandle<'a> {
    fn drop(&mut self) {
        if !self.returned {
            // Forgotten credit return — log it. This is a programming bug,
            // not a recoverable condition; production code should alert.
            tracing::error!("CreditHandle dropped without returning credit; credit leaked");
        }
    }
}

/// Pair constructor: returns matched sender + receiver with the given
/// initial credit grant.
pub fn credit_channel<T: Send + 'static>(
    data_capacity: usize,
    initial_credits: u32,
) -> (CreditSender<T>, CreditReceiver<T>) {
    let (data_tx, data_rx) = mpsc::channel(data_capacity);
    let (credit_tx, credit_rx) = mpsc::channel(data_capacity);
    (
        CreditSender { data_tx, credit_rx, local_credits: initial_credits },
        CreditReceiver { data_rx, credit_tx },
    )
}
```

The `CreditHandle` with its `Drop`-emits-error pattern is a useful safety net: forgetting to return a credit is the canonical bug in this pattern, and the warning at `Drop` makes the bug visible in logs rather than mysterious in production. Production code goes a step further and refuses to compile if `return_credit` is not called (via a `must_use` lint or similar); for clarity here we use the runtime warning. The `data_capacity` parameter sets the data channel's bound — it should equal or exceed the maximum credit grant ever issued so the data channel itself is never the limiting factor.

### A Receiver That Pauses by Withholding Credits

The case the lesson called out as the primary use: pausing the upstream during a checkpoint flush without occupying any in-flight slots.

```rust,ignore
use std::time::Duration;
use tokio::time::sleep;

/// Operator that periodically checkpoints. During the checkpoint
/// window it withholds credits, pausing the upstream sender cleanly.
pub async fn run_checkpointing_operator<T>(
    mut input: CreditReceiver<T>,
    output: mpsc::Sender<T>,
    checkpoint_interval: Duration,
) -> Result<()>
where
    T: Send + 'static,
{
    let mut last_checkpoint = std::time::Instant::now();

    loop {
        if last_checkpoint.elapsed() >= checkpoint_interval {
            // Time to checkpoint. The CRITICAL property: by NOT calling
            // input.recv() during the flush, we both stop accepting new
            // items AND withhold credit returns to the upstream. The
            // upstream's local credit counter drains; the upstream stops
            // producing without occupying any in-flight slot.
            tracing::info!("starting checkpoint flush");
            do_checkpoint_flush().await?;
            last_checkpoint = std::time::Instant::now();
            tracing::info!("checkpoint flush complete; resuming");
            // Returning credits resumes the upstream.
            continue;
        }

        match input.recv().await {
            Some((item, credit)) => {
                // Process the item.
                output.send(item).await
                    .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
                // Return the credit AFTER processing. Returning before
                // processing would defeat the bounded-in-flight property
                // (the upstream could fire another item while this one
                // is still in flight at the operator).
                credit.return_credit().await?;
            }
            None => return Ok(()),
        }
    }
}

async fn do_checkpoint_flush() -> Result<()> {
    // Module 5 develops checkpointing in full. Here: stand-in.
    sleep(Duration::from_millis(150)).await;
    Ok(())
}
```

Two design points worth dwelling on. The credit return happens *after* item processing, not before. Returning before processing breaks the in-flight-bounded property: the upstream sees the credit, fires the next item, and now there are two items in flight at this operator — one being processed, one in the data channel. With the AFTER ordering, the bound is exactly the initial credit grant: at most that many items are in flight at any moment.

The second is structural: the operator's "pause during checkpoint" mechanism is *not calling recv*. There is no explicit "pause" or "resume" message; the credit-return mechanic falls out of the recv-loop's natural shape. When the operator is in the checkpoint branch, no recv happens, no credit is returned, the upstream's counter drains. When the operator returns to the recv loop, credits flow again, the upstream resumes. The implementation is small precisely because the mechanism is doing the work.

### Fairness Across Multiple Senders

When multiple senders share a credit pool with a single receiver, the issuance policy decides who gets what share. Two strategies, each with a different fairness profile.

```rust,ignore
use std::collections::VecDeque;

/// Round-robin credit issuance: cycle through senders, granting one
/// credit per sender per round. Fair share regardless of demand.
pub struct RoundRobinCreditIssuer {
    senders: Vec<mpsc::Sender<u32>>,
    cursor: usize,
}

impl RoundRobinCreditIssuer {
    pub async fn grant_one(&mut self) -> Result<()> {
        let target = self.cursor % self.senders.len();
        self.senders[target].send(1).await
            .map_err(|_| anyhow::anyhow!("sender's credit channel closed"))?;
        self.cursor += 1;
        Ok(())
    }
}

/// First-asker-wins issuance: senders queue up requests; the issuer
/// satisfies in arrival order. Greedy senders dominate.
pub struct FifoCreditIssuer {
    request_queue: VecDeque<usize>,  // sender indices in arrival order
    senders: Vec<mpsc::Sender<u32>>,
}

impl FifoCreditIssuer {
    pub fn enqueue_request(&mut self, sender_idx: usize) {
        self.request_queue.push_back(sender_idx);
    }

    pub async fn grant_one(&mut self) -> Result<()> {
        if let Some(target) = self.request_queue.pop_front() {
            self.senders[target].send(1).await
                .map_err(|_| anyhow::anyhow!("sender's credit channel closed"))?;
        }
        Ok(())
    }
}
```

Round-robin gives every sender a predictable share regardless of their per-sender rate. FIFO gives faster senders a larger share because they request more. The choice is operational. SDA's three sources have very different per-source rates (radar at thousands per second; ISL beacons at tens per second), and FIFO would let the radar dominate the credit pool — possibly correct if "throughput" is the optimization, definitely wrong if "fair representation" is. Round-robin is the SDA default. Production credit-issuance schemes can be more sophisticated still (weighted round-robin, deficit weighted, fair queueing); the framework above is the starting point that the rest builds on.

---

## Key Takeaways

* **Credit-based flow control** is the alternative to bounded-channel-plus-await for cases where the receiver needs to pause the upstream *without* occupying any in-flight slot. Credits flow on a separate channel from data; the receiver's pause is "stop returning credits."
* The structural difference vs `send().await`: backpressure-via-await binds the credit signal to the channel capacity; credit-based decouples them. **Use credit-based when decoupling matters** — checkpoint flushes, cross-runtime edges, graceful-drain pauses. Use bounded-channel-plus-await for everything else.
* Production protocols use credits widely. **HTTP/2** flow control windows. **AMQP prefetch**. **Kafka's `max.in.flight.requests.per.connection`** as a single-credit degenerate case. The pattern is well-established in distributed systems; this lesson brings it into the single-process pipeline for the cases that benefit.
* The implementation is small: data channel + credit-return channel + per-sender credit counter. The **credit return happens after item processing, not before** — the AFTER ordering is what makes the in-flight bound exactly equal to the credit grant.
* **Credit-issuance fairness is operational**. Round-robin gives predictable per-sender share regardless of demand; FIFO lets greedy senders dominate. SDA defaults to round-robin; the heterogeneity of per-source rates would otherwise let one source starve the others.

---

{{#quiz lesson-02-quiz.toml}}
