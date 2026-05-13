# Lesson 1: The Unreliable Network

## Context

At 03:47Z, the Pacific ground station reported that satellite **MSS-23** had stopped acknowledging telemetry pulls. The on-call engineer paged the constellation team, who attempted a manual command session and got no response for forty-eight seconds — then the satellite returned to operation as if nothing had happened. There was no failure log on the satellite, no dropped link on the ground side, and no recoverable trace on the relay. By the time the incident report was filed, three different teams had arrived at three different conclusions about what had failed.

This is the operating reality of the Constellation Network. Forty-eight LEO satellites communicate with twelve ground stations through asynchronous, lossy packet networks. From any single node's perspective — satellite or ground — a missing acknowledgment is fundamentally ambiguous: the request may have been lost, the reply may have been lost, the remote node may be down, the remote node may be processing slowly, or the remote node may have processed successfully and crashed before replying. The network gives you no way to tell these apart from the outside. This is what DDIA calls the **partial failure** model, and it is the foundational truth that every other lesson in this track builds on.

Before you can build consensus, replication, or fault tolerance, you have to internalize what the network actually guarantees — which is almost nothing. This lesson is the foundation. The eight fallacies of distributed computing are not historical curiosities; they describe the specific ways production engineers continue to write code that breaks under partial failure. By the end of this lesson, you should be able to read a piece of Rust code that talks to another node over the network and identify, by inspection, what it assumes that the network does not actually guarantee.

## Core Concepts

### Partial Failure and the Asymmetry of Knowledge

On a single machine, computation is **deterministic in failure**: either the program runs or it crashes outright. There is no in-between state where some of the registers have updated and others have not. Operating systems and hardware go to significant lengths to maintain this illusion — a CPU prefers to halt with a kernel panic rather than return a wrong result.

Distributed systems shatter this illusion. A "node" in your system is not the satellite's CPU — it is the satellite plus the network path that connects it to whoever is asking. That composite system can be in states the local CPU cannot: the satellite can be alive and well, processing your command, but the response packet can be sitting in a router queue that has just exceeded its memory limit and is silently dropping packets. The local satellite did its job. The local CPU is fine. The system as a whole has failed in a way that has no analog in single-node programming.

The consequence is an **asymmetry of knowledge**: a node always knows more about its own state than any other node can ever know about it. From the outside, you observe only what arrives over the network within some window of time. Everything else — what the remote node was doing, whether it received your message, whether it is still running — is an inference, never an observation.

### The Timeout Is Your Only Failure Detector

If you cannot directly observe a remote node's state, how do you decide whether to give up on it? The standard answer in practice — and the only one that is actually implementable on an asynchronous network — is **the timeout**. You send a request, you wait some duration, and if no reply arrives in that window, you treat the request as failed.

This sounds simple but encodes a deep tradeoff. Choose the timeout too short and you will falsely declare healthy nodes dead during normal queueing delays, causing spurious failovers, duplicate work, and cascading retries that can amplify the original load spike. Choose the timeout too long and you will leave clients hanging through real failures, blocking ground station operators and missing pass windows. There is no universally correct value. The right timeout depends on the network's tail latency distribution, the cost of a false-positive failure declaration, and the cost of a delayed failure declaration — all of which vary by workload.

Critically, **a timeout does not tell you that the remote node failed**. It tells you that you stopped waiting. The remote may complete the request five milliseconds after you give up, send a reply you never see, and continue operating in a state inconsistent with what you now believe. This gap — between "I declared you dead" and "you actually are dead" — is the source of an enormous class of distributed systems bugs, and we will return to it repeatedly in this track when we cover fencing tokens, leader leases, and split-brain scenarios.

### The Eight Fallacies of Distributed Computing

The list was compiled at Sun Microsystems in the 1990s, but every fallacy still appears in production code today. They are the implicit assumptions that programmers make when they treat a network call as if it were a local function call:

1. **The network is reliable.** Packets are dropped, links flap, NICs corrupt frames, and a backhoe will eventually sever your fiber. RPC frameworks that retry transparently mask this — at the cost of duplicate operations.
2. **Latency is zero.** A round trip across the constellation can be hundreds of milliseconds. Code that issues N sequential calls instead of one batched call will be N× slower.
3. **Bandwidth is infinite.** Satellite uplinks are bandwidth-constrained; pushing a megabyte of debugging output through a kilobit channel will starve the actual mission data.
4. **The network is secure.** Adversaries can replay packets, observe traffic, and inject frames. Anything that matters needs authentication and integrity, not just transport.
5. **Topology doesn't change.** Satellites enter and leave coverage; ground stations rotate; relay paths shift. Any static configuration of node addresses will be wrong within hours.
6. **There is one administrator.** Different ground stations are operated by different agencies. There is no global authority who can fix anything you need fixed.
7. **Transport cost is zero.** Each round trip costs CPU on both ends, plus serialization, plus the wire itself. Naïve serialization formats (JSON for high-throughput telemetry) will dominate CPU usage.
8. **The network is homogeneous.** Some links are 10 Gbps fiber; others are 9.6 kbps S-band over polar regions with 600 ms of latency.

When you read code, look for these assumptions as **implicit invariants** the author is relying on. A `.unwrap()` after a `send()` assumes (1). A request handler that processes N sub-requests in serial assumes (2). A configuration file that lists peer addresses assumes (5).

### Indistinguishable Failures: Why You Cannot Tell What Broke

When a request times out, the actual cause could be any of these, and you cannot distinguish them from outside:

- **The request was lost in flight.** The remote node never saw it. Retrying is safe (with caveats around idempotency).
- **The remote node received and processed the request, but the response was lost.** Retrying will perform the operation a second time.
- **The remote node received the request but crashed before processing.** Retrying is safe.
- **The remote node received the request, processed it, and then crashed before responding.** Like case 2, retrying will duplicate the operation.
- **The remote node received the request and is still processing it, just slowly** (long GC pause, swapping, IRQ storm). Retrying may cause two concurrent executions of the same operation on the remote node.
- **The remote node is partitioned from you but not from other peers.** It is doing useful work for others, and may have a quorum that does not include you. Retrying via you will fail; the operation may still complete via another path.

Notice that GC pauses sit in this list. A process pause of several seconds — common with stop-the-world JVM collectors, but also possible in Rust if you call into a library that takes a global mutex in a long-running thread — is **indistinguishable from the process being dead**. This is why Cassandra's failure detector treats unresponsiveness, not termination signals, as the operational notion of failure.

The practical implication is that you cannot write **"retry only if the remote node died"** code, because you cannot know whether it died. You can only write **"retry if I didn't hear back in time, and design every operation so that double-execution is harmless."** This is idempotency, and it is non-negotiable in distributed systems.

## Code Examples

### A Telemetry Client That Assumes Too Much

The Meridian control plane's legacy Python client looked roughly like this — pseudocode, but the structure is real:

```rust,ignore
// SCENARIO: First-pass port of the legacy Python ground station client.
// This is wrong in multiple ways. We'll dissect it.

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn fetch_telemetry(satellite_id: u32) -> Vec<u8> {
    let mut stream = TcpStream::connect("relay.meridian.internal:7400")
        .await
        .unwrap();  // Fallacy 1: assumes the network is reliable.

    let request = build_request(satellite_id);
    stream.write_all(&request).await.unwrap();  // Fallacy 1 again.

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    // No timeout. If the relay hangs, this future is blocked forever.

    response
}
```

Three failures hide in nine lines. The `connect` call has no timeout — a half-open TCP connection can sit in `SYN_SENT` for minutes on Linux defaults before the kernel gives up, during which your task is wedged. The `write_all` call has no timeout and `.unwrap()`s on error, propagating the implicit assumption that writes always succeed. The `read_to_end` call has no timeout *and* no application-level framing — it relies on the remote closing the connection to terminate the read, which means a remote that processes the request but then hangs will block this caller indefinitely.

In production, this exact shape is what produces the symptom that an alert dashboard summarizes as **"telemetry latency went to infinity at 03:47"**. The underlying node didn't crash. The relay was healthy. A single TCP connection got stuck in a state the client could not detect, and the client's task never returned to the runtime.

### A Telemetry Client That Acknowledges Reality

```rust,ignore
// PRODUCTION: same fetch, but explicit about every failure mode it can see.

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use anyhow::{Context, Result};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn fetch_telemetry(satellite_id: u32) -> Result<Vec<u8>> {
    // Each I/O operation has its own timeout. The connect timeout is shorter
    // than the request timeout: failing to establish TCP at all is a stronger
    // signal of failure than a slow response.
    let mut stream = timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect("relay.meridian.internal:7400"),
    )
    .await
    .context("connect timed out")?
    .context("connect failed")?;

    // We wrap the request/response cycle in a single timeout because partial
    // progress (writing the request but never receiving a reply) is still a
    // failure from the caller's perspective. The caller does not care which
    // half got stuck; they care that the operation did not complete.
    let response = timeout(REQUEST_TIMEOUT, async {
        let request = build_request(satellite_id);
        stream.write_all(&request).await?;

        // Length-prefixed framing: the protocol declares the size of the reply
        // before sending it, so we know exactly how many bytes to read instead
        // of waiting for the connection to close.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut response = vec![0u8; len];
        stream.read_exact(&mut response).await?;
        anyhow::Ok(response)
    })
    .await
    .context("request timed out")??;

    Ok(response)
}

fn build_request(_satellite_id: u32) -> Vec<u8> {
    // Encoding omitted; in production this uses a versioned binary protocol.
    Vec::new()
}
```

Notice what this version still doesn't do: it doesn't retry. That's deliberate. Retry policy is a *higher-level* concern than this function — it depends on whether the operation is idempotent, what the caller's deadline budget is, and whether duplicate execution on the remote node is harmful. A reusable transport function should surface the failure cleanly and let the policy layer above it decide. We will return to retry policy in the Fault Tolerance module; for now, the win here is that **every observable failure mode produces a distinct, actionable error rather than an infinite wait**.

### The Indistinguishability Problem in Code

To make the indistinguishability concrete: here is a snippet that captures the timeout, retries, and then later receives a reply for the request it already gave up on.

```rust,ignore
// PROBLEM: at-least-once delivery without idempotency.
// What happens if the original request *did* succeed on the remote?

use anyhow::Result;
use std::time::Duration;

pub async fn enqueue_command(cmd: Command) -> Result<()> {
    for attempt in 0..3 {
        match send_with_timeout(&cmd, Duration::from_secs(2)).await {
            Ok(()) => return Ok(()),
            Err(_) if attempt < 2 => {
                // The remote may have processed `cmd`. Or not. We can't tell.
                // Retrying enqueues it AGAIN if it did succeed, creating a
                // duplicate. This is a bug if `cmd` is "fire thruster for 3s".
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
# struct Command;
# async fn send_with_timeout(_c: &Command, _d: Duration) -> Result<()> { Ok(()) }
```

The standard fix is **idempotency**: every command carries a unique `command_id`, and the remote node tracks which IDs it has already executed. Retrying the same `command_id` is a no-op on the remote. This shifts the problem from "did the network deliver my message" — which is unanswerable — to "has the remote seen this ID before" — which is local and tractable.

This is the pattern that allows the rest of the system to work despite the network's failures. Once you accept that the network will deliver some messages zero times, some once, and some many times, you stop trying to control delivery and start designing operations whose semantics are independent of how many times they execute.

## Key Takeaways

- A node has direct knowledge only of itself; everything else is an inference from messages observed over a lossy network with unbounded delay. Code that conflates "remote node alive" with "I received a recent message from it" will be wrong during every transient delay.
- The timeout is the only practical failure detector on an asynchronous network. A timeout firing does not mean the remote node failed — it means you decided to stop waiting. Choose timeouts deliberately, calibrate them against your tail latency distribution, and design for the case where the remote completes the operation after you give up.
- All `n` of (a) lost request, (b) lost response, (c) crashed remote, (d) slow remote, (e) partitioned remote produce the same observable symptom: no reply. You cannot distinguish them from outside. Stop writing code that tries to.
- Idempotency is not a nice-to-have. It is the mechanism by which at-least-once message delivery — which is the only delivery guarantee any real network provides — becomes safe to use. Every command in the constellation control plane should carry a unique ID, and every receiver should be safe under double-delivery.
- The eight fallacies are not a historical artifact. They describe the implicit assumptions in production code right now. When you review a colleague's PR that does anything across the network, look for which fallacies it has not yet stopped believing.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 9, "The Trouble with Distributed Systems" — specifically the sections "Faults and Partial Failures" and "Unreliable Networks." The eight fallacies of distributed computing originate with Peter Deutsch and James Gosling at Sun Microsystems (c. 1994); this lesson restates them but the framing is synthesized from training knowledge. Any specific historical claims about Sun, Deutsch, or Gosling should be verified before publication.

{{#quiz quiz-1.toml}}
