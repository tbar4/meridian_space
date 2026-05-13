# Module 01 — Distributed Systems Fundamentals

> *"Two relay satellites just lost contact simultaneously over Antarctica. The grid needs to elect a new coordinator before the next pass window closes."*

## Mission Context

This module is the prerequisite for everything else in the Distributed Systems track. Before you can reason about replication, consensus, fault tolerance, or coordination, you have to internalize three foundational truths about the systems you are about to build:

1. **The network is unreliable in ways that are fundamentally indistinguishable from outside.** A timeout does not tell you what failed. Asynchronous packet networks deliver some messages zero times, some once, and some many times — and you cannot tell which.
2. **Time is unreliable.** Wall clocks disagree across machines, NTP gives you approximate synchronization at best, and any algorithm that depends on tight clock agreement is one bad sync away from misbehaving.
3. **You cannot have everything.** CAP and PACELC formalize the tradeoffs every distributed data system makes, both during partitions and in normal operation. The right question is not "is this system consistent?" but "in which cell of the PACELC matrix does this system live, and is that the cell we want?"

The opening incidents — the MSS-23 telemetry timeout (Lesson 1), the MSS-17 attitude-update overwrite (Lesson 2), and the Antarctic partition (Lesson 3) — are not failure stories from elsewhere. They are the kind of incidents the Constellation Network will produce next quarter if these concepts are not understood by the engineers building it.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [The Unreliable Network](./lesson-01-unreliable-network.md) | DDIA Ch. 9 |
| 2 | [Clocks, Ordering, and Causality](./lesson-02-clocks-ordering.md) | DDIA Ch. 9 + Lamport 1978 |
| 3 | [CAP, PACELC, and the Consistency Spectrum](./lesson-03-cap-pacelc.md) | DDIA Ch. 10 + Abadi 2012 |

## Project

[Constellation Clock Sync](./project.md) — a simulated 4-node satellite cluster with injected partitions, demonstrating that correct distributed ordering does not require NTP. Implements Lamport and vector clocks, a partition-aware message bus, and a test harness that verifies causal ordering under adversarial conditions.

## Position

Module 1 of 6 in the Distributed Systems track.

## What You Should Be Able to Do After This Module

- Read code that talks to other nodes and identify, by inspection, which of the eight fallacies of distributed computing it implicitly relies on.
- Choose deliberately between `SystemTime` and `Instant` based on whether the operation requires absolute time or elapsed time, and articulate the failure mode of each.
- Implement and apply Lamport and vector clocks to order distributed events without depending on physical clock agreement.
- Place a candidate data system on the PACELC matrix and explain, in one sentence, what behavior the system exhibits during a partition and what behavior it exhibits during normal operation.
- Distinguish linearizability from serializability and from "strong consistency" claims in vendor documentation, and ask the right follow-up questions when a system claims a consistency model.

## Source Materials

- **DDIA 2nd Edition** (Kleppmann & Riccomini, 2026) — Chapter 9 ("The Trouble with Distributed Systems") is the primary source for Lessons 1 and 2. Chapter 10 ("Consistency and Consensus") opens the framing for Lesson 3.
- **Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System"** (Communications of the ACM, July 1978) — the canonical reference for logical clocks. Strongly recommended supplemental reading.
- **Abadi, "Consistency Tradeoffs in Modern Distributed Database System Design"** (IEEE Computer, February 2012) — the original PACELC paper.
- **Gilbert & Lynch, "Brewer's conjecture and the feasibility of consistent, available, partition-tolerant web services"** (SIGACT News, June 2002) — the formal proof of CAP.

Source notes on individual lessons flag where content has been synthesized beyond the available source material and should be verified before publication.
