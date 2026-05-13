# Module 04 — Fault Tolerance Patterns

> *"The catalog's resilience layer has prevented 14 documented incidents. It has also failed to prevent three incidents that occurred because of failure modes the resilience layer had not been designed for. The next outage is, by definition, an unknown failure mode."*

## Mission Context

Modules 1 through 3 built up the foundations: how to reason about an unreliable network and clock, how to replicate state across nodes, and how to reach agreement on shared state via consensus. The constellation needs one more layer before it can run unattended at 3 AM: the operational discipline that bounds failure when something does go wrong, and the testing practice that discovers failure modes before they discover production.

This module covers three patterns and one practice. **Failure detection** (Lesson 1) is the primitive every higher-level recovery mechanism depends on; without an accurate liveness signal, elections happen for no reason and real failures take too long to surface. **Circuit breakers and bulkheads** (Lesson 2) bound the resource coupling that turns a single slow downstream into a constellation-wide cascade. **Chaos engineering** (Lesson 3) is the discipline that closes the inevitable gap between the failure modes you designed against and the failure modes the system will actually encounter.

The lessons are synthesis-heavy compared to Modules 1–3, because the canonical references (DDIA Ch 9 for failure detection; *Release It!* and the Hystrix design documents for resilience patterns; Netflix and FoundationDB writeups for chaos) are scattered across multiple sources. Source notes call out where claims are synthesized versus directly cited.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [Failure Detection — Heartbeats, Timeouts, Phi Accrual](./lesson-01-failure-detection.md) | DDIA Ch. 9 + Chandra-Toueg 1996 + Phi Accrual 2004 |
| 2 | [Circuit Breakers and Bulkheads](./lesson-02-circuit-breakers-bulkheads.md) | *Release It!* + Hystrix design |
| 3 | [Chaos Engineering and Fault Injection](./lesson-03-chaos-engineering.md) | Principles of Chaos Engineering + FoundationDB simulation |

## Project

[Ground Station Failover](./project.md) — a Rust crate that integrates the module's patterns into an operational failover system. Phi accrual failure detection, rate-based circuit breakers, semaphore bulkheads, retry budgets with backoff, and a chaos-injection harness for verifying the integrated system survives the failure modes it was designed for.

## Position

Module 4 of 6 in the Distributed Systems track.

## What You Should Be Able to Do After This Module

- Calibrate a failure detector to the actual latency distribution of the network it monitors, and articulate the tradeoff between detection speed and false-positive rate.
- Recognize resource-coupling failure modes in code by inspection and choose the appropriate isolation pattern (semaphore vs thread-pool bulkhead) for each.
- Configure a circuit breaker with rate-based thresholds, minimum-volume floors, and Half-Open recovery probing.
- Design a retry policy that does not amplify load: exponential backoff, jitter, retry budget, and idempotency requirements.
- Plan a chaos engineering program with a rotating set of experiment categories, measurable success metrics (MTTD, MTTR, findings per experiment), and the operational guardrails to run experiments in production safely.
- Distinguish where chaos engineering applies (service-level failure modes), where deterministic simulation applies (low-level consensus and storage), and where neither replaces good architecture review.

## Source Materials

- **DDIA 2nd Edition** (Kleppmann & Riccomini, 2026), Chapter 9 — covers fault detection and the response to degraded performance. The primary direct source for Lesson 1.
- **Michael Nygard, *Release It!* (2nd ed, Pragmatic Bookshelf, 2018)** — the canonical reference for circuit breaker, bulkhead, and stability patterns. Strongly recommended for engineers operating production systems.
- **Casey Rosenthal & Nora Jones, *Chaos Engineering: System Resiliency in Practice* (O'Reilly, 2020)** — the modern reference for chaos engineering practice.
- **Hayashibara et al., "The φ Accrual Failure Detector"** (SRDS 2004) — for the adaptive failure detection algorithm.
- **Chandra & Toueg, "Unreliable Failure Detectors for Reliable Distributed Systems"** (JACM 1996) — the foundational failure detector taxonomy.
- **Will Wilson, "Testing Distributed Systems w/ Deterministic Simulation"** (Strange Loop 2014) — FoundationDB's approach. Recommended viewing.

> **Track-level synthesis note:** *Foundations of Scalable Systems* — the source book originally planned for parts of this module — was not available during authoring. Lessons 1, 2, and 3 are synthesized from training knowledge plus the cited papers and books. Source-note callouts within each lesson flag specific claims that should be cross-referenced against *Foundations of Scalable Systems* or another systems-scaling reference before publication.
