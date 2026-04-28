# Meridian Space Academy

Meridian Space Academy is the internal engineering training program for Meridian Space Systems. It exists to bring senior engineers — most arriving from web, fintech, or systems backgrounds where Rust's role is established but the operational domain is not — up to working competence on the production data systems that power Meridian's flight operations.

The curriculum is organized as five tracks of six modules each. Every module is grounded in a real piece of Meridian's stack: a service we operate, a pipeline we maintain, or a system we are actively rebuilding. The lessons read like internal engineering documentation because that is what they are.

---

## The mission

Meridian operates a 48-satellite Earth observation constellation, twelve ground station sites distributed across four continents, and a forward operations node at the Artemis lunar base. The original control plane was a Python codebase written for a six-satellite constellation in 2018. It does not scale. Single-threaded I/O loops cannot keep up with the uplink and downlink schedule of forty-eight active vehicles, the GIL prevents the parallelism we need on a fundamentally I/O-bound workload, and the type system has let an unmanageable amount of operational fragility accumulate over six years.

The replacement is being written in Rust. This curriculum is how new engineers — and existing engineers moving into the platform team — develop the mental model required to contribute to that replacement without making the system worse.

---

## Prerequisites

Three or more years of Rust experience. The curriculum assumes fluency with ownership, borrowing, lifetimes, trait bounds, generics, error handling with `Result`, and Cargo workspaces. There are no introductory lessons on these topics; references are made as needed but the basics are not re-taught.

Engineers who have not yet shipped Rust to production should complete the Rustlings exercises and read *Programming Rust* (Blandy, Orendorff, Tindall) before starting. This is not a beginner curriculum, and starting it without the prerequisites in place produces frustration without learning.

---

## The five tracks

### Foundation — Mission Control Platform *(shipped)*

Async Rust, concurrency, message passing, networking, data layout, and performance profiling, anchored in the rebuild of the legacy Python control plane. Modules cover the `tokio` runtime and task lifecycle, shared-state vs. message-passing concurrency, channel patterns for telemetry fan-in and fan-out, TCP and UDP servers for ground station traffic, cache-friendly data layouts for hot paths, and CPU and allocation profiling. Foundation is the prerequisite for every other track.

### Database Internals — Orbital Object Registry *(shipped)*

A storage engine for the Orbital Object Registry — the system of record for every tracked orbital object, its TLE history, and the conjunction analyses computed against it. Modules cover page-level storage and the buffer pool, B+ tree indexes, LSM trees and compaction, write-ahead logging and crash recovery, transactions and isolation levels, and query processing with the Volcano model. The capstone projects compose into a working single-node OLTP storage engine over the course of the track.

### Data Pipelines — Space Domain Awareness Fusion *(in progress — Module 1 shipped)*

Real-time sensor fusion across radar, optical, and inter-satellite link feeds, producing the conjunction alerts that flight ops acts on. Modules cover stream processing semantics, pipeline orchestration internals, watermarks and event-time, exactly-once delivery, and backpressure. The mission framing is the SDA Fusion service that ingests tens of thousands of observations per second from heterogeneous sources and emits a unified, deduplicated catalog.

### Data Lakes — Artemis Base Cold Archive *(planned)*

A versioned mission-data lakehouse for the Artemis base archive. Modules cover columnar formats, table format internals (Parquet and Iceberg), partition layout and clustering, compaction and table maintenance, and time-travel and lineage. The mission framing is the cold-archive storage tier for high-resolution sensor data that flows down from the lunar base on a multi-day cadence and must remain queryable for the operational lifetime of the program.

### Distributed Systems — Constellation Network *(planned)*

Consensus, replication, and partitioning across the 48-satellite grid and its ground footprint. Modules cover failure detection, leader election and Raft, replication and quorum, sharding and rebalancing, and split-brain recovery. The mission framing is the inter-satellite coordination layer that maintains constellation-wide state — orbital schedules, contact windows, downlink priorities — without a stable connection to any single ground site.

---

## Recommended order

Foundation first. Every other track depends on async Rust, channels, and the data-oriented performance habits introduced there.

After Foundation, the Database Internals, Data Pipelines, Data Lakes, and Distributed Systems tracks may be taken in any order. They reference one another where useful — the Database track's WAL chapter is referenced from the Distributed Systems track's replication chapter, for example — but each is self-contained and does not require the others as a prerequisite.

---

## How a module works

Every module follows the same structure:

- **Three lessons.** Written readings with code examples and source citations where applicable. Lessons that synthesize from training knowledge rather than a primary source are flagged with a `Source note` callout at the top.
- **Three quizzes.** One quiz at the end of each lesson, mixing multiple choice, free response, and Rust tracing problems. A score of seventy percent or higher is required to mark a lesson complete.
- **One capstone project brief.** A production-realistic engineering task that exercises the module's material. Projects are scoped to one to two weeks of focused work and include explicit acceptance criteria, suggested architecture, and the relevant Meridian operational context.

A module is complete when all three lessons are passed and the capstone project brief has been worked through end to end. The capstone is not optional reading; it is where the module's material becomes a real system.

---

## Source material

Each lesson cites the primary text it draws from. The core references across the curriculum are *Async Rust* (Flitton, Morton), *Database Internals* (Petrov), *Designing Data-Intensive Applications* (Kleppmann), *The Linux Programming Interface* (Kerrisk), and the Raft and Spanner papers. Lessons that synthesize beyond a primary source are explicitly marked.
