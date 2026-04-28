# Module 01 — Stream Processing Foundations

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 1 of 6
**Source material:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11; *Streaming Data* — Andrew Psaltis, Chapters 1–3; *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7; *Kafka: The Definitive Guide* — Shapira et al., Chapter 1
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0118**
> **Classification:** PIPELINE STAND-UP
> **Subject:** Heterogeneous sensor ingestion for SDA Fusion Service
>
> Meridian's existing Space Domain Awareness pipeline is a Python script that polls each sensor source on a 30-second cron, batches the results into Parquet, and uploads to S3. End-to-end latency from observation to fused track is currently 4–7 minutes. After last quarter's Cosmos-1408 anti-satellite test, the post-event debris environment requires sub-30-second conjunction detection to maintain constellation safety.
>
> **Directive:** Stand up the front door of the SDA Fusion Service. Three sensor source types (X-band radar arrays, optical telescopes, inter-satellite link feeds) must be ingested as a continuous stream, normalized into a common observation envelope, and forwarded to downstream fusion stages. No more cron, no more batching at the edge.

This module establishes the conceptual and physical foundations of stream processing — what a stream is, what it isn't, and how production systems are built around the source/sink boundary. Every architectural decision that follows in this track (orchestration, windowing, delivery semantics, observability) assumes you can reason fluently about the model introduced here.

The code you write in this module is the literal first stage of the SDA Fusion Service. It will be extended, not replaced, in every subsequent module.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Define what makes a system a *stream processor* rather than a low-latency batch processor, and articulate the operational consequences of the difference
2. Design a common observation envelope that unifies heterogeneous sensor wire formats without losing source-specific provenance
3. Implement async source and sink abstractions in Rust using the dataflow model — operators that consume from upstream and produce to downstream
4. Choose between push, pull, and poll ingestion patterns for a given source based on latency, control, and reliability requirements
5. Reason about the bounded-vs-unbounded data distinction and its implications for memory, completeness, and correctness

---

## Lesson Summary

### Lesson 1 — Streams, Sources, and Sinks

The conceptual model. What distinguishes a stream from a queue, a log, or a continuously polled batch. The source/sink abstraction as the boundary of every streaming pipeline. Bounded vs unbounded data and why the distinction shapes the entire system. The observation envelope pattern for unifying heterogeneous sources.

*Key question:* If a sensor produces a fixed dataset of 10 million observations from a one-time fragmentation event, is processing that dataset a stream or a batch problem?

### Lesson 2 — The Dataflow Model

The streaming abstraction in production systems. Operators as functions over streams: map, filter, fold, merge, partition. Why dataflow composition beats imperative loops for pipeline code. The graph topology — sources at the edges, sinks at the other edges, operators in between. State, statelessness, and where state lives in a streaming pipeline.

*Key question:* Why does the dataflow model treat the pipeline as a graph that runs continuously rather than as a function that is called with a batch of inputs?

### Lesson 3 — Push, Pull, and Poll Semantics

The three patterns by which data enters and traverses a pipeline. Push (the source initiates and forwards), pull (the consumer requests and receives), poll (the consumer requests on a schedule). Why Kafka consumers poll rather than subscribe to a callback. Where each pattern fits in the SDA fusion topology — radar arrays push, optical archives pull, ISL beacons poll. The hidden cost of polling and the hidden risk of pushing.

*Key question:* The optical telescope archive exposes only an HTTP REST endpoint with no notification mechanism. How should the ingestion service interact with it, and what are the operational consequences?

---

## Capstone Project — SDA Sensor Ingestion Service

Build the front door of the SDA Fusion Service. Three async source tasks (radar, optical, ISL) consume from their respective wire formats, normalize observations into a common `Observation` envelope, and forward them to a shared sink. The sink writes a structured event log that downstream stages will consume. Acceptance criteria, suggested architecture, and the full project brief are in `project-sensor-ingestion.md`.

This is the module where the SDA Fusion Service begins to exist. Every subsequent module's project extends what you build here.

---

## File Index

```
module-01-stream-processing-foundations/
├── README.md                              ← this file
├── lesson-01-streams-sources-sinks.md     ← Streams, sources, sinks
├── lesson-01-quiz.toml                    ← Quiz (5 questions)
├── lesson-02-dataflow-model.md            ← The dataflow model
├── lesson-02-quiz.toml                    ← Quiz (5 questions)
├── lesson-03-push-pull-poll.md            ← Push, pull, and poll semantics
├── lesson-03-quiz.toml                    ← Quiz (5 questions)
└── project-sensor-ingestion.md            ← Capstone project brief
```

---

## Prerequisites

* Foundation Track completed (all 6 modules) — async Rust, channels, network programming, and data layout are assumed
* Familiarity with `tokio`, `tokio::sync::mpsc`, `serde`, and `anyhow::Result`
* Working understanding of TCP, UDP, and HTTP — the three transport types you'll deal with in the project

## What Comes Next

Module 2 (Pipeline Orchestration Internals) takes the source-to-sink primitives you build here and composes them into a multi-stage DAG with a real task scheduler. The `Observation` envelope you define in this module is the data structure that flows through every subsequent stage of the pipeline.
