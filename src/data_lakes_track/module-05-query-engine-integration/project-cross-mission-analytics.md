# Capstone — Cross-Mission Analytics Engine

**Module:** Data Lakes — M05: Query Engine Integration
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%). The Module 2 (table format), Module 3 (partition + clustering), and Module 4 (replay) capstones are the substrate.

---

## Mission Briefing

### From: Cold Archive Platform Lead

```
ARCHIVE BRIEFING — RC-2026-04-DL-005
SUBJECT: Cross-Mission Analytics Engine — SQL surface over the
         Artemis cold archive for analyst-driven exploration.
PRIORITY: P1 — required for Q3 analytics-portal rollout.
```

The cold archive has a transactional table format (M2), good partitioning and clustering (M3), and a replay engine for time-travel reads (M4). What it does not have is a SQL surface analysts can use directly. The Replay Engine answers specific point-in-time queries; it does not aggregate, join, or filter beyond the table format's pushdown.

The job: build a thin SQL layer over the Module 2-4 substrate so analysts can write `SELECT mission_id, AVG(panel_voltage) FROM sda_observations WHERE day BETWEEN '2024-03-01' AND '2024-03-08' GROUP BY mission_id` and get results. Use Apache DataFusion as the engine — it handles SQL parsing, logical planning, optimization, and physical execution. Your job is the integration shim: a custom `TableProvider` that wires the table format's planning and reading into DataFusion's execution path.

The engine must support the predicate-pushdown contract (Lesson 1's three-way classification), projection pushdown (Lesson 2's column subset selection), and time-travel queries via SQL syntax (`AS OF TIMESTAMP '...'`). It must not blow up memory on large result sets — the streaming discipline from Module 4 carries forward. The capstone is the user-facing tool the analytics portal will call.

---

## What You're Building

A Rust crate, `artemis-analytics`, exposing:

- An `ArtemisCatalog` (implements DataFusion's `CatalogProvider`) that surfaces the Module 5 Lesson 3 catalog's namespaces and tables to DataFusion.
- An `ArtemisTableProvider` (implements DataFusion's `TableProvider`) that wires the Module 2-4 table format into DataFusion's scan path, with predicate pushdown classification (`Exact`/`Inexact`/`Unsupported`) and projection pushdown.
- An `ArtemisExec` (implements DataFusion's `ExecutionPlan`) that returns a `SendableRecordBatchStream` over the scan plan's files, decoding Parquet via Module 1's reader and streaming Arrow batches.
- Time-travel SQL: `SELECT ... FROM table AS OF TIMESTAMP '2024-03-15T12:00:00Z'` and `SELECT ... FROM table AS OF SNAPSHOT 4729` parse to a pinned-snapshot read. Implementation routes through DataFusion's table-function machinery or a query-rewrite step depending on what your DataFusion version supports natively.
- A CLI binary, `artemis-sql`, with a REPL for interactive SQL exploration and a `--query` flag for single-shot queries.
- A small HTTP service exposing the SQL API over Arrow Flight (the Lesson 2 cross-process transport), so the analytics portal can consume results without a Rust client.

The engine must complete a representative analytics workload (the 3,200-query log from Module 3's capstone, projected through a SQL `GROUP BY` step) in under twice the time the Module 3 scan-only benchmark took for the same queries. The 2× overhead budget covers DataFusion's aggregation, materialization, and result-shipping work; if the engine exceeds it, the integration shim has a bug.

---

## Functional Requirements

1. **`CatalogProvider` integration.** DataFusion's `SessionContext::register_catalog` accepts the `ArtemisCatalog`; `SHOW SCHEMAS`, `SHOW TABLES`, and `SELECT * FROM information_schema.tables` work without further setup.
2. **`TableProvider::supports_filters_pushdown`** classifies each filter per Lesson 1's rules. Identity-partition + equality → `Exact`. Day-partition + range → `Exact`. Non-partition column with statistics + comparison → `Inexact`. Function calls on columns → `Unsupported`. Conjunction and disjunction compose per the rules.
3. **`TableProvider::scan`** translates DataFusion's `Expr` filters into the Module 2-4 `Predicate` shape, calls the table format's `plan_scan`, and returns an `ArtemisExec` wrapping the plan.
4. **`ArtemisExec::execute`** returns a `SendableRecordBatchStream` that reads the scan plan's files lazily. The stream applies projection pushdown via the Parquet reader's `ProjectionMask`. Files are read concurrently with a configurable parallelism cap (default 64); batches arrive in unspecified order.
5. **Time-travel via SQL.** `AS OF TIMESTAMP '<rfc3339>'` and `AS OF SNAPSHOT <id>` route the table provider to pin the corresponding snapshot for the duration of the query. The pinned snapshot ID appears in the query's structured logs (Module 4 Lesson 1's discipline).
6. **Snapshot-time vs current schema projection.** Time-travel queries default to snapshot-time projection (Module 4 Lesson 3). A SQL session-level option (`SET artemis.projection_mode = 'current'`) switches the default.
7. **Arrow Flight server.** The `artemis-flight` binary exposes the same SQL surface over the Flight gRPC API. The Flight `DoGet` endpoint streams the query result as an IPC stream; the client sees the same data the in-process API produces.
8. **Streaming memory bound.** Across all query types (point reads, full scans, aggregations), the engine's resident set stays under 4 GB. Aggregations spill to disk via DataFusion's built-in spill mechanism when memory pressure exceeds the threshold.

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] `SHOW TABLES IN artemis.sda` returns the table identifiers registered in the catalog.
- [x] `SELECT COUNT(*) FROM artemis.sda.observations WHERE mission_id = 'apollo-7' AND day = '2024-03-15'` produces the correct count and the query plan (via `EXPLAIN`) shows the predicate pushed exactly to the table provider, with no residual FilterExec for these predicates.
- [x] `SELECT COUNT(*) FROM artemis.sda.observations WHERE UPPER(notes) = 'X'` produces a correct count and the query plan shows a residual FilterExec applied to the table provider's output, with the UPPER predicate not pushed.
- [x] `SELECT * FROM artemis.sda.observations AS OF TIMESTAMP '2024-03-01T00:00:00Z'` reads against the snapshot that was current at that timestamp; the result schema matches the schema-as-of-that-timestamp (snapshot-time projection).
- [x] The same query with `SET artemis.projection_mode = 'current'` returns the result with the current schema, columns added since back-filled as nulls.
- [x] A query `SELECT mission_id, COUNT(*) FROM artemis.sda.observations WHERE day BETWEEN '2024-03-01' AND '2024-03-08' GROUP BY mission_id` completes in under 2× the Module 3 scan-only baseline for the same data range.
- [x] An aggregation query that produces results larger than the configured memory bound spills to disk and completes successfully; the spill is visible in the query's metrics.
- [x] The Flight server returns identical results to the in-process API for a representative query suite (the same 50 queries run via both paths produce byte-identical Arrow IPC streams).

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The pushdown classification is documented in `docs/pushdown-rules.md` with a row per `(column-kind, op, transform)` triple and the corresponding classification. The doc is the artifact a future engineer extending the engine's pushdown will consult.
- [ ] *(self-assessed)* The time-travel SQL extension is documented in `docs/time-travel-sql.md`. The doc describes the syntax accepted, how it parses, how it routes to the pinned snapshot, and the interaction with retention-window expiration.
- [ ] *(self-assessed)* The Flight server's deployment is documented in `docs/flight-deployment.md`. The doc covers the authentication scheme (token-based; how tokens are issued and validated), the rate-limiting strategy (per-token request budget), and the failure modes the operator should monitor.
- [ ] *(self-assessed)* The memory-spill threshold is documented in `docs/spill-tuning.md` against the workload. The doc describes how the threshold was chosen, what queries exercise the spill path, and the performance characteristics with and without spill.

---

## Architecture Notes

A reasonable module layout:

```text
artemis-analytics/
├── src/
│   ├── lib.rs
│   ├── catalog.rs           # ArtemisCatalog (CatalogProvider impl)
│   ├── provider.rs          # ArtemisTableProvider (TableProvider impl)
│   ├── exec.rs              # ArtemisExec (ExecutionPlan impl)
│   ├── pushdown.rs          # classify(), exprs_to_predicate()
│   ├── time_travel.rs       # AS OF TIMESTAMP / AS OF SNAPSHOT parsing
│   ├── stream.rs            # file-list -> SendableRecordBatchStream
│   ├── bin/artemis_sql.rs   # REPL + single-query CLI
│   └── bin/artemis_flight.rs  # Flight server
├── tests/
│   ├── show_tables.rs
│   ├── pushdown_classification.rs
│   ├── time_travel_sql.rs
│   ├── streaming_memory.rs
│   ├── aggregation_spill.rs
│   ├── flight_parity.rs
│   └── workload_bench.rs    # ignored by default; the 2x SLA bench
└── docs/
    ├── pushdown-rules.md
    ├── time-travel-sql.md
    ├── flight-deployment.md
    └── spill-tuning.md
```

The DataFusion version against which to develop is the one currently deployed in the Artemis analytics portal — pin it in `Cargo.toml` rather than tracking head. The DataFusion `TableProvider` trait has evolved across recent versions; the integration code is sensitive to which signatures are current.

The Flight server's `arrow-flight` crate provides the gRPC machinery; the heavy lifting is in the IPC stream production, which reuses the same `RecordBatchStream` the in-process API produces. The Flight handler's job is mostly transport: receive a `Ticket` (DataFusion can encode the SQL query directly), execute it, return the result as an IPC stream over gRPC.

---

## Hints

<details>

<summary>Hint 1 — Translating DataFusion `Expr` to the table format `Predicate`</summary>

DataFusion's filter expressions are `Expr` trees rooted at `BinaryExpr`, `ScalarFunction`, `Column`, etc. The table format's `Predicate` is a smaller AST built around `LeafOp` and conjunctions/disjunctions. Write a recursive translator that walks the `Expr` tree, matches on the node types, and produces the corresponding `Predicate` node — or returns `None` for nodes the table format doesn't support. The translator's failure modes feed directly into the pushdown classification (untranslatable → `Unsupported`).

</details>

<details>

<summary>Hint 2 — DataFusion's `ExecutionPlan` boilerplate</summary>

`ExecutionPlan` requires a fair amount of boilerplate: `schema()`, `output_partitioning()`, `children()`, `with_new_children()`, `execute()`, plus metrics and properties. Most of it is trivial for a leaf scan operator; copy the structure from DataFusion's built-in `ParquetExec` and adapt. The non-trivial method is `execute`, which is where your stream is built. Setting `output_partitioning` to `Partitioning::UnknownPartitioning(n)` where `n` is the number of file-reading tasks is the simplest correct choice; DataFusion's optimizer handles the rest.

</details>

<details>

<summary>Hint 3 — Parsing `AS OF TIMESTAMP` without forking DataFusion</summary>

If your DataFusion version doesn't natively parse `AS OF TIMESTAMP`, the cleanest workaround is a query-rewrite step: intercept the SQL string before parsing, extract any `AS OF` clauses with a regex, replace them with a table-function syntax DataFusion does parse (e.g., `table_at_time('artemis.sda.observations', '2024-03-15T...')`), and register the table function to return an `ArtemisTableProvider` pinned to the resolved snapshot. The user-visible syntax is `AS OF`; the internal mechanism is a table function.

</details>

<details>

<summary>Hint 4 — The 2× SLA benchmark structure</summary>

The benchmark runs the 3,200-query log from Module 3 in two configurations: scan-only (the M3 capstone's `read_plan` + raw Parquet read) and SQL (the M5 capstone's full DataFusion pipeline projecting to a `GROUP BY` aggregation). Both configurations read the same data; the difference is the DataFusion overhead. The 2× budget is generous because DataFusion's aggregation is well-optimized; if your benchmark exceeds 2×, the typical culprits are (a) repeated metadata reads per query when one cache would suffice, (b) inefficient `Expr` translation, (c) projection-pushdown failing silently and the scan returning all columns.

</details>

<details>

<summary>Hint 5 — Flight server's `Ticket` encoding</summary>

The Flight `Ticket` is an opaque byte string the server interprets. The simplest encoding: a JSON object with `{"sql": "SELECT ..."}`. The server deserializes the ticket, runs the SQL through the in-process API, returns the result. More sophisticated deployments encode pre-planned queries or include auth claims; for the capstone the simple JSON pass-through is sufficient. The Flight `GetFlightInfo` endpoint can return a single endpoint pointing at the server itself (no parallelism needed for the capstone's scale).

</details>

---

## References

- Apache DataFusion documentation — `TableProvider` and `ExecutionPlan` traits
- `arrow-flight` crate documentation — server and client patterns
- Apache Iceberg Java reference implementation — for the canonical pushdown classification rules to cross-check against

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI, the four self-assessed docs are written, and the workload benchmark hits the 2× SLA. Module 6 begins with the assumption that this SQL surface is in place; the lifecycle operations Module 6 develops (compaction, snapshot expiration, orphan cleanup) will need their own SQL hooks for operator-driven invocation.
