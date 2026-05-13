# Capstone — Artemis Archive Parquet Writer

**Module:** Data Lakes — M01: Columnar Storage Foundations
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%).

---

## Mission Briefing

### From: Artemis Cold Archive Lead

```
ARCHIVE BRIEFING — RC-2026-04-DL-001
SUBJECT: Artemis cold-archive ingestion writer, replacement for legacy
         JSONL pipeline.
PRIORITY: P1 — current pipeline is the bottleneck for analyst query SLA.
```

The legacy archive writes downlinked telemetry as gzipped JSONL into the cold-storage object bucket. Writes are fast; reads are the problem. The analyst query SLA is "ninety-fifth percentile query under thirty seconds"; we are currently at "fiftieth percentile query under fifteen minutes." The bottleneck is the row-oriented, parse-every-byte storage layout. Three weeks ago we approved the migration to Parquet for the cold archive, and this is the writer that produces the new files.

The writer ingests record batches from the ground-segment handoff (already Arrow-formatted upstream — your input is `RecordBatch`, not JSONL) and produces Parquet files into the cold-archive bucket. The files this writer produces are the files Module 2's table-format layer will wrap into Iceberg tables, Module 3's partition strategy will organize, and Module 5's query engine will read against the analyst workload. Your writer is the foundation of the rest of the track.

This is not a research project. The writer's design decisions — row group size, per-column encoding, compression codec — are operational decisions with measurable consequences. Document the decisions and the evidence behind them; the engineering review at the end of this project will read that documentation.

---

## What You're Building

A Rust crate, `artemis-archive-writer`, that exposes:

- A `Writer` struct, constructed from an output path and an Arrow `SchemaRef`, that accepts `RecordBatch`es via a `write(&mut self, batch: &RecordBatch) -> Result<()>` method and a `close(self) -> Result<WriteSummary>` method.
- A `WriteSummary` containing the output file path, total rows written, total compressed bytes, row group count, and per-column encoding choices.
- A CLI binary, `artemis-write`, that reads Arrow IPC files from stdin (or a path argument) and writes a Parquet file to a configurable output path, suitable for use in the ingestion pipeline.
- A configuration module that holds the per-column encoding map and is unit-tested against the canonical Artemis schema.

The writer must produce files that are readable by the standard `parquet` and `parquet-arrow` Rust crates and by `pyarrow` from Python — the cross-language readability is what makes the cold archive useful to the analyst tooling. Your tests must verify both.

---

## Functional Requirements

1. **Streaming ingestion.** The writer accepts record batches incrementally; the caller does not have to materialize all data before calling `close`. The writer's resident memory must stay bounded regardless of total rows written — bounded by the configured row group size, not the input size.
2. **Configured row group size.** The writer flushes a row group when the buffered byte estimate exceeds the configured target (default 128 MB). The actual size is approximate because flushes happen at record-batch boundaries.
3. **Per-column encoding map.** The writer applies the encoding map from Lesson 2: `DELTA_BINARY_PACKED` for `sample_timestamp_ns` and `sequence_number`, `BYTE_STREAM_SPLIT` for `panel_voltage` and `temperature_c`, `DELTA_BYTE_ARRAY` for `archive_uri` and `object_path`, dictionary (default) for `mission_id`, `payload_id`, `sensor_kind`, and `quality_flag`.
4. **ZSTD compression at level 3** as the file-level codec.
5. **Statistics enabled on numeric columns** to support downstream predicate pushdown.
6. **Atomic visibility.** The writer writes to `<output_path>.inprogress` and renames to `<output_path>` only on successful `close()`. A crashed writer produces a file readers never see.
7. **Observability hooks.** The writer emits structured log events on each row-group flush (`rg_idx`, `rows`, `compressed_bytes`, `elapsed_ms`) and on dictionary-fallback detection (`column`, `row_group`). The format is a single JSON object per line, written to a configurable `tracing` subscriber.

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] The writer accepts `RecordBatch`es via `write()` and produces a valid Parquet file via `close()` against the canonical Artemis test schema (defined in `tests/schema.rs`).
- [x] The output file is readable by `parquet::arrow::ParquetRecordBatchReaderBuilder` and round-trips every column's values bit-exact for primitive types and byte-exact for variable-length types.
- [x] The output file is readable by `pyarrow.parquet.ParquetFile` (verified via a test harness that shells out to a small Python helper); the Python read produces the same row count and per-column null count as the Rust read.
- [x] For an input of 10 million rows with the canonical schema, the output file has at least 3 row groups when configured for 128 MB target, demonstrating that row-group flushing works.
- [x] The output file's footer reports `DELTA_BINARY_PACKED` for `sample_timestamp_ns`, `BYTE_STREAM_SPLIT` for `panel_voltage`, and a dictionary encoding for `mission_id` in every row group's column chunk metadata.
- [x] An interrupted write (simulated by dropping the `Writer` without calling `close`) leaves no file at the final output path; the `.inprogress` file is present and is detectably invalid as Parquet (the standard reader rejects it).
- [x] The compressed output file is at least 5× smaller than the same data written as gzipped JSONL via a baseline writer in `tests/baseline_jsonl.rs`.
- [x] The writer's resident memory, measured by `jemalloc-ctl::stats::resident` at peak during a 10-million-row write, does not exceed 1.5× the configured row-group target.

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The per-column encoding map is justified in `docs/encoding-decisions.md` against measured compression ratios on the canonical test data. Each non-default encoding choice has a one-paragraph rationale and a measurement.
- [ ] *(self-assessed)* The row-group-size choice is justified in `docs/row-group-decision.md` against the writer-memory-budget constraint and a reference analyst query pattern.
- [ ] *(self-assessed)* The crash-handling discipline (`.inprogress` rename) is justified against the alternative of writing directly to the final path — the doc names at least one failure mode the rename pattern prevents.
- [ ] *(self-assessed)* The writer's API surface (`write`, `close`, `WriteSummary`) is documented with rustdoc comments that explain the memory and atomicity properties from the caller's perspective.

---

## Architecture Notes

The writer is fundamentally a thin layer over `parquet::arrow::ArrowWriter`. The crate the engineer is building is not "a new Parquet writer from scratch" — that is a reasonable exercise but not this exercise. The work is in the configuration discipline (per-column encoding map driven by the schema), the atomic-rename discipline, and the observability hooks. The `ArrowWriter` does the actual Parquet bytes.

A reasonable module layout:

```text
artemis-archive-writer/
├── src/
│   ├── lib.rs              # public API: Writer, WriteSummary, error types
│   ├── config.rs           # encoding map, row group size, codec, statistics
│   ├── writer.rs           # the Writer impl wrapping ArrowWriter
│   ├── observability.rs    # tracing-subscriber events for flush and fallback
│   └── bin/artemis-write.rs  # CLI binary
├── tests/
│   ├── schema.rs           # canonical Artemis test schema
│   ├── baseline_jsonl.rs   # gzipped-JSONL baseline writer for size comparison
│   ├── roundtrip.rs        # write-then-read tests
│   └── pyarrow_compat.rs   # Python-helper shell-out test
└── docs/
    ├── encoding-decisions.md
    └── row-group-decision.md
```

The canonical Artemis schema includes at least:

| Column | Arrow type | Encoding |
| --- | --- | --- |
| `sample_timestamp_ns` | `Timestamp(Nanosecond, None)` | `DELTA_BINARY_PACKED` |
| `mission_id` | `Utf8` | dictionary (default) |
| `payload_id` | `UInt32` | dictionary (default) |
| `sensor_kind` | `Utf8` | dictionary (default) |
| `sequence_number` | `UInt64` | `DELTA_BINARY_PACKED` |
| `panel_voltage` | `Float64` | `BYTE_STREAM_SPLIT` |
| `temperature_c` | `Float64` | `BYTE_STREAM_SPLIT` |
| `pressure_kpa` | `Float64` | `BYTE_STREAM_SPLIT` |
| `quality_flag` | `UInt8` | dictionary (default) |
| `archive_uri` | `Utf8` | `DELTA_BYTE_ARRAY` |
| `object_path` | `Utf8` | `DELTA_BYTE_ARRAY` |
| `notes` | `Utf8` (nullable) | dictionary (default; falls back to plain) |

---

## Hints

<details>

<summary>Hint 1 — Per-column encoding configuration in Rust</summary>

The `WriterProperties` builder method `set_column_encoding` takes a `ColumnPath`, which is constructed from a `Vec<String>`. For top-level columns the path has one element. Remember that overriding the encoding does not by itself disable Parquet's dictionary attempt — you need `set_column_dictionary_enabled(path, false)` for columns where the override should bypass dictionary entirely (delta-encoded timestamps, byte-stream-split floats).

</details>

<details>

<summary>Hint 2 — The atomic rename pattern</summary>

The standard library's `std::fs::rename` is atomic *on the same filesystem* on Linux and macOS. The writer should construct the output file with a `.inprogress` suffix appended to the configured path, write into it, call `ArrowWriter::close`, and then rename. The rename happens after the writer's `Drop` runs but before the caller's `close()` returns the `WriteSummary`. Be explicit about the ordering: `close()` returns `Ok` only after the rename succeeds.

</details>

<details>

<summary>Hint 3 — Verifying the per-column encoding choice in tests</summary>

After writing a test file, open it with `SerializedFileReader` and walk `metadata.row_groups()[i].columns()`. Each `ColumnChunkMetaData` has an `encodings()` method returning the encodings the chunk actually used. For columns with a non-default encoding, you should see exactly the configured encoding (no `PLAIN_DICTIONARY`/`RLE_DICTIONARY` entry alongside it). For columns left at default, you will typically see `PLAIN_DICTIONARY` *and* `PLAIN` if dictionary fallback occurred — the `nullable` test column with high-cardinality values is the case that exercises fallback detection.

</details>

<details>

<summary>Hint 4 — Cross-language verification with pyarrow</summary>

The `pyarrow_compat.rs` test does not need a full Python integration. A simple `std::process::Command::new("python3").arg("-c").arg(script)` invocation where `script` is a one-liner that opens the file with `pyarrow.parquet.ParquetFile` and prints the row count and the per-column null counts is enough. Parse the stdout in Rust and compare to the Rust-side read. If `python3` is not present on the test runner, `#[cfg_attr(not(feature = "pyarrow-test"), ignore)]` lets you keep the test in the suite without requiring Python in CI.

</details>

<details>

<summary>Hint 5 — Bounded writer memory under a long write</summary>

The `ArrowWriter` buffers an entire row group's worth of data in memory before flushing. With `max_row_group_size = 128 MB`, the writer's peak memory should be ~1.2× the row group size (the row group plus encoding scratch). If your test shows higher peak memory, check that the test isn't accidentally retaining the input `Vec<RecordBatch>` — a streaming iterator that yields each batch once and is consumed-then-dropped is what produces the bounded-memory shape. The `jemalloc-ctl` measurement is taken at the `write()` call boundary, not after holding all batches in a vector.

</details>

---

## References

- *In-Memory Analytics with Apache Arrow* (Topol), Chapter 3 — "Format and Memory Handling"
- Apache Parquet specification, particularly the Encodings document (`github.com/apache/parquet-format/blob/master/Encodings.md`)
- `parquet` crate documentation (`docs.rs/parquet`) — `WriterProperties`, `ArrowWriter`, `ColumnPath`
- `arrow` crate documentation (`docs.rs/arrow`) — `RecordBatch`, `Schema`, primitive array builders

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI and the four self-assessed docs are written. Open a draft PR against the `meridian-data-systems/archive-writer` repo with the implementation, the tests, and the docs. The review will read the docs first; the docs are how the next engineer who touches this code understands the decisions you made.
