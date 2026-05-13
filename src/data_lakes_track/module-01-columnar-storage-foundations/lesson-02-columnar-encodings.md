# Lesson 2 — Columnar Encodings

**Module:** Data Lakes — M01: Columnar Storage Foundations
**Position:** Lesson 2 of 3
**Source:** *In-Memory Analytics with Apache Arrow* — Matthew Topol, Chapter 1 ("Getting Started — Dictionary-Encoded and Run-End-Encoded Arrays") and Chapter 3 ("Long-Term Storage Formats"); *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 4 ("Column Compression"); Apache Parquet specification (`github.com/apache/parquet-format/blob/master/Encodings.md`).

---

## Context

Lesson 1's writer flushed 128 MB row groups out to disk, one column chunk per column. What lives inside a column chunk is not the raw values — it is encoded bytes. Parquet's compression ratio is the product of two passes: an **encoding** pass that exploits the column's structure (repeated values, sortedness, small dynamic range), and a **compression** pass that hits the residual entropy with a general-purpose codec (ZSTD, SNAPPY, LZ4). For the Artemis archive the encoding pass is where most of the size savings live. The same telemetry data that was 8 GB as gzipped JSONL is 800 MB as Parquet with default encodings — and 480 MB with per-column encodings chosen deliberately. The difference between the default and the deliberate is one engineer-day of effort, and it pays back forever in storage cost and read I/O.

Encoding choice is also where the writer's per-column understanding of the data matters most. The dictionary encoding that wins on `payload_id` (8 distinct values across a row group) loses on `sample_timestamp` (no repeats; every value distinct). The delta encoding that wins on `sample_timestamp` (monotonically increasing, small differences) loses on `panel_voltage` (random float, no useful delta structure). The byte-stream-split encoding that wins on `panel_voltage` is a no-op on `payload_id`. There is no single best encoding — there are right answers per column, and the writer's job is to know which is which.

This lesson develops the encoding pipeline end to end. The encoding-then-compression order and why it is in that order. The four encodings that cover ninety-five percent of real workloads — dictionary, RLE, delta, byte-stream-split — and the access patterns each is built for. The interaction between encoding choice and Parquet's dictionary fallback behavior. And the practical question the writer faces on every column: what encoding to pick, and how to tell when the default was wrong.

---

## Core Concepts

### The Encoding–Compression Pipeline

A Parquet column chunk is produced in two passes. The **encoding** pass takes the column's values and emits a byte stream that exploits structure in those values — repeated values become indices into a dictionary, sorted integers become deltas from a base value, small integers become bit-packed. The **compression** pass takes the encoded byte stream and applies a general-purpose codec (ZSTD, SNAPPY, LZ4, GZIP) to squeeze the residual entropy. Encoding then compression — never the reverse. Compressing first would destroy the structure the encoder relies on; the encoder's win is over raw values, not over already-compressed bytes.

The order matters operationally. The encoder is the lever the writer controls most directly per column. The compressor is a global file-level choice. ZSTD-3 is the Artemis default because it gives compression ratios within five percent of GZIP at one-fifth the CPU cost, and substantially better ratios than SNAPPY at modest CPU cost. The compressor matters; the encoder matters more.

A second important property: **decoding is a streaming, per-page operation**. The reader does not decompress an entire column chunk in one go. It decompresses one page at a time, decodes its values, emits them to the consumer, and discards the decoded buffer. This is what keeps the reader's memory footprint bounded regardless of column chunk size.

### Dictionary Encoding

Dictionary encoding is the encoding to know first because it is the encoding Parquet picks by default for most columns and the one that produces the largest single ratio improvement on real data.

The mechanic is direct. The encoder builds a **dictionary** of the distinct values seen in the column chunk (or row group; the level varies by Parquet version), assigns each distinct value an integer index, and writes the column's data pages as a sequence of integer indices into the dictionary. The dictionary itself lives in a separate **dictionary page** at the start of the column chunk. A column with eight distinct values across two million rows compresses to two million small integers plus eight values' worth of dictionary — roughly two orders of magnitude smaller than the raw values, before the compressor even runs.

The encoder's index stream is itself encoded: typically as bit-packed RLE (covered next), because the indices are by construction small integers. Topol (Ch. 1) makes the same observation about Arrow's `DictionaryArray` type — dictionary encoding is "an optional property on any array" and is the obvious choice "in the case of an array with a lot of repeated values."

Parquet's dictionary encoding has a built-in **fallback rule** the writer must understand. The encoder maintains a dictionary size cap (default 1 MB). If the dictionary fills during encoding, the writer falls back to plain encoding for the remainder of the column chunk. This means a column that is dictionary-friendly for the first million rows but suddenly explodes into high cardinality will produce a mixed-encoding chunk that is no better than plain. The Artemis writer monitors per-column dictionary-fallback events and surfaces them as a structured log; persistent fallbacks on a column are the signal that the column should switch to a different encoding or that the dictionary cap should be raised.

The columns that dictionary-encode well, in the Artemis schema: `mission_id` (~40 distinct), `payload_id` (~8 distinct), `sensor_kind` (~12 distinct), `quality_flag` (~5 distinct). Together these four columns are sixty percent of the row width and produce ninety percent of dictionary's benefit.

### Run-Length Encoding and Bit-Packing

Run-length encoding and bit-packing are a pair, and Parquet uses them together in a hybrid called **RLE_BITPACK**. The hybrid is what encodes the integer indices produced by dictionary encoding, the definition and repetition levels used to represent nullability and nested data, and any small-integer column the writer specifies it for.

RLE on its own encodes runs of identical values as `(run_length, value)` pairs. A column with `[7, 7, 7, 7, 7, 7, 7, 7]` becomes `(8, 7)` — two integers instead of eight. RLE wins on data with long runs of the same value: dictionary indices on a sorted column, boolean flags that are mostly `false`, dictionary indices when the dictionary is small and adjacent rows tend to share values.

Bit-packing encodes a sequence of small integers by storing them in fewer bits than the value's nominal width allows. A `u32` column whose values all fit in five bits is bit-packed into five-bit slots — six and a half bits saved per value. Bit-packing wins on small-dynamic-range integers: dictionary indices when the dictionary is dense and adjacent rows are *unlike* (no runs to exploit), short-count columns, and any small enum's underlying integer representation.

The hybrid switches between the two based on which is locally cheaper, in eight-value or thirty-two-value batches depending on the encoding variant. Kleppmann (Ch. 4) makes the same point about bitmap encoding's adaptive hybrid in the data warehouse world: "techniques such as roaring bitmaps switch between the two bitmap representations, using whichever is the most compact." Parquet's RLE_BITPACK is the same idea applied to integer streams.

The takeaway for the writer: RLE_BITPACK is not a knob to flip on or off. It is the encoding Parquet uses for dictionary indices and for the definition/repetition level streams that every nullable or nested column carries. The writer's lever is whether the *values* go through dictionary first (and then RLE_BITPACK on the indices) or go through some other encoding directly.

### Delta Encoding

Delta encoding is the right answer for monotonically advancing integer columns. The most common cases in telemetry: `sample_timestamp` (monotonic per source, never repeats, small inter-sample gaps), `sequence_number` (strictly increasing per stream), `tle_epoch` (sortable timestamp). For these columns dictionary encoding is useless — there are no repeated values — and plain encoding gives no compression. Delta encoding gives both.

The mechanic: the encoder stores a base value (the first value) and a sequence of deltas (`value[i] - value[i-1]`). The deltas are typically tiny — for a 100 Hz sensor stream, the delta between consecutive samples is exactly 10 ms in nanoseconds, which is a constant. Parquet's `DELTA_BINARY_PACKED` encoding stores the deltas using a frame-of-reference plus bit-packing scheme: deltas are stored relative to a frame minimum, which keeps the per-delta bit width small even when the absolute delta values are large.

The wins are dramatic for the right columns. An 8-byte nanosecond timestamp column with 10 ms gaps between samples compresses to roughly 1 byte per value before the general-purpose compressor runs. After ZSTD-3 the same column is often under 0.5 bytes per value. Dictionary encoding on the same column produces zero benefit because every value is distinct.

Parquet also offers `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` for string columns. The former encodes the *lengths* of strings as deltas (useful when strings have similar lengths) while keeping the string bytes plain; the latter encodes lengths *and* shared prefixes (incremental encoding — store only the bytes that differ from the previous string). `DELTA_BYTE_ARRAY` is the right answer for sorted string columns like file paths, URLs, or hierarchical identifiers — a column of object-storage paths with shared prefixes can compress to 5–10% of its plain size.

### Byte Stream Split

Byte stream split is the encoding floating-point columns need. The problem: dictionary encoding fails on floats (every value is distinct), delta encoding fails on floats (the deltas are also floats, and float-to-float subtraction does not produce compressible structure), and plain encoding of a `Float64` column is exactly eight bytes per value with no win available.

The byte-stream-split trick: take a `Float64` value's eight bytes and split them across eight parallel streams — all the first bytes together, all the second bytes together, and so on. The high-order bytes of float values that share a magnitude (typical of physical measurements) cluster together; the resulting streams are highly compressible because adjacent values' corresponding bytes are often equal or nearly so. The encoded byte count is the same as plain, but the structure that emerges through the splitting compresses substantially better under the general-purpose codec that runs next.

For the Artemis schema, byte-stream-split is the right encoding on `panel_voltage`, `temperature_c`, `pressure_kpa`, and every other physical-measurement float column. The compression-ratio improvement over plain-encoded float, after ZSTD, is typically two to three times.

### Picking an Encoding per Column

The writer's per-column decision is a short table of rules, derived from the column's value distribution and access pattern. The Artemis writer's per-column encoding map looks like this:

| Column kind | Example | Encoding | Why |
| --- | --- | --- | --- |
| Low-cardinality categorical | `mission_id`, `payload_id`, `sensor_kind` | Dictionary (default) | Repeats are the entire structure of the column. |
| Boolean or small enum | `quality_flag`, `is_eclipsed` | Dictionary + RLE_BITPACK on indices | Trivially small dictionary; long runs in time-ordered data. |
| Monotonic integer | `sample_timestamp_ns`, `sequence_number` | DELTA_BINARY_PACKED | Inter-sample deltas are tiny and regular. |
| Sorted string | `object_path`, `archive_uri` | DELTA_BYTE_ARRAY | Shared prefixes are the structure. |
| Physical-measurement float | `panel_voltage`, `temperature_c` | BYTE_STREAM_SPLIT | High-order-byte clustering after the split. |
| High-cardinality string with no structure | `event_uuid`, `request_id` | PLAIN | Nothing to exploit; just compress the bytes. |

The defaults the writer is not overriding — dictionary for categorical, dictionary fallback to plain for high-cardinality — handle the common cases. The overrides are for timestamps, sorted strings, and floats. Three rules of thumb, three explicit settings in the writer, and the writer is producing files that compress 50–80% better than defaults on Artemis data.

The rule of thumb the writer must never forget: **measure**. The right encoding for the data you assume you have may be the wrong encoding for the data you actually have. Sample a row group, write it with three candidate encodings, and pick by measured compressed size. The Artemis ingestion pipeline runs a weekly encoding-audit job that does exactly this on the latest week of downlinks and surfaces any column whose chosen encoding is no longer optimal.

---

## Code Examples

### Inspecting the Encodings a File Uses

Before tuning the writer, the engineer needs to know what encodings the current files use. The footer records the encodings per column chunk. The `parquet` crate exposes this through the column chunk metadata.

```rust,ignore
use std::fs::File;

use anyhow::Result;
use parquet::basic::Encoding;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Print the encoding(s) used by every column chunk in every row group.
/// A column chunk lists the encodings it actually uses — typically the
/// data-page encoding plus the dictionary-page encoding if dictionary
/// encoding was selected.
fn inspect_encodings(path: &str) -> Result<()> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        println!("row group {}:", rg_idx);
        for col in rg.columns() {
            let encodings: Vec<&Encoding> = col.encodings().iter().collect();
            let dict_fallback = encodings.contains(&&Encoding::PLAIN)
                && (encodings.contains(&&Encoding::PLAIN_DICTIONARY)
                    || encodings.contains(&&Encoding::RLE_DICTIONARY));
            println!(
                "  {:<24} compressed={:>10} encodings={:?}{}",
                col.column_path().string(),
                col.compressed_size(),
                encodings,
                if dict_fallback { "  [DICT-FALLBACK]" } else { "" },
            );
        }
    }
    Ok(())
}
```

The pattern to notice. A column chunk whose `encodings()` list contains *both* a dictionary encoding (`PLAIN_DICTIONARY` or `RLE_DICTIONARY`) and `PLAIN` is a column chunk that started dictionary-encoding and **fell back** to plain partway through, because the dictionary hit its size cap. Flagging these is the cheapest possible writer-side observability — if a column persistently dictionary-falls-back, either its cardinality is higher than the writer assumes (raise the cap or switch encoding) or the column has changed shape upstream. The Artemis writer emits a `dict_fallback{column=$col}` Prometheus counter from this path so the encoding-audit job can detect drift.

### Setting Per-Column Encodings in the Writer

The `WriterProperties` builder accepts per-column-path overrides via `set_column_encoding`. Use this for the columns whose ideal encoding the writer knows in advance, and leave the default (`PLAIN_DICTIONARY` with fallback to `PLAIN`) for everything else.

```rust,ignore
use std::sync::Arc;

use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// Build the Artemis writer properties with per-column encodings. The
/// columns listed here are the ones where the writer's understanding of
/// the data beats the default; every other column gets dictionary with
/// fallback to plain, which is correct for low-cardinality categoricals.
fn artemis_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_size(128 * 1024 * 1024)
        .set_compression(Compression::ZSTD(Default::default()))

        // Monotonic timestamps — DELTA_BINARY_PACKED. The dictionary
        // default fails because every timestamp is distinct.
        .set_column_encoding(
            ColumnPath::new(vec!["sample_timestamp_ns".to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["sample_timestamp_ns".to_string()]),
            false, // disable dictionary explicitly; delta is the encoding.
        )

        // Sorted strings with shared prefixes — DELTA_BYTE_ARRAY.
        .set_column_encoding(
            ColumnPath::new(vec!["archive_uri".to_string()]),
            Encoding::DELTA_BYTE_ARRAY,
        )
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["archive_uri".to_string()]),
            false,
        )

        // Physical-measurement floats — BYTE_STREAM_SPLIT.
        .set_column_encoding(
            ColumnPath::new(vec!["panel_voltage".to_string()]),
            Encoding::BYTE_STREAM_SPLIT,
        )
        .set_column_encoding(
            ColumnPath::new(vec!["temperature_c".to_string()]),
            Encoding::BYTE_STREAM_SPLIT,
        )

        // Everything else: dictionary (default) with statistics for
        // row-group-level predicate pushdown downstream.
        .set_statistics_enabled(
            parquet::file::properties::EnabledStatistics::Page,
        )
        .build()
}
```

Two subtleties. First, `set_column_encoding` does not automatically disable dictionary encoding for that column — the writer would otherwise try dictionary first and only fall back to the configured encoding if the dictionary failed. The explicit `set_column_dictionary_enabled(_, false)` is required to skip the dictionary attempt entirely on columns where it would be wasted. Second, the column path is a `Vec<String>` to handle nested schemas — top-level columns are single-element paths, fields inside structs are multi-element paths. The Artemis writer's encoding map lives in a separate config module and is applied programmatically from the column metadata, not hardcoded as in the example here.

### Measuring Encoding Choices on Real Data

The encoding-audit job is what closes the loop. It samples one row group of recent data, writes it with each candidate encoding for each column, and reports the compressed size per choice. The function below sketches the per-column part of the audit: write the same `RecordBatch` with two different encodings and report the byte count.

```rust,ignore
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// Write a single record batch with the given encoding for the named
/// column and return the resulting file size in bytes. Used by the
/// encoding-audit job to compare candidate encodings on real data.
fn measure_encoding(
    batch: &RecordBatch,
    column_name: &str,
    encoding: Encoding,
    use_dictionary: bool,
) -> Result<u64> {
    let path = format!("/tmp/audit-{column_name}-{encoding:?}.parquet");
    let file = std::fs::File::create(&path)?;

    let column_path = ColumnPath::new(vec![column_name.to_string()]);
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_column_encoding(column_path.clone(), encoding)
        .set_column_dictionary_enabled(column_path, use_dictionary)
        .build();

    let mut writer = ArrowWriter::try_new(
        file,
        batch.schema(),
        Some(props),
    )?;
    writer.write(batch)?;
    writer.close()?;

    let size = std::fs::metadata(&path)?.len();
    std::fs::remove_file(&path)?;
    Ok(size)
}

/// Run the audit for a column: try the candidate encodings and return
/// the one with the smallest compressed output.
fn audit_column(batch: &RecordBatch, column_name: &str) -> Result<Encoding> {
    let candidates: &[(Encoding, bool)] = &[
        (Encoding::PLAIN, false),                  // baseline
        (Encoding::RLE_DICTIONARY, true),          // dictionary attempt
        (Encoding::DELTA_BINARY_PACKED, false),    // monotonic-integer attempt
        (Encoding::BYTE_STREAM_SPLIT, false),      // float-clustering attempt
    ];

    let mut best: Option<(Encoding, u64)> = None;
    for &(encoding, use_dict) in candidates {
        // Skip candidates that are not type-applicable; the writer would
        // error otherwise. Production code checks the column's Arrow type
        // against the encoding's compatibility table here.
        match measure_encoding(batch, column_name, encoding, use_dict) {
            Ok(size) => {
                match best {
                    None => best = Some((encoding, size)),
                    Some((_, prev)) if size < prev => best = Some((encoding, size)),
                    _ => {}
                }
            }
            Err(_) => continue, // not applicable to this column's type
        }
    }
    Ok(best.expect("at least PLAIN should succeed").0)
}
```

The interesting part of this code is what is missing from the example. A real audit job runs the candidate encodings concurrently rather than sequentially (each write is independent), filters candidates by type compatibility before attempting them (BYTE_STREAM_SPLIT on a string column is an error, not a graceful no-op), and measures over a representative sample of row groups across the data corpus rather than one batch. The shape of the function is the right shape; the production version is a parallel job that writes its output to a Prometheus gauge and runs weekly. The discipline the audit enforces is the most important thing — measured encoding choices, not assumed encoding choices.

---

## Key Takeaways

- A Parquet column chunk is produced by **encoding then compression**, in that order. The encoder exploits structure (repeats, sortedness, dynamic range); the compressor hits the residual entropy. Encoding choice is where most of the size savings live; ZSTD-3 over SNAPPY is the second-largest lever.
- **Dictionary encoding** is the default for most columns and the right answer for low-cardinality categoricals. Watch for dictionary-fallback events: a column listed with both `RLE_DICTIONARY` *and* `PLAIN` encodings has fallen back partway through and is no longer benefiting from dictionary.
- **Monotonic integer columns** (timestamps, sequence numbers) want `DELTA_BINARY_PACKED`, not dictionary. Dictionary on a column with no repeats is wasted work. Disable dictionary explicitly when overriding the encoding.
- **Physical-measurement float columns** want `BYTE_STREAM_SPLIT`. The trick exploits high-order-byte clustering across values of similar magnitude; the win over plain-encoded floats is typically 2–3x after ZSTD.
- **Sorted string columns with shared prefixes** want `DELTA_BYTE_ARRAY`. Object paths, hierarchical identifiers, and URLs are the typical winners.
- **Measure, don't assume.** Run an encoding-audit job over real data periodically. The right encoding shifts as data shape evolves; the writer's encoding map needs to evolve with it.

{{#quiz quiz-02.toml}}
