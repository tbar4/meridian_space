# Lesson 1 — Parquet File Layout

**Module:** Data Lakes — M01: Columnar Storage Foundations
**Position:** Lesson 1 of 3
**Source:** *In-Memory Analytics with Apache Arrow* — Matthew Topol, Chapter 3 ("Format and Memory Handling"); *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 4 ("Storage and Retrieval — Column-Oriented Storage"); Apache Parquet specification (`github.com/apache/parquet-format`).

---

## Context

The legacy Artemis cold archive stored every downlinked frame as compressed JSONL — one JSON object per row, gzipped per file. Writing was easy. Reading was not. An analyst asking "what was the panel-2 voltage across mission 2024-Q3?" had to wait for the archive reader to decompress every file in that mission's directory, parse every JSON object, and discard ninety-nine percent of the fields it had just parsed. The query pattern that mattered most — one column out of forty — was the pattern the storage format was worst at.

The replacement is Parquet. Parquet is a columnar on-disk format: values from the same column live together physically, so a reader that only needs `panel_voltage` reads only the bytes that hold `panel_voltage` values. Topol (Ch. 3) calls this the central tradeoff of binary columnar formats — they sacrifice human-readability and append-friendliness to make columnar reads cheap. Kleppmann (Ch. 4) makes the same point at the conceptual level: column storage is the answer to the "read few columns out of many" workload that dominates analytical querying.

This lesson develops Parquet's physical layout end to end. The file's overall structure, the row group as the unit of parallelism and memory budget, the column chunk and page levels that live inside row groups, the footer that holds the metadata and why it lives at the end of the file, and the design constraints that the layout imposes on writers. Subsequent lessons in this module add per-column encoding (Lesson 2) and the in-memory Arrow representation that the writer feeds from and the reader produces (Lesson 3). The capstone wires those together into the Artemis archive writer.

---

## Core Concepts

### The File, End to End

A Parquet file is a binary container with a fixed-size envelope and a variable-size body. The envelope is the four-byte magic number `PAR1` at the very start of the file and the same four-byte magic number at the very end. Between them, in order, are row groups (the body), the footer metadata (a Thrift-serialized structure), and the four-byte little-endian length of the footer immediately preceding the trailing magic number. A reader opening an unknown Parquet file performs a deterministic sequence: seek to eight bytes before EOF, read those eight bytes, verify the magic, decode the four-byte footer length, seek backward by that length plus the eight-byte trailer, read the footer, and now the reader knows where every column chunk in the file lives.

The trailer-driven layout has one operational consequence the engineer must internalize: **a Parquet file cannot be read until it has been fully written**. The footer holds the offsets of every page in every column chunk; until the writer has emitted the footer and the trailing length, the reader has no way to find anything. This is incompatible with append-only streaming writes in the way that, say, a JSONL file is compatible — and that incompatibility is the central reason why open table formats (Iceberg, Delta) exist as a layer above Parquet rather than streams sitting alongside it. We cover that in Module 2.

### Row Groups: The Unit of Parallelism and Memory Budget

A row group is a horizontal slice of the table: some number of consecutive rows, with all of their column values laid out columnarly inside the slice. A Parquet file contains one or more row groups, written sequentially. Each row group is self-contained — its column chunks live entirely inside the row group's byte range, and the footer records the row group's offset and total byte length.

Row groups are the unit of three things at once. They are the **unit of parallelism**: a reader can hand each row group to a separate worker thread without coordination, because no row group's data depends on any other row group's data. They are the **unit of the writer's memory budget**: the writer must buffer an entire row group in memory before flushing it to disk, because the per-column statistics (min, max, null count) and encoding decisions (dictionary fits or doesn't) depend on seeing every value in the row group. They are also the **unit of column statistics**: the footer's per-column-chunk min/max values are computed over a row group, so partition pruning at query time operates at row-group granularity, not row granularity.

The row group size is therefore a four-way tradeoff: bigger row groups produce better compression (more values to find patterns across), better statistics granularity (fewer false-positive row groups during pruning), and larger units of parallelism — but they require more writer memory and produce coarser-grained predicate pushdown. Topol (Ch. 3) reports that Parquet defaults to row groups in the 64 MB to 1 GB range; Artemis archive workloads use 128 MB row groups, which balances writer memory pressure against the analyst query patterns we see.

### Column Chunks and Pages

Inside a row group, each column's values live in a **column chunk**: a contiguous byte range holding every value of that column for the row group's rows. A row group with forty columns has forty column chunks. The column chunks are written sequentially within the row group's byte range — column 0's chunk, then column 1's chunk, then column 2's chunk, and so on. The footer records each column chunk's byte offset within the file and its total byte length.

A column chunk is further subdivided into **pages**. A page is the smallest unit Parquet reads from disk: the reader cannot read fewer bytes than one page contains. There are three page types — data pages (the encoded column values themselves), dictionary pages (the dictionary for dictionary-encoded columns; we cover encoding in Lesson 2), and index pages (rare; column index and offset index in newer versions of the spec). The default page size is 1 MB, but this is configurable and pages are not required to be uniform within a column chunk — a writer typically targets a page size but emits a page whenever the encoder produces enough bytes.

The three-level structure — row groups, column chunks within row groups, pages within column chunks — is the structural fact that makes Parquet's read path efficient. A query that reads one column from one row group of a hundred-column file with twenty row groups skips ninety-nine percent of the column chunks at the row-group level (only reads one column out of a hundred per row group), and reads one row group of twenty. The reader hits about 0.5% of the file's total bytes. That is the speedup over JSONL that the Artemis migration captures.

### The Footer

The footer is a Thrift-serialized `FileMetaData` structure containing the file's schema (column names, types, and nullability), the row group descriptors (count of rows, total byte size, and a column chunk descriptor for each column), and file-level metadata (created-by string, key-value metadata, schema-level statistics). Each column chunk descriptor inside a row group descriptor records the column's encoding, compression codec, byte offset within the file, total compressed byte size, total uncompressed byte size, count of values, and the column statistics (min value, max value, null count, distinct count if cheap to compute).

The footer-at-end layout is what makes the schema, the row group offsets, and the per-chunk statistics available to the reader before any data pages are read. Critically, the column statistics are what let the query engine prune row groups without reading their data — a query for `panel_voltage > 28.5` reads the footer, finds that row group 7's `panel_voltage` chunk has `max = 27.4`, and skips the entire row group. This is **partition pruning at the row-group level**, and it is the property that the table-format layer in Module 2 will lift up to the file level. The pruning power of the footer is directly proportional to the size of the row group: small row groups have small ranges that prune more aggressively; large row groups have wider ranges that prune less aggressively. The 128 MB row group target for Artemis is calibrated to keep pruning useful for the analyst query patterns.

### Picking a Row Group Size

The row group size decision is the writer's most consequential lever. The defaults are wrong for most workloads — the Parquet 2.x default of 128 MB is reasonable for general analytics, but the right size depends on the actual workload shape. Three factors drive the choice.

**Writer memory budget.** The writer holds an entire row group in memory while encoding. A 128 MB row group with forty columns averages 3.2 MB per column chunk in memory; a 1 GB row group is 25 MB per column chunk. For the Artemis writer, which runs on the ground-segment ingestion node alongside other services, 128 MB is the upper bound that keeps the writer's resident set under 2 GB total.

**Query pattern and pruning granularity.** Smaller row groups produce tighter column statistics, which prune more aggressively. If the typical analyst query selects on `mission_id` or `orbit_pass`, and the partition layout (Module 3) does not already isolate these, then smaller row groups buy more pruning per file. If queries scan large ranges, the pruning value is lower and the row group can be larger.

**Parallelism shape.** A reader parallelizes across row groups. A file with one row group is read by one worker; a file with thirty-two is read by up to thirty-two workers. For Artemis files, which top out at 4 GB on disk, 128 MB row groups produce roughly thirty-two row groups per file — well-matched to typical query-engine worker pools.

The Artemis archive's standard is 128 MB row groups for downlinked telemetry. Production code documents the choice in the writer's config and revisits it whenever query patterns or worker pool sizes change materially.

---

## Code Examples

### Reading the Footer

Before processing any data, a Parquet reader fetches the file's footer and works out which byte ranges to read. The `parquet` crate exposes this through `SerializedFileReader`, which performs the footer read internally and gives access to the parsed metadata. The example below opens a file from the Artemis archive, parses the footer, and inspects what is in it.

```rust,ignore
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::ParquetMetaData;

fn inspect_parquet(path: &str) -> Result<()> {
    let file = File::open(path)
        .with_context(|| format!("opening {}", path))?;
    let reader = SerializedFileReader::new(file)
        .context("parsing footer; file may be truncated or corrupt")?;

    let metadata: &ParquetMetaData = reader.metadata();
    let file_meta = metadata.file_metadata();

    println!("schema:        {}", file_meta.schema_descr().root_schema().name());
    println!("num_rows:      {}", file_meta.num_rows());
    println!("num_row_groups: {}", metadata.num_row_groups());
    println!("created_by:    {:?}", file_meta.created_by());

    // Iterate the row group descriptors. Each one exposes the byte offsets
    // and statistics that let the reader prune without touching data pages.
    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        println!(
            "  rg {:>3}: rows={:>8} total_bytes={:>10}",
            rg_idx, rg.num_rows(), rg.total_byte_size()
        );
        for (col_idx, col) in rg.columns().iter().enumerate() {
            // file_offset points into the file; the reader uses this to
            // issue a single bounded read for the column chunk.
            if let Some(stats) = col.statistics() {
                println!(
                    "    col {:>2} ({}): offset={} size={} nulls={:?}",
                    col_idx,
                    col.column_path(),
                    col.file_offset(),
                    col.compressed_size(),
                    stats.null_count_opt(),
                );
            }
        }
    }
    Ok(())
}
```

The pattern to notice is that **nothing here touches a data page**. The footer parse gives the reader every byte offset, every compressed size, every per-chunk statistic — enough to plan exactly which byte ranges of the file to read for a given query. For a query like "give me `panel_voltage` from row group 7", the planner has the file offset (where to seek) and the compressed size (how many bytes to read) for that one column chunk. The one wrinkle in production code is that opening a `File` and reading the footer is a synchronous, blocking operation; the Artemis reader uses `parquet::arrow::async_reader` to do the equivalent over `object_store` for files in object storage, which is what the cold archive actually uses. The synchronous version here is for clarity.

### Selectively Reading One Column

The footer told the reader where the column chunks live. The actual read pulls only the column chunks the query needs, decodes their pages, and emits values. The `parquet::arrow` integration produces Arrow record batches — covered in detail in Lesson 3 — but the projection mechanism is worth seeing in isolation here, because it is what turns the footer's per-chunk offsets into actual I/O savings.

```rust,ignore
use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

/// Read only the `panel_voltage` column from an Artemis Parquet file.
/// Demonstrates the projection-driven read path: column chunks for unselected
/// columns are never read from disk.
fn read_panel_voltage(path: &str) -> Result<Vec<f64>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Resolve the column name to a leaf index in the schema. Production code
    // looks the column up by name rather than hard-coding indices.
    let schema = builder.schema().clone();
    let panel_voltage_idx = schema
        .index_of("panel_voltage")
        .map_err(|e| anyhow::anyhow!("missing panel_voltage column: {e}"))?;

    let mask = ProjectionMask::leaves(
        builder.parquet_schema(),
        std::iter::once(panel_voltage_idx),
    );

    let reader = builder
        .with_projection(mask)
        .with_batch_size(8192)
        .build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        // The projected batch has exactly one column; extract it as f64.
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("panel_voltage is not Float64"))?;
        out.extend(col.iter().flatten());
    }
    Ok(out)
}
```

What to notice. The `ProjectionMask::leaves` call is what makes the read efficient — the builder consults the footer, identifies the column chunks that hold `panel_voltage` across all row groups, and issues reads for only those byte ranges. The other thirty-nine column chunks per row group are not touched. `batch_size` controls how many rows are decoded per emitted Arrow batch; 8192 is a common default that fits comfortably in L2 cache. Under load, two failure modes are worth knowing. First, if `panel_voltage` is dictionary-encoded (Lesson 2) and the dictionary page is large, the reader pays the dictionary-decode cost once per row group — typically negligible, but worth checking on hot columns with very large dictionaries. Second, if the file was written with no statistics on `panel_voltage`, predicate pushdown for downstream filters cannot prune row groups, and every row group's column chunk is read. The Artemis writer always enables statistics on numeric columns for this reason.

### Writing With a Target Row Group Size

The writer side is where the row-group-size decision becomes a concrete number in code. The `ArrowWriter` accepts `WriterProperties` that bound the row group size; once the writer has buffered enough rows to hit the target size, it flushes the row group and starts a new one.

```rust,ignore
use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Build a Parquet writer configured for the Artemis archive: 128 MB
/// row groups, ZSTD compression, statistics enabled on numeric columns.
fn artemis_writer(
    path: &str,
    schema: Arc<Schema>,
) -> Result<ArrowWriter<File>> {
    let file = File::create(path)?;

    let props = WriterProperties::builder()
        // Target row group size in bytes. The writer flushes the row group
        // when buffered bytes reach this threshold; the actual size is
        // approximate because flushes happen at batch boundaries.
        .set_max_row_group_size(128 * 1024 * 1024)
        // ZSTD at level 3 is the Artemis default — substantially better
        // ratios than SNAPPY on telemetry data with no meaningful CPU cost
        // at the read side.
        .set_compression(Compression::ZSTD(Default::default()))
        // Statistics are what enable row-group pruning downstream. Disabling
        // them on a column saves a small amount of write time and a large
        // amount of read-time pruning power; rarely the right tradeoff.
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
    Ok(writer)
}

/// Drain an iterator of record batches to a Parquet file, respecting the
/// configured row group size. The writer flushes row groups on its own as
/// the buffered byte count crosses the threshold; the caller does not need
/// to manage row group boundaries explicitly.
fn write_batches<I>(
    path: &str,
    schema: Arc<Schema>,
    batches: I,
) -> Result<u64>
where
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    let mut writer = artemis_writer(path, schema)?;
    let mut total_rows: u64 = 0;
    for batch in batches {
        let batch = batch?;
        total_rows += batch.num_rows() as u64;
        writer.write(&batch)?;
    }
    // `close` is what emits the footer and the trailing magic number. Until
    // this returns, the file is unreadable — see the `Footer` concept above.
    writer.close()?;
    Ok(total_rows)
}
```

The `close()` call is the critical line — it is what writes the footer and the trailing magic number that make the file readable. If the process crashes before `close()` returns, the partial file is unreadable; any recovery has to discard it and re-emit. The Artemis ingestion pipeline handles this by writing to a `.parquet.inprogress` filename and renaming to `.parquet` only after `close()` succeeds, which gives the readers a simple "if it exists with the final name, it is complete" invariant. The pattern generalizes: writers to object storage use the equivalent (S3 multipart complete, GCS compose, Azure commit-block-list) to make the visibility atomic with the file's validity.

---

## Key Takeaways

- A Parquet file has a fixed structure: `PAR1` magic, row groups, Thrift-serialized footer, four-byte footer length, `PAR1` magic. The reader works backward from EOF: read the trailer, decode the footer length, seek backward, read the footer, then plan its data reads.
- **Row groups are the unit of parallelism, of writer memory, and of column statistics.** Picking a row group size is a four-way tradeoff between writer memory budget, parallelism shape, statistics granularity, and compression ratio. Artemis defaults to 128 MB.
- Inside a row group, each column lives in a contiguous **column chunk**, and inside the column chunk values live in **pages**. The three-level hierarchy is what lets a reader fetch one column out of forty by reading bytes corresponding to one chunk per row group.
- The footer-at-end layout means **a Parquet file is unreadable until its writer has closed it**. This is the structural reason streaming writes need a layer above Parquet (Iceberg, Delta — Module 2), not raw Parquet.
- Predicate pushdown operates at row group granularity using the per-chunk statistics in the footer. Always enable statistics on numeric columns; disabling them is rarely the right tradeoff.
- The Parquet write side's `close()` call is what makes the file valid. Production writers stage to a temporary filename and rename on success to give readers a clean "exists ⇒ complete" invariant.

{{#quiz quiz-01.toml}}
