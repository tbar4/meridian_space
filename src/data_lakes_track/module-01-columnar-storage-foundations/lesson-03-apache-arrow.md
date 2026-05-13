# Lesson 3 — Apache Arrow and the In-Memory Side

**Module:** Data Lakes — M01: Columnar Storage Foundations
**Position:** Lesson 3 of 3
**Source:** *In-Memory Analytics with Apache Arrow* — Matthew Topol, Chapter 1 ("Getting Started with Apache Arrow"), Chapter 2 ("Working with Key Arrow Specifications"), and Chapter 3 ("Memory Mapping"); Apache Arrow specification (`arrow.apache.org/docs/format/Columnar.html`).

---

## Context

Parquet is the on-disk format. Arrow is the in-memory format. They are different formats, designed for different goals, and the engineer who treats them as the same thing produces code that is either slow on reads or wrong on writes. Parquet optimizes for **small on disk and read-once into memory**: encoded, compressed, footer-at-end, page-decoded as you go. Arrow optimizes for **random access in memory and zero-copy across process boundaries**: uncompressed, fully materialized, structured as buffers the CPU can scan directly. The Artemis archive uses both. Cold-archive files are Parquet; the working sets analysts hold in memory are Arrow; the hot caches between cold storage and the compute layer are Arrow IPC files, memory-mapped from a local SSD tier.

Understanding the boundary between them is what makes the read path work. When a query engine reads `panel_voltage` from a Parquet file, the work is: locate the column chunk (Lesson 1), decompress its pages (page-by-page, streaming), decode the values out of their encoding (dictionary indices → values, deltas → values, byte-split → values; Lesson 2), and *materialize them as an Arrow array in memory*. That last step is the Parquet-to-Arrow boundary. The cost of that boundary is the cost the query engine pays per read; the Arrow format is what the cost is *paid into*, and what makes every subsequent operation on the data cheap.

This lesson develops the Arrow side end-to-end. Arrow's columnar in-memory layout and how it differs from Parquet's compressed-on-disk layout. The record batch as the unit of in-memory work, and the chunked array that represents a column across multiple record batches. The Parquet-Arrow boundary and what crossing it costs. And the IPC format that lets two processes share Arrow data without copying — the basis of the hot-cache tier between the Artemis cold archive and the analyst-facing compute layer.

---

## Core Concepts

### Arrow vs Parquet: Two Different Goals

The two formats answer different questions. Topol (Ch. 3) frames the distinction directly: Parquet is a "long-term storage format" optimized for size on disk and column-projected reads; Arrow is an "in-memory representation" optimized for random-access analytical processing. Three concrete differences capture the design split.

**Compression.** Parquet column chunks are encoded and compressed; the bytes on disk are not the bytes the CPU operates on. Arrow buffers are uncompressed and laid out for direct CPU access; the bytes in memory are the bytes the CPU's vector instructions consume. Decompressing on read is the cost; getting a memory layout the CPU loves is the benefit.

**Random access.** Parquet's encodings — delta, RLE, dictionary — break O(1) random access to individual values. Finding the value at row 12,345 of a delta-encoded column requires decoding from a frame-of-reference boundary, not seeking by index. Arrow's columnar layout preserves O(1) random access by row index for every primitive type: the i-th value of a `Float64Array` lives at byte offset `i * 8` in the value buffer, full stop. The Arrow layout is the reason a compute kernel can vectorize over a column without conditionals or decoders in the inner loop.

**Mutability and shareability.** Arrow buffers are designed to be shared across processes and language runtimes without copying. The buffer layout is language-agnostic and stable across versions of the spec; a Python process, a Rust process, and a C++ process can all hold pointers to the same memory and treat it as the same array. Parquet does not have this property — Parquet is a file format, not a memory format, and inter-process sharing of Parquet bytes still requires each process to do its own decode.

The operational consequence: the Artemis read path is Parquet-on-cold-storage → Arrow-in-memory at the boundary, and Arrow-in-shared-memory → Arrow-in-memory for the hot path. Each leg of the pipeline uses the format suited to its constraints.

### The Arrow Memory Layout

An Arrow array is a small number of contiguous **buffers** plus a small amount of metadata. The buffers for a primitive type are two: a **validity bitmap** (one bit per row, 1 = valid, 0 = null) and a **value buffer** (the raw values laid out end to end). For a `Float64Array` of length 8192, the validity bitmap is 1024 bytes (8192 / 8) and the value buffer is 65,536 bytes (8192 × 8). The reader of this array does pointer arithmetic on the value buffer; nullability is a separate, dense, branch-friendly check against the bitmap.

Variable-length types — strings, binary, lists — add a third buffer: an **offset buffer** of `i32` or `i64` values that record where each row's value starts in the value buffer. A `StringArray` of length 8192 has a 1024-byte validity bitmap, an offset buffer of 8193 × 4 bytes (one entry per row plus a terminator), and a value buffer holding the concatenated UTF-8 bytes of every string. Reading row 47's string is `value_buffer[offsets[47]..offsets[48]]` — three pointer reads and a slice, no parsing.

Nested types — structs, lists of structs, maps — compose the same primitives. A struct array is metadata pointing at child arrays of the field types; a list array is an offset buffer pointing at a child values array; a dictionary array is an index array pointing at a dictionary values array. Topol (Ch. 1) walks the layout for each type. The composition rule is the same for all of them: every Arrow array, however complex its type, decomposes into a small number of flat, contiguous buffers that the CPU can scan directly.

The buffer-based layout is what makes Arrow's claim to "zero-copy" credible. Sharing an Arrow array across a process boundary is sharing the pointers and lengths of its buffers — no serialization, no deserialization, no encoding decode. Crossing a process boundary is a few hundred bytes of metadata regardless of the array's data size.

### Record Batches and Chunked Arrays

A **record batch** is a group of Arrow arrays, each the same length, with named fields — Arrow's equivalent of a chunk of rows from a table. A record batch of 8192 rows with forty columns is forty Arrow arrays of length 8192 plus a small schema descriptor. The record batch is Arrow's unit of in-memory work: compute kernels operate on a record batch's columns directly, and downstream operators consume and emit record batches.

The size of a record batch is the analog of Parquet's row group size, but the constraint is different. Parquet row groups are sized for compression ratio and parallelism (Lesson 1, 128 MB target). Arrow record batches are sized for cache friendliness — small enough that a single column's values fit comfortably in L2 cache while the kernel runs. 8192 rows is the conventional default and the right answer for most kernels; the Artemis compute layer uses 8192 across the board.

A **chunked array** is what you get when a column spans multiple record batches — a logical "the values of `panel_voltage` across this entire query result." Topol (Ch. 3) explains the relevant connection: "Since Parquet files can be split into multiple row groups, we can avoid copying data by using a chunked array to treat the collection of one or more discrete arrays as a single contiguous array." A reader that pulls one Parquet row group at a time and produces one record batch per row group naturally produces a chunked array per column. Compute kernels handle this directly; a kernel that operates on a `ChunkedArray` iterates the chunks in turn and produces a chunked result. No concatenation, no copy.

### The Parquet ↔ Arrow Boundary

The Parquet-to-Arrow boundary is where the encoded, compressed bytes on disk become the uncompressed, random-access buffers in memory. The cost of crossing it is the per-page work: decompress (ZSTD/SNAPPY) and decode (dictionary → values, delta → values, byte-stream-split → values). For a typical Artemis row group — 128 MB on disk, 600 MB uncompressed — the boundary cost is roughly 100 ms of CPU on a modern core. The product to think about: that 100 ms produces an in-memory representation that subsequent kernels operate on at memory-bandwidth speeds, billions of values per second.

The other direction — Arrow to Parquet — is what the writer does. Arrow record batches go in, encoded and compressed bytes come out, plus a footer at the end. The boundary cost on the write side is the encoding-then-compression cost from Lesson 2.

The boundary has a few practical implications the engineer must know.

**Schema mapping is not free.** Arrow's type system and Parquet's type system overlap but are not identical. Parquet has logical types that map to Arrow types via a conversion table, and the conversion is sometimes lossy. Decimal precision, timestamp time zones, and nested null handling are the typical edge cases. Production code pins the schema explicitly on both sides rather than relying on inference.

**Reading is streaming.** The `parquet::arrow::ArrowReader` does not decode an entire file to one giant Arrow table. It produces a stream of record batches, each corresponding to one Parquet row group or a configurable batch-size slice within it. Memory stays bounded; the consumer processes batches as they arrive and discards them.

**Writing buffers a row group.** The `ArrowWriter` accumulates incoming record batches until it has enough rows to flush a row group, then encodes-and-compresses the column chunks for the row group and writes them. During that accumulation, the writer holds the row group's worth of data uncompressed in memory — Lesson 1's writer-memory budget constraint comes from this.

### IPC and Zero-Copy via Memory Mapping

Arrow IPC ("Inter-Process Communication") is the wire format and file format Arrow uses to share record batches between processes. The format is a sequence of FlatBuffers-framed messages: a schema message first, then one or more record-batch messages, optionally with dictionary-batch messages for shared dictionaries. The wire format is the streaming version; the file format is the same wire format with a magic-number envelope and a footer that records the byte offsets of every record batch in the file, enabling random access to any batch without reading the others.

The point of IPC is what Topol (Ch. 3) calls "no deserialization cost": the bytes on disk in an Arrow IPC file are the *same bytes* the CPU operates on in memory. There is no parse step, no decompression step, no decode step. A reader that memory-maps an Arrow IPC file gets pointers directly into the kernel's page cache; the values are addressable without any work beyond the page faults that bring them into RAM.

The Artemis hot cache is built on this property. The 50 GB working sets analysts query interactively are stored as Arrow IPC files on the local SSD tier (NVMe-backed). When an analyst's query touches a working set, the cache server memory-maps the relevant IPC files and hands the record batch pointers to the query engine. No copy, no decode, no allocation beyond a few hundred bytes of metadata. The query engine operates on the mmap'd pages; the kernel pages in only the bytes the kernel touches. Topol's memory-mapping discussion in Ch. 3 makes this concrete: a 1.6 GB file accessed via memory mapping reads "only the pages from the file that it needs when the corresponding virtual memory locations are accessed," with no allocation up front.

The tradeoff: Arrow IPC is much larger on disk than Parquet because it is not encoded or compressed. The same data that is 800 MB as a well-encoded Parquet file is 6 GB as an Arrow IPC file. The hot-cache tier is sized accordingly — it holds a working set, not the cold archive. The cold archive is Parquet; the IPC files are derived caches built from materialized Parquet reads.

---

## Code Examples

### Building an Arrow Record Batch From Rust Data

Most production code receives record batches from the Parquet reader rather than constructing them by hand, but the buffer-level construction is worth seeing once. It is what the writer pipeline produces as its last step before handing the batch to the Parquet writer.

```rust,ignore
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, RecordBatch, TimestampNanosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Build a record batch holding one row group's worth of Artemis telemetry.
/// In production this is built incrementally by the ingestion loop, one
/// observation at a time, using `ArrayBuilder` types; the from-vec
/// construction here is for illustration.
fn build_telemetry_batch(
    timestamps_ns: Vec<i64>,
    panel_voltages: Vec<f64>,
    payload_ids: Vec<u32>,
) -> Result<RecordBatch> {
    assert_eq!(timestamps_ns.len(), panel_voltages.len());
    assert_eq!(timestamps_ns.len(), payload_ids.len());

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "sample_timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false, // non-nullable; timestamps are always present
        ),
        Field::new("panel_voltage", DataType::Float64, false),
        Field::new("payload_id", DataType::UInt32, false),
    ]));

    let ts = TimestampNanosecondArray::from(timestamps_ns);
    let voltage = Float64Array::from(panel_voltages);
    let payload = UInt32Array::from(payload_ids);

    RecordBatch::try_new(
        schema,
        vec![Arc::new(ts), Arc::new(voltage), Arc::new(payload)],
    )
    .map_err(Into::into)
}
```

What to notice. The `RecordBatch::try_new` call is cheap: it does not copy the underlying buffers, only validates the lengths match the schema and wraps the arrays in a struct. The `Arc::new` calls share ownership of the arrays without copying. Cloning the resulting batch downstream is similarly cheap — `RecordBatch` is `Clone`, and cloning increments the `Arc` refcounts on each column. This is the in-memory side of Arrow's zero-copy promise: passing record batches around the pipeline is a pointer-copy operation, not a data-copy operation.

The construction-from-Vec path used above is the slow path. Production ingestion uses `ArrayBuilder` types (`Float64Builder`, `TimestampNanosecondBuilder`) that accept values one at a time, manage their own buffer growth, and finish into an array at the end of a row group's worth of rows. The Artemis writer's ingestion loop is built around builders, one per column, finished and reset at each row group boundary.

### Reading a Parquet File Into Arrow Record Batches

The `parquet::arrow` integration produces Arrow record batches from a Parquet file. The reader is an iterator: each `.next()` produces one record batch, and the iterator terminates when the file is fully read. Memory stays bounded regardless of file size.

```rust,ignore
use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Read an entire Parquet file as a stream of Arrow record batches. The
/// configurable batch size controls how many rows are emitted per batch;
/// 8192 is the cache-friendly default the Artemis compute layer uses.
fn read_to_record_batches(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    // The reader is an Iterator<Item = Result<RecordBatch>>. Each batch
    // corresponds to a 8192-row slice; the boundary aligns with Parquet
    // row groups when row groups are larger than the batch size.
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Same read, but with column projection. The Parquet reader only
/// decodes the column chunks for the projected columns; the others are
/// not read from disk. This is the I/O savings the columnar layout
/// promised in Lesson 1, expressed through the Arrow boundary.
fn read_projected(path: &str, columns: &[&str]) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::ProjectionMask;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = builder.schema();
    let leaves: Vec<usize> = columns
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("missing column: {e}"))?;

    let mask = ProjectionMask::leaves(builder.parquet_schema(), leaves);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(8192)
        .build()?;

    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}
```

The boundary's cost is hidden inside the iteration. Each `batch?` line pulls one row group's worth of column chunks from disk, decompresses them, decodes them out of their encodings, and materializes them as Arrow arrays. The Arrow arrays the reader produces are the same `Float64Array`, `TimestampNanosecondArray`, etc. types the builder code produces — once the boundary is crossed, the data is indistinguishable from data that was built in memory directly.

Two production touches that the example omits. First, the synchronous `File` reader is correct for local files but wrong for the Artemis cold archive, which lives in object storage; production reads use `parquet::arrow::async_reader` with `object_store::ObjectStore` to issue range reads against S3-compatible storage. Second, the `Vec<RecordBatch>` materialization at the end of the example defeats the streaming property of the reader — production code consumes the iterator lazily and never materializes the whole result.

### Memory-Mapping an Arrow IPC File for Zero-Copy Reads

The Artemis hot-cache tier stores frequently-queried working sets as Arrow IPC files on a local NVMe SSD. The cache server memory-maps these files; the query engine operates on the mmap'd pages directly. No allocation, no decode, no copy.

```rust,ignore
use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use memmap2::Mmap;

/// Open an Arrow IPC file in read-only memory-mapped mode and read its
/// record batches. The bytes in the returned record batches' buffers
/// point directly into the mmap'd region; no copy of the data is made
/// at any point in this function.
fn read_ipc_mmap(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;

    // Safety: the file is opened read-only, and the mmap lives at least
    // as long as the record batches returned. If the underlying file is
    // truncated or replaced while this mmap is live, reads will SIGBUS;
    // the Artemis cache server protects against this by never modifying
    // a cache file in place — new versions are written to new paths and
    // the index updated atomically.
    let mmap = unsafe { Mmap::map(&file)? };

    // The arrow-ipc FileReader accepts any Read + Seek. A Cursor over
    // the mmap slice gives it that; reads from the cursor are reads
    // from the mmap'd memory, which the kernel pages in on demand.
    let cursor = std::io::Cursor::new(&mmap[..]);
    let reader = FileReader::try_new(cursor, None)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    // The mmap must outlive the returned batches' buffer references.
    // The simplest discipline is to keep the mmap alive in an enclosing
    // struct alongside the batches — the example here returns owned
    // batches whose data has been copied out of the mmap by the IPC
    // reader's deserialization. True zero-copy requires the batches'
    // buffers to alias the mmap directly, which the arrow-ipc reader
    // can do with the right buffer-allocator configuration; see the
    // production cache server's implementation for the wiring.
    Ok(batches)
}
```

The `unsafe` block is the irreducible cost of memory mapping: the `Mmap` type cannot enforce, at compile time, that no one truncates or replaces the underlying file. The Artemis cache server addresses this with a write-discipline invariant — cache files are immutable, new versions are written to new paths, and the cache index is updated atomically — so the unsafety is contained to a known boundary.

The example's last comment is the production caveat worth understanding. True zero-copy from an Arrow IPC file requires the IPC reader to construct Arrow array buffers that *alias* the mmap'd memory rather than copying out of it. The default `FileReader` reads the FlatBuffers metadata zero-copy but materializes the data buffers into freshly-allocated `Buffer` objects. The Artemis cache server uses a custom buffer allocator that returns slices of the mmap'd region instead — this is what makes the cache hit path allocation-free. The mechanics are out of scope here; the principle is that Arrow IPC's wire format is byte-identical to its in-memory format, and aliasing is technically possible whenever the file's alignment matches Arrow's alignment requirements (8-byte by default).

---

## Key Takeaways

- **Arrow and Parquet are different formats with different goals.** Parquet is small-on-disk and read-once into memory; Arrow is random-access in memory and zero-copy across process boundaries. The Artemis read path crosses the boundary between them deliberately.
- An Arrow array is a small number of flat, contiguous **buffers** — validity bitmap plus value buffer for primitives, offset buffer added for variable-length types. The layout is what makes O(1) random access and vectorized kernels possible.
- A **record batch** is the in-memory unit of work — multiple equal-length arrays with a schema. **Chunked arrays** represent a column across multiple batches without concatenation or copy.
- The **Parquet-Arrow boundary** is where on-disk encoded bytes become in-memory random-access buffers. The crossing is per-page decompression and decoding; the result is data the CPU can scan at memory-bandwidth speeds.
- **Arrow IPC files are memory-mappable for zero-copy reads.** The bytes on disk are the bytes the CPU operates on. The hot-cache tier between the Artemis cold archive and the compute layer uses this property to serve queries against working sets at zero allocation cost per query.

{{#quiz quiz-03.toml}}
