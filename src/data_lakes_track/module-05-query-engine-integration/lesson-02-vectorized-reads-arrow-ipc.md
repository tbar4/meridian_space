# Lesson 2 — Vectorized Reads via Arrow IPC

**Module:** Data Lakes — M05: Query Engine Integration
**Position:** Lesson 2 of 3
**Source:** *In-Memory Analytics with Apache Arrow* — Matthew Topol, Chapter 6 ("Arrow IPC and the Flight Protocol"). Apache Arrow specification, "IPC Format" section. Apache DataFusion `RecordBatchStream` documentation.

> **Source note:** The Arrow IPC format is well-specified and the Topol book is the authoritative reference; this lesson grounds the framing there and applies it to the lakehouse-to-engine connection.

---

## Context

Module 1 produced Arrow record batches as the in-memory shape for columnar data; Module 2-4 produced a table format that returns Arrow batches as the result of read plans. Lesson 1 introduced the engine-storage boundary through the predicate-pushdown contract; this lesson is the *data* side of the same boundary. How do batches get from the storage layer to the engine? In the simplest case (storage and engine in the same process), they get passed by `Arc`. In the cross-process case (storage as a separate service, engine as a client), they cross the boundary via **Arrow IPC**.

The Arrow IPC format exists for exactly this: a binary wire format for record batches that preserves Arrow's in-memory layout. The decoder doesn't reconstruct rows or parse field-by-field — it maps the wire bytes directly into the buffer layout that the in-memory `RecordBatch` expects. The transport cost is `memcpy`; the decode cost is constant per batch. This is what makes the lakehouse architecture economical at any scale: the same Arrow buffers that came out of the Parquet decoder go straight onto the wire and into the engine's execution path, with no per-row materialization or per-column serialization step in between.

This lesson develops the IPC format's structure (schema message then batch messages), the streaming protocol (an open-ended sequence of batch messages with an EOS marker), and the protocol's properties that make it the right transport for the lakehouse. The capstone uses the IPC machinery in-process — DataFusion holds the table provider's stream directly without any wire format — but the cross-process pattern is structurally identical and worth understanding.

---

## Core Concepts

### Why Arrow IPC, Not Some Other Format

A query engine consuming the table format's output has a choice of wire format. The lakehouse community has converged on Arrow IPC for reasons that come straight out of *In-Memory Analytics with Apache Arrow* (Topol, Ch. 6). The framing the book develops, and the relevant comparisons:

- **Row-oriented formats** (JSON, MessagePack, Protobuf-with-row-encoding) require row reconstruction on the receiver's side. Each row's values are decoded into per-row structures, then the receiver typically rebuilds a columnar layout for processing. This is wasted work; the lakehouse's output is already columnar, and the engine wants columnar input.
- **Bespoke columnar formats** (custom column-by-column protocols) require per-column type-specific decoders. Adding a new type means adding a new decoder. Versioning is fragile.
- **Parquet over the wire** uses the file format as the transport. The bytes have to be re-encoded for the engine's in-memory format (Parquet's encodings — RLE, dictionary, BYTE_STREAM_SPLIT — don't match Arrow's flat layout). Decode is per-column-chunk, not constant.
- **Arrow IPC** is the in-memory Arrow layout serialized as-is. The receiver reads the schema message, allocates the right number of buffers, and `memcpy`s the wire bytes into them. The result is an Arrow `RecordBatch` ready to use; no per-row, per-column, or per-type work.

The cost basis matters at scale. A query reading 100 GB of data through a row-oriented format spends roughly 100 GB of CPU on decoding; through Arrow IPC it spends roughly the network/disk I/O cost plus a few hundred microseconds per batch in IPC overhead. The 10×-100× advantage compounds; lakehouse query engines have all converged on Arrow IPC or a close relative for the same reason.

### The IPC Streaming Protocol

The IPC streaming protocol is a sequence of messages, each one a small header followed by an optional binary payload. The message types relevant to the read path are:

1. **Schema message.** Sent once at the start of the stream. Describes the schema of every batch that will follow. Includes column names, types, nullability, and any nested-type metadata. After this message, every batch in the stream conforms to this schema.
2. **RecordBatch message.** Sent repeatedly. Each message describes one record batch: the row count, the buffer offsets and lengths within the payload, and any dictionary references. The payload is the buffers themselves — null bitmaps, offset arrays, value arrays, all concatenated.
3. **DictionaryBatch message.** Sent when dictionary-encoded columns appear. Each dictionary is sent once (the first time a column uses it); subsequent batches reference the dictionary by ID. This is the wire-format counterpart to Module 1's dictionary encoding.
4. **EndOfStream marker.** Sent at the end. Tells the receiver no more batches are coming.

The protocol has one important property: **the schema is sent once, batches are sent many times.** The receiver allocates the type-specific decoder state once, then processes batches in a tight loop. The per-batch overhead is the message header (a few hundred bytes) plus the payload `memcpy`. For batches of typical size (8K-64K rows), the per-batch overhead is dominated by the payload, not the framing.

The streaming property is what makes the protocol work over network connections. The sender can produce batches as it reads data files; the receiver can process batches as they arrive; neither side needs to buffer the entire result set. This is the same property the `RecordBatchStream` type provides in-process — the protocol generalizes it across process boundaries.

### Schema Negotiation and Forward Compatibility

The schema message at the start of the stream is the receiver's contract about what types to expect. The schema is fully self-describing: column names, Arrow types (with parameters: timestamp units, decimal precision, list inner types, struct fields), and metadata (the `key_value_metadata` field for arbitrary annotations).

The Iceberg field-ID annotation lives in the schema metadata. When a table format produces an IPC stream of a time-traveled snapshot, the schema message includes the field IDs in the per-column metadata; the engine can use them for projection if it needs to map the stream's schema against a different target schema. This is the wire-format counterpart to Module 4 Lesson 3's column-ID-based projection — same mechanism, applied to the IPC layer.

The schema is sent before any batches, which gives the receiver a chance to reject incompatible streams. If the receiver requires a specific schema (the consumer is a specific query plan that knows what columns it needs), it can compare the incoming schema against its expectation and abort if they disagree. The discipline is the same as Module 2 Lesson 1's schema-enforcement check, applied at the wire layer.

### Bounded Memory Through Streaming

The streaming protocol bounds memory because batches arrive one at a time. The receiver's memory at any moment is one batch in flight (typically a few MB to a few tens of MB, depending on batch size and column count) plus whatever the downstream consumer holds onto. A consumer that processes each batch and immediately drops it (the typical filter-and-aggregate pattern) sees its memory stay constant regardless of how many batches the stream produces; a consumer that buffers batches (collecting all results into a vector) sees its memory grow linearly with the result size.

The Artemis Mission Replay Engine (Module 4's capstone) exploits this directly. A full-mission replay returns terabytes of result data; the engine's memory stays under 2 GB because each batch is processed and dropped immediately. The downstream consumer (an analyst's tooling) batches results into its own bounded queue with an explicit flow-control mechanism — if the analyst's tool is slow, the engine's stream backpressures and pauses producing new batches.

The DataFusion `SendableRecordBatchStream` type implements this exactly: a `Stream<Item = Result<RecordBatch>>` with the `Send` bound. The stream produces one batch at a time; the consumer pulls; the producer reads the next file's batches when the consumer is ready. The flow-control is implicit in the stream's poll-based semantics — the producer doesn't run until the consumer wants more data.

### The `RecordBatchStream` Trait

DataFusion's primary abstraction for batch streams is `RecordBatchStream`:

```text
trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}
```

The trait extends `futures::Stream` with a schema accessor. The schema is the same one every batch the stream produces conforms to; the consumer reads it once and uses it for all downstream operations. This matches the IPC schema-once-then-batches pattern; the in-process and cross-process cases share the same abstraction.

A `TableProvider::scan` returns an `Arc<dyn ExecutionPlan>`; the `ExecutionPlan::execute` returns a `SendableRecordBatchStream` (the `Send`-bound variant for parallel execution). The data flow is:

```text
ArtemisTable → ScanPlan → FileReader → RecordBatchStream → DataFusion FilterExec → ...
```

The stream is built up lazily. The `scan` call constructs the `ScanPlan` (using Module 3's three-pass pruning) but doesn't read any data. The `execute` call returns a stream that, when polled, reads the next file's batches. The consumer's pulling drives the storage's reading; the storage's reading drives the network/object-store I/O.

### IPC at the Storage-Engine Boundary

The lesson has so far considered the streaming-protocol shape and the in-process consumption pattern. The cross-process case — storage as a service, engine as a client — uses the same protocol over a network connection. The standard transports:

- **Arrow Flight** is the gRPC-based protocol layered on Arrow IPC. The client issues a query (a `FlightDescriptor`); the server returns an IPC stream over gRPC. Used by datawarehouse-as-a-service offerings; the Artemis archive uses Flight for inter-region replay queries where the analyst tooling lives in a different region from the catalog.
- **Direct IPC over TCP** is the lightest-weight transport. No gRPC framing, no service abstraction, just the IPC stream sent over a socket. Used in tightly-coupled deployments where the engine and the storage share infrastructure.
- **HTTP with IPC streaming** is the cloud-native variant. The client GETs a URL; the server streams the IPC bytes as the response body. The HTTP/2 streaming primitives carry the flow control; Arrow IPC handles the data shape. Used by some data-warehouse REST APIs; the Artemis read service's external API uses this.

The choice of transport is operational, not architectural. The lakehouse's storage layer produces Arrow IPC; the transport wraps it. Changing transports doesn't change the storage code, the engine code, or the data flow shape. This is the same architectural property as the catalog backend choice (Lesson 3): the table format defines the contract; the implementations swap underneath.

---

## Core Mechanics in Code

### Writing an IPC Stream from a `RecordBatchStream`

The producer side: take a stream of record batches, write an IPC stream to a writer (a TCP socket, an HTTP response body, a file).

```rust,ignore
use std::sync::Arc;
use anyhow::Result;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;

/// Stream-encode a sequence of record batches as an Arrow IPC stream.
/// Writes one Schema message, then a RecordBatch message per batch,
/// then an EndOfStream marker. The writer is a generic `std::io::Write`
/// or a Tokio-style AsyncWrite wrapped via an adapter.
pub async fn write_ipc_stream<S>(
    mut stream: impl futures::Stream<Item = Result<RecordBatch>> + Unpin,
    schema: SchemaRef,
    writer: S,
) -> Result<()>
where
    S: std::io::Write,
{
    // The IPC StreamWriter handles all the framing: the schema header,
    // the per-batch headers, the dictionary tracking, and the
    // end-of-stream marker on drop.
    let mut ipc_writer = StreamWriter::try_new(writer, &schema)?;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        // The write_batch call serializes the batch's buffers to the
        // wire format. The serialization is essentially a memcpy of the
        // Arrow buffers plus a small header — no per-row or per-column
        // work beyond the framing.
        ipc_writer.write(&batch)?;
    }

    // The finish call emits the EndOfStream marker and flushes the writer.
    ipc_writer.finish()?;
    Ok(())
}
```

The pattern. The `StreamWriter` from the `arrow-ipc` crate handles all the framing details — the schema flatbuffer, the per-batch message envelopes, the dictionary tracking, the end-of-stream marker. The user code is just a loop over the stream, calling `write` for each batch. The cost per batch is the framing (a few hundred bytes) plus the `memcpy` of the batch's buffers.

### Reading an IPC Stream into a `RecordBatchStream`

The consumer side: read the IPC bytes from a reader, produce a stream of record batches.

```rust,ignore
use anyhow::Result;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::io::Read;

/// Decode an Arrow IPC stream from a reader. Reads the schema header,
/// then yields one RecordBatch per batch message. The schema is
/// available via the StreamReader's accessor after construction.
pub fn read_ipc_stream<R: Read>(
    reader: R,
) -> Result<(SchemaRef, impl Iterator<Item = Result<RecordBatch>>)> {
    let stream_reader = StreamReader::try_new(reader, None)?;
    let schema = stream_reader.schema();
    // The StreamReader implements Iterator<Item = Result<RecordBatch>>,
    // yielding one batch per .next() call until EndOfStream.
    let iter = stream_reader.map(|r| r.map_err(Into::into));
    Ok((schema, iter))
}
```

What to notice. The reader returns the schema separately from the batch iterator. The schema is the contract for every batch the iterator yields; the consumer can validate it against its expectation before processing any batches. The iterator is a normal `Iterator<Item = Result<RecordBatch>>`; each `.next()` call reads one message from the underlying reader, decodes it, and returns the batch.

The synchronous `Iterator` shape works for the in-process and file-backed cases. The async cross-network case uses the `tokio`-flavored `arrow-flight` crate, which exposes an `async_stream` of batches; the shape is the same with async polling instead of synchronous iteration.

### A `RecordBatchStream` from the Module 3 Read Plan

Wiring the Module 3 scan plan into the DataFusion `RecordBatchStream` shape. The function below is the heart of the capstone's `ArtemisExec`:

```rust,ignore
use std::sync::Arc;
use async_stream::stream;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use futures::Stream;

/// Produce a stream of record batches from a scan plan. The stream
/// reads files in plan order, decoding each file's row groups into
/// batches and yielding them lazily. Memory stays bounded by the
/// current batch size; the consumer's pulling drives the I/O.
pub fn execute_plan(
    plan: ScanPlan,
    schema: SchemaRef,
    object_store: Arc<dyn ObjectStore>,
) -> impl Stream<Item = Result<RecordBatch>> {
    stream! {
        for file_scan in plan.files {
            // Open the file's Parquet reader with row-group projection
            // and column projection from the scan plan.
            let parquet_reader = open_parquet_with_projections(
                &object_store,
                &file_scan.path,
                &file_scan.row_groups,
                &schema,
            ).await?;

            // Yield each batch as it's decoded. The Parquet reader's
            // own internal buffering means batches arrive in row-group
            // order; the consumer can process and drop them
            // immediately.
            let mut batch_iter = parquet_reader.read_batches();
            while let Some(batch_result) = batch_iter.next().await {
                yield batch_result;
            }
        }
        // The stream ends when all files have been read; the consumer's
        // .next() returns None.
    }
}
```

The structure. The stream reads one file at a time, one row group at a time, one batch at a time. The Parquet reader's row-group projection (from Module 3 Pass 3) controls which row groups are read; the column projection (from Module 1 Lesson 3's ProjectionMask) controls which columns are read; the scan plan's file list controls which files are read. Every layer's pruning composes; the I/O is minimal.

Production code parallelizes by opening multiple files concurrently. The `tokio::stream::FuturesUnordered` pattern works directly: spawn one task per file, push tasks onto an unordered stream with a concurrency cap, yield batches as they arrive. The order of batches is no longer file-order, which is fine for query engines that don't depend on input order (DataFusion's scan operators don't); for operators that do, the engine adds an explicit sort.

---

## Key Takeaways

- **Arrow IPC is the right wire format for engine-storage data transport** because it preserves Arrow's in-memory layout. Decode is constant-time `memcpy`; no row reconstruction; no per-column type-specific work.
- **The streaming protocol is schema-then-batches-then-EOS.** The schema is sent once at the start; batches stream open-endedly; an EOS marker ends the stream. The receiver allocates decoder state once, processes batches in a tight loop.
- **Streaming bounds memory.** A consumer that processes-and-drops each batch sees constant memory regardless of result size. The flow-control is implicit in the poll-based stream semantics — the producer pauses when the consumer isn't pulling.
- **Schema metadata carries field IDs and other annotations** for column-ID-based projection at the wire layer. The Iceberg field_id is preserved through IPC; the engine can use it to map streams against different target schemas.
- **The choice of transport (Flight, raw TCP, HTTP-with-IPC) is operational, not architectural.** The storage produces IPC; the transport wraps it. Changing transports doesn't change the storage or the engine.

{{#quiz quiz-02.toml}}
