# Lesson 1 — WAL Fundamentals

**Module:** Database Internals — M04: Write-Ahead Logging & Recovery  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 9–10; [Mini-LSM Week 2 Day 6](https://skyzh.github.io/mini-lsm/week2-overview.html)

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's WAL record format, LSN semantics, and `fsync` discussion against Chapters 9–10.

---
<!-- toc -->
---
## Context

The LSM engine from Module 3 achieves high write throughput by buffering writes in a memtable and flushing them to SSTables in the background. But the memtable is volatile — it lives in process memory. If the process crashes, the OS kills the process, or power fails, the memtable's contents are lost. For the OOR, this means losing every TLE update since the last flush — potentially minutes of orbital data during an active pass window.

The **Write-Ahead Log (WAL)** is the solution: an append-only file on durable storage that records every mutation *before* it is applied to the memtable. The key insight is the **write-ahead rule**: no modification to the in-memory state is visible until its corresponding log record has been durably written to the WAL. If the engine crashes after logging but before flushing, the WAL contains enough information to reconstruct the memtable by replaying the logged operations.

This changes the durability guarantee from "data is safe after SSTable flush" (every 5–60 seconds) to "data is safe after WAL write" (every operation, or every batch). The cost is one sequential disk write per operation (or per batch) — but sequential appends are cheap, especially on SSDs.

---

## Core Concepts

### The Write-Ahead Rule

The rule is simple and inviolable: **log first, then mutate.** The LSM write path becomes:

1. Serialize the operation (`put` or `delete`) into a WAL log record.
2. Append the record to the WAL file.
3. Call `fsync` on the WAL file (or batch `fsync` for a group of records).
4. Apply the operation to the memtable.
5. Return success to the caller.

If the engine crashes between steps 2 and 4, the WAL contains the operation but the memtable does not — recovery will replay it. If the engine crashes before step 2, the operation was never logged — the caller did not receive a success response, so the operation is not considered committed.

### WAL Record Format

Each WAL record is a self-contained, checksummed unit:

```text
┌──────────────────────────────────────────┐
│ Record Header                            │
│  - LSN: u64 (log sequence number)        │
│  - record_type: u8 (Put=1, Delete=2)     │
│  - key_len: u16                          │
│  - value_len: u16                        │
│  - checksum: u32 (CRC32 of key+value)    │
├──────────────────────────────────────────┤
│ Key bytes (key_len bytes)                │
├──────────────────────────────────────────┤
│ Value bytes (value_len bytes, if Put)    │
└──────────────────────────────────────────┘
```

The **Log Sequence Number (LSN)** is a monotonically increasing identifier assigned to each record. LSNs establish a total order over all operations — recovery replays records in LSN order to reconstruct the exact pre-crash state. The LSN also correlates WAL records with SSTable flushes: when the memtable is flushed, the engine records the highest LSN contained in that flush. On recovery, only WAL records with LSNs greater than the last flushed LSN need to be replayed.

The **checksum** protects against partial writes: if the engine crashes mid-write, the incomplete record will have an invalid checksum and is discarded during recovery.

### fsync Strategies

`fsync` is expensive: 0.1–1ms on SSD, 5–30ms on spinning disk. Three strategies for trading durability against throughput:

**Per-operation fsync:** Maximum durability — at most one operation can be lost. Throughput limited by fsync latency (~1,000–10,000 ops/sec on SSD).

**Group commit (batch fsync):** Buffer multiple WAL writes, then `fsync` the batch. If the batch covers 100 operations and `fsync` takes 0.5ms, the amortized cost is 5µs per operation. Standard approach — used by RocksDB, PostgreSQL, MySQL.

**No fsync:** Maximum throughput, minimum durability — a crash can lose up to 30 seconds of data. Acceptable for caches, not for the OOR.

For the OOR, **group commit** is the correct choice: batch TLE updates per pass window, fsync once per batch.

### WAL in the LSM Write Path

```text
put(key, value)
     │
     ▼
 WAL append (serialize + write + optional fsync)
     │
     ▼
 Memtable insert (in-memory, fast)
     │
     ▼
 Return success
```

When the memtable is flushed to an SSTable, the engine records the **flushed LSN**. WAL records at or below this LSN are no longer needed for recovery, enabling **WAL truncation**.

---

## Code Examples

### WAL Writer: Appending Checksummed Records

```rust,ignore
use std::io::{self, Write, BufWriter};
use std::fs::{File, OpenOptions};

#[repr(u8)]
#[derive(Clone, Copy)]
enum WalRecordType {
    Put = 1,
    Delete = 2,
}

struct WalWriter {
    writer: BufWriter<File>,
    next_lsn: u64,
}

impl WalWriter {
    fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            next_lsn: 0,
        })
    }

    fn log_put(&mut self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.write_record(WalRecordType::Put, key, Some(value))
    }

    fn log_delete(&mut self, key: &[u8]) -> io::Result<u64> {
        self.write_record(WalRecordType::Delete, key, None)
    }

    fn write_record(
        &mut self,
        rec_type: WalRecordType,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> io::Result<u64> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        let val = value.unwrap_or(&[]);

        // Checksum covers key + value
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key);
        hasher.update(val);
        let checksum = hasher.finalize();

        // Header: LSN(8) + type(1) + key_len(2) + val_len(2) + checksum(4) = 17 bytes
        self.writer.write_all(&lsn.to_le_bytes())?;
        self.writer.write_all(&[rec_type as u8])?;
        self.writer.write_all(&(key.len() as u16).to_le_bytes())?;
        self.writer.write_all(&(val.len() as u16).to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(val)?;

        Ok(lsn)
    }

    /// Flush to durable storage. Call after a batch for group commit.
    fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }
}
```

### WAL Reader: Replaying Records for Recovery

```rust,ignore
struct WalRecord {
    lsn: u64,
    rec_type: WalRecordType,
    key: Vec<u8>,
    value: Vec<u8>,
}

struct WalReader {
    reader: std::io::BufReader<File>,
}

impl WalReader {
    fn next_record(&mut self) -> io::Result<Option<WalRecord>> {
        let mut header = [0u8; 17];
        match self.reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let lsn = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let rec_type = match header[8] {
            1 => WalRecordType::Put,
            2 => WalRecordType::Delete,
            _ => return Err(io::Error::new(
                io::ErrorKind::InvalidData, "invalid WAL record type",
            )),
        };
        let key_len = u16::from_le_bytes(header[9..11].try_into().unwrap()) as usize;
        let val_len = u16::from_le_bytes(header[11..13].try_into().unwrap()) as usize;
        let stored_checksum = u32::from_le_bytes(header[13..17].try_into().unwrap());

        let mut key = vec![0u8; key_len];
        let mut value = vec![0u8; val_len];
        self.reader.read_exact(&mut key)?;
        self.reader.read_exact(&mut value)?;

        // Verify checksum — detects partial writes from crashes
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key);
        hasher.update(&value);
        if hasher.finalize() != stored_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL checksum mismatch at LSN {} — partial write detected", lsn),
            ));
        }

        Ok(Some(WalRecord { lsn, rec_type, key, value }))
    }
}
```

The checksum verification is the corruption boundary: a failed checksum means the engine crashed mid-write. All prior records are valid; this record and everything after it are discarded.

---

## Key Takeaways

- The write-ahead rule is absolute: log the operation to durable storage before applying it to the memtable. This guarantees that committed operations survive crashes.
- `fsync` is the durability boundary. Group commit amortizes the cost across many operations — the standard approach for production engines.
- Each WAL record is self-contained with a CRC32 checksum. Partial writes are detected and discarded during recovery.
- The LSN orders all operations and correlates WAL records with SSTable flushes. WAL records at or below the flushed LSN are safe to truncate.
- WAL writes are sequential appends — the cheapest form of disk I/O.

---

{{#quiz lesson-01-quiz.toml}}
