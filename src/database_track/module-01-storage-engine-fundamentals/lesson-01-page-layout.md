# Lesson 1 — File Formats and Page Layout

**Module:** Database Internals — M01: Storage Engine Fundamentals  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 1–3; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3

> **Source note:** This lesson was synthesized from training knowledge. The following concepts would benefit from verification against the source books: exact page header field sizes in Petrov Ch. 3, the magic byte conventions across SQLite/InnoDB/RocksDB, and Petrov's specific framing of the page abstraction layer.

---

## Context

Every storage engine eventually answers the same question: how do bytes get from memory to disk and back? The answer is almost never "write them sequentially and hope for the best." Sequential writes are fast, but random reads against an unstructured file are catastrophic — seeking to an arbitrary byte offset in a 10GB file on a spinning disk costs 5–10ms per seek. Even on SSDs, random 512-byte reads are an order of magnitude slower than reading aligned 4KB blocks.

The solution, used by virtually every production storage engine from SQLite to RocksDB to PostgreSQL, is the **page abstraction**: divide the file into fixed-size blocks (pages), give each page a numeric identifier, and build all higher-level structures — indices, records, free lists — on top of this uniform unit. Pages align with the OS virtual memory system (typically 4KB) and the storage device's block size, which means reads and writes hit the hardware at its natural granularity.

For the Orbital Object Registry, each TLE record is approximately 140 bytes (two 69-character lines plus metadata). A single 4KB page can hold roughly 25–28 TLE records. With 100,000 tracked objects, the entire catalog fits in approximately 4,000 pages — about 16MB. The page format you design in this lesson is the physical foundation that every subsequent module builds on.

---

## Core Concepts

### The Page Abstraction

A page is a fixed-size block of bytes — the atomic unit of I/O in a storage engine. The engine never reads or writes less than one page. This constraint seems wasteful (reading 4KB to retrieve a 140-byte TLE record), but it aligns with how hardware and operating systems actually work:

- **Disk drives** read and write in sectors (512 bytes or 4KB for modern drives). Reading 1 byte costs the same as reading 4KB — the drive fetches the entire sector regardless.
- **The OS page cache** manages memory in 4KB pages. A storage engine that uses the same page size gets free alignment with the kernel's caching layer.
- **`mmap` and direct I/O** both operate on page-aligned boundaries. Misaligned reads require the kernel to fetch extra pages and copy the relevant bytes — an unnecessary overhead.

Common page sizes: 4KB (SQLite, default PostgreSQL), 8KB (PostgreSQL configurable, InnoDB), 16KB (InnoDB default), 64KB (some OLAP systems). Larger pages reduce the number of I/O operations for sequential scans but increase I/O amplification for point lookups (you read 64KB to get 140 bytes). The Orbital Object Registry uses **4KB pages** — the catalog is small enough that point lookup amplification matters more than scan throughput.

### Page Layout

Every page begins with a **header** that identifies the page and describes its contents. The header is the first thing the engine reads after loading a page from disk, and it must contain enough information to interpret the rest of the page without external context.

A minimal page header contains:

| Field | Size | Purpose |
|-------|------|---------|
| Magic bytes | 4 bytes | Identifies this as a valid OOR page (e.g., `0x4F4F5231` = "OOR1") |
| Page ID | 4 bytes | Unique identifier for this page within the file |
| Page type | 1 byte | Discriminant: data page, index page, overflow page, free page |
| Record count | 2 bytes | Number of active records in this page |
| Free space offset | 2 bytes | Byte offset where free space begins |
| Checksum | 4 bytes | CRC32 of the page body for integrity verification |

Total header: 17 bytes. The remaining 4,079 bytes (in a 4KB page) are available for records.

**Magic bytes** serve two purposes: they let the engine detect corrupted or misidentified files on open (if the first 4 bytes of the file aren't `OOR1`, this isn't an OOR database), and they enable file-level identification by external tools (`file` command, hex editors). Production systems like SQLite use `SQLite format 3\000` as the first 16 bytes of the file header.

**Checksums** detect bit rot and partial writes. A page whose checksum doesn't match its body was either corrupted on disk or partially written during a crash. The engine must reject it and attempt recovery from the WAL (Module 4). CRC32 is standard; some engines use xxHash for speed or SHA-256 for cryptographic integrity.

### File Organization

The database file is a contiguous sequence of pages. Page 0 is typically a **file header page** that stores metadata: database version, page size, total page count, pointer to the free list head, and engine configuration. Pages 1 through N hold data.

```text
┌──────────┬──────────┬──────────┬──────────┬─────┐
│  Page 0  │  Page 1  │  Page 2  │  Page 3  │ ... │
│ (header) │  (data)  │  (data)  │  (free)  │     │
└──────────┴──────────┴──────────┴──────────┴─────┘
     ↑
     File header: version, page size, page count,
     free list head → Page 3
```

**Addressing:** Given a page ID and the page size, the byte offset in the file is `page_id * page_size`. This arithmetic is the reason pages must be fixed-size — variable-size pages would require a separate index to locate each page, adding a layer of indirection to every I/O operation.

**Free list management:** When a page is deallocated (all records deleted, or a B-tree node is merged), it goes on the free list rather than being returned to the OS. The next allocation request takes a page from the free list before extending the file. This avoids filesystem fragmentation and keeps the file size stable under delete-heavy workloads.

### Alignment and Direct I/O

When a storage engine bypasses the OS page cache (using `O_DIRECT` on Linux), all reads and writes must be aligned to the device's block size — typically 512 bytes or 4KB. Misaligned I/O fails with `EINVAL`. Even when not using direct I/O, aligned access avoids read-modify-write cycles in the kernel's page cache.

In Rust, allocating page-aligned buffers requires care. `Vec<u8>` does not guarantee alignment beyond the default allocator's alignment (typically 8 or 16 bytes). For direct I/O, you need explicit alignment:

```rust,ignore
use std::alloc::{alloc, dealloc, Layout};

/// Allocate a page-aligned buffer for direct I/O.
/// Safety: caller must ensure `page_size` is a power of two.
fn alloc_aligned_page(page_size: usize) -> *mut u8 {
    let layout = Layout::from_size_align(page_size, page_size)
        .expect("page_size must be a power of two");
    // Safety: layout is valid (non-zero size, power-of-two alignment)
    unsafe { alloc(layout) }
}
```

Production engines wrap this in a `PageBuf` type that handles allocation, deallocation, and provides safe access to the underlying bytes.

---

## Code Examples

### Defining the Page Format for Orbital TLE Records

The Orbital Object Registry needs a page format that can store TLE records with their associated metadata. This example defines the page header, serialization, and deserialization logic.

```rust,ignore
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;

const PAGE_SIZE: usize = 4096;
const MAGIC: [u8; 4] = [0x4F, 0x4F, 0x52, 0x31]; // "OOR1"
const HEADER_SIZE: usize = 17;

/// Page types in the Orbital Object Registry.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum PageType {
    FileHeader = 0,
    Data = 1,
    Index = 2,
    Free = 3,
    Overflow = 4,
}

/// Fixed-size page header. Sits at byte 0 of every page.
#[derive(Debug)]
struct PageHeader {
    magic: [u8; 4],
    page_id: u32,
    page_type: PageType,
    record_count: u16,
    free_space_offset: u16,
    checksum: u32,
}

impl PageHeader {
    fn new(page_id: u32, page_type: PageType) -> Self {
        Self {
            magic: MAGIC,
            page_id,
            page_type,
            record_count: 0,
            // Free space starts immediately after the header
            free_space_offset: HEADER_SIZE as u16,
            checksum: 0,
        }
    }

    fn serialize(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.page_id.to_le_bytes());
        buf[8] = self.page_type as u8;
        buf[9..11].copy_from_slice(&self.record_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.free_space_offset.to_le_bytes());
        buf[13..17].copy_from_slice(&self.checksum.to_le_bytes());
    }

    fn deserialize(buf: &[u8]) -> io::Result<Self> {
        if buf[0..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid page magic bytes — not an OOR page",
            ));
        }
        Ok(Self {
            magic: MAGIC,
            page_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            page_type: match buf[8] {
                0 => PageType::FileHeader,
                1 => PageType::Data,
                2 => PageType::Index,
                3 => PageType::Free,
                4 => PageType::Overflow,
                _ => return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown page type discriminant",
                )),
            },
            record_count: u16::from_le_bytes(buf[9..11].try_into().unwrap()),
            free_space_offset: u16::from_le_bytes(buf[11..13].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[13..17].try_into().unwrap()),
        })
    }
}
```

Notice that all multi-byte integers use **little-endian** encoding (`to_le_bytes`/`from_le_bytes`). This is a deliberate choice — the engine should produce the same on-disk format regardless of the host architecture. Big-endian is equally valid (and simplifies key comparison in B-trees, as we'll see in Module 2), but you must pick one and enforce it everywhere. Mixing endianness across page types is a subtle bug that survives unit tests and explodes in production.

### Reading and Writing Pages to Disk

The page I/O layer translates between page IDs and file offsets. This is the lowest layer of the storage engine — everything above it thinks in pages, not bytes.

```rust,ignore
/// Low-level page I/O against the database file.
struct PageFile {
    file: File,
    page_size: usize,
}

impl PageFile {
    fn open(path: &str, page_size: usize) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { file, page_size })
    }

    /// Read a page from disk into the provided buffer.
    /// The buffer must be exactly `page_size` bytes.
    fn read_page(&mut self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);
        let offset = page_id as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    /// Write a page buffer to disk at the correct offset.
    fn write_page(&mut self, page_id: u32, buf: &[u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);
        let offset = page_id as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        // Note: we do NOT fsync here. Durability is the WAL's job (Module 4).
        // Calling fsync on every page write would destroy throughput —
        // a single fsync costs 1-10ms on SSD, 10-30ms on spinning disk.
        Ok(())
    }

    /// Allocate a new page at the end of the file. Returns the new page ID.
    fn allocate_page(&mut self) -> io::Result<u32> {
        let file_len = self.file.seek(SeekFrom::End(0))?;
        let page_id = (file_len / self.page_size as u64) as u32;
        let zeroed = vec![0u8; self.page_size];
        self.file.write_all(&zeroed)?;
        Ok(page_id)
    }
}
```

Two things to notice: first, `read_page` uses `read_exact`, not `read`. A short read (fewer bytes than `page_size`) means the file is truncated or corrupted — the engine must not silently accept a partial page. Second, `write_page` does not call `fsync`. This is intentional. The WAL (Module 4) provides durability guarantees; the page file relies on the WAL for crash recovery. Calling `fsync` on every page write would reduce throughput from thousands of pages/second to fewer than 100 on spinning disk.

### Computing and Verifying Page Checksums

Every page is checksummed before being written to disk. On read, the checksum is verified before the page contents are trusted. This catches bit rot, partial writes, and storage firmware bugs.

```rust,ignore
/// CRC32 checksum of the page body (everything after the checksum field).
/// We zero the checksum field before computing so the checksum is
/// deterministic regardless of the previous checksum value.
fn compute_checksum(page_buf: &[u8]) -> u32 {
    // Checksum covers bytes 17..PAGE_SIZE (the body).
    // The header's checksum field (bytes 13..17) is excluded from the
    // computation — it stores the result.
    let body = &page_buf[HEADER_SIZE..];
    crc32fast::hash(body)
}

fn write_page_with_checksum(
    page_file: &mut PageFile,
    page_id: u32,
    buf: &mut [u8],
) -> io::Result<()> {
    let checksum = compute_checksum(buf);
    buf[13..17].copy_from_slice(&checksum.to_le_bytes());
    page_file.write_page(page_id, buf)
}

fn read_and_verify_page(
    page_file: &mut PageFile,
    page_id: u32,
    buf: &mut [u8],
) -> io::Result<()> {
    page_file.read_page(page_id, buf)?;
    let stored = u32::from_le_bytes(buf[13..17].try_into().unwrap());
    let computed = compute_checksum(buf);
    if stored != computed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "page {} checksum mismatch: stored={:#010x}, computed={:#010x}",
                page_id, stored, computed
            ),
        ));
    }
    Ok(())
}
```

The checksum covers only the page body, not the header's checksum field itself. This avoids a circular dependency: you can't include the checksum in the data being checksummed. Some engines (e.g., PostgreSQL) checksum the entire page with the checksum field zeroed before computation — both approaches work, but you must document which one you use.

---

## Key Takeaways

- The page is the atomic unit of I/O in a storage engine. All reads and writes operate on full pages, never partial pages. This aligns with hardware block sizes and OS page cache granularity.
- Page size is a tradeoff: larger pages reduce I/O count for scans but increase amplification for point lookups. 4KB is the default for OLTP-style workloads like the Orbital Object Registry.
- Every page starts with a header containing magic bytes, page ID, type discriminant, and a checksum. The header must be self-describing — the engine should be able to interpret any page without external context.
- Byte order must be fixed across the entire on-disk format. Pick little-endian or big-endian and enforce it everywhere. Never rely on native endianness.
- Page writes do not call `fsync`. Durability is provided by the write-ahead log, not by synchronous page flushes. This is a fundamental architectural decision that separates high-throughput engines from naive implementations.

---

{{#quiz lesson-01-quiz.toml}}
