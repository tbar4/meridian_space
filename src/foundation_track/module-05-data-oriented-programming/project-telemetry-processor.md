# Project — High-Throughput Telemetry Packet Processor

**Module:** Foundation — M05: Data-Oriented Design in Rust
**Prerequisite:** All three module quizzes passed (≥70%)

---

## Mission Brief

**TO:** Platform Engineering
**FROM:** Mission Control Systems Lead
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL
**SUBJECT:** RFC-0055 — Telemetry Packet Processor Performance Target

---

The current Rust-language telemetry processor runs at 62,000 frames per second under sustained load. The conjunction avoidance pipeline requires 100,000 frames per second to maintain sub-10-second delivery windows during peak orbital density events. The gap is 38%. Profiling shows two root causes:

1. **Allocator pressure.** The processor allocates a `Vec<u8>` per frame payload on the global heap. At 100k fps, this is 100k `malloc`/`free` round-trips per second — 18% of CPU time.
2. **Cache waste.** The `TelemetryFrame` struct packs hot routing fields with cold payload data. Sequential scan of 100k frames for deduplication loads 2.4× more data than the deduplication logic uses.

Your task is to rebuild the processor core using the three techniques from this module: cache-optimal struct layout, SoA separation of hot and cold data, and arena allocation for frame payloads.

---

## System Specification

### Frame Structure

```rust,edition2021
/// Hot fields — accessed in every pass (routing, deduplication, sorting).
/// Must fit in ≤ 32 bytes and be ordered by descending alignment.
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    pub timestamp_ms: u64,
    pub sequence:     u64,
    pub satellite_id: u32,
    pub byte_count:   u16,
    pub station_id:   u8,
    pub flags:        u8,
}

/// Cold data — accessed only when forwarding to downstream consumers.
/// Held as a reference into the batch arena; lifetime is one processing epoch.
pub struct FramePayload<'arena> {
    pub data: &'arena [u8],
}
```

### Processing Pipeline

The processor receives frames in batches of up to 10,000. For each batch:

1. **Claim** payload space from the batch arena for each frame.
2. **Validate** each frame's CRC (simulated: check that `flags & 0x80 == 0`).
3. **Deduplicate** by `(satellite_id, sequence)` — discard duplicates using a SoA scan over hot headers.
4. **Sort** the batch by `timestamp_ms` ascending — sort only the header array, not the payloads.
5. **Forward** unique sorted frames to a `tokio::sync::mpsc::Sender<ForwardedFrame>`.
6. **Reset** the arena — all payload allocations freed simultaneously.

### Performance Target

- Process 100,000 frames per second sustained across a benchmark of 10,000 batches × 1,000 frames.
- Arena allocation must be used for frame payloads — no `Vec<u8>` per payload.
- Hot field access (deduplication and sort) must operate on the header array, not the full frame struct.
- Struct size assertions must compile: `size_of::<FrameHeader>() == 24`.

### Output

A binary crate that:
1. Generates synthetic frame batches
2. Runs the full pipeline (validate → deduplicate → sort → forward) for 10,000 batches
3. Reports frames per second, percentage of duplicates discarded, and arena reset count
4. Confirms no per-frame heap allocations occur in the hot path (verified by measuring allocator calls)

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | `size_of::<FrameHeader>() == 24` — const assertion in source | Yes — compile-time |
| 2 | Frame payloads allocated from batch arena, not global heap | Yes — code review: no `Vec::new()` or `Box::new()` in hot path |
| 3 | Deduplication operates on `&[FrameHeader]` — no full struct access | Yes — code review |
| 4 | Sort operates on the header array by index — payloads not moved | Yes — code review |
| 5 | Arena resets after each batch — `used` bytes reset to 0 | Yes — assert in batch loop |
| 6 | Benchmark reports ≥ 100,000 frames/sec on a modern laptop | Yes — timing output |
| 7 | Duplicate detection uses a `HashSet<(u32, u64)>` on header fields only | Yes — code review |

---

## Hints

<details>
<summary>Hint 1 — FrameHeader size assertion</summary>

```rust,edition2021
const _: () = assert!(
    std::mem::size_of::<FrameHeader>() == 24,
    "FrameHeader must be 24 bytes — check field order and padding"
);
```

Field order for 24 bytes with no padding:
- `timestamp_ms: u64` (8)
- `sequence: u64` (8)
- `satellite_id: u32` (4)
- `byte_count: u16` (2)
- `station_id: u8` (1)
- `flags: u8` (1)
= 24 bytes, alignment = 8, no padding.

</details>

<details>
<summary>Hint 2 — Batch arena design</summary>

Pre-allocate the slab once per processor lifetime. Reset between batches:

```rust,edition2021
pub struct BatchArena {
    slab: Vec<u8>,
    offset: usize,
}

impl BatchArena {
    pub fn new(capacity: usize) -> Self {
        Self { slab: vec![0u8; capacity], offset: 0 }
    }

    /// Allocate `size` bytes; returns a mutable slice into the slab.
    pub fn alloc(&mut self, size: usize) -> Option<&mut [u8]> {
        let aligned = (self.offset + 7) & !7; // 8-byte alignment
        let end = aligned + size;
        if end > self.slab.len() { return None; }
        self.offset = end;
        Some(&mut self.slab[aligned..end])
    }

    /// Reset — all previous allocations implicitly freed.
    pub fn reset(&mut self) { self.offset = 0; }
    pub fn used(&self) -> usize { self.offset }
}
```

Size the arena for worst-case batch: `max_batch_size * max_payload_size`.

</details>

<details>
<summary>Hint 3 — SoA deduplication</summary>

Maintain a `Vec<FrameHeader>` (hot, dense) separate from payloads:

```rust,edition2021
use std::collections::HashSet;

fn deduplicate(headers: &[FrameHeader]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(headers.len());
    headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| {
            if seen.insert((h.satellite_id, h.sequence)) {
                Some(i) // Unique — keep this index.
            } else {
                None    // Duplicate — discard.
            }
        })
        .collect()
}
```

The deduplication loop touches only `satellite_id` and `sequence` from `FrameHeader` — 12 bytes of the 24-byte struct. With 1,000 headers per batch at 24 bytes each, the working set is 24 KB — fits in L1 cache.

</details>

<details>
<summary>Hint 4 — Sort headers by timestamp without moving payloads</summary>

Sort an index array by `headers[i].timestamp_ms`, not the headers themselves. This avoids any payload movement:

```rust,edition2021
fn sort_by_timestamp(indices: &mut Vec<usize>, headers: &[FrameHeader]) {
    indices.sort_unstable_by_key(|&i| headers[i].timestamp_ms);
}
```

Iterating `indices` in sorted order gives frames in timestamp order without copying or moving any data.

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation</summary>

```rust,edition2021
use std::collections::HashSet;
use std::time::Instant;

// --- FrameHeader ---

#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    pub timestamp_ms: u64,
    pub sequence:     u64,
    pub satellite_id: u32,
    pub byte_count:   u16,
    pub station_id:   u8,
    pub flags:        u8,
}

const _SIZE: () = assert!(std::mem::size_of::<FrameHeader>() == 24);
const _ALIGN: () = assert!(std::mem::align_of::<FrameHeader>() == 8);

// --- BatchArena ---

pub struct BatchArena {
    slab: Vec<u8>,
    offset: usize,
}

impl BatchArena {
    pub fn new(capacity: usize) -> Self {
        Self { slab: vec![0u8; capacity], offset: 0 }
    }

    pub fn alloc(&mut self, size: usize) -> Option<&mut [u8]> {
        let aligned = (self.offset + 7) & !7;
        let end = aligned + size;
        if end > self.slab.len() { return None; }
        self.offset = end;
        Some(&mut self.slab[aligned..end])
    }

    pub fn reset(&mut self) { self.offset = 0; }
    pub fn used(&self) -> usize { self.offset }
}

// --- Pipeline ---

fn validate(header: &FrameHeader) -> bool {
    header.flags & 0x80 == 0  // Simulated CRC: high bit = error flag.
}

fn deduplicate_indices(headers: &[FrameHeader]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(headers.len());
    headers.iter().enumerate().filter_map(|(i, h)| {
        if seen.insert((h.satellite_id, h.sequence)) { Some(i) } else { None }
    }).collect()
}

fn sort_indices_by_timestamp(indices: &mut Vec<usize>, headers: &[FrameHeader]) {
    indices.sort_unstable_by_key(|&i| headers[i].timestamp_ms);
}

fn process_batch(
    arena: &mut BatchArena,
    batch: &[(u64, u64, u32, u16, u8, u8, Vec<u8>)], // (ts, seq, sat, bytes, stn, flags, raw_payload)
) -> (usize, usize) {
    // 1. Fill header array and claim arena slots for payloads.
    let mut headers: Vec<FrameHeader> = Vec::with_capacity(batch.len());
    let mut payload_offsets: Vec<(usize, usize)> = Vec::with_capacity(batch.len()); // (start, len)

    for (ts, seq, sat, bytes, stn, flags, payload) in batch {
        let header = FrameHeader {
            timestamp_ms: *ts,
            sequence: *seq,
            satellite_id: *sat,
            byte_count: *bytes,
            station_id: *stn,
            flags: *flags,
        };

        // Validate before claiming arena space.
        if !validate(&header) { continue; }

        let slot = match arena.alloc(payload.len()) {
            Some(s) => s,
            None => break, // Arena full — drop remaining frames.
        };
        slot.copy_from_slice(payload);

        let start = arena.used() - payload.len();
        payload_offsets.push((start, payload.len()));
        headers.push(header);
    }

    // 2. Deduplicate on hot header array — SoA benefit.
    let mut unique_indices = deduplicate_indices(&headers);
    let duplicates = headers.len() - unique_indices.len();

    // 3. Sort by timestamp — header array only, no payload movement.
    sort_indices_by_timestamp(&mut unique_indices, &headers);

    let forwarded = unique_indices.len();

    // 4. Reset arena — all payload slots freed in O(1).
    arena.reset();

    (forwarded, duplicates)
}

fn main() {
    const BATCH_SIZE: usize = 1_000;
    const NUM_BATCHES: usize = 10_000;
    const MAX_PAYLOAD: usize = 256;

    let mut arena = BatchArena::new(BATCH_SIZE * MAX_PAYLOAD + 64);

    // Generate synthetic batch data.
    let batch: Vec<(u64, u64, u32, u16, u8, u8, Vec<u8>)> = (0..BATCH_SIZE)
        .map(|i| {
            let seq = (i / 3) as u64; // Every 3 frames share a sequence — ~33% duplicates.
            (
                1_700_000_000 + i as u64,
                seq,
                (i % 48) as u32,
                MAX_PAYLOAD as u16,
                (i % 12) as u8,
                0u8,
                vec![(i & 0xFF) as u8; MAX_PAYLOAD],
            )
        })
        .collect();

    let start = Instant::now();
    let mut total_forwarded = 0usize;
    let mut total_duplicates = 0usize;
    let mut resets = 0usize;

    for _ in 0..NUM_BATCHES {
        let (fwd, dups) = process_batch(&mut arena, &batch);
        total_forwarded += fwd;
        total_duplicates += dups;
        resets += 1;
        assert_eq!(arena.used(), 0, "arena must be reset after each batch");
    }

    let elapsed = start.elapsed();
    let total_frames = BATCH_SIZE * NUM_BATCHES;
    let fps = total_frames as f64 / elapsed.as_secs_f64();

    println!("--- Telemetry Processor Benchmark ---");
    println!("frames:     {}", total_frames);
    println!("forwarded:  {}", total_forwarded);
    println!("duplicates: {} ({:.1}%)", total_duplicates,
        100.0 * total_duplicates as f64 / total_frames as f64);
    println!("resets:     {}", resets);
    println!("elapsed:    {:.2?}", elapsed);
    println!("throughput: {:.0} frames/sec", fps);
    println!("FrameHeader size: {} bytes", std::mem::size_of::<FrameHeader>());
}
```

</details>

---

## Reflection

The three optimisations in this project compose:

- **Struct layout** ensures the `FrameHeader` array is compact (24 bytes/entry, no wasted padding). 24 KB for 1,000 headers — fits in L1 cache, fully available during the deduplication and sort passes.
- **SoA separation** means deduplication and sorting never touch payload data — the payload arena is not in the working set during hot-path operations.
- **Arena allocation** eliminates 100,000 per-second `malloc`/`free` round-trips. All payloads for one batch are freed in a single pointer reset.

Each optimisation is independently valuable. Together, they target the three most common sources of throughput bottlenecks in high-frequency data pipelines: allocator pressure, memory bandwidth waste, and cache thrashing.

The benchmarking mindset from Module 6 (Performance and Profiling) will give you the tools to measure these improvements precisely — comparing before and after with `criterion`, identifying the limiting factor with `perf`, and validating that the improvements hold under realistic workload conditions.
