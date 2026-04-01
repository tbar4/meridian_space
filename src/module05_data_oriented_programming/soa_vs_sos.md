# Lesson 2 — Struct-of-Arrays vs Array-of-Structs: When Each Wins

**Module:** Foundation — M05: Data-Oriented Design in Rust
**Position:** Lesson 2 of 3
**Source:** Synthesized from training knowledge. Concepts would benefit from verification against: Mike Acton's CppCon 2014 "Data-Oriented Design and C++" and Chandler Carruth's related talks.

---
<!-- toc -->
---
## Context

The Meridian conjunction screening pass processes 50,000 orbital elements every 10 minutes. Each screening step reads the altitude and inclination of every object in the catalog. It does not read the object name, the launch date, the operator contact, or any other administrative metadata. Those fields exist in the catalog, but the screening loop does not touch them.

With a conventional struct design — one `OrbitalObject` struct with all fields — the screening loop loads each full struct into cache on every iteration. The fields it actually uses are 16 bytes; the fields it ignores are perhaps 120 bytes. The ratio: 13% of every cache line brought in is useful data. The rest is wasted memory bandwidth.

This is the core insight behind struct-of-arrays (SoA) layout: if an operation only accesses a subset of fields, store those fields contiguously rather than interleaved with irrelevant fields. Processing `altitudes[0..50000]` accesses only altitude data; there is no orbital metadata in the working set, no cache line waste.

This lesson covers when SoA beats AoS, when AoS beats SoA, and how to implement the transition idiomatically in Rust.

---

## Core Concepts

### Array-of-Structs (AoS): The Default

The conventional layout: a `Vec<T>` where `T` is a struct containing all fields for one entity.

```rust,edition2021
// Array-of-Structs: all fields for one object are contiguous.
#[derive(Debug, Clone)]
struct OrbitalObject {
    norad_id: u32,         // 4 bytes
    altitude_km: f64,      // 8 bytes — used in conjunction screening
    inclination_deg: f64,  // 8 bytes — used in conjunction screening
    raan_deg: f64,         // 8 bytes — used in conjunction screening
    name: [u8; 24],        // 24 bytes — NOT used in conjunction screening
    launch_year: u16,      // 2 bytes — NOT used in conjunction screening
    _pad: [u8; 6],         // 6 bytes padding
}
// One OrbitalObject: 64 bytes. One cache line.
// The screening loop uses 28 bytes (norad_id + 3 doubles).
// 36 bytes of every cache line are irrelevant to screening.

fn screen_conjunctions_aos(objects: &[OrbitalObject], threshold_km: f64) -> Vec<u32> {
    let mut alerts = Vec::new();
    for (i, a) in objects.iter().enumerate() {
        for b in &objects[i+1..] {
            // Each iteration loads a full 64-byte OrbitalObject into cache.
            // Only altitude_km and inclination_deg are used.
            let delta = (a.altitude_km - b.altitude_km).abs();
            if delta < threshold_km {
                alerts.push(a.norad_id);
            }
        }
    }
    alerts
}
```

For a 50,000-object catalog, AoS loads 50,000 × 64 bytes = 3.2 MB per pass, even though only 28 bytes per object matter. At a 64-byte cache line, 44% of cache bandwidth is wasted on unused fields.

### Struct-of-Arrays (SoA): Fields in Separate Vectors

SoA inverts the layout: instead of one `Vec<Object>`, maintain separate `Vec<field_type>` for each field. Objects are indexed by position across all vectors.

```rust,edition2021
// Struct-of-Arrays: each field is a contiguous array.
// Access patterns that touch only a few fields see only those fields in cache.
struct OrbitalCatalog {
    // "Hot" fields — accessed every screening pass.
    norad_ids:       Vec<u32>,
    altitudes_km:    Vec<f64>,
    inclinations_deg: Vec<f64>,
    raans_deg:       Vec<f64>,
    // "Cold" fields — accessed only for display / export.
    names:           Vec<[u8; 24]>,
    launch_years:    Vec<u16>,
}

impl OrbitalCatalog {
    fn len(&self) -> usize { self.norad_ids.len() }

    fn push(&mut self, id: u32, alt: f64, inc: f64, raan: f64,
            name: [u8; 24], launch: u16) {
        self.norad_ids.push(id);
        self.altitudes_km.push(alt);
        self.inclinations_deg.push(inc);
        self.raans_deg.push(raan);
        self.names.push(name);
        self.launch_years.push(launch);
    }
}

fn screen_conjunctions_soa(catalog: &OrbitalCatalog, threshold_km: f64) -> Vec<u32> {
    let alts = &catalog.altitudes_km;
    let ids  = &catalog.norad_ids;
    let mut alerts = Vec::new();

    for i in 0..catalog.len() {
        for j in i+1..catalog.len() {
            // Only altitudes_km is touched here — 8 bytes per element.
            // 8 f64s fit in one cache line.
            // For 50k objects, working set for altitudes_km = 400KB (fits in L2).
            let delta = (alts[i] - alts[j]).abs();
            if delta < threshold_km {
                alerts.push(ids[i]);
            }
        }
    }
    alerts
}
```

The screening loop now accesses only `altitudes_km` — 50,000 × 8 bytes = 400 KB, which fits in a typical L2 cache (512KB–2MB). The names, launch years, and RAAN values are never loaded. Cache utilisation is near 100%.

### When SoA Wins

SoA is most effective when:

1. **Operations access a small subset of fields.** The conjunction screening loop uses 3 of 6 fields. SIMD vectorization of `altitudes_km - alts[j]` operates on 8 doubles per instruction with AVX2.

2. **Processing is sequential over all objects.** Iterating `altitudes_km[0..50000]` is a linear scan — the hardware prefetcher predicts the access pattern and pre-fetches cache lines ahead of the loop.

3. **Field values have uniform types amenable to SIMD.** A `Vec<f64>` can be processed with `f64x4` or `f64x8` SIMD instructions. An AoS loop cannot be auto-vectorized as efficiently because the fields are interleaved.

4. **Objects are added and removed infrequently.** SoA requires synchronized insertion and removal across all vectors. Random insertion in the middle is O(n) for every field vector simultaneously.

### When AoS Wins

AoS is more appropriate when:

1. **Operations access all or most fields of one object at a time.** Constructing a display record or serializing one object reads all fields — SoA forces jumping across multiple vectors.

2. **Access is random by index.** Looking up object ID 25544 requires one index lookup across all vectors. AoS keeps all of 25544's data in one cache line — one miss. SoA scatters it across multiple cache lines.

3. **Objects are frequently inserted, removed, or moved.** AoS insertion is a single `push`. SoA insertion is `push` across all field vectors — more work and more cache lines touched.

4. **The struct has few fields or all fields are typically accessed together.** If the struct is small (≤ 32 bytes) and all fields are used in every operation, SoA provides no benefit and complicates the API.

### Hybrid: AoS with Field Grouping

The practical approach is not a binary AoS vs SoA choice — it is grouping fields by access pattern:

```rust,edition2021
/// Hot group: fields accessed in every pass of the screening loop.
#[derive(Debug, Clone, Copy)]
struct ObjectHot {
    altitude_km:      f64,
    inclination_deg:  f64,
    raan_deg:         f64,
    eccentricity:     f64,
}

/// Cold group: fields accessed for display, export, and audit only.
#[derive(Debug, Clone)]
struct ObjectCold {
    norad_id:    u32,
    launch_year: u16,
    name:        String,
    operator:    String,
}

/// The catalog splits hot and cold data into separate vectors.
/// The index is the common key between them.
struct OrbitalCatalog {
    hot:  Vec<ObjectHot>,   // Dense; accessed every screening pass.
    cold: Vec<ObjectCold>,  // Sparse access; not in screening hot path.
}
```

The screening pass operates only on `hot` — a `Vec<ObjectHot>` of 50,000 × 32 bytes = 1.6 MB, fitting in L3 cache. `cold` is loaded only for display or export, where its access pattern (one object at a time by index) makes AoS natural.

---

## Code Examples

### Parallel Altitude Screening with Rayon and SoA

With altitudes stored in a contiguous `Vec<f64>`, the screening loop is naturally parallelisable — each parallel chunk accesses an independent range of the altitude slice:

```rust,edition2021
// Note: rayon is not available in the Playground; this demonstrates the
// pattern. In production, add rayon = "1" to Cargo.toml.

/// Simplified O(n) altitude band screening (not the full O(n²) conjunction check).
/// Finds objects in a dangerous altitude band. SoA makes this trivially parallel
/// and cache-friendly: the working set is just Vec<f64>.
fn screen_altitude_band(
    altitudes_km: &[f64],
    norad_ids: &[u32],
    min_km: f64,
    max_km: f64,
) -> Vec<u32> {
    assert_eq!(altitudes_km.len(), norad_ids.len());

    // Sequential: all altitudes fit in one contiguous slice.
    // Hardware prefetcher maximises cache utilisation.
    altitudes_km
        .iter()
        .zip(norad_ids.iter())
        .filter_map(|(&alt, &id)| {
            if alt >= min_km && alt <= max_km {
                Some(id)
            } else {
                None
            }
        })
        .collect()
}

fn main() {
    // Simulate a 10,000-object catalog.
    let altitudes_km: Vec<f64> = (0..10_000u32)
        .map(|i| 350.0 + (i as f64) * 0.05)
        .collect();
    let norad_ids: Vec<u32> = (0..10_000u32).collect();

    // Screen for objects in the 400–450 km band (high debris density).
    let alerts = screen_altitude_band(&altitudes_km, &norad_ids, 400.0, 450.0);
    println!("{} objects in 400–450km band", alerts.len());

    // Verify the working set is contiguous and predictable:
    let working_set_bytes = altitudes_km.len() * std::mem::size_of::<f64>();
    println!("working set: {} KB", working_set_bytes / 1024);
}
```

### Transposing AoS to SoA Incrementally

Transitioning an existing AoS codebase to SoA does not require a full rewrite. Extract the hot fields into a companion SoA structure, index both by the same key:

```rust,edition2021
// Existing AoS type — not changed, other code still uses it.
#[derive(Debug, Clone)]
struct TelemetryFrame {
    satellite_id: u32,
    sequence:     u64,
    timestamp_ms: u64,
    station_id:   u8,
    payload:      Vec<u8>,
}

// New SoA hot path for bulk sequence-number deduplication.
// Built from the AoS data; kept in sync on insert.
struct FrameSequenceIndex {
    satellite_ids: Vec<u32>,
    sequences:     Vec<u64>,
}

impl FrameSequenceIndex {
    fn from_frames(frames: &[TelemetryFrame]) -> Self {
        Self {
            satellite_ids: frames.iter().map(|f| f.satellite_id).collect(),
            sequences:     frames.iter().map(|f| f.sequence).collect(),
        }
    }

    /// Find all duplicate (satellite_id, sequence) pairs — O(n) scan,
    /// cache-friendly because both vecs are small and contiguous.
    fn find_duplicates(&self) -> Vec<usize> {
        let mut seen = std::collections::HashSet::new();
        self.satellite_ids
            .iter()
            .zip(self.sequences.iter())
            .enumerate()
            .filter_map(|(i, (&sat, &seq))| {
                if !seen.insert((sat, seq)) { Some(i) } else { None }
            })
            .collect()
    }
}

fn main() {
    let frames: Vec<TelemetryFrame> = (0..5u32).map(|i| TelemetryFrame {
        satellite_id: i % 3,
        sequence: (i / 3) as u64,
        timestamp_ms: 1_700_000_000 + i as u64,
        station_id: 1,
        payload: vec![i as u8; 128],
    }).collect();

    let index = FrameSequenceIndex::from_frames(&frames);
    let dups = index.find_duplicates();
    println!("{} duplicate frames found", dups.len());
}
```

The `FrameSequenceIndex` co-exists with the original `Vec<TelemetryFrame>`. Hot operations use the index; display and forwarding use the original frames. The transition is incremental — no global refactor required.

---

## Key Takeaways

- AoS is natural when operations access all or most fields of one entity at a time, or when random access by index is common. SoA is natural when operations process all entities but only a few fields — the common case in simulation and batch processing.

- SoA improves cache utilisation for field-subset operations because the working set is smaller: processing `altitudes_km[0..n]` loads only altitude data, not names, metadata, or other cold fields.

- Sequential access of a contiguous `Vec<f64>` is maximally cache-friendly and SIMD-friendly. The hardware prefetcher predicts linear access patterns; SIMD intrinsics or auto-vectorisation require uniformly-typed contiguous data.

- The practical pattern is field grouping: split hot fields (accessed in every loop) from cold fields (accessed occasionally), and store them in separate vecs. This is a hybrid AoS/SoA approach.

- Transitioning from AoS to SoA does not require a full rewrite. Extract hot fields into a companion SoA index, keep both in sync on insert, and route hot-path operations through the index.

---

{{#quiz lesson-02-quiz.toml}}
