# Lesson 3 — Schema and Partition Evolution

**Module:** Data Lakes — M04: Time Travel and Schema Evolution
**Position:** Lesson 3 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 5 ("Encoding and Evolution"), especially "Schema Evolution" and "Forward and Backward Compatibility". Apache Iceberg specification, "Schema Evolution" and "Partition Evolution" sections.

> **Source note:** The schema-evolution principles are well-grounded in DDIA. The format-specific shape (column IDs, per-snapshot schema IDs, partition spec IDs) is from the Iceberg spec; verification of the field-mapping rules against the current spec recommended.

---

## Context

Long-lived tables outlive the schemas they started with. New columns get added as instrumentation grows; old columns get retired as features change; sometimes a column's type needs widening or its meaning needs renaming. In a row-store database these operations are routine — `ALTER TABLE ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` — but in a lakehouse the discipline is different. The data files are immutable; you cannot rewrite a million Parquet files just because a column changed. The schema-evolution mechanism must work *against* the existing data, projecting old data files onto the new schema without rewriting them.

The mechanism Iceberg uses is **column IDs**. Every column in the table has a unique numeric ID, recorded in the schema and stamped into every data file's Parquet schema. Column *names* are display labels in the metadata; what identifies a column physically is its ID. Adding a column allocates a new ID; renaming a column changes the display label but preserves the ID; dropping a column marks the ID as retired. The data files are never touched. Readers project files against the current schema by matching column IDs; columns missing from a file (because they were added after the file was written) appear as nulls; columns present in the file but absent from the current schema (because they were dropped) are filtered out.

Partition specs evolve under the same discipline, with a separate **partition spec ID** that links each snapshot to the spec under which it was written. A table can change its partition spec without rewriting historical data; new commits use the new spec, old commits remain under their original spec, and the planner handles both during reads.

This lesson develops both evolution mechanisms. The capstone's Mission Replay Engine exercises schema evolution heavily — replays against snapshots from before a column was added must project against the snapshot-time schema, with the column absent.

---

## Core Concepts

### Column IDs: The Decoupling Principle

DDIA (Ch. 5, "Schema Evolution") draws the right framing for any schema-evolving system: **physical storage must reference fields by something stable, not by display name**. Protocol Buffers uses field tags; Avro uses position-plus-name lookup with explicit aliases. Iceberg uses column IDs.

The mechanism. The table's schema records, for each column, a numeric `id`, a `name`, a `type`, and a nullability flag. When the writer commits a data file, the Parquet schema in the file's footer records the column IDs as Parquet field metadata (typically via the `field_id` annotation). The data file's bytes are stored in column-ID order, not name order, with the schema in the footer providing the mapping.

When the reader projects a data file against a different schema (a later schema after a rename, an earlier schema after time travel, the current schema after a column drop), the projection uses **column IDs**:

- A column in the projection's schema with ID K is mapped to the data file's column-with-ID-K, regardless of the column's name in either schema.
- A column in the projection's schema with no matching ID in the data file (e.g., added later) is filled with nulls.
- A column in the data file with no matching ID in the projection's schema (e.g., dropped) is ignored.

The result is that **renaming a column is free** at the data layer. The schema's display name changes; the column ID doesn't. Old data files continue to be read correctly because the column ID still matches. No data rewrite, no migration, no compatibility window — the change is a metadata commit.

### Safe Schema Changes

The schema-evolution rules fall into three categories: trivially safe, safe with care, and structurally impossible.

**Trivially safe** — pure metadata changes that do not affect any existing data file's interpretation:

- **Add a nullable column.** Allocates a new column ID. Existing data files don't have it; reads back-fill nulls. New writes include it. The column's nullability is required because existing rows have no value for it.
- **Drop a column.** Marks the ID as retired in the schema. Existing data files retain the bytes; the reader filters them out. The bytes are reclaimed only by Module 6's compaction, which rewrites files under the new schema. A dropped column's ID is never reused — the retirement is permanent, so a future "re-add the same column" allocates a new ID rather than restoring the old one.
- **Rename a column.** Changes the display name; preserves the ID. Trivial.
- **Reorder columns.** Changes the display order in the schema; the data file's physical layout is unchanged. Trivial.

**Safe with care** — changes that affect interpretation but can be handled correctly with explicit rules:

- **Widen a column's type.** `int32` → `int64`, `float32` → `float64`, `decimal(8, 2)` → `decimal(10, 2)`. The new type can represent every value the old type could, so existing data files (which contain the old type's bytes) can be read back and converted at projection time. The Iceberg spec enumerates the allowed widenings; narrowings (the reverse) are not safe and require explicit migration.
- **Make a non-nullable column nullable.** Adds null to the value set; existing data has no nulls but the schema now permits them. Safe.
- **Add a struct field to a complex type.** Same logic as adding a top-level nullable column.

**Structurally impossible without data rewrite**:

- **Narrow a type.** `int64` → `int32` requires checking every value in existing data; values that overflow the narrower type have no defined behavior. The Iceberg spec rejects type narrowings; if the application needs this, it must rewrite the data under a new schema.
- **Change a column's type to an incompatible type.** `string` → `int32` is impossible without parsing every value; some values may not parse. Rejected.
- **Make a nullable column non-nullable.** Requires asserting no nulls exist; existing files may contain nulls that the new schema forbids. Rejected unless the application explicitly validates and is prepared to rewrite.

The Iceberg spec's tables of allowed and disallowed type changes are the canonical reference. The discipline the writer must enforce is: **commit a schema change only if the change is in the allowed-without-rewrite set**, or run an explicit migration that produces new data under the new schema before swapping it in.

### Schema History and Per-Snapshot Schema IDs

A table's schema can change many times over its lifetime. The Iceberg metadata records **every schema the table has had**, in the `schemas` field of the table metadata. Each schema has a `schema_id`; the current schema is identified by `current_schema_id`. Every snapshot records which `schema_id` was current at its commit time.

The shape of this metadata. The table metadata file (the file the catalog points at, distinct from the snapshot files) has fields:

- `schemas`: `Vec<Schema>` — every schema the table has ever had.
- `current_schema_id`: `u32` — the active schema.
- `snapshots`: indirectly via the snapshot files; each snapshot records its `schema_id`.

Time-travel reads use the snapshot's recorded `schema_id` to find the schema to project against (snapshot-time mode) or use the `current_schema_id` (current mode). Both lookups are O(1) against the in-memory `schemas` vector. The vector grows by one entry per schema change; for tables with stable schemas it stays small. For aggressively-evolving tables it grows linearly with time — production deployments compact the schema history periodically (Module 6) by removing schemas that no live snapshot references.

### Partition Spec Evolution

The partition spec evolves under the same per-snapshot pattern. Each snapshot records the `partition_spec_id` that was current at its commit time; the table metadata maintains a `partition_specs` vector recording every spec the table has had.

The structural reason this needs to be per-snapshot is the same as for schema: **data files are immutable**, so a file written under spec A cannot be rewritten under spec B without explicit migration. Reading a table that has changed partition specs requires the planner to apply the *original* spec when reading files written under that spec, and the *current* spec when reading files written under it.

The mechanic. Each manifest is tagged with the partition spec ID under which its data files were committed. Manifest entries' `partition` field is interpreted under that spec. A query against a table whose history includes spec changes plans against multiple specs simultaneously:

- For each manifest, identify its spec.
- Lift the source-column predicates through that spec's transforms (Module 3's lifting rules).
- Apply the lifted predicate to the manifest's partition summary.
- Continue into the manifest if the summary matches.

The cost is bounded by the number of distinct partition specs in the table's history — typically one or two over a table's lifetime. The Artemis cold archive started with `partition by (mission_id)` for the first six months of its life, switched to `partition by (mission_id, day(ts))` when daily query volume justified the finer granularity, and has been on the current spec for two years. The planner handles both specs cleanly; the spec-1-history reads use the coarse partitioning and the spec-2 reads use the fine partitioning. The two coexist in the same query plan without conflict.

### Migrating Data Under a New Spec

Partition spec evolution does not automatically rewrite old data — but eventually the operator may want to. The migration discipline is:

1. **Commit the new spec.** The new `partition_specs` entry is added to the table metadata; `default_spec_id` is updated to point at it. From this point, new commits use the new spec.
2. **Run a compaction job that rewrites old data.** The Module 6 compaction reads files written under old specs, repartitions them under the new spec, and writes them as new files. Each compaction commit removes the old files and adds the new ones (an overwrite commit; Module 2 covered the protocol).
3. **Eventually retire old specs.** When no live snapshot references files written under the old spec, the spec can be removed from the table metadata's `partition_specs` vector. This is cosmetic — the spec being present costs nothing operationally — but keeps the metadata tidy.

The migration is a long-running background job. For the Artemis archive, the spec-1-to-spec-2 migration ran over six weeks, processing roughly 1% of the table per day to avoid impacting the analyst workload. During the migration, queries returned correct results against both specs; users were unaware of the in-progress migration. This is the same property that makes schema evolution safe: **the table's logical contract is unchanged during migration**, and clients see consistent results throughout.

### Operational Discipline: Schema Changes Are Commits

A subtle but important property: **a schema change is a commit, with the same CAS-and-retry semantics as a data commit** (Module 2 Lesson 3). The change is recorded in a new snapshot; the snapshot is added via the optimistic-CAS protocol; conflicts are handled by retry. There is no separate "DDL transaction" or "schema lock" — schema changes are first-class commits.

This has two practical consequences. **Schema changes are observable in the snapshot history.** Every change has a timestamp, a commit author (if tracked), and a snapshot ID; the audit trail is automatic. **Schema changes can be rolled back** by committing a new snapshot that reverts the change — the rollback is just another schema change. There is no "ALTER TABLE UNDO" primitive; the rollback is a forward commit that happens to undo a prior one. Both properties make schema evolution easier to reason about than in traditional databases; the lakehouse's transactional model applies uniformly.

The Artemis archive's schema-change discipline runs every change through a pull request that includes the new schema, the data-validation tests, and a rollback plan. The schema is committed to the table by the merge-to-main CI job; the change appears in the snapshot history with the commit hash recorded in the snapshot's metadata. Six months later, an investigator can identify exactly when a column was added and trace it to the originating PR.

---

## Core Mechanics in Code

### The Schema Type with Column IDs

The schema's shape, with column IDs as the physical-identity field:

```rust,ignore
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub schema_id: u32,
    pub fields: Vec<Field>,
    pub identifier_field_ids: Vec<u32>, // primary-key-like; for merge ops
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Stable column ID. The physical identity of this column.
    /// Allocated at column-creation time; never reused after drop.
    pub field_id: u32,
    /// Display name. Can change via rename without affecting data.
    pub name: String,
    /// The column's logical type.
    pub data_type: DataType,
    /// Whether the column allows nulls.
    pub required: bool,
}
```

### Projecting a Data File Against a Different Schema

The reader's per-file projection: given the schema written into the data file and the schema we want to project against, produce a per-column mapping.

```rust,ignore
use std::collections::HashMap;
use anyhow::Result;

#[derive(Debug)]
pub enum ProjectedColumn {
    /// The file has this column under a known internal name; read it
    /// from there and present under the target schema's name.
    FromFile { file_column_name: String, target_field: Field },
    /// The file doesn't have this column (it was added after the file
    /// was written); fill with nulls of the target type.
    Null { target_field: Field },
}

pub fn build_projection(
    file_schema: &Schema,
    target_schema: &Schema,
) -> Vec<ProjectedColumn> {
    // Index the file's columns by field_id for O(1) lookup.
    let file_by_id: HashMap<u32, &Field> = file_schema
        .fields
        .iter()
        .map(|f| (f.field_id, f))
        .collect();

    target_schema
        .fields
        .iter()
        .map(|target| {
            match file_by_id.get(&target.field_id) {
                Some(file_field) => ProjectedColumn::FromFile {
                    file_column_name: file_field.name.clone(),
                    target_field: target.clone(),
                },
                None => ProjectedColumn::Null {
                    target_field: target.clone(),
                },
            }
        })
        .collect()
}
```

The pattern. The projection is keyed on `field_id`, never on `name`. A file written under schema where `voltage` was the column name and a target schema where the same column has been renamed to `voltage_v` both produce a `FromFile` mapping if their `field_id` matches; the rename is transparent. A target column with `field_id` not in the file produces a `Null` mapping; the file simply doesn't have that column. The result drives the Parquet reader's column-selection and the post-read column rename/null-fill.

### Committing a Schema Change

A schema-change commit produces a new schema, adds it to the table's schemas list, and commits a new snapshot that references the new schema.

```rust,ignore
use anyhow::{Context, Result};

/// Add a new nullable column to the table. The function:
/// 1. Reads the current table metadata.
/// 2. Builds a new schema with the column appended.
/// 3. Commits a new snapshot referencing the new schema.
/// The data files are untouched; this is a pure metadata change.
pub async fn add_nullable_column(
    catalog: &PostgresCatalog,
    table: &str,
    column_name: String,
    data_type: DataType,
) -> Result<SnapshotId> {
    // Standard retry loop (Module 2 Lesson 3).
    for _ in 0..16 {
        let entry = catalog.get_current(table).await?;
        let mut metadata = read_table_metadata_at(&entry.metadata_path).await?;

        // Allocate a new column ID. The convention is "last_column_id + 1";
        // the table metadata tracks this monotonically.
        let new_field_id = metadata.last_column_id + 1;

        let mut new_schema = metadata.current_schema().clone();
        new_schema.schema_id = metadata.last_schema_id + 1;
        new_schema.fields.push(Field {
            field_id: new_field_id,
            name: column_name.clone(),
            data_type: data_type.clone(),
            required: false, // nullable; existing rows have no value
        });

        // Update the table metadata: add the new schema, advance the
        // current_schema_id, advance the column counter, leave the
        // current snapshot (data state) alone.
        metadata.schemas.push(new_schema.clone());
        metadata.current_schema_id = new_schema.schema_id;
        metadata.last_column_id = new_field_id;
        metadata.last_schema_id = new_schema.schema_id;

        // Commit the metadata change as a new snapshot. The snapshot's
        // file set is identical to the parent's; only the schema_id
        // differs. (Iceberg supports this as a "metadata-only" snapshot.)
        let new_snapshot = build_metadata_only_snapshot(
            &metadata,
            new_schema.schema_id,
            "add_column",
        )?;
        let new_metadata_path = write_table_metadata(&metadata, &new_snapshot).await?;

        // CAS the catalog as in any commit.
        match catalog.compare_and_swap(
            table,
            entry.current_snapshot_id,
            new_snapshot.snapshot_id,
            &new_metadata_path,
        ).await {
            Ok(()) => return Ok(new_snapshot.snapshot_id),
            Err(CommitError::Conflict(_)) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(anyhow::anyhow!("schema change failed after retries"))
}
```

What to notice. The schema change is a snapshot with no data changes — the file set is identical to the parent. The CAS protocol from Module 2 is unchanged; schema changes are commits like any other. The new `field_id` is monotonically allocated from the table metadata; it is never reused even if columns are dropped. The validation discipline (is the change in the allowed-without-rewrite set?) is not shown here but is the writer's responsibility before calling the function — adding a non-nullable column, for instance, should error out at the API surface rather than producing data files the reader cannot interpret.

---

## Key Takeaways

- **Column IDs are the physical-identity field**; column names are display labels. Renaming a column is free at the data layer because the ID is unchanged; old data files continue to be read correctly via ID matching.
- **Adding a nullable column, dropping a column, renaming a column, and widening a type are safe schema changes** — pure metadata operations with no data rewrite. Type narrowings, nullability tightenings, and incompatible type changes are structurally impossible without explicit migration.
- **The schema history is recorded in the table metadata**; each snapshot records the `schema_id` that was current at its commit. Time-travel reads project against the snapshot-time schema by default; the current-schema option back-fills nulls and drops removed columns.
- **Partition specs evolve under the same per-snapshot pattern.** A table's history can include multiple specs; the planner applies each manifest's spec independently during reads. Migration to a new spec is a long-running background job (Module 6 compaction) that rewrites old data into the new spec over time.
- **Schema and partition changes are commits**, not separate DDL operations. They go through the same CAS-and-retry protocol; they appear in the snapshot history; they are observable and rollback-able by the standard commit machinery.

{{#quiz quiz-03.toml}}
