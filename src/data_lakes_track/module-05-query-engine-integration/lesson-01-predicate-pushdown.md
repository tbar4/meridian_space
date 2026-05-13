# Lesson 1 — Predicate Pushdown and the Pruning Pyramid

**Module:** Data Lakes — M05: Query Engine Integration
**Position:** Lesson 1 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 4 ("Storage and Retrieval — Column-Oriented Storage"). *Database Internals* — Alex Petrov, Chapter 9 ("Query Processing"). Apache DataFusion documentation, "TableProvider" interface.

> **Source note:** The pushdown protocol's structure is well-established across query engines (DataFusion, Spark, Trino, DuckDB). The lesson grounds the framing in DDIA's column-store treatment; the specific API surface details are DataFusion-flavored but the contract is universal.

---

## Context

Module 3 developed pruning at the storage layer: given a predicate, the table format returns the minimal set of files (and row groups inside them) to read. But the predicate comes from somewhere — specifically, from the query engine that parsed the analyst's SQL. The contract that lets the engine hand the predicate down to the storage layer is **predicate pushdown**. This lesson develops that contract end to end.

The right framing is the one DDIA (Ch. 4) uses for column-store optimization: "If only relevant rows can be loaded from disk, the query is much faster." Predicate pushdown is the discipline that makes "only relevant rows" achievable across the engine-storage boundary. The engine knows the query's predicate; the storage knows the per-file statistics; pushdown lets them collaborate. The engine pushes the predicate down; the storage returns a pruned plan; the engine reads against the pruned plan and applies any residual filtering on the rows the storage returns.

The contract has subtle asymmetries. Not every SQL predicate is pushable (function calls on columns, predicates involving multiple tables, predicates over computed columns). Pushable predicates may be pushed partially (the engine pushes the part the storage can evaluate; the engine evaluates the rest). The storage may not prune as aggressively as the predicate would allow (conservative pruning is the right default; Module 3 Lesson 3 covered this). All of these complications come from the same correctness asymmetry: false-positives are acceptable; false-negatives are silent data loss. The pushdown contract is written to honor the asymmetry at every layer.

This lesson develops the contract's mechanics: what the engine hands down, what the storage returns, how partial pushdown works, and how the residual predicate is reapplied after the storage layer has done its part. The capstone implements a DataFusion `TableProvider` that does exactly this for the Artemis archive.

---

## Core Concepts

### The Pushdown Contract

The interface between the engine and the storage layer for predicate pushdown is a single method, in spirit:

```text
push_predicates(engine_predicates: Vec<Expr>) → PushdownResult {
    pushed:    Vec<Expr>,  // storage handled these completely
    inexact:   Vec<Expr>,  // storage handled approximately; reapply
    unhandled: Vec<Expr>,  // storage cannot evaluate; engine must
}
```

The three-way classification is essential. **Pushed** predicates are evaluated exactly by the storage; the engine does not reapply them. **Inexact** predicates are evaluated approximately — the storage prunes some rows but the survivors may not all match; the engine must reapply the predicate. **Unhandled** predicates the storage couldn't evaluate at all; every row must be presented to the engine for evaluation.

The discipline this classification enforces. **Conservative pruning is honest.** When the storage layer's pruning rule is approximate (a manifest's partition summary admits the predicate but the manifest may also contain non-matching rows), the storage returns the predicate in the `inexact` bucket; the engine knows to reapply. **The engine retains responsibility for correctness.** The engine never assumes the storage's pruning was exact; it consults the classification and reapplies as needed. The result is that pushdown is a pure optimization — the engine's answer is correct regardless of how much pushdown happened.

DataFusion's `TableProvider::supports_filters_pushdown` method returns this classification per predicate as `TableProviderFilterPushDown::Exact`, `Inexact`, or `Unsupported`. The naming is DataFusion-specific; the three-way structure is universal.

### Which Predicates Are Pushable

Not every predicate the engine sees can be pushed to the storage layer. The pushable subset is determined by what the storage knows how to evaluate. Module 3 Lesson 3 developed the leaf-predicate algebra for column statistics; pushdown is the same algebra applied at the engine-storage boundary.

**Pushable, often as exact**:
- `column op constant` where `op` is `=`, `<`, `<=`, `>`, `>=`, `!=` and the column is statistics-bearing.
- `column IN (constant_list)`.
- `column IS NULL`, `column IS NOT NULL`.
- Conjunctions and disjunctions of pushable predicates.

**Pushable, typically as inexact** (the storage pruning is approximate; engine reapplies):
- Predicates on partition columns under transforms that don't admit tight lifting (Module 3's `Bucket` + range case).
- Predicates on columns where statistics exist but are coarse (every file's `min` and `max` span the predicate's value range, so pruning happens at most at the manifest-list level).

**Not pushable**:
- Function calls on columns (`UPPER(name) = 'APOLLO-7'`). The storage's statistics are on the column's values, not on `UPPER(value)`; without an index over the function output, the storage can't prune.
- Predicates involving multiple tables (`a.mission_id = b.mission_id`). Pushdown is per-table; cross-table predicates are evaluated by the engine.
- Predicates on derived columns or aggregates (`SUM(voltage) > 1000`).
- Predicates with non-trivial logic the storage doesn't implement (regex, JSON path expressions, geospatial predicates without spatial index support).

The capstone's `TableProvider` classifies incoming predicates by walking the predicate tree, identifying the leaves, and matching them against the table format's capabilities. The Module 2-3 capstones support exact pushdown for `=`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`, and `IN` against statistics-bearing columns; inexact pushdown for any predicate that includes a bucket-partitioned column with a non-equality op; unsupported pushdown for everything else. The classification is per-leaf; the engine handles the conjunction/disjunction wiring.

### The Projection-Pushdown Companion

Predicate pushdown has a partner: **projection pushdown**. The engine tells the storage which columns it actually needs, and the storage reads only those columns. The mechanism is structural at the Parquet level — the column-store format reads columns independently, and skipping columns means skipping their bytes entirely. Module 1 Lesson 3 introduced this as the Parquet reader's `ProjectionMask`; this lesson is what makes it cross the engine-storage boundary.

Projection pushdown is unambiguously simpler than predicate pushdown: a list of column names, the storage selects them, the result has exactly those columns. There is no "inexact projection" — either the column is in the result or it isn't. DataFusion's `TableProvider::scan` takes a `projection: Option<Vec<usize>>` parameter that the engine sets to the indices of the columns the query actually uses; the storage uses these indices to construct the Parquet `ProjectionMask`.

The two pushdowns compose orthogonally. A query `SELECT mission_id, AVG(voltage) FROM t WHERE day = '2024-03-15'` produces a scan call with `projection = [mission_id, voltage]` (the columns used in the projection and the aggregate) and `predicates = [day = '2024-03-15']` (the WHERE clause). The storage prunes files by `day`, reads only `mission_id` and `voltage` from the surviving files, and returns the result. Without one of the pushdowns the query would either read every file (no predicate pushdown) or read every column from every file (no projection pushdown); with both, the read is minimal.

### The Residual Filter Pattern

When the storage classifies a predicate as `inexact`, the engine must reapply it after reading. The residual filter is exactly the predicate the engine pushed, evaluated against each row in the returned batches. The mechanics are direct: the engine wraps the storage's `RecordBatchStream` in a filter operator that evaluates the residual predicates and drops rows that don't match.

The pattern is straightforward in DataFusion:

```text
storage_stream → FilterExec(residual_predicates) → upstream_operators
```

The cost depends on how much the storage's pruning helped. A perfectly-pushable predicate (`day = '2024-03-15'`) sees the storage's pruning eliminate most files, and the residual filter is unnecessary — the engine knows the storage was exact. An inexact predicate (`payload_id > 'p-005'` on a bucket-partitioned column) sees the storage return many candidate files; the residual filter eliminates the non-matching rows.

The residual-filter pattern's elegance is that it never produces wrong results. The storage's job is to *narrow* the search; the engine's filter is what makes the result *exact*. Even if the storage returns nothing useful (every predicate is `Unsupported`), the engine still produces the right answer — just at the cost of a full table scan plus engine-side filtering. The pushdown contract is a pure optimization; correctness is unaffected by how aggressive the pushdown is.

### Pushdown of Logical Operators

Predicates with `AND` and `OR` compose under pushdown. The rules:

**Conjunction (`A AND B`)**:
- If both `A` and `B` are pushable exact, the conjunction is exact.
- If one is exact and one is inexact, the conjunction is inexact (the engine must reapply the inexact part).
- If one is unsupported, the conjunction is pushed only at the level where the unsupported branch is dropped — the engine pushes `A AND B` as `A` (the supported part) and applies the full `A AND B` as residual. This is the conservative-pruning principle at the conjunction level.

**Disjunction (`A OR B`)**:
- Disjunction pushes only if *every* branch is pushable. If any branch is unsupported, the disjunction is unsupported (the engine can't drop a branch from a disjunction; the dropped branch's potentially-matching rows would be lost).
- If every branch is exact, the disjunction is exact.
- If any branch is inexact, the disjunction is inexact.

The conjunction-vs-disjunction asymmetry is the key insight. A conjunction can drop branches the storage doesn't support (because adding `AND TRUE` doesn't change the result; the engine reapplies the dropped branch). A disjunction cannot drop branches (because `OR FALSE` removes matching rows; the engine couldn't reapply because the rows aren't in the result to filter). The pushdown logic encodes this directly.

The capstone's predicate-classification function applies these rules during the tree walk. The result is a per-tree classification that DataFusion consumes; DataFusion's `FilterExec` wraps the storage stream with the residual predicates the storage didn't handle exactly.

### Statistics Quality: The Hidden Variable

The pushdown's effectiveness depends on the *quality* of the storage's statistics. Three failure modes are worth knowing.

**Coarse statistics.** A file whose `payload_id` ranges from `'p-001'` to `'p-200'` cannot be pruned by a predicate `payload_id = 'p-042'`; the file *might* match. If every file in the table has the full range, no pruning happens at the file level. The clustering work from Module 3 Lesson 2 is what tightens statistics — clustered files have narrow ranges per cluster column, so predicates on cluster columns prune effectively.

**Missing statistics.** Module 1's writer can be configured to omit statistics for very wide columns (long strings, complex nested types) to save metadata space. Predicates on those columns push to the storage but the storage can't evaluate them; classification is `Unsupported`, and the engine handles the full predicate. The fix is to enable statistics on the column, paying the metadata cost; this is a workload-tuning decision.

**Stale statistics.** The statistics in a manifest entry are the writer's view at commit time; they don't update if the data file changes. In Iceberg, data files are immutable, so this doesn't happen — every commit produces new files with fresh statistics. In some other systems (Hudi's COW vs MOR distinction; Delta's old in-place-update protocol) statistics can become stale; the result is over-conservative pruning. The lakehouse formats covered in this track all use immutable data files, so stale statistics are not a failure mode.

The Artemis observability stack tracks **per-query pruning effectiveness** as a leading indicator of statistics quality. A query whose predicate is theoretically selective but produces low pruning ratios is a signal that the statistics are coarser than the predicate needs — typically because the table needs better clustering, or because the predicate column is missing from the table's clustering spec.

---

## Code Examples

### A Predicate-Classification Walk

The capstone's classifier: walk a predicate tree, return the three-way classification for each node, compose the results.

```rust,ignore
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;

#[derive(Debug, Clone, Copy)]
pub enum Pushdown {
    Exact,
    Inexact,
    Unsupported,
}

/// Classify a predicate against the table format's capabilities.
/// Returns the strongest level the storage can handle; the engine
/// reapplies whatever the storage didn't handle exactly.
pub fn classify(expr: &Expr, caps: &TableCapabilities) -> Pushdown {
    match expr {
        // Leaf: column op constant. The storage's leaf rule (Module 3 L3)
        // determines whether the result is Exact, Inexact, or Unsupported.
        Expr::BinaryExpr(b) => {
            classify_binary(b, caps)
        }
        // Conjunction: both branches must be at least supported.
        // Branches' classifications combine: Exact + Exact = Exact;
        // any Inexact among supported branches makes the result Inexact;
        // Unsupported branches are dropped (engine reapplies as residual).
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            let l = classify(&b.left, caps);
            let r = classify(&b.right, caps);
            match (l, r) {
                (Pushdown::Exact, Pushdown::Exact) => Pushdown::Exact,
                (Pushdown::Unsupported, x) | (x, Pushdown::Unsupported) => x,
                _ => Pushdown::Inexact,
            }
        }
        // Disjunction: every branch must be at least supported.
        // If any branch is Unsupported, the disjunction is Unsupported.
        Expr::BinaryExpr(b) if b.op == Operator::Or => {
            let l = classify(&b.left, caps);
            let r = classify(&b.right, caps);
            match (l, r) {
                (Pushdown::Unsupported, _) | (_, Pushdown::Unsupported) => Pushdown::Unsupported,
                (Pushdown::Exact, Pushdown::Exact) => Pushdown::Exact,
                _ => Pushdown::Inexact,
            }
        }
        // Negation: classified by the inner expression's classification.
        // Note: NOT over Inexact remains Inexact (the storage's pruning
        // doesn't necessarily map cleanly through negation).
        Expr::Not(inner) => classify(inner, caps),
        // Function calls on columns: typically unsupported at the storage
        // layer unless a function-specific index exists.
        Expr::ScalarFunction(_) => Pushdown::Unsupported,
        // Anything else: be conservative.
        _ => Pushdown::Unsupported,
    }
}

fn classify_binary(b: &BinaryExpr, caps: &TableCapabilities) -> Pushdown {
    // Identify the column reference; if neither side is a column,
    // the leaf is unsupported.
    let (col, op, _val) = match (&*b.left, &*b.right) {
        (Expr::Column(c), _) => (c, b.op, &b.right),
        (_, Expr::Column(c)) => (c, swap_op(b.op), &b.left),
        _ => return Pushdown::Unsupported,
    };
    if !caps.has_statistics(&col.name) {
        return Pushdown::Unsupported;
    }
    if caps.is_partition_column(&col.name) {
        // Partition column + transform: check whether the lifting rule
        // for this (transform, op) is tight, conservative, or unsupported.
        match caps.lifting_rule(&col.name, op) {
            Some(LiftingRule::Tight) => Pushdown::Exact,
            Some(LiftingRule::Conservative) => Pushdown::Inexact,
            None => Pushdown::Unsupported,
        }
    } else {
        // Non-partition column with statistics: the storage prunes files
        // via per-file stats. Pruning is exact for the predicate's effect
        // on file selection (the file is read or not), but residual rows
        // in the read file may not match. Classification depends on
        // whether the per-file stats are tight enough; we treat all
        // non-partition pushdowns as Inexact to be safe.
        Pushdown::Inexact
    }
}
```

The structure. The classifier walks the predicate tree and applies the composition rules. Leaves classify based on the storage's lifting and statistics support; conjunctions and disjunctions combine per the rules in the previous section. The result tells the engine exactly which predicates can be dropped from the residual filter and which must be reapplied.

### A `TableProvider` Skeleton

The DataFusion `TableProvider` shape, with the predicate-pushdown and projection-pushdown plumbing wired in:

```rust,ignore
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::error::Result;

pub struct ArtemisTableProvider {
    table: ArtemisTable,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ArtemisTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    /// DataFusion calls this for each predicate; the return value
    /// determines whether DataFusion includes the predicate in the
    /// scan call and whether it adds a residual FilterExec.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let caps = self.table.capabilities();
        Ok(filters
            .iter()
            .map(|e| match classify(e, &caps) {
                Pushdown::Exact => TableProviderFilterPushDown::Exact,
                Pushdown::Inexact => TableProviderFilterPushDown::Inexact,
                Pushdown::Unsupported => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    /// DataFusion calls this to build the physical scan. The
    /// `projection` and `filters` arguments are the ones DataFusion
    /// decided to push down based on supports_filters_pushdown.
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Translate DataFusion's Expr predicates to the table format's
        // Predicate type, plan the scan against the table format, and
        // wrap the result in a DataFusion ExecutionPlan that streams
        // record batches.
        let table_predicate = exprs_to_predicate(filters)?;
        let plan = self.table.plan_scan(&table_predicate).await?;
        let exec_plan = ArtemisExec::new(plan, projection.cloned(), self.schema.clone());
        Ok(Arc::new(exec_plan))
    }
}
```

What to notice. The `TableProvider` is two methods: classification (returns the per-predicate pushdown level) and scan (returns the physical execution plan). DataFusion handles the rest — the SQL parser, the logical planner, the optimizer, the projection-and-filter wiring on top of the scan, the parallel execution of the scan stream. The `TableProvider` is the integration shim; DataFusion is the engine.

The `ArtemisExec` is the physical plan that produces the actual `RecordBatchStream`. Module 5 Lesson 2 develops the streaming side; the relevant DataFusion trait is `ExecutionPlan` with its `execute` method returning a `SendableRecordBatchStream`.

---

## Key Takeaways

- **Predicate pushdown is a three-way contract**: predicates are pushed exact, pushed inexact, or unsupported. The storage handles exact predicates completely; the engine reapplies inexact predicates as a residual filter; the engine handles unsupported predicates entirely.
- **The classification per predicate** depends on the storage's capabilities (which columns have statistics, which partition transforms admit tight lifting). The composition rules for `AND` (drop unsupported branches) and `OR` (any unsupported branch makes the whole disjunction unsupported) follow from the false-positive/false-negative asymmetry.
- **Projection pushdown is the unambiguous partner.** The engine tells the storage which columns it actually needs; the storage reads only those columns. Composition with predicate pushdown is orthogonal; both pushdowns together minimize the read.
- **The residual filter pattern** is what makes pushdown a pure optimization. The engine never assumes the storage's pruning was exact; the engine reapplies inexact predicates after reading. Correctness is preserved regardless of how much pushdown happens.
- **Statistics quality determines pushdown effectiveness.** Clustered data, partition-column predicates, and tight per-file ranges produce strong pruning; unclustered data, missing statistics, and inexact transforms produce weak pruning. Pushdown effectiveness is monitored as a leading indicator of statistics-quality drift.

{{#quiz quiz-01.toml}}
