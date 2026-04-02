# Lesson 1 — The Volcano (Iterator) Model

**Module:** Database Internals — M06: Query Processing  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 14

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's volcano model description and pipelining analysis against Chapter 14.

---
<!-- toc -->
---
## Context

The B+ tree range scan iterator from Module 2 and the LSM merge iterator from Module 3 are both instances of a general pattern: **pull-based iteration**. A consumer calls `next()`, the producer returns the next item or signals completion. The volcano model (also called the iterator model, introduced by Goetz Graefe in 1990) generalizes this into a complete query execution framework.

Every query operator — scan, filter, project, join, sort, aggregate — implements the same interface: `open()`, `next()`, `close()`. Operators compose by nesting: a filter's `next()` calls its child scan's `next()` and applies the predicate. A join's `next()` calls both children's `next()` methods to find matching rows. The entire query plan is a tree of iterators, and execution proceeds one row at a time from the root.

This model is simple, composable, and universal — it's used by PostgreSQL, MySQL, SQLite, and most traditional database engines.

---

## Core Concepts

### The Operator Interface

Every query operator implements:

```rust,ignore
trait Operator {
    /// Initialize the operator (open files, allocate buffers).
    fn open(&mut self) -> io::Result<()>;

    /// Return the next row, or None if exhausted.
    fn next(&mut self) -> io::Result<Option<Row>>;

    /// Release resources.
    fn close(&mut self) -> io::Result<()>;
}
```

`Row` is a tuple of typed values — for the OOR, it's a TLE record or a subset of its fields.

### Operator Composition

A query plan is a tree of operators. The root operator's `next()` pulls data through the entire tree:

```text
   ProjectOperator (select norad_id, epoch, mean_motion)
        │
   FilterOperator (where inclination > 80.0)
        │
   SeqScanOperator (scan TLE table)
```

Execution: `Project.next()` calls `Filter.next()`, which calls `SeqScan.next()` repeatedly until finding a row with `inclination > 80.0`, then returns it to `Project`, which extracts the requested columns.

### Pipelining

In the volcano model, rows **pipeline** through operators: each row flows from leaf to root without being materialized in an intermediate buffer. `SeqScan` produces row 1, `Filter` checks it and passes it to `Project`, which emits it. Then row 2, and so on.

Pipelining is memory-efficient — only one row is in flight at a time (per operator). But some operators break the pipeline:

- **Sort:** Must consume all input rows before producing any output (can't emit the smallest row until it has seen all rows).
- **Hash join (build phase):** Must consume the entire build side before probing with the probe side.
- **Aggregate:** Must consume all input to compute aggregates (e.g., COUNT, AVG).

These are **pipeline breakers** (also called **blocking operators**). They require materializing intermediate results, consuming memory proportional to the input size.

### Volcano Model Limitations

**CPU overhead:** Each `next()` call involves a virtual method dispatch (or trait object call in Rust), a function call per operator per row. For 100,000 rows through 5 operators, that's 500,000 function calls. On modern CPUs, this overhead is small for I/O-bound queries but significant for CPU-bound analytical queries.

**Cache inefficiency:** Processing one row at a time means the CPU repeatedly jumps between different operator code paths. The instruction cache is constantly invalidated. Vectorized execution (Lesson 2) addresses this by processing batches.

**No SIMD opportunity:** Single-row processing cannot exploit SIMD instructions that process multiple values simultaneously.

For the OOR's I/O-bound conjunction queries (dominated by SSTable reads), the volcano model's CPU overhead is acceptable. For the CPU-bound catalog merge (5 sources × 100k objects × comparison logic), vectorized execution provides a significant speedup.

---

## Code Examples

### Operator Trait and Basic Operators

```rust,ignore
use std::io;

/// A row in the OOR catalog: simplified for the query layer.
#[derive(Debug, Clone)]
struct TleRow {
    norad_id: u32,
    epoch: f64,
    inclination: f64,
    mean_motion: f64,
    source: String,
}

/// Pull-based query operator.
trait Operator {
    fn open(&mut self) -> io::Result<()>;
    fn next(&mut self) -> io::Result<Option<TleRow>>;
    fn close(&mut self) -> io::Result<()>;
}

/// Sequential scan over an in-memory vector of TLE records.
struct SeqScan {
    data: Vec<TleRow>,
    cursor: usize,
}

impl Operator for SeqScan {
    fn open(&mut self) -> io::Result<()> {
        self.cursor = 0;
        Ok(())
    }
    fn next(&mut self) -> io::Result<Option<TleRow>> {
        if self.cursor < self.data.len() {
            let row = self.data[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
    fn close(&mut self) -> io::Result<()> { Ok(()) }
}

/// Filter operator: passes through rows that match a predicate.
struct Filter {
    child: Box<dyn Operator>,
    predicate: Box<dyn Fn(&TleRow) -> bool>,
}

impl Operator for Filter {
    fn open(&mut self) -> io::Result<()> { self.child.open() }
    fn next(&mut self) -> io::Result<Option<TleRow>> {
        loop {
            match self.child.next()? {
                Some(row) if (self.predicate)(&row) => return Ok(Some(row)),
                Some(_) => continue,  // Row doesn't match — pull next
                None => return Ok(None),
            }
        }
    }
    fn close(&mut self) -> io::Result<()> { self.child.close() }
}

/// Projection operator: transforms rows.
struct Projection {
    child: Box<dyn Operator>,
    project_fn: Box<dyn Fn(TleRow) -> TleRow>,
}

impl Operator for Projection {
    fn open(&mut self) -> io::Result<()> { self.child.open() }
    fn next(&mut self) -> io::Result<Option<TleRow>> {
        match self.child.next()? {
            Some(row) => Ok(Some((self.project_fn)(row))),
            None => Ok(None),
        }
    }
    fn close(&mut self) -> io::Result<()> { self.child.close() }
}
```

### Composing a Query Plan

```rust,ignore
fn build_high_inclination_query(tle_data: Vec<TleRow>) -> Box<dyn Operator> {
    let scan = Box::new(SeqScan { data: tle_data, cursor: 0 });

    let filter = Box::new(Filter {
        child: scan,
        predicate: Box::new(|row: &TleRow| row.inclination > 80.0),
    });

    let project = Box::new(Projection {
        child: filter,
        project_fn: Box::new(|mut row: TleRow| {
            // Strip fields we don't need downstream
            row.source = String::new();
            row
        }),
    });

    project
}

fn execute_query(mut plan: Box<dyn Operator>) -> io::Result<Vec<TleRow>> {
    plan.open()?;
    let mut results = Vec::new();
    while let Some(row) = plan.next()? {
        results.push(row);
    }
    plan.close()?;
    Ok(results)
}
```

The query plan is built bottom-up (scan → filter → project) and executed top-down (project pulls from filter pulls from scan). This separation between plan construction and execution is what makes the volcano model so composable — you can swap operators, add layers, or optimize the plan without changing the execution engine.

---

## Key Takeaways

- The volcano model defines a universal operator interface: `open()`, `next()`, `close()`. Every query operator — scan, filter, join, sort — implements this interface and composes with any other operator.
- Pipelining passes rows through operators one at a time without intermediate materialization. Pipeline breakers (sort, hash build, aggregate) must buffer all input before producing output.
- The model's simplicity is its strength: it's easy to implement, test, and extend. Its weakness is per-row overhead (function calls, cache misses) that hurts CPU-bound analytical queries.
- The B+ tree range scan (Module 2) and LSM merge iterator (Module 3) are both volcano-model operators. The query layer from this module composes on top of them.

---

{{#quiz lesson-01-quiz.toml}}
