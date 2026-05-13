# Lesson 3 — Catalog Architecture

**Module:** Data Lakes — M05: Query Engine Integration
**Position:** Lesson 3 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 9 ("Replication and Consistency") for the consistency framing. Apache Iceberg REST catalog specification. Project Nessie documentation. Hive Metastore protocol documentation.

> **Source note:** Catalog implementations evolve rapidly; the four backends compared here are the well-known ones as of the curriculum's writing. Specific feature matrices should be verified against current vendor documentation.

---

## Context

Module 2 introduced the catalog as the source of the per-table snapshot pointer. The catalog's job at the transaction layer is simple: linearizable compare-and-swap on a single piece of state per table. But the catalog has another job at the query-engine layer: **table discovery**. A query engine connecting to a lakehouse needs to know which tables exist, what their current snapshots are, and how to read their metadata. The catalog is the discovery layer.

The lakehouse community has not converged on a single catalog implementation; the spec layer is settled (catalogs implement a small CAS-and-list API) but the implementations vary widely in their operational properties. The four common backends — **REST catalog**, **Hive Metastore**, **Project Nessie**, **Postgres-JDBC** — have substantially different operational behaviors at scale, and the choice has long-running consequences for how the lakehouse runs.

This lesson develops the catalog's role from the query engine's perspective, then compares the four backends on the dimensions that matter operationally: consistency, branching, multi-region behavior, operational complexity, and the API surface the query engine sees. The capstone uses the Postgres catalog from Module 2 — appropriate for the Artemis archive's single-region, single-writer deployment — but the engineer should be able to defend the choice against the alternatives.

---

## Core Concepts

### The Catalog's Two Jobs

The catalog answers two questions for the query engine. **Table discovery**: which tables exist, and what are their current snapshots? **Snapshot retrieval**: given a table identity, what is its current snapshot's metadata path? The first is a `list_tables(namespace)` call; the second is `get_current_snapshot(table)`. Both are read-only; the catalog's CAS responsibility is on the writer side and not directly exposed to query engines.

The discovery API is small. Iceberg's catalog spec defines:

- `list_namespaces() -> Vec<Namespace>`
- `list_tables(namespace) -> Vec<TableIdentifier>`
- `load_table(table_id) -> TableMetadata`
- `register_table`, `create_table`, `update_table`, `drop_table` — the writer-side mutation operations

The query engine uses the first three. `list_namespaces` for hierarchical organization (a namespace per mission, per team, per project); `list_tables` for the tables-in-namespace browse; `load_table` for the actual table-metadata read that drives the read planning.

The Iceberg REST catalog API standardizes this surface as HTTP endpoints. The other backends implement the same logical API through their native protocols (Hive Metastore over Thrift, Nessie over a Git-flavored REST API, JDBC over SQL). The query engine code that consumes the catalog is identical across backends; the transport differs.

### REST Catalog: The HTTP-Native Choice

The Iceberg REST catalog is an HTTP API specification — a small set of REST endpoints that an implementation exposes. The spec is open; multiple implementations exist (Tabular, Polaris from Snowflake, Apache Polaris, in-house implementations). The properties:

- **Stateless API.** The REST endpoints are stateless; clients can connect to any instance of the catalog service. Horizontal scaling is straightforward.
- **HTTP/2 with token auth.** Authentication is standard OAuth2 or token-based; client libraries (PyIceberg, iceberg-rust) handle the auth handshake transparently.
- **Multi-tenant by design.** Namespaces are first-class; per-namespace access control is the standard pattern.
- **Operational shape: a stateless service.** The service itself is stateless; the backing store (typically a SQL database) holds the per-table state. Failover is conventional service-failover; consistency depends on the backing store.

The REST catalog's main advantage is its **client-side simplicity**. Any HTTP client can talk to it; the network protocol is standard; debugging is straightforward (a browser can hit the endpoints). For deployments that need to expose the lakehouse to multiple clients in multiple languages, REST is the lowest-friction choice.

The disadvantage is that the REST service is an extra deployment unit. The lakehouse needs the catalog database *plus* the REST service *plus* the auth provider. For small deployments this overhead is significant; for large deployments the REST service is a benefit (it centralizes auth and access control).

### Hive Metastore: The Legacy-Compat Choice

The Hive Metastore is the catalog that emerged from the Hadoop era. It is a Java service backed by a SQL database, exposing a Thrift API. Iceberg supports it as a catalog backend for compatibility with existing Hadoop-shaped deployments.

The relevant properties:

- **Thrift API.** Older than REST; well-supported in Java/Scala ecosystems; less convenient outside them.
- **Wire-level compatibility with Hive tooling.** Catalog metadata for Iceberg tables is stored in Hive's existing tables, sharing the database with Hive's own table metadata. A deployment migrating from Hive to Iceberg can use the same Metastore; client tooling that was already pointing at Hive Metastore continues to work.
- **Single-process service.** The Metastore runs as one Java service; horizontal scaling is via load balancing in front of multiple instances against a shared database.
- **No multi-version branching.** Hive Metastore's model is a single linear history per table; the branching support that Nessie provides (below) is not available.

Hive Metastore is the right choice in exactly one scenario: a deployment that already runs a Hive Metastore and is incrementally migrating to Iceberg. New deployments rarely choose it; the operational overhead of running a JVM service plus the SQL database is significant relative to the simpler alternatives, and the Hive-specific tooling integrations are useful only when Hive itself is also in the picture.

### Nessie: The Git-for-Data Choice

Project Nessie is the most architecturally distinctive option. Nessie adds **Git-like branching and merging** semantics to the catalog: tables can have branches (`main`, `experiment`, `recovery-2024-03`), commits to branches, merges between branches, and tags pinning specific snapshots.

The properties:

- **Branching as a first-class concept.** A query engine can read from `main`, from a branch, or from a tag. The same table identity exists under multiple branches with potentially different states.
- **Merge semantics across multiple tables.** A commit to Nessie can update multiple tables atomically; the commit is a multi-table transaction in a way the other catalog backends don't support. This is the catalog's analog of a multi-statement transaction in a row-store database.
- **Audit-trail by construction.** Every commit has an author, a message, and a timestamp; the history is a directed acyclic graph in the Git sense, navigable like any Git repository.
- **REST-style HTTP API.** The protocol is similar to Iceberg's REST catalog; multi-tenant; horizontally scalable.

Nessie is the right choice for workflows that need branching. The two patterns the Artemis team evaluated:

- **Experimental work on production data.** An analyst wants to run a transformation that modifies a table; they branch from `main`, commit the transformation, validate the result, and merge back. The branch is isolated from `main` during the work; other analysts continue to see the unmodified `main`.
- **Multi-table atomic schema migration.** A migration that needs to update three related tables consistently can commit all three changes in one Nessie commit; readers see either the pre-migration state or the post-migration state, never an in-progress mix.

The disadvantage is operational complexity. Nessie is a separate service with its own backing store; it has its own merge-conflict semantics that the writer code must handle; the Git-flavored mental model is unfamiliar to teams used to traditional databases. The Artemis team chose not to use Nessie for the cold archive specifically because the workload (single-writer ingest, append-only data) has no need for branching; for a more dynamic workload with multiple writers experimenting on shared data, Nessie would have been the right choice.

### Postgres-JDBC: The Pragmatic Choice

The simplest catalog backend is a JDBC connection to a transactional database — typically Postgres, occasionally MySQL or SQLite. The catalog state is a small set of tables; the CAS is a row-level update; the discovery API is straightforward SQL.

The properties:

- **No new infrastructure.** A team that already runs Postgres for application data adds the catalog tables to it. No new service to deploy, no new auth scheme, no new monitoring.
- **Transactional semantics inherited from Postgres.** The CAS is a `UPDATE ... WHERE` against a single row; multi-row updates are SQL transactions. The catalog's consistency guarantees are exactly Postgres's, which are well-understood.
- **No native branching.** Like Hive Metastore, the JDBC catalog has a linear per-table history. Branching is achievable via convention (one table per branch) but not as a first-class concept.
- **Single-region typically.** A multi-region deployment requires replicating Postgres across regions, which is operationally non-trivial and changes the consistency model. Most JDBC-backed lakehouses are single-region.

The Artemis cold archive uses Postgres-JDBC because the operational profile matches: single-region, single-writer per table, an existing Postgres instance available on the ground-segment infrastructure. The catalog's overhead is essentially zero — adding a few tables to an existing database. For a small or medium deployment with no need for branching or multi-region, this is the obvious choice.

The disadvantage is scale. A deployment with thousands of tables across hundreds of teams typically outgrows a single Postgres instance; the JDBC catalog doesn't scale horizontally without sharding the catalog tables, which adds operational complexity. The REST catalog (which can run multiple instances against the same database, or against a horizontally-scalable database like CockroachDB) handles this case better.

### Comparison Matrix

A summary of the four backends on the dimensions that matter operationally:

| Dimension | REST | Hive Metastore | Nessie | Postgres-JDBC |
|---|---|---|---|---|
| **Setup overhead** | Medium (service + DB + auth) | High (JVM service + DB + auth) | Medium-high (service + DB) | Low (just DB) |
| **Horizontal scaling** | Yes (stateless service) | Yes (load balancer) | Yes (stateless service) | Limited (single DB) |
| **Branching/merging** | No | No | First-class | No (convention only) |
| **Multi-table txn** | No | No | Yes | Via SQL transaction |
| **Multi-region** | Yes with replicated DB | Yes with replicated DB | Yes | Limited |
| **Client ergonomics** | Excellent (HTTP) | Mediocre (Thrift) | Good (REST) | Excellent (SQL) |
| **Audit trail** | Per backend | Per backend | First-class | Via SQL history |
| **Best for** | Multi-tenant prod | Hadoop migration | Branching workloads | Small/medium single-region |

The "Best for" row is the decision shortcut. The Artemis archive fits "small/medium single-region" cleanly; Postgres-JDBC was the obvious choice. A growing-multi-team deployment with multiple language clients would land on REST. A team with active experimentation on shared production data would land on Nessie. A team migrating from Hadoop would inherit Hive Metastore.

### Consistency Across Backends

DDIA (Ch. 9) develops the consistency framing that applies here. The catalog's CAS must be **linearizable** for the table format's atomicity guarantees to hold — concurrent writers cannot both successfully advance the same pointer. The four backends provide this differently:

- **Postgres-JDBC**: linearizability via Postgres's per-row locking under the default `READ COMMITTED` isolation. The CAS is one `UPDATE` against one row; Postgres serializes concurrent updates against the same row.
- **Hive Metastore**: linearizability via the underlying SQL database (same as JDBC). The Thrift layer adds no consistency mechanism of its own.
- **REST catalog**: depends on the backing store. Most REST catalogs use a SQL database backend and inherit its linearizability; cloud-native variants on DynamoDB inherit DynamoDB's strong-consistency option (must be explicitly enabled).
- **Nessie**: linearizability via its own commit-log mechanism (single-writer at the global commit-log head; commits are serialized through it).

The dimension where the backends diverge is **stale-read behavior**. Postgres replicas can lag behind the primary by up to the replication latency; a read from a replica may see a stale snapshot pointer. Multi-region Nessie has explicit propagation latency; cross-region reads may see commits the local region has not yet seen. The lakehouse format itself does not handle these stale-read cases; clients must be aware of them and pin snapshots explicitly when consistency-across-reads matters (Module 4 Lesson 1's `--pin-snapshot` flag).

---

## Code Examples

### A `Catalog` Trait

The integration abstraction across backends: a single trait the query engine consumes, with one impl per backend.

```rust,ignore
use async_trait::async_trait;
use anyhow::Result;
use std::collections::HashMap;

#[async_trait]
pub trait Catalog: Send + Sync {
    /// Discovery: list namespaces (typically hierarchical, e.g.
    /// ["artemis", "sda", "observations"]).
    async fn list_namespaces(&self) -> Result<Vec<Vec<String>>>;

    /// List tables in a namespace.
    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<TableIdentifier>>;

    /// Load a table's metadata: the current snapshot, the schema
    /// history, the partition specs.
    async fn load_table(&self, id: &TableIdentifier) -> Result<TableMetadata>;

    /// Writer-side: CAS the table's current snapshot.
    async fn compare_and_swap(
        &self,
        id: &TableIdentifier,
        expected: SnapshotId,
        new: SnapshotId,
        new_metadata_path: &str,
    ) -> Result<(), CommitError>;

    /// Writer-side: create a new table.
    async fn create_table(
        &self,
        id: &TableIdentifier,
        initial_metadata: &TableMetadata,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}
```

The discipline. The trait is small. The query engine consumes it through `list_namespaces`, `list_tables`, `load_table`; the writer code uses it through `compare_and_swap` and `create_table`. Each backend implements the trait once; swapping backends is a single dependency-injection point.

### A Postgres-JDBC Implementation Sketch

The Artemis archive's catalog, distilled to the trait:

```rust,ignore
use sqlx::PgPool;

pub struct PostgresCatalog {
    pool: PgPool,
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn list_namespaces(&self) -> Result<Vec<Vec<String>>> {
        let rows = sqlx::query!(
            "SELECT DISTINCT namespace_path FROM iceberg_tables ORDER BY namespace_path"
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter()
            .map(|r| r.namespace_path.split('.').map(String::from).collect())
            .collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<TableIdentifier>> {
        let ns_path = namespace.join(".");
        let rows = sqlx::query!(
            "SELECT table_name FROM iceberg_tables WHERE namespace_path = $1",
            ns_path,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter()
            .map(|r| TableIdentifier {
                namespace: namespace.to_vec(),
                name: r.table_name,
            })
            .collect())
    }

    async fn load_table(&self, id: &TableIdentifier) -> Result<TableMetadata> {
        let ns_path = id.namespace.join(".");
        let row = sqlx::query!(
            "SELECT metadata_path FROM iceberg_tables
             WHERE namespace_path = $1 AND table_name = $2",
            ns_path,
            id.name,
        )
        .fetch_one(&self.pool)
        .await?;
        read_metadata_file(&row.metadata_path).await
    }

    async fn compare_and_swap(
        &self,
        id: &TableIdentifier,
        expected: SnapshotId,
        new: SnapshotId,
        new_metadata_path: &str,
    ) -> Result<(), CommitError> {
        let ns_path = id.namespace.join(".");
        let result = sqlx::query!(
            "UPDATE iceberg_tables
             SET current_snapshot_id = $1, metadata_path = $2
             WHERE namespace_path = $3 AND table_name = $4 AND current_snapshot_id = $5",
            new as i64, new_metadata_path, ns_path, id.name, expected as i64,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CommitError::Database(e.into()))?;

        if result.rows_affected() == 1 {
            Ok(())
        } else {
            // Read current state to give the caller a useful error.
            let row = sqlx::query!(
                "SELECT current_snapshot_id FROM iceberg_tables
                 WHERE namespace_path = $1 AND table_name = $2",
                ns_path, id.name,
            )
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CommitError::Database(e.into()))?;
            Err(CommitError::Conflict(CommitConflict {
                expected_old: expected,
                actual_current: row.current_snapshot_id as u64,
            }))
        }
    }
    // create_table omitted for brevity; standard INSERT.
}
```

The mechanics. The CAS is one SQL `UPDATE`; the discovery operations are straightforward selects. The Postgres row-locking provides linearizability for the CAS at no extra cost. The implementation is small because Postgres is doing most of the work; the catalog logic is mostly translation between the trait and the database.

### A REST Catalog Client Sketch

The REST catalog client side, showing how the same trait swaps backends:

```rust,ignore
pub struct RestCatalog {
    base_url: String,
    client: reqwest::Client,
}

#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(&self) -> Result<Vec<Vec<String>>> {
        let resp: NamespaceList = self.client
            .get(format!("{}/v1/namespaces", self.base_url))
            .send().await?
            .json().await?;
        Ok(resp.namespaces)
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<TableIdentifier>> {
        let ns_path = namespace.join(".");
        let resp: TableList = self.client
            .get(format!("{}/v1/namespaces/{}/tables", self.base_url, ns_path))
            .send().await?
            .json().await?;
        Ok(resp.identifiers)
    }

    async fn load_table(&self, id: &TableIdentifier) -> Result<TableMetadata> {
        let ns_path = id.namespace.join(".");
        let resp: LoadTableResult = self.client
            .get(format!("{}/v1/namespaces/{}/tables/{}", self.base_url, ns_path, id.name))
            .send().await?
            .json().await?;
        Ok(resp.metadata)
    }
    // CAS and create_table omitted; both are POSTs to the equivalent endpoints.
}
```

The contrast. The same trait, the same operations, different protocols underneath. The query engine code that consumes the trait is identical; choosing the backend is a startup-time decision. This is the architectural property that makes the catalog choice operational rather than structural — the lakehouse's correctness does not depend on which backend is in use.

---

## Key Takeaways

- **The catalog has two jobs**: discovery (list namespaces, list tables, load metadata) for query engines, and CAS for writers. The discovery API is small and well-defined; backends differ in implementation, not in interface.
- **Four common backends** with substantially different operational properties: REST (HTTP-native, multi-tenant, horizontally scalable), Hive Metastore (legacy-Hadoop-compat), Nessie (Git-like branching), Postgres-JDBC (minimal overhead, single-region).
- **The choice is operational, not architectural.** The catalog interface is consistent across backends; swapping backends is a dependency-injection decision; lakehouse correctness is unaffected by the choice.
- **Linearizability of the CAS** is the central correctness requirement across all backends. Each backend provides it differently (per-row locking for SQL, commit-log for Nessie); the lakehouse format inherits whatever the chosen backend offers.
- **Branching is Nessie's exclusive territory.** Workloads that need experimental branches off production data, or multi-table atomic transactions, justify the Nessie operational overhead. Single-writer append-only workloads don't.

{{#quiz quiz-03.toml}}
