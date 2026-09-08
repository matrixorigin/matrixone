# Materialized views: implemented contract (D2)

Status: implementation contract for this PR. The persisted definition is format
1 and requires protocol 57. This document describes the implemented aggregate
and UNION ALL variants; it does not specify a future operator-graph format.

## SQL and refresh policy

`CREATE MATERIALIZED VIEW ... REFRESH FAST|FORCE|COMPLETE ON CHANGE AS SELECT ...`
creates a physical result maintained by ISCP. `INCREMENTAL` aliases FAST and
`FULL` aliases COMPLETE. The default is FORCE ON CHANGE. COMPLETE ON DEMAND
creates a result refreshed explicitly by `REFRESH MATERIALIZED VIEW` in the
caller's transaction. FAST and FORCE ON DEMAND are rejected.

FAST supports the planner's deterministic direct-source grouped aggregates and
compatible UNION ALL branches. The supported state includes COUNT, SUM, AVG,
MIN/MAX, DISTINCT aggregates and HAVING. Nullable groups and inputs follow SQL
semantics. Unsupported expressions, set operations or incompatible branch state
are rejected for FAST. FORCE selects COMPLETE when the definition is outside the
incremental subset. COMPLETE supports up to 16 direct base sources, including
joins. Temporary, external and view sources are rejected.

A UNION ALL branch has an explicit branch identity even when two branches read
the same table. Sources are deduplicated for scheduling, while each matching
branch consumes a source change. Output aliases and hidden state layouts are
aligned before admitting incremental maintenance.

## One authoritative definition

`pkg/catalog/mvdefinition` owns the shared codec and identity checks. A target's
catalog-owned `mv_definition` envelope contains:

- Format and capability, tenant, target database/table IDs and names, generation.
- Canonical CREATE and refresh SQL, refresh method/timing and visible columns.
- Deduplicated source database/table IDs, names and present schema versions.
- Optional incremental description and exact auxiliary relation identity.

Generation 1 is immutable. DROP/CREATE allocates a different target ID. There is
no in-place definition alteration or inferred migration by table name. Direct
incremental descriptions use version 2; UNION ALL uses version 3. Unknown,
incomplete and oversized definitions are rejected before execution.

The ISCP job stores only an MV reference (target ID, generation and SHA256 digest)
and the derived scheduling sources. It does not duplicate refresh SQL or state
layout. The consumer loads the authoritative definition in its refresh
transaction and verifies the reference, tenant, sources and auxiliary owner.

Read planning and every cached-plan execution validate source identity and schema
version. DROP/recreate, TRUNCATE, rename and schema-version-changing ALTER
invalidate the dependent generation. Reads and refreshes then fail with a
recreation diagnostic. DROP MV remains available to retire the invalid object.

Public writes and ALTER/rename/TRUNCATE on a result or its auxiliary state are
rejected. An ordinary comment has no ownership meaning. Public CREATE TABLE
rejects reserved `mv_*` properties and `__mo_mv_state_` names. Internal refresh
uses a private context capability containing exactly the tenant, target ID,
generation and optional state ID. A general internal-executor flag grants no
write authority.

## Creation, refresh and cleanup

CREATE resolves and locks sources, creates the target and optional auxiliary
relation in the same transaction, then persists their final IDs. The auxiliary
owner records the tenant, target ID/generation and state ID. Creation publishes
an ON CHANGE job only after these identities are complete. Consumers never run
`CREATE TABLE IF NOT EXISTS` to recover state by name.

Initial snapshot replacement publishes target and auxiliary data atomically.
Initial watermark finalization can follow this transaction; a crash in that gap
repeats replacement. Tail refresh publishes result, state and watermark CAS in
one transaction. Every generated delta DML advances the statement boundary so
later statements observe preceding workspace writes.

A refresh holds shared locks on the same database/relation catalog keys used by
DDL, in a stable order. Generation CAS fences stale workers. A recoverable delta
error may enter FORCE replacement only after its transaction rolled back
successfully. Cancellation, deadline, invalid identity/format, resource limits,
CAS loss, failed rollback and uncertain commit outcomes do not permit fallback.

Deleted-row values are read immediately before each tombstone commit, rather
than at the beginning of the whole iteration. This handles inserts and repeated
updates within one interval. Historical lookup owns copied variable-width data,
uses historical object ranges, and excludes tombstones committed after the
requested snapshot. Missing historical data is an error, never a partial delta.

DROP retires all active job generations by target ID before deleting owned
relations. Source rename cannot rewrite the target name. In-memory tombstone GC
compares timestamps in the same units. Malformed job payloads are quarantined
individually so unrelated log replay can continue.

## Resource and cancellation bounds

- At most 16 direct sources/branches; catalog decoding is limited to 1 MiB.
- Incremental rows are decoded in chunks before copying, with at most 32,768
  rows and 8 MiB of accounted scratch per chunk. Historical snapshot groups
  share one lookup budget. A single oversized value fails before copying.
- Generated SQL has an 8 MiB bound. Chunks are released before advancing the
  borrowed batch. Iteration and row scans check cancellation; iterators, batches,
  SQL results and transaction cleanup retain their existing effective owners.
- Async refresh inherits the finite iteration deadline; manual refresh has a
  one-hour timeout. Worker fanout and shared-stream references use ISCP's bounded
  admission, cancellation and drain lifecycle.
- Persistent DISTINCT multiplicities and affected-group state use database
  storage quotas. Zero multiplicities and affected markers are removed. There
  is no arbitrary global row-count limit or full state COUNT scan on each delta.

## Upgrade and rollback

All creation, including ON DEMAND, requires protocol 57. Admission is a rollout
check; safety also relies on representations understood by predecessor readers.
On disk, the job uses the existing IndexSync discriminator with an empty index
selector and an additive MVReference. A predecessor's writer registry rejects
that selector before constructing a consumer or executing SQL. New decoding
validates the reference and restores the in-memory MV consumer discriminator.

Targets and auxiliary relations retain physical view kind and a MATERIALIZED
CREATE syntax barrier. Current catalog readers project authenticated metadata as
physical target/state relations. Predecessors retain the view definition and
reject its syntax for reads and its view kind for DML. This also covers ON DEMAND
objects, which have no background job.

Mixed-version MV availability is unsupported. Use capable binaries for reads,
refresh and cleanup, and drop materialized views before a binary rollback.
Pre-envelope metadata from earlier revisions of this unreleased PR requires
recreation. Component compatibility tests exercise the actual predecessor's
job decode/consumer construction and syntax parser; they do not claim a supported
whole-cluster downgrade or an automatic migration of those earlier revisions.

## Validation and performance gates

Focused tests cover codec/version/tenant/generation boundaries, exact authority,
job replay isolation, historical row ownership and snapshot deletion visibility,
chunk limits/cancellation, transaction routing and cleanup timing. SQL BVT
compares physical results with independent source aggregates through snapshot,
append, delete, update, flush, UNION ALL and full replacement. Identity tests
cover ordinary comments, reserved properties, prepared reads, source DDL, manual
transaction rollback and rename/drop cleanup. Restart validates persisted target
and job references. Shared lifecycle changes require targeted race validation.

Reference-host acceptance requires a source control sustaining 6,000 rows/s,
6,000 rows/s steady ingestion with one algebraic MV, p99 freshness at most 5 s,
maximum freshness at most 10 s, source throughput reduction at most 20%, and burst
drain at most twice the burst duration. Logical auxiliary state must stop growing
when the live groups/values stop growing. Record measured data and host/config
with the PR; these are acceptance gates, not guarantees derived from unit tests.
