# RENAME INDEX through COPY ALTER

Owner: issue #23392. Status: implemented and locally validated.
Reviewed base: bc523def3b55de2b305c14ee9fafb61dfec24126.
Independent design approval: GPT-6 Astra, medium, session
01a0bfe5-b28a-7943-9f28-18f37b5b6bf0 (DESIGN PASS).

## Contract and cost

Support `ALTER TABLE t RENAME INDEX old TO new` and the `RENAME KEY` alias.
This initial implementation uses COPY: O(table size) row copying plus index
construction and temporary replacement storage. Existing COPY locking and
async-index readiness apply. Physical IDs, caches, background generations and
maintenance timestamps can change; rows, uniqueness, index configuration,
visibility, comments, included columns and foreign-key bindings must survive.

One or more rename clauses may be combined with ALGORITHM/LOCK hints. Other
ALTER combinations are rejected before execution. INPLACE/INSTANT and LOCK=NONE
are unsupported. Sources and destinations use existing index-name normalization;
missing sources, duplicate destinations, invalid identifiers and PRIMARY renames
are rejected. Sequential renames and name swaps retain original index lineage.

## Alternatives and ownership

The status quo rejects the syntax. In-place renaming needs coordinated ISCP
cursor/generation migration, pending InitSQL transformation, cron ABA protection
and foreign-key migration. COPY reuses the existing transactional reconstruction,
plugin initialization and source-retirement owners, avoiding a new distributed
protocol. A later optimization may add INPLACE without changing SQL semantics.

The planner updates a deep copy of all physical definitions in each logical
index group. It preserves list membership/order and source descriptors, allowing
the executor to validate original-to-final lineage from the two carried table
definitions. All indexes rebuild: no name-keyed cloning, and every final plugin
index is marked affected. Rename does not introduce a new protobuf action.

COPY must supply stored index session variables to internal CREATE planning
before publication/build, retaining newly allocated physical identities. Existing
plugin interfaces perform rebuild and background registration; no algorithm
dispatch is added to SQL/catalog code.

Foreign-key restoration uses live source/child constraints. Rewrite explicit
bindings to renamed keys for self and incoming FKs, preserving empty legacy
bindings, PRIMARY and unrelated outgoing FKs. Catalog rows use a simultaneous
CASE mapping scoped to the referenced database/table, so name swaps cannot
cascade. All changes participate in the existing COPY transaction.

## Failure, compatibility and lifecycle

The outer transaction owns row/schema/FK/maintenance publication and rollback.
Source retirement uses existing COPY drain/fence cleanup; fresh indexes use
existing create hooks. No watermark is transferred or advanced by rename.
Failures and cancellation must roll back new metadata and retain usable old
rows/indexes. Existing stable table logical-ID/data-branch lineage rules remain.
Old coordinators reject the new syntax; internal reconstruction uses existing
plan, catalog and job formats. No persisted schema format changes are required.

## Validation

- Parser/tree: aliases, quoting, pooling, table-rename control.
- Planner: normalized names, errors, chains/swaps, grouped indexes, immutable
  source, rebuild selection, unsupported mixed operations/hints.
- Metadata/FK: stored session variables before CREATE; self/incoming/unrelated
  and legacy bindings; simultaneous swaps, quoting and rollback.
- Public SQL: ordinary/unique indexes, DML, SHOW/catalog names, FK enforcement;
  fake-PK table with deleted rows; FULLTEXT2/HNSW query, DML, new-name REINDEX and
  replacement readiness using minimum data.
- Lifecycle: reuse existing COPY fault/maintenance tests for rollback and stale
  worker identity, plus focused checks for new metadata transformations.

No constant-time or uninterrupted async-index availability claim is made.
