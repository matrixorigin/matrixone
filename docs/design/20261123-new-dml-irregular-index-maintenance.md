# DML maintenance of FULLTEXT and vector indexes on the new DML planner

- Status: Review required
- Tracking issues: [#21790](https://github.com/matrixorigin/matrixone/issues/21790),
  [#21791](https://github.com/matrixorigin/matrixone/issues/21791),
  [#33108](https://github.com/matrixorigin/matrixone/issues/33108)
- Implementation PR: [#33110](https://github.com/matrixorigin/matrixone/pull/33110)
- Design revision: 1
- Contract revision: R1
- Last updated: 2026-11-23

## Design-gate classification

The change is a feature and a major refactor. It changes more than 500
non-generated production lines in the SQL planner, enables primary-key UPDATE
on tables with synchronous FULLTEXT and IVF_FLAT indexes, and changes which
planner route owns hidden-index writes for DELETE, UPDATE, REPLACE and
foreign-key actions. It does not change a catalog, storage, wire or
configuration format.

## Problem

A table with an irregular index keeps one or more hidden tables. INSERT and
LOAD already maintained them on the new DML planner (bind + MULTI_UPDATE).
DELETE and UPDATE did not:

1. DELETE on a table with any irregular index fell back to the legacy planner.
2. Primary-key UPDATE on a table with a synchronous FULLTEXT or IVF_FLAT index
   was rejected (#25617).
3. Foreign-key actions did not maintain the child table's hidden tables
   (#33108). UPDATE on a parent was rejected when the action changed a child
   with an irregular index. REPLACE on a parent panicked when its cascade
   reached a child with a synchronous one. A parent DELETE that fired both
   ON DELETE CASCADE and ON DELETE SET NULL on one child dropped the index
   entries of the rows set to NULL, regular secondary indexes included.

## Index classes

Every irregular index belongs to exactly one class, decided per index from
the index-plugin descriptor and the index's `async` parameter:

| Class | Algorithms | Hidden-table writer |
|---|---|---|
| Asynchronous | HNSW, CAGRA, IVF-PQ, FULLTEXT2; IVF_FLAT and FULLTEXT created `ASYNC` | CDC, from committed base-table changes |
| Synchronous, maintained | IVF_FLAT, FULLTEXT, MASTER | The DML statement |
| Synchronous, unmaintained | none today | DELETE and UPDATE are rejected |

The planner never writes the hidden tables of an asynchronous index on any
route. DELETE and UPDATE on a table with a synchronous index that has no
statement maintenance return a not-supported error on every planner route,
instead of committing base rows without hidden rows. INSERT, REPLACE and LOAD
do not check this class, so a new synchronous algorithm must provide
statement maintenance on every DML route before it registers.

## Contract

### Hidden-index identity

Every hidden row of a synchronous index carries the base-table primary key
(`doc_id` for FULLTEXT, the original-key column for IVF_FLAT entries, the
IVF_FLAT ignored table and MASTER). For a composite key this is the serialized
key. The primary key is the only identity the maintenance uses.

### Maintenance invariant

At commit, for every synchronous index, the hidden rows are exactly the hidden
rows generated from the live base rows, keyed by their primary keys.

A statement preserves the invariant through two operations per affected base
row:

- **Delete by old key:** remove every hidden row whose key is the row's key
  before the statement.
- **Insert from final image:** generate the hidden rows from the row's final
  image, keyed by its final key.

A deleted row receives only the first. An updated row receives both when the
update changes an input of the index: an indexed column, the primary key, or a
column a plugin declares as a rewrite input. The primary key is an input of
every synchronous index, so a primary-key change rebuilds every one.

### Ownership

Each affected base row has exactly one maintenance owner per index: the row
stream that owns the row's physical base-table delete. The owner emits both
operations for that row, from a materialized image of the row, and no other
stream writes that row's hidden entries.

| Route | Owner of the row's hidden writes |
|---|---|
| DELETE planned by the new binder | The statement's target rows |
| UPDATE planned by the new binder | The root UPDATE rows, including rows a self-referencing foreign-key action merges into them. Columns that such an action changes count as updated columns of the root. |
| Parent UPDATE, foreign-key action on another table | That action's child rows |
| REPLACE | The replaced and inserted rows, and separately each foreign-key action's child rows |
| DELETE planned by the legacy planner (foreign keys, multi-table) | Each target table's delete stream, and each foreign-key action's stream |

When two legacy delete streams reach one table, their row sets merge into one
stream, which then owns the hidden deletes of every merged row. The merge
applies only to streams that may share rows. A SET NULL stream that excludes
every row a CASCADE action of the same parent removes is disjoint from that
CASCADE stream. It never merges into it, and it owns both operations for its
own rows.

### Ordering

For each owner, the delete-by-old-key operation precedes the insert of the
final image. Maintenance planned before the statement's plan is finalized
(REPLACE foreign-key actions) is queued and emitted after it in queue order.
The queue keeps the same delete-then-insert order per owner. A queued
maintenance source keeps its column layout, so the primary key is read from
the position it was recorded at.

### Transactions and CDC

Hidden-table writes belong to the statement's transaction. A failed statement
or a rolled-back transaction discards them together with the base-table
writes, so no route publishes a partial hidden-index update. Asynchronous
indexes observe only committed base-table changes through CDC, so they
converge to the committed rows whichever planner route the statement took.

## Supported shapes

| Statement | Shapes |
|---|---|
| DELETE | Point, predicate, IN subquery, ORDER BY ... LIMIT, single-target join, multi-table, prepared, WHERE TRUE, inside an explicit transaction |
| UPDATE | Indexed or unindexed columns, primary key (including key swaps in one statement), RETURNING, composite keys |
| Foreign-key actions | ON DELETE CASCADE and SET NULL, ON UPDATE CASCADE and SET NULL, two SET NULL keys with CASCADE on one parent, self-referencing tables, multi-level cascades |
| REPLACE | Cascade and SET NULL children, self-referencing tables, multi-level cascades |

Known gap: REPLACE whose cascade reaches an ON DELETE SET NULL grandchild fails
at planning even without any index (#33109). It is the only shape that could
merge a non-disjoint SET NULL stream into a cascade stream, so the
merged-stream rebuild is unreachable until #33109 is fixed.

## Resource bounds

Per owner and per synchronous index, a statement adds one delete sub-plan and
at most one insert sub-plan. It also keeps one materialized row image per
owner, which holds the affected rows the base-table delete already reads. The
maintenance queue holds one entry per owner and operation, bounded by the
statement's foreign-key action graph. A delete sub-plan pushes an exact
runtime filter on the deleted keys into its hidden-table scan, bounded by the
existing IN-filter cardinality limit. Above that limit the scan reads the
whole hidden table, as a join without a filter does. No state outlives the
statement.

## Alternatives considered

- **Keep the legacy fallback for irregular indexes.** This keeps two
  maintenance implementations per algorithm and the foreign-key defects in
  #33108.
- **Move foreign-key and multi-table DELETE to the new binder first.** This
  removes the legacy route entirely. It is larger than this change and
  touches cascade semantics unrelated to indexes. The legacy route now calls
  the same maintenance sub-plans, so it can migrate later without changing
  this contract.
- **Rebuild every index after a merge.** A merged SET NULL stream would then
  re-insert all of its rows' entries. That needs the set of indexes the
  merged owner deleted, which depends on registration order. Keeping disjoint
  streams separate avoids the question.
- **Queue all legacy maintenance.** Queuing also after the plan is finalized
  would move the FULLTEXT re-insert of legacy updates. That re-insert is
  shared with the legacy insert route, so the change is limited to maintenance
  planned before finalization.

## Compatibility and rollout

The change alters query plans only. It adds no catalog, hidden-table, storage,
protocol or configuration change, and needs no migration. Hidden tables
written by the legacy and new routes have the same rows. Rollback is a source
revert.

## Validation contract

Each statement in the BVT cases below is followed by an oracle query. For every
synchronous index, it counts hidden keys without a live base row (orphans) and
live base rows without a hidden key (missing). Both counts must be 0. Index-
served reads (MATCH, l2_distance top-k, MASTER lookups) must return the
expected rows. For asynchronous indexes the oracle polls until CDC converges.

- `dml/delete/delete_irregular_index_sync`: DELETE and primary-key UPDATE
  shapes on IVF_FLAT, FULLTEXT and MASTER, foreign-key DELETE and multi-table
  DELETE, rollback and composite keys.
- `pessimistic_transaction/vector/delete_async_irregular_index`: DELETE on
  FULLTEXT2, HNSW and ASYNC IVF_FLAT after CDC catch-up.
- `foreign_key/fk_irregular_index`: every foreign-key and REPLACE shape listed
  above.
- `gpu_cases/.../vector_cagra_dml_irregular`, `vector_ivfpq_dml_irregular`:
  DELETE, primary-key UPDATE and foreign-key actions on CAGRA and IVF-PQ.

Unit tests cover the index classification, the fail-closed rejection on the
legacy route, the runtime filter on hidden-table deletes, and the producer-
before-consumer order of sink folding.
