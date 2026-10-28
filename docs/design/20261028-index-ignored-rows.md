# Rows an index cannot store

Status: proposed.
Issue: #31583 (fixes #31493).

## Problem

An index can be given a source row whose value it cannot store, for example a `vecf32` element
above 65504 in an HNSW index that stores `float16`. The source table accepted the row, so the
write cannot be undone, and an asynchronous index learns about the row only after it is
committed. Retrying the change cannot succeed: the same value fails the same way on every
replay. The index stops at that row, every later change of the table is missing from it, and
queries keep using the stale index without any warning.

## Decision

Decided by @cpegeric, 2026-10-28.

A source row whose value an index cannot store is left out of that index and recorded, with
the reason, in the index's ignored rows. The index keeps building and syncing. This follows
Elasticsearch's `ignore_malformed`: the document is indexed without the malformed field, and
the field name is recorded in the document's `_ignored` metadata.

- **The ignored rows hold exactly the source rows that exist and are not in the index** because
  their value cannot be stored. A row is recorded when it is inserted or updated with such a
  value. It leaves the ignored rows when it is deleted, or updated to a value the index stores.
  Updated to another value the index cannot store, its reason changes.
- **A synchronous CREATE INDEX rejects the row instead.** The user is waiting on the statement
  and it rolls back as a whole, so the error reaches the person who can fix the data, as
  Elasticsearch rejects a malformed document at write. This covers `CREATE INDEX`,
  `ALTER TABLE ... ADD INDEX` and an index declared in `CREATE TABLE`, when they build the index
  synchronously.
- **Every other path ignores and records the row**, by the same rule: an asynchronous build,
  incremental sync, REINDEX (including `FORCE_SYNC` and the rebuild after a restore) and the
  rebuild of ALTER TABLE. These run without a user to answer, or on an index that already exists
  and may already hold ignored rows, so failing them would stop the index again. REINDEX and
  TRUNCATE start the ignored rows over.
- **Only the value of one row is ignored.** An error that does not belong to one row's value,
  such as a lost connection, a failed write of the index or a missing model, keeps its existing
  handling.
- **A search vector is not a row.** A query vector the index cannot store is rejected.
- Each sync step that ignores rows writes one log line with the index, the count, a few primary
  keys and the reason.

## Interface

```sql
SHOW INDEX IGNORED FROM [db.]tbl [WHERE expr]
```

It returns one row per ignored source row of every index of the table:

| Column | Content |
|---|---|
| `Key_name` | the index |
| `Primary_key` | the primary key of the source row |
| `Reason` | why the index cannot store the row |
| `Ignored_at` | when the row was recorded |

`WHERE` filters on these columns, as in `SHOW INDEX`. For a partitioned table it lists the
ignored rows of the indexes of every partition.

## Scope

HNSW records its ignored rows first. IVF-Flat, CAGRA, IVF-PQ and FULLTEXT2 follow under #31583.
The ignored table is created with the index. An index created before this change has none and
keeps its previous behavior, without SHOW INDEX IGNORED rows, until it is dropped and created
again; REINDEX does not add the table.

## Claims

| # | Claim | Black-box proof | Unit proof |
|---|---|---|---|
| I1 | A row an HNSW index cannot store does not stop later rows from being indexed. | `vector_hnsw_ignored.sql` | `TestHnswWriterIgnoresUnstorableVectors` |
| I2 | The ignored rows follow inserts, updates and deletes of the source rows, and REINDEX and TRUNCATE start them over. | `vector_hnsw_ignored.sql` | `TestIgnoredRowsTransitions` |
| I3 | REINDEX and incremental sync ignore the same rows, by the same rule as the conversion the index stores with. | `vector_hnsw_ignored.sql` | `TestCheckStorable` |
| I4 | A synchronous CREATE INDEX fails on a row the index cannot store and leaves no index. | `vector_hnsw_ignored.sql`, `vector_hnsw_quantization.sql` C14 | `TestGenBuildSQL` |
| I5 | `SHOW INDEX IGNORED` lists the ignored rows of every index and partition of a table, and `WHERE` filters them. | `vector_hnsw_ignored.sql` | `TestShowIndexIgnored` |
