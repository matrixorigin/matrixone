# Index file I/O: a logical file layer over index chunk tables

- Status: proposed
- Branch: `index_io`
- Last updated: 2026-10-24

## Problem

HNSW, CAGRA, IVF-PQ and FULLTEXT2 store each index file as fixed-size chunk rows in a
hidden table. A file is identified by the index table, an `index_id` and a `tag`; byte
offset `o` lives in the chunk `o / chunk size`. Every byte moves through SQL:

- **Reading** runs a `SELECT` over all chunks of the file and materializes the whole file
  (a spill file that is then mapped, or the heap) before the search can use any of it.
  The query planner, executor and result batches see chunk rows, not a file: no read can
  fetch only the chunks it needs, and nothing below the index cache can be reused
  between loads.
- **Writing** builds `INSERT` statements over the chunks (bound parameters or hex
  literals), parses, plans and executes them. The write is slow, and the bytes just
  written are not available to the next read: the writing CN loads the file again
  through the read path.

Incremental index maintenance (the next PR) multiplies both costs: it writes many small
files and reads them back often.

## Contract

### Physical storage does not change (decision)

Chunks stay rows of the existing hidden tables, with the existing columns, primary key
and `tag` meaning. The index file is a **logical** view of those rows, not a new storage
object. Everything that already works on the table keeps working with no change:
transactions and visibility, snapshot and PITR reads, clone, restore, backup, account
isolation, DROP and garbage collection.

Rejected alternative: storing index files as fileservice objects with only metadata rows
in the table. It would give the fastest I/O, but every lifecycle path above would need a
second, file-aware path, and a file orphaned by an aborted transaction or a merge would
need its own garbage collection.

### File identity and versions

A file is `(index table, index_id, tag)`. Its bytes at a given read snapshot are exactly
the chunk rows visible at that snapshot, concatenated by `chunk_id`. A file version is
identified by the content checksum recorded in the index metadata table.

The CDC tail (`tag` 1) is not one immutable file: it grows by appended frames, each
with its own metadata row. The unit of identity and versioning for the tail is the
frame (its starting chunk and its checksum), not the whole tail.

### Reading

- A read runs under the caller's transaction and snapshot and sees exactly the rows a
  SQL `SELECT` would see at that snapshot, including historical (snapshot) reads.
- It reads the chunk rows through the storage engine's relation readers with a
  primary-key filter on the file. No SQL is parsed, planned or executed.
- A range read fetches only the chunks covering the range. A whole-file read is a range
  read of the whole file.
- A missing or duplicated chunk is an error, never a short or padded file.
- A whole-file read verifies the content checksum, as the current loaders do.

### Caching

- Chunks are cached individually, keyed by the table, file identity, chunk number and
  file version (content checksum). A cached chunk is served only to a read of the same
  version, so a rewritten file (same `index_id`, new content) never serves old bytes and
  no invalidation is needed.
- The cache is the fileservice memory and disk cache, so its size is the configured
  cache capacity, not the index size, and it competes with table data under the same
  eviction policy. This is how index I/O shares memory with SQL instead of claiming it.
- The cache holds bytes only. What a search keeps resident (a loaded graph, a GPU index,
  mapped postings) stays under the existing index-cache governor.

### Writing

- A write produces chunk batches and writes them through the storage engine's relation
  write path in the caller's transaction. No SQL is parsed, planned or executed. Commit
  and abort are atomic with the metadata row, exactly as with today's `INSERT`.
- Large writes spill to storage objects through the existing transaction workspace
  mechanism.
- Chunks written are also placed in the cache under the version they will be read as,
  so the writing CN reads them warm. Because the key is the content checksum, a chunk
  cached by a transaction that later aborts can only ever be served for identical
  content, and is otherwise evicted unused.

### Interoperability and compatibility (decision)

The layer reads rows written by the SQL path and writes rows the SQL path can read. A
file can be written by one path and read by the other, on the same CN or across CNs. No
schema, catalog or wire change is involved, so a rolling upgrade needs no capability
gate, and each algorithm can switch its readers and writers independently.

## Scope and non-goals

- In scope: the logical file layer (identity, read, range read, cache, write) and its
  contract. Algorithms adopt it one at a time.
- Not in scope: the incremental-update algorithm itself (the next PR); a new on-disk
  format; changes to what clone, restore or backup copy; IVFFLAT, which stores no
  chunks; GPU residency.
- A consumer that needs one contiguous mapped file (for example a library that maps the
  whole index) still materializes the whole file, but through the cache instead of SQL.
  Range reads benefit consumers that address blocks: FULLTEXT2 postings and CDC tail
  frames.

## Open questions

1. **Chunk-level integrity.** The checksum covers the whole file, so a range read has no
   integrity check of its own. Options: rely on the storage engine's own object
   checksums, or add a per-chunk checksum (a schema change, which this design avoids).
2. **When a written chunk enters the cache.** The content checksum is known only once
   the whole file is written. Either buffer the keys until the file is complete, or key
   the write-through by a provisional version and re-key at completion.
3. **Adoption order.** Proposed: FULLTEXT2 tails and CAGRA/IVF-PQ CDC frames first (small,
   frequent files), then base files.

## Validation

Every statement in "Contract" is checked by a test that would fail if it were false:

- the bytes read through the layer equal the bytes read through SQL, for every algorithm,
  at the current snapshot and at a historical snapshot;
- a file written through the layer is read back identically through SQL, and the
  reverse;
- a range read returns the requested bytes and reads only the covering chunks;
- a missing or duplicated chunk fails the read;
- an aborted write leaves no rows and no served cache entry;
- a rewritten file never serves its previous content;
- clone, restore and snapshot reads of an index written through the layer behave as
  before;
- cache residency stays within the configured fileservice capacity under a load larger
  than it.
