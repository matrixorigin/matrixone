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
  The query planner, executor and result batches see chunk rows, not a file, and
  nothing below the index cache is reused between loads: every load reads every chunk
  through SQL again.
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

### The unit is the whole file (decision)

Decided by @cpegeric, 2026-10-24. The layer reads and writes whole files, named. A reader
needs only the file name; it never addresses chunks or byte ranges. The chunks are an
internal detail of how a file is stored.

### File names

A file name is derived from the file identity and its version: the index table,
`index_id`, `tag` and the checksum in the metadata row the loader already reads. A name
is never reused for different content, so a rewritten file has a new name and nothing is
invalidated.

### Writing

- The writer takes the whole file as a stream. It writes the chunk rows through the
  storage engine's relation write path in the caller's transaction (no SQL is parsed,
  planned or executed) and, from the same bytes, the file into the CN's disk cache. Commit
  and abort of the rows are atomic with the metadata row, exactly as with today's
  `INSERT`. Large writes spill to storage objects through the existing transaction
  workspace mechanism.
- The file goes to the disk cache only, never the memory cache. The writing CN reads it
  warm from local disk.
- A file cached by a transaction that aborts is never named by a committed metadata row,
  so no reader reaches it; it is evicted unused.

### Reading

- A read by name returns the local file from the disk cache when present.
- On a miss, the layer rebuilds the file from its chunk rows through the storage engine's
  relation readers, under the caller's transaction and snapshot (no SQL), verifies it,
  places it in the disk cache, and returns it. It sees exactly the rows a SQL `SELECT`
  would see at that snapshot, including historical (snapshot) reads.
- A missing or duplicated chunk is an error, never a short or padded file.
- Each CN fills its own disk cache from the rows on first read.

### Integrity (decision)

No new checksum is added. A base file is verified against the whole-file checksum in its
metadata row; a CDC tail frame carries a checksum in its own footer. Because reads are
whole files, these cover every read.

### Memory

The layer uses no memory cache: file bytes live in the disk cache, sized and evicted by
its configured capacity. What a search keeps resident (a loaded graph, a GPU index,
mapped postings) stays under the existing index-cache governor. This is how index I/O
shares the CN with SQL instead of claiming its memory.

### Interoperability and compatibility (decision)

The layer reads rows written by the SQL path and writes rows the SQL path can read. A
file can be written by one path and read by the other, on the same CN or across CNs. No
schema, catalog or wire change is involved, so a rolling upgrade needs no capability
gate, and each algorithm can switch its readers and writers independently.

## Scope and non-goals

- In scope: the logical file layer (naming, whole-file write and read, disk cache) and
  its contract. Algorithms adopt it one at a time, FULLTEXT2 first (decision,
  @cpegeric, 2026-10-24): its base segments and its CDC tail frames.
- Not in scope: the incremental-update algorithm itself (the next PR); a new on-disk
  format; changes to what clone, restore or backup copy; IVFFLAT, which stores no
  chunks; GPU residency.
- Range reads of a file. Consumers map or load whole files today, and the disk cache
  serves the whole file.

## Open questions

1. **Mapping a cached file.** FULLTEXT2 maps whole files. The disk cache exposes no local
   path for an entry today, and it must not evict (or count as free) a file that is still
   mapped. The layer needs a way to hold a cached file open for the life of the mapping.
2. **CDC tail as files.** The tail grows by appended frames. Each frame becomes its own
   named file; whether a reader opens frames individually or the layer presents the tail
   as an ordered list of frame files is to be decided with the incremental-update design.

## Validation

Every statement in "Contract" is checked by a test that would fail if it were false:

- the bytes read through the layer equal the bytes read through SQL, for each adopting
  algorithm,
  at the current snapshot and at a historical snapshot;
- a file written through the layer is read back identically through SQL, and the
  reverse;
- a read on a disk-cache miss rebuilds the file from the rows, and a second read is served
  from the disk cache without reading the rows;
- a missing or duplicated chunk fails the read;
- an aborted write leaves no rows, and no reader reaches its cached file;
- a rewritten file never serves its previous content;
- a corrupted cached file or chunk fails verification instead of being served;
- clone, restore and snapshot reads of an index written through the layer behave as
  before;
- writes and reads use no memory cache, and disk-cache use stays within its configured
  capacity under a load larger than it.
