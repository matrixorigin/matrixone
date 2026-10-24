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

The name carries no snapshot timestamp (decision, @cpegeric, 2026-10-24). Snapshot reads
are handled by the storage engine and the content-addressed name: a reader resolves names
from the metadata visible to its transaction, and identical content read at different
snapshots is one file in the cache. Rejected alternative: a timestamp in the name, which
would store one copy of identical bytes per snapshot read.

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

- A read takes a file name and the caller's transaction, which may be pinned to a
  snapshot; the layer has no snapshot logic of its own. It returns the local file from
  the disk cache when present.
- On a miss, the layer rebuilds the file from its chunk rows through the storage engine's
  relation readers, under the caller's transaction and snapshot (no SQL), verifies it,
  places it in the disk cache, and returns it. It sees exactly the rows a SQL `SELECT`
  would see at that snapshot, including historical (snapshot) reads, uncommitted rows of
  the caller's transaction and rows not yet flushed to storage objects. The relation
  readers read the stored objects through the fileservice, as any scan does.
- How a rebuild uses the existing memory and table-data disk caches is a CN configuration
  setting (decision, @cpegeric, 2026-10-24): read without filling them (default), bypass
  them, or read and fill them like an ordinary scan. The default reuses chunk data that is
  already cached without storing it a second time beside the assembled file, which goes
  to the index-file cache in every mode.
- A missing or duplicated chunk is an error, never a short or padded file.
- Each CN fills its own disk cache from the rows on first read.

### Integrity: a manifest beside the file (decision)

Decided by @cpegeric, 2026-10-24. Each cached index file is paired with a separate
manifest (format version, length, checksum). The index file itself is exactly the
payload, byte for byte what the chunk rows hold: a tarball stays a tarball, a graph file
stays a graph file. Chunk rows carry neither, so the rows keep today's bytes and no
format gate is needed.

- Consumers that open the file by path (an archive unpacker, a library that maps the
  whole file) see the same bytes as today; nothing is appended or prepended.
- When the layer rebuilds a file from its rows, it verifies the bytes against the
  checksum in the file's metadata row, by the rule of that row's kind (a whole-file
  checksum for a base file, the frame's own footer checksum for a CDC tail frame), then
  writes the file and its manifest.
- When a cached file is opened, its manifest verifies the local copy. The manifest never
  leaves the CN, so it uses one checksum for every file kind.
- The file and its manifest are written together. A missing or mismatched manifest makes
  the file a miss, rebuilt from the rows; a partial cache state is never served.

Rejected alternatives: a header or trailer inside the cached file, which changes the
bytes path-based consumers read; and a header inside the chunk rows, which changes the
stored format and needs a rollout gate.

### Where files are cached (decision)

Decided by @cpegeric, 2026-10-24. Index files live in their own disk cache: a separate
directory and cache instance on the CN, built from the same disk-cache code as the
table-data cache but not shared with it. Its capacity is a configured share carved out of
the existing disk-cache capacity, so total disk use is unchanged and the split between
table data and index files is explicit: a large index load cannot evict hot table data,
nor the reverse. Index files exist only in this cache: a miss is never served from the
storage backend, only rebuilt from the chunk rows. A CN without a disk cache uses a
private temporary directory instead, with no reuse between loads.

Rejected alternative: placing index files in the shared table-data cache under a reserved
namespace. It needs no new capacity setting, but a large index load and hot table blocks
would evict each other.

### Eviction: the existing disk-cache rules (decision)

Decided by @cpegeric, 2026-10-24. The index-file cache uses the existing disk cache's
eviction mechanism and rules unchanged; index files get no pinning or other special
treatment.

- Eviction follows the existing policy against the cache's capacity, including eviction
  when the disk is full, and removes the evicted file.
- A reader never blocks eviction. It only waits for an in-progress write or eviction of
  the same file, then opens it. A file evicted while a reader holds it open or mapped stays
  readable through that handle; its space is freed when the last handle closes.
- A file that is gone when a reader opens it is a miss, rebuilt from the chunk rows.
- Cached files survive a CN restart, as table-data cache files do; the manifest is checked
  on open.

Accepted consequences:

- Disk use may exceed the configured capacity by evicted files that are still mapped.
  Table-data reads hold a file briefly; a loaded index holds its file while it is loaded.
  The excess is bounded by what the index-cache governor keeps loaded.
- A consumer must use the handle the layer returns, not reopen the cache path by name: an
  eviction between the two opens makes the name disappear.

Rejected alternative: pinning open files so eviction skips them. It keeps disk use within
capacity, but adds a second eviction rule to the shared disk-cache code.

### Memory

The layer never stores file bytes in the memory cache: file bytes live in the index-file
disk cache, sized and evicted by its configured capacity. A miss rebuild may read chunk
data already in the memory cache, under the rebuild setting above. What a search keeps resident (a loaded graph, a GPU index,
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

1. **CDC tail as files.** Proposed: each frame is one file, whatever the number of chunks
   it spans; readers open the frame files in order. The frame encoding, which both the
   vector indexes and FULLTEXT2 already share, moves out of the GPU package so neither
   depends on it. To be confirmed with the incremental-update design.

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
- a corrupted chunk fails verification at rebuild, and a corrupted cached file or
  manifest, or a file without its manifest, is rebuilt instead of being served;
- the cached file is byte-identical to the payload (an archive or mapped file opens
  unchanged);
- the chunk rows written through the layer are byte-identical to those the SQL path writes
  for the same file;
- clone, restore and snapshot reads of an index written through the layer behave as
  before, including a snapshot search after a merge, which rebuilds the merged-away
  segment from the rows visible at the snapshot;
- the layer never stores file bytes in the memory cache, and index-file cache use stays
  within its configured capacity, plus evicted files still mapped, under a load larger
  than it;
- a file evicted while open or mapped stays readable through the open handle, and the next
  open is a miss that rebuilds it;
- each rebuild cache setting behaves as stated: the default fills neither the memory nor
  the table-data disk cache, bypass reads none of them, read-and-fill fills them;
- cached index files are reused after a CN restart;
- a CN without a disk cache still serves reads, rebuilding each load from the rows;
- the table-data cache behaves exactly as before: its capacity is reduced by the
  configured share and nothing else changes.
