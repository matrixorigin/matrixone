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

A file name is derived from the file identity and its version: the index table's id,
`index_id`, `tag` and the checksum in the metadata row the loader already reads; a CDC
tail frame's name also carries its starting chunk and its length. A name is never reused
for different content, so a rewritten file has a new name and nothing is invalidated.

- The index table is named by its table id (decision, @cpegeric, 2026-10-24). Table ids
  are never reused. An operation that gives the hidden table a new id (truncate, a copying
  ALTER, clone, restore) gives its files new names; identical bytes under two table ids
  are cached twice. A rebuild in place (REINDEX) keeps the table id; its new generation
  has a new `index_id` and checksum, so a new name. A snapshot read resolves the table id
  visible at its snapshot.
- A CDC tail frame's checksum is 32 bits, weaker than a base file's (decision,
  @cpegeric, 2026-10-24). After a merge rewrites the tail, a frame with different content
  may start at the same chunk; the length in the name narrows a collision, and the
  remaining chance (same start, length and checksum) is accepted.

The name carries no snapshot timestamp (decision, @cpegeric, 2026-10-24). Snapshot reads
are handled by the storage engine and the content-addressed name: a reader resolves names
from the metadata visible to its transaction, and identical content read at different
snapshots is one file in the cache. Rejected alternative: a timestamp in the name, which
would store one copy of identical bytes per snapshot read.

### Writing (decision)

Decided by @cpegeric, 2026-10-24.

- The writer takes the whole file as a stream and keeps it as one local file. The chunk
  rows reach the table through internal `INSERT ... SELECT` statements in the caller's
  transaction, whose source is a table function that cuts the local file into chunk rows.
  The usual insert pipeline therefore applies unchanged: primary-key locks, the duplicate
  check, the composite key and the workspace spill. Commit and abort of the rows are atomic
  with the metadata row, as with today's `INSERT`.
- Each statement writes a bounded range of chunks, as today's writers do, so what one
  statement holds in memory does not grow with the file. Chunks travel as batches of the
  pipeline, not as statement parameters.
- The table function is reachable only from internal SQL, and only for the transaction
  that registered the file; it takes a handle, never a path.
- After the statements succeed, the same local file becomes the cached file, without a
  second copy. It goes to the disk cache only, never the memory cache. The writing CN reads
  it warm from local disk.
- A file cached by a transaction that aborts is never named by a committed metadata row,
  so no reader reaches it; it is evicted unused.

Rejected alternative: writing the rows through the storage engine's relation write path
without SQL. It skips the primary-key lock and duplicate check that `INSERT` performs, so
two writers that choose the same chunk ids could both commit and corrupt the file; the
storage engine catches such a duplicate only between overlapping transactions, and not at
all once the rows are flushed to storage objects.

### Reading

- A read takes a file name and the caller's transaction, which may be pinned to a
  snapshot; the layer has no snapshot logic of its own. It returns the local file from
  the disk cache when present.
- On a miss, the layer rebuilds the file from its chunk rows through the storage engine's
  relation readers, under the caller's transaction and snapshot (no SQL), verifies it,
  places it in the disk cache, and returns it. It sees exactly the rows a SQL `SELECT` in
  the caller's position would see: at the caller's snapshot, including historical
  (snapshot) reads, rows not yet flushed to storage objects, and the caller's uncommitted
  rows up to the same workspace position (the statement boundary for a statement, the
  whole workspace for internal SQL that opens no statement). The relation
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
- The manifest never leaves the CN, so it uses one checksum for every file kind. The
  full checksum is verified on the first open of a cached file after the CN starts; later
  opens on the same CN check the length only (decision, @cpegeric, 2026-10-24). A failed
  check removes the cached file and rebuilds it from the rows.
- The file and its manifest are one cache entry: a directory holding both (decision,
  @cpegeric, 2026-10-24). It is written complete under a temporary name and renamed into
  place in one step, evicted and deleted as a unit, and counted as one entry of their
  combined size. A partial entry is never visible; a missing or mismatched manifest makes
  the file a miss.

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

- The share is enabled by default (decision, @cpegeric, 2026-10-24): on upgrade the
  table-data cache shrinks by that share. A runtime resize of the disk cache applies the
  same split to the new size.
- A CN whose storage backend is local disk has no table-data disk cache, because table
  data is already local, but it still gets the index-file cache, sized as the same share of
  the configured disk capacity (decision, @cpegeric, 2026-10-24): rebuilding a file from its
  rows costs the same on any backend.
- Only CNs get the cache; TN, LOG and proxy nodes do not serve index searches.
- Several CNs in one process configured with the same cache directory share it, as they
  share the table-data cache directory today (decision, @cpegeric, 2026-10-24). Files are
  immutable and content-named and every write is a single rename, so sharing is safe; a
  file evicted by one CN is a miss for the other. Rejected alternative: a CN id in the
  path, which stores identical bytes once per CN.
- The cache is best effort. A failure to write the cache (disk full, a file larger than
  the cache) never fails the transaction or the query: the rows are the truth, and the
  reader gets its file from a private temporary file removed when it is closed.

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

Explicit removal frees disk as today. Today a loaded index's file is unlinked and freed
when the loaded index is destroyed. When the index cache destroys a loaded index on
explicit removal (DDL, a CDC or MERGE replacement, a replaced generation), its files are
deleted from the index-file cache. When it evicts a current index because it is idle or
under memory pressure, the files stay, so the next load reads them from disk instead of
from the rows; this is the one intended change (decision, @cpegeric, 2026-10-24). A CN
that never learns of a DROP keeps the files until capacity eviction.

Rejected alternative: pinning open files so eviction skips them. It keeps disk use within
capacity, but adds a second eviction rule to the shared disk-cache code.

### One rebuild per file (decision)

Decided by @cpegeric, 2026-10-24. Concurrent misses of the same file rebuild it once. A
rebuild holds a lock per file name, shared by every CN in the process; other readers of
that file wait for it (and can be cancelled), then find it in the cache. If the rebuild
fails, the next waiter tries. A short wait on the cache's own write coordination is not
enough: a rebuild takes seconds, and a reader that stops waiting would rebuild the file
again.

### CDC tail: one file per frame (decision)

Decided by @cpegeric, 2026-10-24, for the first version. Each CDC tail frame is one
file, whatever the number of chunks it spans; a reader opens the frame files in order.
The frame encoding, which the vector indexes and FULLTEXT2 already share, moves out of
the GPU package so neither depends on it.

- A frame is immutable and matches its metadata row one to one, so a reload rebuilds only
  frames it has not cached, and the writing CN already holds the frames it wrote.
- MERGE removes the whole tail, and its files with it, so the number of frame files is
  bounded by the frames between merges.

Rejected alternatives: one file for the whole tail, whose content and name change on
every appended frame, rewriting the tail each time (quadratic bytes); and appending to a
cached file in place, which reuses a name for different content and breaks the write-once
entry that CNs sharing a directory rely on.

Revisit when measurement shows the frame count between merges makes per-frame opens or
the file count matter. The candidate then is packing inside the cache, in doubling tiers:
new frames form small packs, two packs of similar size combine into one twice as large,
and each pack is immutable and named by its first and last frame and a checksum over its
frames' checksums. About log n files per tail, each byte rewritten about log n times; the
chunk rows are unchanged. Decide together with the incremental-update design.

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
- Before a vector algorithm adopts the layer: its libraries open files by name, so they
  must be given the handle the layer returns; and IVF-PQ and CAGRA unpack their archive
  into a second copy on disk, outside the cache and its capacity.
- Range reads of a file. Consumers map or load whole files today, and the disk cache
  serves the whole file.

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
- writing chunk rows that already exist fails with a duplicate-key error, as an `INSERT`
  does;
- the table function cannot be called from user SQL, nor by another transaction, nor after
  its write finished;
- clone, restore and snapshot reads of an index written through the layer behave as
  before, including a snapshot search after a merge, which rebuilds the merged-away
  segment from the rows visible at the snapshot;
- the layer never stores file bytes in the memory cache, and index-file cache use stays
  within its configured capacity, plus evicted files still mapped, under a load larger
  than it;
- a file evicted while open or mapped stays readable through the open handle, and the next
  open is a miss that rebuilds it;
- concurrent readers missing the same file rebuild it once; cancelling a waiter does not
  affect the rebuild; a failed rebuild lets the next waiter rebuild;
- the file and its manifest are never visible apart, and are evicted together; a
  corrupted cached file is detected on the first open after a restart and rebuilt;
- two CNs in one process sharing the directory never serve a partial file;
- explicit removal deletes the removed index's files; idle eviction keeps them;
- a tail reload rebuilds only frames not yet cached, and a merge deletes every frame file
  of the old tail;
- a cache write failure does not fail the write transaction or the read;
- a runtime disk-cache resize applies the share;
- each rebuild cache setting behaves as stated: the default fills neither the memory nor
  the table-data disk cache, bypass reads none of them, read-and-fill fills them;
- cached index files are reused after a CN restart;
- a CN without a disk cache still serves reads, rebuilding each load from the rows;
- the table-data cache behaves exactly as before: its capacity is reduced by the
  configured share and nothing else changes.
