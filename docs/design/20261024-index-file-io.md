# Index file I/O: a logical file layer over index chunk tables

- Status: approved by @fengttt on 2026-10-27, revision `fc9d8c02dd`
  ([review](https://github.com/matrixorigin/matrixone/pull/31050#pullrequestreview-5661299764))
- Issue: #31051
- Implementation PR: #31050
- Branch: `index_io`
- Last updated: 2026-10-29; the decisions dated 2026-10-28 and later await review

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

Incremental index maintenance multiplies both costs: it writes many small files and reads
them back often.

This document covers the layer itself: the file contract, the index-file disk cache, and
the read and write paths, and how FULLTEXT2, the first index to adopt it, maps its files onto
it. Each other algorithm adopts it in its own change.

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

A file is a contiguous range of chunks of one `(index table, index_id, tag)`: a base file
is all of its chunks from 0, and an appended unit such as a CDC tail frame is the chunks
from its starting chunk. Its bytes at a given read snapshot are exactly the chunk rows of
that range visible at that snapshot, concatenated by `chunk_id`. A file version is
identified by the size and content checksum the caller reads from the index's metadata
row; the layer reads no metadata itself.

### The unit is the whole file (decision)

Decided by @cpegeric, 2026-10-24. The layer reads and writes whole files, named. A reader
needs only the file name; it never addresses chunks or byte ranges. The chunks are an
internal detail of how a file is stored.

### File names

A file name is derived from the file identity and its version: the index table's id,
`index_id`, `tag`, the starting chunk, the size and the checksum. A name is never reused
for different content, so a rewritten file has a new name and nothing is invalidated.

- The index table is named by its table id (decision, @cpegeric, 2026-10-24). Table ids
  are never reused. An operation that gives the hidden table a new id (truncate, a copying
  ALTER, clone, restore) gives its files new names; identical bytes under two table ids
  are cached twice. A rebuild in place (REINDEX) keeps the table id; its new generation
  has a new `index_id` and checksum, so a new name. A snapshot read resolves the table id
  visible at its snapshot.
- Files are named by a 32-bit checksum: the CRC32-C the layer records, or a caller's own,
  such as a frame checksum (decision, @cpegeric, 2026-10-24; CRC32-C for every file
  written since 2026-10-29). The starting chunk and size in the name narrow a collision,
  and the remaining chance (same start, size and checksum for different content) is
  accepted.
- A file rewritten in place, under the same `index_id` with a new checksum in its metadata
  row, is named by its writer's own full-strength checksum, such as an MD5, not by the
  32-bit one (decision, @cpegeric, 2026-10-29). HNSW rewrites each changed model this way
  on every sync. A model's saved size follows its node count, which slot reuse and in-place
  updates leave unchanged, so successive versions often share a size and differ only by
  checksum. Each sync then adds another version under the same name prefix, the 32-bit
  collision risk grows with the number of syncs, and a collision would silently serve an
  older model. The rewrite still needs no timestamp in the name: a reader at a snapshot
  reads the metadata row and the chunk rows visible at that snapshot, and each version
  has its own name. Each sync also caches a whole new version of the model; the version
  it replaces is no longer read, so the cache evicts it first.

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
  second copy, unless the caller writes the file without a cache entry. It goes to the disk cache only, never the memory cache. The writing CN reads
  it warm from local disk.
- A file cached by a transaction that aborts is never named by a committed metadata row,
  so no reader reaches it; it is collected once idle, or evicted.

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
  setting (decision, @cpegeric, 2026-10-26): read and fill them like an ordinary scan
  (default, today's behavior), read without filling them, or bypass them. The default
  keeps today's caching of chunk data, so when the index-file cache is disabled, too
  small or full to hold a file, reads still benefit from the memory and disk caches as
  before. The assembled file goes to the index-file cache in every mode.
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
- The checksum recorded for a file is its CRC32-C (decision, @cpegeric, 2026-10-29). A
  rebuild verifies it while the chunks stream in, in any order: each chunk's CRC is
  computed as it arrives and the file's is combined from them, so the file is never read
  back. Files recorded earlier with an MD5 are still verified by MD5 after assembly, and an
  appended frame can supply its own check. Rationale: MD5 runs at about 650 MB/s on one
  core and was over half of a 38 s cold load of a 13 GB index, while CRC32-C runs at
  several GB/s and is what Lucene, RocksDB, HDFS and Kafka use for the same purpose.
- The manifest never leaves the CN, so it uses one checksum for every file kind, the
  CRC32-C the rebuild already computed. Whether a cached file's full checksum is verified
  on its first open after the CN starts is a CN configuration setting, off by default
  (decision, @cpegeric, 2026-10-29, after Elasticsearch's `check_on_startup`). Off, every
  open checks the manifest and the length only; on, the first open in the process reads
  the whole file, and later opens check the length only. A failed check removes the cached
  file and rebuilds it from the rows. Rationale: the full check of a 13 GB index took
  most of a 4.4 s first query after a restart, and every new index generation reloads all
  of an index's files.
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
table-data cache for storing and publishing files but with its own eviction (see
"Eviction"), not shared with it, with its own configured capacity, so a large
index load cannot evict hot table data, nor the reverse. Index files exist only in this cache: a miss is never served from the
storage backend, only rebuilt from the chunk rows. Without an index-file cache, a file is
rebuilt into a temporary file in a directory the caller chooses, with no reuse between
loads; FULLTEXT2 uses the CN's local spill directory, not the system temporary directory.

- The capacity is a setting of its own, not a share of the table-data disk capacity
  (decision, @cpegeric, 2026-10-29; replaces the 30% share of 2026-10-24). The
  table-data cache keeps its configured capacity and behaves as before an upgrade, so
  existing caches and benchmarks are unchanged, and an index-heavy CN sizes the index
  files without resizing table data. The capacity has a default, used when the setting is
  absent; total disk use is the sum of the two capacities. A runtime resize of the disk
  cache applies to the table-data cache only.
- Operators size the index-file capacity so that it holds the files of every index the CN
  serves (decision, @cpegeric, 2026-10-28). Below capacity nothing is evicted. Of an index
  larger than the capacity, the files that do not fit are rebuilt from their rows on each
  load (see "Eviction").
- A CN whose storage backend is local disk has no table-data disk cache, because table
  data is already local, but it still gets the index-file cache (decision, @cpegeric,
  2026-10-24): rebuilding a file from its rows costs the same on any backend.
- Only CNs get the cache; TN, LOG and proxy nodes do not serve index searches.
- Several CNs in one process configured with the same cache directory share it, as they
  share the table-data cache directory today (decision, @cpegeric, 2026-10-24). Files are
  immutable and content-named and every write is a single rename, so sharing is safe; a
  file evicted by one CN is a miss for the other. Rejected alternative: a CN id in the
  path, which stores identical bytes once per CN.
- The index-file cache directory must not be, contain or lie inside the table-data cache
  directory; such a configuration is rejected at startup. Each cache's restart scan removes
  files it does not own.
- The cache is best effort. A failure to write the cache (disk full while staging or
  publishing a file, a file larger than the cache) never fails the transaction or the
  query: the rows are the truth, and the reader or writer continues with a private
  temporary file removed when it is closed. A writer keeps the bytes it already took
  from its source, so a source that cannot be re-read is not lost.

Rejected alternative: placing index files in the shared table-data cache under a reserved
namespace. It needs no new capacity setting, but a large index load and hot table blocks
would evict each other.

### Eviction: least recently used, never a held file (decision)

Decided by @cpegeric, 2026-10-29, replacing the 2026-10-24 decision to keep the table-data
cache's eviction. When the cache is over its capacity, it removes the files with no holds
whose last use is oldest, until it is within capacity. The table-data cache keeps its
own eviction.

- A file's last use is when it was published, opened or its last hold released. Every
  read through the layer holds its file, so the release of the last hold, recorded on disk
  with the publication, keeps the order across a restart. Files found on disk after a
  restart rank behind every file used since.
- A held file is passed over, so a file in use is never evicted. A newly written file is
  the most recently used and is evicted only after every older file that is not held.
- When the disk is full, the cache removes files the same way, a tenth of its capacity at
  a time.
- Removing a file leaves nothing of it on disk, so the files and directories the cache
  keeps are bounded by its live entries.
- A reader never blocks eviction. It only waits for an in-progress write or eviction of
  the same file, then opens it.
- A file that is gone when a reader opens it is a miss, rebuilt from the chunk rows.

Accepted consequences:

- Disk use exceeds the capacity only while held files alone exceed it, as after the
  capacity is lowered; the cache returns to its capacity on its next write once they are
  released.
- An index larger than the capacity keeps the files that fit, held while it is loaded; a
  file rebuilt once those fill the cache is evicted at once and served uncached, so each
  load rebuilds the same remainder (pmc, 13 GB index in a 10 GB cache: 3.0 s first query
  after a restart, against 0.25 s when it fits). The first load after the cache holds a
  different set of the index's files, as after the capacity is lowered, can rebuild more:
  a rebuilt file evicts the oldest file not yet loaded, which the same load needs next
  (30.8 s in the same case). Operators size the capacity to hold the indexes the CN serves.
- A consumer must use the handle the layer returns, not reopen the cache path by name: a
  removal between the two opens makes the name disappear. A library that takes a path is
  given a path to the open handle itself, which stays valid until the handle is closed.

Rejected alternative: the table-data cache's eviction (S3-FIFO). A new file enters a small
queue of a tenth of the capacity and is evicted from it unless read twice, while files of
replaced generations stay in the main queue; an index that does not fit settled only after
three loads, and a new generation arriving in a full cache could be evicted ahead of the
one it replaces. Index files are few, large, written once and read on every query, which
is what that admission rule is built to filter out.

### Holds and idle collection (decision)

Decided by @cpegeric, 2026-10-28. Readers and writers of every index share the cache, so
whether a file is in use is known only from holds; whether it is still needed is decided
by time since its last use, not by which index generation or snapshot names it.

- Every read through the layer holds its file from before the open until the reader is
  done with it; a loaded index holds the files it keeps mapped until it is unloaded. Holds
  are counted in memory per file across every cache in the process, so CNs sharing a
  directory see each other's holds, and do not survive a restart.
- Neither eviction nor collection removes a held file.
- Deleting a file, as a failed check does, takes effect at once, held or not: holders keep
  reading the handles they have open, and the rebuilt file takes its place.
- A file's last use is when it was published or when its last hold was released, as for
  eviction.
- A file with no holds whose last use is older than the idle TTL is deleted by a periodic
  collection in each CN. The TTL is configurable, 1 hour by default; 0 turns idle collection
  off, leaving eviction and the free-space collection below. A hold taken while a file is being collected either keeps
  it or finds it missing and rebuilds it from the rows; a collection never removes a file
  that is held.
- The same periodic collection also watches the free space of the cache's filesystem
  (decision, @cpegeric, 2026-10-29). Below 10% free, it removes files with no holds, least
  recently used first and whatever the TTL, until 15% is free, and checks again every 30
  seconds until it is; it logs once when only held files are left. The cache thus gives
  space back before writes fail, also when something else fills the disk while index
  files sit unused.
- Collection covers every way a file stops being used: a merge or rebuild replacing it, a
  dropped index or table, a write whose transaction aborted, a snapshot no longer queried,
  and files written or loaded on a CN that was down or had the index unloaded when it
  changed. An index needs no code of its own to delete its cached files.

Accepted consequence: a file that is still current but unused for longer than the TTL is
deleted, and its next use rebuilds it from the rows.

Rejected alternative: deleting the files of a generation when a newer one is loaded, and
all of an index's files when it is dropped. It acts only on a CN that has the index loaded
at the current snapshot when it changes, misses files written but never loaded, and
deletes files that queries at an older snapshot keep needing.

### One rebuild per file (decision)

Decided by @cpegeric, 2026-10-24. Concurrent misses of the same file rebuild it once. A
rebuild holds a lock per file name, shared by every CN in the process; other readers of
that file wait for it (and can be cancelled), then find it in the cache. If the rebuild
fails, the next waiter tries. A short wait on the cache's own write coordination is not
enough: a rebuild takes seconds, and a reader that stops waiting would rebuild the file
again.

### Memory

The index-file cache is a layer on top of the existing memory and disk caches, not a
replacement: the assembled file lives in the index-file disk cache, sized and evicted
by its configured capacity, while the chunk data a rebuild reads goes through the
memory and disk caches as today, under the rebuild setting above. What a search keeps
resident (a loaded graph, a GPU index, mapped postings) stays under the existing
index-cache governor. This is how index I/O
shares the CN with SQL instead of claiming its memory.

### Interoperability and compatibility (decision)

The layer reads rows written by the SQL path and writes rows the SQL path can read. A
file can be written by one path and read by the other, on the same CN or across CNs. No
schema, catalog or wire change is involved, so a rolling upgrade needs no capability
gate, and each algorithm can switch its readers and writers independently.

## FULLTEXT2 files (decision)

Decided by @cpegeric, 2026-10-28.

- Each base segment is one file. Each CDC tail frame is one file starting at its first
  chunk, and every frame has a metadata row recording its size and checksum.
- A frame that fits in one chunk is not cached. Its chunk rows are written together with
  the other such frames of the same flush, a bounded number of rows per statement, and are
  read back from the rows, a bounded number of frames per statement, each checked against
  its metadata row. Caching a one-chunk file costs more than reading its row, and a busy
  index writes many of them. On a 25% pmc corpus, the reload after a tail change stayed
  at about 100 ms from 10 to 100 one-chunk frames.
- A tail with frames that have no metadata row, written before every frame had one, is read
  entirely from its chunk rows. A MERGE or REINDEX rewrites it with a row for every frame.

## Scope and non-goals

- In scope: the logical file layer (naming, whole-file write and read, the index-file disk
  cache, the internal table function the write path uses) and its contract.
- Not in scope: adopting the layer in any index other than FULLTEXT2. Each algorithm
  adopts it in its own change, which decides how its files map onto the layer (for example how a CDC tail is
  split into files). When cached files are deleted is the
  layer's (see "Holds and idle collection").
- Not in scope: the incremental-update algorithm; a new on-disk format; changes to what
  clone, restore or backup copy; IVFFLAT, which stores no chunks; GPU residency.
- Range reads of a file. Consumers map or load whole files today, and the disk cache
  serves the whole file.

## Validation

Every statement in "Contract" is checked by a test that would fail if it were false:

- a file written through the layer reads back identically through SQL, through the writing
  transaction before commit, and through another cache after commit; rows written by a
  plain `INSERT` read back identically through the layer;
- a read at a historical snapshot rebuilds a file whose rows were deleted after it;
- a read on a disk-cache miss rebuilds the file from the rows, and a second read is served
  from the disk cache without reading the rows; a cached file is served without its rows;
- the rebuild reads only the file's chunk range: rows of an `index_id` that is a prefix or
  an extension of the file's, chunks beyond the file and neighbouring ranges are excluded,
  for rows in memory and rows flushed to storage objects;
- a missing, duplicated, wrongly sized or corrupted chunk fails the read, and nothing is
  cached; a cache disk that fails while staging a file does not fail the read or the
  write, keeps the bytes already read from the source, and caches nothing, while a source
  error still fails the write;
- a file's CRC32-C combined from its chunks, added in any order, equals the CRC32-C of the
  whole file; a file recorded with an MD5 is verified by MD5, and a mismatch of either
  fails the read;
- with the check on startup configured, a corrupted cached file is detected on the first
  open in the process and rebuilt; without it, the length is checked only; an entry
  missing a file is replaced;
- writing chunk rows that already exist fails with a duplicate-key error, as an `INSERT`
  does; a failed write caches nothing and leaves no registered source or temporary file;
- the table function cannot be called from user SQL, nor by another transaction, nor
  after its write finished, nor for chunks outside the file, nor with an input;
- a file evicted while open stays readable through the open handle and through the path to
  the handle, and the next open is a miss that rebuilds it;
- files are evicted least recently used first: a new file is never the victim while an
  older file is not held, a file published again becomes the most recently used, an opened or released file counts as used, a held file is never
  evicted and stays in the cache's accounting, and after a restart the files reloaded from
  disk are evicted in their order of last use before it and before any file published
  or opened while they load, which skip files removed meanwhile; a hold kept past the reader's
  close holds the file until it is released; a loaded index holds the files it maps until
  unloaded; a held file
  deleted is removed at once, stays readable through open handles, and a file found
  invalid while held is replaced by its rebuild; a hold taken through one cache keeps the
  file from the collection of another cache sharing the directory;
- a file with no holds last used longer than the idle TTL ago is collected and removed from
  the cache's accounting, while a held one, a recently used one and one whose last hold
  was just released stay; below the low-water mark of free space, files with no holds are
  removed least recently used first until the target is reached, whatever their recorded
  last use, a held file stays, and an unknown filesystem is left alone; collection runs periodically until the cache closes, a TTL of 0
  runs no idle collection while free space is still watched, and a negative TTL is rejected; holds, deletions and collections racing on
  the same files leave the directory and the cache index in agreement;
- concurrent readers missing the same file rebuild it once; a cancelled waiter does not
  hold the lock;
- an entry is never visible before it is complete, its files come from one version, and it
  is evicted and deleted as a unit, leaving no directory of its own behind; concurrent
  puts, opens, deletes and evictions leave the directory and the cache index in agreement;
- two cache instances sharing a directory never serve a partial entry, and exactly one
  publishes an entry both write;
- cached entries are reused after a restart, and leftover temporary and trash directories
  are removed;
- the index-file cache gets its configured capacity, or the default when unset, and a
  capacity of zero disables it; the table-data cache keeps its own capacity, and a runtime
  resize changes only that; only a CN's SHARED file service gets the cache; an
  overlapping directory and an invalid rebuild setting are rejected; each rebuild setting maps to its cache policy, and a
  rebuild in the `read` or `none` setting starts no full-file prefetch through the storage
  engine's reader while the default setting does;
- a CN without an index-file cache still serves reads, rebuilding each load from the rows
  into the caller's temporary directory;
- FULLTEXT2 writes one-chunk tail frames as batched chunk rows and larger frames as files,
  every frame with its metadata row, and reads one-chunk frames back in bounded scans;
- a table-data disk cache never treats a directory as a cache entry.
