# File-service cache ownership, eviction and object downloads

Version: v3, 2026-11-07. Scope: [PR #32275](https://github.com/matrixorigin/matrixone/pull/32275) and its follow-up fixes, issue [#32274](https://github.com/matrixorigin/matrixone/issues/32274). Builds on [thrash-aware whole-object prefetch](20261102-thrash-aware-prefetch.md) (#32033, #32034, #32040).

## Review status

The implementation already existed when this design was written. The PR's earlier
reviews found no runtime defect but stopped at the missing versioned design for a
change of cache ownership, concurrency and capacity bounds. This version makes that
contract reviewable; it does not claim an approval that predates it. An independent
design review must approve the design-only commit that adds this document, and the PR
must link that revision, before implementation review resumes. Material deviations of
the implementation from this document require a new version and review.

v2 was approved and merged with #32275. v3 adds the contracts of the fixes that review of
the merged code found: a download keeps its reader's deadline and ends when the disk
cache closes; the cost estimates use only downloads from object storage and range reads
read to the end; reads of a downloading object count toward its read share; an item
larger than the disk cache's capacity is evicted first; and an item is neither evicted
nor counted toward eviction before its insert notification returns.

## Problem

The file service caches object data in three places on a CN: a memory cache of
decoded data, a meta cache of object metadata, and a disk cache of object files and
ranges; the index-file cache keeps whole index files on disk. Before this change the
memory, meta and disk caches shared one generic FIFO cache, and the index-file cache
used an LRU.

- **Ownership.** Callers managed the reference counts of cached values through insert
  and evict callbacks. A missed or doubled callback leaks a value or frees one still in
  use; memory-cache values are manually allocated, so the latter corrupts memory.
- **Visibility.** A key could be found before its value was fully inserted, so every
  caller had to handle half-inserted entries.
- **Capacity.** Under parallel inserts the memory cache stopped evicting and stayed at
  about twice its capacity.
- **Disk admission.** S3-FIFO admission evicts a file unless it is read again soon
  after insertion. TPC-H re-reads its files one full round later, so with TPC-H and
  vector search alternating on a 4 GB disk cache, about twice as many objects were
  read from object storage as with LRU, and TPC-H rounds were about 27% slower.
- **Concurrent readers of a downloading object.** A read that misses the disk cache
  for a whole object downloads it. Other reads of the same object waited up to 200 ms
  for the whole object, then read their ranges from object storage anyway, because a
  large object rarely finishes in 200 ms. The reader that started the download waited
  for all of it.
- **Thrash signal.** A table whose whole-object files were evicted unread switched its
  narrow reads to ranges, and switched back after a quiet period with no such eviction.
  While switched it fills no whole object, so none can be evicted, and the quiet period
  always ran out: after a pause or a phase of other work the table downloaded whole
  objects again only to have them evicted unread.

## Goals and non-goals

Goals: the cache, not its callers, owns value references; a key is never visible
half-inserted; memory use has a stated bound under any concurrency; the disk caches
evict least recently used files; an object being downloaded is fetched from object
storage once, and its readers wait only for their own bytes; a table switches its
narrow reads back to whole objects only with evidence that they fit.

Non-goals: changing what is cached or the on-disk cache format; changing query
results; a general-purpose cache library; tuning the policy per workload at runtime.

## Contracts

### Memory and meta caches: S3-FIFO with cache-owned references (decision)

- **Policy.** S3-FIFO (Yang et al., SOSP '23): a small FIFO for new values, a main FIFO
  for values read again, and a ghost queue of recently evicted keys whose re-insertion
  goes to main. The queues are split over policy shards and keys over index shards;
  every push is ordered across shards and eviction takes the oldest member of any
  shard, so the cache behaves as one S3-FIFO. Ghost entries are accounted as before.
- **Ownership.** The cache retains a value when it inserts it and releases it after the
  value leaves the cache; a Get retains the value for its caller, who releases it.
  Insert and remove notifications only report changes; they never transfer a
  reference. Notifications and releases run outside the cache's locks and may call
  into the cache.
- **Publication.** An insert that succeeds has made its value findable, counted it in
  used bytes and made it evictable, in that order; a concurrent delete or eviction may
  remove it afterwards. An insert that fails cached nothing. A value larger than the
  capacity replaced into an existing key deletes the key. Zero-size values are not
  remembered in the ghost queue.
- **Eviction and bound.** One goroutine evicts at a time. An insert that finds another
  goroutine evicting leaves the eviction to it, unless used bytes exceed the capacity
  by more than a sixteenth, in which case it waits and evicts too. The evicting
  goroutine checks used bytes again each time it stops. Used bytes therefore exceed
  the capacity only while inserts run, by at most a sixteenth plus the values being
  inserted, after the capacity shrinks until the next insert or eviction, or when an
  explicit eviction to a target is cancelled.
- **Lock order.** Evictor, then policy shard, then index shard, then the optional
  charge guard that lets a caller reserve capacity atomically with an insert. A Get
  takes only its index shard.

### Disk caches: sharded LRU (decision)

- The data disk cache evicts least recently used files, as the index-file cache already
  did; both use the same sharded LRU.
- **Order.** Keys are spread over shards, each ordered by a global stamp of last use.
  Eviction removes the evictable item with the oldest stamp across all shards when it
  removes it; an item that becomes evictable, or is inserted as oldest, while an
  eviction scans may be passed over by that scan. Files found on disk at startup are
  inserted as oldest, and so is an item larger than the capacity, which eviction then
  removes before any other; a use of it does not make it newer.
- **Held entries.** Index-file entries that are held are never evicted.
- **Eviction and bound.** Inserts leave eviction to an evictor already running, which
  checks again when it stops, so a burst cannot lose a wakeup. Eviction removes
  bounded batches and runs every removal callback outside all locks, so a callback may
  use the cache; if a callback panics, the remaining removals still complete. An item
  is neither evicted nor counted toward eviction before its insert notification
  returns, so the notifications of one inserted item arrive in order and an item
  larger than the capacity evicts no other while it waits. The
  index of a file and its bytes on disk stay in step through the existing disk-cache
  update protocol.

### Narrow reads switch back only when whole objects fit (decision)

This amends the turning-off rule of the
[thrash-aware prefetch design](20261102-thrash-aware-prefetch.md): a table's signal turns
off after its quiet period only while another whole object fits in the disk cache
beside the cached files and the whole-object downloads in progress. The per-table
choice of which table switches is unchanged. Bytes read from an object while it
downloads count as read from its cached file.

### An object being downloaded is fetched once (decision)

- While the disk cache downloads a whole object, it records how many bytes it has
  written. Every read of the object, including the read that started the download,
  takes the bytes already written instead of fetching them from object storage, and
  returns as soon as its own bytes are written. The rest of the object downloads in
  the background.
- A read whose bytes are not yet written waits if the download will write them no
  more than `disk-download-max-extra-wait` (default 200 ms) later than a range read
  could fetch them. The time to the bytes comes from the download's own rate, or that of
  earlier downloads from object storage; the time of a range read from the latency and
  bandwidth of the range reads the file service read to the end itself.
- A read beyond that margin reads its range at once only when its bytes are at most an
  eighth of what the download still writes before them; a read larger than that would
  fetch again most of what the download is about to write, so it waits instead.
- A wait is bounded by the predicted time plus the margin, after which the read falls
  back to a range read.
- A download that fails after a read got its bytes does not fail that read, and caches
  nothing. A read that waits for its download still fails when its own context ends. Until the read that started a download returns, cancelling it cancels the
  download; afterwards the download no longer depends on it. The download keeps that
  read's deadline and ends when the disk cache closes.
- Ranges read while their object is downloading are not cached separately.
- The cache's synchronous update mode, used only by tests, keeps the old behavior for
  the read that starts a download: it returns once the object is cached.

## Bounds and ownership of resources

| Resource | Owner | Bound |
| --- | --- | --- |
| Memory- and meta-cache values | the cache, plus one reference per Get caller | capacity plus a sixteenth plus values being inserted |
| Ghost queue | the cache | accounted bytes, as before |
| Disk-cache files | disk cache index | capacity, evicted in bounded batches; files being written are counted against room for whole objects |
| Download records | the disk cache, one per object being downloaded | removed when the download is published or fails; at most one per object |
| Downloads continuing after their reader returned | background goroutines | 8 at a time; past that the reader waits for its download |
| Cost estimates | process-wide moving averages | constant size |

## Alternatives

- **Patch the FIFO cache in place.** Caller-managed references and half-inserted keys
  are part of its contract, so patching does not remove the defect class.
- **S3-FIFO on disk as well.** Measured: about twice the object-storage reads and TPC-H
  27% slower with TPC-H and vector search alternating (1 GB memory, 4 GB disk).
- **One lock for the disk LRU.** Simpler, but parallel Get-or-Set took 343 ns at 4
  threads and 706 ns at 12 against 106–115 ns and 143 ns sharded; end-to-end results
  are the same.
- **Decide whole-object prefetch by room alone.** Best after other work evicted the
  tables, but every narrow table reads ranges once the cache is full: warm filtered
  vector queries 2.5–3.5 times slower on caches smaller than the index (details in the
  prefetch design).
- **Readers of a downloading object always wait for it.** Fewest object-storage
  requests (3,373 against 5,365 in one mixed run) but the slowest first filtered round
  (p95 1,146 ms against 854 ms).
- **Readers never wait.** Fastest filtered rounds but about twice the requests (11,193),
  because on local object storage a range read costs about 1 ms.
- **A 50 ms margin.** On TPC-H SF10, two cold rounds at 1 GB memory and 4 GB disk: 53.4 s
  and 4,428 requests against 47.6 s and 2,552 at 200 ms on local object storage, and
  67.7 s and 3,199 against 49.3 s and 2,193 behind an emulated remote store (25 ms per
  request, 100 MB/s per stream). Small reads that share one download wait for it faster
  than each fetches its own range.
- **The duplication bound alone, with no margin.** 58.3 s and 5,926 requests on the same
  TPC-H runs: the reads behind a download are mostly small, about 38 KB, so the bound
  lets them fetch their ranges. It guards wide reads, and the margin decides the rest.
- **Configure the network speed instead of measuring it.** Object-storage latency
  varies with load from tens to hundreds of milliseconds; a fixed value is wrong in
  both directions. The margin expresses a preference, latency against requests, so it
  is configured; the speeds are measured.

## Compatibility, rollout and operations

- No persisted format changes. The disk cache keeps its file layout; existing files are
  indexed as least recently used on startup.
- New configuration: `disk-download-max-extra-wait` in `[fileservice.cache]`, default
  200 ms, the budget the previous fixed wait for a downloading object used. Larger
  values make fewer object-storage requests and slower reads; 0 the fastest reads.
- New metrics: `mo_fs_disk_cache_download_read_total{type}` counts reads of
  downloading objects served from written bytes, after waiting, or by range reads after
  a timed-out or declined wait; `mo_fs_disk_cache_download_declined_bytes_total` counts
  the bytes those range reads fetch again.
- Upgrade is by full shutdown; no mixed-version behavior is involved.
- Fallback: setting the margin to a large value restores waiting for downloads; the
  disk caches' policy and the memory cache's ownership model have no runtime switch.

## Validation and acceptance

- **Model tests.** The S3-FIFO cache is checked against a sequential model after every
  one of 20k random operations (used, small, main and ghost bytes and membership); the
  LRU against a single-list model after every one of 10k operations (full order and
  used bytes).
- **Concurrency.** Used-byte accounting and exactly-once callbacks under concurrent
  operations; eviction order under a concurrent Get; all under the race detector.
- **Downloads.** Reads return their bytes once written, without waiting for the rest;
  fall back when the download fails, is cancelled, or ends short; read the published
  file after publication; decline a download far behind; and the configured margin
  reaches the disk cache.
- **Mutation checks.** Each regression test fails without its fix, including eviction
  from the wrong shard, an evictor that ignores the bound on background downloads, and
  uncounted downloads in the room check.
- **Performance acceptance.** On TPC-H SF10 and wiki_all 1M alternating at 1 GB
  memory and 4 GB disk: object-storage reads and TPC-H time no worse than main, warm
  vector search no worse than #32040 at 0.5/1.5, 1/4 and 1/8 GB, and filtered vector
  search after a TPC-H round faster. Fully cached (14 GB memory, 20 GB disk): no
  regression on TPC-H, vector search or TPC-C.

## Open questions

- The margin is measured on local object storage and behind an emulated remote store,
  not on a real remote deployment. Owner: the PR author, using the new metrics on a
  remote deployment, before changing the default.
- A whole-object download does not reuse ranges of the same object already cached,
  which can store those bytes twice until the ranges are evicted. Non-blocking.
