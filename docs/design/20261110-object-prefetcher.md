# Object prefetcher

Version: v2, 2026-11-13 (v1, 2026-11-10). Scope: [PR #32382](https://github.com/matrixorigin/matrixone/pull/32382), issue [#32381](https://github.com/matrixorigin/matrixone/issues/32381). Builds on [thrash-aware whole-object prefetch](20261102-thrash-aware-prefetch.md) and [file-service cache ownership](20261106-fileservice-cache-ownership.md).

## Review status

The implementation already existed when this design was written. This version makes
its contract reviewable. An independent design review must approve it. Later changes
to the implementation that depart materially from this document need a new version
and a new review.

## Problem

A table scan prefetches the objects it is about to read into the CN disk cache as
whole objects, through the IO pipeline. With a disk cache smaller than the working
set, that path does not schedule the downloads; it only starts them:

- **No order.** A prefetch is started when the scan's window reaches the object,
  whatever else is downloading. The downloads that queue behind it are often the
  ones some scan needs next, and that scan then reads its own ranges.
- **Duplicate bytes.** A read that reaches an object whose prefetch has not started
  yet either waits for it or downloads the whole object itself. A read that takes
  over a prefetch, and the prefetch that then runs anyway, fetch the same bytes twice.
- **No bound.** The prefetches started can add up to more than the disk cache holds,
  so objects are evicted before the scan reaches them.
- **Size lookups.** Splitting a download into parallel ranges needs the object's
  size, which the IO pipeline gets from object storage with one request per object,
  although the catalog already records it.
- **Single-stream downloads.** On object storage, one stream reads a large object
  slowly. In one measurement, a 45 MB object took about 700 ms as one stream and
  about 140 ms as two parallel ranges.

## Goals and non-goals

Goals:
- Each object is downloaded at most once at a time, whoever asks for it.
- The scans set the priority of the downloads. The prefetcher owns scheduling,
  bounds and cancellation.
- The queued bytes are bounded by what the disk cache can keep and what the
  downloads can write in time.
- The object size comes from metadata MO already has.

Non-goals:
- Deciding whether a narrow read prefetches at all. The per-table thrash signal of
  the thrash-aware design keeps deciding that, unchanged.
- Changing what a query returns. Prefetch changes only what is cached and fetched.
- Prefetching metadata or tombstones. They keep their current path.

## Contracts

### The prefetcher is independent of the scans (decision)

The file service owns one prefetcher. A scan only tells it what it will read; the
call queues the request and returns without waiting and without downloading. The
prefetcher runs a fixed number of download workers, scaled to the connections the
file service may hold to object storage. It runs them whether or not the scan that
asked is still waiting for a result. The IO pipeline no longer carries object
prefetches for table scans.

A file service without a prefetcher keeps the IO-pipeline prefetch.

### Prefetch objects (decision 2026-11-13)

Every prefetch the prefetcher runs is one of two kinds of prefetch object:
- A **MemoryPrefetchObject** holds ranges of at most `disk-prefetch-memory-max` bytes in
  all (default 32 MiB, at least 8 MiB). Each run of adjacent ranges is read into a buffer, which readers
  copy their bytes from as they arrive, and is then cached as a range file.
- A **FilePrefetchObject** downloads to a file: an object read whole, or each run of
  ranges of more than `disk-prefetch-memory-max` bytes. Its bytes are written to the
  temporary file that becomes its cache file, which readers read as the bytes arrive; it
  holds no memory buffer.

The choice is made from the request, before any byte is downloaded. Both kinds:
- are joined by every read of their bytes, which waits for its own bytes, not for the
  whole prefetch object;
- wait in the prefetch queue and move to the live queue when a read reaches them before
  they start (below);
- end the same way on a delete of their object: the delete cancels them before it
  removes the object's files, and none is cached after a delete counted since it began.

The prefetcher has two queues of one type, run by the same code with the same number of
workers: the prefetch queue, for downloads ahead of the reads, and the live queue, for
downloads a read waits for. A read that reaches an object whose prefetch object still
waits moves it to the live queue and waits until reads can join it. The prefetch queue
starts nothing while the live queue waits. The live queue keeps a prefetch object whose
scans all ended, as a read waits for it; the prefetch queue drops it.

The buffers of MemoryPrefetchObjects under way hold at most 10% of the memory available
on the node, measured at most once a second, and at least `disk-prefetch-memory-max`. A
prefetch object past that bound waits until it fits, or until none is under way.
Buffers are reused by power-of-two size up to 32 MiB; a buffer is reused only once its
prefetch object ended and no read copies from it.

### What a scan tells the prefetcher (decision)

For each object in its prefetch window, a scan passes:
- the object and its size, from the catalog;
- when the scan expects to reach it, from the scan's position and pace;
- the share of the object's blocks the scan reads;
- for an index row fetch, the ranges of the columns it reads;
- a token for the scan, which ends when the scan closes.

The pace is measured. A scan uses its own pace once it has read enough blocks over
enough time to measure it. Before that it uses the average pace of earlier scans,
which each measured scan updates.

The window stays at least half a window ahead of the scan. It includes the object the
scan reads first, so that read follows the prefetch's download, or reads its own
ranges, like any other read. An object is passed once per scan, with all the blocks
the scan reads in it, even those past the window.

### Download order: share read first, then soonest due (decision)

The queue downloads first the objects of which the reads read the largest share, in
tenths, then those due soonest, then those asked for first. When a scan's share is
not known, the object counts as read whole.

When a second scan asks for an object that is already queued, the two requests merge.
The merged entry keeps the sooner due time and the larger share, and serves both
scans. It is dropped only when both scans have ended.

Share comes before due time because a download of an object read only in part
displaces more useful bytes from the cache. A read that reaches such an object first
loses little by reading its ranges.

### At most one download of an object (invariant)

- An object is never queued while it is queued, downloading or cached.
- A request for an object that is already cached marks the object as used in the disk
  cache's recency order, so eviction removes objects that no scan asked for first. A
  request of ranges does not: a scan that reads only some columns of an object does not
  keep the whole object.
- A read of an object that is being downloaded takes its bytes from the download, as
  the cache-ownership design specifies.
- A read that reaches an object whose prefetch object is still queued moves it to the
  live queue and joins it.
- A full-object read does not wait for another read of the same object to finish. It
  looks in the disk cache again once, because that other read's download may have
  started by then.

### The queue is bounded (decision)

The queued bytes stay below the smaller of:
- half the disk cache's capacity;
- what the download workers can write within a 30-second horizon, at the measured
  download rate.

When a new request does not fit:
- A request larger than the bound is dropped, and the queue is left unchanged.
- Otherwise, entries whose scans have all ended are dropped first.
- Then, if the new request orders before the last entry in download order, that
  entry is dropped. If not, the new request is dropped.

A dropped request costs nothing: the scan's read falls back to the normal read path.

### Scans end; the file service closes (decision)

- When a scan closes, its token ends. A queued download whose scans have all ended is
  dropped before it starts, unless a read waits for it in the live queue. A download that
  has started runs to the end, because another reader may already be using its bytes.
- Closing the file service drops every queued download, cancels the downloads that
  are running and waits for them, bounded by the caller's context. Closing the disk
  cache also cancels running prefetch downloads, and the reads of prefetch ranges,
  which outlive the reads that start them, before waiting for them.

### An index row fetch prefetches only its columns (decision)

A vector or fulltext index search (IVF-Flat, HNSW, IVF-PQ, CAGRA, fulltext, fulltext2)
joins its candidates back to the base table by primary key. That base-table scan, the row
fetch, reads a few rows of each object, and only the columns the query needs from the
base table. The planner marks it; no other scan is marked. The full-table scan a
pre-filter plan copies from it, and the exact branches of adaptive and scalar vector
plans, are not marked. Master and regular secondary indexes are out of scope.

A row fetch never downloads a whole object on a read. Ahead of the scan, for each object:
- It prefetches the ranges of the columns it reads, across all the object's blocks,
  whatever share of the object they are.
- When the object records no column ranges, or stores none of the columns, as for
  columns added after it was written, it prefetches nothing.
- A read that reaches an object before its ranges are downloaded joins their prefetch
  object through the live queue. A read that misses otherwise reads each of its entries
  apart when the range around them holds other bytes.

Every read made under a row fetch's context, including the block pruning's reads, reads
exactly its entries and never a whole object. Object metadata and bloom filter loads read
exactly their extents in every scan: a prefetch range around them would hold the last
columns of the object. Probes that read a few columns of an object to check keys or
visibility, as the CN's insert and upsert dedup, the TN's precommit dedup and MVCC loads,
read exactly their entries and never the whole object. A
row fetch never reads the bytes of a column it does not read, such as the vector column
(decision 2026-11-12). The reads a prefetch object makes for itself join no prefetch
object.

A row fetch exists to skip the columns it does not read, chiefly the vector column. With
a share rule, a table whose scalar columns read outweigh its vector column would download
the whole object, vector column included (decision 2026-11-12). When the columns are most
of the object, the cost is a few requests more than one whole download, for fewer bytes.

The choice depends only on the object and the columns read, so a query makes it the same
way each time.

- An object stores its data column by column: the chunks of one column for every block
  lie one after another, whatever the column's type, and the object's metadata records
  where that run starts and how long it is. Each column is one range per object.
- A request keeps one range per column. Its download skips the columns already cached
  and reads the others that lie next to each other, as consecutive columns of an object
  do, in one read; each read is cached as it is, so later reads of the columns in any
  block of the object are served from the disk cache, as from a whole object.
- A request of ranges is skipped while cached ranges cover them or the whole object is
  cached or downloading. Requests of ranges for one object add up, and a request of the
  whole object takes over a queued request of ranges.
- The index search keeps whole-object prefetch of its own tables.

### Parallel range downloads of whole objects (decision)

- A whole-object download of an object of at least `disk-prefetch-object-split-min`
  (default 16 MiB) reads `disk-prefetch-object-split` parts at once (default 2, at most
  4), aligned to 64 KiB, into one cache file. This happens only when the object's size
  is known and fewer than 4 whole-object downloads are in flight.
- A read-started download splits at the read's first needed block, so that read does
  not wait for the bytes before it.
- A read of the object waits only for the part that holds its bytes.
- The size is the one the prefetch passed. When no prefetch passed a size, the
  download is one stream and makes no size request.
- The last part reads to the object's end, so a wrong size makes the download fail.
  Any failed part fails the whole download, which publishes nothing.
- Whole-object downloads hold at most half of the host's object-storage connections,
  so range reads always keep connections of their own. A download that cannot
  reserve its parts runs as one stream. A download that cannot reserve even one
  connection is skipped, and its read reads ranges.

## Bounds and ownership of resources

| Resource | Owner | Bound |
| --- | --- | --- |
| Queued downloads | the prefetcher | the queue bound above; at most one entry per object |
| Running prefetch downloads | the prefetcher's workers | the worker count; cancelled at file-service or disk-cache close |
| Scan tokens | the scan, which ends its token on close | one per scan; an entry holds tokens only of live scans |
| Object sizes told by prefetches | the disk cache | a fixed number of entries, cleared when full; a size told before a delete of any object is not stored |
| Buffers of MemoryPrefetchObjects under way | the disk cache | 10% of the memory available on the node, at least `disk-prefetch-memory-max`; past it a prefetch object waits and a read reads its own entries |
| FilePrefetchObjects under way | the prefetcher's workers | the worker count; the object-storage connections they hold count with whole-object downloads |
| Deleted objects remembered for cache writes | the disk cache | a fixed number of entries; when cleared, entries read before are not cached |
| Object-storage connections held by whole-object downloads | the disk cache | half the host's connections |

## Alternatives

- **Keep prefetches in the IO pipeline and add ordering there.** The pipeline is
  shared with metadata and tombstone reads, and does not know about scans or the disk
  cache's room. Ordering and bounding downloads belongs where the downloads and the
  cache are.
- **A read takes over a queued prefetch and downloads the whole object.** Measured
  worse. With partial cache on local object storage, the totals of two TPC-H rounds
  were about 52 s against about 47 s for range reads. Taking over also allowed the
  object to be fetched twice.
- **Check that an object is cached without marking it used, for every scan.** The
  objects a scan needed next aged out of the LRU. With partial cache, one traced TPC-H run filled
  54 GB into the disk cache against 29 GB for the parent build, and its first round
  took 49.4 s against 26.7–36.2 s for the parent build.
- **Column ranges for every scan.** A scan that reads every block of an object reads a
  large share of it, and TPC-H's whole-object prefetch is measured; changing it needs its
  own measurement.
- **Whole object for a row fetch whose columns are half the object or more.** One whole
  download costs fewer requests, but downloads the columns the row fetch skips, such as
  the vector column when many scalar columns are read.
- **Prefetch only the blocks a row fetch reads.** A `limit 10` fulltext row fetch reads
  about one block per object, yet 20 such queries downloaded the whole table (38 objects,
  about 2.8 GB on the emulated remote store) to read about 200 rows. Prefetching only
  those blocks serves one query on a cold cache, but concurrent and later queries of the
  same objects then fetch their own blocks again; one whole-object or whole-column
  download serves them all from the cache.
- **Let the thrash signal decide the row fetch's whole objects.** The signal turned the
  base table to range reads only after it had downloaded whole objects, about 140 MB each
  for a read of under 1 MB, and those downloads evicted the entries files. Its column
  ranges are about 0.3% of each object's bytes.
- **Whole-object requests from the index row fetch.** The IVF-Flat row fetch reads about 0.1%
  of each base-table object, yet its requests kept those objects newest. The entries
  table's files, read 30–100%, were evicted instead, and the base table's first eviction
  came 2–3 s later. On local object storage the first filtered round took 16.7–19.6 s
  against 9.2–14.5 s for the parent build; on the emulated remote store the entries table
  also switched to ranges and the second filtered round took 29 s against 12 s. The
  row fetch now prefetches only its columns.
- **Limit split downloads by the connections they hold.** That limit counts
  connections, not downloads, and with the prefetcher running it stopped splitting
  almost every download. The first TPC-H round took 36.8 s, against 23.6–31.9 s when
  the limit counts downloads in flight.
- **Order by due time only.** Downloads objects read in small part ahead of objects
  read whole. Ordering by share first is the developer's decision, for the reason
  given in the order contract.
- **Stat each object for its size.** One extra request per object, which on S3
  costs a request's latency. The catalog already has the size.

## Compatibility, rollout and operations

- No persisted format changes.
- **Configuration** (`[fileservice.cache]`): `disk-prefetch-object-split` (default 2;
  1 turns splitting off), `disk-prefetch-object-split-min` (default 16 MiB) and
  `disk-prefetch-memory-max` (default 32 MiB, at least 8 MiB), the most bytes of a
  MemoryPrefetchObject.
- **Metrics:** `mo_fs_prefetch_object_total{result}` counts prefetch requests and
  their outcomes: queued, merged, cached, downloaded, failed, missed (a read reached
  the object first), ended and dropped. A warning is logged, at most once per
  interval, when object-storage requests wait long for an HTTP connection.
- The HTTP pool allows 128 connections per host, up from 100, which TPC-H reads
  exceeded.
- Upgrade is by full shutdown; no mixed-version behavior is involved.
- **Fallback:** there is no runtime switch for the prefetcher itself. Splitting can be
  turned off by configuration.

## Validation and acceptance

- **Prefetcher contracts**, checked as a black box:
  - download order by share, then due time, then arrival;
  - merging of requests for the same object;
  - no second queue entry for an object that is queued, running or cached;
  - the queue bound, dropping ended entries first, and an oversized request leaving
    the queue unchanged;
  - a read dropping a queued download;
  - an ended scan dropping its downloads;
  - close cancelling running downloads;
  - a prefetch of a cached object protecting it from eviction, except a prefetch of ranges.
- **Downloads:**
  - split downloads read their parts and publish one file;
  - a read waits only for its own part;
  - a wrong size caches nothing;
  - no split while 4 downloads are in flight.
- Each regression test fails without its fix. The new tests pass 100 runs under the
  race detector.
- **Performance acceptance.** The benchmark is TPC-H SF10, then wiki_all 1M, then
  TPC-C. It runs with partial cache (1 GB memory, 4 GB disk) and with full cache
  (14 GB memory, 20 GB disk), on local object storage and behind an emulated remote
  store, interleaved with the parent build. The acceptance bar is no regression
  beyond the variance seen between passes.

## Open questions

- The scan pace starts from the average of earlier scans, which may not fit a scan
  much faster or slower than the others. Non-blocking.
- Range reads of an object queued for prefetch are not tied to its download, so a
  read that reaches many queued objects issues many range reads. Measured faster than
  downloading those objects whole. Revisit on a real remote deployment.
- The prefetcher has no runtime switch. Owner: the PR author, if an operator needs one.
