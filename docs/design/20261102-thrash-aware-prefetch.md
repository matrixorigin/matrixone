# Thrash-aware whole-object prefetch

- Status: draft
- Issue: #32033
- Implementation PRs: #32034, #32040, #32275
- Last updated: 2026-11-06

## Problem

A table scan prefetches every object it is about to read into the CN disk cache as a
whole object. For a scan that reads most of each object this is the fastest way to fill
the cache: one streamed download per object instead of many range reads. For a scan that
reads a few rows from each object it can be the slowest:

- An IVFFLAT query that filters or projects a column outside the index joins the
  candidates back to the base table. Each candidate lands in a different block, so the
  join touches tens of objects and reads a few rows from each.
- The IVFFLAT search itself reads `nprobe` lists from the entries table; each probe reads
  one or two blocks of an object that holds many lists.

When the whole objects these scans touch fit in the disk cache, prefetch warms the cache
in one pass and later queries hit it. When they do not fit, every query downloads whole
objects that are evicted before they are read again. On wiki_all 1M (both tables about
6.2 GB as whole objects) with a 4 GB disk cache, 91 objects were downloaded 2,017 times
for 750 queries, about 325 MB per query to read about 2 MB of the base table, and a
filtered query took 500–800 ms instead of 17 ms. At larger scales, such as 88M vectors,
the disk cache can never hold the whole objects, so these scans are always in this state.

Neither fixed rule is right. Always prefetching is the problem above. Never prefetching
for narrow scans makes the first pass about three times slower whenever the objects do
fit: measured cold p95 760 ms against 150 ms with 8 GB and 16 GB disk caches.

## Contract

### A narrow read prefetches whole objects only while its table's objects survive (decision)

Whole-object prefetch is a speculation that the rest of the object will be read before it
is evicted. A narrow read keeps that speculation while the disk cache shows that its
table's whole objects survive long enough to be used, and stops it while they do not.
Reads that are not narrow always prefetch, as before.

- A narrow read that does not prefetch reads the ranges it needs and caches those ranges
  on disk, so a repeated query still hits the disk cache.
- The decision changes only what is cached and fetched, never what a query returns.
- With a disk cache that holds the working set, behavior is unchanged from before this
  design: no table's signal turns on.

### Which reads are narrow (decision)

A read is narrow when the plan knows it reads a small share of each object it touches:

- the base-table scan of an IVFFLAT join back from candidates to rows, with or without a
  pre-filter; the pre-filter's own scan of the base table reads the whole table and is
  not narrow;
- the IVFFLAT entries search.

The plan marks these scans. The mark is a hint, not a policy: it lets the scan skip whole-
object prefetch, and its table's signal decides whether it does. Other vector and fulltext
joins can adopt the mark when measured.

### The disk cache keeps one signal per table (decision)

Each narrow read tells the disk cache which table it reads for. The cache maps each
object a narrow read touches to that table, and keeps one signal per table, from what it
observes, with no configuration:

- **Efficiency.** For each whole-object file, the cache counts the bytes it serves from
  the file between filling it and evicting it. A file's efficiency is those bytes over the
  file size, at most 1. Files no narrow read touched belong to no table and do not count.
- **Turning on.** A table's signal turns on when the moving average of efficiency over the
  evictions of its files falls below 0.1 after at least 4 evictions, or below 0.4 after at
  least 16.
- **Turning off.** A table's signal turns off after a quiet period with no eviction of its
  whole-object files, once another whole object fits in the disk cache beside the cached
  files and the whole-object fills in progress. The period is 30 seconds, doubled up to
  10 minutes each time the signal turns on again soon after it turned off.

Two tables that evict each other's whole objects both look thrashed at first, and the one
that switches first decides the outcome: once it reads ranges, the other often fits. The
two thresholds make the table that uses its whole objects least switch first. On the
measured workload the join back reads under 0.1% of each base-table object and the entries
search about 20% of each entries object; when the entries table switched first, the base
table fit as whole objects and warm queries took 113 ms instead of 25 ms.

Turning off must not use efficiency. While a signal is on, that table stops filling whole
objects, so the files still being evicted are used efficiently, and a signal that turned
off on efficiency would turn on and off repeatedly. Measured with one signal for the whole
cache: an efficiency threshold pair of 0.4 and 0.6 still alternated three times in 100
queries. A table that stops being evicted after it switches, such as an entries table that
fits once the base table reads ranges, turns off and prefetches again.

The quiet period alone is not evidence that a table fits: while its signal is on, the
table fills no whole object, so none of its files can be evicted, and its quiet period
always runs out. Without the room condition, a table that still did not fit turned off
after every pause or every phase of other work, such as a TPC-H round, and downloaded at
least four more whole objects that were evicted unread before turning on again. The
room condition keeps the signal on while the cache is full, and lets it turn off as soon
as whole objects fit, after the cache grows or cached files are deleted. The LRU disk
cache evicts only to make room, so files do not age out of a full cache.

The signal is per table because tables in one cache need different answers. One signal
for the whole cache switched the entries search to range reads too: warm p95 225 ms against
60 ms per table, at 1 GB memory and 4 GB disk.

### An object being downloaded is fetched once (decision)

While the disk cache downloads a whole object, every read of that object takes the bytes
the download has written instead of fetching them from object storage, including the
read that started the download. A read returns as soon as its own bytes are written;
the rest of the object keeps downloading in the background. Before, the read that
started the download waited for the whole object, and other reads waited up to 200 ms
for the whole object and then read their ranges from object storage anyway.

- A read of bytes not yet written waits for them if the download will write them no
  more than `disk-download-max-extra-wait` (default 200 ms) later than a range read
  could fetch them, or if they are more than an eighth of what the download still
  writes before them; otherwise it reads its range at once. The time to the bytes comes
  from the download's rate, and the time of a range read from the latency and bandwidth
  of the range reads observed. A wait that runs over is abandoned for a range read.
- Larger values make fewer object storage requests and slower reads, 0 the fastest
  reads. With local object storage, where a range read takes about 1 ms, values from
  20 ms to 300 ms changed under 2% of the reads of downloading objects; on remote object
  storage a range read costs 10 ms or more and the margin decides more reads.
- A download that fails after a read got its bytes does not fail the read; it caches
  nothing. Until the read that started a download returns, cancelling it cancels the
  download. The downloads that continue after their reader returned are bounded; past
  the bound a reader waits for its download.
- Ranges read while their object is being downloaded are not cached separately; the
  download caches the whole object.
- The disk cache's synchronous update mode keeps the old behavior for the read that
  starts a download: it returns once the object is cached.

## Rejected alternatives

- **Never prefetch for narrow scans.** Fixes the thrash, but makes the first pass about
  three times slower whenever the objects fit (cold p95 760 ms against 150 ms).
- **Readers never wait for an in-flight prefetch.** A read of an object being filled waits
  up to 200 ms before reading the range itself. Removing the wait recovered 15–30% of the
  latency and raised S3 requests about threefold; the cost is the wasted downloads, not the
  wait.
- **Treat any read that selects at most half of an object's blocks as narrow.** Covers
  primary-key and point lookups without plan marks, but also the TPC-H joins whose
  runtime filters select a few blocks per object. When those large tables thrash they
  switch to range reads: TPC-H SF10 round one took 64 s against 46 s, with 9,678 S3
  requests against 3,362, at 1 GB memory and 4 GB disk.
- **One signal per disk cache.** Switches every narrow read when one table thrashes;
  warm p95 225 ms against 75 ms per table at 1 GB memory and 4 GB disk.
- **One signal per object.** An object is skipped only after its own whole file was
  evicted unused, so a cache that holds few of the objects downloads each of them whole
  before switching. Measured worse than one signal per cache when the cache is small:
  warm 271 ms against 149 ms at 0.5 GB memory and 1.5 GB disk.
- **Decide by room alone.** A narrow read fills a whole object only when it fits, with no
  per-table signal. It is the best after other work evicted the tables (filtered queries
  after a TPC-H round 4.3–5.6 s per 100 against 7.8–8.6 s per table, at 1 GB memory and
  4 GB disk), but every narrow table reads ranges once the cache is full, including an
  entries table whose whole objects would be reused: warm filtered queries settled at
  52–71 ms against 20 ms per table at 1 GB memory and 4 GB disk, and 134–171 ms against
  44–50 ms at 0.5 GB memory and 1.5 GB disk.
- **Room alone, except tables whose whole objects are mostly read.** Close to room alone
  after a TPC-H round (7.0 s per 100), and equal to one signal per table warm at 1 GB
  memory and 4 GB disk, but an entries table that cannot fit at 0.5 GB memory and 1.5 GB
  disk still reads about 40% of each object before its eviction, keeps filling whole
  objects and thrashes: 174–270 ms warm.
- **Change the eviction policy.** Replaying the measured traces, the eviction policy moved
  S3 bytes by about 1.5× (LRU against S3-FIFO) while the fill unit moved them 13–20×. No
  policy keeps a 3 GB working set through a scan eight times the cache.
- **Bound the S3-FIFO ghost queue by entry count instead of bytes.** Worse in the replay
  (17.9 GB against 11.2 GB of S3 reads).

## Measurements

wiki_all 1M, 768-dim vecf32, IVFFLAT lists=1000, probe_limit=8; filtered query
`select id from w where cat < 10 order by l2_distance(emb, q) limit 10`; single CN on
MinIO; the disk cache is empty at the start of each run. Milliseconds, average (p95).

| Memory / disk | Always prefetch: cold | warm | Never for narrow: cold | warm | One signal per cache: cold | warm | One signal per table: cold | warm |
|---|---|---|---|---|---|---|---|---|
| 0.5 / 1.5 GB | 2632 (5692) | 2443 | 497 (1528) | 320 | 516 (1161) | 149 (413) | 537 (1324) | 126 (407) |
| 1 / 4 GB | 629 (1721) | 571 | 125 (757) | 17 | 214 (602) | 55 (225) | 128 (370) | 24 (77) |
| 4 / 4 GB | 256 (1458) | 20 | 114 (741) | 3.4 | 102 (420) | 3.5 | — | — |
| 1 / 8 GB | 49 (153) | 17 | 124 (755) | 17 | 102 (310) | 16 | 84 (304) | 16 |
| 4 / 16 GB | 43 (156) | 3.5 | 126 (770) | 3.5 | 40 (133) | 3.5 | — | — |

The two signal columns use 50 queries per batch; the others 100. At 1 / 4 GB only the base
table's signal turns on; at 0.5 / 1.5 GB both tables' signals do. TPC-H SF10 scans are not
narrow and do not change.

With the room condition and early return (#32275), same protocol, 50 queries per batch,
then a 35 s pause and 50 more; milliseconds, average (p95):

| Memory / disk | Before: cold | warm | after pause | After: cold | warm | after pause |
|---|---|---|---|---|---|---|
| 0.5 / 1.5 GB | 117 (311) | 46 (98) | 110 (602) | 107 (231) | 42 (77) | 41 (84) |
| 1 / 4 GB | 92 (303) | 18 (42) | 52 (187) | 94 (301) | 18 (43) | 16 (37) |

Alternating TPC-H SF10 rounds with 100 filtered queries at 1 GB memory and 4 GB disk, the
filtered queries after a TPC-H round took 7.8–8.6 s per 100 (p95 about 575 ms), against
8.2–17.8 s (p95 353–835 ms) before. Early return alone cut the p95 of the first filtered
round from 898 ms to 455 ms.

Reading downloading objects from their download, same alternating workload at 1 GB memory
and 4 GB disk, medians of 4 runs: 5,054–4,135 object storage requests in the TPC-H rounds
for a max extra wait of 50–200 ms, against 4,508 when other reads waited up to 200 ms for
the whole object; filtered queries after a TPC-H round 5.7–6.8 s per 100 (p95 174–229 ms)
against 9.5 s (p95 641 ms). Always waiting for the download made 3,373 requests in all but
had the slowest first filtered round (p95 1,146 ms); never waiting made 11,193.

## Invariants

- Query results do not depend on the narrow mark or the signals.
- A read that is not narrow fills the disk cache exactly as before.
- A narrow read never fills whole objects while its table's signal is on, and always
  caches the ranges it reads instead.
- A table's signal turns off only after a period with no eviction of its whole-object files,
  and only while another whole object fits in the disk cache.
- The cache's record of tables and objects is bounded. A table idle for twice its quiet
  period may be forgotten; a table whose signal is on is kept, and a table the cache
  cannot record is treated as having no signal.
