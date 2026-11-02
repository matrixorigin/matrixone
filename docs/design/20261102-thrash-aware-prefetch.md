# Thrash-aware whole-object prefetch

- Status: draft
- Issue: #32033
- Implementation PR: #32034
- Last updated: 2026-11-02

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

### A scan prefetches whole objects only while the disk cache keeps them (decision)

Whole-object prefetch is a speculation that the rest of the object will be read before it
is evicted. A narrow scan keeps that speculation while the disk cache shows that whole
objects survive long enough to be used, and stops it while the cache shows they do not.
Scans that are not narrow always prefetch, as before.

- A narrow scan that does not prefetch reads the ranges it needs and caches those ranges
  on disk, so a repeated query still hits the disk cache.
- The decision changes only what is cached and fetched, never what a query returns.
- With a disk cache that holds the working set, behavior is unchanged from before this
  design: the signal never turns on.

### Which scans are narrow (decision)

A scan is narrow when the plan knows it reads a small share of each object it touches:

- the base-table scan of an IVFFLAT join back from candidates to rows;
- the IVFFLAT entries search.

The plan marks these scans. The mark is a hint, not a policy: it lets the scan skip whole-
object prefetch, and the disk cache's signal decides whether it does. Other vector and
fulltext joins can adopt the mark when measured.

### The disk cache owns the signal (decision)

Each disk cache instance decides for itself whether it is thrashing, from what it
observes, with no configuration:

- **Turning on.** For each whole-object file, the cache counts the bytes it serves from
  the file between filling it and evicting it. A file's efficiency is those bytes over the
  file size, at most 1. The signal turns on when the moving average of efficiency over
  recent evictions falls below 0.4, after at least 8 evictions. A scan that reads most of
  each object keeps efficiency near 1; thrashed narrow scans use a few percent of each file.
- **Turning off.** The signal turns off after 30 seconds with no evictions, that is, once
  the working set fits again.

Turning off must not use efficiency. While the signal is on, narrow scans stop filling
whole objects, so the files still being evicted are scan fills and write-through, which are
used efficiently. Efficiency then rises although the narrow working set still does not fit,
and a signal that turned off on efficiency would turn on and off repeatedly. Measured: an
efficiency threshold pair of 0.4 and 0.6 still alternated three times in 100 queries;
eviction pressure held the state.

The signal is per disk cache, not per table. A cache where only one table thrashes switches
every narrow scan to range reads. Measured cost at 1 GB memory and 4 GB disk: warm median
unchanged at 17 ms, warm p95 225 ms against 35 ms with the per-scan fixed rule. Finer
signals are left until a workload needs them.

## Rejected alternatives

- **Never prefetch for narrow scans.** Fixes the thrash, but makes the first pass about
  three times slower whenever the objects fit (cold p95 760 ms against 150 ms).
- **Readers never wait for an in-flight prefetch.** A read of an object being filled waits
  up to 200 ms before reading the range itself. Removing the wait recovered 15–30% of the
  latency and raised S3 requests about threefold; the cost is the wasted downloads, not the
  wait.
- **Prefetch an object only when the scan selects a large share of its blocks.** No
  measurable effect once the join back was marked narrow; it also needs per-object block
  counts at prefetch time.
- **Change the eviction policy.** Replaying the measured traces, the eviction policy moved
  S3 bytes by about 1.5× (LRU against S3-FIFO) while the fill unit moved them 13–20×. No
  policy keeps a 3 GB working set through a scan eight times the cache.
- **Bound the S3-FIFO ghost queue by entry count instead of bytes.** Worse in the replay
  (17.9 GB against 11.2 GB of S3 reads).

## Measurements

wiki_all 1M, 768-dim vecf32, IVFFLAT lists=1000, probe_limit=8; filtered query
`select id from w where cat < 10 order by l2_distance(emb, q) limit 10`; single CN on
MinIO; the disk cache is empty at the start of each run. Milliseconds, average (p95).

| Memory / disk | Always prefetch: cold | warm | Never for narrow: cold | warm | Thrash-aware: cold | warm |
|---|---|---|---|---|---|---|
| 0.5 / 1.5 GB | 2632 (5692) | 2443 | 497 (1528) | 320 | 561 (1281) | 158 |
| 1 / 4 GB | 629 (1721) | 571 | 125 (757) | 17 | 165 (420) | 49 |
| 4 / 4 GB | 256 (1458) | 20 | 114 (741) | 3.4 | 102 (420) | 3.5 |
| 1 / 8 GB | 49 (153) | 17 | 124 (755) | 17 | 53 (147) | 17 |
| 4 / 16 GB | 43 (156) | 3.5 | 126 (770) | 3.5 | 40 (133) | 3.5 |

The 0.5 / 1.5 GB and 1 / 4 GB thrash-aware rows use 50 queries per batch; the others 100.
TPC-H SF10 scans are not narrow and do not change.

## Invariants

- Query results do not depend on the signal or the mark.
- A scan that is not marked narrow fills the disk cache exactly as before.
- A narrow scan never fills whole objects while its disk cache signals thrashing, and
  always caches the ranges it reads instead.
- The signal turns off only after a period with no evictions.
