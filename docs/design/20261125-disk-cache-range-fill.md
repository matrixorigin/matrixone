# Disk cache range fill

Version: v1, 2026-11-25. Builds on [object prefetcher](20261110-object-prefetcher.md),
[index objects in the index-file cache](20261111-index-objects-in-index-file-cache.md) and
[file-service cache ownership](20261106-fileservice-cache-ownership.md).

## Review status

Draft for the developer's review. No implementation follows until it is approved.

## Problem

The disk cache downloads bytes of an object in two ways that are one mechanism underneath
and two everywhere else:

- A **whole-object download** writes the object to a temporary cache file while readers
  wait on its offsets, then publishes it as the object's whole file.
- A **range prefetch object** downloads a range of the object the same way, into a
  temporary file or a memory buffer, then publishes it as a range file.

The bytes in flight are written the same way, but the two are registered apart, found by
reads through different lookups, chosen between by different rules, failed and stopped
by different code, published under different names, and deleted, evicted and loaded
along different paths. Every place that must treat them alike has two copies to keep in
step, and most of the review findings on the disk cache came from those copies
disagreeing: a whole download taken as covering another cache's request, a read that
declined one kind and then waited on it, a stopped download picked over a live one,
errors classified one way on one path and another way on the other, a split limit
checked apart from the registration it guards.

## Goals and non-goals

Goals:
- One kind of fill for every byte the disk cache downloads: a **range fill** of an object.
- One registry of the fills under way, one rule by which a read joins one, and one
  cached form of what they publish.
- The same bytes and the same errors for every read as today.

Non-goals:
- Removing memory-backed fills. A range up to the memory limit is still held in memory
  while in flight; that is a parameter of the fill, not a second kind.
- Merging the index-file cache into the disk cache. Fills still publish into either
  cache; the cache is part of the fill's identity.
- Changing column-range prefetch, the prefetcher's queue, async publication, trash or the
  connection budget. Those keep their own designs; this one only changes what they
  start, find and publish.

## Architecture

A **range fill** downloads the bytes `[start, end)` of one object into one cache, and is
identified by the object, the cache, and the range. A whole object is the fill of
`[0, size)`; when the size is not known, of `[0, end of object)`, which records the size
it finds.

Every fill has the same parameters, chosen when it is made, before any byte is
downloaded:

| Parameter | Values | Chosen by |
|---|---|---|
| Backing | memory buffer, or temporary cache file | the range's size against the memory limit, as today |
| Parts | one, or several read at once | the split policy (below) |
| Cache | disk cache, or index-file cache | the request's tier, as today |
| Starter | a read, a prefetch, or the write path caching bytes it holds | who makes it |

and the same life: it is made, waits holding nothing for its buffer or file slot and its
object-storage connection, registers so reads can join it, downloads, publishes, and
ends. A fill that finds, once it may register, a fill of its cache holding its bytes
joins that one instead.

## Contracts

### One registry (decision)

All fills under way are registered in one place, by object, cache and range. A read, a
prefetch, a delete and close look there and nowhere else. There is no second registry of
whole-object downloads.

### At most one fill of the same bytes into the same cache (invariant)

- A fill is not registered while a fill of the same cache already holds its bytes; it
  joins that one.
- Fills of one object into different caches are independent: neither covers, joins or
  takes the place of the other, and they download one after the other when they hold
  the same bytes.
- The prefetcher's rules for not queueing what is queued, downloading or cached keep
  holding, with "downloading" meaning a registered fill of that cache.

### How a read joins (decision)

A read that misses the cache joins a registered fill of its own cache that holds the
bytes it needs, whatever the fill's backing, parts or starter, and waits until its own
context ends or the fill's source stops sending bytes. Otherwise it starts a fill of its
own, or reads object storage without caching, as today. There is one rule for which fill
a read may join, applied the same way at every place a read looks:
- a read that caches what it reads joins only a fill that ends with its bytes cached;
- a read that caches nothing may join any fill of its cache;
- a fill's own reads of object storage join no fill;
- a streamed entry larger than the memory limit opens its own reader.

### Failure (unchanged contract)

A fill's failure means the same for every read joined to it, whatever kind of fill it
was: a failed read of object storage fails the reads waiting on bytes it did not write; a
fill that is canceled, stops for lack of bytes from its source, or ends short of its
range leaves its readers to read their own bytes. A fill never makes a read wait past the
read's own context.

### Split policy (decision)

Splitting a fill into parts is a property of any fill large enough, not of whole objects
only. The existing limits apply unchanged: a minimum size, a number of parts, a limit on
fills in flight, alignment, and connections taken part by part. Whether ranges smaller
than a whole object split is the developer's decision before implementation; until then,
only fills of whole objects split, as today.

### Publication and the cached form (decision)

A fill publishes one cache file per part, under the range name of the bytes it holds, and
indexes it like any range. A whole object is the range `[0, size)`: there is no separate
whole-file name. A read finds cached bytes in one index, whether a whole object, a range,
or several ranges that together cover its bytes.

### Deletes and eviction

A delete of an object stops every fill of the object in both caches, then removes every
cached range of it, then forgets them, in that order; a range whose file is not removed
stays indexed until it is. Eviction removes ranges; a whole object is one range.

### Startup (decision 2026-11-25)

Cache files left by an earlier version under the separate whole-file name are not read:
startup removes them as stale cache. The cache refills them on use.

## Bounds and ownership of resources

Unchanged from the object-prefetcher design: memory-backed fills count against the flight
memory budget, file-backed fills against the disk cache, every fill's parts against the
download connections. What changes is only that each bound is charged in one place,
because there is one kind of fill.

## Alternatives

- **Keep both kinds and share more helpers.** The read-path refactor of PR #32812 did
  this for eligibility, error classification and fallback; the review findings kept
  coming from the places the two kinds still differ, so sharing helpers is not enough.
- **Make every fill file-backed.** Rejected: memory-backed fills of small ranges are
  required for scans that read column ranges.
- **Store index objects in the disk cache.** Rejected: the index-file cache is required
  as a separate cache.

## Compatibility, rollout and operations

- No persisted data format changes; only the disk cache's own file layout does.
- After an upgrade, whole objects cached by the earlier version are removed at startup,
  so the first reads of them download them again. Range files keep their names and stay.
- Configuration, metrics and the connection budget are unchanged. If splitting is later
  extended to ranges, the existing split settings apply to them.
- Upgrade is by full shutdown; no mixed-version behavior is involved.

## Validation and acceptance

- Every existing fileservice test, and the review tests of PR #32812, pass unchanged,
  except tests that observe the removed whole-file name.
- A test for each contract above, including joins across backings and starters, fills of
  one object into both caches, a delete during each phase of a fill, and the startup
  removal of old whole files.
- TPC-H SF100 on two CNs, cold and warm, at least as fast as before the change, against
  the baseline measured on the same cluster before work began.
- A fresh independent review of the change finds none of the bug families listed in the
  problem statement.

## Open questions

- Whether ranges smaller than a whole object may split into parts.
- Whether this lands in PR #32812 or as a follow-up PR.
