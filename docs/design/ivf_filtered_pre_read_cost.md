# Filtered IVF relation scan read policy

## Scope and evidence

Issue #29531 reports a filtered PRE throughput gap between main and 4.2.
This change is based on main `c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`
and addresses a query reader policy discontinuity. It does not establish the
root cause of the complete reported gap.

The typed relation scanner bypasses the ordinary SQL scan adapter. Its readers
can prefetch whole data objects before applying an exact filter. The reader
also discards context FileService policy, and the early scalar filter loader
hardcodes policy zero. Filtering few rows consequently does not guarantee
that unrelated object bytes avoid prefetch/cache admission.

Logical extent sizes and cache-hit counters do not measure actual I/O or
actual decompression. A legacy LZ4 extent can require full decompression even
for one selected row. Suppressing prefetch does not eliminate that cost.

## Ownership and implementation

| Owner | Contract |
| --- | --- |
| `scanEntriesInDomain` | For membership or scalar residual filters, request `SkipFullFilePreloads`. Metadata, centroids, and unfiltered entries retain their default policy. |
| `RelationScanRequest` | Carry a value `ReadPolicy`; no new execution interface or persistent state. |
| `relationScanner.ScanRelation` | OR the request with inherited FileService flags in a local context. Never mutate `proc.Ctx`. |
| `readutil.Reader` | Initialize from the read context. Above the existing block threshold, OR `SkipMemoryCacheWrites`; preserve other flags and the threshold boundary. |
| `blockio.ReadDataByFilter` | Pass the same explicit policy to both cached-search and general scalar-column loaders. |
| `LocalDisttaeDataSource.Next` | Skip data-object prefetch when requested; retain the existing ordinary scan behavior. |
| `RemoteDataSource` | Skip only data-object prefetch; retain tombstone prefetch and batch cursor advancement. |

Existing merge readers already forward context to their children; they need no
new policy field or setter. Both capable and fallback readers consume the same
request. `PostFilterTopOnly` cannot identify a filtered scan: unfiltered DESC
and unsupported distance expressions also use it. Passing a distance function
to `SetOrderBy` is invalid because the data-source order adapter expects a
column reference.

## Invariants and limits

- No changes to write, flush, merge, writer policy, object encoding, chunk
  size, storage layout, catalog, or existing stored data.
- Preserve exact membership, `MustApply`, `ForceOneCN`, snapshots and tombstones,
  probe count, candidate budget, distance qualification, recall, and ties.
- Preserve necessary scalar/function/cast/type and distance-bound fallbacks;
  no duplicate ranking, decoder, cache, allocator, or scheduling path.
- Existing full-object disk cache files remain usable by range reads. Existing
  decoded memory hits retain their behavior.
- Dense reads can save few bytes while issuing more range requests. Measure
  this control; a filtered request is not proof of lower latency.
- The proposed no-write shared-decoder/allocator extension is excluded. It
  lacked a relevant query witness and a complete peak-memory bound. The
  original 31-block diagnosis does not reach the 1024-block threshold.

## Validation

Reuse existing fixtures and expected BVT results. Required unit evidence:
request classification; inherited flags and caller-context isolation; real
reader threshold equality and greater-than boundaries; both early scalar
loader branches with visibility/tombstones; local three/four-range controls;
remote data prefetch suppression with tombstone and cursor preservation.
Observe terminal prefetch calls for positive controls. Negative controls use
an isolated service without an IO pipeline, so an erroneous submission fails
synchronously; pipeline shutdown alone cannot prove that nothing was queued.

Run affected normal and race UT and incremental repository SCA. Run existing
vector BVT twice with comparison and teardown, including membership, INCLUDE,
prepared ranges, quantized/narrow types, nulls, DML synchronization and Top-K
consumers. Independent QA must report failures and uncovered scenarios.

Benchmark versions sequentially against identical logical data and explicit
resource limits; main before/after must use the same stored objects. Separate
cold object reads, compressed disk hits, and decoded memory hits. Record actual
bytes/requests, results, plan domain and budget, repeated latency/throughput,
and variation. A small local test cannot close the original Wiki10M gap.

## Review and rollback

This document retains only Q1 from the Astra xhigh v2 proposal, independently
approved by GPT-6.1-sol xhigh. Reviewed proposal SHA256:
`c6c95bca15aacc79baeef7a87deaf1a5dba5b44a4095013a447acbf7f5d43d56`.
The excluded proposals are not delivery dependencies. There is no migration:
reverting the query change restores the previous reader behavior.
