# HNSW quantization, narrow vector columns and exact prefiltered search

Branch `hnsw_quantization`. Every claim in "Claims" names the black-box test that proves it.
The black-box suite is `test/distributed/cases/vector/vector_hnsw_quantization.sql`; each of
its sections is headed by the claim it proves (`-- C<n>`).

Status: proposed.

## Problem

1. An HNSW index stores every vector at the width of its column. A `vecf32` index costs four
   bytes per element in memory and on disk, and there is no way to trade precision for a
   smaller graph, as IVF-Flat and IVF-PQ already allow with `QUANTIZATION`.
2. HNSW indexes only `vecf32` and `vecf64` columns. A `vecf16`, `vecbf16`, `vecint8` or
   `vecuint8` column cannot be indexed, although usearch stores those element types natively.
3. A prefiltered search (`mode=pre`) walks the graph and skips nodes that fail the filter.
   When few rows qualify, the walk can end before it reaches them and the page comes back short
   or with worse neighbours, while a scan of the qualifying rows alone would be cheap and exact.

## Goals

1. `QUANTIZATION` on HNSW, with the same option name and the same rules as IVF-Flat / IVF-PQ.
2. Index the narrow vector column types directly.
3. A prefiltered search over few qualifying rows is exact, with a session-level bound on
   "few".

## Non-goals

- Per-dimension or per-generation quantization ranges. One range serves the whole index.
- Changing the quantization of an existing index in place. Drop and recreate it.
- Product quantization or other lossy codecs usearch does not store natively.
- int8 / uint8 with inner product or cosine: an affine map does not preserve them.

## Design decisions

**D1 — MO converts, the library stores.** Every vector and every query is converted to the
index's element type before the HNSW library sees it, exactly as IVF-Flat and IVF-PQ do. The
library never receives a type other than the one the index stores. This keeps one conversion
rule per type, shared by build, incremental sync and search. A finite element the
stored type cannot hold (for example above 65504 for `float16`) is rejected, at build, sync
and search, instead of being stored as infinity.

**D2 — The element type is chosen once.** `QUANTIZATION` is fixed at `CREATE INDEX`.
Omitted, the index stores the column's own element type. REINDEX keeps it and rejects a new
value: an index created before this change has no quantization to merge with, and changing
the element type is a rebuild with different semantics, which is what DROP + CREATE says.

**D3 — No widening.** A quantization wider than the column element is rejected: it adds
memory without adding information. `float64` is valid only on a `vecf64` column.

**D4 — int8 / uint8 use one trained affine range per index.** For a float column, the build
samples the column (the same sampling IVF-Flat uses: a percentage sample bounded by a row
limit, sized so the sample holds about two million elements; NULL vectors excluded) and takes
a clipped range (0.1 / 99.9 percentiles) over all elements. Each element `x` is stored as
`round(x·mul + add)` clamped to the integer range. The map is the same for every dimension,
so L2 ordering is preserved up to rounding and the stored distance is the true distance scaled
by `mul²`; search divides it back, so reported distances are in the column's units.
A `vecint8` / `vecuint8` column needs no training: its range is its type's range.

**D5 — The range is persisted with the index.** Every model row of the index's metadata table
carries the range. Incremental sync converts new rows with the range of the existing index; an
index created on an empty table trains its range from the first rows it syncs. REINDEX
retrains from the current data. A cluster upgrade adds the two nullable range columns to the
metadata table of every existing HNSW index; a NULL range means "not an int8/uint8 index".

**D6 — Out-of-range values saturate.** A value outside the trained range is clamped to the
nearest end. Such rows are still indexed and returned, but cannot be ordered against each
other or against rows at that end of the range. REINDEX after the data distribution changes
restores the ordering.

**D7 — Exact prefiltered search below a threshold.** When a prefiltered search knows the
exact number of qualifying rows and it is at most `hnsw_prefilter_exact_threshold`, the index
search computes the distance to every qualifying row instead of walking the graph. The
threshold is a session variable, range `[0, 1e9]`, default 5000; `0` disables exact search.
The decision uses the qualifying row count, not a selectivity estimate: it bounds the number
of distances an exact search computes. The search still tests every indexed row against the
filter, a membership lookup per row with no distance, so its cost also grows with the index
size. The threshold and its value are shown in `EXPLAIN VERBOSE` of a prefiltered search.

## Claims

| # | Claim | Black-box proof (`-- C<n>` section) | Unit proof |
|---|-------|-------------------------------------|------------|
| C1 | `QUANTIZATION` accepts `float32`, `float16`, `bf16`, `float64`, `int8`, `uint8`, case-insensitively, and `SHOW CREATE TABLE` reports it; any other value is rejected naming the supported set. | C1 | `TestHnswValidQuantization`, `TestHnswParamsFromTreeQuantization` |
| C2 | Omitted, the index stores the column type; `vecf16`, `vecbf16`, `vecint8`, `vecuint8` columns are indexed directly and searched through the index. | C2 | `TestNarrowColumns`, `TestHnswVectorWidensNarrowColumns` |
| C3 | A quantization wider than the column element is rejected (`float64` on `vecf32`, `float32` on `vecf16`, `float16` on `vecint8`/`vecuint8`); equal or narrower is accepted. | C3 | `TestHnswCheckQuantizationBase` |
| C4 | `int8` / `uint8` are accepted with `vector_l2_ops` and `vector_l2sq_ops` and rejected with `vector_ip_ops` and `vector_cosine_ops`. | C4 | `TestHnswValidQuantization` |
| C5 | For every quantization, when neighbour gaps exceed the quantization step, the index top-k equals the exact top-k of the same rows with no index. | C5 | `TestQuantizedModelSearch`, `TestStorageQuantization`, `TestToStorage` |
| C6 | Distances reported by an int8 / uint8 index are in the column's units, within the quantization error of the exact distance. | C6 | `TestQuantizedModelSearch` (distance units) |
| C7 | Rows added or deleted after `CREATE INDEX` are synced into the int8 index with its range; an int8 index created on an empty table is usable once its first rows sync. | C7 | `TestSyncQuantizeBounds`, `TestQuantizedBuildUsesBounds`, `TestSampleQuantizeBounds` |
| C8 | A row outside the trained range is indexed and returned, saturated to the end of the range. | C8 | — |
| C9 | REINDEX keeps the quantization, rejects a new one, accepts other options, and retrains the int8 range so a previously saturated row is ordered again. | C9 | `TestValidateReindexParams_RejectsMerge` |
| C10 | Operations that rebuild or copy the index — copy ALTER, CLONE, TRUNCATE followed by inserts — keep the quantization and serve searches through the index. | C10 | — |
| C11 | A prefiltered search reports the exact-search threshold in `EXPLAIN VERBOSE` when it is positive and omits it when it is `0`; values outside `[0, 1e9]` are rejected. | C11 | `TestExplainSettings`, `TestApplyIndicesForSortUsingHnswKeepsFiltersOnScan`, `TestHnswIterativeWidens` |
| C12 | A prefiltered search whose qualifying rows are within the threshold returns the exact top-k of those rows, also on a sparse graph (small `m`, `ef_search`). | C12 | `TestFilteredSearchMembershipExact` (a sparse graph where the graph walk misses rows the exact search finds), `TestHnswSearchMembershipExactThreshold` |
| C13 | An existing HNSW index keeps working after the upgrade that adds the range columns, and the upgrade is idempotent. | — (needs a pre-upgrade cluster) | `TestUpgradeHnswMetadataQuantize*`, `TestListHnswMetadataTables`, `TestColumnsUpgradeReachesExistingTenants`, `TestLoadMetadataReadsBounds`, `TestMetadataSqlCarriesBounds` |
| C14 | A finite vector element the stored type cannot hold fails the build or the search instead of being stored or compared as infinity. | C14 | `TestToStorage` |

## Compatibility

- On-disk: an index created before this change has no range columns until the upgrade adds
  them; reading tolerates either shape, and a NULL range is the pre-change meaning.
- Search results of existing (unquantized `vecf32` / `vecf64`) indexes are unchanged, except
  that a prefiltered search within the threshold is now exact.
- Upgrade is shutdown-only; no wire or protocol gate is added.

## Risks

- int8 / uint8 reduce recall when neighbour gaps are below the quantization step; this is the
  documented trade, as for IVF-Flat / IVF-PQ.
- A range trained on a sample can differ between two builds of a large table, so two REINDEXes
  of the same data can order near-ties differently. Small tables are sampled whole and are
  deterministic.
- An exact prefiltered search costs one distance per qualifying row, which the threshold
  bounds, plus one filter test per indexed row, which it does not. A filtered graph walk
  that finds fewer than `ef_search` qualifying rows visits the whole graph, so for few
  qualifying rows exact is cheaper at any index size; the walk becomes cheaper once
  qualifying rows exceed roughly 30–85 × `ef_search`, nearly independent of index size and
  dimension. The default sits at that crossover for the default `ef_search`; a query with a
  much larger `ef_search` can raise the threshold.
