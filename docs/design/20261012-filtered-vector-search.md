# Filtered vector search: pre, post and auto for every vector index

Owner issue: #28988. Branch `bug_28988`, delivered as PR1–PR7 below. Design revision 1,
2026-10-12, based on main `cd8d5abf96` (IndexSearchScan for all index searches, #29746).
Every claim in "Claims" names the black-box test that proves it.

Status: awaiting design approval.

Design trigger: configuration semantics (new `BY RANK` options and session variables), a wire
contract change (one MORPC bump), a plugin extension point (membership capability), a GPU hot
path, and more than five owning packages.

## Problem and invariant

A vector top-k with a relational filter,

```sql
SELECT id FROM t WHERE category = 1 ORDER BY l2_distance(v, '[0,0]') LIMIT 10;
```

searches the vector index for a fixed candidate budget and then applies the filter. When the
nearest candidates fail the filter, the page comes back short or empty while qualifying rows
exist:

- the issue fixture (1,000 rows, the nearest 500 have `category = 0`): HNSW returns 0 of 10 rows
  on both CNs; the exact path returns ids 501..510;
- a 100k × 128-d benchmark with a ~5% filter: recall@10 is 0.18 for HNSW and 0.33 for ivfflat in
  the default mode, against 0.998 for pgvector 0.8 with an iterative scan.

Today `mode=pre|post|auto` (`BY RANK WITH OPTION`) is honored by ivfflat only. hnsw, cagra and
ivfpq check for `force` alone and silently ignore every other mode
(`apply_indices_{hnsw,cagra,ivfpq}.go`). Under a fulltext `MATCH` they skip the vector index and
run an exact sort (`vectorIndexSupportsContext`, `pkg/sql/plan/apply_indices.go`).

### Non-goal: no mode returns exactly `LIMIT` rows in general

Approximate search under a filter can return fewer than `LIMIT` rows while qualifying rows
exist. This design narrows *when* that happens, makes the budget visible and tunable, and states
the remaining cases per algorithm and mode in the short-page matrix. `mode=force` (exact search,
no index) is the only mode that never returns a short page.

### Goals

1. Pre-filtering (`mode=pre`) for hnsw, cagra and ivfpq: the filter's qualifying keys are applied
   inside the index search.
2. A search budget that widens when the page is short, up to a cap the user can set, for pre and
   post on all four vector indexes.
3. `auto` for every index that supports pre.
4. Hybrid fulltext + vector top-k uses the vector index on hnsw, cagra and ivfpq.
5. The chosen mode, knobs, caps and rounds are visible in `EXPLAIN` / `EXPLAIN ANALYZE`.
6. With `gpu_mode` on, cagra and ivfpq never fall back to a CPU search.

Success criteria: the issue fixture returns ids 501..510 through the index in `pre` on hnsw,
cagra and ivfpq with the default caps; every cell of the short-page matrix has a test showing its
behavior; queries that use none of the new modes or knobs produce byte-identical plans and
results.

## Precedent

| System | Mechanism |
|---|---|
| pgvector 0.8 | `hnsw.iterative_scan` / `ivfflat.iterative_scan`: the index scan is a resumable iterator; the executor filters each tuple and keeps pulling until `LIMIT` is met or `hnsw.max_scan_tuples` (total tuples produced) / `ivfflat.max_probes` is reached. The page can be short at the cap. |
| Vespa | `targetHits` (internal candidates) with `approximate-threshold` / `filter-first-threshold` chosen from the estimated hit ratio. |
| Lucene / OpenSearch | filtered HNSW (bitset during traversal), internal budget `num_candidates`, output `k`. |
| Weaviate, Qdrant, Milvus | allow-list or bitset pre-filter during graph search; exact scan below a cardinality cutoff. |
| cuVS CAGRA | bitset filter; `search_params.filtering_rate` (auto) widens the internal `itopk` from the filter rate. |

Where MatrixOne differs. pgvector streams and resumes one scan. usearch (hnsw) and cuVS
(cagra, ivfpq) return a fixed top-k per call and expose no resume or list-subset API, so widening
here restarts the search with larger knobs. ivfflat already has a resumable cursor
(`IvfSearchCursor`), so its widening continues instead. MatrixOne keeps one restart-and-double
loop per algorithm and a predictive starting point so that restarts are rare (see
"Performance").

## Semantics

### Modes

| mode | meaning |
|---|---|
| `post` (default) | index search, then the residual filter; the budget widens while the filtered page is short |
| `pre` | the filter's qualifying keys (membership) are applied inside the index search; the budget widens while the page is short |
| `auto` | `ADAPTIVE_TOP [post, pre, force]`: the next branch runs when the previous one is short |
| `force` | exact search without the vector index |

Explicit `pre` or `auto` on an index that does not support membership is an error (today it is
ignored).

### Knobs

Set in `BY RANK WITH OPTION`, one `key=value` per string literal, by index family:

```sql
... ORDER BY l2_distance(v, q) LIMIT 10 BY RANK WITH OPTION 'mode=pre', 'itopk=200', 'max_itopk=5000';
```

A knob that does not belong to the index's family is an error; values are positive integers and
a cap is at least its start. A string literal that is not one `key=value` pair is ignored, as
before.

| family | knob | meaning |
|---|---|---|
| graph: hnsw, cagra | `itopk`, `max_itopk` | internal candidate list (search breadth): start / cap |
| IVF: ivfflat, ivfpq | `nprobe`, `max_nprobe` | lists probed: start / cap; `max_nprobe = n_lists` probes every list |
| IVF: ivfflat, ivfpq | `itopk`, `max_itopk` | candidates returned from the probed lists: start / cap |

`LIMIT` rows are returned; the knobs only size the internal search.

Defaults and precedence (most specific wins):

| knob | SQL option | session variable | index parameter | built-in |
|---|---|---|---|---|
| `itopk` | `itopk` | `vector_itopk` | `ef_search` (hnsw), `itopk_size` (cagra) | today's over-fetch of `LIMIT` |
| `max_itopk` | `max_itopk` | `vector_max_itopk` | — | 0 (unset); the widening PRs set it from the 1M benchmark |
| `nprobe` | `nprobe` | `probe_limit` (existing, unchanged) | — | `probe_limit` default |
| `max_nprobe` | `max_nprobe` | `vector_max_nprobe` | — | 0 (unset); the widening PRs set it from the 1M benchmark |

`probe_limit` keeps its name, range, default and plan-time read; an explicit `nprobe` replaces it
(and turns off ivfflat's `auto` nprobe amplification). The plan carries only the SQL options.
The new session variables are read for every execution, never stored in a reusable plan or
compiled scope: a scan running on the session's CN reads them when it builds its search request,
and the coordinator writes them into the per-message copy of the scan it sends to another CN,
which has no session. So a `SET` after `PREPARE` applies to the next `EXECUTE`.

With an internal top-k set, the candidate budget is `max(itopk, LIMIT)`, lowered to `max_itopk`;
unset, it is today's over-fetch. In PR2 the search runs once with that budget: hnsw requests it
from usearch, cagra raises `itopk_size` to it, ivfpq and ivfflat return that many candidates from
the probed lists.

### Which knob acts, per mode

In `pre` the filter runs inside the search; in `post` it runs after it. That decides which knob
can fill a short page.

| | pre | post |
|---|---|---|
| hnsw | `itopk` / `max_itopk`: non-members still occupy beam slots while the graph is traversed | `itopk` / `max_itopk` |
| cagra | `itopk` / `max_itopk` as start and cap; cuVS `filtering_rate` (auto) scales the internal list for the filter rate | `itopk` / `max_itopk` |
| ivfpq | `nprobe` / `max_nprobe`. `itopk` is accepted and has no effect: with an exact membership set, non-members never take a candidate slot | `nprobe` and `itopk` |
| ivfflat | `nprobe` / `max_nprobe`. `itopk` matters only with a bloom membership (non-integer primary key), whose false positives are dropped later | `nprobe` and `itopk` |

A knob with no effect in the chosen mode is accepted, so the same SQL works across modes, and
`EXPLAIN` marks it inactive.

### Predictive start

In `pre`, the selectivity is exact: `|S|` qualifying keys over `N` indexed rows.

- hnsw starts at `itopk ≈ k · N / |S|` (times a safety factor), clamped to `[itopk, max_itopk]`.
- ivfflat and ivfpq scale `nprobe` the same way, clamped to `[nprobe, max_nprobe]`.
- cagra passes the plain start and leaves cuVS `filtering_rate` on auto, so the filter rate is
  applied once, by cuVS. `max_itopk` caps what MatrixOne passes; cuVS may scale its internal list
  above it.

In `post`, the start comes from the planner's filter-selectivity estimate, which already scales
ivfflat's `nprobe` today: `itopk ≈ k / selectivity`, clamped.

Retries cover what the prediction misses, chiefly filters correlated with distance (the issue
fixture: every near neighbor fails the filter).

## Per algorithm

### hnsw (CPU, usearch)

- Regular search: `HnswModel.Search` per sub-index, merged into a global top-k
  (`pkg/vectorindex/hnsw/search.go`). For `pre` it calls the existing
  `usearchex.FilteredSearchUnsafeWithMembership` (`pkg/vectorindex/usearchex/search.go`): a C
  predicate tests each usearch key, which is the BIGINT primary key, against the membership
  filter (cbitmap or sorted64), with one cgo crossing per sub-index search.
- Knob mapping: the requested count is `itopk`; usearch searches with `max(count, ef_search)`, so
  a larger count widens the beam for that call without changing the shared index's
  `ExpansionSearch`.
- Widening: `hnsw.AdvancedSearch` (`pkg/vectorindex/hnsw/advsearch.go`) starts at the larger of
  the candidate budget and `k·N/|S| × 2`, and restarts with `itopk × 2` up to `max_itopk`
  (default `DefaultMaxInternalTopK` = 20000, pgvector's `hnsw.max_scan_tuples` default), bounded
  by the indexed rows. usearch has no resumable search. An exact empty key set returns no rows
  without searching.
- Traversal: usearch's filtered search navigates through non-matching nodes and only keeps
  matching keys in its results. On the issue fixture (the 500 nearest rows fail the filter,
  `ef_search` 16) `mode=pre` returns the 10 qualifying nearest rows even with `max_itopk` = 10.
- Short page when: `max_itopk` is reached before the qualifying nearest rows are found. The
  tested fixtures (prefix, periodic, `IN`, combined and `NULL` filters) do not reach it.
- Plan: `mode=pre`, a membership join, or a fulltext `MATCH` filter on the scan builds the PRE
  shape. A query vector from a provider row runs the search per row in APPLY, which receives no
  membership runtime filter; that query keeps the exact sort. `mode=auto` is an error until
  PR6 builds it for hnsw.
- Ranking: exact distances on the stored vectors. Inner product on vectors of different norms is
  not a metric; on collinear vectors of different norms the usearch graph returns 33 of 600
  requested rows with no filter, so that fixture is not used for pre.

### cagra (GPU, cuVS)

- Regular search: cuVS CAGRA with a per-query bitset built by `build_filter_host_mask`
  (`cgo/cuvs/index_base.hpp`): INCLUDE predicates AND not-deleted AND membership. Membership is a
  sorted array of primary keys tested against `host_ids` in the same pass.
- Knob mapping: `itopk_size = max(itopk, LIMIT)`; `filtering_rate` stays auto.
- Widening: restart with `itopk × 2` up to `max_itopk`, clamped to cuVS's `itopk_size` limit.
- Short page when: `max_itopk` (or the cuVS limit) is reached.
- Ranking: exact distances on the stored vectors (see "Ranking precision by storage type").

### ivfpq (GPU, cuVS)

- Regular search: cuVS IVF-PQ with the same per-query bitset as cagra.
- Knob mapping: `n_probes = nprobe`; `k = itopk` in `post`, `LIMIT` in `pre`.
- Widening: restart with `nprobe × 2` up to `max_nprobe` (and `itopk × 2` up to `max_itopk` in
  `post`). `ivf_pq::search` takes only `n_probes`; it has no list-subset or resume API, so earlier
  lists are scanned again. Because the knobs double, the total over all rounds is at most about
  twice the final round.
- `max_nprobe = n_lists` probes every list, so every qualifying row can be returned.
- Short page when: the caps are reached with `max_nprobe < n_lists`.
- Ranking: PQ distances (approximate) in every case, including `n_probes = n_lists`. The index
  holds PQ codes only; there are no raw vectors to rank exactly.

### ivfflat (CPU and GPU centroids)

- Regular search: the existing ivfflat reader (`pkg/vectorindex/ivfflat/plan_reader.go`,
  `search.go`), with membership through the PRE shape and INCLUDE predicates as pre-filters.
- Knob mapping: `nprobe` lists per round; candidate count `itopk`.
- Widening: incremental. The `IvfSearchCursor` (`ivfflat/search.go`) ranks the centroids once and
  each round scans only the next lists, merging with earlier rounds.
- Short page when: the caps are reached with `max_nprobe < n_lists`.
- Ranking: exact on the stored entries; quantized entries (`quantization` option) rank by the
  quantized values.

### Ranking precision by storage type

| storage | ranking |
|---|---|
| native f32 / f16 / bf16 / int8 / uint8 columns | exact on the user's values (hnsw, cagra, ivfflat) |
| scalar-quantized storage of an f32/f16 column | exact on the quantized values only |
| ivfpq | PQ-approximate always |

## Short-page matrix

A cell reads "short when ..." under the default and configured caps. Every cell has a test in
"Claims".

| | post | pre | auto | force |
|---|---|---|---|---|
| hnsw | `max_itopk` reached and the filtered candidates still number fewer than `LIMIT` | `max_itopk` reached before the qualifying nearest rows are found (not reached by the tested fixtures) | `mode=auto` is an error until PR6 | never |
| cagra | as hnsw | `max_itopk` / cuVS limit reached | as hnsw | never |
| ivfpq | `max_nprobe < n_lists` and `max_itopk` reached | `max_nprobe < n_lists` reached | as hnsw | never |
| ivfflat | `max_nprobe < n_lists` and `max_itopk` reached | `max_nprobe < n_lists` reached | as hnsw | never |

"Short" here means fewer than min(`LIMIT`, qualifying rows).

## Architecture

### Two layers

- **Regular search**: today's single search call per algorithm (Go reader plus the C++ engine for
  cagra/ivfpq): query, knobs and filter in, candidates out. Its behavior does not change. The one
  addition is the membership set as one more filter input, next to the INCLUDE predicates; with
  no membership set the call is identical to today.
- **Advanced search**: one Go file per algorithm, `pkg/vectorindex/<algo>/advsearch.go`, for
  hnsw, cagra, ivfpq and ivfflat, including the GPU indexes. It owns the predictive start, the
  retry loop, the caps, the cuVS clamps and the round diagnostics. Each round calls the regular
  search; for cagra and ivfpq that is a cgo call into cuVS. It computes no distances.
- Each algorithm's policy implements `searchplugin.AdvancedSearchHooks`
  (`pkg/indexplugin/search/hooks.go`): `StartRound` returns the first `Round` (`ITopK`, and
  `NProbe` for IVF) from the request and the `SearchStats` the caller knows (indexed rows,
  membership keys); `NextRound` returns the round after one that returned too few rows, or
  stops. The algorithm's membership (pre) search runs the rounds itself; the plan-level post
  retry asks the same hooks. The search of each round is the algorithm's regular search.
- The only other shared code is option parsing and precedence.
- The reader calls the regular search directly when no mode or knob needs the advanced layer.

### Membership capability

An optional search-plugin interface next to `CandidateBudgetHooks`
(`pkg/indexplugin/search/hooks.go`):

```go
type MembershipHooks interface {
	SupportsMembership() bool
}
```

It replaces the algorithm-name checks in `vectorIndexSupportsContext`
(`pkg/sql/plan/apply_indices.go`) and `hasIvfFlatIndex` (`pkg/sql/plan/apply_indices_vector.go`).
PR1 declares it for ivfflat only, which keeps every plan unchanged; PR3 and PR4 declare it for
hnsw, cagra and ivfpq.

### PRE plan shape

The ivfflat PRE builder (`applyIndicesForSortUsingIvfflatWithContext`,
`pkg/sql/plan/apply_indices_ivfflat.go`) becomes the builder for every capable algorithm:

```
INNER JOIN(scan, SEMI(IndexSearchScan, filtered scan → PROJECT pk))
```

with a membership runtime filter (`UseMembershipFilter`, `MustApply`) from the filtered scan to
the `IndexSearchScan`. The executor passes it to the reader as `Request.MembershipFilter`
(`pkg/sql/colexec/vectorscan/execution.go`).

### Hybrid fulltext + vector

A `MATCH` in the scan's filters is one more filter the PRE shape evaluates in the filtered scan.
With the capability declared, the `MATCH` gate in `vectorIndexSupportsContext` admits the
algorithm. hnsw (PR3) builds the PRE shape for a `MATCH` filter by default, so its hybrid top-k
uses both the fulltext index and the vector index and returns the exact matching rows; ivfflat
keeps its post-filtered behavior. cagra and ivfpq follow in PR4.

The PRE shape is built by one function for every capable algorithm,
`buildVectorPreFilterJoin` (`pkg/sql/plan/apply_indices_vector_pre.go`), extracted from the
ivfflat builder; ivfflat passes its IVF-only steps (removing implied not-null filters of a
synchronous index, estimating scan work for distributed PRE) as hooks, so its plans are
unchanged.

### Post widening

The residual filter runs in the join above the `IndexSearchScan`, so the index cannot count the
rows that survive it. Post widening is a retry mode of `ADAPTIVE_TOP`
(`pkg/sql/colexec/adaptivetop`): it re-runs the unchanged post branch, taking the next knobs from
the algorithm's advanced search, while the page is short, the index is not exhausted, and a cap
is not reached. `pre` and `post` therefore share one widening policy per algorithm.

### End-to-end flow (pre on cagra)

1. The planner builds the PRE shape; the filtered scan produces the qualifying primary keys.
2. The runtime filter carries them to the `IndexSearchScan`; the reader decodes them into a
   sorted, de-duplicated `[]int64`.
3. `cagra/advsearch.go` computes the start knobs and calls the regular search through cgo.
4. C++ builds the per-query bitset (membership AND INCLUDE AND not-deleted) under the existing
   shared lock and uploads it; cuVS searches with no MatrixOne lock held.
5. If the page is short and a cap allows, step 3 repeats with doubled knobs.
6. The candidates join back to the table; the top `LIMIT` rows are returned.

## GPU policy

With `gpu_mode` on, cagra and ivfpq search and widen on the GPU only:

- knobs are clamped to cuVS limits; a GPU out-of-memory error is returned as an error;
- `force` and `auto`'s force branch use the `vector_matmul` engine on the GPU (PR7);
- CPU by design: the relational filtered scan that produces the membership set, the host-side
  mask build (`build_filter_host_mask`), and hnsw, which is a CPU index.

ivfflat's PRE with a required membership and several partitions routes its centroid search on
the CPU (`ivfflat/plan_reader.go`); ivfflat is outside the GPU policy in this design.

## Compatibility and rollout

- **Wire**: PR2 adds `IndexSearchScan.internal_topk`, `max_internal_topk` and `max_nprobe`
  (`nprobe` keeps the IVF algorithms' existing `algo_options` probe count) with MORPC version 112
  and the alias `vectorscan.SearchKnobsProtocolVersion`. A scan that carries any of them, from
  the SQL option or a session variable, requires 112 on every CN that runs it; a scan without
  them keeps requiring 111. PR5 adds the `ADAPTIVE_TOP` retry marker and bumps the version again
  only if the marker crosses CNs.
- **Membership**: no version change. Since #29746 the hnsw, cagra and ivfpq readers refuse
  `MembershipFilterRequired`, so a CN running an older build fails a PRE plan instead of ignoring
  the filter.
- **Defaults**: `post` stays the default; the caps have session defaults, so widening is on for
  `pre` and `post` without any option. A query using no new mode or knob runs exactly as today.
- **Observability**: `EXPLAIN` prints `Candidate Limit`, ivfflat's `NProbe`, and the SQL options
  `Internal Top-K`, `Max Internal Top-K` and `Max NProbe` when set; a scan without them prints
  as before. Session-variable values are not in the plan and are not printed by `EXPLAIN`.
  `EXPLAIN ANALYZE` prints the rounds and final knobs once the search widens.

## Resources and performance

- **Membership mask**: built per query; never written into the shared delete bitsets or their
  device caches, and never bumps their version. `id_to_index_` is not used (its lazy build costs
  about 40 bytes per row and a unique lock).
- **Membership filter (hnsw)**: built once per reader through the existing docfilter memory
  admission, shared read-only by the sub-index searches, freed when the reader closes.
- **Rounds**: bounded by the caps, about `log2(cap / start)`. Each cagra/ivfpq round rebuilds the
  host mask (one pass over `host_ids`). ivfpq rounds re-scan earlier lists, about twice the final
  round in total. ivfflat rounds do not re-scan.
- **Predictive start**: makes extra rounds the exception (distance-correlated filters).
- **No lock is held across a GPU call** (`CLAUDE.md`).

## Alternatives

| Alternative | Decision |
|---|---|
| Keep a fixed over-fetch multiplier only | Rejected: the issue's 0/10 and recall 0.18 come from exactly this. Over-fetch stays as the default start when `itopk` is absent. |
| An exact (brute-force) tier inside `pre` for small qualifying sets | Rejected: `pre` stays a fast filtered ANN; exact results are `mode=force`. |
| An `exact=true|false` option | Rejected: with no exact tier inside `pre`, it has nothing to control; `mode=force` covers it. |
| One generic advanced search for all algorithms | Rejected: each algorithm's knobs and widening differ (restart vs incremental, beam vs probes); one file per algorithm. |
| The widening loop in C++ | Rejected: the loop is policy, kept in Go next to the other algorithms' loops; C++ keeps the single search. |
| ivfpq exact ranking (decode member codes, or read raw vectors from the table) | Rejected: decoded PQ codes are still approximate; raw vectors would mean a table read inside the index path. `n_probes = n_lists` is ivfpq's end state. |
| pgvector-style `max_scan_tuples` (total produced across rounds) | Rejected for naming: `max_itopk` / `max_nprobe` match the per-round knobs of usearch and cuVS. The mapping is `max_itopk` ≈ `max_scan_tuples` for the final round and `max_nprobe` = `ivfflat.max_probes`. |

## Delivery

| PR | Content |
|---|---|
| PR1 | membership capability; only ivfflat declares it (plans unchanged); explicit `pre`/`auto` on an incapable index is an error |
| PR2 | knobs, session variables, wire fields and MORPC version, `EXPLAIN`; single search, no widening |
| PR3 | hnsw `pre` + `hnsw/advsearch.go`; hybrid fulltext on hnsw |
| PR4 | cagra and ivfpq `pre` (C++ membership mask) + their `advsearch.go`; hybrid fulltext on both |
| PR5 | post widening through `ADAPTIVE_TOP` |
| PR6 | `auto` for every capable algorithm; remove the unused `rewriteAutoModeToPre` path |
| PR7 | `force` under `gpu_mode` through the `vector_matmul` engine (after #29554 is on main) |

## Claims

`cases/` and `gpu_cases/` are under `test/distributed/`.

| # | Claim | Black-box test |
|---|---|---|
| 1 | A query with no new mode or knob produces the same plan and rows as before, on all four indexes. | the existing case files that create an ivfflat, hnsw, cagra or ivfpq index under `cases/vector/` and `gpu_cases/vector/` |
| 2 | Explicit `mode=pre` or `mode=auto` on an index without membership support is an error; ivfflat accepts both. | `cases/vector/vector_filtered_search_modes.sql` (hnsw, ivfflat), `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq) |
| 3 | `itopk`, `max_itopk`, `nprobe`, `max_nprobe` are accepted on their family and rejected on the other; values must be positive with max ≥ start. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 4 | `EXPLAIN` shows the SQL options `Internal Top-K`, `Max Internal Top-K`, `Max NProbe` and ivfflat's `NProbe`. | `cases/vector/vector_filtered_search_knobs.sql`, `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` |
| 5 | A `SET vector_itopk` after `PREPARE` applies to the next `EXECUTE` and stops applying once reset; the SQL option wins over it. | `cases/vector/vector_filtered_search_knobs.sql` |
| 5a | With a single search, `itopk` covering the index returns ids 501..510 on the issue fixture where the default budget returns none, on hnsw, ivfflat, cagra and ivfpq; `max_itopk` lowers `itopk`. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 6 | The issue fixture returns ids 501..510 in `pre` through the hnsw index, for l2 on a five-sub-index table and cosine, where the default post returns none. | `cases/vector/vector_filtered_search_hnsw.sql`, `cases/vector/vector_filtered_search_modes.sql` |
| 7 | The issue fixture returns ids 501..510 in `pre` through the cagra and the ivfpq index. | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 8 | `pre` returns zero rows for zero qualifying rows, all qualifying rows when fewer than `LIMIT` qualify, and `LIMIT` rows when exactly or more than `LIMIT` qualify, with periodic, combined and `NULL` filters, `OFFSET` and a prepared `LIMIT`, equal to `mode=force`. | `cases/vector/vector_filtered_search_hnsw.sql` (hnsw); `gpu_cases/vector/vector_filtered_search_gpu.sql` (cagra, ivfpq, PR4); `cases/vector/vector_filtered_search_ivfflat.sql` (ivfflat) |
| 9 | Each "short when" cell of the short-page matrix that a fixture reaches: with a cap set below what the filter needs, the page is short; with the cap raised, it fills. hnsw `pre` fills at `max_itopk` = k on the tested fixtures. | the three files of claim 8 |
| 10 | `post` widening fills the page where a single round was short, up to the caps, on all four indexes. | the three files of claim 8 |
| 11 | `auto` on hnsw, cagra and ivfpq returns the `force` rows when `post` and `pre` are short. | the three files of claim 8 |
| 12 | A hybrid `MATCH` + vector top-k on hnsw, cagra and ivfpq uses the fulltext index and the vector index through the PRE shape and returns the same rows as the table without a vector index; with a query vector from a provider row it keeps the exact sort. | `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq, PR4) |
| 13 | ivfpq with `max_nprobe = n_lists` returns min(`LIMIT`, qualifying) rows in `pre`. | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 14 | Literal and prepared `LIMIT` give the same rows under the same knobs. | the three files of claim 8 |
| 15 | With `gpu_mode` on, `force` on a cagra or ivfpq table runs on the GPU and returns the exact rows. | `gpu_cases/vector/vector_filtered_search_gpu.sql` (PR7) |

## Decision log

2026-10-12 (Eric):

- `post` stays the default mode.
- No mode is described as returning exactly `LIMIT` rows; the design documents per algorithm and
  mode when a page can be short.
- Knobs by family: graph `itopk` / `max_itopk`; IVF `nprobe` / `max_nprobe` plus `itopk` /
  `max_itopk`. A knob on the wrong family is an error; an inactive knob is accepted and shown.
- Knobs are set in SQL options and session variables; the SQL option wins.
- `probe_limit` is unchanged and remains `nprobe`'s session default.
- Without `itopk`, today's over-fetch is the starting candidate count.
- No exact tier inside `pre` and no `exact` option; `mode=force` is the exact path.
- The advanced search is Go, one file per algorithm, on top of the unchanged regular search,
  including the GPU indexes.
- hnsw, cagra and ivfpq widen by restarting; ivfflat continues its cursor.
- cagra leaves cuVS `filtering_rate` on auto.
- ivfpq's end state is `n_probes = n_lists`; its ranking stays PQ-approximate.
- With `gpu_mode` on, cagra and ivfpq never fall back to a CPU search; `force` runs through the
  `vector_matmul` engine for vector-indexed queries only.
- Hybrid fulltext + vector is in scope.

2026-10-13 (PR3, Eric):

- Every algorithm's advanced search implements one policy-only interface,
  `searchplugin.AdvancedSearchHooks` (`StartRound` / `NextRound` over `Round{ITopK, NProbe}`);
  the search of each round stays the algorithm's regular search.
- hnsw builds the PRE shape for `mode=pre`, a membership join and a fulltext `MATCH` filter; a
  query vector from a provider row keeps the exact sort; `mode=auto` is an error until PR6.
- The default `max_itopk` of a widening hnsw search is 20000 (pgvector's
  `hnsw.max_scan_tuples` default).
- Without `itopk`, each algorithm keeps its own over-fetch table (ivfflat
  `FilteredPostModeLimit`, hnsw/cagra/ivfpq `PostFilterLimit`).
