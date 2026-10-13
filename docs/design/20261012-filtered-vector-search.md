# Filtered vector search: pre, iterative, post and auto for every vector index

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
2. `mode=iterative`: a pre-filtered search whose budget widens when the page is short, up to a
   cap the user can set, on all four vector indexes. `pre` and `post` run one search whose
   budget the user can set.
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
| pgvector 0.8 | `hnsw.iterative_scan` / `ivfflat.iterative_scan`: the index scan is a resumable iterator; the executor filters each tuple and keeps pulling until `LIMIT` is met or `hnsw.max_scan_tuples` (total tuples produced) / `ivfflat.max_probes` is reached. The page can be short at the cap. With `iterative_scan = off`, one scan of `hnsw.ef_search` / `ivfflat.probes` is filtered once. |
| Vespa | `targetHits` (internal candidates) with `approximate-threshold` / `filter-first-threshold` chosen from the estimated hit ratio. |
| Lucene / OpenSearch | filtered HNSW (bitset during traversal), internal budget `num_candidates`, output `k`. |
| Weaviate, Qdrant, Milvus | allow-list or bitset pre-filter during graph search; exact scan below a cardinality cutoff. |
| cuVS CAGRA | bitset filter; `search_params.filtering_rate` (auto) widens the internal `itopk` from the filter rate. |

Where MatrixOne differs. pgvector documents its iterative scan as post-filtering, but the filter
runs on each tuple while the scan is still going, so the scan learns how many rows qualify: in
MatrixOne's terms that is `pre`. MatrixOne's `post` filters in the join after the index search has
returned, like pgvector with `iterative_scan = off`. MatrixOne's `iterative` is pgvector's
iterative scan; its `pre` is the filter inside one search. pgvector streams and resumes one scan. usearch (hnsw) and cuVS
(cagra, ivfpq) return a fixed top-k per call and expose no resume or list-subset API, so widening
here restarts the search with larger knobs. ivfflat already has a resumable cursor
(`IvfSearchCursor`), so its widening continues instead. MatrixOne keeps one restart-and-double
loop per algorithm and a predictive starting point so that restarts are rare (see
"Performance").

## Semantics

### Modes

| mode | meaning |
|---|---|
| `post` | one index search; only filters on the primary-key column run inside it, every other filter (INCLUDE columns too) after it |
| no option (default) | `post`, except that filters on INCLUDE columns also run inside the index search |
| `pre` | the filter is applied inside one index search: the qualifying keys of a filtered scan (membership), or, on ivfflat, a filter the index search evaluates itself (the primary key) |
| `iterative` | `pre` whose search widens while the page is short, up to `max_itopk` / `max_nprobe`; on ivfflat a filter covered by INCLUDE columns runs inside the index search |
| `include` | ivfflat only: `iterative` (the mode INCLUDE columns introduced, #24168) |
| `auto` | `ADAPTIVE_TOP [post, pre, force]`: the next branch runs when the previous one is short |
| `force` | exact search without the vector index |

Explicit `pre`, `iterative` or `auto` on an index that does not support membership is an error
(today `pre` and `auto` are ignored); `include` on an index other than ivfflat is an error.

### Knobs

Set in `BY RANK WITH OPTION`, one `key=value` per string literal, by index family:

```sql
... ORDER BY l2_distance(v, q) LIMIT 10 BY RANK WITH OPTION 'mode=iterative', 'itopk=200', 'max_itopk=5000';
```

A knob that does not belong to the index's family is an error; values are positive integers and
a cap is at least its start. A string literal that is not one `key=value` pair is ignored, as
before.

| family | knob | meaning |
|---|---|---|
| graph: hnsw, cagra | `itopk`, `max_itopk` | internal candidate list (search breadth): start / cap |
| IVF: ivfflat, ivfpq | `nprobe`, `max_nprobe` | lists probed: start / cap; `max_nprobe = n_lists` probes every list |
| IVF: ivfflat, ivfpq | `itopk`, `max_itopk` | candidates returned from the probed lists: start / cap |

`LIMIT` rows are returned; the knobs only size the internal search. The caps `max_itopk` and
`max_nprobe` bound the widening of `iterative`; `pre` and `post` run one search and do not use
them.

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

In `iterative` the filter runs inside the search and the search widens; in `post` the filter
runs after one search. That decides which knob can fill a short page. `pre` runs one search
with `itopk` (or `nprobe`) and no cap.

| | iterative | post |
|---|---|---|
| hnsw | `itopk` / `max_itopk`: non-members still occupy beam slots while the graph is traversed | `itopk` |
| cagra | `itopk` / `max_itopk` as start and cap when the GPU filter (INCLUDE columns, primary key) holds every filter; cuVS `filtering_rate` (auto) scales the internal list for the filter rate | `itopk` |
| ivfpq | `nprobe` / `max_nprobe` when the GPU filter holds every filter; `itopk` is the candidate count of each round | `nprobe` and `itopk` |
| ivfflat | `nprobe` / `max_nprobe`. `itopk` matters only with a bloom membership (non-integer primary key), whose false positives are dropped later | `nprobe` and `itopk` |

In `post`, `max_itopk` lowers `itopk` and `max_nprobe` has no effect.

A knob with no effect in the chosen mode is accepted, so the same SQL works across modes, and
`EXPLAIN` marks it inactive.

### Predictive start

In `iterative`, the selectivity is exact: `|S|` qualifying keys over `N` indexed rows.

- hnsw starts at `itopk ≈ k · N / |S|` (times a safety factor), clamped to `[itopk, max_itopk]`.
- ivfflat and ivfpq scale `nprobe` the same way, clamped to `[nprobe, max_nprobe]`. ivfflat
  readers that split one search across partitions do not.
- cagra passes the plain start and leaves cuVS `filtering_rate` on auto, so the filter rate is
  applied once, by cuVS. `max_itopk` caps what MatrixOne passes; cuVS may scale its internal list
  above it.

`post` has no rounds: its one search uses `itopk`, or today's over-fetch of `LIMIT` when `itopk` is
absent; ivfflat's `auto` keeps scaling its `post` branch's `nprobe` from the planner's
selectivity estimate.

Retries cover what the prediction misses, chiefly filters correlated with distance (the issue
fixture: every near neighbor fails the filter).

## Per algorithm

### hnsw (CPU, usearch)

- Regular search: `HnswModel.Search` per sub-index, merged into a global top-k
  (`pkg/vectorindex/hnsw/search.go`). For `pre` and `iterative` it calls the existing
  `usearchex.FilteredSearchUnsafeWithMembership` (`pkg/vectorindex/usearchex/search.go`): a C
  predicate tests each usearch key, which is the BIGINT primary key, against the membership
  filter (cbitmap or sorted64), with one cgo crossing per sub-index search.
- Knob mapping: the requested count is `itopk`; usearch searches with `max(count, ef_search)`, so
  a larger count widens the beam for that call without changing the shared index's
  `ExpansionSearch`.
- `pre`: one membership search with the candidate budget.
- Widening (`iterative`, `ScanOptions.Iterative`): `hnsw.AdvancedSearch`
  (`pkg/vectorindex/hnsw/advsearch.go`) starts at the larger of
  the candidate budget and `k·N/|S| × 2`, and restarts with `itopk × 2` up to `max_itopk`
  (default `DefaultMaxInternalTopK` = 20000, pgvector's `hnsw.max_scan_tuples` default), bounded
  by the indexed rows. usearch has no resumable search. An exact empty key set returns no rows
  without searching.
- Traversal: usearch's filtered search navigates through non-matching nodes and only keeps
  matching keys in its results. On the issue fixture (the 500 nearest rows fail the filter,
  `ef_search` 16) `mode=pre` returns the 10 qualifying nearest rows in one search, and
  `mode=iterative` with `max_itopk` = 10.
- Short page when: `pre`, the one search misses qualifying nearest rows; `iterative`,
  `max_itopk` is reached before they are found. The tested fixtures (prefix, periodic, `IN`,
  combined and `NULL` filters) reach neither.
- Plan: `mode=pre`, `mode=iterative`, a membership join, or a fulltext `MATCH` filter on the scan builds the PRE
  shape. A query vector from a provider row runs the search per row in APPLY, which receives no
  membership runtime filter; that query keeps the exact sort.
- Auto: `mode=auto` builds `ADAPTIVE_TOP [post, pre, force]` with the builder shared with
  ivfflat (`buildAdaptiveVectorTop`, `pkg/sql/plan/apply_indices_vector_auto.go`). The pre branch
  is left out when it would equal post: without a filter, or with a `MATCH` filter, under which
  post already searches the matching keys. A region the planner estimates as small
  (`LIMIT × 2` rows or fewer), with a volatile expression, a membership join or a provider row
  runs `force`, as for ivfflat. Only an explicit `mode=auto` selects it; the session defaults
  `enable_vector_auto_mode_by_default` and `enable_vector_prefilter_by_default` stay ivfflat's.
- Ranking: exact distances on the stored vectors. Inner product on vectors of different norms is
  not a metric; on collinear vectors of different norms the usearch graph returns 33 of 600
  requested rows with no filter, so that fixture is not used for pre.

### cagra (GPU, cuVS)

- Regular search: cuVS CAGRA with a per-query bitset built by `build_filter_host_mask`
  (`cgo/cuvs/index_base.hpp`): the filters on INCLUDE columns and the primary key
  (`FilterJSON`, every mode but `force`; `post` sends the primary-key filters only, as ivfflat
  `post` does) AND not-deleted. cagra applies no membership key set
  (no `MembershipHooks`); it declares `IndexFilterHooks`, so `pre`, `iterative` and `auto` apply.
  Other filters run after the search, in the join.
- Auto: `ADAPTIVE_TOP [post, pre, force]` through `buildAdaptiveModeTop`, the builder hnsw uses;
  the pre branch is left out unless a filter reads an INCLUDE column, since `pre` equals `post`
  otherwise. The force branch is the exact sort, on the GPU through `vector_matmul` with PR7.
- Knob mapping: `itopk_size = max(itopk, LIMIT)`; `filtering_rate` stays auto.
- Widening (`iterative`, `ScanOptions.Iterative`, set when `FilterJSON` holds every filter):
  `cagra.AdvancedSearch` (`pkg/vectorindex/cagra/advsearch.go`) restarts with `itopk × 2` up to
  `max_itopk` (default 20000), bounded by the indexed rows. `pre` searches once.
- Short page when: `pre`, the one search misses qualifying rows; `iterative`, `max_itopk` is
  reached; a filter outside the INCLUDE columns, as in `post`.
- Ranking under a filter: a full page can miss nearer qualifying rows when the filter
  removes the rows between the query and them. On 1,000 rows on one line, `graph_degree` 8 and
  `id between 501 and 1000` from the origin, `pre` and `iterative` return 583, 617, 631, 647,
  673 with the default `itopk_size` 64 and 501..505 with `itopk` 256. `iterative` widens on the
  row count, so it does not widen a full page; a larger `itopk` is the remedy, and cuVS
  `filtering_rate` (auto) already scales the internal list for the filter rate.
- Ranking: exact distances on the stored vectors (see "Ranking precision by storage type").

### ivfpq (GPU, cuVS)

- Regular search: cuVS IVF-PQ with the same per-query bitset as cagra, and the same modes.
  cuVS fills the slots the probed lists cannot fill with rows the bitset excludes; the host
  post-filter (`apply_host_post_filter_locked`) replaces every one with a sentinel. It used to
  be skipped when the filter admitted at least `4 · LIMIT` rows in the whole index, so a
  filter the probed lists did not hold returned excluded rows and failed the distance check.
- Knob mapping: `n_probes = nprobe`; `k` = the candidate budget.
- Widening (`iterative`, set as for cagra): `ivfpq.AdvancedSearch`
  (`pkg/vectorindex/ivfpq/advsearch.go`) restarts with `nprobe × 2` up to `max_nprobe` (every
  list when unset); `pre` searches once. `ivf_pq::search` takes only `n_probes`; it has no list-subset or resume API, so earlier
  lists are scanned again. Because the knobs double, the total over all rounds is at most about
  twice the final round.
- `max_nprobe = n_lists` probes every list, so every qualifying row can be returned.
- Short page when: `pre`, the `nprobe` lists hold too few qualifying rows; `iterative`, the
  caps are reached with `max_nprobe < n_lists`; a filter outside the INCLUDE columns, as in
  `post`.
- Ranking: PQ distances (approximate) in every case, including `n_probes = n_lists`. The index
  holds PQ codes only; there are no raw vectors to rank exactly.

### ivfflat (CPU and GPU centroids)

- Regular search: one round of `IvfflatSearchIndex.Search` (`pkg/vectorindex/ivfflat/search.go`):
  the `IvfSearchCursor` ranks the centroids once, and a round scans the next lists' entries with
  the membership set and the filters evaluated in the index search (INCLUDE columns and the
  primary key), up to its row limit.
- Pre-filters: a membership set (the PRE shape, when a filter needs the table), or filters the
  index search evaluates itself: the primary key in every mode, and INCLUDE columns in the
  default mode, `iterative` / `include` and `auto` (not in `pre` or `post`). No membership join is
  built when the index search evaluates every filter. A membership set of 100 keys or fewer
  (`exactPkFilterThreshold`) scans those keys in every list.
- Knob mapping: `nprobe` lists in the first round; candidate count `itopk`.
- Widening: `ivfflat.AdvancedSearch` (`pkg/vectorindex/ivfflat/advsearch.go`) runs in the reader
  (`searchPlanReader`, `plan_reader.go`). The first round probes `nprobe` lists; under a
  membership set, at least `n_lists · budget / |S| × 2`. Each further round continues the cursor
  with twice the previous round's lists (at most 4096), until the reader holds the candidate
  budget (at most `|S|`), all lists are probed, or the lists probed reach `max_nprobe`. It runs
  for `mode=iterative` (and `include`): when the index search evaluates every filter the scan
  is multi-round (`BucketExpandStep`); a membership search widens on a reader that holds the
  whole search (`ScanOptions.Iterative`). A PRE search split across readers (local DOP or one
  reader per CN, for large entries tables) shares one route and searches one round: no reader
  holds the merged row count.
- `pre`, `post` and the default mode search one round, also when the index search evaluates
  the filter.
- Centroid ranking: the rounds walk one ranking of all lists, a permutation of the centroid ids
  (`completeCentroidRanking`, `ivfflat/search.go`). A query vector whose centroid distances tie
  or are not numbers (a zero vector under cosine) can rank a centroid twice and leave others
  out; the duplicates are dropped and the missing lists appended in id order.
- Short page when: `pre`, the `nprobe` lists hold too few qualifying rows (a membership set of
  100 keys or fewer is never short); `iterative`, a membership set over 100 keys or
  index-evaluated filters and the lists probed reach `max_nprobe < n_lists`, or the search is
  split across readers and the `nprobe` lists hold too few qualifying rows.
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

| | post | pre | iterative | auto | force |
|---|---|---|---|---|---|
| hnsw | the residual filter rejects more candidates than the one search returned (`itopk`, or the over-fetch) | the one search misses qualifying nearest rows (not reached by the tested fixtures) | `max_itopk` reached before the qualifying nearest rows are found (not reached by the tested fixtures) | never: a short `post` and `pre` fall through to `force` | never |
| cagra | as hnsw | the one search misses qualifying rows, or a filter outside the INCLUDE columns rejects them | `max_itopk` reached, or a filter outside the INCLUDE columns (one search) | as hnsw | never |
| ivfpq | as hnsw, with the candidates of `nprobe` lists | the `nprobe` lists hold too few qualifying rows, or a filter outside the INCLUDE columns rejects them | `max_nprobe < n_lists` reached, or a filter outside the INCLUDE columns (one search) | as hnsw | never |
| ivfflat | as ivfpq | the `nprobe` lists hold too few qualifying rows; 100 keys or fewer: never | over 100 keys or index-evaluated filters: `max_nprobe < n_lists` reached, or the search split across readers; 100 keys or fewer: never | as hnsw | never |

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
  stops. The algorithm's `iterative` search runs the rounds itself. The search of each round
  is the algorithm's regular search.
- The only other shared code is option parsing and precedence.
- The reader calls the regular search directly unless the scan is `iterative`.

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
PR1 declares it for ivfflat only, which keeps every plan unchanged; PR3 declares it for hnsw.
cagra and ivfpq declare `IndexFilterHooks` instead: their search evaluates filters on INCLUDE
columns and the primary key itself, which admits `pre` and `iterative` without a membership set.

### PRE plan shape

The ivfflat PRE builder (`applyIndicesForSortUsingIvfflatWithContext`,
`pkg/sql/plan/apply_indices_ivfflat.go`) becomes the builder for every capable algorithm:

```
INNER JOIN(scan, SEMI(IndexSearchScan, filtered scan → PROJECT pk))
```

with a membership runtime filter (`UseMembershipFilter`, `MustApply`) from the filtered scan to
the `IndexSearchScan`. The executor passes it to the reader as `Request.MembershipFilter`
(`pkg/sql/colexec/vectorscan/execution.go`).

### Auto

`buildAdaptiveVectorTop` builds the `ADAPTIVE_TOP` region for every capable algorithm: it copies
the vector top-k region, annotates the copies `post`, `pre` and `force`, and asks the algorithm's
plan rewrite for the post and pre branches. Each copy binds its index search under its own alias
(`mo_<algo>_alias_N`, the smallest unused `N`), so the first keeps the alias of a non-adaptive
plan. The operator runs the branches one after another and returns the first full page, or the
last branch's page.

### Hybrid fulltext + vector

A `MATCH` in the scan's filters is one more filter the PRE shape evaluates in the filtered scan.
With the capability declared, the `MATCH` gate in `vectorIndexSupportsContext` admits the
algorithm. hnsw (PR3) builds the PRE shape for a `MATCH` filter by default, so its hybrid top-k
uses both the fulltext index and the vector index and returns the exact matching rows; ivfflat
keeps its post-filtered behavior. cagra and ivfpq apply no membership set, so a `MATCH` filter
keeps their exact sort.

The PRE shape is built by one function for every capable algorithm,
`buildVectorPreFilterJoin` (`pkg/sql/plan/apply_indices_vector_pre.go`), extracted from the
ivfflat builder; ivfflat passes its IVF-only steps (removing implied not-null filters of a
synchronous index, estimating scan work for distributed PRE) as hooks, so its plans are
unchanged.

### Post: one search

The residual filter runs in the join above the `IndexSearchScan`, after the search has returned
all its candidates, so the search cannot count the rows that survive it and has no signal to
widen on. Only predicates on the primary-key column run inside the search: every index entry
carries the key (`__mo_index_pri_col` on ivfflat, `host_ids` on cagra and ivfpq), so they need
no table read. Filters on INCLUDE columns run after the search in `post`, as filters on other
columns do. `post` runs one search sized by `itopk` (or the over-fetch). A short `post` page is
filled by `iterative`, or by `auto`, whose `ADAPTIVE_TOP` (`pkg/sql/colexec/adaptivetop`) runs
`pre` and then `force` when the previous branch is short.

### End-to-end flow (iterative on cagra)

1. The planner serializes the filters on INCLUDE columns and the primary key to `FilterJSON`
   and sets `Iterative` when no other filter remains on the scan.
2. `CagraSearch.Search` runs the rounds of `cagra/advsearch.go`, each one the regular search
   through cgo.
3. C++ builds the per-query bitset (filter AND not-deleted) under the existing shared lock and
   uploads it; cuVS searches with no MatrixOne lock held.
4. If fewer than `LIMIT` keys came back and the cap allows, step 2 repeats with doubled `itopk`.
5. The candidates join back to the table; the top `LIMIT` rows are returned.

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
  them keeps requiring 111.
- **Membership**: no version change. Since #29746 the hnsw, cagra and ivfpq readers refuse
  `MembershipFilterRequired`, so a CN running an older build fails a PRE plan instead of ignoring
  the filter.
- **Defaults**: `post` stays the default; widening runs only under `mode=iterative` (or ivfflat's
  `include`). A query using no new mode or knob runs exactly as today.
- **Scan options**: `iterative` is a field of the hnsw and ivfflat `algo_options` JSON. A CN of an
  older build ignores it: hnsw then searches once (as `pre`), ivfflat widens a scan marked
  multi-round and searches a membership scan once.
- **Observability**: `EXPLAIN` prints `Candidate Limit`, ivfflat's `NProbe`, the filter the
  search applies (`Index Filter`: ivfflat's pre-filters, cagra's and ivfpq's GPU filter rendered
  as SQL from `FilterJSON`), and the SQL options
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
| Widening in `post`: re-run the post branch with larger knobs while the filtered page is short | Rejected: `ADAPTIVE_TOP` starts each compiled branch once, so a re-run means restarting a running scope tree (receivers, remote runs, runtime filters); unrolling rounds as extra branches repeats the table join per round. `pre` and `auto` already fill a short `post` page. |
| pgvector-style `max_scan_tuples` (total produced across rounds) | Rejected for naming: `max_itopk` / `max_nprobe` match the per-round knobs of usearch and cuVS. The mapping is `max_itopk` ≈ `max_scan_tuples` for the final round and `max_nprobe` = `ivfflat.max_probes`. |

## Delivery

| PR | Content |
|---|---|
| PR1 | membership capability; only ivfflat declares it (plans unchanged); explicit `pre`/`auto` on an incapable index is an error |
| PR2 | knobs, session variables, wire fields and MORPC version, `EXPLAIN`; single search, no widening |
| PR3 | hnsw `pre` + `hnsw/advsearch.go`; hybrid fulltext on hnsw |
| PR4 | hnsw `auto`: the `ADAPTIVE_TOP [post, pre, force]` builder for every capable algorithm; remove the unused `rewriteAutoModeToPre` path |
| PR5 | ivfflat onto the two layers: `ivfflat/advsearch.go` drives the `iterative` (and `include`) rounds in the reader; `mode=iterative` on ivfflat and hnsw |
| PR6 | cagra and ivfpq `pre`, `iterative` and `auto` on their GPU filter (INCLUDE columns, primary key) + their `advsearch.go`; `post` without INCLUDE columns; `EXPLAIN` Index Filter; ivfpq host post-filter always runs |
| PR7 | `force` under `gpu_mode` through the `vector_matmul` engine (after #29554 is on main) |

## Claims

`cases/` and `gpu_cases/` are under `test/distributed/`.

| # | Claim | Black-box test |
|---|---|---|
| 1 | A query with no new mode or knob produces the same plan and rows as before, on all four indexes, except an ivfflat query vector that ties every centroid (claim 16) and ivfflat `include`, now `iterative`, whose membership search also widens. | the existing case files that create an ivfflat, hnsw, cagra or ivfpq index under `cases/vector/` and `gpu_cases/vector/` |
| 2 | Explicit `mode=pre`, `mode=iterative` or `mode=auto` on an index with neither membership nor index-filter support is an error; ivfflat, hnsw, cagra and ivfpq accept all three. `mode=include` is accepted by ivfflat only. | `cases/vector/vector_filtered_search_modes.sql` (hnsw, ivfflat), `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql`, `gpu_cases/vector/vector_filtered_search_gpu.sql`, `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 3 | `itopk`, `max_itopk`, `nprobe`, `max_nprobe` are accepted on their family and rejected on the other; values must be positive with max ≥ start. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 4 | `EXPLAIN` shows the SQL options `Internal Top-K`, `Max Internal Top-K`, `Max NProbe` and ivfflat's `NProbe`. | `cases/vector/vector_filtered_search_knobs.sql`, `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` |
| 5 | A `SET vector_itopk` after `PREPARE` applies to the next `EXECUTE` and stops applying once reset; the SQL option wins over it. | `cases/vector/vector_filtered_search_knobs.sql` |
| 5a | With a single search, `itopk` covering the index returns a full page on the issue fixture where the default budget returns none, on hnsw, ivfflat, cagra and ivfpq (ids 501..510 on all but cagra, whose graph build is not deterministic); `max_itopk` lowers `itopk`. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 6 | The issue fixture returns ids 501..510 in `pre` and `iterative` through the hnsw index, for l2 on a five-sub-index table and cosine, where the default post returns none. | `cases/vector/vector_filtered_search_hnsw.sql`, `cases/vector/vector_filtered_search_modes.sql` |
| 7 | With the filter on an INCLUDE column, the issue fixture returns a full page in `pre` and `iterative` through the cagra index, and ids 501..510 in `iterative` through the ivfpq index where `pre` with `nprobe=1` returns none; with a filter outside the INCLUDE columns `iterative` searches once. | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 8 | `iterative` returns zero rows for zero qualifying rows, all qualifying rows when fewer than `LIMIT` qualify, and `LIMIT` rows when exactly or more than `LIMIT` qualify, with periodic, combined and `NULL` filters, `OFFSET` and a prepared `LIMIT`, equal to `mode=force`. | `cases/vector/vector_filtered_search_hnsw.sql` (hnsw); `gpu_cases/vector/vector_filtered_search_gpu.sql` (cagra, ivfpq, PR6); `cases/vector/vector_filtered_search_ivfflat.sql` (ivfflat) |
| 9 | Each "short when" cell of the short-page matrix that a fixture reaches: with a cap set below what the filter needs, the `iterative` page is short; with the cap raised, it fills. ivfflat `pre` searches one round and is short on the issue fixture. hnsw `pre` and `iterative` fill at `max_itopk` = k on the tested fixtures. | the three files of claim 8 |
| 10 | `post` runs one search: `max_itopk` and `max_nprobe` without `itopk` do not change its rows. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 11 | `auto` on hnsw, cagra and ivfpq builds `ADAPTIVE_TOP` over `post`, `pre` and `force` (`post` and `force` when `pre` would equal `post`) and returns the `force` rows where `post` and `pre` are short and where they fill, with a prepared `LIMIT`. | `cases/vector/vector_filtered_search_hnsw.sql` (hnsw), `gpu_cases/vector/vector_filtered_search_gpu.sql` (cagra, ivfpq) |
| 12 | A hybrid `MATCH` + vector top-k on hnsw uses the fulltext index and the vector index through the PRE shape and returns the same rows as the table without a vector index; with a query vector from a provider row it keeps the exact sort. cagra and ivfpq keep the exact sort under `MATCH`. | `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq) |
| 13 | ivfpq with `max_nprobe = n_lists` returns min(`LIMIT`, qualifying) rows in `iterative`. | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 14 | Literal and prepared `LIMIT` give the same rows under the same knobs. | the three files of claim 8 |
| 15 | With `gpu_mode` on, `force` on a cagra or ivfpq table runs on the GPU and returns the exact rows. | `gpu_cases/vector/vector_filtered_search_gpu.sql` (PR7) |
| 16 | ivfflat rounds scan each list once: a query vector that ties every centroid (a zero vector under cosine) returns no duplicate rows. | `cases/vector/vector_filtered_search_ivfflat.sql` |
| 17 | ivfflat `include` builds the plan of `iterative` and returns its rows; `pre` does not use INCLUDE columns. | `cases/vector/vector_filtered_search_ivfflat.sql`, the existing `mode=include` cases under `cases/vector/` |
| 18 | An ivfpq search whose filter on an INCLUDE column admits no row in the probed lists returns no row instead of an error, in every mode. | `gpu_cases/vector/vector_filtered_search_gpu.sql`; `cgo/cuvs/test/ivf_pq_test.cu` `FilterEmptyInProbedListsPadsWithSentinels` |

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
  query vector from a provider row keeps the exact sort; `mode=auto` is an error until PR4.
- The default `max_itopk` of a widening hnsw search is 20000 (pgvector's
  `hnsw.max_scan_tuples` default).
- Without `itopk`, each algorithm keeps its own over-fetch table (ivfflat
  `FilteredPostModeLimit`, hnsw/cagra/ivfpq `PostFilterLimit`).

2026-10-13 (after PR3, Eric):

- All hnsw modes come before cagra and ivfpq, which follow hnsw as the reference.
- `post` runs one search; there is no `post` widening. pgvector's iterative scan filters while it
  scans, which is `pre` in MatrixOne's terms. `max_itopk` / `max_nprobe` cap `pre` only.
- The over-fetch tables stay; whether `itopk` defaults replace them is decided after the modes
  are complete, since they set today's `post` behavior.

2026-10-13 (PR4):

- hnsw `auto` uses the ivfflat `ADAPTIVE_TOP` builder, generalized; only an explicit
  `mode=auto` selects it on hnsw.
- The statement-level auto retry (`ErrVectorNeedRetryWithPreMode`, `Output.IsAdaptive`,
  `rewriteAutoModeToPre`) is removed: no plan set `IsAdaptive`, so it never ran.

2026-10-13 (PR5, Eric):

- ivfflat runs its rounds through `ivfflat.AdvancedSearch`; the reader's include-mode loop is
  replaced, with the same round sizes.
- A PRE search split across readers keeps one shared round and its `nprobe`, as before.
- `mode=pre` also widens when the index search evaluates every filter itself (no membership
  join); `post` and the default mode keep one round there.
- The centroid ranking of a round search is a permutation of the lists. Before, a zero query
  vector under cosine ranked some lists twice and others never, so include-mode rounds
  returned duplicate rows; pre now runs in rounds too.
- Checked against the merge-base `cd8d5abf96` on one data directory, with 2,070 ivfflat
  queries wrapped in an aggregate and 1,782 bare queries (the form that builds
  `ADAPTIVE_TOP`), over four tables (integer and string keys, INCLUDE columns, l2 and cosine),
  every mode, filter shape, LIMIT/OFFSET, `probe_limit` and session default. `post`, the
  default mode, `force` and INCLUDE-column include mode return the same rows. The rows that
  differ are `pre` searches (explicit, the session default, include mode's PRE fallback, and
  `auto`, whose `pre` branch now fills pages it used to pass to `force`) and zero-vector cosine
  queries. None of them is further from the `force` rows than before, except zero-vector
  cosine `auto` pages, whose rows all tie at distance 1 and so equal any `force` page.

2026-10-13 (pre and iterative, Eric):

- `pre` is the filter inside one search, as INCLUDE columns' #24168 kept it ("keep mode=post and
  mode=pre on their original single-round path"); widening is `mode=iterative` (pgvector's
  iterative scan), on every index with membership support. hnsw's widening moved under
  `iterative`.
- `mode=include` stays on ivfflat as `iterative`, its existing behavior; hnsw, cagra and ivfpq
  reject it.
- `auto`'s middle branch stays `pre`, as on main.
- `pre` on ivfflat does not use INCLUDE columns, as on main; `iterative` does.- Checked again against the merge-base `cd8d5abf96` after the split, with the same 2,070
  wrapped and 1,782 bare ivfflat queries on one data directory per comparison: `pre`, `auto`,
  `post`, the default mode and `force` return the rows of main. The rows that differ are
  `include` on tables without INCLUDE columns, whose PRE fallback now widens (29 bare / 34
  wrapped queries), and zero-vector cosine queries (claim 16); none has fewer rows than before
  or a duplicate row.

2026-10-13 (PR6, Eric):

- cagra and ivfpq do not apply a membership key set; no C ABI change. `pre` and `iterative`
  run on their existing GPU filter of INCLUDE columns and the primary key; other filters run
  after the search, in the join (residual filtering). `iterative` widens only when the GPU
  filter holds every filter. Hybrid `MATCH` keeps the exact sort on cagra and ivfpq.
- `auto` on cagra and ivfpq: `[post, pre, force]`, pre only when a filter reads an INCLUDE
  column. Its force branch is the exact sort, which `vector_matmul` runs on the GPU (PR7).
- A pk-space bitmap or sorted key list in the C++ mask was considered and dropped with the
  membership path: a per-row binary search over |S| keys costs O(rows · log |S|) per query.
- The ivfpq host post-filter always runs when a filter is present: its skip assumed padding
  happens only when the whole index holds fewer than `4 · LIMIT` qualifying rows, but IVF-PQ
  pads whenever the probed lists hold fewer than `LIMIT`. Found with `iterative`, present on main.
- `post` on cagra and ivfpq does not use INCLUDE columns, as on ivfflat: primary-key predicates
  run in the search, every other filter after it. The default mode (no option) keeps the
  INCLUDE columns in the search, so plans without a mode do not change.

