# Filtered vector search: post, pre, iterative and auto for every vector index

Owner issue: #28988. Branch `bug_28988`, based on main with IndexSearchScan for all index
searches (#29746). Every claim in "Claims" names the black-box test that proves it.

Status: proposed.

Design trigger: configuration semantics (new `BY RANK` options and session variables), a wire
contract change (one MORPC bump), a plugin extension point (search capabilities), a GPU hot
path, and more than five owning packages.

## Problem and invariant

A vector top-k with a relational filter,

```sql
SELECT id FROM t WHERE category = 1 ORDER BY l2_distance(v, '[0,0]') LIMIT 10;
```

searches the vector index for a fixed candidate budget and then applies the filter. When the
nearest candidates fail the filter, the page comes back short or empty while qualifying rows
exist:

- the issue fixture (1,000 rows, the nearest 500 have `category = 0`): hnsw returns 0 of 10 rows;
  the exact path returns ids 501..510;
- a 100k × 128-d benchmark with a ~5% filter: recall@10 is 0.18 for hnsw and 0.33 for ivfflat in
  the default mode, against 0.998 for pgvector 0.8 with an iterative scan.

`mode=pre|post|auto` (`BY RANK WITH OPTION`) is honored by ivfflat only; hnsw, cagra and ivfpq
check for `force` alone and ignore every other mode. Under a fulltext `MATCH` they skip the
vector index and run an exact sort.

### Non-goal: no mode returns exactly `LIMIT` rows in general

Approximate search under a filter can return fewer than `LIMIT` rows while qualifying rows
exist. This design narrows *when* that happens, makes the budget visible and tunable, and states
the remaining cases per algorithm and mode in the short-page matrix. `mode=force` (exact search
without the index) is the only mode that never returns a short page.

### Goals

1. `mode=pre` on every vector index: the filter runs inside one index search.
2. `mode=iterative` on every vector index: `pre` whose search widens while the page is short,
   up to a cap the user can set.
3. `mode=auto` on every vector index.
4. Hybrid fulltext + vector top-k on hnsw uses both indexes.
5. The knobs and the filter the index search applies are visible in `EXPLAIN`.
6. With `gpu_mode` on, the cagra and ivfpq searches and their widening never fall back to a CPU
   search.

Success criteria: the issue fixture returns ids 501..510 through the index in `pre` and
`iterative` on hnsw, a full page in `pre` and `iterative` on cagra, and ids 501..510 in
`iterative` on ivfpq and ivfflat; every short-page cell a fixture reaches has a test; queries
without a mode or knob return the rows they return on main.

## Precedent

| System | Mechanism |
|---|---|
| pgvector 0.8 | `hnsw.iterative_scan` / `ivfflat.iterative_scan`: the index scan is a resumable iterator; the executor filters each tuple and keeps pulling until `LIMIT` is met or `hnsw.max_scan_tuples` (total tuples produced) / `ivfflat.max_probes` is reached. The page can be short at the cap. With `iterative_scan = off`, one scan of `hnsw.ef_search` / `ivfflat.probes` is filtered once. |
| Vespa | `targetHits` (internal candidates) with `approximate-threshold` / `filter-first-threshold` chosen from the estimated hit ratio. |
| Lucene / OpenSearch | filtered HNSW (bitset during traversal), internal budget `num_candidates`, output `k`. |
| Weaviate, Qdrant, Milvus | allow-list or bitset pre-filter during graph search; exact scan below a cardinality cutoff. |
| cuVS CAGRA | bitset filter; `search_params.filtering_rate` (auto) widens the internal `itopk` from the filter rate. |

pgvector filters each tuple while its scan runs, so the scan learns how many rows qualify: in
MatrixOne's terms that is `iterative`. MatrixOne's `post` filters in the join after the index
search has returned, like pgvector with `iterative_scan = off`; its `pre` is the filter inside one
search. usearch (hnsw) and cuVS (cagra, ivfpq) return a fixed top-k per call and expose no resume
or list-subset API, so their widening restarts the search with larger knobs. ivfflat has a
resumable cursor (`IvfSearchCursor`), so its widening continues.

## Semantics

### Modes

| mode | meaning |
|---|---|
| `post` | one index search; filters on the primary-key column run inside it, every other filter (INCLUDE columns too) after it; on ivfflat a filter on an INCLUDE column adds a `force` fallback for an empty page |
| no option (default) | `post`, except that filters on INCLUDE columns also run inside the index search |
| `pre` | the filter runs inside one index search (per algorithm below) |
| `iterative` | `pre` whose search widens while the page is short, up to `max_itopk` / `max_nprobe` |
| `include` | deprecated: an error on every index, `mode=include is deprecated. use mode=pre, post, iterative` |
| `auto` | `ADAPTIVE_TOP [post, pre, force]`: the next branch runs when the previous one is short |
| `force` | exact search without the vector index |

How each index runs the filter inside its search:

| | `pre` | `iterative` |
|---|---|---|
| hnsw | membership: the keys of the filtered rows (PRE shape) | membership, widening |
| ivfflat | INCLUDE-column and primary-key filters in the search; any other filter through membership | as `pre`, widening |
| cagra, ivfpq | INCLUDE-column and primary-key filters in the GPU search; any other filter through membership, ANDed into the GPU filter mask | as `pre`, widening |

`mode=include` is an error on every index. Every vector index supports `pre`,
`iterative` and `auto`; the capability check is by interface (below), so an index that supports
neither membership nor an index filter rejects them.

### Knobs

Set in `BY RANK WITH OPTION`, one `key=value` per string literal, by index family:

```sql
... ORDER BY l2_distance(v, q) LIMIT 10 BY RANK WITH OPTION 'mode=iterative', 'itopk=200', 'max_itopk=5000';
```

| family | knob | meaning |
|---|---|---|
| graph: hnsw, cagra | `itopk`, `max_itopk` | internal candidate list (search breadth): start / cap |
| IVF: ivfflat, ivfpq | `nprobe`, `max_nprobe` | lists probed: start / cap; `max_nprobe = n_lists` probes every list |
| IVF: ivfflat, ivfpq | `itopk`, `max_itopk` | candidates returned from the probed lists: start / cap |

A knob outside the index's family is an error; values are positive integers, `nprobe` at most
80000 (the `probe_limit` range), and a cap is at least its start. An unknown key, or a string
literal that is not one `key=value` pair, is an error. `LIMIT` rows are
returned; the knobs only size the internal search.

With `itopk` set, the candidate budget of a search is `max(itopk, LIMIT)`, lowered to
`max_itopk`; without it, the budget is the algorithm's over-fetch of `LIMIT`. `pre` and `post` run
one search with that budget. Otherwise the caps bound only the widening of `iterative`.

Defaults and precedence (most specific wins):

| knob | SQL option | session variable | index parameter | built-in |
|---|---|---|---|---|
| `itopk` | `itopk` | `vector_itopk` | `ef_search` (hnsw), `itopk_size` (cagra) | the over-fetch of `LIMIT` |
| `max_itopk` | `max_itopk` | `vector_max_itopk` | — | `iterative`: 20000 (hnsw), 1024 (cagra) |
| `nprobe` | `nprobe` | `probe_limit` | — | `probe_limit` default |
| `max_nprobe` | `max_nprobe` | `vector_max_nprobe` | — | `iterative`: every list |

The session variables default to 0 (unset). `probe_limit` keeps its name, range, default and
plan-time read. The plan carries only the SQL options. `vector_itopk`, `vector_max_itopk` and
`vector_max_nprobe` are read for every execution and never stored in a reusable plan: a scan on
the session's CN reads them when it builds its search request, and the coordinator writes them
into the per-message copy of a scan it sends to another CN. A `SET` after `PREPARE` applies to
the next `EXECUTE`. A session knob is written only into a scan of an algorithm that uses it
(`itopk`, `max_itopk`: vector indexes; `max_nprobe`: ivfflat, ivfpq).

### Which knob acts, per mode

| | `iterative` | `pre` | `post` |
|---|---|---|---|
| hnsw | `itopk` start, `max_itopk` cap | `itopk` | `itopk` |
| cagra | `itopk` start, `max_itopk` cap; cuVS `filtering_rate` (auto) scales the internal list | `itopk` | `itopk` |
| ivfpq | `nprobe` start, `max_nprobe` cap; `itopk` per round | `nprobe`, `itopk` | `nprobe`, `itopk` |
| ivfflat | `nprobe` start, `max_nprobe` cap; `itopk` matters with a bloom membership (non-integer key) | `nprobe`, `itopk` | `nprobe`, `itopk` |

A knob with no effect in the chosen mode is accepted, so the same SQL runs in every mode.

### Predictive start

In `iterative` with a membership set, the selectivity is exact: `|S|` qualifying keys over `N`
indexed rows (hnsw) or `n_lists` lists (ivfflat).

- hnsw starts at the larger of the candidate budget and `k · N / |S| × 2`, at most `max_itopk`.
- ivfflat starts at the larger of `nprobe` and `n_lists · budget / |S| × 2`, at most `max_nprobe`.
- cagra starts at the candidate budget and leaves cuVS `filtering_rate` on auto, so the filter
  rate is applied once, by cuVS. `max_itopk` caps what MatrixOne passes; cuVS may scale its
  internal list above it.
- ivfpq starts at `nprobe`: its filter runs in the GPU search, which reports no qualifying count
  before the search.

## Per algorithm

### hnsw (CPU, usearch)

- Search: `HnswModel.Search` per sub-index, merged into a global top-k
  (`pkg/vectorindex/hnsw/search.go`). With a membership set it calls
  `usearchex.FilteredSearchUnsafeWithMembership`: a C predicate tests each usearch key (the
  BIGINT primary key) against the membership filter (cbitmap or sorted64), one cgo crossing per
  sub-index search. usearch navigates through non-matching nodes and keeps only matching keys.
- Knob mapping: usearch searches with `max(itopk, ef_search)`, so a larger count widens the beam
  for that call without changing the shared index's `ExpansionSearch`.
- `pre`: one membership search with the candidate budget.
- `iterative`: `hnsw.IterativeSearch` (`pkg/vectorindex/hnsw/iterative.go`) restarts with
  `itopk × 2` up to `max_itopk`, bounded by the indexed rows, until `LIMIT` rows (at most `|S|`)
  are found. An exact empty key set returns no rows without searching.
- Plan: `pre`, `iterative`, a membership join or a fulltext `MATCH` filter build the PRE shape. A
  query vector from a provider row runs the search per row in APPLY, which receives no membership
  runtime filter; that query keeps the exact sort.
- `auto`: `ADAPTIVE_TOP [post, pre, force]`; the pre branch is left out when it would equal
  post (no filter, or a `MATCH` filter, under which post already searches the matching keys).
  Only an explicit `mode=auto` selects it; the session defaults
  `enable_vector_auto_mode_by_default` and `enable_vector_prefilter_by_default` apply to ivfflat.
- Ranking: exact distances on the stored vectors.

### cagra (GPU, cuVS)

- Search: cuVS CAGRA with a per-query bitset built by `build_filter_host_mask`
  (`cgo/cuvs/index_base.hpp`) from `FilterJSON` (the filters on INCLUDE columns and the primary
  key the mode admits) AND the membership AND not-deleted. The membership (`mo_membership_t`,
  `cgo/membership.h`, shared with usearch) is the key set of a PRE plan, built in C memory by
  `docfilter.NewForC` for every C/C++ consumer (hnsw, cagra, ivfpq): a cbitmap when its span
  bitmap is eligible, else the smaller estimate of a CRoaring bitmap (128 B per 2^16-value
  container + 4 B per key) and a sorted64 array (8 B per key). The estimate is reserved with
  the CN memory admission before the build and released by the last `Free`, as for cbitmap.
  The consumers accept only memberships in C memory (`docfilter.CMembershipOf`): these
  three, or a C bloom filter for usearch. cuVS takes only the three exact ones
  (`docfilter.CExactMembershipOf`); neither receives a bloom filter, the keys being int64). A Go-owned sorted64 is an error, never
  searched as no membership.
  `eval_filter_bitmap_cpu_fused` (`cgo/cuvs/filter.hpp`) tests each row's `host_ids` key in
  the same pass as the predicates, so one bitset reaches cuVS, with no allocation for the
  membership: a cbitmap is read in place with an inline bit test, CRoaring and sorted64 with
  their `contains`. An index without `host_ids` rejects a membership.
  The search's filter mask and the delete-bitmap copy ANDed into it (rows/8 bytes of CPU
  memory each) are reserved with the CN memory admission for the duration of the search
  when together they are 1 MiB or more (`vectorindex.AdmitFilterMask`); a refusal fails the
  query. In `pre` and `iterative`, and under a membership join or a `MATCH` filter, a filter
  outside the INCLUDE columns builds the PRE shape; in the default mode and `post` it runs
  after the search, in the join. A distance range predicate (`l2_distance(v, q) < K`) runs
  on the search output and never builds the PRE shape.
- Knob mapping: `itopk_size = max(itopk, LIMIT)`; `filtering_rate` stays auto.
- Limit: one cuVS CAGRA search returns at most 1024 keys. `itopk` or `max_itopk` above 1024 as
  a SQL option is an error; a session `vector_itopk` or `vector_max_itopk` above 1024 is lowered
  to 1024, so a value set for hnsw does not fail cagra queries. The `iterative` default cap is
  1024.
- `iterative` (`ScanOptions.Iterative`): `cagra.IterativeSearch`
  (`pkg/vectorindex/cagra/iterative.go`) restarts with `itopk × 2` up to `max_itopk`, bounded by
  the indexed rows, until `LIMIT` rows qualify, or every member of a smaller membership.
- `auto`: `ADAPTIVE_TOP [post, pre, force]`; the pre branch only when a filter reads an INCLUDE
  column, since `pre` equals `post` otherwise.
- Ranking under a filter: a full page can miss nearer qualifying rows when the filter removes
  the rows between the query and them. On 1,000 rows on one line, `graph_degree` 8 and
  `id between 501 and 1000` from the origin, `pre` and `iterative` return 583, 617, 631, 647, 673
  with the default `itopk_size` 64 and 501..505 with `itopk` 256. `iterative` widens on the row
  count, so it does not widen a full page; a larger `itopk` is the remedy.
- Ranking: exact distances on the stored vectors.

### ivfpq (GPU, cuVS)

- Search: cuVS IVF-PQ with the same per-query bitset as cagra. cuVS fills the slots the probed
  lists cannot fill with rows the bitset excludes; the host post-filter
  (`apply_host_post_filter_locked`) replaces each of them with a sentinel whenever a filter is
  present, so a filter the probed lists do not hold returns no row instead of an error.
- Knob mapping: `n_probes = nprobe`; `k` = the candidate budget.
- Filters: as cagra, membership included; the host post-filter also drops slots the
  membership excludes.
- `iterative` (set as for cagra): `ivfpq.IterativeSearch` (`pkg/vectorindex/ivfpq/iterative.go`)
  restarts with `nprobe × 2` up to `max_nprobe` (every list when unset). `ivf_pq::search` has no
  list-subset or resume API, so each round re-scans the earlier lists; with doubling, all rounds
  cost at most about twice the final round. `max_nprobe = n_lists` probes every list.
- `auto`: as cagra.
- Ranking: PQ distances (approximate) in every mode, including `n_probes = n_lists`; the index
  holds PQ codes only.

### ivfflat (CPU, GPU centroids)

- Search: `IvfflatSearchIndex.Search` (`pkg/vectorindex/ivfflat/search.go`): the
  `IvfSearchCursor` ranks the centroids once; a round scans the next lists' entries with the
  membership set and the filters the index search evaluates (INCLUDE columns and the primary
  key), up to its row limit.
- Filters in the search: a single-column primary key in every mode; INCLUDE columns in every
  mode but `post` (default, `pre`, `iterative`, `auto`). A filter on an INCLUDE
  column in the search admits only qualifying entries, so a round returns more qualifying rows
  than when the filter runs after it.
  No membership join is built when the index search evaluates every filter. Otherwise the
  membership producer scan applies every filter, those the index search evaluates included, so
  the key set holds only rows that pass all of them. A membership set of 100 keys or fewer
  (`exactPkFilterThreshold`) scans those keys in every list.
- `pre`, `post` and the default mode search one round of `nprobe` lists. `post` with a filter on
  an INCLUDE column builds `ADAPTIVE_TOP [post, force]` where a non-empty `post` page wins, so an
  empty page runs the exact sort.
- `auto`: the `post` branch probes `ceil(nprobe × sqrt(1 / selectivity))` lists, at least `nprobe`
  and at most `n_lists`, from the scan's estimated filter selectivity. `nprobe` is the `nprobe`
  option when set, else `probe_limit`: `'mode=auto', 'nprobe=4'` at selectivity 0.25 probes 8
  lists. Without a selectivity estimate or the list count, the branch probes `nprobe` lists.
- `iterative`: `ivfflat.IterativeSearch` (`pkg/vectorindex/ivfflat/iterative.go`) runs in the
  reader (`plan_reader.go`). Each further round continues the cursor with twice the previous
  round's lists (at most 4096 per round) until the reader holds the candidate budget (at most
  `|S|` for an exact set), all lists are probed, or the lists probed reach `max_nprobe`. A
  search split across readers (local DOP or one reader per CN, for large entries tables) shares
  one route and searches one round: no reader holds the merged row count.
- Centroid ranking: the rounds walk one permutation of the centroid ids
  (`completeCentroidRanking`). A query vector whose centroid distances tie or are not numbers
  (a zero vector under cosine) has its duplicate centroids dropped and the missing lists appended
  in id order, so each list is scanned once.
- `EXPLAIN ANALYZE` prints the rounds (`round_count`).
- Ranking: exact on the stored entries; quantized entries (`quantization` option) rank by the
  quantized values.

### Ranking precision by storage type

| storage | ranking |
|---|---|
| native f32 / f16 / bf16 / int8 / uint8 columns | exact on the user's values (hnsw, cagra, ivfflat) |
| scalar-quantized storage of an f32/f16 column | exact on the quantized values |
| ivfpq | PQ-approximate |

## Short-page matrix

"Short" means fewer than min(`LIMIT`, qualifying rows). A cell reads "short when ...".

| | post | pre | iterative | auto | force |
|---|---|---|---|---|---|
| hnsw | the filter rejects more candidates than the one search returned | the one search misses qualifying nearest rows | `max_itopk` reached first | never | never |
| cagra | as hnsw | the one search misses qualifying rows | `max_itopk` reached | as hnsw | never |
| ivfpq | as hnsw, over `nprobe` lists | the `nprobe` lists hold too few qualifying rows | `max_nprobe < n_lists` reached | as hnsw | never |
| ivfflat | as ivfpq; with a filter on an INCLUDE column, never empty in a bare top-k | the `nprobe` lists hold too few qualifying rows, also when only a few rows pass a filter on INCLUDE columns and the primary key (it runs in the search; no exact membership set); never for 100 keys or fewer through membership | `max_nprobe < n_lists` reached, or the search split across readers; never for 100 keys or fewer | as hnsw | never |

`auto` runs `force` after a short `pre` (or a short `post` when it has no pre branch), and
`force` is never short.

## Architecture

### Two layers

- **Regular search**: the algorithm's single search call (Go reader, plus the C++ engine for
  cagra and ivfpq): query, knobs and filter in, candidates out.
- **Iterative search**: one Go file per algorithm, `pkg/vectorindex/<algo>/iterative.go`. It owns
  the start, the rounds and the caps; each round is the regular search (for cagra and ivfpq a cgo
  call into cuVS). It computes no distances. A scan that is not `iterative` calls the regular
  search directly.
- Each policy implements `searchplugin.IterativeSearchHooks` (`pkg/indexplugin/search/hooks.go`):
  `StartRound` returns the first `Round{ITopK, NProbe}` from the request and the `SearchStats`
  known to the caller (indexed rows, members, lists, lists probed); `NextRound` returns the next
  round after one that returned too few rows, or stops.

### Capabilities

Optional search-plugin interfaces next to `CandidateBudgetHooks`:

```go
type MembershipHooks interface{ SupportsMembership() bool }   // hnsw, ivfflat, cagra, ivfpq
type IndexFilterHooks interface{ SupportsIndexFilter() bool } // cagra, ivfpq, ivfflat
```

`pre`, `iterative` and `auto` need either. They replace the algorithm-name
checks in `vectorIndexSupportsContext` (`pkg/sql/plan/apply_indices.go`) and
`apply_indices_vector.go`. The mode check runs after an index proves it serves the query, so an
index the planner does not use cannot reject the query.

### PRE plan shape

```
INNER JOIN(scan, SEMI(IndexSearchScan, filtered scan → PROJECT pk))
```

with a membership runtime filter (`UseMembershipFilter`, `MustApply`) from the filtered scan to
the `IndexSearchScan`, passed to the reader as `Request.MembershipFilter`. One builder,
`buildVectorPreFilterJoin` (`pkg/sql/plan/apply_indices_vector_pre.go`), serves every vector
index; ivfflat passes its IVF-only steps as hooks. ivfflat, cagra and ivfpq pass the filters
their index search evaluates to the copied scan too (`producerFilters`), so the key set holds
only rows that pass every filter. The hnsw, cagra and ivfpq readers build the exact key set
with `vectorindex.BuildMembership` and reject `MembershipFilterRequired` without a filter.

### Auto

`buildAdaptiveVectorTop` (`pkg/sql/plan/apply_indices_vector_auto.go`) copies the vector top-k
region, annotates the copies `post`, `pre` and `force`, and asks the algorithm's plan rewrite for
the post and pre branches. Each copy binds its index search under its own alias
(`mo_<algo>_alias_N`, the smallest unused `N`). `ADAPTIVE_TOP` (`pkg/sql/colexec/adaptivetop`)
runs the branches in order and returns the first full page, or the last branch's page. A region
whose SELECT outputs more than one column, or that reads a volatile expression, a membership
join or a provider row, or that the planner estimates at `LIMIT × 2` rows or fewer, runs `force`.
`ADAPTIVE_TOP` needs a projection boundary over the top-k, which a bare top-k query has; an
`auto` top-k inside an outer query (a derived table under an aggregate) runs `force`.

### Hybrid fulltext + vector

A `MATCH` in the scan's filters is one more filter the PRE shape evaluates in the filtered scan.
hnsw builds the PRE shape for it, so its hybrid top-k uses both indexes and returns the exact
matching rows; so do cagra and ivfpq, whose GPU mask ANDs the matching keys. ivfflat keeps its
post-filtered behavior. On every index the PRE shape runs a filter-only `MATCH` once, in the
membership producer: the row-fetch scan drops it once both primary-key joins are built (#30178,
shared by `buildVectorPreFilterJoin`). A `MATCH` whose score the query also reads runs on both.

### Post: one search

The residual filter runs in the join above the `IndexSearchScan`, after the search has returned,
so the search cannot count the rows that survive it and has no signal to widen on. Only
primary-key predicates run inside it: every index entry carries the key (`__mo_index_pri_col` on
ivfflat, `host_ids` on cagra and ivfpq). A short `post` page is filled by `iterative`, or by
`auto`.

### End-to-end flow (iterative on cagra)

1. The planner serializes the filters on INCLUDE columns and the primary key to `FilterJSON`
   and sets `Iterative` when no other filter remains on the scan.
2. `CagraSearch.Search` runs the rounds of `cagra/iterative.go`, each the regular search
   through cgo.
3. C++ builds the per-query bitset (filter AND not-deleted) under the shared lock and uploads
   it; cuVS searches with no MatrixOne lock held.
4. While fewer than `LIMIT` keys came back and the cap allows, step 2 repeats with doubled
   `itopk`.
5. The candidates join back to the table; the top `LIMIT` rows are returned.

## GPU policy

With `gpu_mode` on, cagra and ivfpq search and widen on the GPU only; a GPU out-of-memory error
is returned as an error. The relational filtered scan, the host-side mask build
(`build_filter_host_mask`) and hnsw are CPU by design. `force` is the exact sort without the
index and is unchanged by this design. ivfflat's PRE with a required membership and several
partitions routes its centroid search on the CPU (`ivfflat/plan_reader.go`).

## Compatibility

- **Wire**: `IndexSearchScan.internal_topk`, `max_internal_topk` and `max_nprobe` (`nprobe` is the
  IVF algorithms' existing `algo_options` probe count) with a dedicated MORPC version,
  `vectorscan.SearchKnobsProtocolVersion`. A scan carrying any of them, from the SQL option or a
  session variable, requires that version on every CN that runs it; a scan without them requires
  `vectorscan.IndexSearchScanProtocolVersion`.
- **Membership**: no version change; an older hnsw, cagra or ivfpq reader refuses
  `MembershipFilterRequired`, so a CN of an older build fails a PRE plan instead of ignoring the
  filter. The cuVS filtered-search C functions take a `mo_membership_t`; libmo and the Go
  bindings ship together.
- **Defaults**: the default mode is unchanged; widening runs only under `mode=iterative`.
  `mode=include`, ivfflat's earlier name for it (#24168), is an error naming its replacements.
- **Scan options**: `iterative` is a field of the hnsw, cagra, ivfpq and ivfflat `algo_options`
  JSON. A CN of an older build ignores it: hnsw, cagra and ivfpq search once; ivfflat widens a
  scan marked multi-round (`BucketExpandStep`) and searches a membership scan once.
- **Observability**: `EXPLAIN` prints `Candidate Limit`, ivfflat's and ivfpq's `NProbe`, the SQL
  options `Internal Top-K`, `Max Internal Top-K` and `Max NProbe` when set, and the filter the
  search applies (`Index Filter`: ivfflat's pre-filters; cagra's and ivfpq's `FilterJSON` as SQL).
  Session-variable values are not in the plan and are not printed.

## Resources and performance

- **Membership filter (hnsw)**: built once per reader through the docfilter memory admission,
  shared read-only by the sub-index searches, freed when the reader closes.
- **GPU mask**: built per query; never written into the shared delete bitsets or their device
  caches, and never bumps their version. `id_to_index_` is not used.
- **Rounds**: bounded by the caps, about `log2(cap / start)`. Each cagra/ivfpq round rebuilds
  the host mask (one pass over `host_ids`); ivfpq rounds re-scan earlier lists, about twice the
  final round in total; ivfflat rounds do not re-scan.
- **No lock is held across a GPU call** (`CLAUDE.md`).

## Alternatives

| Alternative | Decision |
|---|---|
| A fixed over-fetch multiplier only | Rejected: the issue's 0/10 and recall 0.18 come from it. The over-fetch stays the budget when `itopk` is unset. |
| An exact tier inside `pre` for small qualifying sets, or an `exact` option | Rejected: `pre` is a filtered ANN; `mode=force` is the exact path. |
| One generic iterative search for all algorithms | Rejected: knobs and widening differ (restart vs continue, beam vs probes); one policy per algorithm. |
| The widening loop in C++ | Rejected: the loop is policy, kept in Go next to the other algorithms'; C++ keeps the single search. |
| Membership key sets in the cagra/ivfpq C++ mask | Accepted: the mask pass already visits every row for the predicates and deletes; the membership adds one probe per row whose predicates pass (a cbitmap bit test inline, a CRoaring or sorted64 `contains`), no allocation and no GPU code. |
| Membership format for C/C++ consumers | cbitmap first, else the smaller estimate of CRoaring and a C sorted64, all in C memory and admitted before the build. A Go-owned sorted64 copied into C++ would double the memory, and CRoaring alone is about 90 B per key when every key has its own container (11x sorted64). Go consumers (ivfflat, fulltext) keep cbitmap or Go sorted64. |
| ivfpq exact ranking (decode codes, or read raw vectors) | Rejected: decoded PQ codes are approximate; raw vectors need a table read inside the index path. |
| Widening in `post` | Rejected: `ADAPTIVE_TOP` starts each compiled branch once; re-running means restarting a running scope tree, and unrolled rounds repeat the table join. `iterative` and `auto` fill a short `post` page. |
| pgvector's `max_scan_tuples` name | Rejected: `max_itopk` / `max_nprobe` match the per-round knobs of usearch and cuVS; `max_itopk` ≈ `max_scan_tuples` for the final round, `max_nprobe` = `ivfflat.max_probes`. |

## Claims

`cases/` and `gpu_cases/` are under `test/distributed/`.

| # | Claim | Black-box test |
|---|---|---|
| 1 | A query without a mode or knob returns the rows it returns on main, on all four indexes, except an ivfflat query vector that ties every centroid (claim 15). | the existing case files that create an ivfflat, hnsw, cagra or ivfpq index under `cases/vector/` and `gpu_cases/vector/` |
| 2 | `pre`, `iterative` and `auto` are accepted on hnsw, ivfflat, cagra and ivfpq; `include` is rejected on every index as deprecated. | `cases/vector/vector_filtered_search_modes.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 3 | `itopk` and `max_itopk` are accepted on every vector index, `nprobe` and `max_nprobe` on ivfflat and ivfpq only; values must be positive integers with a cap at least its start. | `cases/vector/vector_filtered_search_knobs.sql`, `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` |
| 4 | `EXPLAIN` shows `Internal Top-K`, `Max Internal Top-K`, `Max NProbe` and `NProbe` from the SQL options. | `cases/vector/vector_filtered_search_knobs.sql`, `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` |
| 5 | A `SET vector_itopk` or `SET vector_max_nprobe` after `PREPARE` applies to the next `EXECUTE` and stops applying once reset; the SQL option wins over the session variable. | `cases/vector/vector_filtered_search_knobs.sql` |
| 6 | With one search, `itopk` covering the index returns a full page on the issue fixture where the default budget returns none, on all four indexes (ids 501..510 on all but cagra, whose graph build is not deterministic); `max_itopk` lowers `itopk`. | `cases/vector/vector_filtered_search_knobs.sql` (hnsw, ivfflat), `gpu_cases/vector/vector_filtered_search_knobs_gpu.sql` (cagra, ivfpq) |
| 7 | `post` runs one search: `max_itopk` and `max_nprobe` without `itopk` do not change its rows. | `cases/vector/vector_filtered_search_knobs.sql` |
| 8 | `post` runs only primary-key filters inside the index search; the default mode also runs INCLUDE-column filters there (cagra, ivfpq, ivfflat); ivfflat `pre` and `iterative` also run them there, `pre` without a membership join when every filter is on an INCLUDE column or the primary key; ivfflat `post` with a filter on an INCLUDE column runs `force` when its page is empty. | `cases/vector/vector_filtered_search_ivfflat.sql`, `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 9 | The issue fixture returns ids 501..510 in `pre` and `iterative` through the hnsw index, for l2 on a five-sub-index table and for cosine, where the default mode returns none. | `cases/vector/vector_filtered_search_hnsw.sql`, `cases/vector/vector_filtered_search_modes.sql` |
| 10 | With the filter on an INCLUDE column, the issue fixture returns a full page in `pre` and `iterative` on cagra, and ids 501..510 in `iterative` on ivfpq where `pre` with `nprobe=1` returns none; with a filter outside the INCLUDE columns `pre` and `iterative` run it through membership and return the `force` rows. | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 11 | `iterative` (hnsw, ivfflat) and `pre` (hnsw) return zero rows for zero qualifying rows, all qualifying rows when fewer than `LIMIT` qualify, and the `force` rows when exactly or more than `LIMIT` qualify, with periodic, `IN`, combined and `NULL` filters, `OFFSET` and a prepared `LIMIT`. | `cases/vector/vector_filtered_search_hnsw.sql`, `cases/vector/vector_filtered_search_ivfflat.sql` |
| 12 | Each short-page cell a fixture reaches: with `max_nprobe` below what the filter needs the `iterative` page is short on ivfflat and ivfpq, and with the cap raised it fills; ivfflat and ivfpq `pre` search one round and are short on the issue fixture; ivfflat `pre` with a filter on INCLUDE columns and the primary key that 10 rows pass is short where `iterative` fills; hnsw `iterative` fills at `max_itopk` = `LIMIT`. | `cases/vector/vector_filtered_search_ivfflat.sql`, `gpu_cases/vector/vector_filtered_search_gpu.sql`, `cases/vector/vector_filtered_search_hnsw.sql` |
| 13 | A bare `auto` top-k builds `ADAPTIVE_TOP` on hnsw, cagra and ivfpq; on hnsw it has a pre branch only with a filter, returns the `force` rows where `post` is short and where it fills, with a prepared `LIMIT`, and runs `force` for a region with more than one output column; on cagra and ivfpq it returns a full page where `post` is short. | `cases/vector/vector_filtered_search_hnsw.sql` (hnsw), `gpu_cases/vector/vector_filtered_search_gpu.sql` (cagra, ivfpq) |
| 14 | A hybrid `MATCH` + vector top-k on hnsw uses the fulltext index and the vector index through the PRE shape and returns the rows of the table without a vector index; with a query vector from a provider row it keeps the exact sort. cagra and ivfpq use the PRE shape under `MATCH` and for a primary-key join with another table: cagra returns the rows of the table without a vector index, ivfpq a full page of matching rows ranked by PQ distance. | `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq) |
| 15 | ivfflat rounds scan each list once: a query vector that ties every centroid (a zero vector under cosine) returns no duplicate rows in `iterative`. | `cases/vector/vector_filtered_search_ivfflat.sql` |
| 16 | ivfflat `pre` and `iterative` build the membership key set from every filter: an INCLUDE-column filter narrows the keys as well as the index search. | `cases/vector/vector_filtered_search_ivfflat.sql` |
| 17 | An ivfpq search whose filter on an INCLUDE column admits no row in the probed lists returns no row instead of an error, in the default mode, `pre` and `iterative`. | `gpu_cases/vector/vector_filtered_search_gpu.sql`; `cgo/cuvs/test/ivf_pq_test.cu` `FilterEmptyInProbedListsPadsWithSentinels` |
| 18 | `EXPLAIN` shows the filter the cagra and ivfpq search applies as SQL (`Index Filter`). | `gpu_cases/vector/vector_filtered_search_gpu.sql` |
| 19 | On a cagra index over more than 1024 rows where fewer than `LIMIT` rows qualify, `iterative` returns the qualifying rows; `itopk` or `max_itopk` above 1024 is an error as an SQL option and lowered to 1024 as a session variable. | `gpu_cases/vector/vector_cagra_itopk_limit.sql` |
| 20 | On cagra and ivfpq a filter outside the INCLUDE columns in `pre` and `iterative` builds the PRE shape, whose producer scan also applies the INCLUDE filter, and the default mode does not; `iterative` fills the page with qualifying rows (the `force` rows, up to PQ ranking on ivfpq) and an `id IN (subquery)` membership join returns the `force` rows; the GPU mask ANDs a cbitmap, CRoaring or sorted64 membership with the predicates and deletes, also on a shard slice, and rejects other kinds and an index without `host_ids`. | `gpu_cases/vector/vector_filtered_search_gpu.sql`; `cgo/cuvs/test/filter_test.cu` `EvalFilterMembershipTest`, `cagra_test.cu` / `ivf_pq_test.cu` `FilteredSearchAppliesMembership`; `pkg/common/docfilter/cmembership_test.go` `TestNewForCPicksByEstimate`, `TestNewForCAdmissionDenied` |

## Decision log

- No mode is described as returning exactly `LIMIT` rows; the design documents per algorithm and
  mode when a page can be short. `mode=force` is the exact path; there is no exact tier inside
  `pre` and no `exact` option.
- The default mode is unchanged. `post` is one search with only primary-key filters inside it;
  there is no `post` widening.
- `pre` is the filter inside one search; widening is `iterative` (pgvector's iterative scan).
  `include`, ivfflat's earlier name for it, is deprecated: an error on every index. `auto`'s
  middle branch is `pre`.
- ivfflat `pre` runs filters on INCLUDE columns in the index search, as the default mode,
  `iterative`, cagra and ivfpq do; only filters on other columns go through membership. Before
  this design ivfflat `pre` sent them through membership.
- cagra and ivfpq run `pre` and `iterative` on their GPU filter of INCLUDE columns and the
  primary key, and any other filter through membership ANDed into the same mask.
- The ivfpq host post-filter runs whenever a filter is present: IVF-PQ pads with excluded rows
  whenever the probed lists hold fewer than `LIMIT` qualifying rows, not only when the whole
  index does.
- Knobs by family, set in SQL options and session variables, the SQL option first. `probe_limit`
  stays `nprobe`'s session default. The over-fetch tables stay the budget when `itopk` is unset.
- The `iterative` `max_itopk` default is 20000 (pgvector's `hnsw.max_scan_tuples` default) on hnsw
  and 1024 on cagra, the most keys one cuVS CAGRA search returns. cagra rejects a SQL `itopk` or
  `max_itopk` above 1024 and lowers a session value above it: the session variables are shared
  with hnsw.
- One Go iterative search per algorithm on top of the regular search, including the GPU indexes;
  hnsw, cagra and ivfpq restart, ivfflat continues its cursor; cagra leaves `filtering_rate` on
  auto; ivfpq's end state is `n_probes = n_lists` with PQ ranking.
- An `auto` region with more than one output column runs `force`, as before this design:
  `ADAPTIVE_TOP` pins the same output positions in every branch.
- With `gpu_mode` on, cagra and ivfpq never fall back to a CPU search.
- A session knob is written only into a scan of an algorithm that uses it, so a fulltext search
  does not require `vectorscan.SearchKnobsProtocolVersion`.
- Accepted costs: members without an index entry (NULL vectors, rows not yet indexed) make an
  hnsw, cagra or ivfpq `iterative` search widen to its cap; each cagra and ivfpq `iterative`
  round rebuilds its CPU filter mask; the first ivfflat round is not capped at 4096 lists;
  session knobs are resolved per row in a correlated APPLY.
