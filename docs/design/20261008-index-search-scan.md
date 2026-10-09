# Index search scan: vector and fulltext as one

Owner issue: #27453. Branch `bug_27453`, PR #29746. The issue body is the original
proposal; this document is the design as built, plus hybrid fulltext + vector planning.
Every claim in "Claims" names the black-box test that proves it.

Status: awaiting design approval from fengttt.

## Plan node

Every index search — ivfflat, hnsw, cagra, ivfpq, classic fulltext, fulltext2 — is
one plan node, `INDEX_SEARCH_SCAN`, carrying `plan.IndexSearchScan`:

| Field | Meaning |
|---|---|
| `index` | the searched index; `IndexAlgo` selects the plugin |
| `source_table`, `hidden_tables` | `ObjectRef`s of the base table and the index tables, with `PubInfo` for a subscribed table |
| `query_payload` | query vector or MATCH pattern expression |
| `candidate_limit` | result window; absent means no limit (fulltext streams) |
| `distance_function`, `distance_range`, `pre_filters`, `included_columns` | vector search inputs |
| `algo_options` | per-algorithm static options: sonic JSON of `ScanOptions` in `<algo>/plugin/plan/scan_options.go` |
| `algo_exprs`, `algo_expr_names` | per-algorithm row-dependent expressions, evaluated by name |
| `scan_snapshot` | snapshot read timestamp |

The proto carries no algorithm-specific field. The node has a dedicated MORPC protocol
version, one bump for all six index algorithms. A remote CN below that version cannot
receive the node: dispatch to it fails instead of misreading the plan.

## Execution

`pkg/sql/colexec/vectorscan` evaluates the node's expressions into a
`searchplugin.Request` (query payload, result limit, candidate budget, evaluated
`algo_exprs`, membership filter) and calls the plugin's hooks
(`pkg/indexplugin/search/hooks.go`):

| Hook | Role |
|---|---|
| `Hooks.NewReader` | builds the reader of one search |
| `ParallelHooks` | optional partitioned readers |
| `ExplainHooks` | optional EXPLAIN settings line |
| `CandidateBudgetHooks` | optional post-filter over-fetch budget |
| `EmptyScanHooks` | optional check run when the scan is skipped (DROP, NULL query) |

`pkg/indexplugin/search/planreader` is the shared reader: a plugin implements
`Searcher.Next(ctx) → Chunk{Keys, Scores, Include}` and the shared reader emits
batches. The hnsw, cagra, ivfpq, fulltext2 and classic fulltext readers port the executors of
their former table functions.

cagra and ivfpq register only in the GPU build (`pkg/indexplugin/all/all_gpu.go`).

A query with a partitioned scan (a plugin implementing `ParallelHooks`: ivfflat) requires the
current CN among its workers, as worker 0: that CN's partition reads the coordinator's
appendable ranges, and the other partitions run on other CNs. `TestQueryHasPartitionedIndexSearchScan`
covers which scans are partitioned; the worker-0 assignment rests on code review
(`pkg/sql/compile/scheduler.go`); claim 7 runs a partitioned scan on two CNs. Other index
search scans run as one local scope.

## Planning

Plugins build the node through plan hooks; `pkg/sql/plan` holds no per-algorithm
switch. Vector rewrites run in two passes:

- **Early** (`applyVectorIndicesEarly`), before join ordering and before the
  fulltext rewrite: plugins implementing `LogicalSearchHooks.BuildLogicalSearch`.
  ivfflat, hnsw, cagra and ivfpq all implement it.
- **Late** (`applyIndicesForProject` / `applyIndicesForSort`): `Hooks.ApplyForSort`.

A fulltext MATCH is rewritten in the late pass into a join of the base table with a
fulltext search scan.

### Hybrid fulltext + vector

A MATCH filter restricts the vector Top-K to the rows the MATCH keeps. The vector
rewrite admits an algorithm for such a query only if it may serve that restriction
(`vectorIndexSupportsContext`, the same gate as a membership join):

- **ivfflat** rewrites in the early pass: the query becomes
  `JOIN(scan[MATCH filter], vector search scan)`, and the late fulltext pass then serves
  the MATCH filter on that scan. Both indexes are used; the vector side applies ivfflat's
  residual-filter behavior.
- **hnsw, cagra, ivfpq** only post-filter their candidates, which can drop rows of the
  Top-K, so they skip a scan with a MATCH filter. The late fulltext pass serves the MATCH
  and the Top-K is an exact sort over the fulltext hits.
- **A MATCH only in the projection** of the Top-K is served by the fulltext rewrite of
  that projection, which needs the scan directly under it, so no algorithm rewrites the
  scan; the Top-K is an exact sort over the fulltext hits.

hnsw, cagra and ivfpq implement `BuildLogicalSearch`; `vectorIndexSupportsContext` keeps
them off a scan with a MATCH filter (claim 3).

The `BY RANK WITH OPTION 'mode=...'` clause is honored by ivfflat only; hnsw, cagra and
ivfpq ignore it (claim 10), so the hybrid claims are stated without it.

## Compatibility and rollout

Mixed-version operation is not supported, in either direction, and there is no fallback:
a query whose plan needs an index search fails until every CN runs this version.

| Case | Behavior | Evidence |
|---|---|---|
| New coordinator, a CN below the scan's protocol version | `compileIndexSearchScan` refuses the placement before dispatch: "index search scan requires MORPC protocol version N on every CN" | claim 7 (multi-CN, black box) |
| Older coordinator, newer CN, ivfflat search | the receiver refuses, in `decodeScope`, an index search scan without `algo_options` (every planner of this version sets them): "index search scan from an older version is not supported"; it also refuses one below the CN's protocol version (`MOProtocolVersion`) | unit tests `TestRemoteIndexSearchScanFromOlderVersionIsRefused`, `TestVectorScanPartitionTransportAndRollback`; no mixed-binary test |
| Older coordinator, newer CN, hnsw/cagra/ivfpq/fulltext search | a pipeline carrying a removed search table function fails to prepare: "table function NAME is not supported" | unit test `TestPrepareRemovedSearchTableFunction`; no mixed-binary test |
| Rollback to the older version | the rows above, with the roles reversed | no mixed-binary test |

The removed search table functions (`hnsw_search`, `cagra_search`, `ivfpq_search`,
`fulltext2_search`, `fulltext_index_scan`) were built by the planner for index rewrites;
they have no deprecation or migration path. A statement or view that calls one fails with
"table function ... not supported" (claim 8).

## Reader lifecycle

The readers port the table-function executors; each guarantee lives in one owner. None has a
black-box test: each item names the unit test that covers it, or says it rests on code review.

- **Skipped scans** (`pkg/sql/compile/scope.go`, `buildVectorIndexReaders`): a dropped
  runtime filter or a NULL query runs the plugin's `EmptyScan` check and builds empty readers,
  so no reader is opened (`TestBuildVectorIndexReadersRunsEmptyScanHooks`). When a parallel
  factory returns the wrong reader count, every reader it opened is closed (code review).
- **Shared reader** (`pkg/indexplugin/search/planreader`): a cancelled context ends `Read` with
  the cancellation, and `Close` then closes the searcher once
  (`TestReaderStopsOnCancellationAndEmpty`); a searcher error or a malformed chunk is returned
  from `Read` (`TestReaderRejectsMalformedResults`). `Close` is idempotent, and a `Read` after
  `Close` returns end of data (code review).
- **Correlated APPLY** (`pkg/sql/colexec/apply/vector_source.go`): each row closes the previous
  reader before opening the next; end of data closes the reader; `End`, `Reset` and `Free`
  close the reader and then the execution, both idempotent (code review).
- **Prepared reuse** (`prepareIndexSearchScanForExecution`, `pkg/sql/compile/compile.go`): each
  execution builds from the unchanged plan template (code review; claim 9 checks the results of
  re-executions).
- **No-LIMIT and probe-tail buffering**: fulltext2 streams bounded batches through a channel of
  capacity 4 (`TestReadStreaming`, `TestReadStreamingCovered`); `Close` cancels and drains the
  stream and the probe-tail producer (`TestCloseDrains`, `TestProbeTailStreamError`); classic
  fulltext `Close` returns before any read and after a partial read (`TestClassicCloseEarly`);
  that it joins its search goroutine on every exit rests on code review.

## Performance

Each reader calls the same index cache, cuVS and fulltext engine paths as the table function
it replaces. The native change is in cuVS brute force, IVF-Flat and IVF-PQ: a search that
runs with a bitset (a filter or deleted rows) re-tests each returned row on the host, against
the filter mask or by a per-row lookup into the deleted bitset, and replaces a failing row with
a sentinel. This was IVF-PQ's post-filter, moved to the shared base. A search with no filter
and no deleted rows skips it (`cgo/cuvs/test/*_test.cu`).

IVF-PQ, 1M wiki_all rows, dim 768, INCLUDE `file_id`, lists 1024, m 192, k=20,
probe_limit 16, concurrency 8, 5000 queries, RTX 5070 Laptop. Baseline: main on
2026-09-28; this PR: 2026-10-09. Same data, config and recipe (drop index, restart,
create index, two recall passes).

| | main | this PR |
|---|---|---|
| create index | 42 s | 52 s |
| recall@20 | 0.8269 | 0.8297 |
| QPS, pass 1 / pass 2 | 349.1 / 399.1 | 352.5 / 409.4 |
| p50 | 19.12 ms | 18.31 / 18.01 ms |
| p99 | 38.75 ms | 39.42 / 52.11 ms |

The main baseline records one p50/p99 per run; this PR's are pass 1 / pass 2. Pass 2 QPS is
2.6% higher and p50 1.1 ms lower than main; pass 2 p99 is 13 ms higher. Index build code is
not changed by this PR. The run has no deleted rows and no filter, so the changed
post-filter path is not exercised.

Classic fulltext and fulltext2, ranked Top-K: `SELECT id FROM t WHERE MATCH(body)
AGAINST('<2-4 terms>' IN BOOLEAN MODE) LIMIT k`, 200 queries (half common terms, half rare)
on one connection, gojieba parser. Corpus: the first 50,000 1024-word chunks of English
Wikipedia dump part 1 (`enwiki-latest-pages-articles-multistream1.xml-p1p41242`). Harness:
`fulltext/retrieval_topk_3way.py` from mo_vector_benchmark. main = `d2a0055ebe`, the main
merged into this PR; this PR = `4f8ad9ed7c` (later commits change no search code). Each binary ran twice on a fresh instance (round 1: main first; round 2:
this PR first). Average latency in ms, round 1 / round 2:

| | k | main | this PR |
|---|---|---|---|
| fulltext | 10 | 10.35 / 11.35 | 10.06 / 10.88 |
| fulltext | 100 | 10.76 / 12.32 | 10.27 / 11.93 |
| fulltext | 1000 | 12.63 / 16.78 | 11.86 / 14.84 |
| fulltext2 | 10 | 1.14 / 1.17 | 1.07 / 1.13 |
| fulltext2 | 100 | 1.46 / 1.33 | 1.75 / 1.33 |
| fulltext2 | 1000 | 5.47 / 3.42 | 3.02 / 3.24 |
| fulltext build until searchable, s | | 28.4 / 35.7 | 28.2 / 33.5 |
| fulltext2 build until searchable, s | | 22.8 / 26.3 | 22.6 / 26.6 |

Each latency of this PR is within the range of main's two rounds or below it, except
fulltext2 at k=100 in round 1 (1.75 ms against main's 1.46 ms; 1.33 ms on both in round 2).
Build times differ from main's by at most 2.2 s.

ivfflat: its search calls are unchanged; the change on its path is decoding its settings from
`algo_options` once per reader (`NewPlanReader`). 1M wiki_all rows, dim 768, lists 1000,
probe_limit 16, k=20, concurrency 8, 5000 queries, launch config `etc/bench` (8 GB memory
cache). Each binary ran on its own instance (import, restart, create index); recall passes
alternated main, this PR, this PR, main, each after a restart. Pass 2 of each round, round 1 /
round 2:

| | main | this PR |
|---|---|---|
| QPS | 369.5 / 276.8 | 379.8 / 337.0 |
| p50 | 17.04 / 20.70 ms | 17.91 / 18.54 ms |
| p99 | 101.2 / 184.4 ms | 68.8 / 76.6 ms |
| recall@20 | 0.9298 | 0.9349 |

Recall differs because each instance built its own index. Pass 1 (cold) ranged 56–135 QPS on
both binaries.

## Removed

The search table functions `hnsw_search`, `cagra_search`, `ivfpq_search`,
`fulltext2_search` and `fulltext_index_scan` are removed. `TableFunction.
fulltext_source_ref` / `fulltext_index_ref` are removed and their proto numbers
reserved: publisher identity travels on `IndexSearchScan.source_table` /
`hidden_tables`.

## Divergence from the proposal

| Proposal | As built |
|---|---|
| `Hooks.ShapeRequest` + `RequestCore` + `ExprEval` | `vectorscan` fills `Request`; plugins read `AlgoValues` by name |
| `Plan().BuildIndexSearchScan` | `ApplyForSort` / `BuildLogicalSearch` hooks |
| `Searcher{HiddenTableRoles, Init, Next(mp)}` | `planreader.Searcher{Next(ctx), Close}`; hidden tables from `spec.hidden_tables` |
| `pkg/indexplugin/planexpr` | not created |
| rename `vectorscan` → `indexscan` | not renamed |

## Claims

`cases/` and `gpu_cases/` are under `test/distributed/`. "Hybrid" means one SELECT
with a MATCH filter (classic fulltext or fulltext2) and
`ORDER BY <distance>(v, q) LIMIT k` on the same table.

| # | Claim | Black-box test |
|---|---|---|
| 1 | A hybrid query on an ivfflat table uses both the fulltext index and the vector index: the plan has a `Fulltext Index Scan` and a `Vector Index Scan`. Shapes: natural-language MATCH; boolean MATCH with a scalar filter; a far query vector; the MATCH score projected; a selective MATCH; a query vector from a single-row provider table. | `cases/vector/vector_hybrid_fulltext.sql` |
| 2 | On ivfflat, hybrid results are post-filtered vector candidates: every returned row satisfies the MATCH and the scalar filters; a selective MATCH can return fewer than k rows. With a MATCH that keeps one third of the rows and k = 3, results equal the same query without a vector index. | same case: `outside_exact` = 0 on a MATCH of 4 of 200 rows; other queries followed by their `t_ref_*` reference |
| 3 | A hybrid query on an hnsw, cagra or ivfpq table uses the fulltext index and no vector index, and returns exactly the rows of the same query without a vector index, for all the claim 1 shapes including the selective MATCH (provider shape: hnsw). | `cases/vector/vector_hybrid_fulltext.sql` (hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq) |
| 3a | A Top-K whose only MATCH is in the projection uses the fulltext index and no vector index, for ivfflat, hnsw, cagra and ivfpq, and returns exactly the rows of the same query without a vector index. | `cases/vector/vector_hybrid_fulltext.sql`, `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` |
| 4 | `hnsw_search`, `ivfpq_search`, `cagra_search`, `fulltext2_search` and `fulltext_index_scan` are not callable from SQL. | `cases/vector/vector_hybrid_fulltext.sql`, `cases/publication_subscription/pub_sub_fulltext.sql` |
| 5 | A subscriber's MATCH on a published table searches the publisher's fulltext index (table and database publications, two subscribers, publisher DML visible, prepared statement invalidated by revoke/drop). | `cases/publication_subscription/pub_sub_fulltext.sql` |
| 6 | Moving hnsw, cagra and ivfpq to the early pass leaves the plans and results of their existing cases unchanged. | the 77 case files creating an hnsw, cagra or ivfpq index under `cases/` and `gpu_cases/`; the 8 of them with a MATCH rerun with the gate |
| 7 | An index search that would be placed on a CN reporting a protocol version below the scan's fails the query ("index search scan requires MORPC protocol version N on every CN"); with that CN at the current version the same query, run on both CNs, returns the exact rows. | `pkg/tests/sqlintegration/multicn/index_search_protocol_test.go` |
| 8 | The removed search table functions fail with "table function ... not supported" when called directly and when used in CREATE VIEW. | `cases/vector/index_search_scan_contract.sql`, `cases/vector/vector_hybrid_fulltext.sql` |
| 9 | Re-executing a prepared ivfflat, hnsw, classic fulltext or fulltext2 search with different parameters returns, at every execution, the rows of the same search on a table without the index. | `cases/vector/index_search_scan_contract.sql` |
| 10 | With a scalar filter, ivfflat `mode=pre` adds a membership join and `mode=post` does not; hnsw, cagra and ivfpq plan without a membership join under `mode=pre`, `mode=post` and no clause, and return the same rows under all three. | `cases/vector/vector_hybrid_fulltext.sql` (ivfflat, hnsw), `gpu_cases/vector/vector_hybrid_fulltext_gpu.sql` (cagra, ivfpq) |
| 11 | A vector Top-K over a join with another table uses the vector index on ivfflat and none on hnsw, cagra and ivfpq, with or without a MATCH; a query vector from a provider table uses none on cagra and ivfpq, with or without a MATCH. Where no vector index is used, results equal the same query without a vector index. | same files |

## Scope

On hnsw, cagra and ivfpq these shapes use no vector index, with or without a MATCH: a
vector Top-K over a join with another table (`JOIN meta m ON m.id = d.id`), and, for cagra
and ivfpq, a query vector from a provider table (claim 11). ivfflat uses its vector index
for the join shape.

## Decision log

- **Exact sort for hnsw, cagra, ivfpq under a MATCH** (Eric, 2026-10-08). Measured with
  their index, a MATCH keeping 4 of 200 rows returned 0 of the 3 Top-K rows, because they
  only post-filter. They keep the result they had before this change: the exact sort over
  the fulltext hits. Their early-pass path stays in place behind the gate.
- **ivfflat unchanged.** ivfflat keeps its hybrid plan and its residual-filter behavior
  (claim 2).
- **Composition, not a hybrid operator.** Hybrid search is the early vector rewrite
  followed by the late fulltext rewrite on the same scan; no node or hook is specific
  to the combination.
- **No mixed-version operation** (Eric, 2026-10-09). Index search fails until every CN
  runs this version; there is no fallback in either direction.
- **Rank-mode clause.** `BY RANK WITH OPTION 'mode=...'` is honored by ivfflat only;
  hnsw, cagra and ivfpq ignore it.
- **Publisher identity on the node.** The fulltext readers take publisher identity
  from `IndexSearchScan.source_table` / `hidden_tables`, which replaces the table
  function's `fulltext_source_ref` / `fulltext_index_ref`.
