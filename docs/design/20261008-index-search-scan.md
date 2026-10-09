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
batches. Each plugin's reader is a port of its former table function's executor.

cagra and ivfpq register only in the GPU build (`pkg/indexplugin/all/all_gpu.go`).

A partitioned scan (a plugin implementing `ParallelHooks`: ivfflat) pins its query to
the current CN, whose partition sees the coordinator's appendable ranges. Other index
search scans run as one local scope and do not pin.

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

hnsw, cagra and ivfpq implement `BuildLogicalSearch`; the gate is the only thing that
keeps them off a scan with a MATCH filter.

The `BY RANK WITH OPTION 'mode=...'` clause is honored by ivfflat only; hnsw, cagra and
ivfpq ignore it, so the hybrid claims are stated without it.

## Compatibility and rollout

Mixed-version operation is not supported, in either direction, and there is no fallback:
a query whose plan needs an index search fails until every CN runs this version.

| Case | Behavior | Evidence |
|---|---|---|
| New coordinator, a CN below the scan's protocol version | `compileIndexSearchScan` refuses the placement before dispatch: "index search scan requires MORPC protocol version N on every CN" | claim 7 (multi-CN, black box) |
| Older coordinator, newer CN, ivfflat search | the receiver refuses, in `decodeScope`, an index search scan without `algo_options` (every planner of this version sets them): "index search scan from an older version is not supported"; it also refuses one below the CN's protocol version (`MOProtocolVersion`) | unit test `TestRemoteIndexSearchScanFromOlderVersionIsRefused`; no mixed-binary test |
| Older coordinator, newer CN, hnsw/cagra/ivfpq/fulltext search | the older plan carries a removed search table function: "table function ... not supported" | claim 8 (the same error on a direct call); no mixed-binary test |
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
  the cancellation (`TestReaderStopsOnCancellationAndEmpty`); a searcher error is returned and
  the reader still closes (`TestReaderRejectsMalformedResults`). `Close` is idempotent, closes
  the searcher exactly once, and a `Read` after `Close` returns end of data (code review).
- **Correlated APPLY** (`pkg/sql/colexec/apply/vector_source.go`): each row closes the previous
  reader before opening the next; end of data closes the reader; `End`, `Reset` and `Free`
  close the reader and then the execution, both idempotent (code review).
- **Prepared reuse** (`prepareIndexSearchScanForExecution`, `pkg/sql/compile/compile.go`): each
  execution builds from the unchanged plan template (code review; claim 9 checks the results of
  re-executions).
- **No-LIMIT and probe-tail buffering**: fulltext2 streams bounded batches through a channel of
  capacity 4 (`TestReadStreaming`, `TestReadStreamingCovered`); `Close` cancels and drains the
  stream and the probe-tail producer (`TestCloseDrains`, `TestProbeTailStreamError`); classic
  fulltext joins its search goroutine on every exit (`TestClassicCloseEarly`).

## Performance

The search kernels are unchanged: each reader calls the same index cache, cuVS and fulltext
engine paths as the table function it replaces. No benchmark against main is recorded for
this PR.

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

## Scope

These shapes plan the same with and without a MATCH, and neither uses the vector index:
a vector Top-K over a join with another table (`JOIN meta m ON m.id = d.id`), and a
query vector from a provider table for cagra and ivfpq.

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
