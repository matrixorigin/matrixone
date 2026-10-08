# Index search scan: vector and fulltext as one

Owner issue: #27453. Branch `bug_27453`. The issue body is the original proposal;
this document is the design as built, plus hybrid fulltext + vector planning.
Every claim in "Claims" names the black-box test that proves it.

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

The proto carries no algorithm-specific field. A remote CN below `MORPCVersion107`
cannot receive the node: dispatch to it fails instead of misreading the plan.

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

hnsw, cagra and ivfpq implement `BuildLogicalSearch`; the gate is the only thing that
keeps them off a scan with a MATCH filter.

The `BY RANK WITH OPTION 'mode=...'` clause is honored by ivfflat only; hnsw, cagra and
ivfpq ignore it, so the hybrid claims are stated without it.

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
| 4 | `hnsw_search`, `ivfpq_search`, `cagra_search`, `fulltext2_search` and `fulltext_index_scan` are not callable from SQL. | `cases/vector/vector_hybrid_fulltext.sql`, `cases/publication_subscription/pub_sub_fulltext.sql` |
| 5 | A subscriber's MATCH on a published table searches the publisher's fulltext index (table and database publications, two subscribers, publisher DML visible, prepared statement invalidated by revoke/drop). | `cases/publication_subscription/pub_sub_fulltext.sql` |
| 6 | Moving hnsw, cagra and ivfpq to the early pass leaves the plans and results of their existing cases unchanged. | the 77 case files creating an hnsw, cagra or ivfpq index under `cases/` and `gpu_cases/`; the 8 of them with a MATCH rerun with the gate |

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
- **Rank-mode clause.** `BY RANK WITH OPTION 'mode=...'` is honored by ivfflat only;
  hnsw, cagra and ivfpq ignore it.
- **Publisher identity on the node.** The fulltext readers take publisher identity
  from `IndexSearchScan.source_table` / `hidden_tables`, which replaces the table
  function's `fulltext_source_ref` / `fulltext_index_ref`.
