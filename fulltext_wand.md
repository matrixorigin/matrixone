# WAND-based fulltext `retrieval` index — Milestone 1 (queryable + reindex)

## Context

Boolean-mode OR fulltext is the slow path in MatrixOne. A query like
`MATCH(question) AGAINST('孩子 营养 早餐 视频 文案' IN BOOLEAN MODE)` with a `LIMIT`
compiles to: a `UNION ALL` of per-term posting scans (`SqlBoolean` in
`pkg/fulltext/sql.go`) → aggregate/score the full match set → a **planner-injected
`ORDER BY score DESC LIMIT k` SORT** (`apply_indices_fulltext.go:76-129`). Every
matching doc is scored and funneled through that sort. The sort is already a heap
top-K (`compile.go:3611 → compileTop`), so the cost isn't the algorithm — it's that
the whole disjunction is materialized before the top-K can prune.

**WAND / Block-Max WAND** is the standard fix for disjunctive top-K: keep per-term
max-score upper bounds and skip docs that can't reach the current k-th score — no
full scan, no SQL sort. It needs an in-memory, doc_id-ordered, skippable posting
structure, which MO's SQL storage can't provide per-query — so we build it as a
**derived binary index loaded into RAM**, exactly like the HNSW/cuVS vector plugins.

We expose this as a new **`retrieval` parser** bound to a new **`IN RETRIEVAL MODE`**.
A `retrieval` index is jieba-tokenized, **positionless** (no phrase), int-term-id,
BM25, and answers ranked top-K via an in-operator WAND search (`FastMaxHeap`) — no
SQL sort. It does **not** replace boolean/phrase fulltext (those need positions);
the two coexist and are mutually exclusive (parser ⟺ mode).

**Source table is the single source of truth.** Postings is build scratch; the WAND
chunk store is the derived, rebuildable index.

**Milestone 1 scope:** parser/mode surface + WAND index format + in-memory search +
**synchronous reindex** build. Fully queryable and rebuildable; updates require
reindex. Incremental (ISCP/idxcron) is designed-for but deferred (Phase B).

## Architecture

```
source ─jieba tokenize→ postings (transient scratch)
                          └─ fulltext_wand_create ─▶ WAND chunk store (persistent, HNSW-style)
                                                        └─ load to RAM + cache ─▶ fulltext_wand_search
                                                              (Block-Max WAND + FastMaxHeap top-K, NO SQL sort)
                                                                    └─ INNER JOIN source on doc_id (existing)
```
Binding: `WITH PARSER retrieval` ⟺ `IN RETRIEVAL MODE` (default mode on a retrieval index).

## Implementation

### A. SQL surface — parser + mode (parser changes need goyacc regen)
- **Mode enum**: add `FULLTEXT_RETRIEVAL` to `FullTextSearchType` — `pkg/sql/parsers/tree/expr.go:1860` (enum) and a `ToString()` case `"IN RETRIEVAL MODE"` at `:1878`.
- **Grammar**: add `IN RETRIEVAL MODE → tree.FULLTEXT_RETRIEVAL` to `fulltext_search_opt` — `pkg/sql/parsers/dialect/mysql/mysql_sql.y:10812`. Requires a new **non-reserved `RETRIEVAL` keyword** (add to the mysql keyword table; `MODE`/`IN` already exist). Then **regenerate** `mysql_sql.go`: `cd pkg/sql/parsers/dialect/mysql && go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y && gofmt -w mysql_sql.go` (must report 0 conflicts).
- **Parser-name validation**: add `"retrieval"` to the switch at `pkg/fulltext/plugin/plan/schema.go:40` and the check at `:117`.
- **Parser↔mode + default resolution**: in `fulltext.ParsePattern` (`pkg/fulltext/fulltext.go:1046`) add `case int64(tree.FULLTEXT_RETRIEVAL)` (parse as a **bag of jieba tokens**), and resolve `FULLTEXT_DEFAULT → RETRIEVAL` when the index parser is `retrieval` (else keep `→ NL`). Validate the pairing at bind time (`apply_indices_fulltext.go` / `findMatchFullTextIndex:726`): reject boolean/NL on a retrieval index and `IN RETRIEVAL MODE` on a non-retrieval index.
- **Routing**: when parser/mode are retrieval, route the MATCH to the **`fulltext_wand_search`** TVF instead of `fulltext_index_scan` (`pkg/sql/plan/fulltext.go:buildFullTextIndexScan`/`getFullTextSql:119`). The existing INNER-JOIN-to-source + score-projection machinery downstream is unchanged.

### B. WAND index format + in-memory structure — new package `pkg/fulltext/wand/`
- **Dictionaries**: jieba token → dense `term_id`; source pk → dense `doc_ord` (both persisted in the index).
- **Per term**: doc_id-ordered postings (delta-encoded / roaring), **`uint8` capped-tf** (reuse the cap-at-255 of `cappedTfExpr` in `sql.go:139`), per-block `max_doc_id` (skip) + `max_weight` (Block-Max), and `df`.
- **Global**: `N` (doc count) + per-term `idf` derived from `df`/`N` at query time. **No positions** (retrieval = no phrase) → minimal memory.
- **Serialize/load**: chunk-blob format + RAM load via the HNSW pattern in `pkg/vectorindex/hnsw/model.go` (`ToSql` write, `LoadIndexFromBuffer`/`loadChunk` read, mmap/view). Reuse the storage column convention (`index_id, chunk_id, data, tag` + metadata `index_id, timestamp, checksum, filesize`, `pkg/catalog/types.go:423-432`).

### C. `fulltext_wand_create` TVF — build postings → WAND chunks
- New `pkg/sql/colexec/table_function/fulltext_wand_create.go`, mirroring `hnsw_create.go` (prepare/state, `tvfState` interface `types.go:83`). Register in the switch at `pkg/sql/colexec/table_function/table_function.go:142`.
- **Input = SQL-sorted/grouped postings**: feed it `SELECT word, doc_id, <cappedTf> FROM <postings> GROUP BY word, doc_id ORDER BY word, doc_id` so the **engine does the heavy aggregate+sort (with spill)**; the TVF streams ordered postings into segments + serializes (low memory).
- **Output = chunk rows** `(index_id, chunk_id, data, tag)` → `INSERT` into the WAND storage table.

### D. `fulltext_wand_search` TVF — query → top-K
- New `pkg/sql/colexec/table_function/fulltext_wand_search.go`, mirroring `hnsw_search.go`. Register in the same switch.
- Implement `VectorIndexSearchIf` (`pkg/vectorindex/cache/cache.go:58`) in `pkg/fulltext/wand/search.go`; load + cache via `veccache.Cache` (load-once, TTL eviction, `cache.go:255`).
- Run **Block-Max WAND** over the in-memory postings; top-K via `FastMaxHeap[float64,int64]` (`pkg/vectorindex/index.go:176`) — note: add a `Len()`/size accessor if absent on this branch. Emit `(doc_id, score)` rows.

### D2. Prefiltering (membership pushdown — like cuVS)
WAND supports prefiltering naturally: pass a **doc-id allow-set** into the search and consult it during the walk.
- The `fulltext_wand_search` TVF accepts the **existing** `FulltextMembershipFilter` (the `fulltext_bloom_filter_pushdown` plumbing already attached in `runWordStats`/`apply_indices_fulltext.go`) — no new pushdown mechanism.
- **Apply during WAND**: at each pivot-surviving candidate, check membership; skip non-allowed docs (cheap bitmap lookup). Optionally skip a block whose doc-ord range (block `max_doc_id` + the allow-bitmap) contains no allowed docs.
- **Exact** (roaring bitmap of allowed doc-ords, built from the WHERE filter) vs **probabilistic** (bloom → false positives removed by the post-join filter) — same two modes as cuVS filtered search.
- **Resolves the filter+top-K tension**: with the prefilter applied *inside* WAND, the returned top-K is already filtered → no over-fetch, no post-join shrinkage (the `WHERE delete_flag IS NULL` case). This is strictly better than the SQL OR path, which scores the whole disjunction then filters.

### E. Reindex — synchronous build wiring  [APPROACH CHOSEN: additive "Postings + WAND store" (2026-06-19)]
Keep the existing `(word,doc_id,pos)` postings table **unchanged** and add the WAND store on top (reuses all existing fulltext creation/drop; safer/less invasive than postings-transient). The postings table is built normally; the WAND store is built additionally from it.
- **Schema** (`pkg/fulltext/plugin/plan/schema.go` `BuildFullTextIndexDefs`): for `parser == "retrieval"`, emit the normal postings IndexDef/TableDef **plus** two more TableDefs (WAND storage `FullTextWand_TblType_Storage` + metadata `FullTextWand_TblType_Metadata`) and their IndexDefs (mirror the HNSW dual-table builder in `pkg/vectorindex/hnsw/plugin/plan/schema.go:43`).
- **⚠ Enumeration gotcha (verified):** `findMatchFullTextIndex` (`apply_indices_fulltext.go:756`), the "same column" collision check in `BuildFullTextIndexDefs`, and drop/show/reindex all select fulltext defs by `catalog.IsFullTextIndexAlgo(IndexAlgo)` + matching `Parts` only. Extra WAND IndexDefs on the **same column** collide and break enumeration. Distinguish the WAND tables by `IndexAlgoTableType` and make every fulltext-index enumeration filter **postings-only** (`IndexAlgoTableType == ""`), or carry the WAND tables under a distinct algo. Audit ALL enumeration call sites before wiring.
- **Reindex** (`pkg/fulltext/plugin/compile/compile.go` `HandleCreateIndex:45` / `HandleReindex`): after the normal `source → postings` INSERT (`genInsertSQL`, tokenizer dispatch already has `case "retrieval"` = jieba), run `postings → WAND` via `INSERT INTO <wand_store/meta> SELECT … FROM (SELECT word, doc_id, <cappedTf> FROM <postings> GROUP BY word, doc_id ORDER BY word, doc_id) AS s CROSS APPLY fulltext_wand_create('<param>', '<cfg+n>', s.word, s.doc_id, s.tf)`. No transient table, no drop (postings persists). Compute `n` = source `COUNT(*)` and embed in the cfg JSON (`TableConfig.DocCount`).

## Phase B — async DML (INSERT/UPDATE/DELETE) via CDC  [DESIGNED 2026-06-19, not yet built]

M1 builds the index synchronously from a postings table; updates need a rebuild.
A serialized binary index **can't be row-patched inside a txn** — the same reason
HNSW/CAGRA are `AlwaysAsync` and cuVS uses CDC + a tombstone + periodic rebuild.
**Decision (confirmed): the retrieval index is always-async.** Inline DML is
dropped; INSERT/UPDATE/DELETE flow through CDC/ISCP, which maintains the WAND
store as **immutable segments + a delete tombstone**, compacted by idxcron.

### How "only retrieval is async" — make `AlwaysAsync` a function of the IndexDef
Today `AlwaysAsync` is a static `bool` field of `SyncDescriptor`
(`pkg/indexplugin/catalog/hooks.go`), so the fulltext plugin can't be async for
`retrieval` only. **Change it to a parser-aware function instead of forcing
`async=true` into `algo_params`** (no param mutation, no `async=false` rejection
needed):
- **Make `AlwaysAsync` index-aware.** Either a `Hooks` method
  `AlwaysAsync(idx *plan.IndexDef) bool` or a func field
  `AlwaysAsync func(*plan.IndexDef) bool` on `SyncDescriptor`. HNSW/CAGRA/IVF-PQ
  return `true` (ignore `idx`); IVF-FLAT returns `false`; **fulltext returns
  `parserName(idx) == "retrieval"`** (parser from `idx.Option.ParserName` /
  `IndexAlgoParams` `"parser"` key). The two existing read sites already hold the
  `IndexDef` (`checkValidIndexCdcByIndexdef` `iscp_util.go:72`,
  `cloneUnaffectedIndex` `alter.go:1184`) — pass it through.
- **Unify the async decision.** Add one helper, e.g.
  `IndexIsAsync(idx) = (plugin.AlwaysAsync(idx)) || catalog.IsIndexAsync(idx.IndexAlgoParams)`,
  and route **all** sites through it: the CDC registration
  (`checkValidIndexCdcByIndexdef`), the alter-clone skip, **and the DML
  early-returns** `buildPreInsert/PreDelete/PostDmlFullTextIndex`
  (`build_dml_util.go`, which currently call `IsIndexAsync(params)` directly).
  This is what makes inline DML skip for `retrieval` without injecting a param.
  (Verify `pkg/sql/plan` can import `indexplugin` without a cycle; if not, expose
  the parser-aware check via a small `catalog`/plugin-registry shim the plan layer
  already uses.)
- ngram/gojieba fulltext: `AlwaysAsync` returns false → their per-index `async`
  param is still honored exactly as today (behavior unchanged).

### Architecture: CDC → segments + tombstone + merge
```
source DML ─CDC─▶ ISCP retrieval sinker
                   ├─ INSERT/UPSERT: jieba-tokenize CDC batch → Builder → pre-built segment → append as tag=1 delta (new index_id)
                   └─ DELETE: append (pk, deleteLSN) as a tag=1 record in ft_index
        idxcron ─▶ merge tag=0 main + tag=1 delta segments, apply tag=1 deletes → new tag=0, clear tag=1 (cuVS rebuild)
         query  ─▶ load tag=0 main + tag=1 delta segments + tag=1 deletes → SearchSegments(allow ∩ live-by-LSN)
```
Storage mirrors cuVS in the **same `ft_index` table** via its existing `tag`
column: **tag=0** = the **compacted main** index; **tag=1** = the **incremental
delta** — pre-built insert segments **and** delete records. No separate tombstone
table. Insert segments start in tag=1 and **graduate to tag=0 at compaction**, so
**tag=1 size crossing a threshold is the natural compaction gate** (cuVS's
`countTag1Records`). The WAND engine core is already built + unit-tested in
`pkg/fulltext/wand`: `Builder.FinishSegments(capacity)`, `SearchSegments`
(corpus-global BM25 across segments), `Merge(id, segs…)`, and the `Membership`
hook.

**Loading tag=1 is NOT cuVS-style replay.** tag=1 insert segments are **pre-built
serialized postings**, so loading them is a plain `Deserialize` + `SearchSegments`
— as cheap as loading tag=0, no re-tokenize, no posting rebuild. The thing we
reject (below) is storing *raw row text* in tag=1 and rebuilding postings at load.

### Reused cuVS CDC primitives (post-`cuvs_quantize` merge, 2026-06-19)
The quantize merge made the cuVS CDC codec **payload-agnostic** (`Vec []byte`
instead of `[]float32`), so WAND reuses it directly instead of mirroring it:
- **Tag constants are shared** — `pkg/vectorindex/types.go`: `Tag_ModelChunk=0`,
  `Tag_CdcEvents=1`, sentinel `CdcTailId="cdc_tail"`. Use these (the `ft_index`
  `tag` column already carries 0/1); don't invent new ones.
- **Two kinds of tag=1 rows.** Delta **segments** are self-contained WAND models →
  `index_id = batchLSN` (+ `ft_meta` rows, like tag=0). The **delete log** is
  homogeneous small records → a `cdc_tail`-style single sentinel `index_id` with
  appended framed chunks (no `ft_meta` row; per-chunk CRC like cuVS).
- **Codec reuse.** `FrameCdcChunk`/`UnframeCdcChunk` (`pkg/vectorindex/cuvs/cdc.go`:
  magic+version+`n_inserts`/`n_deletes`/`n_upserts`+CRC32+footer) frames the delete
  log. A WAND delete record = `(op, encodePk(pk), deleteLSN int64)` — pk via the
  existing `encodePk`/`decodePk` (`serialize.go`) since WAND pk is `any` (cuVS
  `Pkid` is int64). **WAND "replay" = the DELETE half of `ReplayEventLog` only**
  (decode framed delete chunks → `pk→maxDeleteLSN` map); insert deltas are loaded
  as pre-built segments, never replayed.
- **No quant leakage / global-stats parallel.** cuVS keeps quant params in the
  tag=0 model tar, never in CDC (`cagra/model_gpu.go:75-81`). The exact analog:
  WAND's global `N`/`avgdl`/`df` are aggregated at query from segment metadata,
  never in the delete log. The quantize work is orthogonal to WAND — it only left
  a cleaner codec to borrow.

### Segment granularity: one segment per *flush*, not per row/event
A segment = one sinker `FinishSegments` flush, batching **many** rows. A CDC batch
of 100 inserts → **one** segment (`index_id=batchLSN`), not 100. `max_index_capacity`
caps docs-per-segment (a huge insert yields a few segments, not thousands); idxcron
tiered merge collapses small/recent segments so live count K stays small.
**Load = O(K) `Deserialize` + read tag=1; it never merges or replays.** Compaction
is background idxcron only. Build cost is paid **once at write** (row in hand);
the read path stays cheap.

### Write path: NO full-index download
- **INSERT/UPSERT** → build a small new segment from only this CDC batch's rows →
  append as a new tag=1 delta segment with `index_id=batchLSN` (chunk rows
  `REPLACE INTO` → idempotent on replay). Existing segments untouched.
- **DELETE** → append one tag=1 record `(pk, deleteLSN)`. Zero index I/O.
- Big segments are downloaded **only at compaction**, under a **tiered/leveled
  policy** (merge small/recent; leave the large base) — LSM/Lucene behavior.

### CDC duplicates & ordering — LSN-as-identity (no separate segno)
Two **distinct** duplicate problems; only the second needs an ordering, and the
segment's LSN-based `index_id` already *is* it — so **no separate `segno`**.

**Problem A — replay duplicates (the `REPLACE INTO` problem).** ISCP redelivers the
same INSERTs (snapshot replay, watermark rewind, at-least-once). Postings solves
this with `REPLACE INTO` on PK `(word, doc_id)`. **WAND does the same at chunk
grain:** segment `index_id = batchLSN` (deterministic), chunk rows `REPLACE INTO`
on `(index_id, chunk_id)`. The build is deterministic (same rows → same bytes →
same chunks), so a redelivered batch **overwrites the identical segment** — no
duplicate, no segno.

**Problem B — UPDATE/UPSERT puts the same pk in two segments** (old + new). `REPLACE
INTO` can't merge them (different `index_id`s); source PK is unique so this only
arises from UPDATE/UPSERT. Showing both → INNER-JOIN-to-source yields the doc
twice. Keep the **newest** copy (its score matches current text) via the LSN:
- segment `index_id = batchLSN`; delete record = `(pk, deleteLSN = its own batch LSN)`.
- a pk's live copy = the **highest-LSN segment** holding it; dead iff a delete with
  `deleteLSN > thatSegmentLSN` (strict `>`).
- **UPDATE in batch L** (ISCP → DELETE+INSERT both at L): delete `(pk, L)` kills
  every older copy (`L > olderLSN`) but **not** the new copy in segment `index_id=L`
  (`L` is not `> L`) → one live copy, newest wins. Resolved by LSN *value*, so
  ISCP's delete-first / lost intra-ts order is **irrelevant**.

**Idempotency falls out**: deletes record their own batch LSN, segments are keyed by
batch LSN — nothing reads "current committed max," so replay reproduces identical
state. This is cuVS's `ReplayEventLog` net-state using the LSN we already have, with
the *segments themselves* as the ordered "overflow." (cuVS keeps a deleted-set +
pk-keyed overflow-map and `CdcOpInsert` never clears the deleted-set,
`cuvs/cdc.go:534-549` — we get the same effect structurally from LSN ordering.)
**Cost:** at load, dedup multi-segment pks by max LSN (only *updated* pks appear in
>1 segment) → precompute a **per-segment deny-bitmap** so `Membership.Contains(ord)`
is unchanged.

### Rejected alternative: replay RAW EVENTS at load (re-tokenize/rebuild)
The rejected variant is storing **raw row text as CDC events** in tag=1 and
**re-tokenizing + rebuilding postings at every cold load** (5-min cache TTL) —
plus storing raw text dwarfs the postings it produces. (cuVS can replay because
its tag=1 is raw vectors fed to a GPU **brute-force** overflow — cheap; WAND's
recent inserts must stay block-max/global-BM25 searchable, i.e. *postings*, so
rebuilding them at load is the expensive part.) **We instead persist pre-built
WAND segments in tag=1** so load is a pure `Deserialize` + `SearchSegments` — the
build cost is paid once at write. This keeps cuVS's tag=0/tag=1 main/delta split
and its ordering semantics (LSN-as-identity), without re-tokenizing.

### Components
0. **Parser-aware async** (`pkg/indexplugin/catalog/hooks.go` + the two read sites):
   turn `SyncDescriptor.AlwaysAsync` from a `bool` into `AlwaysAsync(idx)` (method
   or func field); fulltext returns `parser=="retrieval"`. Add a unified
   `IndexIsAsync(idx)` helper and route the CDC-registration, alter-clone, and the
   three `build_dml_util.go` DML early-returns through it. (See "How only retrieval
   is async".)
1. **Schema** (`schema.go`): keep `ft_index`+`ft_meta`, **no new table** (delta
   insert segments + delete records both = tag=1 in `ft_index`, compacted main =
   tag=0); **drop the postings hidden table** (sinker tokenizes CDC rows directly,
   like HNSW). **No `async` param injection** — async-ness comes from the
   parser-aware `AlwaysAsync(idx)` (see above).
2. **Create** (`compile.go HandleCreateIndex`): replace sync `genWandBuildSQL` with
   CDC registration (`CreateIndexCdcTask`, replay-from-creation builds initial
   segments) — mirror the HNSW async branch.
3. **WAND ISCP sinker** (`pkg/iscp` + `pkg/fulltext`): model on `index_consumer.go
   runHnsw` / `hnsw/sync.go sequentialUpdate`. Per batch: tokenize INSERT/UPSERT →
   `Builder.Add` → on flush `FinishSegments` → `ToInsertSqls` as a **tag=1 delta
   segment** with `index_id=batchLSN` (deterministic; chunk rows `REPLACE INTO` →
   replay-safe); DELETE/UPSERT → append tag=1 `(pk, deleteLSN=batchLSN)`. Nothing
   reads "current committed max." Advance watermark only after both commit.
4. **idxcron compaction** (template `pkg/vectorindex/cuvs/idxcron`): the only op
   that downloads segments; fires on tag=1 growth (`countTag1Records`). Tiered
   policy → load tag=0 main + tag=1 deltas → `Merge` with tag=1 deletes as skip-set
   → write merged **tag=0**, delete inputs, **clear tag=1** (LSN-bounded: only ≤ the
   snapshot LSN; coordinate with sinker like cuVS's lease/swap).
5. **Search adapter** (`wandsearch.go` + search TVF): load tag=0 main + tag=1 delta
   segments + the tag=1 `pk→maxDeleteLSN` map; resolve liveness by LSN into a
   **per-segment deny-bitmap** (`deny = {ord : a higher-LSN segment also holds pk,
   OR maxDeleteLSN[pk] > segLSN}`) so `Membership.Contains(ord)` is unchanged and
   the WHERE-prefilter is ANDed in; `Cache.Remove` on flush/merge.
6. **`max_index_capacity`** algo-param/session-var — caps docs-per-segment; sinker
   rolls at capacity. Default 0 = compaction *target* of one segment (NOT "no
   deltas"; deltas always accumulate between compactions) — plumb like
   `hnsw_max_index_capacity`.

### Consistency / concurrency notes
- Async ⇒ eventually consistent (CDC lag), like HNSW/cuVS. **CREATE now returns
  before the index is populated**; queries until CDC catch-up are partial. REINDEX
  = an idxcron full-rebuild action (define à la cuVS reindex).
- A deleted/superseded doc still contributes to `df`/`N`/`avgdl` until compaction
  (standard Lucene/cuVS); `Merge` makes it exact. **Stat drift scales with the
  un-compacted delete ratio**, so the compaction gate should weigh delete-ratio,
  not only segment count (ranking is relative → top-K largely preserved; absolute
  BM25 drifts).
- UPDATE = DELETE record `(pk, batchLSN)` + new delta segment `index_id=batchLSN`;
  newest-LSN copy wins, older copies denied at load.
- **Compaction concurrency:** idxcron snapshots an LSN, applies + clears tag=1 only
  ≤ that LSN, coordinated with the sinker (mirror cuVS's index lease/swap).

### Phasing
- **B1**: parser-aware `AlwaysAsync(idx)` + unified `IndexIsAsync` helper (drops
  inline DML for retrieval) + CDC registration + tag=1 delete format + search-side
  tombstone filter (deletes work; inserts via existing build).
- **B2**: WAND ISCP sinker (incremental INSERT/UPSERT → segment append) +
  multi-segment search adapter.
- **B3**: idxcron `Merge` compaction + `max_index_capacity` rollover.

## Verification
- **Unit**: (1) `tokenize → postings → fulltext_wand_create` round-trip builds a loadable WAND index. (2) **Differential**: `fulltext_wand_search` top-K vs a brute-force reference (`Σ tf·idf²` over all docs + full sort) on randomized corpora → assert identical top-K and scores (the WAND-correctness gold test). (3) Parser/mode parse tests in `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go` (`IN RETRIEVAL MODE`, default-on-retrieval-parser).
- **Build**: goyacc regen compiles (0 conflicts); `go build ./pkg/...`; `go test ./pkg/fulltext/... ./pkg/sql/parsers/dialect/mysql/ ./pkg/sql/colexec/table_function/`.
- **E2E (manual / BVT)**: `CREATE FULLTEXT INDEX … WITH PARSER retrieval`; insert docs; `MATCH(question) AGAINST('孩子 营养 早餐 视频 文案')` (bare → retrieval mode) returns ranked top-K; `EXPLAIN` shows `fulltext_wand_search` and **no SORT node**; boolean/NL mode on a retrieval index errors; `IN RETRIEVAL MODE` on an ngram index errors; reindex rebuilds the index; relevance sanity-check vs `IN BOOLEAN MODE` on an ngram index.
- **Prefiltering**: the same query with `WHERE delete_flag IS NULL` (and a selective `category = …`) returns exactly the filtered top-K (membership applied inside WAND); compare results to a brute-force filtered scan; confirm no over-fetch/post-filter shrinkage and that the membership filter is pushed into `fulltext_wand_search`.
- **Perf**: the original slow OR query on a `retrieval` index — confirm no SORT operator and materially lower latency than the boolean path.
