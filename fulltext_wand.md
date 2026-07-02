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

### Architecture: CDC → single CdcTail log → full-reindex compaction
```
source DML ─CDC─▶ ISCP retrieval sinker
                   ├─ INSERT/UPSERT: jieba-tokenize CDC batch → Builder → pre-built segment → append to tag=1 CdcTail log at next chunk_id
                   └─ DELETE: append (op, pk) to tag=1 CdcTail log at next chunk_id
        idxcron ─▶ full REINDEX from source → fresh tag=0; build txn wipes the tag=1 CdcTail (cuVS rebuild)
         query  ─▶ load tag=0 + tag=1 CdcTail (chunk_id order) → SearchSegmentsLive(allow ∩ live-by-chunk_id)
```
Storage mirrors cuVS in the **same `ft_index` table** via its existing `tag`
column: **tag=0** = the **compacted main** index; **tag=1** = a **single append log**
(`index_id = vectorindex.CdcTailId`) holding pre-built insert segments **and** delete
records as `chunk_id`-ordered frames. No separate tombstone table, **no per-segment
`index_id`** (that was the superseded multi-segment idea — see the dedup decision
below). tag=1 grows until a **full reindex** wipes it and writes a fresh tag=0, so
**tag=1 size crossing a threshold is the natural reindex gate** (cuVS's
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
- **One append log, two frame kinds** — reuse `CdcTailId`. Both insert **segments**
  (pre-built WAND models) and **delete** records live in the single tag=1 log
  (`index_id = CdcTailId`), interleaved and ordered by `chunk_id` via the existing
  `NextChunkIdSql` (`COALESCE(MAX(chunk_id)+1,0)`). **No per-segment `index_id`, no
  `segno`, no per-chunk timestamp** (see the dedup decision). One `ft_meta` row for
  the tag=1 log (per-chunk CRC like cuVS).
- **Codec reuse.** `FrameCdcChunk`/`UnframeCdcChunk` (`pkg/vectorindex/cuvs/cdc.go`:
  magic+version+`n_inserts`/`n_deletes`/`n_upserts`+CRC32+footer) frames both a
  segment blob and a delete-record batch. A WAND delete record = `(op, encodePk(pk))`
  — pk via the existing `encodePk`/`decodePk` (`serialize.go`) since WAND pk is `any`
  (cuVS `Pkid` is int64); **no order field — the frame's `chunk_id` position is its
  order**, so no LSN/segno/timestamp is stored. **WAND "replay" = the DELETE half of
  `ReplayEventLog` only** (walk frames in `chunk_id` order → `pk→deleted-after-chunk_id`
  map); insert frames are loaded as pre-built segments, never re-tokenized.
- **No quant leakage / global-stats parallel.** cuVS keeps quant params in the
  tag=0 model tar, never in CDC (`cagra/model_gpu.go:75-81`). The exact analog:
  WAND's global `N`/`avgdl`/`df` are aggregated at query from segment metadata,
  never in the delete log. The quantize work is orthogonal to WAND — it only left
  a cleaner codec to borrow.

### Segment granularity: one segment per *flush*, not per row/event
A segment = one sinker `FinishSegments` flush, batching **many** rows. A CDC batch
of 100 inserts → **one** appended segment frame at the next `chunk_id`, not 100.
`max_index_capacity` caps docs-per-segment (a huge insert yields a few segment
frames, not thousands). **Load = O(K) `Deserialize` over the `chunk_id`-ordered
tag=1 frames; it never re-tokenizes.** Reclamation is a background idxcron **full
reindex** only (no incremental merge). Build cost is paid **once at write** (row in
hand); the read path stays cheap.

### Write path: NO full-index download
- **INSERT/UPSERT** → build a small new segment from only this CDC batch's rows →
  append it as framed chunk(s) to the tag=1 `CdcTail` log at the next `chunk_id`
  (`NextChunkIdSql`; a redelivered batch just becomes later frames at higher
  `chunk_id`, masked at load). Existing frames untouched.
- **DELETE** → append one `(op, pk)` frame at the next `chunk_id`. Zero index I/O.
- The full index is downloaded **only at reindex** (idxcron full rebuild from
  source, which wipes tag=1) — not on the write path.

### CDC duplicates & ordering — single CdcTail log, `chunk_id`-ordered (cuVS-aligned)  [SUPERSEDES "LSN-as-identity" AND the interim "segno" note — DECISION 2026-07-01, revised]

**Decision (2026-07-01, revised):** follow cuVS/IVF-PQ exactly — tag=1 is a **single
append log** (`index_id = vectorindex.CdcTailId`), ordered by **`chunk_id`** via the
existing `NextChunkIdSql` (`COALESCE(MAX(chunk_id)+1, 0)`, `cuvs/cdc.go:945`).
Dedup is a **load-time PK reconciliation ordered by `chunk_id`** (append order in
that one log). There is **no ISCP LSN, no wall-clock timestamp, and no cross-segment
`segno` counter** — the earlier `segno`/`WandModel.LSN` idea is dropped, and
`DataRetriever.GetLSN()` is not added. This supersedes every "batchLSN /
`index_id = LSN`" and interim "`segno`" reference elsewhere in this doc.

**Why one log, not N segments.** The multi-`index_id`-segment design forced a
cross-segment ordering key (LSN → segno → timestamp), and every candidate had a
sharp edge (LSN restarts on ISCP re-snapshot; wall-clock goes backwards;
`MAX(segno)+1` is a fragile read-modify-write; compaction stamping). cuVS sidesteps
all of it: **one `CdcTail` log, `chunk_id` = `MAX(chunk_id)+1` as the order key**
(`sync.go:245,272`), sorted at load (`model_gpu.go:652` `replayEventChunks` sorts by
`chunk_id`). WAND reuses that primitive verbatim.

**What WAND appends to the log** (heterogeneous frames, one chunk_id sequence):
- **INSERT/UPSERT** → a *pre-built mini-`WandModel`* serialized and appended as
  framed chunk(s) at the next `chunk_id`. (WAND's tag=1 stays *pre-built segments*,
  never raw text — so load is `Deserialize`, never re-tokenize; this is the one
  divergence from cuVS, whose tail is raw vectors.)
- **DELETE** → a small framed `(op, pk)` record at the next `chunk_id` (reuse
  `FrameCdcChunk` / the delete-log codec; **no `segno`/LSN field** — the record's
  `chunk_id` position *is* its order).

**Dedup at load — `chunk_id`-ordered PK reconciliation.** Read tag=0 base + the
tag=1 `CdcTail` chunks sorted by `chunk_id`; deserialize insert frames into the
ordered segment list `segs` and fold delete frames into `deletes`. Then
`ComputeLiveness` (`search.go:83`, already implemented) resolves duplicates using
each segment's own `pk→ord` dict (`s.pks`, from `Builder.docOrd` `wand.go:323`):
`owner[pk] = the segment with the highest chunk_id holding pk`; a pk is dead in a
segment iff a delete frame at a **higher** `chunk_id` exists (strict `>`). Only the
ordering key changes — `s.LSN` → the segment's `chunk_id` (append position). The
dedup identity is the **source PK**, exactly like HNSW (`Contains(PKey)`,
`hnsw/sync.go:223,298`) and IVF-PQ (`pkid` fold, `cuvs/cdc.go` `ReplayEventLog`) —
never ISCP's LSN.

**Compaction = full REINDEX, not incremental merge.** Exactly as cuVS: the idxcron
`Updatable` hook only *gates* when to fire (on tag=1 growth); the rebuild
re-tokenizes from source into a fresh tag=0 and **the build txn wipes the tag=1
`CdcTail` log** (`small_tail.go:43`). This is WAND's M1 synchronous build, re-run.
No base+delta timestamp reconciliation, no tiered merge — so the "compaction must
stamp `max(input ts)`" hazard never arises.

**Two duplicate problems, resolved by `chunk_id` order:**
- **Problem A — replay duplicates (at-least-once).** A redelivered batch is appended
  as later frames (higher `chunk_id`) with the same pks → at load, `ComputeLiveness`
  gives the later copy ownership → earlier copies masked → **correct top-K
  immediately**; the next reindex wipes the redundant frames. (Idempotent-at-query,
  convergent-at-reindex.)
- **Problem B — UPDATE/UPSERT puts a pk in two frames** (old + new). The newer write
  has the higher `chunk_id` → owns the pk; the old copy is denied. A same-batch
  UPDATE's DELETE and its INSERT segment share adjacent `chunk_id`s; owner uses `>=`,
  delete uses strict `>`, so newest survives — no dependence on ISCP intra-ts order.

**Robustness to the ts=0 re-snapshot (the case that killed LSN-as-identity).** A full
ISCP re-snapshot just re-appends frames to the `CdcTail` log at higher `chunk_id`,
masking stale copies at load; a reindex then wipes the tail. **No ISCP LSN, no
truncate-triggered-by-ISCP, no special snapshot path** — the store is an idempotent
projection of the source.

**Cost:** one O(total docs) load pass builds `owner[pk]` + the per-segment
deny-bitmaps (already in `ComputeLiveness`); only pks present in >1 frame
(updated/redelivered) get masked. Log size is bounded by reindex cadence (idxcron
gate on tag=1 growth), same as cuVS.

#### Ordering key — SETTLED, do not re-open

This was debated to exhaustion; the ordering key is **`chunk_id` in the single tag=1
`CdcTail` log**. The rejected alternatives and *why*, so this is not re-litigated:

| Candidate | Rejected because |
|---|---|
| **ISCP LSN** (`batchLSN`) | Couples the persistent index to a transport-layer counter. A full ISCP restart re-snapshots from `ts=0` and **restarts the LSN sequence** → old frames collide with new → a rebuild forced by an ISCP event outside WAND's control. |
| **Wall-clock timestamp** (`time.Now()`) | Not monotonic: NTP steps, VM migration, and per-CN clock skew (multi-CN) can make a *later* write get a *smaller* value → an update's old copy wins → silent corruption. |
| **Cross-segment `segno` counter** (`MAX(segno)+1` over N `index_id`s) | Fragile read-modify-write; needs single-writer + must never desync/reset; and a *tiered/incremental* compaction would have to carefully stamp `max(input)` to avoid the merged base out-ranking a newer un-merged delta. Too many invariants. |
| **Source-row commit TS** | Data-intrinsic and safe, but unnecessary: one append log already gives a total order for free, and `chunk_id` needs no extra column or plumbing. |

**Chosen — `chunk_id` in one `CdcTail` log:** monotonic by construction
(`NextChunkIdSql = MAX(chunk_id)+1` scoped to one `index_id`), no cross-segment
sort, no timestamp, no ISCP coupling; **exactly the cuVS/IVF-PQ mechanism**
(`cuvs/cdc.go`, `ivfpq/model_gpu.go:652`), which is already built and tested.
Compaction is a **full reindex** (not incremental merge), so even the `segno`
"stamp `max(input)`" hazard never arises. If a future change wants to reopen this,
the burden is to show a concrete case `chunk_id` ordering gets wrong — the four
rows above are already answered.

### Rejected alternative: replay RAW EVENTS at load (re-tokenize/rebuild)
The rejected variant is storing **raw row text as CDC events** in tag=1 and
**re-tokenizing + rebuilding postings at every cold load** (5-min cache TTL) —
plus storing raw text dwarfs the postings it produces. (cuVS can replay because
its tag=1 is raw vectors fed to a GPU **brute-force** overflow — cheap; WAND's
recent inserts must stay block-max/global-BM25 searchable, i.e. *postings*, so
rebuilding them at load is the expensive part.) **We instead persist pre-built
WAND segments in tag=1** so load is a pure `Deserialize` + `SearchSegments` — the
build cost is paid once at write. This keeps cuVS's tag=0/tag=1 main/delta split
and its ordering semantics (**`chunk_id` order in the single `CdcTail` log**),
without re-tokenizing.

### Components
0. **Parser-aware async** (`pkg/indexplugin/catalog/hooks.go` + the two read sites):
   turn `SyncDescriptor.AlwaysAsync` from a `bool` into `AlwaysAsync(idx)` (method
   or func field); fulltext returns `parser=="retrieval"`. Add a unified
   `IndexIsAsync(idx)` helper and route the CDC-registration, alter-clone, and the
   three `build_dml_util.go` DML early-returns through it. (See "How only retrieval
   is async".)
1. **Schema** (`schema.go`): keep `ft_index`+`ft_meta`, **no new table** (delta
   insert segments + delete records both = tag=1 in `ft_index`, compacted main =
   tag=0). **KEEP the postings hidden table** — unlike HNSW, WAND builds its store
   *from* postings via SQL aggregation (`source → postings → GROUP BY word,doc_id
   ORDER BY … → fulltext_wand_create → tag=0`), so `CREATE INDEX` and full-reindex
   need it; only the incremental CDC delta path (the sinker) is postings-free.
   **No `async` param injection** — async-ness comes from the parser-aware
   `AlwaysAsync(idx)` (see above).
2. **Create** (`compile.go HandleCreateIndex`): flip the async check to
   `indexplugin.IndexIsAsync` so retrieval registers a CDC task, but **keep the
   postings-based build** — pass `CreateIndexCdcTask` a non-empty **InitSQL** that
   builds the WAND store from postings (`genInsertSQL` → postings, then
   `genWandBuildSQL` → tag=0), replacing the current empty `sql=""`. ISCP runs the
   InitSQL at task start (beginning of ISCP), so the initial build still goes
   through the postings table; `genWandBuildSQL` is not dropped, it just moves into
   the InitSQL. Then CDC deltas maintain tag=1.
3. **WAND ISCP sinker** (`pkg/iscp` + `pkg/fulltext`): model on `index_consumer.go
   runHnsw` / `hnsw/sync.go sequentialUpdate`. Per batch: tokenize INSERT/UPSERT →
   `Builder.Add` → on flush `FinishSegments` → append the segment frame(s) to the
   tag=1 `CdcTail` log at `chunk_id = NextChunkIdSql(CdcTailId, tag=1)`; DELETE/UPSERT
   → append an `(op, pk)` frame at the next `chunk_id`. A redelivered batch just
   becomes later frames (higher `chunk_id`), masked at load — see the dedup decision.
   Advance watermark only after commit.
4. **idxcron reindex** (template `pkg/vectorindex/cuvs/idxcron` + `small_tail.go`):
   the only op that rebuilds; fires on tag=1 growth (`countTag1Records`). It runs a
   **full reindex from source** (WAND's M1 build) → fresh tag=0, and **the build txn
   wipes the tag=1 `CdcTail`** (`small_tail.go:43`). No incremental merge, no
   base+delta reconciliation. Coordinate the wipe/swap with the sinker like cuVS's
   lease (bound the wipe to the reindex's source snapshot; post-snapshot frames stay
   in the fresh tail).
5. **Search adapter** (`wandsearch.go` + search TVF): load tag=0 base + the tag=1
   `CdcTail` frames sorted by `chunk_id` → ordered `segs` + `pk→deleted-after-chunk_id`
   map; `ComputeLiveness` resolves liveness **by `chunk_id`** into a per-segment
   deny-bitmap (`deny = {ord : a higher-chunk_id frame also holds pk, OR a delete
   frame at a higher chunk_id}`) so `Membership.Contains(ord)` is unchanged and the
   WHERE-prefilter is ANDed in; `Cache.Remove` on flush/reindex.
6. **`max_index_capacity`** algo-param/session-var — caps docs-per-segment-frame;
   sinker rolls at capacity. Frames always accumulate in the tag=1 log between
   reindexes — plumb like `hnsw_max_index_capacity`.

### Consistency / concurrency notes
- Async ⇒ eventually consistent (CDC lag), like HNSW/cuVS. **CREATE now returns
  before the index is populated**; queries until CDC catch-up are partial. REINDEX
  = an idxcron full-rebuild action (define à la cuVS reindex).
- A deleted/superseded doc still contributes to `df`/`N`/`avgdl` until reindex
  (standard Lucene/cuVS); the reindex makes it exact. **Stat drift scales with the
  un-reindexed delete/update ratio**, so the reindex gate should weigh delete-ratio,
  not only frame count (ranking is relative → top-K largely preserved; absolute
  BM25 drifts). Also: masked frames still inflate block-max bounds → WAND prunes
  fewer blocks (perf, not correctness) until the next reindex. **Invariant:** count
  `N` and per-term `df` over the *same* frame set (both include masked, or both
  exclude) — mixing them lets `df > N` → negative `idf` → NaN scores.
- UPDATE = `(op, pk)` delete frame + new segment frame at higher `chunk_id`;
  highest-`chunk_id` copy wins, older copies denied at load.
- **Reindex concurrency:** idxcron takes a source snapshot, rebuilds tag=0, and
  wipes tag=1 frames bounded to that snapshot — coordinated with the sinker (mirror
  cuVS's index lease/swap in `small_tail.go`).

### Phasing
- **B1**: parser-aware `AlwaysAsync(idx)` + unified `IndexIsAsync` helper (drops
  inline DML for retrieval) + CDC registration + tag=1 delete format + search-side
  tombstone filter (deletes work; inserts via existing build).
- **B2**: WAND ISCP sinker (incremental INSERT/UPSERT → segment append) +
  multi-segment search adapter.
- **B3**: idxcron `Merge` compaction + `max_index_capacity` rollover.

### Implementation status (branch `fulltext_wand`, HEAD `18713cb44`, updated 2026-07-02)

**Branch state:** the Phase-B foundation (commit `eb94f4ae8`) is committed, then
**`cuvs_quantize` was merged into `fulltext_wand`** (merge commit `7c20d9286`).
The merge's only conflict was the goyacc-**generated** `mysql_sql.go` (the
`RETRIEVAL` grammar vs. the quantize-side grammar); resolved by regenerating from
the cleanly-merged `mysql_sql.y` (0 goyacc conflicts, `RETRIEVAL` preserved), not
by hand-merging the generated output. Post-merge `go test ./pkg/fulltext/wand/`
is **green**, so the foundation below survived the merge unchanged.

**Update 2026-07-02:** the **read path (item e) is now built + unit-tested on top**
— `chunk_id`-ordered liveness (commit `2a0fdc4b8`), the tag=1 `CdcTail` frame
codec (`frames.go`, `2a0fdc4b8`), and tag-aware multi-segment load/search
(`a91af0e15`). Only the **write path** (sinker b–d) and idxcron (g) remain; the
SQL of `loadTailFrames`/`Load` still needs a live e2e run.

**✅ Done + unit-tested (committed in `eb94f4ae8`)** — every piece verifiable without a running server:
- **Parser-aware async.** `SyncDescriptor.AlwaysAsync` bool → `Hooks.AlwaysAsync(indexAlgoParams string) bool` (`pkg/indexplugin/catalog/hooks.go`); HNSW/CAGRA/IVF-PQ=true, IVF-FLAT=false, **fulltext=`parser=="retrieval"`** via new `catalog.GetIndexParser` + `IndexAlgoParamParser`. Unified `indexplugin.IndexIsAsync(algo, params)` routed through CDC-registration (`iscp_util.go`), ALTER-clone (`alter.go`), and the three fulltext DML early-returns (`build_dml_util.go`). No `async`-param injection. Tests: `TestFullTextAlwaysAsync` + the vector runtime tests.
- **PK-reconciliation liveness** (`pkg/fulltext/wand`). `ComputeLiveness(segs, deletes) []Membership` (per-segment allow precomputed once at load: owner = winning segment holding pk, dead iff a later delete). `SearchSegmentsLive(...)` ANDs liveness with the WHERE-prefilter; `SearchSegments` = the `nil` fast path. **[2026-07-02 DONE (`2a0fdc4b8`): applied — `WandModel.LSN` renamed `ChunkId` and `ComputeLiveness` orders by it; `AssembleFrames` sets it from each frame's `chunk_id` at load. No `LSN`/`segno`/timestamp anywhere.]** Fixes a latent bug: the old multi-segment path emitted a pk twice if it lived in two segments. Tests: `TestWandLiveness` (dedup_update / delete_then_reinsert / delete_after_insert / pure_delete / mixed). NB: assert the live **pk set**, not exact scores — global `N`/`df`/`avgdl` still include superseded+deleted docs until reindex (accepted drift).
- **tag=1 delete-log codec** (`deletes.go`): `DeleteRecord{Pk, LSN}`, `EncodeDeleteLog`/`DecodeDeleteLog` (self-contained binary+crc32, **no** GPU-coupled cuVS import), `DeleteMap` fold → feeds `ComputeLiveness`. Tests: `TestWandDeleteLogRoundTrip` (int64/varchar/corruption/empty). **[2026-07-02 DONE (`2a0fdc4b8`): `DeleteRecord` slimmed to `{Pk}` (stored order field dropped); `DeleteMap` → `FoldDeleteFrame(m, recs, chunkId)` folds each delete frame at its `chunk_id`.]**
- **`ToInsertSqls(cfg, ts, tag)`** — tag=0 compacted main / tag=1 CDC delta segment (`storage.go`; caller `fulltext_wand_create.go` passes 0). Test: `TestWandToInsertSqlsTag`.

**Status (2026-07-02): read + write path DONE and validated e2e on a live
single-node instance** — CREATE → async InitSQL build → BM25-ranked top-K; CDC
INSERT searchable in ~15s; CDC DELETE removes the doc; the search cache is evicted
after each build. BVT: `fulltext/fulltext_retrieval` +
`pessimistic_transaction/fulltext/fulltext_retrieval_async`. **Only idxcron
compaction (g) remains.** Two known gaps: the planner still injects a score Sort
above `fulltext_wand_search` (perf, not correctness — the "no SORT" goal is unmet;
the LIMIT does push into the operator), and cache eviction is CN-local (multi-CN
broadcast is a follow-up). Item detail:
- **(a) ~~`DataRetriever.GetLSN()` accessor~~ — DROPPED (2026-07-01).** The dedup
  decision orders frames by `chunk_id` in the single tag=1 `CdcTail` log, not ISCP's
  LSN, so **no `DataRetriever` interface change is needed**. The sinker appends at
  `chunk_id = NextChunkIdSql(CdcTailId, tag=1)` (item (b)). Rationale: the "single
  CdcTail log, chunk_id-ordered" decision — neither HNSW (`Contains(pk)`) nor IVF-PQ
  (`pkid` fold at `Load`) couples identity to ISCP's LSN.
- **(b) `WandSqlWriter` + `RunWand`** — ✅ **DONE (`fd2de4978`; cache-evict `005646426`), e2e-validated** (store showed `tag=1 cdc_tail` frames; cold-cache reload returned the CDC'd doc). The CDC blob is BINARY (`WandCdc`, typed `encodePk`) not JSON, because a retrieval pk is `any` (int64 OR varchar). In `pkg/iscp` — mirror `NewFulltextSqlWriter` (`index_sqlwriter.go:251`) + `runHnsw` (`index_consumer.go:240`). Writer accumulates insert `{pk,text}` + delete `{pk}` rows → blob; `RunWand` consumes blobs → tokenize via `tokenizer.SharedJiebaTokenizer(false)` (**same** path as search `parsePatternInNLModeJieba`, so build/query tokens match) → `Builder.Add` → on channel close `FinishSegments` → **append the segment frame(s) and delete frame(s) to the tag=1 `CdcTail` log at `NextChunkIdSql`** + `UpdateWatermark`. (`pkg/iscp` already imports algo pkgs e.g. hnsw, so importing `pkg/fulltext/wand` is consistent — no cycle.) **Codec ready (2026-07-02, `a91af0e15`):** `FrameSegment` / `FrameDeletes` produce the tag=1 frames and the read side (`AssembleFrames`/`loadTailFrames`) already consumes exactly what the sinker writes — so (b) is now just: accumulate rows → `Builder.Add` → `FinishSegments` → `FrameSegment` → append at `NextChunkIdSql`; deletes → `FrameDeletes`.
- **(c)** — ✅ **DONE (`fd2de4978`).** Branch the fulltext plugin iscp `Hooks` (`pkg/fulltext/plugin/iscp/iscp.go` `NewSqlWriter`/`Run`) on `parser==retrieval` → Wand writer/run, else the postings writer/`RunIndex`.
- **(d)** — ✅ **DONE (`4135f9cc2`; InitSQL atomicity `f1b4d5d8d`).** In `HandleCreateIndex` (`pkg/fulltext/plugin/compile/compile.go`), retrieval is forced async (`parser=="retrieval"`, mirroring `Hooks.AlwaysAsync`) → registers a CDC task with a non-empty **InitSQL that builds the WAND store from postings** (`genInsertSQL` → postings, `genWandBuildSQL` → tag=0), run at ISCP task start; the postings table stays required. **ISCP had no multi-statement InitSQL**, so the InitSQL format became a **JSON array of statements** (`splitInitSQL`: array / JSON string / raw single, backward-compat) run in **one atomic txn** (commit-on-success / rollback-on-error). The dead sync-branch WAND build was removed.
- **(e) Multi-frame search adapter** — ✅ **DONE (2026-07-02, `a91af0e15`).** New
  tag=1 frame codec (`frames.go`: `FrameSegment`/`FrameDeletes`/`AssembleFrames`
  over the reused cuVS `FrameCdcChunk` envelope — confirmed pure-Go, no gpu
  coupling); `loadTailFrames` (`storage.go`) reads tag=1 `CdcTail` rows (each row
  one complete frame) `ORDER BY chunk_id`; `WandSearch` now holds `segs` (tag=0
  base at `ChunkId=-1`, below the tail, + tag=1 tail) + `deletes`, searched via
  `searchSegsLive` = `ComputeLiveness` + a **per-segment** WHERE prefilter (built
  once per segment so a pk-filter resolves against that segment's own ord→pk map)
  + `SearchSegmentsLive`. Unit-tested (`TestWandTailFrames`,
  `TestWandSearchSegsLive`); the SQL of `loadTailFrames`/`Load` still needs a
  live e2e run.
- **(f) ~~Drop the postings hidden table~~ — DROPPED.** Postings is the
  CREATE / full-reindex build pipeline (`source → postings → SQL aggregate/sort →
  fulltext_wand_create → tag=0`), not scratch — it stays. Only the incremental CDC
  delta path is postings-free.
- **(g)** idxcron **full-reindex** trigger + gate (B3) — full rebuild from source that wipes the tag=1 `CdcTail`; no incremental merge.

✅ **Resolved (2026-07-02):** the sinker (b–d) has landed and is e2e-validated — a
retrieval index now reflects INSERT/DELETE via CDC (tag=1 frames + cache eviction),
searchable within CDC latency (~15s in the single-node test). The intermediate
"no CDC maintenance" state is closed. Remaining: (g) idxcron compaction that wipes
the tag=1 `CdcTail` on a full reindex.

## Verification
- **Unit**: (1) `tokenize → postings → fulltext_wand_create` round-trip builds a loadable WAND index. (2) **Differential**: `fulltext_wand_search` top-K vs a brute-force reference (`Σ tf·idf²` over all docs + full sort) on randomized corpora → assert identical top-K and scores (the WAND-correctness gold test). (3) Parser/mode parse tests in `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go` (`IN RETRIEVAL MODE`, default-on-retrieval-parser).
- **Build**: goyacc regen compiles (0 conflicts); `go build ./pkg/...`; `go test ./pkg/fulltext/... ./pkg/sql/parsers/dialect/mysql/ ./pkg/sql/colexec/table_function/`.
- **E2E (manual / BVT)**: `CREATE FULLTEXT INDEX … WITH PARSER retrieval`; insert docs; `MATCH(question) AGAINST('孩子 营养 早餐 视频 文案')` (bare → retrieval mode) returns ranked top-K; `EXPLAIN` shows `fulltext_wand_search` and **no SORT node**; boolean/NL mode on a retrieval index errors; `IN RETRIEVAL MODE` on an ngram index errors; reindex rebuilds the index; relevance sanity-check vs `IN BOOLEAN MODE` on an ngram index.
- **Prefiltering**: the same query with `WHERE delete_flag IS NULL` (and a selective `category = …`) returns exactly the filtered top-K (membership applied inside WAND); compare results to a brute-force filtered scan; confirm no over-fetch/post-filter shrinkage and that the membership filter is pushed into `fulltext_wand_search`.
- **Perf**: the original slow OR query on a `retrieval` index — confirm no SORT operator and materially lower latency than the boolean path.
