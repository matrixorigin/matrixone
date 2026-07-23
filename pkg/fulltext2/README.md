# `pkg/fulltext2` — WAND-based positional fulltext engine

`fulltext2` is the full-text index engine selected by `CREATE FULLTEXT INDEX … WITH (VERSION = 2)`
(and by the experimental `CREATE FULLTEXT2 INDEX`). It is a **completely separate engine** from
classic v1 fulltext (generated-SQL + Go `Eval`) and from `bm25` (position-free ranked retrieval).
The `fulltext` plugin is a thin version-router that delegates each hook to v1 verbatim or to this
package.

It reuses the shared **index-plugin framework** with bm25 — the plugin hooks, the ISCP CDC
consumer, `VectorIndexCache`, idxcron `MERGE`/`REBUILD`, and the payload-agnostic CDC chunk
framing — plus bm25's leaf serialization patterns (tar members, little-endian buffers, PK
encode/decode). But the segment *backbone* genuinely diverges from bm25, so the engine is kept
separate rather than sharing a lowest-common-denominator core:

| | bm25 | fulltext2 (here) |
|---|---|---|
| **term dict** | jieba dict-id + unordered overflow map | **sorted** dict of the actual indexed terms (needed for `word*`) |
| **posting payload** | df / impact only | **+ positions** (phrase, ngram reassembly) |
| **query model** | whole-sentence bag → WAND | parser → operator tree |
| **scoring** | BM25 | **TF-IDF *and* BM25** |

---

## Table of contents

1. [Architecture at a glance](#1-architecture-at-a-glance)
2. [The segment format](#2-the-segment-format)
3. [Core in-memory structures](#3-core-in-memory-structures)
4. [The query model](#4-the-query-model)
5. [Algorithm: Block-Max WAND (top-k)](#5-algorithm-block-max-wand-top-k)
6. [Algorithm: phrase & boolean evaluation](#6-algorithm-phrase--boolean-evaluation)
7. [Scoring: TF-IDF and BM25](#7-scoring-tf-idf-and-bm25)
8. [Multi-segment: liveness, deletes, recency](#8-multi-segment-liveness-deletes-recency)
9. [Build & lifecycle: sync build, CDC, MERGE/REBUILD](#9-build--lifecycle-sync-build-cdc-mergerebuild)
10. [Memory model: mmap, lazy decode, bounded build](#10-memory-model-mmap-lazy-decode-bounded-build)
11. [File map](#11-file-map)

---

## 1. Architecture at a glance

An index is a set of **segments** plus a small overlay of deletes:

```
CREATE INDEX ──► sync build (from source rows) ──► tag=0 BASE segments
     │
INSERT/UPDATE/DELETE ──► CDC (ISCP) ──► tag=1 TAIL segments (+ delete frames)
     │
ALTER … REINDEX  ──► MERGE (fold tail into base) / REBUILD (from source)

query ──► Index{ segments:[base…, tail…], deletes } ──► per-segment WAND/phrase ──► global top-k
```

- **Base segments** (`tag=0`) are written by the synchronous build (`fulltext2_create` TVF) and
  by REBUILD; they are stored as chunk rows and materialized on the LOCAL (SSD) fileservice under
  `__fulltext2` and **mmap'd read-only**.
- **Tail segments** (`tag=1`) are CDC deltas appended incrementally; they are smaller and loaded
  into the Go heap.
- A query builds an `Index` over *all* base + tail segments, runs the retrieval algorithm on each
  segment against **global** corpus statistics, and merges into one top-k. Per-segment **liveness**
  hides docs superseded by a newer copy or a tombstone.

The engine answers three query shapes, all through one `Index`:

| Mode | SQL | Semantics |
|---|---|---|
| **NL (natural language)** | `MATCH(col) AGAINST('quick brown')` | exact **ordered phrase** (positional) |
| **BOOLEAN** | `… AGAINST('+quick -fox "brown dog" quic*' IN BOOLEAN MODE)` | operator tree (`+`/`-`/`~`/`>`/`<`/`()`/`"…"`/`*`) |
| **BM25 / bag-of-words** | `… IN BM25 MODE` | pure disjunction of tokens (position-free) |

---

## 2. The segment format

A segment is the unit of storage and retrieval. Every segment — base or tail — has the same
logical shape (`doc.go`, `serialize.go`):

```
segment
 ├─ term dict :  term(string) → byte offset of its posting directory   (SORTED, dictionary-free)
 ├─ postings  :  per term, doc-sorted:  docID/tf blocks + block-max meta + POSITIONS
 ├─ docmap    :  ord → { pk, docLen };   N = doc count
 └─ avgDocLen :  Σ docLen / N   (computed at LOAD across all loaded segments)
```

**On disk** it is a **tar archive** of five members (`serialize.go`):

| Member | Contents | Residency at load |
|---|---|---|
| `docmap` | `pkType`, `N`, `ord→pk` (length-prefixed), `ord→docLen` | viewed (mmap); pk decoded on demand |
| `termdict` | the **vellum FST**: `term → offset of its directory entry` | resident (compact, minimized) |
| `postings` | the **ranking directory**: per-term `df`, per-block max-TF / skip meta | resident (small) |
| `blocks` | per-term `docID/tf` blocks (delta + varint) | **mmap; block-decoded on demand** |
| `positions` | per-term compressed positions (phrase-only) | **mmap; decoded on demand** |

Two deliberate properties:

- **Dictionary-free.** The term dict holds exactly the terms that were indexed (gojieba words or
  ngram bigrams), with *no jieba-dict dependency* in the backbone. One sorted structure gives both
  exact term lookup **and** prefix enumeration (`word*`). On disk it is a **vellum FST** — a
  compact minimized automaton; because it is byte-oriented and UTF-8 byte order equals Unicode
  code-point order, CJK terms need no special handling (Go's string sort already produces the FST's
  required ascending byte order). See `termdict.go`.
- **Scorer-agnostic block-max.** The block-max section stores the **raw** per-block max-TF and
  min-doc-length (`deriveTermStats`), never a baked impact score. So *one* segment serves both
  TF-IDF and BM25; the active scorer derives its max-impact bound at query time.

Blocks are `BlockSize = 128` docs. Block-level bounds (`blockLastDoc`, `blockMaxTf`,
`blockMinDocLn`) let WAND locate and skip whole 128-doc regions without decoding them.

---

## 3. Core in-memory structures

### `Index` (`index.go`)

The queryable handle over a set of segments.

```go
type Index struct {
    segments []*Segment
    deletes  map[any]int64   // normalizeKey(pk) → recency at/after which older copies are dead
    globalN         int64    // total live docs across segments
    globalAvgDocLen float64  // global average doc length (for BM25 length normalization)
    liveOrd [][]bool         // liveOrd[si][ord] = is that doc live?  nil ⇒ whole segment live
}
```

- `NewIndex(segments, deletes)` builds it; `resolve()` (lazy, once) computes `globalN`,
  `globalAvgDocLen`, and the per-segment `liveOrd` bitmaps.
- **Global stats matter**: a term's IDF and the corpus `avgDocLen` are computed *across all
  segments*, not per-segment — otherwise appending a CDC tail would shift every score. WAND uses
  these global stats so a multi-segment index ranks identically to a single rebuilt one.
- `liveOrd` is the **sole resident liveness structure**. A fully-live (append-only) segment keeps
  `liveOrd[si] == nil` — the "all live" fast path — so an append-only index costs *zero* resident
  liveness heap. The transient `pk→loc` map that `resolve()` builds to derive it is local and
  discarded (it was the dominant load-time heap floor).

### `Segment` (`segment.go`)

A segment has **two representations** that never coexist:

- **Build-side** (in-memory, freshly built): `pks []any`, `terms map[string]*termPostings`,
  `sortedTerms []string`. Postings hold docIDs/tfs/positions as plain Go slices.
- **Loaded-side** (deserialized for query): `pks == nil`. Instead:
  - `dict *termDict` — the vellum FST (`term → directory-entry offset`).
  - `ranking / blocks / positions []byte` — **views into the mmap/blob**; a base segment's are
    backed by `mmapData` (page-cache, reclaimable, shared by all concurrent queries, no copy).
  - `pkOffsets []int32` + `pkRaw []byte` — the docmap bytes; `pk(ord)` decodes a pk on demand
    (instead of materializing `N` boxed `any` pks, ~24 B each).

A loaded segment **expands nothing at load**: `LookupLoaded(term)` decodes only the touched term's
directory entry on demand; WAND then decodes only the *blocks* its walk lands on. Resident heap is
`O(current query)`, not `O(vocabulary)`. `Free()` releases the mmap under the cache's eviction
write-lock.

`Recency` orders segments when the same pk lands in several (UPDATE / reinsert / stale base copy):
only the highest-`Recency` copy is live.

### `termPostings` (`segment.go`)

One term's posting list.

```go
type termPostings struct {
    // build-side (nil on a loaded segment):
    docIDs    []int64     // ascending doc ords
    tfs       []uint8     // capped term frequency (≤ MaxCappedTf)
    positions [][]int32   // per-doc token positions (byte offsets)

    ndoc int              // df (document frequency)

    // loaded-side: block-compressed views into the mmap
    blockData []byte;  blockOff []int64      // docID/tf blocks (delta+varint), one per 128 docs
    posRaw    []byte;  blockPosOff []int64   // compressed positions, block-seekable

    // scorer-agnostic score-UB inputs (raw):
    maxTf     uint8;  minDocLen int32        // whole-list bounds
    blockLastDoc []int64;  blockMaxTf []uint8;  blockMinDocLn []int32  // per-block bounds
}
```

The docID/tf blocks are the largest section (~46 % at load) and positions ~48 %; **neither is
expanded at load** — WAND touches only the blocks its block-max walk visits; positions are read
only for phrase verification / MERGE.

### `wandIter` (`wand.go`)

A per-term posting **cursor** for the WAND walk, carrying its `maxImpact` bound and a small
per-cursor decoded-block cache (`bDocs`/`bTfs`, cap 128). Key methods: `doc()` (cached current
ord), `skipTo(d)` (locate block via resident `blockLastDoc`, binary-search within), `blockMax(d)`
(block-level score UB), `blockEndAt(d)`. See §5.

### `Builder` / `TailBuilder` (`build.go`, `tailbuild.go`)

- `Builder` accumulates `(word, position, pk)` occurrences via `Add`, then `FinishSegments` /
  `Finish` assembles a `Segment`. It seals at `min(max_index_capacity docs, max_postings_capacity
  postings)` so per-segment build memory is bounded (see §10).
- `TailBuilder` is the streaming CDC builder: it tokenizes insert rows into capacity-capped
  segments, **spilling each sealed segment to a temp file** as it fills, so the sinker's peak
  memory is one open segment, not the whole CDC stream. Deletes are spilled likewise.

---

## 4. The query model

`SearchQuery(pattern, boolean, parser, algo, k, filter)` (`query.go`) is the dispatch:

- **BOOLEAN mode** → `buildBooleanQuery` parses the pattern into a `BoolQuery` (an operator tree)
  and calls `SearchBoolean`.
- **NL mode** → `phraseSlots` tokenizes into positioned slots and calls `SearchPhrase` — an exact
  **ordered phrase** (matching classic fulltext NL semantics, including CJK).
- **BM25 mode** → `SearchBagOfWords` tokenizes the whole pattern into a pure disjunction of tokens
  (each a SHOULD term) and runs the position-free WAND — bag-of-words retrieval that works on a
  `POSITION_FREE` index.

The boolean operator tree is built from `clause` nodes (`boolean.go`):

```go
type clause struct {
    kind     clauseKind      // clauseTerm | clausePhrase | clausePrefix | clauseAnd | clauseOr | clauseNot | group
    terms    []string        // leaf term(s) / prefix
    phrase   []phraseSlot    // positional (byte-offset) slots for a "…" phrase
    children []clause        // group
    weight   float32         // impact multiplier: 1.0 default; +/-/~/</>/() change it
}
```

Operators: `+term` (MUST), `-term` (MUST NOT), `~term` (weight down), `>`/`<` (weight up/down),
`(…)` (group), `"…"` (exact phrase), `word*` (prefix → OR of all terms with that prefix, enumerated
from the FST). A bare multi-word run in boolean mode is an implicit contiguous phrase.

`phraseSlot{ term, off }` carries a term and its byte offset within the query, so phrase adjacency
in the query matches adjacency in the source text.

---

## 5. Algorithm: Block-Max WAND (top-k)

**WAND** (Weak-AND, Broder 2003) + **Block-Max WAND** (Ding & Suel 2011) answer disjunctive
top-k — a pure OR of terms — returning *the exact same top-k as a full scan* while skipping most
documents. Implemented in `searchWAND` (`wand.go`).

**Idea.** Each term carries a `maxImpact` (its largest possible weighted contribution). The top-k
min-heap's k-th best score is a moving threshold **θ**. Any document whose summed term upper bounds
cannot reach θ can never make the top-k, so it is never scored.

**Per-iteration loop** (`searchWAND`):

```
loop:
  1. insertion-sort cursors by current doc ascending   (nearly-sorted between iters ⇒ O(n))
  2. pivot = first cursor where Σ maxImpact[0..i] ≥ θ   (term-level WAND)
     extend pivot over all cursors also sitting on pivotDoc
  3. blockSum = Σ blockMax(pivotDoc)[0..pivot]          (Block-Max refinement, tighter UB)
     if blockSum ≤ θ:  skipTo(block end) — skip the whole 128-doc region, score nothing
  4. elif iters[0].doc() == pivotDoc:   score pivotDoc, push to heap, advance; update θ
  5. else:                              skipTo(pivotDoc) — align a lagging cursor
```

Design notes worth knowing:

- **Insertion sort, not `sort.Slice`** (step 1): the cursor array is nearly sorted between
  iterations (only the skipped cursor moved), so insertion sort is O(n); `sort.Slice` boxed the
  slice into an `interface{}` and heap-allocated the `less` closure every call — that alloc churn
  dominated query CPU.
- **Cached `doc()`**: `wandIter.cur` is recomputed only on cursor move (`refresh`), because it is
  read many times per pivot iteration (sort, pivot scan, blockMax, alignment).
- **Block-skip is free**: it only removes docs whose score UB ≤ θ — which the `score > θ`
  insertion rule would reject anyway. Same top-k **set** and **scores** as the full scan; only the
  work differs.
- **WHERE prefilter**: `allow` (a `Membership`) admits a doc to the heap only if its ord passes,
  so `LIMIT` bounds the *filtered* set. Block-skip stays valid — `blockSum` bounds every doc in the
  region regardless of the filter.
- **`newTopKHeap` bounds k** to the segment's live-doc count, so an absurd pushed `LIMIT 5e8`
  can't eagerly allocate GB-sized heap buffers and OOM the CN.

**Tie caveat**: documents with an *exactly equal* score have unspecified order (WAND sums
contributions in cursor order, the full scan in clause-map order — they can differ in the last
float ULP). The top-k *set* above the boundary and the score *multiset* are identical.

The no-LIMIT sibling `streamWAND` (`stream.go`) reuses the cursors but walks *every* matching doc
in ord order (no θ, no heap), emitting in bounded batches — used when the upstream `ORDER BY score`
does the ranking.

---

## 6. Algorithm: phrase & boolean evaluation

### Exact phrase (NL mode) — `SearchPhrase` (`index.go`, `search.go`)

Two-phase positional match. Candidate docs come from the *rarest* term's posting list (fewest
docs); each candidate is verified by decoding positions and checking that the query's terms occur
at the exact relative byte offsets (`phraseSlot.off`). Only verified docs are scored. This is the
positional analogue that makes `AGAINST('brown fox')` match "brown fox" but not "fox … brown".
`boundedTopK` keeps the top-k without materializing all matches.

### Boolean — `SearchBoolean` (`index.go`, `boolean.go`)

The operator tree is evaluated per segment:

- **SHOULD** (bare/OR terms) → `searchWAND` (disjunctive top-k), or `searchBooleanFull` when the
  tree also has MUST/MUST-NOT/phrase clauses that need a materialized candidate set.
- **MUST / MUST-NOT** intersect / subtract candidate sets.
- **phrase** clauses verify positionally (as NL).
- **prefix** (`word*`) enumerates matching terms from the FST and unions their posting lists.
- Boolean scoring is `O(N ≤ capacity)` per segment (a dense score array), deliberately bounded by
  the segment size.

All three top-k paths use `vectorindex.FastMaxHeap` (SoA, keyed by ord, distance = −score) — zero
per-candidate allocation, ties unspecified (bm25 parity).

---

## 7. Scoring: TF-IDF and BM25

`ScoreAlgo` selects the scorer; both are supported from the *same* segment because block-max is
raw (§2).

- **TF-IDF**: `weight · tf · idf`.
- **BM25**: `weight · idf · bm25Factor(tf, docLen, avgDocLen)` — the saturating term-frequency
  factor with document-length normalization against the **global** `avgDocLen`.

Key quantities:

- `idf` is computed from **global** df/N (across all segments), so a CDC tail doesn't shift scores.
- `termMaxImpact` / `blockMax` derive the WAND upper bounds in the *same* scorer, so the block-skip
  bound is always ≥ the real score — WAND returns the identical ranking to a full scan.
- `tf` is capped (`MaxCappedTf`) to one byte; fulltext2 keeps *real* tf (unlike a position-free
  impact-only index), so phrase/NL scoring is faithful.

---

## 8. Multi-segment: liveness, deletes, recency

An index is append-mostly: base segments plus CDC tail segments plus delete frames. Correctness
comes from three overlays computed in `Index.resolve()`:

- **Recency**: when the same pk appears in several segments (UPDATE, reinsert, a stale base copy),
  only the **highest-Recency** copy is live. Tail chunk_id > base recency, so a later append always
  wins.
- **Deletes**: `deletes[pk] = recency` — any copy of `pk` older than that recency is dead
  (tombstone). CDC delete frames carry pk-only.
- **`liveOrd[si]`**: a per-segment ord-indexed liveness bitmap derived once; `nil` means "whole
  segment live" (append-only fast path). Every liveness check — phrase `isLive`, boolean/stream
  `livenessMembership`, and compaction `ReconstructLiveDocs` — is an O(1) bitmap index, allocation
  free.

A skipped/dead doc is never scored and never emitted; `globalN` counts only live docs.

---

## 9. Build & lifecycle: sync build, CDC, MERGE/REBUILD

Index CREATE → sync build → CDC maintenance → periodic compaction. All bases/tails are chunk rows
in two hidden tables (storage + metadata), framed with the shared CDC chunk format.

- **Sync build** (`fulltext2_create` TVF): CROSS APPLY over the source table, tokenize each row
  (datalink → plain text, json → values, ngram/gojieba parser), `Add` to a streaming `Builder`,
  seal + persist tag=0 base sub-segments as they fill. `CREATE FULLTEXT` with `VERSION=2` (or
  `CREATE FULLTEXT2`) registers this + an always-async CDC task.
- **CDC** (`cdc.go`, `tailbuild.go`, `sink.go`, ISCP consumer): INSERT/UPSERT/DELETE flow into the
  tag=1 tail. `TailBuilder` tokenizes inserts into capped, spilled tail segments; deletes into
  spilled tombstone frames; on flush it appends them (delete-frames first, then insert segments) at
  the next chunk_id in one txn, advancing the watermark.
- **MERGE** (`compact.go`, `fulltext2_compact` TVF): fold the tag=1 tail into the tag=0 base and
  reclaim dead space — load base+tail+deletes, `ReconstructLiveDocs` (from postings, no re-tokenize)
  into a fresh capacity-bounded base, atomically replace all prior bases + the whole tail.
- **REBUILD**: discard the tail and rebuild the base from source (`buildFromSource`).
- idxcron schedules MERGE (or REBUILD once dead-doc % is high) when the tail grows past a chunk
  threshold.

---

## 10. Memory model: mmap, lazy decode, bounded build

Full-text indexes are large; the engine is careful on both the query and build sides.

**Query side:**

- Base segments are **mmap'd read-only** on the fast LOCAL (SSD) `__fulltext2` fileservice dir —
  page-cache-backed (reclaimable, *not* Go heap), shared by all concurrent queries, no copy.
- A loaded segment expands **nothing** at load: FST resident (compact), directory entries decoded
  per touched term, docID/tf blocks decoded per block the WAND walk visits, positions decoded only
  for phrase verification. Resident heap is `O(current query)`, not `O(vocabulary)`.
- pks are decoded on demand from the docmap bytes (no `N` boxed `any`).
- `liveOrd == nil` for an append-only segment ⇒ zero resident liveness heap.
- A base-load budget guard fails fast (clear error) rather than letting a huge base OOM-kill the CN.

**Build side** — per-segment build memory ≈ **Σ postings** (term occurrences held in
`Builder.docs`), *not* doc count (a doc can hold 1 token or thousands). So every streaming build
path (create TVF, CDC `TailBuilder`, MERGE/REBUILD `CompactSegments`) seals a segment on
`ReachedSegmentCap(b, docCap, postingCap)` — **whichever of `max_index_capacity` (docs) or
`max_postings_capacity` (postings, default 8M ≈ ~512 MB build peak) fires first**. This bounds
build memory regardless of document shape.

---

## 11. File map

| File | Responsibility |
|---|---|
| `doc.go` | package overview + segment-format summary |
| `index.go` | `Index` type, `resolve()`, `SearchPhrase`/`SearchBoolean`/`SearchText` entry points, liveness |
| `segment.go` | `Segment` (build-side vs loaded-side), `termPostings`, `deriveTermStats`, `BlockSize` |
| `wand.go` | `wandIter`, Block-Max WAND (`searchWAND`), `buildWandIters`, top-k heap |
| `stream.go` | no-LIMIT streaming WAND (`streamWAND`, `StreamQuery`, `StreamBagOfWords`) |
| `query.go` | `SearchQuery` dispatch, `SearchBagOfWords`, boolean-query build, phrase slots |
| `boolean.go` | boolean operator tree (`clause`, `clauseKind`), boolean evaluation |
| `search.go` | phrase evaluation, per-segment scoring glue, `boundedTopK` |
| `membership.go` | WHERE-prefilter + liveness `Membership` adapters |
| `termdict.go` | vellum FST term dict: build, load, exact `get`, `prefixIter`, `forEachTerm` |
| `build.go` | `Builder` (Add/Finish/FinishSegments), `TokenizedDoc`, `ReachedSegmentCap`, seal caps |
| `tailbuild.go` | streaming CDC `TailBuilder` (spill-as-you-fill) |
| `cdc.go` | CDC event decode (`Cdc`), pk typing |
| `deletes.go` | delete-record framing / tombstones |
| `frames.go` | CDC chunk framing (`FrameSegment`, `UnframeTail`) |
| `sink.go` | ISCP sink adapter glue |
| `compact.go` | `CompactSegments` (MERGE): reconstruct live docs → fresh bounded base |
| `serialize.go` | tar-archive segment encode/decode (docmap/termdict/postings/blocks/positions) |
| `storage.go` | chunk-row persistence, `LoadAllBases`/tail, `__fulltext2` spill, load budgets, `TableConfig` |
| `search_cache.go` | `VectorIndexCache` integration (per-CN cached loaded index) |
| `mmap_unix.go` / `mmap_other.go` | read-only mmap + munmap (platform) |

For the plugin wiring (hooks, grammar, CDC registration, idxcron), see `pkg/fulltext2/plugin/…`.
