# `pkg/fulltext2` — WAND-based positional fulltext engine

`fulltext2` is a full-text index engine registered as the distinct index algorithm `"fulltext2"`
and created with `CREATE FULLTEXT2 INDEX` (experimental, gated by the
`experimental_fulltext2_index` session var). It is a **completely separate engine** from classic
v1 fulltext (generated-SQL + Go `Eval`): a from-scratch Block-Max WAND positional engine with its
own registered plugin (`pkg/fulltext2/plugin`), segment format, and query pipeline — **not** a
version of the `fulltext` plugin.

> There is no `WITH (VERSION = 2)` syntax. fulltext2 is its own algorithm keyed by
> `catalog.MoIndexFullText2Algo`; you select it with `CREATE FULLTEXT2 INDEX`, not by versioning a
> classic `FULLTEXT` index.

fulltext2 also **absorbs the position-free ranked-retrieval role** once served by the former
standalone `bm25` engine (now removed): a `POSITION_FREE` fulltext2 index queried `IN BM25 MODE`
is the bag-of-words path.

It rides the shared **index-plugin framework** — the plugin hooks, the ISCP CDC consumer,
`VectorIndexCache`, idxcron `MERGE`/`REBUILD`, and the payload-agnostic CDC chunk framing — plus
common leaf serialization patterns (tar members, little-endian buffers, PK encode/decode).

One engine serves two build modes, chosen at CREATE:

| | positional (default) | `POSITION_FREE = TRUE` (bag-of-words) |
|---|---|---|
| **term dict** | **sorted** FST of the actual indexed terms (`word*`) | same sorted FST |
| **posting payload** | **+ positions** (phrase, ngram reassembly) | df / tf only (no positions, ~57% smaller) |
| **query model** | NL phrase · BOOLEAN tree · BM25 bag | `IN BM25 MODE` bag-of-words **only** |
| **scoring** | **TF-IDF *and* BM25** | **TF-IDF *and* BM25** |

A `POSITION_FREE` index rejects NL / BOOLEAN phrase queries up front (it has no positions) — only
`MATCH(…) AGAINST(… IN BM25 MODE)` is valid against it.

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
9. [INCLUDE columns: in-index prefilter & coverage](#9-include-columns-in-index-prefilter--coverage)
10. [Build & lifecycle: sync build, CDC, MERGE/REBUILD](#10-build--lifecycle-sync-build-cdc-mergerebuild)
11. [Memory model: mmap, lazy decode, bounded build](#11-memory-model-mmap-lazy-decode-bounded-build)
12. [File map](#12-file-map)

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
 ├─ docmap    :  ord → { pk, docLen [, INCLUDE values] };   N = doc count
 └─ avgDocLen :  Σ docLen / N   (computed at LOAD across all loaded segments)
```

**On disk** it is a **tar archive** of five members (`serialize.go`):

| Member | Contents | Residency at load |
|---|---|---|
| `docmap` | `pkType`, `N`, `ord→pk` (length-prefixed), `ord→docLen`, optional `ord→INCLUDE values` (§9) | viewed (mmap); pk / include decoded on demand |
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

**Multi-term query: how the cursors combine.** One `wandIter` per SHOULD term
(`buildWandIters`); a document's score is the **sum of the contributions of whichever query
terms it contains** — disjunctive, so a doc need not hold every term. Only cursors *sitting on
the same doc* contribute to that doc's score (step 4 sums `it.tf()`-based contributions for
every `it` at `pivotDoc`). The pivot is what makes it fast: because cursors are sorted by
current doc and `maxImpact` is accumulated in that order, any doc below `pivotDoc` could only
contain the terms of cursors `0..pivot-1`, whose max-impacts sum to `< θ` — so it provably
can't make the top-k and is skipped without ever being read.

Worked example — query `A B C`, want top-1, θ currently 3.0, per-term
`maxImpact{A:4, B:1, C:5}`, cursors at `A@5, B@2, C@8`:

```
1. sort by doc         → [B@2, A@5, C@8]
2. pivot scan          → acc=B(1)=1 < 3 ; +A(4)=5 ≥ 3  ⇒ pivot=A, pivotDoc=5
                         ⇒ doc 2 is skipped: B alone (max 1) can never beat θ=3
3. blockSum{B,A}@5 = 4.2 > 3          ⇒ don't block-skip
4. iters[0].doc()==5 ?  lead is B@2 ≠ 5 ⇒ NOT aligned ⇒ skipTo(5) on B, re-loop
   (once cursors align on a doc, it is scored = Σ contributions of the terms it has;
    that raises θ, which prunes even more of the next iteration)
```

The heavier a term (larger `maxImpact`), the earlier the pivot lands on it and the more docs
below it are pruned; a rare, high-idf term therefore does most of the skipping.

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

WAND (§5) is **only** the pure-OR path. `SearchBoolean` dispatches on query shape:

```
pure OR of single-term SHOULDs      → searchWAND        (§5: θ/pivot/block-skip)
any MUST / MUST-NOT / phrase clause  → searchBooleanFull (dense accumulator, NO skip)
NL / "…" phrase                      → SearchPhrase      (conjunctive block-cursor)
```

The three engines treat their per-term cursors in fundamentally different ways — desynchronized
(OR), no cursors at all (boolean), or forced-to-converge (phrase). One worked example each below;
all share a corpus where the postings are:

```
term "quick" → docs {2, 5, 8, 40}          term "brown" → docs {5, 8, 9}
term "fox"   → docs {8, 60}                 (doc 8 = "quick brown fox", doc 5 = "quick brown …")
```

### Exact phrase (NL mode) — `SearchPhrase` → `matchPhraseCursor` (`search.go`)

Conjunctive, anchored on the **rarest** slot, then a byte-offset positional verify. Cursors are
forced to *converge* on the anchor's current doc — the opposite of WAND.

`AGAINST('brown fox')` → slots `[{brown, off=0}, {fox, off=6}]`:

```
1. rarest slot = "fox" (df 2)  ⇒ drive its docs {8, 60}
2. doc 8:  skipTo(8) the "brown" cursor → present. Positional check:
             fox @ pos p ⇒ phrase-start = p-6 ; is "brown" @ start+0 ?  yes ⇒ MATCH
   doc 60: skipTo(60) the "brown" cursor → absent ⇒ drop, no position decode
3. score only doc 8 (the one verified doc)
```

So `AGAINST('brown fox')` matches doc 8 ("brown fox") but **not** a doc containing "fox … brown"
out of order — the offset check (`phraseSlot.off`) is what makes it a phrase, not a bag. Positions
are decoded **only** for docs that survive the doc-level intersection; `boundedTopK` keeps the
top-k without materializing all matches. (This is also the only mode that reads positions.)

### Boolean AND / MUST / MUST-NOT — `searchBooleanFull` (`boolean.go`)

No impact cursors, no skipping: every clause's **whole** posting list is materialized into a dense
`O(N)` per-doc `score[]` array; AND is an intersection by **hit-count**, MUST-NOT an exclusion
bitset.

`AGAINST('+quick +brown -fox' IN BOOLEAN MODE)` → must `{quick, brown}`, mustNot `{fox}`:

```
1. mustNot "fox" → set bits {8, 60} in the exclusion bitset
2. MUST pass, count hits per doc (need == 2):
     quick {2,5,8,40}: mustHit[2,5,8,40]=1 ; score[..]+=
     brown {5,8,9}    : mustHit[5]=2, mustHit[8]=2, mustHit[9]=1 ; score[..]+=
3. admit docs with mustHit == 2  → {5, 8}
     doc 8 excluded by mustNot("fox") ⇒ dropped
   ⇒ candidate = {5}, pushed to the top-k heap by its summed score
```

Every MUST/SHOULD/MUST-NOT term is read in full (`materializeDocIDs` decodes all its blocks) —
cost `O(Σ df)` to decode + `O(N)` dense memory, no block-skip. WAND's impact bound is useless here
because MUST/MUST-NOT need *membership* of every doc, which an impact upper bound cannot tell you.
`~term` (ADJUST) adds a (typically negative) contribution but does **not** exclude — a doc matching
only a `~`-term still ranks, just low (MySQL parity).

### Boolean OR — routes to WAND

`AGAINST('quick brown fox' IN BOOLEAN MODE)` with no `+`/`-`/`"…"` is a pure disjunction, so
`disjunctiveTerms` returns true and it runs the §5 WAND engine — identical to the OR example there.

`word*` prefix clauses enumerate matching terms from the FST and union their posting lists; a group
`(…)` recurses. Both fall back to the dense `searchBooleanFull` path (they need a per-doc MAX over
their expansion).

### Mode comparison

| Mode | SQL | Engine | Cursor behavior | Skips docs? | Reads positions? |
|---|---|---|---|---|---|
| **OR** | `AGAINST('a b' IN BM25 MODE)` / pure-OR `IN BOOLEAN MODE` | `searchWAND` | desynchronized, impact-driven `skipTo` | **yes** (θ + block-max) | no |
| **AND / NOT** | `+a +b -c IN BOOLEAN MODE` | `searchBooleanFull` | none — full `materializeDocIDs` + dense arrays | no (full read) | no |
| **phrase** | `AGAINST('a b')` (default NL) / `"a b"` | `matchPhraseCursor` | conjunctive, converge on rarest-slot doc | yes (skip to anchor) | **yes** (offset check) |

All three top-k paths use `vectorindex.FastMaxHeap` (SoA, keyed by ord, distance = −score) — zero
per-candidate allocation, ties unspecified (the top-k *set* and score *multiset* are stable; the
order among exactly-equal scores is not).

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

## 9. INCLUDE columns: in-index prefilter & coverage

A fulltext2 index can carry **INCLUDE columns** — the actual per-document values of chosen scalar
columns, stored *inside* the segments (and the CDC tail) alongside the pk. They let a filtered
top-k query run entirely inside `fulltext2_search`, dropping the two JOINs a plain fulltext query
needs (a prefilter second-scan for the WHERE, and a base-table join-back for the SELECT):

```sql
CREATE FULLTEXT2 INDEX ftidx ON docs (body)
  WITH PARSER json
  INCLUDE (status, priority);

-- filter on an include column is evaluated INSIDE the WAND walk; status/priority are
-- served straight from the index — no base-table JOIN, no prefilter second-scan:
SELECT id, status, priority
FROM docs
WHERE status = 'active' AND priority > 3
  AND MATCH(body) AGAINST('search terms');
```

**Supported types.** Exactly what the segment pk codec already round-trips
(`CatalogHooks.SupportedIncludeColumnTypes`, `pkcodec.go`): the integer family
`int8/16/32/64`, `uint8/16/32/64` (+ `bit`), and `varchar`/`char`. The value is stored as its
**actual value, not a hash** — so both prefiltering *and* covering projection work, including
range and prefix/`LIKE 'x%'` predicates that a hash could not answer. `float`/`date`/`decimal`/
`text` are deferred (the codec has no float case; widening the set is a codec extension, not a
plan change). The set + column names are pinned into `IndexAlgoParams` at CREATE and read back on
every build / CDC / query path.

**Storage.** Include values live in the segment **docmap**, ord-aligned next to `pk` and `docLen`
(`serialize.go` `encodeDocmap`/`decodeDocmap`): a `[nCols][per-col type]` header, then per-doc,
per-col values encoded with the same codec as pks — integers dense fixed-width, strings as
`[u32 len][content]` varlena. A NULL include value carries a per-element null flag plus a
well-formed placeholder so the byte cursor stays aligned. On load the section is an mmap view
(`Segment.includeRaw` + per-varlena-col offset tables); lazy accessors decode on demand:
`Segment.includeVal(ord, colIdx)` (for the predicate evaluator) and the box-free
`Segment.appendIncludeTo(buf, colIdx, ord)` (for covering TVF output — the mirror of `appendPkTo`).

**Prefilter** (`include_predicate.go`). A WHERE predicate on include columns is pushed into the
TVF as JSON and parsed into an `includePredMembership` implementing the existing
`Membership.Contains(ord)`: it decodes `seg.includeVal(ord, colIdx)` and tests the op
(`= < > BETWEEN IN`, string comparison, and **prefix** / `LIKE 'x%'`) with 3-valued NULL logic.
It is `andAllow`'d with the liveness/docfilter memberships, so it runs **inside** the Block-Max
WAND walk *before* top-k admission — a pushed `LIMIT` therefore bounds the already-filtered set
(no over-fetch). Block-skip stays valid: `blockSum` bounds every doc in a region regardless of the
filter. A pk predicate is peeled only when the pk type is one the evaluator can compare
(`fulltext2PeelablePkColName`); otherwise it stays a residual on the base scan and the JOIN is
retained — correct, just unoptimized.

**Coverage** (`buildFulltext2SearchNodeCovered`, `search_cache.go` `SearchInto`/streaming). When
the index's INCLUDE columns cover the query's SELECT, the `fulltext2_search` TVF's output coldefs
are `[__mo_ft_doc_id, __mo_ft_score, <include cols with their real base types>]`, so the projection
reads pk/score/include directly from the TVF with **no base-table JOIN**. The pk/score outputs use
the reserved names `catalog.FullText2Search_OutCol_DocId` / `_Score` (`__mo_ft_doc_id` /
`__mo_ft_score`) precisely so an INCLUDE column named `doc_id` or `score` cannot collide with them
(the runtime classifies the output batch by name). Results carry include values box-free through
`vectorindex.ColumnBuffer` / `SearchOutput` on both the LIMIT (`SearchInto`) and no-LIMIT
(streaming `Emit`) paths.

**Value source.** Build (CREATE/REBUILD) selects the INCLUDE columns after the text columns and
passes them as trailing `fulltext2_create` args (the tokenizer stops *before* them so they are not
indexed as text); CDC (ISCP `Fulltext2SqlWriter`) resolves each include column's position from
`Name2ColIndex` and threads its value through `cdc.Insert/Upsert` into `SetDoc`, which replaces
include values in lock-step with terms so an UPSERT supersedes them.

Cost tradeoff: the docmap grows by the include values per doc (full string values bloat the
segment — the same tradeoff fulltext2 already accepts for varchar pks; prefer small scalar
include columns).

---

## 10. Build & lifecycle: sync build, CDC, MERGE/REBUILD

Index CREATE → sync build → CDC maintenance → periodic compaction. All bases/tails are chunk rows
in two hidden tables (storage + metadata), framed with the shared CDC chunk format.

- **Sync build** (`fulltext2_create` TVF): CROSS APPLY over the source table, tokenize each row
  (datalink → plain text, json → values, ngram/gojieba parser), `Add` to a streaming `Builder`,
  seal + persist tag=0 base sub-segments as they fill. `CREATE FULLTEXT2 INDEX` registers this +
  an always-async CDC task.
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

## 11. Memory model: mmap, lazy decode, bounded build

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

## 12. File map

| File | Responsibility |
|---|---|
| `doc.go` | package overview + segment-format summary |
| `index.go` | `Index` type, `resolve()`, `SearchPhrase`/`SearchBoolean`/`SearchText` entry points, liveness |
| `segment.go` | `Segment` (build-side vs loaded-side), `termPostings`, `deriveTermStats`, `BlockSize`, INCLUDE accessors (`includeVal`, `appendIncludeTo`) |
| `wand.go` | `wandIter`, Block-Max WAND (`searchWAND`), `buildWandIters`, top-k heap |
| `stream.go` | no-LIMIT streaming WAND (`streamWAND`, `StreamQuery`, `StreamBagOfWords`) |
| `query.go` | `SearchQuery` dispatch, `SearchBagOfWords`, boolean-query build, phrase slots |
| `boolean.go` | boolean operator tree (`clause`, `clauseKind`), boolean evaluation |
| `search.go` | phrase evaluation, per-segment scoring glue, `boundedTopK` |
| `membership.go` | WHERE-prefilter + liveness `Membership` adapters |
| `include_predicate.go` | in-index INCLUDE-column predicate → `Membership` (`= < > BETWEEN IN`, prefix/`LIKE`, 3-valued NULL) |
| `pkcodec.go` | typed pk / INCLUDE value encode/decode (integer family dense fixed-width; varchar/char varlena `[len][value]`) |
| `termdict.go` | vellum FST term dict: build, load, exact `get`, `prefixIter`, `forEachTerm` |
| `build.go` | `Builder` (Add/Finish/FinishSegments), `TokenizedDoc`, `ReachedSegmentCap`, seal caps |
| `tailbuild.go` | streaming CDC `TailBuilder` (spill-as-you-fill) |
| `cdc.go` | CDC event decode (`Cdc`), pk typing |
| `deletes.go` | delete-record framing / tombstones |
| `frames.go` | CDC chunk framing (`FrameSegment`, `UnframeTail`) |
| `sink.go` | ISCP sink adapter glue |
| `compact.go` | `CompactSegments` (MERGE): reconstruct live docs → fresh bounded base |
| `serialize.go` | tar-archive segment encode/decode (docmap incl. INCLUDE section / termdict/postings/blocks/positions) |
| `storage.go` | chunk-row persistence, `LoadAllBases`/tail, `__fulltext2` spill, load budgets, `TableConfig` |
| `search_cache.go` | `VectorIndexCache` integration (per-CN cached loaded index) |
| `mmap_unix.go` / `mmap_other.go` | read-only mmap + munmap (platform) |

For the plugin wiring (hooks, grammar, CDC registration, idxcron), see `pkg/fulltext2/plugin/…`.
