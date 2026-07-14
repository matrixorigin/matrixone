# FULLTEXT v2 — a WAND-based positional fulltext engine (`VERSION=2`)

## Goal

Replace classic FULLTEXT's *generated-SQL + Go-`Eval`* execution with a purpose-built,
cached, early-terminating **WAND** engine — selected by a **`VERSION=2` index parameter** on
the existing FULLTEXT index (**not** a new keyword/index type). `VERSION` defaults to `1`
(classic SQL engine), so every existing index is unchanged; `VERSION=2` is opt-in and the v1
code path stays the **baseline / correctness oracle**. The v2 engine lives in a **completely
separate package** (`pkg/fulltext2/`); the fulltext plugin is a **thin version-router** that
delegates each hook to v1 (called verbatim) or v2. Supports **all existing fulltext behavior**:

- **Parsers:** `gojieba` **and** `ngram`/default (dictionary-free), plus json/json_value.
- **Modes:** NATURAL LANGUAGE (= **exact contiguous phrase** in MO, *not* OR bag-of-words),
  BOOLEAN, phrase.
- **Boolean operators:** `+` `-` `>` `<` `~` `( )` `"…"` **and `*` prefix (complete, not skipped).**
- **Scoring:** **both TF-IDF and BM25**, selected by `ft_relevancy_algorithm` (existing session var).

DDL: `CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER gojieba VERSION=2` (a numeric
`index_option`, default 1). Query surface unchanged: `MATCH(cols) AGAINST('q' [mode])`. Same
logical index, faster engine — **migrate in place** via `ALTER TABLE t ALTER REINDEX ft …
VERSION=2` (rebuild into the WAND format; reversible to `VERSION=1`).

---

## 1. Architecture principle — two separate engines on one shared framework

Not a shared WAND "core" (bm25 and fulltext backbones genuinely diverge), and not an
up-front component framework (the pieces are correlated bundles, not orthogonal knobs — an
interface-with-2-impls smell, and per-posting interface dispatch is costly in Go).

| | **bm25** (unchanged) | **fulltext v2** (`VERSION=2`) |
|---|---|---|
| term resolution | jieba dict-id + **unordered** overflow map | **sorted FST term dict** (mandatory for prefix) |
| posting payload | df/impact only | **+ positions** (phrase, ngram reassembly) |
| query model | whole-sentence → bag → disjunctive WAND | parser → **operator tree** |
| scoring | BM25 | **TF-IDF and BM25** |

Three of four rows differ, and the term dictionary is the *backbone* — so a shared core would
either burden bm25 with FST/positions it never uses (regression risk on shipped code) or
become a leaky lowest-common-denominator. **Share the framework, keep the engines separate.**

**Reuse (already shared framework):** index-plugin hooks, ISCP CDC consumer + PostDml
maintenance, `VectorIndexCache`, idxcron MERGE/REBUILD, payload-agnostic CDC chunk framing
(`FrameCdcChunk`), the unified plan routing / `(doc_id, score)` contract, the tokenizer TVF.

**Build (new):** `pkg/fulltext2/` — FST term dict, positional postings, operator-tree
evaluator, TF-IDF/BM25 scorer, build sink, search TVF — a **completely separate package**.

**Version-router (thin), not a new plugin:** the existing fulltext plugin stays the single
registered `fulltext` algo; at each hook (schema/hidden-tables, build, search, maintenance,
idxcron) it reads `VERSION` from algo_params and delegates in **one line** — `if version>=2 {
fulltext2.X() } else { <existing v1 code, called verbatim> }`. v1 logic is never refactored,
so a v2 bug is confined to `pkg/fulltext2/` + the router branch and cannot reach v1.

**Share only leaf utilities** (extract on seeing duplication, rule of three): top-k heap,
block-max posting-block reader, varint helpers. Not a "core engine."

---

## 2. DDL surface — a `VERSION` index parameter, not a new keyword

Stays the **classic FULLTEXT index type** (`algo="fulltext"`); a numeric `VERSION`
`index_option` selects the engine. No new reserved keyword, no new index category/type, no
coexistence ambiguity (one index, one version).

- Grammar: add `VERSION` as a numeric `index_option` — the rule `VERSION equal_opt INTEGRAL`,
  mirroring the existing numeric options (`MAX_INDEX_CAPACITY`, `SECOND`, …). `VERSION=2` is a
  plain integer; **default 1** when absent.
- Plumb it like other options: parse into `tree.IndexOption` → the fulltext plugin's
  `ParamsFromTree` writes `version` into flat `algo_params` (only when explicitly set) → read
  back at build/search/maintenance → rendered by `IndexParamsToStringList` for SHOW CREATE.
- **No changes to the FULLTEXT keyword, `INDEX_TYPE_FULLTEXT`, `MOIndexFullTextAlgo`, or MATCH
  routing** — a v2 index is still a `fulltext` index everywhere; only the engine behind the
  hooks differs.

DDL & migration:
```sql
CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER gojieba VERSION=2   -- opt-in v2
ALTER TABLE t ALTER REINDEX ft ... VERSION=2                        -- migrate v1 -> v2 (rebuild)
ALTER TABLE t ALTER REINDEX ft ... VERSION=1                        -- roll back (rebuild)
```

---

## 3. Routing — the thin version-router (v1 verbatim, v2 delegated)

Because `algo` stays `fulltext`, the existing resolution (`IsFullTextIndexAlgo`, the unified
`applyJoinFullTextIndices` loop, the `(doc_id, score)` contract) is **unchanged**. The only new
logic is a **one-line version branch at each plugin hook**, reading `version` from algo_params:

| Hook | v1 (default, verbatim) | v2 (`version>=2`) |
|---|---|---|
| schema / hidden tables | postings table `(word,doc_id,pos)` | WAND segment store (storage+metadata) |
| build (compile) | `INSERT … CROSS APPLY fulltext_index_tokenize` | positional segment builder |
| search (plan → TVF) | `fulltext_index_scan` | `fulltext2_index_scan` (WAND) |
| maintenance / CDC | PostDml / ISCP row writes | positional-frame sink |
| idxcron | (n/a today) | MERGE / REBUILD |

- Router lives in the existing fulltext plugin; each branch delegates wholesale to
  `pkg/fulltext2/` (v2) or calls the **existing v1 code verbatim** — v1 is never refactored, so
  its behavior is byte-for-byte preserved (the baseline/oracle guarantee).
- Keep the branch trivial (`if version>=2 { … } else { … }`) and auditable — that thinness is
  what preserves the isolation a separate plugin would have given.
- Bonus: WAND unlocks proper **multi-term intersection top-k pushdown** (the planner's fulltext
  pushdown is single-stream-only today).

---

## 4. Segment format — the FST term dictionary is the backbone

Prefix (`word*`) forces a **sorted term dictionary of the index's actual terms** (mirrors
today's postings table `CLUSTER BY word` range scan). This single structure delivers term
lookup **and** prefix enumeration **and** makes gojieba/ngram uniform. bm25's dict-id +
*unordered* overflow map cannot do this — so the FST term dict is a genuinely new structure.

```
fulltext v2 segment  (tag=0 base sub  /  tag=1 CDC tail frame):
  ├─ FST term dict:  term(string) → { df, posting-offset }   ← sorted; dictionary-FREE (holds whatever was indexed)
  ├─ postings:       per term, doc-sorted, block-max (per-block max-TF) + POSITIONS list
  ├─ docmap:         ord → { pk, docLen };  N = doc count
  └─ avgDocLen:      computed at LOAD (Σ docLen / N across all loaded segments)
```

- **Dictionary-free by construction:** the FST holds exactly the indexed terms — gojieba *words*
  or ngram *bigrams* — with no jieba-dict dependency in the backbone. (A jieba-dict-id fast-path
  is optional, not required.) FST keeps large CJK-bigram vocabularies compact.
- **Per-doc length is stored** (docmap) and **avgDocLen** aggregated at load — needed by BM25;
  TF-IDF ignores it. `docLen` matches today's `__DocLen` semantics for exact parity.
- **block-max stores raw max-TF (scorer-agnostic)**, NOT a baked impact, so one index serves
  both TF-IDF and BM25; the active scorer derives its max-impact bound at query time.

---

## 5. Parser handling — gojieba **and** ngram, uniformly

Query front-end **unchanged** (`ParsePatternInBooleanMode`/`NLMode` → operator tree → per-term
tokenization by the index's parser). Only *execution* moves to WAND.

- **gojieba:** a term → one word token → one FST lookup → posting iterator; a multi-token term →
  positional phrase.
- **ngram (dict-free):** a term → several ngrams → each an FST lookup → **positional adjacency
  (reassembly)** over the *same* positional engine used for phrases. ngram is not a special
  mechanism; the FST already stores the ngram vocab (prefix demands it anyway), so ngram adds no
  structure beyond what prefix requires.

---

## 6. Query evaluator — operator tree → WAND iterators

| Pattern op | Iterator | Notes |
|---|---|---|
| **NL (whole query)** | exact-phrase conjunctive | skip-intersect all terms → positional anchor verify → top-k. **Primary path.** |
| Boolean bare terms | disjunctive **OR-WAND** top-k | over space-separated terms; each child a TEXT term *or* a positional phrase sub-iterator |
| `+` / JOIN | conjunctive skip | |
| `-` MINUS | must-not filter | |
| `>` `<` `~` | impact-scaled (×1.1/×0.9/×−1.0) | **`~` negative ⇒ ≤0 in the WAND upper bound** (safe; only lowers scores) |
| `( )` GROUP | max sub-scorer | |
| `"…"` phrase | two-phase positional (TEXT-only) | same engine as NL |
| **`*` prefix (boolean)** | **FST-enumerate `[p, p⁺)` → disjunctive slot**, combined max-impact = max over expanded terms | min-prefix-length guard; never inside a phrase |

- **Cached** in-memory segment (`VectorIndexCache`); **early termination** on top-k — the source
  of the win, independent of parser/dict.
- **bm25 vs fulltext query contract stays distinct:** bm25 = raw sentence → gojieba → bag; fulltext
  v2 boolean = parsed operator tree with positional phrase terms.

### Scoring — both TF-IDF and BM25 (per query, from `ft_relevancy_algorithm`)

- **TF-IDF:** `w·tf·idf²`, `idf=log₁₀(N/df)` — needs `tf, df, N`.
- **BM25:** `w·idf²·tfSq`, `tfSq = tf·(K1+1)/(tf + K1·(1−B + B·docLen/avgDocLen))`,
  `K1=1.5, B=0.75` — additionally needs per-doc `docLen` + `avgDocLen`.
- Preserve MO quirks: `idf²` (squared), tf capped at 255, BM25→TF-IDF downgrade when
  `avgDocLen=0`.
- `Scorer` strategy (`TfIdf` | `BM25`) chosen **once per query**; **monomorphize** the inner loop
  (generic traversal over the concrete `Scorer`, or a top-level `if algo==BM25` selecting a
  concrete inner loop) so there is **no per-posting virtual dispatch**.
- **WAND bound must use the active scorer's max-impact:** TF-IDF's tf-term is linear (≈ `w·255·idf²`);
  BM25's `tfSq` **saturates** (≈ `w·idf²·(K1+1)`, modulated by docLen). Compute the bound from
  `(max-TF, df, docLen)` under the active scorer — do not bake a scorer-specific impact into the
  postings, or algo-switching and pruning break.

---

## 7. Maintenance

- **Sync build:** tokenizer output → positional segment builder → tag=0 base (replaces
  `INSERT INTO idxtbl`).
- **Async / incremental:** reuse ISCP CDC consumer + PostDml hooks; **only the sink changes** —
  write tag=1 positional frames instead of table rows.
- **Compaction:** reuse bm25's idxcron MERGE/REBUILD (validated: fold tag=1→tag=0, rebuild on
  high dead-doc %; MO_IDXCRON_* knobs).

---

## 8. Phasing

- **P0 — `VERSION` param + router + engine skeleton.**
  - ✅ **`VERSION` index-option → algo_params** — *done, commit `13ff1e48d`.* Grammar `VERSION
    equal_opt INTEGRAL` (non-reserved keyword, goyacc regen 0 conflicts) + `tree.IndexOption.Version`
    + Format/merge; `catalog.IndexAlgoParamVersion` + SHOW CREATE rendering; `buildFullTextParams`
    records `version` only when `>=2`. Parser round-trip + plumbing unit tests green. Classic
    fulltext (unset/1) byte-for-byte unchanged.
  - ◑ **thin version-router** at each fulltext-plugin hook (schema/build/search/maintenance) —
    `if version>=2 { fulltext2.X() } else { <v1 verbatim> }`.
    - ✅ **maintenance-model fork (v2 ⇒ always-async)** — v2 is CDC-only; classic v1 is
      synchronous (opt-in async). NOT expressed by faking the `async` param and NOT by a
      param-aware SyncDescriptor (the static descriptor is a per-algo constant — making it take
      params forces `""` at every param-less caller). Instead: a canonical async resolver in the
      plugin layer — `indexplugin.IsAsync(algo, params)` = `AlwaysAsync` (algo identity via the
      static `SyncDescriptor().AlwaysAsync` — hnsw/bm25/cagra/ivfpq — **OR** engine version≥2 —
      fulltext v2) **OR** the user `async` param (`catalog.IndexParamAsync`). `catalog` keeps only
      the pure-param primitives (`IndexAlgoParamVersionOf`, `IndexParamAsync`, née `IsIndexAsync`),
      since a complete `AlwaysAsync` needs the plugin registry. Every fulltext async-decision site
      (compile CREATE, the fulltext DML maintenance builders, CDC-validity, clone-skip) now routes
      through `IsAsync`, collapsing the old hand-written `d.AlwaysAsync || IsIndexAsync` combines;
      vector/bm25 build-timing sites keep the raw-param call. Unit-tested; v1 byte-for-byte
      unchanged (`IsAsync` ≡ the param there). *(only fulltext behavior changes, and only for v2)*
    - ☐ engine-swap hooks (schema → WAND segment store, build → positional builder, search →
      `fulltext2_index_scan`) — per-hook `version>=2` branches, once `pkg/fulltext2/` exists.
  - ✅ **`pkg/fulltext2/` package — segment format** — *done (P0.3a/b).* `Segment` (positional
    analogue of bm25's `WandModel`): docmap (`pks`/`docLen`) + `AvgDocLen` + `Recency`;
    `termPostings` with per-doc positions and raw `maxTf`/block-max (scorer-agnostic).
    Dictionary-free term dict keyed by term string; on disk a **vellum FST** (`term → ordinal`,
    byte-oriented so CJK/Unicode needs no special handling) — exact `get` + `word*` prefix range.
    `Serialize`/`Deserialize` on the same multi-member tar shape as bm25 (docmap + FST + positional
    postings), docmap + PK codec byte-identical to bm25's. Round-trip + FST + CJK unit tests green.
    Adds `blevesearch/vellum` dep. (Storage/CDC-chunk streaming reuse — bm25's `storage.go` shape —
    lands with P4 maintenance.)
- **P1 — NL exact-phrase.** Positional two-phase conjunctive + **TF-IDF and BM25** top-k, cached,
  early-terminating. gojieba + ngram. **A/B parity** vs `VERSION=1` on the same data (below).
- **P2 — Boolean operators.** ◑ *core done* — `+` MUST / `-` MUST-NOT / bare OR, `"…"` phrase,
  and `word*` prefix (FST-expanded, max-impact), over the positional segment; TF-IDF/BM25 scoring;
  self-contained parser + evaluator, runnable on fulltext text cases. ☐ remaining: `> < ~` weight
  operators, nested `( )` groups, OR-WAND block-max early termination, and translating the reused
  `fulltext.ParsePattern` front-end into these clauses at plugin integration.
- **P3 — Prefix `*`.** FST prefix-enumeration → disjunctive slot + min-prefix guard.
- **P4 — Maintenance + migration + BVT.** CDC sink, sync build, idxcron MERGE/REBUILD; multi-term
  pushdown; `ALTER … REINDEX … VERSION=2/1` migration; BVT parity. (v1 stays the default baseline;
  v2 is opt-in per index.)

---

## 9. Validation

- **A/B parity vs `VERSION=1`** — the payoff of keeping v1 untouched: load the same corpus into
  two tables, index one `VERSION=1` and one `VERSION=2`, run identical `MATCH` queries, diff
  doc-set + score ordering, per mode (NL/boolean/phrase/prefix) × parser (gojieba/ngram) ×
  `ft_relevancy_algorithm` (TF-IDF/BM25). v1 is a **live oracle** — no hand-maintained golden files.
- BVT parity; the segment-count / latency benchmark harness (confirm the WAND ~5× materializes;
  v1 vs v2 on one dataset is exactly the two-index shape it already runs);
  adversarial unhappy-path pass on the new engine.

---

## 10. Risks

1. **Scoring-exactness regression** → the A/B parity harness (P1), both algos, is the control.
2. **ngram posting fan-out / vocab size** → FST compression; benchmark before recommending;
   `VERSION=1` remains the fallback.
3. **Prefix expansion width** (short prefixes) → min-prefix-length (inherent wildcard cost).
4. **`~` monotonicity** → ≤0 in the WAND bound.
5. **Version-router leaking into v1** → keep the router branch trivial and call v1 verbatim; a
   fat router is the one way v2 could destabilize v1 (the isolation a separate plugin would give).
6. **Go hot-loop dispatch** → keep interfaces at per-query/per-segment granularity; monomorphize
   the inner traversal + scorer.
