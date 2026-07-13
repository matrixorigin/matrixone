# FULLTEXT2 — a WAND-based positional fulltext index

## Goal

Replace classic FULLTEXT's *generated-SQL + Go-`Eval`* execution with a purpose-built,
cached, early-terminating **WAND** engine — delivered as a **new index type `FULLTEXT2`**
so the existing FULLTEXT index is never modified and stays as the **baseline / correctness
oracle**. FULLTEXT2 supports **all existing fulltext behavior**:

- **Parsers:** `gojieba` **and** `ngram`/default (dictionary-free), plus json/json_value.
- **Modes:** NATURAL LANGUAGE (= **exact contiguous phrase** in MO, *not* OR bag-of-words),
  BOOLEAN, phrase.
- **Boolean operators:** `+` `-` `>` `<` `~` `( )` `"…"` **and `*` prefix (complete, not skipped).**
- **Scoring:** **both TF-IDF and BM25**, selected by `ft_relevancy_algorithm` (existing session var).

Query surface is unchanged: `MATCH(cols) AGAINST('q' [mode])`. FULLTEXT2 is a drop-in
faster *engine* for the same query language, opt-in at DDL, no migration.

---

## 1. Architecture principle — two separate engines on one shared framework

Not a shared WAND "core" (bm25 and fulltext backbones genuinely diverge), and not an
up-front component framework (the pieces are correlated bundles, not orthogonal knobs — an
interface-with-2-impls smell, and per-posting interface dispatch is costly in Go).

| | **bm25** (unchanged) | **FULLTEXT2** (new) |
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

**Build (new):** `pkg/fulltext2/plugin/` + `pkg/fulltext2/wand/` — FST term dict, positional
postings, operator-tree evaluator, TF-IDF/BM25 scorer, build sink, search TVF.

**Share only leaf utilities** (extract on seeing duplication, rule of three): top-k heap,
block-max posting-block reader, varint helpers. Not a "core engine."

---

## 2. FULLTEXT2 as a grammar sibling of FULLTEXT (`CREATE FULLTEXT2 INDEX …`)

Follows the **FULLTEXT SQL parser path**, not the generic `USING <algo>` seam — because it
*is* a fulltext index. Distinguished only by keyword → algo; everything downstream dispatches
by algo through the plugin framework.

FULLTEXT today: token `FULLTEXT` (mysql_sql.y:404) → productions at y:8262
(`INDEX_CATEGORY_FULLTEXT`), y:10300 (`FULLTEXT key_or_index_opt name '(' cols ')'
index_option_list`), y:10313 (`USING` variant) → `INDEX_TYPE_FULLTEXT` (tree/create.go:2062,
`.String()="fulltext"`) → `MOIndexFullTextAlgo` (secondary_index_utils.go:37).

FULLTEXT2 =
1. new token `FULLTEXT2`;
2. new enum `INDEX_TYPE_FULLTEXT2`, `.String()="fulltext2"`;
3. mirror the FULLTEXT productions — `CREATE FULLTEXT2 INDEX …`, inline-table
   `FULLTEXT2 [INDEX|KEY] (cols)`, `ALTER TABLE ADD FULLTEXT2` — each emitting the **same tree
   node shape** as FULLTEXT tagged `INDEX_TYPE_FULLTEXT2`; goyacc regen (must stay 0 conflicts);
4. catalog `MOIndexFullText2Algo = tree.INDEX_TYPE_FULLTEXT2`.

Reusing the identical production shape means FULLTEXT2 **inherits every fulltext DDL behavior
for free** (`WITH PARSER {gojieba|ngram}`, column/varchar validation, index_option handling,
inline-table + ALTER forms). Only the keyword and resulting algo differ.

**Query side unchanged:** `MATCH…AGAINST` and the mode productions (y:10939+) are untouched.

---

## 3. Routing — plugin-framework driven, no per-algo behavior switches

- Build/compile: `build_ddl` → `buildFullTextIndexTable` already delegates to
  `indexplugin.Get(algo)`. algo=`fulltext2` lands on the fulltext2 plugin → WAND segment store
  instead of the postings table. No `case fulltext2:` behavior switches (keeps the mo-self-review
  §8 "no new per-algo dispatch" guard clean).
- Two legitimately-shared seams must learn "fulltext2 is fulltext-family":
  1. the **family-membership** check used to resolve `MATCH` to an index (today likely
     `IsFullTextIndexAlgo`) → accept `fulltext2` too (membership, not behavior switching);
  2. the **unified match-rewrite loop** (`applyJoinFullTextIndices`, which already branches
     fulltext vs bm25) → route `fulltext2` to the `fulltext2_index_scan` TVF, preferably via the
     plugin's search hook rather than a hardcoded algo compare.
- **Coexistence policy:** reject creating both FULLTEXT and FULLTEXT2 on the same column set
  (keeps `MATCH` routing unambiguous).
- Bonus: WAND unlocks proper **multi-term intersection top-k pushdown** (the planner's fulltext
  pushdown is single-stream-only today).

---

## 4. Segment format — the FST term dictionary is the backbone

Prefix (`word*`) forces a **sorted term dictionary of the index's actual terms** (mirrors
today's postings table `CLUSTER BY word` range scan). This single structure delivers term
lookup **and** prefix enumeration **and** makes gojieba/ngram uniform. bm25's dict-id +
*unordered* overflow map cannot do this — so the FST term dict is a genuinely new structure.

```
FULLTEXT2 segment  (tag=0 base sub  /  tag=1 CDC tail frame):
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
- **bm25 vs ngram query contract stays distinct:** bm25 = raw sentence → gojieba → bag; FULLTEXT2
  boolean = parsed operator tree with positional phrase terms.

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

- **P0 — Register the type + engine skeleton.** Grammar (`FULLTEXT2` token + `INDEX_TYPE_FULLTEXT2`
  enum + mirrored productions + goyacc regen, 0 conflicts); catalog `MOIndexFullText2Algo` +
  family-resolver recognizes both; `pkg/fulltext2/plugin/` (all hooks + `var _ AlgoPlugin` + blank
  imports) → hidden WAND segment tables; `pkg/fulltext2/wand/` segment format (FST term dict +
  positional postings + per-doc length + docmap + serialize/load on the shared framing). Existing
  FULLTEXT untouched.
- **P1 — NL exact-phrase.** Positional two-phase conjunctive + **TF-IDF and BM25** top-k, cached,
  early-terminating. gojieba + ngram. **A/B golden parity** vs FULLTEXT (below).
- **P2 — Boolean operators.** OR-WAND + `+ - > < ~ ( )` + `"…"` phrase.
- **P3 — Prefix `*`.** FST prefix-enumeration → disjunctive slot + min-prefix guard.
- **P4 — Maintenance + BVT.** CDC sink, sync build, idxcron MERGE/REBUILD; multi-term pushdown;
  BVT parity. (No deprecation/migration phase — FULLTEXT stays the baseline; FULLTEXT2 is opt-in.)

---

## 9. Validation

- **A/B golden parity** — the big win of the new-index-type approach: build FULLTEXT and
  FULLTEXT2 on the *same data*, run identical `MATCH` queries, diff doc-set + score ordering, per
  mode (NL/boolean/phrase/prefix) × parser (gojieba/ngram) × `ft_relevancy_algorithm`
  (TF-IDF/BM25). FULLTEXT is a **live oracle** — no hand-maintained golden files.
- BVT parity; the segment-count / latency benchmark harness (confirm the WAND ~5× materializes;
  FULLTEXT vs FULLTEXT2 is exactly the two-index-on-one-dataset shape it already does);
  adversarial unhappy-path pass on the new engine.

---

## 10. Risks

1. **Scoring-exactness regression** → the A/B parity harness (P1), both algos, is the control.
2. **ngram posting fan-out / vocab size** → FST compression; benchmark before recommending;
   FULLTEXT remains the fallback.
3. **Prefix expansion width** (short prefixes) → min-prefix-length (inherent wildcard cost).
4. **`~` monotonicity** → ≤0 in the WAND bound.
5. **Coexistence ambiguity** → reject two fulltext-family indexes on one column set.
6. **Go hot-loop dispatch** → keep interfaces at per-query/per-segment granularity; monomorphize
   the inner traversal + scorer.
