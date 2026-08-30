# A tuple-encoded JSON word breaker (issue #27704)

Status: **implemented end to end** — tuple encoder on both build paths, term
ranges, probe dispatch, and the optimizer rule wired into `applyIndices`
(`addJSONFulltextProbes`). Covered by BVT `fulltext2_json_probe.sql` and the
rewritten json section of `fulltext2_parser.sql`. §7 lists what remains.

## 1. Where we are today

Three facts from the current tree drive the whole design.

**JSON documents are indexed by leaf value only; keys are thrown away.**
`bytejson.TokenizeValue(includeKey bool)` (`pkg/container/bytejson/fttokenizer.go`)
already takes an `includeKey` flag, and every caller passes `false` —
`pkg/fulltext2/query.go:150,201` and `pkg/sql/colexec/table_function/fulltext_tokenize.go:219,259`.
So `{"a":{"b":"XXX","c":"YYY"}}` indexes as the tokens `XXX`, `YYY`.

Note this differs from the issue's premise. The issue says we produce `b=XXX`.
We do not — we produce `XXX`. Nothing today can distinguish `{"b":"XXX"}` from
`{"c":"XXX"}`. Even with `includeKey=true` the tokenizer emits the key and the
value as **two adjacent tokens**, not one compound token, so the key/value
association would only be recoverable through a positional phrase query — which
a `position_free` index cannot do at all. Either way, the association we need
does not exist in the index.

**A term is a `string` and is capped at 127 bytes.** `FullTextEntry.Word` is a
Go `string` (`fulltext_tokenize.go:38`), `bytejson.MAX_TOKEN_SIZE = 127`, and
classic fulltext interpolates terms into SQL text (`fulltext.go:43`,
`where word = '%s'`). Query patterns also arrive as SQL string literals through
`MATCH(col) AGAINST('...')` → `fulltext_match(pattern, mode, cols...)`
(`base_binder.go:3225`). Those two text-carrying surfaces are what this design
routes around rather than encodes for: the v1 engine is excluded and the probe
becomes a function argument (§3.2, §5.1). The 127-byte cap still applies.

**Terms are stored sorted, and prefix scans already work.**
`Segment.PrefixRange` (`pkg/fulltext2/segment.go:755`) binary-searches a sorted
`[]string`. A lexicographic *range* scan is a few lines away from what is
already there. This is what makes order-preserving encoding worth doing.

Two more constraints worth naming up front:

- **Two build paths must agree exactly.** The CREATE path
  (`fulltext_tokenize.go`) and the CDC path (`CdcTokenizer` + `Fulltext2SqlWriter.rowText`)
  tokenize independently. The comment at `query.go:264` records a past bug where
  they disagreed and CDC-inserted JSON rows became silently unsearchable. Any
  new breaker must be implemented once and called from both.
- **Config already has a home.** `IndexAlgoParams` is a JSON blob
  (`parser`, `position_free`, …) in `pkg/catalog/secondary_index_utils.go`,
  surfaced by `SHOW CREATE` (`build_show_util.go:322`).

## 2. Goals

1. **Fix the existing JSON word breaker** rather than adding a parallel one.
   Discarding keys is the defect; a key/value association becomes a single
   searchable term.
2. Encode terms with the **order-preserving tuple encoding** (`types.Packer`)
   so numeric comparisons become lexicographic range scans.
3. One option, persisted on the index: **`includeKeys`** (reusing the existing
   dormant `includeKey` hook).
4. Let the optimizer turn `json_extract_*(col, '$.path') <op> const` into an
   index probe, keeping the original predicate for correctness.

Non-goals for this design: changing classic (v1) fulltext, or full JSONPath
wildcard support.

## 3. Term encoding

### 3.1 Tuple shape

`types.Packer` (FDB-derived, `pkg/container/types/packer.go`) is byte-order
preserving: `encodeFloat64` big-endians and sign-adjusts the bits, ints are
type-code prefixed, strings are `0x00`-terminated and escaped.

For a leaf at path `a.b.….y.z` with value `V`, encode the tuple:

```
( z , V )
```

The element order is the useful part, and it is the issue's proposal:

| Probe | Encoding |
|---|---|
| `$..z = V` (any path ending in `z`) | exact prefix `(z, V)` |
| `$.a.b.z = V` (exact path) | exact full tuple |
| `$..z > 3.14` | prefix `(z)` + **range** on the value element |
| `$.a.*.z = V` | prefix `(z, V)` + residual path filter |

The term deliberately does NOT carry the ancestor path. A probe is therefore
path-agnostic — it matches that key/value wherever it appears — and a predicate
on a specific path gets a superset that the retained predicate narrows. An
earlier draft had an `includeFullPath` mode appending the ancestor path; it was
removed because the benefit was unclear and it added a second term shape that
every probe had to agree with (an index built one way and probed the other way
silently returns nothing).

**The value is encoded under its JSON type**, not stringified: a JSON number
becomes `EncodeFloat64`, a string becomes `EncodeStringType`. This is the whole
point — it is what makes `> 3.14159` a range scan. It also has a sharp
consequence for the rewrite (§5.2).

**Every number is float64, whatever width `bytejson` parsed.** `{"b":3}` is
`TpCodeInt64` and `{"b":3.0}` is `TpCodeFloat64`, but JSON has one number type:
they are the same value and must produce the same term, and a probe cannot know
which internal width a document happened to use. Encoding ints as ints leaves
`{"b":3}` unreachable from every numeric probe — a dropped row. (Implementation
found this: the first version encoded per width, and the int/float test failed.)

Integers past 2^53 lose precision in the term. That is safe because the *same*
normalization runs on both sides: two ints colliding as doubles share one term
and still match each other. The loss produces false positives, never a dropped
row.

### 3.2 Raw bytes; make the term path binary-clean

**Decided: no hex.** The term is the raw packed tuple, and the whole term path
is made binary-safe instead of encoding around it. Full 127 bytes stay usable
and there is no 2× size tax.

This is a real work item, not a declaration. Packed tuples contain `0x00`,
arbitrary type-code bytes, and bytes that are syntactically meaningful to the
BOOLEAN-mode pattern parser (`+ - * " ( )`). Every place a term is carried as
text has to be audited:

| Surface | Today | Required |
|---|---|---|
| `FullTextEntry.Word` | `string` | fine — Go strings hold arbitrary bytes |
| `Segment.sortedTerms` | `[]string` + `sort.SearchStrings` | fine — byte-lexicographic already |
| tokenize TVF output vector | `varchar` | must be binary-clean (`varbinary`) |
| CDC `rowText` carrier | text, `'\n'`-joined for `json_value` | needs a length-prefixed carrier; a byte separator is unsafe |
| classic (v1) fulltext | `where word = '%s'` SQL interpolation | **cannot** be made safe — see below |
| query pattern | SQL literal parsed as a BOOLEAN pattern | replaced by a function argument (§5.1) |

The SQL-interpolating v1 engine is the one surface that cannot carry these
bytes. The tuple-encoded `json` parser is therefore **fulltext2-only**, and DDL
must reject it on a v1 index rather than silently producing a corrupt one.

### 3.3 Truncation must be symmetric

When the encoded tuple exceeds the cap, **both** the build side and the query
side truncate to the same length. Then `T == Q ⟹ T[:n] == Q[:n]`, so the probe
stays a *necessary* condition and degrades to a prefix match — extra rows, no
lost rows, and the retained original predicate removes the extras. Truncating
on only one side would silently lose rows, which is the failure mode to avoid.

## 4. Configuration: fixing the existing breaker

No new parser. The existing `json` parser is **fixed in place**, governed by two
options on the index.

### 4.1 `includeKeys` — default **true**

This reuses the dormant `includeKey` flag already threaded through
`bytejson.TokenizeValue` (§1), repurposed from "emit the key as its own token"
to "emit the `(tag, value)` tuple".

| Value | Behaviour |
|---|---|
| `true` (default) | Index the `(tag, value)` tuple term for every leaf. |
| `false` | Do **not** index the tuple. Leaf values only — today's behaviour. |

Making `true` the default means a plain `WITH PARSER json` index becomes
structure-aware by default, which is the point of the fix.

### 4.3 Surface

**Implemented today** — the defaults, with no way to change them from SQL:

```sql
CREATE FULLTEXT2 INDEX idx ON t(j) WITH PARSER json;   -- keys on, leaf-only
```

**Proposed, NOT yet implemented.** The grammar has no `INCLUDE_KEYS` index
option (only `POSITION_FREE` exists), so the param below can currently only be
set by writing `IndexAlgoParams` directly. It is read end to end — build,
incremental build, and the optimizer probe — so wiring the DDL is the only
missing piece:

```sql
CREATE FULLTEXT2 INDEX idx ON t(j) WITH PARSER json, INCLUDE_KEYS = FALSE;
```

Both land in `IndexAlgoParams` beside `parser` / `position_free`
(`IndexAlgoParamJSONIncludeKeys`, `IndexAlgoParamJSONIncludeFullPath` in
`secondary_index_utils.go`) and must round-trip through `SHOW CREATE`
(`build_show_util.go:322`). They are **persisted per index**, because the
optimizer has to know which term shape an index holds before it can synthesize
a probe (§5.2) — an index built one way and queried the other way silently
returns nothing.

`position_free` should default to **true** whenever `includeKeys = true`:
positions are meaningless for tuple terms and it halves the footprint.

### 4.4 Existing `json` indexes

Settled in review: **no table currently uses the old value-only terms**, so
there is no migration to design. The `json` parser simply becomes the tuple
breaker, and `includeKeys = false` remains available for a value-only index.

`json_value` is untouched and keeps its own whole-value tokenization.

One implementation consequence worth recording: the option is carried in
`TableConfig` as `JSONNoKeys` — **inverted**. Keys are on by default, so the
zero value of a config must mean "keys on"; a plain `IncludeKeys bool` would
make any construction site that forgot the field silently build an index with
no tuple terms.

## 5. The optimizer rewrite

### 5.1 The contract and the probe surface

Rewrite

```sql
WHERE json_extract_string(j,'$.a.b') = 'XXX'
```

into

```sql
WHERE json_extract_string(j,'$.a.b') = 'XXX'   -- retained, decides correctness
  AND __mo_ft_json_probe(j, <lo>, <hi>, true, true)   -- index prefilter
```

The single invariant:

> **The injected conjunct must be implied by the original predicate.**

If it is implied, the rewrite can only remove rows the original would have
removed anyway, and the retained original deletes any extra rows the index
lets through. False positives are free; a false negative is a wrong answer.

**The probe is a function argument, not a pattern string.** Raw tuple bytes
cannot go through `AGAINST('...')`: they would be parsed as BOOLEAN-mode
syntax. So the probe takes the term as a `varbinary` argument that is used
verbatim as a term-dictionary key — no pattern parsing at any point.

**The probe is a union of exact terms and INCLUSIVE ranges.** A document
qualifies if it holds any listed term or any term inside any listed range:

| Predicate | probe |
|---|---|
| `= V` | term `(z,V)` — plus the other encoding when V is numeric-looking |
| `> V`, `>= V` | range `[ (z,V) , (z, +∞ of V's type) ]` |
| `< V`, `<= V` | range `[ (z, −∞ of V's type) , (z,V) ]` |

Both ends are **always inclusive**, so `>` and `>=` produce the same range.
Including the boundary term only adds documents whose value equals the bound,
and the retained predicate removes them — the probe owes a superset, so
carrying exclusivity would buy one term of precision at the cost of threading a
flag through the whole scan path.

The union is what lets one comparison probe two encodings at once, which
`json_extract_string` inequalities need (§6).

This does mean the probe is a **new internal function** rather than a reuse of
`fulltext_match`, because `fulltext_match` takes a parsed varchar pattern. It
still resolves to the same `fulltext2_search` TVF and the same index-application
path; only the operand form differs.

### 5.2 Where implication actually fails

These are the cases that make or break the feature, and each one is a rule the
rewrite must obey.

**Collation — not an issue; my earlier claim was wrong.** I previously flagged
case-insensitive collation as a source of false negatives. It is not: MO
compares `varchar` by bytes, with no collation-aware comparison in the eval
path (no case-folding or collation handling in the comparison operators or
`pkg/vectorize`). Byte equality in the index is exactly the SQL semantics, so
no restriction is needed and none is imposed.

**Type family — the one real trap left.** A JSON number `3.14` is indexed as a
float element, but `json_extract_string(j,'$.a.b') = '3.14'` is **true** for
that document (`json_extract_string` renders a numeric leaf, it does not return
NULL). Probing only the string encoding would drop that row — a wrong answer,
not a missed optimization.

The fix is cheap and keeps the issue's main use case: **the probe is the `OR`
of the plausible encodings of the constant.** For `= 'XXX'` (not numeric) that
is one string term — an exact lookup, unchanged. For `= '3.14'` it is the
string term OR the float term. The document must contain one of them, so the
disjunction is still a necessary condition, and the retained predicate removes
the extras.

Accelerating a genuinely cross-type comparison (`json_extract_float64` against
a leaf stored as a string) is out of scope for v1 and is simply left
unrewritten — correct, just unaccelerated.

**Non-scalar and wildcard paths.** `$.a[*].b`, or a path resolving to an object
or array, extracts something that is not a single scalar. v1 rewrites only
literal scalar paths. Documents where the same key occurs many times just
produce extra terms → false positives → filtered.

**Boolean context.** The conjunct may only be injected where the original
predicate *must* hold: top-level `AND` operands. Never under `NOT`, and under
`OR` only if every branch yields a probe (then inject the `OR` of the probes).
v1: top-level conjuncts only.

**Missing path / NULL.** `json_extract_*` returns NULL, `= 'XXX'` is NULL, row
is excluded — and no term exists. Consistent, no special case.

**No matching index.** Rewrite only when a `json`-parser fulltext2 index exists
on exactly that column **and** its persisted `includeKeys` is true. The probe
term shape must be derived from that index's recorded options, never assumed:
probing an index that holds no tuple terms finds nothing and drops rows.

### 5.3 Placement

The rewrite is a filter-level pre-pass that runs *before* index application, so
that the synthesized `fulltext_match` is picked up by the existing
`getFullTextMatchFiltersFromScanNode` path in `apply_indices_fulltext.go`. It
adds a conjunct and never removes one, so it cannot change results even if
index application later declines to use it.

## 6. Range queries — in v1

**Decided: v1 covers `=`, `>`, `>=`, `<`, `<=`.** Ranges are not deferred.

`json_extract_float64(j,'$.a.b') > 3.14159` scans all terms in
`[ (z, 3.14159) , (z, +∞ float) ]`. `Segment.PrefixRange` (`segment.go:755`)
already binary-searches `sortedTerms`; `TermRange(lo, hi)` is the same
`sort.SearchStrings` with a computed upper bound instead of a prefix test, and
because both ends are inclusive it needs no inclusivity parameters.

**`json_extract_string` inequalities need TWO ranges.** That function renders
numbers, so a leaf satisfying the comparison may be stored under either
encoding, and the orders disagree (`"10" < "9"` as text). The probe unions the
ordered string range with **every** numeric term under the tag. The numeric side
is deliberately untightened: there is no order-preserving map from the text
comparison onto the float encoding, so narrowing it risks excluding a qualifying
leaf. Wider scan, exact answer.

`json_extract_float64` needs only one range — it returns NULL for every
non-numeric leaf, so the numeric range is exactly implied.

**How the two options change the search shape:**

Equality is an **exact single-term lookup** and an inequality is a range over
the value element under a fixed `tag` prefix. Because the term carries no
ancestor path, both are path-agnostic: they match the key/value at any depth,
and the retained predicate narrows to the requested path.

## 7. Work breakdown

**Done**

- `bytejson.TokenizeLeaves` — the structure-aware walk (tag + ancestor path +
  typed value).
- `fulltext2` tuple encoder, probe builders, the `OR`-of-encodings equality
  probe, and the length-prefixed CDC carrier.
- **Both build paths wired to that one encoder**: `fulltext2_create.rowTerms`
  (CREATE) and `Fulltext2SqlWriter.rowText` + `CdcTokenizerWithJSONOptions`
  (ISCP), with a parity test asserting byte-identical `(word, pos)` pairs.
- `include_keys` / `include_full_path` algo params, read identically by both
  paths.

The ISCP side needed no architecture change after all: the CDC blob is already
length-prefixed and CRC-checked (`Cdc.Encode`), so it carries raw binary terms
safely. The writer emits finished terms into that carrier and the tokenizer
decodes them verbatim — no text intermediate, so the keys are never discarded.

**Phase 1 — encoding + exact match**

1. `bytejson`: replace the value-only walk with a path-aware tuple tokenizer.
   The existing `includeKey` bool becomes `includeKeys` and gains a companion
   the current "key as its own adjacent token" behaviour is
   **removed**, since it is the half-measure this change supersedes and nothing
   in the tree calls it.
2. `pkg/fulltext2`: fix the `json` parser path — one shared encoder called from
   **both** `DocTokenizer`/CREATE and `CdcTokenizer`/CDC (§1), reading the two
   options.
3. `pkg/catalog` + DDL + `SHOW CREATE`: persist and round-trip `includeKeys` /
   and read a missing
   value as `includeKeys = false` for pre-existing indexes (§4.4).
4. `pkg/sql/plan`: the predicate → probe rewrite with the §5.2 rules, keyed off
   the index's persisted options.

5. `pkg/fulltext2`: `Segment.TermRange(lo, hi)` (inclusive both ends) and the probe
   plumbed through `fulltext2_search` — required by v1's `>`/`>=`/`<`/`<=`.
6. Binary-clean audit of the §3.2 table: the TVF output vector, the CDC
   carrier, and DDL rejection of the `json` parser on a v1 index.

**Later**

7. DDL: an `INCLUDE_KEYS` index option so §4.3's proposed surface is reachable
   from SQL, plus its `SHOW CREATE` round-trip. Everything below the grammar
   already reads the param.
8. `$.a.*.z` wildcard paths via prefix + residual filter.
8. Cross-type acceleration (`json_extract_string` against a numeric leaf gets
   an exact probe rather than the §5.2 `OR`-of-encodings widening).

## 8. Decisions

Settled in review:

1. **Raw binary terms, not hex.** Make the whole term path binary-safe; carry
   the probe as a `varbinary` **function argument** rather than a parsed
   pattern (§3.2, §5.1). Consequence: this parser is fulltext2-only, because
   the v1 engine interpolates terms into SQL text.
2. **Collation is irrelevant** — index matching is byte comparison, which is
   also what MO's `varchar` `=` already does. My earlier concern was wrong and
   the restriction it implied is dropped (§5.2).
3. **Replace the old `json` parser in place** and keep the name `json`.
4. **No ancestor path in the term** (§6): equality stays an exact lookup and
   probes are path-agnostic. The `includeFullPath` mode an earlier draft
   proposed was removed — unclear benefit, and a second term shape every probe
   would have to agree with.
5. **v1 scope is `=`, `>`, `>=`, `<`, `<=`** — ranges are in, not deferred.
   Cross-type acceleration (`json_extract_string` on a numeric leaf) is a later
   task.

6. **`json_extract_string` probes the `OR` of plausible encodings.** Confirmed.
   `= 'XXX'` (not numeric) stays a single exact string-term lookup; `= '3.14'`
   probes the string term OR the float term, because
   `json_extract_string` renders a numeric leaf and the predicate is genuinely
   true for it. The disjunction is still a necessary condition, so the retained
   predicate removes the extras. Without this the row would be dropped — a
   wrong answer, not a missed optimization.

Assumption taken from decision 3, recorded rather than asked a third time:
**"replace" means tuple terms only** — plain leaf-value terms are no longer
emitted when `includeKeys = true`. A rebuilt index therefore stops answering
`MATCH(j) AGAINST('XXX')` free-text queries over JSON; `includeKeys = false`
remains available for that. Say the word if value terms should be emitted
alongside the tuples instead — it is a one-line change in the tokenizer, but it
roughly doubles term count, so it should be a deliberate choice.

## 9. Testing

Following the counterexample discipline: the invariant is §5.1, and the
negation to hunt is **a row the original predicate accepts but the probe
rejects**. A generator that builds random JSON docs, indexes them, and asserts
that `WHERE pred` and `WHERE pred AND probe` return identical row sets is the
primary oracle — it tests implication directly rather than a plan shape.

The generator must cover all five operators and values chosen to sit on range
boundaries — `>` vs `>=` on a value that
exists in the index is exactly where an off-by-one in the term bounds shows up
as a dropped row.

Targeted cases derived from §5.2 and §3.2: number-vs-string extraction of the
same leaf (the `OR`-of-encodings rule); values that straddle the 127-byte
truncation boundary; **bytes that would be BOOLEAN-mode syntax** (`+ - * "`)
and embedded `0x00`, proving the probe is never pattern-parsed; duplicate keys
and arrays; `NOT`/`OR` contexts; missing paths; DDL rejection of the parser on
a v1 index; and — because §1 says it is a known failure mode — a
**CREATE-vs-CDC parity test** asserting both build paths emit byte-identical
terms for the same document.

`EXPLAIN` assertions may confirm the index is used, but must never be the only
oracle for correctness.
