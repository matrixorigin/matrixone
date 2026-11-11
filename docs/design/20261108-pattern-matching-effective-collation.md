# Pattern matching honors the effective collation (LIKE, ILIKE, REGEXP, FIND_IN_SET)

Status: design r6 submitted for independent design review (2026-11-09).
Supersedes approved r4 (SHA256
`c31aef6c201f712c72aca88c59149d6805f1b99fddad9d2a0cde724021edc098`), the r5
validation-contract correction, and one implementation-review resolution:
the alternative-deduplication prepass is linear and carries its own charged
work bound (4 MiB), so many identical siblings and deeply nested groups
cannot perform quadratic work before the rewrite budgets are initialized.
Implementation in progress on PR #32357 against this revision.

- Design revision: r6, 2026-11-09. Supersedes r5 (2026-11-08), r4 (2026-11-08, SHA256
  `c31aef6c201f712c72aca88c59149d6805f1b99fddad9d2a0cde724021edc098`,
  design-only PASS with one non-blocking P3), r3 (2026-11-08, SHA256
  `590a0eabdfeab02f21b7b8d708d27ced42652747e76997a10135e72ced11a4f6`), r2
  (SHA256 `c309345e456cf92c06446cd6bb12a0fb94b7c49637214e743ee71a4f47a95e9a`),
  and r1 (SHA256
  `215a86b250e88a8a6f3f1bfc1ccc6024d09c5c501a745a9ce609e4d6504ce574`); r1-r3
  each received a design-only REQUEST_CHANGES and r4 a design-only PASS, and
  the record section at the end carries the decisions and the resolutions.
  r5 changes only design contracts: it corrects the validation map's
  fold-fully expectations to the engine bounds measured during
  implementation and names the regexp2 compiled backtracking-state bound
  those measurements identified as the binding constraint for long fold
  runs; no invariant, ownership decision, budget value, or error family
  changes. The preserved implementation head
  `b554558f9d3c191244f01b0e162a83d6af88ba8d` predates the resolutions, whose
  production deltas (eligibility-checked fold-rewrite deduplication plus the
  resource-limit error, and the LIKE search work bound with shared
  cancellation accounting) are planned for the post-approval implementation
  turn together with their UT and BVT proof.
- Owner: gouhongshen (implementation agent), issue group owner per the tracker.
- Owning issue: [#31175](https://github.com/matrixorigin/matrixone/issues/31175)
  (consolidates [#31179](https://github.com/matrixorigin/matrixone/issues/31179)).
- Implementation: [PR #32357](https://github.com/matrixorigin/matrixone/pull/32357),
  head `b554558f9d3c191244f01b0e162a83d6af88ba8d` at this revision.
- Related design: [regexp-icu-compatibility.md](regexp-icu-compatibility.md)
  (issue #28305) owns the regexp2 admission layer, engine cache, and
  backtracking budgets this change threads case folding through; this document
  supersedes none of it and reuses its invariants by reference.
- Design gate: requested by the mo-bot implementation review at the
  implementation head above (design-first phase unmet). The gate triggers on
  size (about 3,377 production additions plus 119 deletions, excluding tests)
  and on complexity triggers: per-row hot-path matching semantics change, a new
  bounded pattern-rewrite stage with budget-dependent semantic fallbacks, and a
  SQL compatibility-contract change across two regexp engines and the planner.

## Problem, evidence, and invariants

### Problem and evidence

`LIKE`, `ILIKE`, `REGEXP`/`RLIKE`/`REGEXP_LIKE`, and `FIND_IN_SET` ignored the
operands' resolved collation and compared byte-wise. Under the server default
`utf8mb4_general_ci`, `'a' LIKE 'A'` and `'abc' REGEXP 'ABC'` returned 0 while
`LOCATE`/`INSTR`/`POSITION` on the same values returned the folded result, and
while `=`/`IN` already honored the collation. On a column declared
`COLLATE utf8mb4_unicode_ci`, `=` matched case-insensitively but `LIKE` and
`REGEXP` did not; no SQL workaround existed. Evidence: the issue's literal
matrix and the `ci1`/`uci2` table reproductions, probed against a MySQL 8.0.44
oracle (every expected value in the BVT case
`test/distributed/cases/charset_collation/issue_31175_pattern_collation.sql`
was recorded from that oracle before result regeneration).

Secondary defects of the same boundary, found while tracing the owner paths:

1. The planner rewrote `col LIKE 'prefix%'` into byte-ordered `=`/`prefix_eq`
   filters. Once LIKE folds under a case-insensitive collation, that rewrite
   returns different rows than the folded predicate, so index and scan paths
   would diverge.
2. `find_in_set` overload registration cast both operands through `varchar`,
   erasing the binary string domain MySQL uses to route membership byte-wise.
3. Constant folding truncated planner-built binary-to-text conversion casts
   whose target width is `-1` (every non-empty constant folded to `''`), and
   re-marked folded `CAST(x AS BINARY)` literals with binary provenance that
   defeated a winning case-insensitive collation.
4. `CAST(x AS BINARY)`, `BINARY 'x'`, and `binary(x)` presented a coercible
   literal's coercibility (4) instead of MySQL's IMPLICIT coercibility (2) for
   cast results, so a case-insensitive column strictly outranked the binary
   operand and the planner converted it to text, flipping byte-wise predicates
   to folding ones.
5. Aggregating the matchers into expression collation resolution exposed the
   implicit ci-column versus binary-CAST tie (equal coercibility, differing
   collation versions) to the generic resolver's version check, raising error
   1267 where MySQL and the pre-change behavior compare byte-wise (LIKE,
   FIND_IN_SET) or reject with error 3995 (REGEXP).

### Goals

- LIKE/ILIKE/REGEXP/REGEXP_LIKE/FIND_IN_SET honor the effective comparison
  collation of their operands exactly as `=`/`IN` already do, matching MySQL
  8.0 behavior for every probed discriminator in the issue acceptance
  checklist.
- Keep `_bin` collations, binary string domains (`_binary` introducer, HEX/BIT
  literals, `CAST(x AS BINARY)`, VARBINARY), and explicit `match_type` case
  flags byte-wise/case-sensitive as MySQL specifies.
- Keep scan and index-prefix plans row-identical under a folding collation.
- Bound every new per-call cost: pattern tokenization, segment search, fold
  expansion, and member scans stay cancellable and memory-bounded.

### Non-goals

- PAD SPACE semantics, sort weights, and key uniqueness under ci collations
  (explicitly excluded by the issue's acceptance checklist).
- LIKE multi-character expansion spanning: MySQL's `my_wildcmp_uca` compares
  one pattern character against one subject character, so `ß LIKE 'ss'` stays
  0; whole-string key comparison is the `=`/FIND_IN_SET contract, not LIKE's.
- Three ICU-only REGEXP corners remain divergent and are pinned as limits by
  tests: character-class string closure, caseless backreference matching, and
  quantified-atom fold-image spanning.
- `REGEXP_INSTR`/`REGEXP_SUBSTR`/`REGEXP_REPLACE` default case behavior
  (unchanged unless `match_type` requests `i`).
- A pre-existing equality/IN pure-literal parity gap under explicit
  `utf8mb4_general_ci`, owned by a follow-up issue.
- No persistent state, catalog, wire, transaction, or configuration change;
  no new background worker, goroutine, or metric schema.

### Invariants

1. **Collation-routed matching.** Each pattern operator compares with the
   character equivalence of its resolved collation: `utf8mb4_general_ci`
   per-character weights (each weight one canonical rune), the `unicode_ci`
   family's UCA 4.0.0 primary weight streams per character, and byte-wise
   comparison whenever either operand is a binary string. An explicit
   `match_type` `c`/`i` flag overrides the collation default for REGEXP;
   non-case flags (`m`/`n`/`u`) do not block it.
2. **Single routing owner.** The planner resolves the comparison collation by
   aggregating the matcher's operands (LIKE/ILIKE including ESCAPE;
   REGEXP_LIKE from subject and pattern only, matching MySQL) and stamps it on
   the operand the executor reads (the LIKE/REGEXP subject, the FIND_IN_SET
   list). The executor never re-derives a different collation.
3. **MySQL binary-mix rules.** At equal non-explicit coercibility a binary
   operand absorbs the tie (`pairwiseBinaryTieWinner` replays MySQL's pairwise
   aggregation only when the generic resolver rejected and only for the
   aggregated matchers): LIKE/ILIKE/FIND_IN_SET compare byte-wise, REGEXP
   keeps error 3995, text/text conflicts keep error 1267, and an explicit
   COLLATE wins before any tie. Binary-domain cast results carry IMPLICIT
   coercibility (2) from creation so the tie rule, not a literal default,
   decides them.
4. **Per-character LIKE semantics.** One pattern character consumes exactly
   one subject character; a character with a multi-weight expansion (U+00DF
   weighing as "ss") compares as one character and is never split or spanned
   by a wildcard. Malformed bytes share one synthetic weight above every real
   weight; characters with no table weight compare by code-point identity
   (MySQL `my_uca_charcmp`).
5. **Full-fold REGEXP equivalence, with an exactness boundary.** Under
   case-insensitive matching, maximal unquantified literal runs match exactly
   the subject strings whose full Unicode case fold equals the run's fold
   image (ICU parity: `ss` matches `ß`, `STRASSE` matches `Straße`).
   Quantified atoms keep their per-character orbit; character classes keep
   their syntax and members and, on the case-insensitive regexp2 route, gain
   the fold-orbit members the engine's simple-fold class closure misses. The
   match set is never silently narrowed: dedup-eligible sibling alternatives
   (byte-identical at equal effective inline-option state, free of captures,
   backreferences and unscoped option setters; first occurrence kept) are
   deduplicated before expansion, and
   identical rewrite-generated fold-image alternatives during emission, and
   when the faithful full-fold form still exceeds a budget the operator
   raises a bounded resource-limit error instead of falling back to
   per-character matching (invariant 7).
6. **Admission verdicts stay with the source pattern.** The case-fold
   expansion is an optimization of expression, never a reason to declare a
   pattern valid or invalid: both engines take malformed-pattern verdicts
   from the source pattern as written (the regexp2 route validates the
   source form, so a rewrite can never launder a malformed pattern). When a
   well-formed, source-admitted pattern's faithful expansion exceeds a
   rewrite budget or a downstream admission bound, the outcome is the
   bounded resource-limit error of invariant 7 — a resource verdict, not a
   pattern verdict — and never a silent retry that matches through the
   weaker per-character caseless orbit.
7. **Bounded rewrite and matching work, with errors at the semantic
   boundary.** One shared search budget threads each LIKE call: every
   weight comparison in KMP scans, the Shift-And scan, and anchor-fallback
   verification — including `_` positions and one-character literals —
   charges one shared step, and the context is checked every 1024 shared
   steps, so no token shape or candidate count can delay cancellation
   beyond that gap. The bit-parallel segment search is capped at 256 KiB of
   masks; over-budget segments use the longest-literal anchor plus
   verification fallback under a deterministic total-work bound of
   `max(1<<20, 4 × words × subject_chars)` comparisons, with
   `words = ceil((width+1)/64)` — a small multiple of the Shift-And cost
   model the segment would have used had its masks fit — and exhaustion
   raises the bounded resource-limit error. FIND_IN_SET member scans probe
   cancellation every 1024 members. The fold rewrite is bounded per run (64
   enumerated alternatives, 128 KiB emission), per pattern (1 MiB emission
   enforced during emission), and per rewrite work (16 MiB including
   discarded attempts); eligibility-checked identical-alternative
   deduplication (invariant 5) runs before and during expansion, and
   exhaustion of any fold budget for fold-relevant content raises the same
   bounded resource-limit error. The deduplication prepass itself is linear
   and bounded (4 MiB of charged work: the pattern length once for the
   scan, one unit per branch for the close-time flag folding, and the
   branch-text bytes each alternation closure hashes): a closing group
   folds its constructs' flags into the enclosing branch in one pass and
   each closure tracks its most recent unscoped-setter branch
   incrementally, so deeply nested constructs and many identical siblings
   cost O(1) each instead of an ancestor walk or an intervening-branch
   rescan; exhausting the prepass bound abandons deduplication and returns
   the source pattern unchanged — never an error, since the expansion
   budgets above still guard the rewrite. Engine-side admission bounds
   then apply to
   the expanded form through the ICU design's regexp2 layer, unchanged by
   this design: the translated-size bound (1 MiB) and the compiled
   backtracking-state bound (4,096 tracks). Exact fold emission grows
   quadratically with run length, so for long fold runs the
   backtracking-state bound binds first on the regexp2 route — measured on
   the implementation: a 40-rune `s` fold run expands to 17,918 bytes and
   compiles at 1,439 tracks (admitted), while an 80-rune run compiles past
   4,096 tracks and raises the bounded resource-limit error naming that
   bound; the RE2 route has no compiled-state bound and admits the same
   80-rune expansion (44,502 bytes). For multi-branch shapes whose distinct
   alternatives each carry long fold runs, the rewrite's own per-pattern
   emission budget (also 1 MiB) fires first — twenty distinct 80-`s`
   branches emit about 1.38 MiB — and the translated-size bound remains the
   engine-side backstop, with exact per-part enforcement in the regexp2
   translation stage keeping the enforced bound exactly the documented one,
   so the error always names the bound that actually rejects the expansion.
   Verbatim emission
   survives only where no full-fold promise exists (fold-irrelevant runs
   and non-UTF-8 patterns). The error mirrors MySQL's own regexp resource
   limits (`regexp_stack_limit`, `regexp_time_limit`): at a resource
   boundary MySQL fails the evaluation rather than narrowing
   case-insensitive semantics, and the issue's acceptance matrix sits
   orders of magnitude inside these budgets, so no accepted behavior
   changes.
8. **Plan/execution agreement.** Under a case-insensitive collation the
   planner keeps the folding LIKE predicate (no byte-ordered `=`/`prefix_eq`
   rewrite), so index and scan paths return the same rows; under byte-ordered
   domains the existing rewrite is unchanged.
9. **Domain preservation through folding.** Binary string domains survive
   overload resolution, conversion casts, and constant folding; a folded
   binary-source literal follows the selected text domain only when a
   case-insensitive collation won by strictly stronger coercibility, and
   negative-width (`-1`) conversion targets never truncate.
10. **Cache contract unchanged.** The existing RE2/regexp2 caches keep their
    100-entry/32 MiB bounds, subject-independent entries, and eviction
    semantics; the fold expansion is deterministic in the existing cache key
    (pattern, match type, binary mode, binary case-fold mode), so no new cache
    dimension or retention path is introduced.

## Standards, precedent, and alternatives

- MySQL 8.0.44/8.0.45 is the behavioral oracle: `my_wildcmp_uca`
  (strings/ctype-uca.cc) for per-character LIKE weights, `my_strnncoll`
  unpadded membership for FIND_IN_SET (trailing spaces stay significant, which
  the implementation reproduces by keying value plus one weighted non-space
  sentinel character), ICU `UREGEX_CASE_INSENSITIVE` full case folding for ci
  REGEXP (ICU 78.3 probes: U+1E9E matches both U+00DF and "ss"), coercibility
  rules (explicit 0, implicit 2, coercible 4) with binary absorbing equal-rank
  ties, and errors 1267/3995 for the conflict shapes.
- Unicode: full case folding data comes from `golang.org/x/text` v0.37.0
  `cases.Fold` (generated table, not hand-edited); UCA 4.0.0 primary weights
  reuse the pinned table already backing `utf8mb4_unicode_ci` equality, so
  LIKE and `=` cannot disagree about a character's weight.
- Precedent inside the repository: `regexp-icu-compatibility.md` established
  admission-bounded engine routing; this change follows the same pattern
  (admission on the source pattern, budgets before the engine runs, bounded
  fallbacks) and leaves its invariants intact.

Alternatives considered:

1. **Whole-string collation keys for LIKE** (the `=`/`IN` mechanism).
   Rejected: it spans expansions across characters (`ß` would match `ss`
   under `_` or a wildcard), changing MySQL's per-character wildcard
   semantics; it is correct for FIND_IN_SET membership, where it is used
   (with the sentinel preserving unpadded comparison).
2. **Engine caseless flags only** (`(?i)`, regexp2 `CaseInsensitive`).
   Rejected: simple per-character folding cannot express full folding
   (`ß`↔`ss`), and `utf8mb4_general_ci`'s MySQL weight table differs from
   Unicode simple case mapping (e.g. the sigma and dotless-i behaviors).
3. **Always-on plan-time conversion of every operand to a canonical folded
   form.** Rejected: erases binary string domains before the executors can
   route them, changes error behavior MySQL pins, and pays conversion on hot
   paths whose operands already carry the selected collation; conversion is
   applied only where resolution requires it.
4. **Direct ICU through CGo.** Rejected as in the ICU design: native
   dependency across the Linux/macOS/arm64 build matrix for a pure-Go
   function path.
5. **Status quo byte-wise matching.** Rejected: violates MySQL compatibility
   and makes sibling operators on the same data disagree.

## Logical and architectural closure

### Ownership and data flow

1. `pkg/common/collation/wildcard.go` (new) owns collation-aware LIKE
   matching: character weighing (`GeneralCICharWeight`, `UCA400CharWeight`
   with malformed-byte and weightless-identity sentinels), pattern
   tokenization (`TokenizeLikePattern` with escape rules and `%%` merging),
   and matching (`MatchLikePattern`). Shapes with zero or one `%` match at
   determined positions; two or more `%` split into fixed-width segments
   located by leftmost-greedy search (complete by the exchange argument
   recorded in the code), each segment found by KMP over weight streams, a
   budgeted bit-parallel Shift-And when literals mix with `_`, or the
   longest-literal anchor-plus-verify fallback when masks exceed the 256 KiB
   word budget. One per-call search budget is shared by every segment
   search: all weight comparisons (KMP scans, the Shift-And scan, and
   per-candidate verification, including `_` positions and one-character
   literals) charge it, cancellation is observed every 1024 shared steps,
   and the anchor fallback's cumulative comparisons are capped at
   `max(1<<20, 4 × words × subject_chars)`; crossing the cap raises the
   bounded resource-limit error, so an adversarial fragmented pattern
   (thousands of one-character literals separated by `_`) cannot multiply
   candidates by verification width into hundreds of millions of
   uncancellable comparisons.
2. `pkg/sql/plan/function/func_builtin_regexp.go` owns LIKE/REGEXP dispatch:
   `likeFoldModeFor` selects the fold from the subject operand's resolved
   charset; `generalCILikeFold` maps values to the 1:1 general_ci weight-rune
   stream so the existing byte fast paths compare exactly the collation's
   weights; `unicodeLikeMatchFn` routes the unicode_ci family to the weighted
   matcher; ILIKE keeps its historical fold-on-every-domain orbit; the REGEXP
   entry points thread the collation default case flag into both engines and
   keep the binary-domain routing and error 3995 contract.
3. `pkg/sql/plan/function/regexp_casefold.go` (new) and
   `regexp_casefold_tables.go` (generated from `cases.Fold`) own the
   grammar-aware full-fold rewrite: scope tracking for `(?i)`/`(?x)` group
   options on both engine grammars (including regexp2's nested class
   subtraction), literal-run segmentation into single-rune blocks and
   fold-image alternations with case-sensitive source alternatives, class
   fold-orbit augmentation, literal-carrying escape decoding, named-reference
   preservation, eligibility-checked alternative deduplication, the
   deduplication prepass work bound, and the
   emission/work budgets. Deduplication is conservative: a sibling
   alternative is dedup-eligible only when it is byte-identical to an
   earlier sibling in the same alternation, contains no capturing group
   (numbered or named), no backreference or named/relative reference, and
   no unscoped inline-option setter, and is evaluated under the same
   effective inline-option state as the kept occurrence — the case (`i`)
   and free-spacing (`x`) letters this rewrite tracks. An unscoped option
   setter applies from its point to the end of the enclosing group, so it
   changes the ambient state of every later sibling: in
   `^(?:ss|(?i)a|ss)$` under a case-sensitive ambient mode only the second
   `ss` inherits `(?i)` and matches `SS` and `ß`, and deleting it silently
   narrows the match set. Conservatively, any unscoped option setter
   between the kept occurrence and the duplicate in the enclosing group
   makes the duplicate ineligible, even when the intervening setters would
   net out; scoped `(?flags:...)` groups restore the prior state at group
   end and do not affect their siblings. The first occurrence is kept so
   branch order and positional inline-option state are preserved. Source
   capture
   numbering, names, and participation are user-observable (regexp2
   backreferences such as `\3`, `REGEXP_REPLACE` replacement references,
   and the `regexp2CaptureMapping` contract of the ICU design's
   capture-identity invariant), so capture-bearing, reference-bearing, or
   option-setting branches are always retained; alternation idempotence
   alone never justifies deleting them. During emission, dedup applies to
   identical fold-image alternatives, which the rewrite itself generates as
   non-capturing forms at one position under one option state, so
   eligibility holds by construction. Ineligible
   redundant branches stay in the pattern; when the faithful expansion
   still exceeds a budget the resource-limit error contract applies.
   Exhaustion of any fold budget for fold-relevant content raises the
   bounded resource-limit error; verbatim emission remains only for
   fold-irrelevant runs and non-UTF-8 patterns, where no full-fold promise
   exists.
4. `regexp_icu.go` and the RE2 cache path in `func_builtin_regexp.go` own
   engine admission: each compiles the expanded form first; when the
   expanded form exceeds an admission bound, the source pattern's own
   verdict stands (malformed stays malformed with its own error, and the
   regexp2 path validates the source form so a rewrite can never launder a
   malformed pattern), while a well-formed, source-admitted pattern raises
   the bounded resource-limit error because its faithful expansion cannot
   be evaluated within bounds. Cache keys, bounds, and eviction are
   unchanged.
5. `pkg/sql/plan/collation_expression.go` owns resolution coverage:
   aggregation indexes for the matchers, the binary-domain guard that keeps a
   binary operand's original domain visible to the REGEXP compatibility
   check, the scoped pairwise binary tie restoration, delivery of the
   selected collation to single-operand-reading matchers when legacy
   conversion stays inactive, and literal-form clearing when a strictly
   stronger case-insensitive selection converts a constant binary literal.
6. `base_binder.go` / `setExpressionCollation` stamp binary cast results
   with IMPLICIT coercibility; `opt_misc.go` keeps folding LIKE predicates
   for ci columns; `rule/constant_fold.go` and `func_cast.go` preserve folded
   domains and negative-width targets; `list_builtIn.go` registers
   `find_in_set` with the domain-preserving check.

Per-call flow for REGEXP:

```text
bind: aggregate operands -> resolve collation (tie restoration if needed)
  -> stamp/deliver selected collation -> executor reads subject charset
evaluate: binary domain? -> byte path (3995 for text mix on regexp)
  -> match_type resolution (explicit c/i beats collation default)
  -> engine admission on the source pattern (RE2 scanner / regexp2 scanner)
  -> case-fold expansion when ci applies (deduplicate eligible identical
     alternatives, then expand; fold-irrelevant content stays verbatim)
  -> on fold-budget exhaustion for fold-relevant content: bounded
     resource-limit error
  -> compile expanded; on admission error with expansion applied, take the
     source verdict when the source itself is inadmissible, else the
     bounded resource-limit error
  -> cache install only after all steps succeed (unchanged contract)
  -> match with existing per-engine deadlines and budgets (unchanged)
```

### States, transitions, and unhappy paths

The change adds no long-lived state: fold tables are static, the folder is an
immutable `cases.Caser`, per-batch LIKE tokenization is discarded with the
batch, and the regexp caches pre-date this change with unchanged ownership.
Unhappy paths: invalid ESCAPE (existing error), malformed patterns (verdict
always from the source), budget exhaustion (LIKE mask overflow enters the
bounded anchor search, whose total-work cap raises the bounded
resource-limit error; fold-rewrite overflow for fold-relevant content
raises the same error; existing bounded errors for the regexp2 budgets),
context cancellation (observed every 1024 shared search steps in LIKE,
every 1024 steps/members elsewhere), and cache eviction
(subject-independent metadata only, unchanged).

## Compatibility, delivery, and operations

- SQL results change only where the issue requires folding: the enumerated
  BVT refreshes (`operator/like_operator`, `operator/operator`,
  `operator/regexp_operator`, `operator/not_regexp_operator`,
  `function/func_regular_like` CRLF-multiline rows, `optimizer/like` plans,
  `optimizer/like_prefix_range`, `charset_collation/charset_collation_basic`
  and `_advanced` ci-table rows, `fulltext2/fulltext2_covered`) are intentional
  flips probed against the MySQL oracle; the `utf8mb4_bin` controls are
  unchanged. SHOW COLUMNS/DESCRIBE LIKE filters now fold with the effective
  collation (a real consumer, pinned by updated oracle tests).
- No catalog, persisted-data, wire, or configuration change; rollback is a
  source-level revert with no data migration. During a mixed-version window a
  folding LIKE/REGEXP on ci data returns different rows on old and new CNs;
  the rollout rule is to upgrade every CN that may receive the workload
  before relying on the folded behavior, matching the ICU design's rule.
- Two designed resource boundaries are new SQL-visible outcomes, both
  confined to pathological inputs outside the acceptance matrix: a LIKE
  pattern whose anchored verification exceeds the total-work bound, and a ci
  REGEXP whose faithful full-fold form exceeds a rewrite or admission budget
  even after alternative deduplication (on the regexp2 route, long fold runs
  reach the engine's compiled backtracking-state bound — 4,096 tracks —
  well before the translated-size bound, because exact fold emission is
  quadratic in run length), each raise a bounded resource-limit error. The pre-change server answered the REGEXP shapes through the
  engines' per-character caseless orbit; that answer was already wrong for
  fold-spanning subjects under the semantics the caller requested (MySQL
  `i` is ICU full folding), and MySQL itself fails evaluations that exceed
  `regexp_stack_limit`/`regexp_time_limit` rather than narrowing semantics,
  so the error is the compatibility-preserving boundary. The r1-era
  silent-fallback test pins are replaced by the boundary tests in the
  validation map.
- Error contract: the resource-limit error uses the same internal-error
  family as the existing regexp2 budget errors
  (`moerr.NewInternalErrorNoCtxf`), with stable message prefixes
  `regexp case-fold resource limit exceeded: <bound>` and
  `LIKE pattern resource limit exceeded: <bound>` naming the exceeded
  bound; pattern-validity verdicts keep the invalid-input family
  (`moerr.NewInvalidInputNoCtxf`) and always come from the source pattern.
  Being a new SQL-visible error, it carries public-path BVT proof (the
  validation map's resource-boundary row): the round-15 BVT statements
  whose `1`/`1`/`0` pins and "never an error" comments this contract
  flips are updated, below-bound controls keep their folded results, and
  expected results are independently reviewed before comparison execution.
- Hot-path cost: unchanged fast paths for binary and `_bin` domains; ASCII
  general_ci fold is `bytes.ToUpper`; the weighted matcher runs only for the
  unicode_ci family; the fold rewrite runs once per compiled pattern inside
  the existing cache, bounded by the budgets above. Cached constant-pattern
  preparation is the only per-pattern cost; per-row subject processing keeps
  its subject-length-dependent storage: the unicode_ci LIKE matcher weighs
  each subject into per-character weight slices per match
  (`prepareLikeSubjectChars`), the general_ci fold produces one folded subject
  buffer per comparison, and REGEXP folds each subject once per row — all
  O(subject length) per row, released with the batch, and unchanged from the
  r1-era implementation this design reviews. (Erratum directed by the
  independent r4 design review's P3 finding: the r4 text's "no new
  allocation on constant-pattern rows beyond the prepared form" conflated
  cached pattern preparation with per-row subject allocation.)
- Observability: budget and admission failures surface as bounded SQL
  errors (the fold/LIKE resource-limit error included); no new metric or
  log schema, and user patterns/subjects are not logged.

## Verification and open decisions

Acceptance mapping (the r1-era tests exist on the implementation head and
pass in CI on Ubuntu/x86 UT, coverage, and BVT compose/proxy jobs at head
`b554558f`; rows marked planned are the boundary tests the review
resolutions add in the post-approval implementation turn):

| Invariant | Cheapest deterministic evidence |
| --- | --- |
| Collation-routed LIKE/REGEXP/FIND_IN_SET (1, 2) | `pkg/sql/plan/function` fold-mode and routing UTs; BVT `issue_31175_pattern_collation.sql` (410 statements, MySQL-oracle values) covering literals, columns, both operand orders, prepared parameters, user variables |
| Binary-mix rules (3) | `collation_binary_tie_test.go`, admission-matrix UTs, BVT `_binary`/HEX/BIT/CAST rows incl. error 3995 both orders |
| Per-character LIKE weights (4) | `pkg/common/collation/wildcard_test.go` (weights, malformed bytes, supplementary identity, sigma/dotless-i), `wildcard_segments_test.go` anchor fallback |
| Full-fold REGEXP (5, 6) | `regexp_casefold_internal_test.go`, `regexp_casefold_test.go` (tables, orbits, budgets, source-verdict preservation), `func_builtin_regexp_test.go` error-shape tests; planned boundary tests on both engines: redundant-literal-alternative equivalence (`A|A|A|A` ≡ `A` over the ß/ss/STRASSE subject matrix, split by engine route after the r5 measurement: on the RE2 route the 328-byte-class four-identical-80-`s`-alternative shape deduplicates to one branch, folds fully, and forty `ß` match eighty `s` under `i`; on the regexp2 route the comment-bearing four-alternative shape deduplicates to two distinct branches — the trailing `(?#x)` joins the last — whose 80-rune fold runs still exceed the engine's 4,096-track compiled backtracking-state bound, so it raises the bounded resource-limit error under `i` and the route's fold-fully equivalence proof relocates below the bound with four identical 12-`s` alternatives plus the comment, six `ß` matching twelve `s`), dedup-eligibility counterexamples (capture-bearing duplicates retained — `^((ss)|(ss))\3$` keeps group 3 valid and matches `ssss` through the second branch with group-3 participation; `REGEXP_REPLACE` `$3` replacement identity preserved; backreference-bearing and unscoped-`(?i)`-bearing duplicates retained; intervening-option duplicates retained on both engine routes — `^(?:ss|(?i)a|ss)$` under an ambient case-sensitive mode keeps both `ss` branches, so `SS` and `ß` still match through the second `ss` on the RE2 route and on the lookahead-prefixed regexp2 route, while the same shape with a scoped `(?i:...)` setter between the occurrences still dedups; capture-free duplicates at equal option state still deduped), and post-dedup budget exhaustion raising the resource-limit error (replacing the silent-false pins in the r1 admission-fallback tests) |
| Bounded work (7) | cancellation and bounded-work UTs in both packages; planned tests: the fragmented-pattern counterexample (`%` + 15,000 `a_` pairs + a 92-distinct-weight alphabet interleaved with `_` + `%` against 60,000 `a` characters) terminating at the total-work bound with the resource-limit error and an instrumented comparison count at or under the bound, cancellation observed after subject preparation within the shared-step gap, and deterministic work-bound checks straddling the 256 KiB mask-budget transition; plus representative per-row allocation measurements (the independent r4 review's P3 action): an allocations-per-run UT recording the unicode_ci LIKE matcher's subject-weight slices and the general_ci folded-subject buffer on constant patterns, reported in the implementation report as evidence for the storage cost described under hot-path cost — informational, proportional to subject length, with no fixed byte bound |
| Resource-boundary SQL proof (5, 7) | planned BVT (public-path proof per the testing contract, since both boundaries are new SQL-visible errors): update `issue_31175_pattern_collation.sql`'s round-14/15 sections — the 999-nested-capture `'i'` statements currently pinned `1`/`1`/`0` with "never an error" comments become the bounded resource-limit error, the 997-nest accept side and the `'c'` control stay unchanged, and the four-alternative comment-bearing section's `'i'` statements become the bounded resource-limit error (the deduplicated two-branch form still exceeds the regexp2 compiled backtracking-state bound) while its `'c'` control stays unchanged, with fold-fully BVT proof relocated to the same four-alternative shape without the comment on the RE2 route (forty `ß` returning `1`) and to a below-bound comment-bearing shape (six `ß` against four identical 12-`s` alternatives returning `1`), plus a public intervening-option proof — `REGEXP_LIKE(subject, '^(?:ss|(?i)a|ss)$', 'c')` returns `1` for `SS` and for `ß` because the second `ss` branch is retained (the deduplicated form would reject both); add `issue_31175_pattern_resource_limits.sql` covering regexp2-route expansion exhaustion at the fold per-pattern emission budget (the engine translated-size backstop sits at the same 1 MiB magnitude), RE2 height-bound exhaustion, LIKE total-work-bound exhaustion, below-bound controls just under each bound returning normal folded results, and malformed-source patterns keeping their own error verdicts. The error is pinned by its stable class and message prefix (`internal error: regexp case-fold resource limit exceeded: <bound>` / `internal error: LIKE pattern resource limit exceeded: <bound>`) using `@regex` on the prefix per BVT quality rules; expected results are hand-written from this contract and independently reviewed before normal comparison execution (generated results are a draft, never accepted merely because generated) |
| Plan/execution agreement (8) | `TestLikePrefixCaseInsensitiveKeepsFoldedPredicate` (pkg/tests/sqlintegration) proving scan, forced-index, and computed-subject paths return the folded rows; prefix fixtures pinned to `utf8mb4_bin` |
| Domain/coercibility preservation (9) | `pkg/sql/plan` and `rule` UTs for folded `CAST(x AS BINARY)`/`BINARY 'x'`/`binary()` stamping, negative-width casts, folded-domain preservation; `operator/mysql_compat_string_binary_comparison` BVT unchanged |
| Cache contract (10) | existing cache/eviction tests plus admission-error tests proving install only after success |

Drawbacks and accepted tradeoffs: the fold rewrite enlarges ci patterns
(bounded; above budgets the query raises the bounded resource-limit error
rather than silently narrowing the match set — exactness is never traded
for completion); the unicode_ci LIKE matcher is slower per row than the
byte path (accepted: correctness per the resolved collation; byte domains
keep the fast path); FIND_IN_SET keys value+sentinel per member comparison
(needle prepared once per row); the LIKE total-work bound can error an
adversarial fragmented pattern that an uncapped matcher would eventually
answer (accepted: a deterministic bound and prompt cancellation over
unbounded per-call work).

Open decisions: none blocking. Non-blocking: the pre-existing equality/IN
pure-literal parity gap under explicit `utf8mb4_general_ci` is owned by a
follow-up issue; the three ICU-only REGEXP corners are documented limits
pending a product decision to pursue full ICU parity.

## Design-first review record

This change triggers the design gate (size: about 3,377 production additions;
complexity: hot-path matching semantics, a new bounded rewrite stage with
budget-dependent fallbacks, and a compatibility-contract change across both
regexp engines and the planner). The gate was raised by the mo-bot
implementation review at head `b554558f9d3c191244f01b0e162a83d6af88ba8d`.

Revision history:

- r1, 2026-11-08, SHA256
  `215a86b250e88a8a6f3f1bfc1ccc6024d09c5c501a745a9ce609e4d6504ce574`:
  initial submission. The independent design-only review returned
  REQUEST_CHANGES with two P2 findings: (1) preserve REGEXP semantics when
  rewrite budgets are exhausted — the silent per-character fallback
  contradicted invariant 5 and was reachable by a short supported pattern
  (four identical 80-`s` alternatives plus a comment, 328 bytes, pinned
  returning false for forty `ß` under `i`); (2) close the LIKE fallback's
  CPU and cancellation bounds — the anchor fallback had no total
  verification-work bound, and its cancellation accounting skipped `_`
  positions and one-character literals (a fragmented counterexample needs
  hundreds of millions of comparisons with millions between checks).
- r2, 2026-11-08, SHA256
  `c309345e456cf92c06446cd6bb12a0fb94b7c49637214e743ee71a4f47a95e9a`:
  resolved r1's findings with alternative deduplication plus the
  resource-limit error and the LIKE shared budget with a total-work bound.
  The independent design-only review returned REQUEST_CHANGES with two P2
  findings: (1) restrict alternative deduplication to semantically
  equivalent branches — byte-identical reasoning excluded capture side
  effects (`^((ss)|(ss))\3$` loses group 3 if either identical branch is
  deleted; source capture identity is user-observable through
  backreferences and `REGEXP_REPLACE` replacements, and unscoped inline
  options make branch position significant); (2) plan public SQL proof for
  the new resource-limit outcomes — the planned validation was UT-only
  while the errors are SQL-visible, the existing BVT pins `1`/`1`/`0` with
  "never an error" comments for shapes the r2 contract turns into errors,
  and no stable error class/diagnostic was defined.
- r3, 2026-11-08, SHA256
  `590a0eabdfeab02f21b7b8d708d27ced42652747e76997a10135e72ced11a4f6`:
  resolved r2's findings with the conservative dedup-eligibility rule
  (byte-identical siblings only, no captures, no backreferences, no
  unscoped option setters, first occurrence kept; emission-level dedup safe
  by construction; ineligible branches retained with the resource-limit
  contract as the fallback) plus planned
  capture/backreference/replacement/inline-option counterexample tests, and
  with the error contract (internal-error family, stable bound-naming
  message prefixes, invalid-input family reserved for source-pattern
  verdicts) plus a planned resource-boundary BVT with independently
  reviewed expected results. The independent design-only review returned
  REQUEST_CHANGES with one P2 finding: require equal effective options
  before deduplicating alternatives — the r3 rule checked only the
  duplicate branch itself for option setters, missing that an intervening
  sibling's unscoped setter changes the duplicate's ambient state
  (`^(?:ss|(?i)a|ss)$` under a case-sensitive ambient mode: only the second
  `ss` inherits `(?i)`, so the permitted deduplicated form `^(?:ss|(?i)a)$`
  rejects `SS` and `ß` that the original matches; an independent ICU 78.3
  probe confirmed the divergence on both engine routes).
- r4, 2026-11-08 (this document): resolves the finding by extending the
  eligibility rule with an equal-effective-option-state condition
  (invariant 5, ownership item 3): a duplicate must be evaluated under the
  same effective inline-option state as the kept occurrence, and
  conservatively any unscoped option setter between the two occurrences in
  the enclosing group makes the duplicate ineligible, while scoped
  `(?flags:...)` groups restore state at group end and do not affect
  siblings. The validation plan gains intervening-option counterexamples on
  both engine routes (RE2 and the lookahead-prefixed regexp2 route,
  including the scoped-setter control that still dedups) and a public
  `REGEXP_LIKE(..., 'c')` BVT case in the planned
  `issue_31175_pattern_collation.sql` update (validation map). The
  preserved implementation head `b554558f9d3c191244f01b0e162a83d6af88ba8d`
  predates the r2-r4 resolutions; their production deltas and the planned
  UT/BVT proof land in the post-approval implementation turn.
- r4 review outcome, 2026-11-08: the independent design-only review of r4
  (SHA256 `c31aef6c201f712c72aca88c59149d6805f1b99fddad9d2a0cde724021edc098`,
  docs commit `b48766d8dae4056ba097e8524d3e91b5e2b2520f`) recorded a
  design-only PASS with one non-blocking P3: the hot-path-cost bullet
  conflated cached constant-pattern preparation with per-row subject
  allocation. This document carries the review-directed erratum in place
  (the hot-path-cost bullet now distinguishes cached pattern preparation
  from subject-length-dependent per-row storage, and the bounded-work
  validation row names representative allocation measurements). No
  invariant, ownership decision, budget, error contract, or validation
  semantic changed, so the erratum does not alter the approved r4 design
  content; the r4 hash above remains the approval's reference.
- r5, 2026-11-08 (this document): corrects r4's validation-map fold-fully
  expectation with engine bounds measured during the post-approval
  implementation turn. r4 planned that the 328-byte four-alternative
  comment-bearing shape would fold fully after deduplication so forty `ß`
  match eighty `s` under `i`. Measurement on the implementation proved the
  expectation physically unachievable on the regexp2 route: exact fold
  emission grows quadratically with run length, and the route's binding
  engine bound is the compiled backtracking-state limit (4,096 tracks) — a
  single 80-rune `s` fold run expands to about 68,956 bytes and compiles
  past the bound (a 40-rune run compiles at 1,439 tracks and is admitted),
  so even the two distinct branches left after deduplication exceed it, and
  the shape raises the bounded resource-limit error under `i` (the `'c'`
  control is unaffected: the rewrite never applies). The translated-size
  bound (1 MiB) is not the binding constraint for this shape — the
  deduplicated expansion is 137,917 bytes — and for many-branch shapes the
  rewrite's own per-pattern emission budget (also 1 MiB) fires first
  (twenty distinct 80-`s` branches emit about 1.38 MiB); exact per-part
  enforcement in the regexp2 translation stage keeps the documented
  translated-size bound exact as the engine-side backstop, so the error
  names the bound that actually rejects the expansion.
  The RE2 route has no compiled-state bound: the same four-alternative
  shape without the comment deduplicates to one branch and folds fully
  (44,502-byte expansion admitted), so the forty-`ß` oracle outcome is
  proved there, while the regexp2 route proves fold-fully equivalence below
  its bound (four identical 12-`s` alternatives plus the comment; six `ß`
  match twelve `s`). Resolutions: the validation-map invariant-5 row splits
  the equivalence proof by engine route; the resource-boundary BVT row
  updates round-14 (the comment-bearing `'i'` statements become the bounded
  resource-limit error, the `'c'` control unchanged, fold-fully BVT proofs
  on the RE2 route at the original scale and on below-bound regexp2
  shapes); invariant 7 and the compatibility bullet name the
  backtracking-state bound with the measured values. No invariant,
  ownership decision, budget value, error family, or approved r4 semantic
  changes: the correction relocates only which shapes deliver fold-fully
  outcomes versus the resource-limit error, inside the error contract r4
  already approved. Alternatives considered and rejected: raising the
  4,096-track bound (weakens an existing resource guard owned by the ICU
  design), post-expansion branch deduplication (a single 80-rune branch
  alone exceeds the bound), and subject-side folding (breaks the position
  mapping `REGEXP_INSTR`/`REPLACE`/`SUBSTR` rely on).
- Design revision: r5, submitted for independent design review of the
  correction above; r4's approval (hash and commit recorded there) remains
  the base, and the review-directed P3 erratum is carried in place.
- r6, 2026-11-09 (this document): resolves the mo-bot implementation
  review's P1 on the r5 head (`bd80b57137855d7e4535198c1cb086c94c6f3fde`):
  the alternative-deduplication prepass performed quadratic work before the
  rewrite budgets were initialized — each duplicate sibling rescanned the
  intervening branches for unscoped setters ((N-1)(N-2)/2 checks for N
  identical siblings: an exact-source probe of 65,536 alternatives took
  2.19 s locally) and every capture, reference, and option-setter construct
  walked the full ancestor frame stack (32,768 nested captures took 0.94 s).
  The resolution makes the prepass linear and bounds it: construct marks
  land on the innermost branch in O(1) and a closing group folds its
  branches' flags into the enclosing branch in one pass (preserving the
  capture/reference/option eligibility verdicts exactly — the flags of a
  construct reach exactly the branches whose text contains it), each
  alternation closure tracks its most recent unscoped-setter branch
  incrementally instead of rescanning, and a charged work bound of 4 MiB
  (pattern length once for the scan, one unit per branch for the close-time
  folding, branch-text bytes hashed per closure) abandons the prepass and
  returns the source pattern unchanged on exhaustion. The same probes now
  take 4.8 ms and 4.7 ms (linear), matching the engines' own parse cost;
  the deep-nested-alternation shape whose branch hashing still grows with
  depth × length (about 410 MB at depth 2,000) is capped by the bound.
  Abandonment adds no SQL-visible outcome: deduplication only shrinks the
  later emission, so skipping it defers to the approved expansion budgets
  and their resource-limit error, and malformed-source verdicts are
  untouched. No invariant, ownership decision, rewrite budget value, error
  family, or approved r4/r5 semantic changes: the prepass bound is a new
  internal resource guard inside invariant 7's bounded-work contract,
  directed by the implementation review itself. Regression coverage:
  exact-budget straddle pins of the charged-work model, both probe shapes
  completing below the production bound, the depth-scaling shape abandoning
  byte-identically, and nested flag-propagation eligibility rows
  (`regexp_casefold_internal_test.go`).
- Implementation proceeds against this document; material implementation
  deviations require updating this document and re-reviewing the affected
  decisions.
