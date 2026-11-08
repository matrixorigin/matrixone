# Pattern matching honors the effective collation (LIKE, ILIKE, REGEXP, FIND_IN_SET)

Status: design review pending

- Design revision: r1, 2026-11-08.
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
5. **Full-fold REGEXP equivalence.** Under case-insensitive matching, maximal
   unquantified literal runs match exactly the subject strings whose full
   Unicode case fold equals the run's fold image (ICU parity: `ss` matches
   `ß`, `STRASSE` matches `Straße`). Quantified atoms keep their
   per-character orbit; character classes keep their syntax and members and,
   on the case-insensitive regexp2 route, gain the fold-orbit members the
   engine's simple-fold class closure misses.
6. **Admission is never widened by the rewrite.** The case-fold expansion is
   an optimization of expression, never a reason to reject or accept a
   pattern: both engines take malformed-pattern verdicts from the source
   pattern as written, and when an expanded form exceeds a downstream
   admission bound the compile retries with the unexpanded form and keeps the
   base verdict (compared through the engine's per-character caseless orbit).
7. **Bounded rewrite and matching work.** LIKE tokenization, segment search,
   and literal comparison check the context every 1024 steps; the bit-parallel
   segment search is capped at 256 KiB of masks with a longest-literal anchor
   plus verification fallback; FIND_IN_SET member scans probe cancellation
   every 1024 members. The fold rewrite is bounded per run (64 enumerated
   alternatives, 128 KiB emission), per pattern (1 MiB emission enforced
   during emission), and per rewrite work (16 MiB including discarded
   attempts); exhaustion emits the pattern verbatim, preserving prior
   behavior for that corner instead of failing the query.
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
   word budget.
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
   preservation, and the emission/work budgets with verbatim fallback.
4. `regexp_icu.go` and the RE2 cache path in `func_builtin_regexp.go` own
   engine admission: each compiles the expanded form first and retries
   unexpanded when expansion pushed the pattern over an admission bound; the
   regexp2 path additionally validates the source form so a rewrite can never
   launder a malformed pattern; cache keys, bounds, and eviction are
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
  -> case-fold expansion when ci applies (budgeted; verbatim on exhaustion)
  -> compile expanded; on admission error with expansion applied, retry
     unexpanded and keep the base verdict
  -> cache install only after all steps succeed (unchanged contract)
  -> match with existing per-engine deadlines and budgets (unchanged)
```

### States, transitions, and unhappy paths

The change adds no long-lived state: fold tables are static, the folder is an
immutable `cases.Caser`, per-batch LIKE tokenization is discarded with the
batch, and the regexp caches pre-date this change with unchanged ownership.
Unhappy paths: invalid ESCAPE (existing error), malformed patterns (verdict
always from the source), budget exhaustion (verbatim fallback for LIKE mask
overflow via anchor search; verbatim pattern for fold-rewrite overflow;
existing bounded errors for the regexp2 budgets), context cancellation
(checked every 1024 steps/members), and cache eviction (subject-independent
metadata only, unchanged).

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
- Hot-path cost: unchanged fast paths for binary and `_bin` domains; ASCII
  general_ci fold is `bytes.ToUpper`; the weighted matcher runs only for the
  unicode_ci family; the fold rewrite runs once per compiled pattern inside
  the existing cache, bounded by the budgets above. No new allocation on
  constant-pattern rows beyond the prepared form.
- Observability: budget and admission failures surface as the existing
  bounded SQL errors; no new metric or log schema, and user patterns/subjects
  are not logged.

## Verification and open decisions

Acceptance mapping (all tests exist on the implementation head and pass in
CI on Ubuntu/x86 UT, coverage, and BVT compose/proxy jobs at head
`b554558f`):

| Invariant | Cheapest deterministic evidence |
| --- | --- |
| Collation-routed LIKE/REGEXP/FIND_IN_SET (1, 2) | `pkg/sql/plan/function` fold-mode and routing UTs; BVT `issue_31175_pattern_collation.sql` (410 statements, MySQL-oracle values) covering literals, columns, both operand orders, prepared parameters, user variables |
| Binary-mix rules (3) | `collation_binary_tie_test.go`, admission-matrix UTs, BVT `_binary`/HEX/BIT/CAST rows incl. error 3995 both orders |
| Per-character LIKE weights (4) | `pkg/common/collation/wildcard_test.go` (weights, malformed bytes, supplementary identity, sigma/dotless-i), `wildcard_segments_test.go` anchor fallback |
| Full-fold REGEXP (5, 6) | `regexp_casefold_internal_test.go`, `regexp_casefold_test.go` (tables, orbits, budgets, verbatim fallbacks, source-verdict preservation), `func_builtin_regexp_test.go` retry and error-shape tests |
| Bounded work (7) | cancellation and bounded-work UTs in both packages; budget-exhaustion tests asserting verbatim emission and retry-unexpanded |
| Plan/execution agreement (8) | `TestLikePrefixCaseInsensitiveKeepsFoldedPredicate` (pkg/tests/sqlintegration) proving scan, forced-index, and computed-subject paths return the folded rows; prefix fixtures pinned to `utf8mb4_bin` |
| Domain/coercibility preservation (9) | `pkg/sql/plan` and `rule` UTs for folded `CAST(x AS BINARY)`/`BINARY 'x'`/`binary()` stamping, negative-width casts, folded-domain preservation; `operator/mysql_compat_string_binary_comparison` BVT unchanged |
| Cache contract (10) | existing cache/eviction tests plus retry tests proving install only after success |

Drawbacks and accepted tradeoffs: the fold rewrite enlarges ci patterns
(bounded, verbatim above budgets with per-character-orbit semantics — an
accepted, test-pinned divergence from full folding for pathological runs);
the unicode_ci LIKE matcher is slower per row than the byte path (accepted:
correctness per the resolved collation; byte domains keep the fast path);
FIND_IN_SET keys value+sentinel per member comparison (needle prepared once
per row).

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

- Design revision under review: r1, 2026-11-08 (this document).
- Implementation review remains blocked until the review of this revision
  records a PASS or REQUEST_CHANGES decision bound to this document's SHA256;
  material implementation deviations require updating this document and
  re-reviewing the affected decisions.
