# REGEXP ICU-compatibility execution path

Status: implementation review

Owning issue: [#28305](https://github.com/matrixorigin/matrixone/issues/28305)

Implementation: [#28862](https://github.com/matrixorigin/matrixone/pull/28862)

Owner: XuPeng-SH

## Problem and compatibility boundary

MatrixOne's existing regexp implementation uses Go RE2. RE2 deliberately
rejects backtracking constructs, while MySQL 8.4 exposes ICU-compatible
regular-expression behavior. Patterns containing lookaround, backreferences,
atomic or possessive groups, `\X`, quoted literals, and related syntax can
therefore fail during compilation or diverge from the MySQL contract. The gap
affects `REGEXP`, `REGEXP_LIKE`, `REGEXP_INSTR`, `REGEXP_SUBSTR`, and
`REGEXP_REPLACE`, including the negated predicate and capture replacement.

The change adds an exceptional compatibility path of about 2.8K production
lines and the `regexp2` and `uax29` dependencies. Syntax-aware admission sends
only patterns requiring an unsupported RE2 construct to that path. Ordinary
patterns retain the existing RE2 implementation and its linear-time behavior.

This document describes the grammar and execution semantics implemented by
this change. It is not a promise of complete ICU, locale, or collation
equivalence. The existing `regexp_string_domain_and_position_semantics.md`
remains the source of truth for string-domain and cross-function position
semantics.

## Goals and non-goals

Goals:

- Support the admitted ICU-visible constructs used by the compatibility tests:
  lookahead/lookbehind, numeric and named backreferences, atomic groups,
  possessive quantifiers, Unicode escapes, and extended grapheme matching
  through `\X`. Quoted literals and free-spacing options are preserved when
  they occur in a pattern admitted for another unsupported construct; they are
  not independently claimed as a new engine route (see the matrix below).
- Preserve NULL propagation, selected-row masks, validation ordering,
  occurrence/position handling, byte/text offsets, capture replacement, and
  bounded replacement output.
- Keep RE2 as the default path for patterns that do not require compatibility
  behavior.
- Bound pattern, translation, subject, compile, match, cache, and aggregate
  temporary-memory costs before the backtracking engine runs.
- Ensure that cached entries never retain a SQL subject and that every
  per-evaluation matcher, input, and runner reference is released on success
  and every error path.

Non-goals:

- Replacing RE2 or routing every regexp call through a backtracking engine.
- Translating arbitrary ICU grammar or promising locale-sensitive collation
  and Unicode-property behavior outside the admitted rules.
- Adding persistent/catalog formats, wire changes, transactions, goroutines,
  retry loops, or a new SQL-context cancellation contract.
- Falling back to another engine after an admitted compatibility evaluation
  has started.

### Syntax and routing matrix

The contract is defined by the syntax scanner, not merely by whether regexp2
could parse a form in isolation:

| Syntax | Route | Contract / coverage |
| --- | --- | --- |
| Lookaround, numeric/named backreference, atomic group, possessive quantifier, `\X`, `\U`, `\c` | compatibility path | translated and exercised by function/resource tests |
| `(?x)` / `(?ix:...)` | RE2 unless another row admits the pattern | free-spacing state is preserved by the compatibility translator; standalone `(?x)` remains outside this change because the scanner does not route it |
| `\Q...\E` without another admitted construct | RE2 | Go RE2 already accepts quoted literals; the compatibility path preserves them when combined with an admitted construct |
| Named capture declaration `(?<name>...)` | compatibility path | routed so named-capture numbering/backreference mapping is preserved |
| Escaped lookaround/backreference/possessive text inside a quoted literal/class | ordinary surrounding route | must not cause false admission; quote/class cases are tested |
| `\U` / `\c` inside a character class | compatibility path | explicit code-point/control escape is translated and covered by class tests |
| Binary Unicode property/code-point escape | compatibility validation | rejected before the private-use byte alphabet can be addressed |

The compatibility oracle for this issue is the MySQL 8.0.45 behavior recorded
by the issue tests. The implementation versions are `regexp2` v1.10.0 and
`uax29` v2.7.0 (Unicode 17 data). Unicode-version differences are therefore
an explicit compatibility boundary for `\X`, not an implicit claim that every
ICU release has identical grapheme tables.

## Invariants

1. **Stable engine selection.** The syntax scanner ignores escaped text,
   quoted literals, ordinary character-class contents, and free-spacing
   comments when deciding whether the compatibility path is required. Explicit
   `\U`/`\c` escapes in a class and named-capture declarations remain the
   exceptions defined by the routing matrix. A pattern remains on RE2 or is
   admitted to the bounded compatibility path; it is not silently retried
   through a different engine.
2. **Function-scoped position semantics.** `SUBSTR` and `REPLACE` retain the
   full subject so anchors and boundaries use the original subject. `INSTR`
   deliberately searches the suffix beginning at `pos`. SQL positions are
   one-based; text offsets count UTF-8 code points and binary offsets count
   bytes. The linked string-domain design governs binary/text selection.
3. **Capture identity.** Source capture numbering and names are mapped to the
   translated engine numbering before replacement templates are evaluated.
   `$0`, numeric captures, and named captures cannot observe implementation
   captures introduced by translation.
4. **Binary isolation.** Binary matching maps source bytes to a private-use
   alphabet and rejects Unicode property or code-point escapes that could
   address that alphabet. Explicit binary case-insensitive matching uses the
   existing Windows-1252 facade.
5. **Bounded backtracking.** Finite timeouts and static code, repetition,
   state, and memory checks are required even for a valid pattern. A timeout
   or budget failure returns a bounded MatrixOne error and never publishes a
   partial SQL result.
6. **Failure does not corrupt operator state.** A failed compile or evaluation
   does not install a partial cache entry or mutate the reusable plan/input.
   Cache eviction only removes subject-independent metadata and updates byte
   accounting.
7. **Idempotent cleanup.** The evaluation matcher clears regexp2's retained
   runner input; the evaluation and input budgets are released on all returns.
   Repeated cleanup is harmless. The shared cache contains no active runner.
8. **Concurrency is bounded.** regexp2 runner state is protected by the
   matcher mutex and aggregate temporary memory by an atomic admission
   counter. The application path creates no goroutine or wait-for relationship;
   regexp2's timeout dependency may start its own shared fast-clock timer
   goroutine while timeout users are active and stops it when idle. That
   dependency-owned lifecycle is not an operator-owned worker or request wait.

## Ownership and data flow

The execution path has four layers:

1. `func_builtin_regexp.go` is the public dispatch layer. Each regexp
   entrypoint keeps existing validation and domain selection, then chooses the
   compatibility path only when `requiresRegexp2Pattern` detects an admitted
   construct. Other calls use the existing RE2 matcher/cache.
2. `regexp_icu.go` owns admission, translation, compilation, cache accounting,
   binary encoding, matching, deadlines, and capture-index mapping. The cache
   key includes pattern, match type, binary mode, and binary case-fold mode, so
   incompatible execution modes cannot share a compiled matcher.
3. `regexp_replacement.go` owns replacement-template parsing and output-size
   accounting independently of the engine. RE2 and regexp2 use the same
   capture contract and maximum result bound.
4. Unit, resource-boundary, race, and distributed tests verify public behavior
   and cleanup. Tests do not bypass production admission limits.

Per-call transitions:

```text
pattern -> syntax admission
  -> ordinary RE2 matcher/cache
  -> compatibility cache lookup
       -> hit: subject-independent metadata
       -> miss: validate -> translate -> parse/code budget -> capture map
                   -> install only after all steps succeed
  -> subject validation and per-subject \X expansion when needed
  -> per-evaluation regexp + rune/byte offset table
  -> mutex-protected matching with occurrence/position deadline
  -> result or bounded error
  -> clear runner input -> release evaluation/input budgets
```

For `\X`, exact UAX #29 grapheme boundaries are derived from the current
subject and expanded per evaluation. The subject-specific expression is never
cached, preventing incorrect reuse and subject retention through runner state.

## Resource and execution budgets

These limits are independent so a large ordinary RE2 value is not subjected to
backtracking limits unless it selects the compatibility path.

| Resource | Limit | Enforcement |
| --- | ---: | --- |
| Source pattern | 1 MiB | compatibility admission |
| Translated pattern | 1 MiB | translation and binary encoding |
| Subject | 1 MiB | per-call validation |
| Subject containing `\X` | 64 KiB | grapheme expansion |
| Compatibility cache | existing 100-entry cache | `regexpSet` eviction |
| Compatibility cache estimate | 32 MiB | `regexpSet.icuBytes` |
| One evaluation estimate | 8 MiB | code, runner, captures, repeats |
| Local active subject/compile estimate | 64 MiB per process-wide counter | atomic admission counter; covers `acquireRegexp2SubjectBudget` reservations only |
| Backtracking states | 1 Mi states | code/subject budget |
| regexp2 match timeout | 1 s | `MatchTimeout` |
| Iteration deadline | 2 s | occurrence/replacement loops |
| Replacement result | `types.MaxBlobLen` | shared size checks |

Track count, repeat count, nullable-repeat count, and capture-history estimates
are also bounded. The 64 MiB counter is not a CN-wide memory quota. It covers
the reservations made by `regexp2PrepareExpression` and `newRegexp2Input`,
including their estimated runner/compile allowance, and is released by their
cleanup closures. It does not account for the per-operator 32 MiB cache
estimate, subject-specific `\X` translation that happens before a compile
reservation, regexp2 implementation overhead beyond the estimate, or the
local replacement `strings.Builder`. Those phases still have independent
source/translated-pattern, subject, and `types.MaxBlobLen` bounds; their
allocations are not represented by the 64 MiB number. The estimates are
admission safeguards rather than a proof of exact allocator usage. Rejecting a
pattern at admission is preferable to allowing user input to consume unbounded
CN CPU or memory.

The accounting boundary is explicit:

| Phase | Counter/accounting | Lifetime |
| --- | --- | --- |
| Parse/write and initial compile | `acquireRegexp2SubjectBudget(0, compileBudget)` | released by the parse/write cleanup closure |
| Per-subject regexp and input | `acquireRegexp2SubjectBudget(len(subject), evaluationBytes)` | released by `regexp2Input.release` and evaluation-matcher cleanup |
| Subject-specific `\X` expansion | bounded by the 64 KiB subject and 1 MiB translated-expression limits, but outside the counter | local builder lifetime only |
| Cache metadata | per-`regexpSet` 100-entry/32 MiB estimate, outside the process-wide counter | until eviction/operator release |
| Replacement output | `types.MaxBlobLen`, outside the counter | local result-builder lifetime only |

This is a set of local admission contracts, not total allocator accounting.
The concurrent test must hold one large reservation while a second reservation
attempts admission, assert deterministic rejection, then release and assert
recovery. A benchmark must separately report ordinary RE2 and compatibility
path `ns/op`, `allocs/op`, and `B/op` for fixed pattern/subject,
multi-occurrence, `\X`, and two-pass replacement cases; no ordinary RE2 call
may populate the ICU cache or leave active-budget bytes behind.

## Error, cancellation, retry, and restart behavior

- A present pattern is validated before NULL or range shortcuts, so malformed
  compatibility syntax is not hidden by a NULL subject. Compile and
  translation errors use the existing function-level boundary; runtime errors
  become bounded MatrixOne diagnostics and do not include subject text.
- regexp2 has a one-second timeout for each engine match. The occurrence and
  replacement helpers create a two-second wall-clock deadline before their
  setup, check it before each occurrence, and check it before each replacement
  pass. A simple predicate uses the one-second engine timeout but no separate
  two-second outer deadline. Compile/translation and cleanup are included in
  the helper's elapsed wall-clock interval only to the extent that the
  deadline checks bound them; slow dependency cleanup can exceed it. SQL
  context cancellation is explicitly not claimed because the current
  entrypoint does not pass a context into regexp2; that is a separate design
  decision.
- Failed matches, size checks, and callbacks unwind through deferred cleanup.
  Replacement output is local and discarded on failure; partial output is
  never published.
- A failed compile does not install a partial cache entry. An evaluation
  failure after a successful cache miss may retain the completed,
  subject-independent cache entry and may have already evicted another entry
  to satisfy the cache bound; it never retains the subject or corrupts the
  reusable plan/input. There is no retry protocol, persistent state, or
  restart migration. Cached metadata may be evicted without restarting a
  session.

The time model is cooperative rather than an end-to-end hard deadline:

| Phase | Clock / check point | Uncovered work |
| --- | --- | --- |
| Public validation, cache lookup, and cache-miss translation/compile | source, translated-expression, code, and memory admission; the outer iteration deadline is created only after the public compatibility matcher is obtained | no wall-clock deadline for initial admission/compile |
| Per-subject `forSubject` and input construction | occurrence/replacement helpers carry the deadline created at helper entry; memory admission remains the primary bound | dependency compile work is not interrupted by SQL context |
| One regexp2 engine match | regexp2 `MatchTimeout` is one second | timeout is coarse-grained and dependency-owned |
| Occurrence/replacement iteration | checks wall clock before each next match; replacement performs a second bounded pass when needed | the final match and callback are not followed by another deadline check |
| Callback, output builder, and cleanup | output is bounded by `types.MaxBlobLen`; deferred cleanup always runs | no hard timeout check is made inside callback, builder, or dependency cleanup |

The two-second value is therefore a cooperative iteration budget, not a claim
that every public call returns within two seconds. The one-second engine
timeout and the documented admission limits are the actual safeguards for a
single predicate; cleanup may take a small dependency-defined amount of time.

## Alternatives considered

### Continue rejecting unsupported syntax

This preserves linear-time execution but leaves the MySQL compatibility issue
unresolved. Rejected.

### Rewrite unsupported syntax into RE2 text

Textual rewrites cannot generally preserve lookaround boundaries, backreference
identity, possessive/atomic commitment, or subject-specific grapheme
boundaries. A partial rewrite would silently return wrong results. Rejected.

### Route every pattern through regexp2

This would impose backtracking allocation and timeout overhead on the hot path,
change existing ordinary-pattern behavior, and apply the expensive budget to
values RE2 already handles linearly. Rejected.

### Keep RE2 and use regexp2 only after admission

Selected. It isolates compatibility risk, retains the fast path, and makes the
expensive behavior explicit and enforceable at one boundary.

### Link ICU directly through CGo

Direct ICU would track the reference engine more closely, but it would add
platform-specific native dependencies to this pure-Go function path and
complicate the existing Linux/macOS/arm64 build matrix. Rejected for this
increment; it remains a separate architecture option if complete ICU parity
becomes a product requirement.

## Compatibility, rollout, and observability

The change affects SQL regexp execution and build dependencies only. It adds no
catalog, persistent-data, wire, or transaction changes. Existing ordinary
patterns retain RE2. Rollback is a source-level removal of the compatibility
branch and dependencies; no data migration is needed. If a construct proves
unsafe, its admission can be removed without changing the ordinary path.

The rollout rule is to upgrade every CN that may receive a workload before
enabling that workload's admitted ICU syntax. During a mixed-version window,
clients must use RE2-safe patterns only; the same advanced pattern must not be
sent to a session whose execution may land on an older CN. The implementation
owner, XuPeng-SH, records the canary BVT result on this PR; the release operator
owns the all-CN version check. Before rollback, drain or stop sessions and
prepared statements using admitted ICU syntax, then verify the canary is back
to RE2-safe SQL before reverting binaries. Because no pattern metadata is
persisted by this change, rollback needs no data migration, but session state
must still be drained/reprepared.

Budget, timeout, and invalid-pattern failures surface as bounded SQL errors.
No new metric or log schema is introduced; future observability must not log
user patterns or subjects and should be a separately reviewed change.

## Validation and acceptance

Acceptance on the implementation head requires:

- focused and full `pkg/sql/plan/function` tests for every public entrypoint,
  NULL/mask behavior, positions, captures, replacement, binary mode, and
  `\X`;
- resource-boundary tests for compile rejection, a past-deadline iteration,
  callback-error cleanup, cache eviction, repeated release, and admission
  exhaustion/recovery under concurrent attempts; each test records the
  `regexp2ActiveSubjectBytes` oracle before and after the failure;
- race validation for resource admission and the complete function package;
- distributed/JDBC BVT coverage for supported ICU constructs;
- `git diff --check`, SCA/static analysis, and the normal MatrixOne CI gates.

The evidence map is:

| Requirement | Test / harness | Oracle |
| --- | --- | --- |
| Routing and entry-point semantics | `TestRegexp2PatternRouting`, `TestRegexp2PatternCompatibilityFunctions`, `TestRegexp2PatternFunctionsPreserveEntryPointSemantics`, `TestRegexp2OrdinaryPathBypassesCompatibilityState`, and `regexp_icu_coverage_test.go` | exact boolean/index/string/NULL/mask results; class/quote routes match the matrix and ordinary calls leave ICU state unchanged |
| Lifecycle failure cleanup | `TestRegexp2EvaluationCleanupOnDeadlineAndCallbackError`, `TestRegexp2AdmissionExhaustionAndRecovery`, `TestRegexp2ResourceAndOffsetContracts` | callback is not called after a past deadline; callback/deadline/admission failures restore `regexp2ActiveSubjectBytes`; admission succeeds after release |
| Race and package correctness | `go test -race -count=1 ./pkg/sql/plan/function` with native CGo flags | pass with no race, panic, or leaked active budget |
| Cost model | `BenchmarkRegexp2CompatibilityPaths` via `go test -run '^$' -bench BenchmarkRegexp2CompatibilityPaths -benchmem -count=5 ./pkg/sql/plan/function` | report `ns/op`, `allocs/op`, and `B/op` for RE2, regexp2, `\\X`, and two-pass replacement; all five samples must complete, `ns/op` max/min must be at most 1.25, and non-zero `B/op`/`allocs/op` must vary by at most 1% (zero remains exact) |
| End-to-end compatibility | `test/distributed/cases/function/func_regular_icu.test` through the JDBC/BVT harness | output remains equal to the checked-in result and the canary runs on every target CN |

Absolute benchmark numbers are host-dependent and are recorded as evidence,
not treated as a universal latency promise. The required performance property
is bounded, explainable cost on the exceptional path and unchanged routing of
ordinary RE2 patterns. The benchmark fixture uses an independent `regexpSet`
per sub-benchmark; the ordinary-path state oracle is a separate unit test, so
benchmark ordering cannot hide ICU-cache or active-budget changes. Local full
function UT, targeted race tests, and the five-run benchmark pass those
stability rules; the remote CI/BVT result remains implementation acceptance
evidence to be attached to the PR. The merge-base `c61ed374a8a6` does not have
this new compatibility benchmark, so this PR does not claim a cross-commit
absolute performance regression number. A future before/after study must
apply the identical test-only fixture to both recorded source SHAs and use
`benchstat` on the same host.

The resource tests now cover error conversion, a real past-deadline iteration,
callback-error cleanup, repeated cleanup, and deterministic admission
exhaustion/recovery. The past-deadline case proves iteration-deadline cleanup;
it is intentionally not described as proof of the regexp2 engine's own
one-second timeout, which remains dependency behavior.

The pull request records the exact implementation commit and the design-first
review decision. Implementation review is blocked until that record is
present.

## Design-first review record

This change triggers the design gate because it introduces a large production
compatibility layer, a backtracking hot path, a long-lived compiled-pattern
cache, and explicit resource admission. The PR review comment must identify
the exact commit containing this file, the reviewer/model, and a PASS or
REQUEST_CHANGES decision before implementation is considered ready.
