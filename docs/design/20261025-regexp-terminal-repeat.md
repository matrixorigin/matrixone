# Terminal repeated-any loop execution

Status: **APPROVED for implementation, v1, 2026-10-25**; empirical acceptance
remains mandatory. Gate: actual **gpt-6.1-sol / xhigh**, explicitly selected via
`collaboration.spawn_agent`; session `/root/clickbench_followup_design_20261025`.
Reviewed production: `69bbcae88174a017ffa1a8c9f60e8e307d6c41cf`, clean branch
`codex/regexp-terminal-repeat-20261025` in
`/private/tmp/mo-clickbench-followup-20261025`. Tracking issue: [#27665](https://github.com/matrixorigin/matrixone/issues/27665).
Implementation: the PR containing this document. Final gate:
**gpt-6-astra / xhigh**, user override. Rejected mixed-copy work stays closed.

The shared matcher hot path/complexity contract triggers mo-dev's design gate.
Version this approved content under `docs/design/` before production edits and
link its exact revision from the implementation PR. This agent inspected source
and existing artifacts only; no production/test edits or build/test execution.

## Evidence and selected owner

The user requires 10M non-spill performance, prioritized by MatrixOne/local-source
DuckDB gaps. The existing official dispersed 10M/18-column screen selected Q29
by its largest absolute gap; its initial 5.219/0.753-second ratio was compiler
contaminated and is **not** an accepted stable comparison. A fresh MO baseline
is 2.194 seconds median, five runs 2.07–2.37. DuckDB's profiled execution reports
zero temp/spill and 1.298 seconds with profiling; that time is not interchangeable
with its screen median. Final paired measurements must establish the real gap.
Q29's ten keys/counts/MIN match DuckDB exactly; AVG matches at MO's four decimals.

Profile (`profile10m-main-q29-cpu.pprof`) proves replacement 53.55% cumulative and bitstate
37.63%. `tryBacktrack` is 11.18/32.85 sampled seconds (34.03%); terminal AnyNotNL
step/check alone is at least 1.99 seconds (6.1%), with shared Alt/dispatch/visited
cost in addition. Host-capture Rune step/match is 0.39/1.11 seconds. The saved
`regexp-program.txt` proves the terminal loop below. This is meaningful direct
work removal to prototype, not an elapsed-speedup promise. Screen/ranking,
profile/source listings and corrected result records remain the evidence.

## Contract and minimal algorithm

The existing private pinned-Go matcher owns execution. Preserve default matches,
captures/priorities, original-context offsets, SQL binary encoding, wide-line
assertions and linear per-search complexity. SQL mask/NULL/error order, template
parsing, occurrence iteration, output bounds and result ownership remain with
their existing owners. No query/schema/pattern/replacement literals are guards.

Add a shortcut at the existing bitstate `InstAlt` boundary for exactly this
compiled topology, using trusted compiler-produced instruction references:

1. Greedy Alt.Out is InstRuneAny or InstRuneAnyNotNL, whose Out is that same Alt.
2. Alt.Arg is exactly InstEmptyWidth(EmptyEndText), whose Out is InstMatch.
3. No capture, Nop, other assertion or branch intervenes in either path.

Apply it on the initial `arg=false` greedy-arm visit; retain the existing
`arg=true` deferred-exit continuation unchanged.

String/byte bitstate only; reader, NFA/one-pass dispatch and implementations
remain unchanged. Do not mutate the program, add an opcode/compiler pass or
reinterpret existing InstAltMatch. Lazy loops, captured bodies, other repeated
classes, multiline-dollar exits and equivalent-but-more-complex shapes fall
back to the current VM.

Any can jump immediately to the existing EndText instruction at `b.end`.
AnyNotNL can do so iff its suffix has no LF byte. Add one integer `failedNL` to
pooled bitState, reset to -1 for **every complete bitstate search**. At an eligible
AnyNotNL loop: if pos <= failedNL, fail only this current branch through the
existing job loop; otherwise strings/bytes IndexByte the suffix for its first
LF. On LF, store its absolute byte index and fail the branch; without LF, jump
to EndText at EOF. Preserve all pre-existing outer jobs/capture restoration.
Use cached string/byte input without allocation or an input-interface expansion.
No whole-input pre-scan, AST retention, compiled-pattern cache or new SQL path.

Proof of semantics: the loop has no observable internal captures, and EOF is its
unique accepting exit. Smaller exits all fail EndText. Jumping therefore retains
outer capture choices, unset groups, priorities and offsets. Execute the normal
EndText/Match publication rather than synthesizing a match. Match explicitly
returns at pos == b.end even under Longest, after recording captures; ncap == 0
also returns. Thus a successful scan ends this search and needs no Longest guard.
An LF witness proves failure for every position at/before that byte, independent
of PC; returning to the existing job loop preserves other possible matches.

Proof of work bound: every failed scan starts strictly after the previous
witnessed LF and stops at a later LF, so scanned intervals are disjoint across
all tryBacktrack invocations/unanchored starts. The one successful scan ends the
search. Added scanning is O(input bytes) total, O(1) scratch, zero allocation;
resetting the witness per tryBacktrack or uncached repeated scans is forbidden.
Early LF stops immediately, avoiding an unrelated long tail. UTF-8 LF is exactly
byte 0x0A, including invalid UTF-8 decoding; every other decoded rune is accepted
by AnyNotNL. CR/NEL/LS/PS still obey ordinary consuming-dot semantics in all line
modes. EndLine is ineligible, preserving scoped/wide assertion policy.

The existing pool owns/recycles the one input-local scalar and clears subject
references as before; no new retained collection, resource owner, account or
lifetime. SQL/storage/spill/wire/configuration/mixed-version contracts do not
change; rollback is a revert. Document the local delta in the private-package
README; preserve BSD headers and the hashes identifying pinned upstream source.

## Alternatives and verification

| Alternative | Decision |
|---|---|
| Constant replacement-template reuse | Parsing is not the primary sampled cost; adds template/key/error-order ownership. DuckDB still validates rewrite strings per row. Do not bundle. |
| Force one-pass/NFA | One-pass ambiguity constraints preserve captures; forcing it is unsafe, while NFA retains per-rune work. No dispatch-change evidence. |
| Whole-input last-LF scan | Linear but can scan a huge unrelated tail after an early LF. The monotone witness is equally bounded and stops early. |
| General repeat optimizer/native RE2 | Unnecessary proof/maintenance/lifetime surface; the current matcher owner can remove the measured work locally. |
| Q5 hash work | Smaller screened absolute gap; quadratic pending-hash premise disproved by bounded 512-entry scratch. Separate future target. |

Reuse the current private matcher and function fixtures, extending tables for
distinct missing oracles. No new embedded service/framework or timing assertions
in UT. No new BVT is required solely for unchanged internal semantics; execute
existing regex public coverage plus the small integration controls below.

| Risk | Cheapest required evidence |
|---|---|
| Eligibility/capture priority | Compiled topology controls plus default differential Match/Find/Submatch/All/Replace against pinned stdlib. Generic eligible families beyond URL patterns, prefix alternatives/captures, unset groups, empty tail, Longest/POSIX; lazy/captured-loop/multiline-dollar exclusions. |
| Newline/rune/policy drift | LF first/middle/last, CR/CRLF/NEL/LS/PS, multibyte and invalid UTF-8, string/bytes, all line modes and scoped dotall/multiline. Include original-context nonzero offsets. |
| Complexity or stale witness | Ambiguous/unanchored prefixes and multiple LF-separated segments, failed branch then other match, reset with another input and concurrent/pool reuse. White-box proof of witness monotonicity/reset; scaling benchmarks challenge quadratic work without UT wall-time assertions. |
| Executor boundary | Bitstate just below and unchanged NFA just above its actual threshold, plus one-pass/RuneReader controls, using existing all-executor tests. |
| SQL/output boundary | Small capture-replacement/occurrence/start cases and binary NUL/invalid UTF-8/folding; reuse NULL/mask/invalid-pattern/template precedence and exact output-limit tests. No selected match must still avoid invalid-template evaluation. |
| Resource/spill | No new matcher allocations; retained output/account bounds unchanged. Reuse unchanged generic Group invariants and a small regex-group spill parity control if needed; spill timing is not performance acceptance. |
| Leverage/regressions | Matcher/vector-regexp benchmarks: eligible tails, short/failing prefixes, early LF plus long tail, ambiguous LF retries, dotall, excluded cases. Accepted public proof is 10M non-spill Q29 plus neighboring screened controls. |

Require reproducible 10M non-spill end-to-end Q29 gain outside paired-run spread,
identical baseline/candidate results and no repeatable neighboring regression.
Use bounded warmed paired/ABBA runs, identical source/data/config, no positive
SpillRows, memory records and host-load samples. Four workers/4 GiB engine or
statement budgets remain; Go's soft limit is not an equivalent process cap.
Micro-only gains and contaminated ratios do not pass. Do not rescue failed
acceptance by expanding into caching, growth or hash work.

Parent owns final incremental static scope, focused/owning normal and affected
race checks, public evidence and Astra/xhigh review. Expected owning production
package is internal/regexp; SQL function is a directly affected real consumer.
Material deviations, especially broader exit paths or new persistent metadata,
require design amendment. **Decision: PASS; no unresolved design blocker.**
