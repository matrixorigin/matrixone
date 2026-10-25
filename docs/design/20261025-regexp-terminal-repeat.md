# Classify terminal-any loops once at compilation

Status: **APPROVED amendment v2, 2026-10-25**. Actual model/effort:
**gpt-6.1-sol / xhigh**, explicitly selected `collaboration.spawn_agent` runtime;
session `/root/clickbench_followup_design_20261025`. Final independent review
remains **gpt-6-astra / xhigh** under the user's override.

This amends the execution-placement restriction in the v1 design committed at
[b59e94775997](https://github.com/matrixorigin/matrixone/blob/b59e94775997700e405df5072392902de54f367c/docs/design/20261025-regexp-terminal-repeat.md), based on production main
`69bbcae88174a017ffa1a8c9f60e8e307d6c41cf`. Reviewed worktree:
`/private/tmp/mo-clickbench-followup-20261025`, branch
`codex/regexp-terminal-repeat-20261025`, v1 implementation plus exit-first
runtime-classification experiment. Its `backtrack.go` SHA256 was
`3935e8d677e8c67bf06297b0ffb2592cd1dd7522f42f6f15ec93f60c0b220a01`;
`regexp.go` was unchanged, SHA256
`31f70f8b84237cdb84ebfbbfec251743bcf3cadc52bc036bf6beeea02f7c5b71`.
Version this amendment before the amended production edits. This agent ran no
build/tests and changed only this external design artifact.

Tracking issue: [#27665](https://github.com/matrixorigin/matrixone/issues/27665).
Implementation: the PR containing this document.

## Reason and invariant

V1 removes real tail work and has preliminary 10M non-spill Q29 gain, but its
topology checks execute at every ordinary Alt. Astra found a repeatable generic
`a.*b` regression. Reordering checks does not close it: the fixed-iteration
child-CPU controls in `regex-v2-cpu-controls.json` still show nonterminal +5.3%
and multiline +7.8%, independently visible in wall time. Case-specific gain
does not justify taxing unrelated patterns. Correctness/race results do not
answer this performance defect.

Keep v1's exact language/capture/line-policy/offset/output invariants and
monotone `failedNL` proof. Add this invariant: an ineligible instruction follows
the original ordinary Alt path with **no terminal-loop classification, metadata
lookup, added load or guard in that path**. Classify immutable program topology
at its first owner, once before the compiled Regexp is published.

## Selected change and complete consumer closure

Use the existing `syntax.InstAltMatch` optimization tag in the existing program;
do not add a Regexp flag, per-PC side table, opcode or duplicate VM.

1. After ordinary compilation has completed one-pass construction, start/prefix
   information, bitstate-size limit and other metadata, but before returning the
   Regexp, run one bounded program walk **only if onepass is nil and the existing
   maxBitStateLen is positive**. Thus only programs eligible for the current
   bounded bitstate executor are classified.
2. Mark only original InstAlt instructions satisfying the unchanged v1 proof:
   greedy Out is InstRuneAny/InstRuneAnyNotNL, that rune's Out returns to this
   PC, and Arg is exactly EmptyWidth(EmptyEndText) followed directly by Match.
   Change only that Alt's Op to InstAltMatch. Preserve PCs, Out/Arg edges, rune
   data, capture counts, instruction count and every other instruction.
3. Restore the ordinary bitstate InstAlt body to its original code. Move the
   terminal operation to the existing InstAltMatch owner. Discriminate its new
   compiler-marked EndText variant there; keep the existing upstream variant's
   handling unchanged. Trust the compiler's complete eligibility proof rather
   than rechecking it at execution. A cheap variant discriminator is paid only
   by InstAltMatch, never by ordinary Alt.
4. Keep LF witness/scan execution in one small private bitState helper called
   only for the marked variant. It retains existing no-allocation input access,
   failure/job restoration and normal EndText/Match publication. No inlining
   directives are required by design; measure code-layout effects instead of
   assuming source restoration alone proves equal runtime cost.

This is a local bytecode annotation, not a new language primitive. Existing NFA
`machine.add` handles InstAlt and InstAltMatch in the **same switch case**, with
the same ordered Out then Arg expansion, so long inputs and RuneReader preserve
the original program language, capture priority and bounds. The bytecode has
identical edges and no new epsilon transitions. The one-pass compiler makes a
separate onePassProg before annotation; programs with a non-nil one-pass result
are never annotated. Its specialized InstAltMatch meaning is therefore untouched.

All consumers were mapped: bitstate push/visited indexes use unchanged PCs;
backtrack executes the tagged variant; NFA consumes the same ordered graph;
one-pass is excluded; prefix/start/capacity metadata is already derived;
remaining Regexp methods use unchanged counts/source text. CompileWithLineMode
publishes its existing immutable policy afterward. Copy may share the immutable
program as before. No execution mutates the program or carries witness state in
the Regexp, so cached/concurrent reuse remains safe.

The existing v1 witness still resets to -1 once per complete bitstate search.
Failed LF scans start after the previous witness; successful EndText/Match at EOF
ends even Longest execution. No topology scan, full-subject pre-scan, extra row
allocation or change to error/NULL/replacement/output admission is introduced.

## Cost, ownership and alternatives

The compiler owns the program until publication; annotation mutates its existing
instruction storage in O(P) work, bounded by the executor's existing maximum
program size (currently 500), once per successful compile/cache miss. It adds
zero retained bytes, slices, maps, keys or allocations. The existing combined
100-entry regexp cache and its eviction/ownership stay unchanged. Per-input
failedNL remains the one scalar already approved in v1, owned/reset by bitState.
No SQL, wire, persistence, configuration, spill or account contract changes.
Rollback is a revert. Document this local compiler/tag delta in the README and
versioned design; retain upstream provenance hashes and BSD headers. A future
Go update must reconcile **both** this producer and all tagged-op consumers.

| Alternative | Decision |
|---|---|
| Regexp eligibility bool or one target PC | Still loads/tests metadata at every ordinary Alt, and a PC restricts multiple eligible loops. Does not structurally close the measured defect. |
| Dense/sparse per-PC metadata | Extra retained state/allocation and a lookup/guard on the ordinary path for information already representable by its instruction Op. |
| New opcode | Requires new consumer cases where existing AltMatch already provides the dispatch boundary; avoid duplicate protocol. |
| Duplicate optimized/unoptimized VM | Avoids checks but duplicates matcher semantics and upstream maintenance. Disproportionate and not approved. |
| Existing AltMatch annotation | Selected: first-owner classification, unchanged ordinary dispatch, no added state, explicit executor closure. |

## Validation and acceptance of the amendment

Reuse valid v1 semantics evidence, but the new producer/consumer path needs the
following discriminating checks. No extra service/profile discovery is needed.

| Risk | Required evidence |
|---|---|
| Wrong annotation | Typed program checks: exact eligible loops tagged; lazy/captured/nonterminal/multiline/other-assertion controls unchanged; mixed program has tagged and ordinary loops; graph/counts/PCs unchanged. One-pass and too-large programs stay unannotated. |
| Executor semantic divergence | Existing default/Longest/POSIX/capture differential tests, bitstate/NFA threshold and RuneReader controls **on a tagged program**, wide modes and binary/offset tests. NFA is an independent unchanged execution oracle for the annotation's optimized bitstate behavior. |
| Immutability/lifetime | Existing copy/cache/concurrent reuse and failedNL reset controls; focused race mode. Verify program annotation occurs only before publication. No metadata accounting test is needed because no allocation/owner is added. |
| Generic performance regression | First rerun fixed-iteration child-CPU plus wall ABBA for nonterminal, multiline, captured/lazy tails, and an ordinary loop in a program that also contains an eligible tail. Require no repeatable regression outside measurement noise. Source-path restoration alone is insufficient. |
| Preserved leverage | Eligible/dotall/short/prefix-failure/early-LF/failed-start micro controls, then fresh 10M non-spill public Q29 and neighboring controls with exact results and host/memory/spill records. V1 public timings do not prove amended performance. |

Parent implements and measures the generic controls **before** spending on the
public 10M round. A repeatable generic regression remains a blocker even with a
large Q29 gain. If code layout still matters, adjust only the marked-case/helper
placement and measure; do not introduce a parallel VM or unrelated optimization.
Refresh affected private-matcher/function normal/race and incremental static
scope after the final diff, reusing unchanged valid evidence explicitly. Feed
this amendment and new results to the active Astra review.

Decision: **PASS for this exact annotation/dispatch amendment**. V1's runtime
classification placement is superseded; its eligibility, semantics, resource,
SQL compatibility and 10M non-spill public-gain gates remain mandatory.
