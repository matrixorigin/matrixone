---
name: mo-self-review
description: Pre-push self-review gate for MatrixOne changes — systematic first-principles review of the complete diff, including functional closure, unhappy paths, state/ownership models, wait-for dependencies, restart/reuse generations, and derived test matrices. Use before push/PR updates, when concurrency or lifecycle code changes, or when repeated review rounds reveal missed closure edges.
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  language: go
---

Compatibility: designed for Codex CLI and compatible agents. Requires a git working tree with a diff vs the base branch and the unhappy-path-audit skill (for Q1-Q3 depth).

## Developer authority and blocker calibration

These rules apply to Codex, Claude, and every other compatible reviewing agent.

- A developer's explicit, recorded decision is authoritative for the reviewed
  change: accept it, capture its rationale in the decision log, and do not
  re-raise it as a finding. Revisit it only when the developer asks, or when
  materially new evidence proves that the stated assumptions no longer hold;
  present that evidence as a question, not as an override.
- **Blocking** is exceptional. Mark a finding blocking only when it has a
  concrete, demonstrated path to a merge-bar failure: correctness breakage,
  data loss/corruption, security exposure, hang/deadlock, resource leak with
  material impact, incompatibility, or required validation that is genuinely
  missing. State the failing inputs/state and consequence.
- A plausible concern, preference, maintainability improvement, micro-nit,
  unproven risk, or item already accepted by the developer is **non-blocking**
  (or omitted). Do not turn every observation into a blocker: if everything is
  blocking, the label has lost its meaning.
- When uncertain, classify non-blocking and ask for the developer's decision;
  never use a blocking label merely to force attention or another review round.

## Resource Map

| Change shape | Read |
|---|---|
| Shared state, cancellation, close/terminal paths, callbacks, retry/restart, pooling/reuse, async cleanup | [references/concurrency-lifecycle.md](references/concurrency-lifecycle.md) |

## Running this skill IS the review (don't retype the long prompt)

Invoking this skill — `/mo-self-review [target]` or the Skill tool — **runs the
whole gate for you**. First resolve the **target** from the args:

| Args | Target to review |
|------|------------------|
| *(empty)* | current branch vs its verified PR base when one exists; otherwise vs a verified remote default branch (normally `origin/main`) |
| a git ref / branch / tag / commit (e.g. `develop`, `origin/release-2.0`, `abc123`) | current branch **vs that ref** |
| `vs <ref>` / `base=<ref>` | current branch **vs `<ref>`** |
| all-digits (e.g. `25199`) | that **GitHub PR** |
| anything else (e.g. `pkg/vectorindex focus on quantizer`) | scope/focus, using the same verified default-base resolution |
| `<ref> <scope…>` | **vs `<ref>`**, restricted to the trailing scope |
| `docs` / `help` | just show §1–§8, run nothing |

Resolve an explicit user base first, then a GitHub PR base for PR review, then a
fresh/verified remote-tracking default. Never silently substitute a stale local
`main`. If network access is unavailable, report the remote ref's object ID and
known freshness limitation. Always report the base ref, resolved commit, and
merge-base; do not guess `main` for a release-branch change.

Then execute (do not shortcut):

1. If a callable **code-review** workflow exists, launch it at **high** on the
   resolved target and state the base ("this branch compared to `<base>`"), appending:
   `多角度评审，第一性原则，系统性思考问题，涉及到的修改需要调研完整的功能闭环，unhappy path cover.`
   Pass scope/skip instructions through. If that capability is absent, execute
   §1–§5 directly with the available repository and review tools; capability
   absence must not block the gate.
2. On results, apply **§3** (trace each finding's functional closure to its terminal
   node; personally spot-check any *cluster of refutations* — a verifier can repeat
   one wrong call) and **§5** (developer decisions are authoritative; severity LAST,
   calibrated to the merge bar; decision-log every won't-fix/known-gap with its
   reason; no finding without a concrete failure).
3. Present a converged, ranked findings list, each with a **fix-or-decision-log
   recommendation** — not another review round.

§1–§8 below are the methodology this executes; consult them when applying the
discipline or running the gate manually. When this skill is surfaced only as
background reference (not explicitly invoked), treat §1–§8 as guidance — do **not**
auto-launch a workflow.

---

## Purpose — break the review → modify loop

The review→modify→review loop repeats because each review pass is *incremental*:
a new pass finds something the last one didn't (a fresh angle, a missed branch),
and re-flags items already decided "won't fix." This gate front-loads **one
exhaustive, calibrated self-review of your own diff** so the eventual PR review
(human or bot) has **nothing new to add** → the loop ends.

Run it on your working diff BEFORE `git push` / opening / updating a PR. It is a
*gate*: it either passes (§7) or produces a fix/decision list — not an endless
stream of nitpicks.

---

## Enforcement gate

| Gate | When | Action |
|------|------|--------|
| **G-SELF-REVIEW** | Before `git push`, before opening/updating a PR, or before declaring a change "done" | Run §1–§4 over the full diff, apply §5 convergence discipline, then check the §7 exit gate. Do not push until it passes. |
| **G-RACE-STRESS** | The diff changes concurrency, lifecycle, shared/global state, background work, synchronization, or behavior with a credible race/timing failure mode | Run a minimal, explicitly named behavioral set with an adaptive `-race -count=N` budget, then run each affected owning package once with `-race`. Ordinary sequential logic uses focused tests plus an ordinary owning-package run. §6 defines the proportional budget and narrow measurement-test exception. |

Scope = committed changes vs merge-base, staged changes, unstaged changes, and
untracked files in scope—not just the last file touched. Inspect `git status
--short` and `git ls-files --others --exclude-standard`; also verify that any
required generated/delivery artifact is not silently excluded by ignore rules.

---

## 1. Multi-angle (多角度) — run each lens as a separate pass

Do not spot-check. Sweep the whole diff once per lens; a defect invisible to one
lens is obvious to another.

| Lens | Ask |
|------|-----|
| **Correctness** | Does each changed function produce the right output for ordinary AND boundary inputs (0, 1, max, empty, nil, overflow)? |
| **Concurrency** | Shared state touched by >1 goroutine? Races, lost wakeups, double-close, ordering assumptions? Apply the mandatory race-stress gate in §6. |
| **Control path** | Can cancel/close/reject/timeout make progress independently of the blocked operation, or does it wait on the same lock/channel/RPC? |
| **State / generation** | Is every transition and failed transition defined? Can old work affect a restarted/reused generation or observe it before admission completes? |
| **Resource lifecycle** | Every fd/goroutine/lock/alloc created on the change's paths — closed/released on **every** branch incl. error/panic? (→ §4 Q1) |
| **Compatibility / boundary** | On-disk/wire format, config default, API signature, catalog metadata: does the change stay backward-compatible? New format opt-in, not a flipped default? Mismatch detected (fail-fast) not silently misread? |
| **Failure modes** | Every error return handled; partial failure leaves consistent state; no silent fallback that hides corruption. |
| **Contract / API** | Callers updated on both ends of a protocol change; interface impls complete (compile-time `var _` checks intact). |
| **Performance / scale** | On frequency-sensitive paths, did allocations, copies, scans, locks/atomics, goroutines, I/O, logs, or metric cardinality materially change? Benchmark/profile only when the cost can be material; otherwise record a bounded cost analysis. |
| **Platform / build** | Are Darwin/Linux, amd64/arm64, build tags, CGo/native loading, container users, and runtime-vs-build platform assumptions explicit and fail-closed where applicable? |

---

## 2. First principles (第一性原则)

**Prove it breaks — don't report "looks like it might."** For each candidate
defect, state a concrete failure: *inputs/state → wrong output / crash / hang*.
If you can't, exhaust every bypass path (defer, cancel watcher, timer, retry,
guard) before ruling — then drop it. This is the single biggest source of
review-loop noise: unproven "might" findings that get fixed, re-reviewed, and
spawn more "might" findings.

---

## 3. Complete functional closure (功能闭环)

For every change, trace the **entire loop it participates in**, not just the
edited line — most missed-in-review defects live one hop away, in the *other
half* of the closure. Trace to the terminal node.

| Change kind | Closure to walk end-to-end |
|-------------|----------------------------|
| storage / on-disk format | create → write → **read** → backup → **restore** → upgrade → restart |
| operator (colexec) | Prepare → Call → **Reset** → Cleanup (+ the error branch) |
| index / CDC | CREATE (+ InitSQL) → sync → query → reindex → DROP |
| resource handle | create → hand-off → … → **Destroy/Free/Close** (all holders) |
| config / flag | parse → default-fill → consume → the *other* backend/mode that shares it |
| state machine | states → events → ownership/linearization point → side effects → failed transition → retry/restart |
| control path | blocked work → cancel/close/reject → every lock/channel/RPC dependency → guaranteed local termination |
| reused object | old work stops → cleanup completes → sealed initialization → admission/publish → new generation |

Rule: if you changed one arc of a closure, open and read the arcs that *consume*
or *reverse* it (the reader for a writer, the restore for a backup, the Reset for
a Call). A change is not reviewed until its closure is closed.

---

## 4. Unhappy-path coverage

Run the **unhappy-path-audit** skill's Q1–Q3 over the resources/waits/growth the
diff touches:
- **Q1 leak/double cleanup** — every creation reaches one effective destruction owner (incl. transfers and error paths).
- **Q2 hung** — every explicit or implicit wait dependency has a guaranteed release; fail-fast/control paths must not queue behind the work they stop.
- **Q3 OOM** — every accumulation has a bound / recycle.

Apply its 5-gate false-positive filter (G1 full-graph, G2 can-fail/block,
G3 bound/release, G4 line-reread, G5 calibrate-last) before keeping any finding.

---

## 5. Convergence discipline — this is what actually breaks the loop

1. **Calibrate to the merge bar.** Blocking means a concrete, demonstrated
   merge-bar failure—not merely a concern. Flag only real defects
   (correctness, data loss, material leak, hang, incompatibility, security) or
   genuinely required missing validation; state the failure path. Style /
   micro-nits: fix silently or skip — never loop on them. When unsure, use
   non-blocking. (Assign severity LAST, per unhappy-path-audit G5.)
2. **Keep a decision log and respect it.** Record every intentional design
   choice and every "won't fix / acceptable" item (with the why). A developer's
   explicit decision closes that item for this review: do not re-raise or relabel
   it blocking unless materially new evidence invalidates its stated assumption.
   Re-reviews and PR reviewers re-surface these constantly; a written decision
   lets you dismiss them in one line instead of re-litigating.
3. **Verify before flagging.** No finding survives without a concrete failure
   (§2) that passed the 5 gates (§4).
4. **One thorough pass beats many incremental.** The whole point: exhaust §1–§4
   now so the next reviewer finds nothing. If you're tempted to "just fix this
   one and re-run," you're back in the loop — finish the sweep first.

---

## 6. How to run

**On your own working diff (the default — this is a *self* gate):**
- If a callable review workflow exists, use it for parallel discovery and then
  apply §3 closure + §5 discipline yourself.
- Otherwise walk §1 lens-by-lens → §3 closure → §4 Q1–Q3 → §5 directly. This is
  the complete supported fallback, not a degraded or blocked review.

For concurrency/lifecycle changes, build the invariant, transition table,
ownership graph, wait-for graph, and generation boundary from
[references/concurrency-lifecycle.md](references/concurrency-lifecycle.md). Derive
the test matrix from semantic axes; do not reuse a remembered case list.

### Proportional Go unit-test validation and race stress

For ordinary sequential changes, run the exact focused test(s) and the owning
package once in normal mode with `GOWORK=off`, `-mod=readonly`, `-count=1`, and a
bounded `-timeout`. Apply the adaptive race protocol below only when
**G-RACE-STRESS** is triggered:

1. Build a minimal focused set from each newly added or modified `TestXxx` plus
   the individual existing regression test(s) that directly prove the changed
   behavior or transition. When a shared helper, package/global state, or
   background worker changes, choose the representative tests for the affected
   contract; the package-wide run in step 5 covers the broader interaction.
   If an issue, CI failure, or review comment names a failing `TestXxx`, that
   exact test is mandatory in the focused set; adjacent tests are not a
   substitute.
2. Prove the selection is non-empty: first enumerate it with `GOWORK=off go test -mod=readonly -list`, or
   verify that the test output names every intended test. A successful command
   whose `-run` expression matched nothing is not evidence.
3. Measure each exact test once under `-race`, excluding first-build time, and
   choose an adaptive repetition count. Read duration `T` from the test's
   terminal event emitted by `GOWORK=off go test -mod=readonly -json`, not the rounded package summary.
   With stress budget `B` and measured test duration `T`, use
   `N = clamp(floor(B/T), 1, 100)`. Default `B` to 30 seconds; if `T` is absent,
   non-positive, or below timer resolution, use the upper cap `N = 100`.
   Adjust `B` for the change's risk and CI budget, and record `T`, `B`, and `N`.
   If a pre-fix reproduction has a known occurrence window, override the formula
   so the post-fix run covers that window; record why.
4. Run each focused test separately so a slow test does not reduce repetitions
   for a fast one:
   `GOWORK=off go test -mod=readonly -race -count=N -timeout 120s -run '^TestA$' ./pkg/path`.
   Independent commands may run in parallel when they do not contend for the
   same external resource. Keep repetitions of one test in the same process so
   leaked package/global state remains observable.
5. Then run the entire owning package once under the race detector:
   `GOWORK=off go test -mod=readonly -race -count=1 -timeout 240s ./pkg/path`.
6. If the package directly or transitively uses CGo, replace `go test` in all
   commands with `.agents/skills/mo-dev/scripts/mo-cgo-test`; follow the
   `mo-dev` environment setup. Do not silently skip tests because the local
   linker or runtime environment is incomplete.

Every repeated-stress command must contain an exact `-run` expression naming one
individual test. Never apply adaptive `-count=N` stress to a package pattern or
the repository; full-package race coverage is step 5 and runs only once.

Use a bounded, test-appropriate `-timeout` when needed. Normal tests,
non-race `-count=N`, coverage runs, or one focused race run do not substitute
for this gate.

The only routine exception is a measurement-only allocation/performance test
whose oracle is invalidated by race-runtime bookkeeping. Isolate only that
measurement behind `//go:build !race`, keep an equivalent functional test in
the race build, and stress the functional test with the adaptive race budget.
Never hide functional behavior or an ordinary timing assertion behind `!race`.
For any other platform, build-tag, or test-kind constraint, report the exact
test and technical reason; the gate remains blocked until the constraint is
resolved or the reviewer explicitly accepts equivalent validation.

Before accepting the stress result, audit the test design against recurring MO
flake classes:

- synchronize phases with channels, callbacks, barriers, or observable
  conditions; do not use `time.Sleep` or a tiny deadline as the scheduler;
- assert durable behavior, not a transient map entry, worker ownership, or
  which goroutine happened to make progress;
- register cleanup immediately so it runs after failed assertions too; restore
  package/global state and stop goroutines, timers, sockets, allocators, and
  other caller-owned resources;
- make topology, ordering, IDs, and map-derived choices deterministic; repeated
  `-count=N` runs share one test process and must not inherit prior-run state;
- use a generous outer deadline only as a hang guard unless timeout behavior is
  itself the contract under test.

Race success does not prove a timing-, allocation-, or instrumentation-sensitive
oracle under non-race or coverage execution. Run the matching CI mode as
additional evidence when the changed test depends on one of those properties.
All evidence must contain the real exit status and be newer than the final
semantic edit or rebase.

On a PR, use the same methodology against the verified PR base. A callable
review workflow may accelerate discovery, but it does not replace personal
closure and severity verification.

Depth delegation: for the leak/hung/OOM analysis, drive the **unhappy-path-audit**
skill; for CGo build/test env and MO operator/format specifics, see **mo-dev**.

---

## 7. Exit gate — the diff is self-review-clean when ALL hold

```
□ every §1 lens swept over the whole diff
□ every changed arc's functional closure (§3) traced to its terminal node
□ Q1–Q3 unhappy paths (§4) checked on touched resources/waits/growth
□ state ownership, wait-for dependencies, and generation transitions modeled where applicable
□ every finding either FIXED or written to the decision log (§5.2)
□ severity calibrated to the merge bar (§5.1) — zero open blockers
□ every new/modified and directly affected Go behavioral unit test passed a proven non-empty focused run
□ when G-RACE-STRESS applies, focused adaptive -race -count=N and one owning-package -race run passed, with T/B/N recorded
□ every !race measurement-only test retains a race-tested functional counterpart when race validation applies
□ test matrix covers every changed transition and evidence is newer than the final edit/rebase
□ applicable domain guards passed (index-plugin → §8) — additive to the §1–§4 sweep above, never a substitute for it
```

Only then push / open the PR. If a subsequent PR review still finds a real
blocker, that's a gap in §1/§3 coverage — add the missed lens/closure arc here so
the gate catches it next time (the gate improves; the loop still ends).

---

## 8. Domain guard — index-plugin changes

> **A domain guard is a supplement to §1–§5, never a replacement.** Always run the
> full multi-angle sweep over the ENTIRE diff (§1–§4) regardless of whether this
> guard applies; §8 only *adds* algo-specific checks when index-plugin files are
> touched. Passing §8 alone is not a review.

Apply when the diff touches index-algorithm dispatch, any
`pkg/vectorindex/<algo>/plugin/`, `pkg/fulltext/plugin`, or `pkg/indexplugin`.
Run [mo-dev index-plugin reference §9](../mo-dev/references/index-plugin.md#9-reviewing-an-index-plugin-change)
as the single source of current algorithm-specific checks. Its greps are
candidate discovery only: inspect matched production code, exclude
comments/tests, and prove registration, hooks, applicable ISCP/CDC wiring,
build-tag behavior, and public-path coverage before passing or blocking. Do not
copy the detailed guard here; duplicated rules drift.
