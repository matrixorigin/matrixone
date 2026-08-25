---
name: mo-self-review
description: Review MatrixOne changes before push or as an external PR reviewer using one complete change map, design-first gates, risk-routed first-principles analysis, functional/unhappy-path closure, and reusable validation evidence. Use for pre-push review, PR/deep review, concurrency/lifecycle changes, test-quality changes, or repeated review-loop reduction.
---

Compatibility: designed for Codex CLI and compatible agents with a MatrixOne
git working tree. Use `unhappy-path-audit` for deep Q1-Q3 ownership, wait, and
growth analysis.

## Review contract

The quality bar is complete coverage of every changed contract, not equal depth
for every file. Read every changed hunk once, build one change/risk map, and
route deep analysis only to applicable closures. This is not spot checking:
every lens must map to a closure or have a concrete not-applicable reason.

Keep two execution modes distinct:

| Mode | Responsibility |
|---|---|
| **Self/delivery review** | May fix in-scope code; must close blockers and ensure required evidence exists before push/PR update. |
| **External PR review** | Review read-only by default. Verify and reuse qualifying author/CI evidence; run only missing or contradiction-resolving checks. Report the decision, but submit GitHub `APPROVE`/`REQUEST_CHANGES` only when the user authorized that external mutation. |

An explicit developer decision closes a subjective tradeoff when its assumptions
and rationale are recorded. Do not repeatedly relitigate it without materially
new evidence. It does not erase a demonstrated correctness/security/data/hang/
leak/compatibility failure or a mandatory user/repository gate. The PR author or
reviewer cannot self-waive such a gate; only its policy owner can explicitly
change an allowed exception, with owner/scope/rationale recorded. User/system
permission boundaries cannot be waived.

A blocker requires either a concrete failure path (`input/state -> consequence`)
or an objectively missing mandatory artifact/validation. Preferences, plausible
concerns, maintainability suggestions, and micro-nits are non-blocking or
omitted. Assign severity only after the finding is verified.

## Resolve target and scope

| Args | Target |
|---|---|
| *(empty)* | current branch vs verified PR base when one exists; otherwise a freshly verified remote default branch |
| git ref / `vs <ref>` / `base=<ref>` | current branch vs the explicit ref |
| all digits | that GitHub PR vs its declared base |
| `<ref> <scope...>` | explicit base with trailing focus |
| anything else | focus text with verified default base |
| `docs` / `help` | show the workflow only |

Resolve explicit user base, then PR base, then fresh remote default. Never
silently use stale local `main`. Record base ref/object, head object, merge-base,
committed range, staged/unstaged changes, and untracked files. Report freshness
limits when the remote cannot be verified. A focus narrows presentation and
deep tracing, not awareness of in-scope changes that can interact with it.

## Resource map

| Trigger | Read |
|---|---|
| Every non-trivial review: change map, R0-R3 routing, evidence reuse, efficient validation | [../mo-dev/references/validation-evidence.md](../mo-dev/references/validation-evidence.md) |
| Credible shared-state/lifecycle/synchronization/timing failure mode | [../mo-dev/references/race-validation.md](../mo-dev/references/race-validation.md) |
| Large/complex feature or major refactor; design document (RFC optional) | [../mo-dev/references/feature-design-review.md](../mo-dev/references/feature-design-review.md) |
| Shared state, cancel/close, callbacks, retry/restart, pooling/reuse, async cleanup | [references/concurrency-lifecycle.md](references/concurrency-lifecycle.md) |
| Production behavior or changed/added/removed/merged/optimized UT/BVT | [../mo-dev/references/testing-contract.md](../mo-dev/references/testing-contract.md) |
| Index algorithm dispatch/plugin/registry/ISCP paths | [../mo-dev/references/index-plugin.md#9-reviewing-an-index-plugin-change](../mo-dev/references/index-plugin.md#9-reviewing-an-index-plugin-change) |

## Single-pass execution

1. **Resolve and inventory.** Establish the exact scope above. Inspect status,
   diff stat/name-status, generated/delivery artifacts, then read every changed
   hunk once.
2. **Build the change map.** Group files into behavioral closures; record
   invariant/purpose, owner/consumers, reverse arcs, R0-R3 triggers, applicable
   lenses, and required evidence. Reuse this map instead of rereading the whole
   diff once per lens.
3. **Run the design gate first.** Classify the complete feature/refactor or PR
   series. Ordinary fixes/maintenance are exempt. When triggered, review the
   exact approved design revision before implementation. Missing/unapproved or
   failed design makes the decision `REQUEST_CHANGES` and short-circuits full
   implementation polishing.
4. **Trace applicable closures.** For each map row, follow edited code into the
   first owner, all changed consumers/reverse arcs, and terminal success/failure
   nodes. Apply the lenses below. R0/R1 does not pay R3 modeling cost; any small
   R3 closure does.
5. **Audit evidence.** Check selection, mode, revision, terminal status, and
   semantic freshness. Reuse valid exact-head author/CI evidence. In self mode,
   produce missing proof; in external mode, run only gaps or checks needed to
   resolve a concrete uncertainty.
6. **Converge once.** Verify every candidate against source and full closure,
   discard speculation, respect recorded decisions, then return one ranked
   finding/fix-or-decision list and final decision. Do not request another broad
   review pass.

If an optional code-review workflow is available, it may help discover
candidates from the same resolved range. Its output does not replace the change
map, closure verification, evidence audit, or final severity calibration.

## Review lenses

Classify every lens for every mapped closure. Group rows that share the same
applicability and reason instead of emitting a prose Cartesian product. `N/A`
needs a fact such as “no executable behavior or generated consumer changed,”
not “diff is small.”

| Lens | Question |
|---|---|
| Correctness/boundaries | Does the invariant hold for ordinary and reachable zero/empty/nil/boundary/invalid inputs? |
| Contract/consumers | Are callers, readers, receivers, interfaces, generated artifacts, and public error/result semantics aligned? |
| State/concurrency | Are transitions, linearization, shared ownership, stale generations, races, and exactly-once side effects defined? |
| Control/liveness | Can cancel/close/reject/timeout terminate independently of the work it controls? |
| Resource lifecycle | Does each resource have one effective cleanup owner on success, partial failure, cancel, panic, reset, and reuse? |
| Boundedness/scale | Are queues, retries, caches, retained state, logs/metrics, memory/disk/FDs, and work admission bounded? |
| Compatibility/security | Are API/wire/disk/catalog/config, mixed-version/migration/rollback, auth, tenant, and trust boundaries preserved? |
| Performance/operations | Did hot-path work, allocations, I/O, synchronization, startup, capacity, rollout, observability, or blast radius change materially? |
| Platform/delivery | Are build tags, OS/arch, CGo/native loading, generated files, packaging, and final committed artifacts correct? |
| Test architecture | Do tests prove distinct contracts with minimum deterministic data/setup, correct UT/BVT layer, isolation, cleanup, and real selection? |

## First-principles finding proof

Do not report “might.” For each candidate:

1. State the invariant and concrete reachable input/state.
2. Follow all guards, ownership transfers, defer/cancel/timer/retry/release paths.
3. Identify the wrong result, crash, hang, data/security/compatibility impact, or
   exactly missing required proof.
4. Re-read the cited source after forming the hypothesis.
5. Keep it only if no existing path closes the failure; calibrate severity last.

The design-first gate is an artifact/decision proof rather than a runtime
counterexample: cite the exact trigger and missing/unresolved design requirement.

## Functional and unhappy-path closure

Trace only applicable arcs, but trace them to terminal nodes:

| Change | Minimum closure |
|---|---|
| persistence/format | create/write -> read -> backup/restore -> upgrade/restart |
| operator/pipeline | prepare -> call/send -> receive -> reset/cleanup -> error/cancel |
| resource/state machine | create/admit -> transfer/transitions -> fail/retry -> close/reuse generation |
| config/API/protocol | parse/default -> producer -> every consumer -> compatibility/fallback |
| shared test fixture | create -> scenario admission -> cleanup after `FailNow` -> reset -> next scenario -> destroy |
| BVT | clean/readiness -> public action -> positive/negative oracle -> restore/teardown -> same-instance rerun |

For touched resources, waits, or accumulating state, use `unhappy-path-audit`:

- **Q1:** every creation reaches exactly one effective destruction owner;
- **Q2:** every wait-for chain reaches guaranteed release/cancel/bound;
- **Q3:** every accumulation has a capacity/admission/recycle/terminal bound.

Apply its full-graph, can-fail/block, bound/release, line-reread, and
calibrate-last filters. Do not load or run the deep audit for a closure that has
no resource, wait, asynchronous generation, or growth dimension.

## Test and evidence gate

Use the testing contract for purpose, orthogonality, fixture cost, BVT, and
cleanup. Use the validation/evidence reference for R0-R3 depth and semantic
evidence reuse; load the race reference only when its trigger applies.

Non-negotiable outcomes:

- changed behavior maps to the cheapest focused oracle and to BVT when a public
  SQL/protocol contract requires it;
- new/heavy fixtures demonstrate existing-case search, a distinct isolation or
  topology need, minimum data, deterministic control, and measured cost when
  unavoidable;
- merged/deleted cases map every prior positive/negative/boundary/metadata/
  privilege/session/error oracle to a retained named scenario;
- sleeps, retries, probabilistic scheduling, huge data, repeated processes, and
  generated-result acceptance never substitute for injection, barriers, scoped
  configuration, cleanup, or normal comparison;
- race/package/topology/repetition evidence is required only by its mapped risk
  trigger, but cannot be skipped when that trigger applies.

Evidence remains valid across unrelated edits. Invalidate and rerun only when a
relevant semantic input, oracle, fixture, build mode/tag, dependency, topology,
or base-side contract changed; ambiguity means stale. Pending, skipped, zero-test,
partial-output, or surviving-process runs are not green.

## Convergence and decision

- Record accepted tradeoffs and every won't-fix/known gap with owner/rationale.
  Reopen only with materially new evidence that invalidates its assumptions.
- Fix harmless local nits silently in self mode or omit them in external mode.
  Do not spend reviewer cycles on style that automation can decide.
- An upstream design/range/artifact blocker may stop downstream review. For code
  findings, complete the other applicable mapped closures so one review returns
  the converged blocker set rather than serial surprises.
- Final external decision is `REQUEST_CHANGES` when any blocker remains,
  otherwise `APPROVE`/no-blocker recommendation. Perform the GitHub mutation only
  when authorized.

## Exit gate

```text
□ exact range plus committed/staged/unstaged/untracked scope recorded
□ every changed hunk read and represented in one R0-R3 change map
□ every lens mapped to a closure or a concrete N/A reason
□ design gate classified; triggered design approved and implementation aligned
□ applicable owners/consumers/reverse arcs and terminal unhappy paths closed
□ Q1-Q3 and concurrency/generation models completed only where triggered
□ UT/BVT/fixture/oracle decisions complete where behavior/tests changed
□ required evidence validly reused or passed; stale/missing/pending proof explicit
□ each finding has a concrete path or objective gate failure and source re-read
□ decisions logged, severity calibrated last, zero unresolved blockers for PASS
□ final delivery diff/generated artifacts checked in self mode
```

For index-plugin changes, additionally run the linked section-9 candidate
searches and prove hook interfaces, registration/build tags, ISCP/CDC where
applicable, CPU-runnable tests, and public-path behavior. Candidate greps are not
standalone findings.
