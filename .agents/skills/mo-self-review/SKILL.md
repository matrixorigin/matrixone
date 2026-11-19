---
name: mo-self-review
description: Review MatrixOne changes before push or as an external PR reviewer using one complete change map, design-first gates, risk-routed first-principles analysis, functional/unhappy-path closure, and reusable validation evidence. Use for pre-push review, PR/deep review, concurrency/lifecycle changes, test-quality changes, or repeated review-loop reduction.
---

Compatibility: designed for Codex CLI and compatible agents with a MatrixOne
git working tree. Use `unhappy-path-audit` for deep Q1-Q3 ownership, wait, and
growth analysis.

## Review contract

Cover every changed contract. Use one change/risk map to route deep analysis
to applicable closures. Group mechanical changes and document exclusions that
could affect the decision.

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

A blocker needs a reachable failure, a material design cost without a
supporting requirement, or missing mandatory validation. For a design blocker,
name the added or duplicated responsibility, its concrete cost or failure mode,
and the simpler viable ownership or mechanism. Preferences and micro-nits are
non-blocking or omitted. Assign severity only after verification.

MatrixOne upgrades by full-cluster shutdown only; old and new binaries never
serve one cluster together. Apply the shared upgrade contract to affected
boundaries, including changes in how unchanged persisted bytes are interpreted.
For each finding, identify the supported source release or external contract,
the reachable artifact/input, and the consequence or missing required proof.

## Design quality and delivery scope

- Trace user entry points to the current owner. Compare new state, validation,
  storage and execution paths with a direct change there. Check old assumptions
  and remove superseded branches across wiring, state, and documentation.
- Check authority during failure and recovery when two paths coexist. Count
  steady-state and failure costs, including background work, compatibility
  branches and operator burden. Tie each material cost to a concrete benefit.
- Check implementation, tests and docs independently. Count maintained
  responsibilities and distinct oracles. A line-count change helps locate work
  for review. It cannot establish improvement or bloat by itself.
- Before delivery, exclude temporary files, raw logs/profiles/benchmark results,
  per-run manifests, agent transcripts, review attestations and one-off proof
  narratives. Retain maintained design docs, regression fixtures and reproduction
  tools only where they have continuing value to the repository.

Keep raw evidence outside tracked source. When reviewers need it, use CI
artifacts, PR attachments or durable external links; do not require a raw-evidence
commit to satisfy review. Keep the PR body concise: problem, resulting behavior,
key validation/results and material limits. Link bulky details instead of pasting
logs, repeated summaries or the experiment/review chronology.

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
| PR description review, PR/issue creation or body update; Mergify description/commit-message failure | [../mo-dev/references/github-delivery.md](../mo-dev/references/github-delivery.md) |
| Credible shared-state/lifecycle/synchronization/timing failure mode | [../mo-dev/references/race-validation.md](../mo-dev/references/race-validation.md) |
| Large/complex feature or major refactor; design review (RFC optional) | [../mo-dev/references/feature-design-review.md](../mo-dev/references/feature-design-review.md) |
| Shared state, cancel/close, callbacks, retry/restart, pooling/reuse, async cleanup | [references/concurrency-lifecycle.md](references/concurrency-lifecycle.md) |
| Production behavior or changed/added/removed/merged/optimized UT/BVT | [../mo-dev/references/testing-contract.md](../mo-dev/references/testing-contract.md) |
| Planner/explain/rewrite correctness or a credible reproducer-specific, bypassed, or self-validating check | [../mo-dev/references/counterexample-testing.md](../mo-dev/references/counterexample-testing.md) |
| Changed persisted format or interpretation (including stored IDs or expression semantics), upgrade/restore, or protocol and plan encoding | [../mo-dev/references/upgrade-compatibility.md](../mo-dev/references/upgrade-compatibility.md) |
| Index algorithm dispatch/plugin/registry/ISCP paths | [../mo-dev/references/index-plugin.md#9-reviewing-an-index-plugin-change](../mo-dev/references/index-plugin.md#9-reviewing-an-index-plugin-change) |

## Single-pass execution

1. **Resolve and inventory.** Establish the exact scope and inspect the complete
   changed range and delivery artifacts. In self-review, carry forward the map
   and valid evidence. Reinspect changed and affected closures.
2. **Route by risk.** Use the shared map for owners, consumers, affected risks
   and required proof. Check an author's supplied map against the diff.
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

On follow-up, check open findings against the current head and re-review their
affected closures. Record which issues are fixed, accepted, or still open.
A reply or a green unrelated check does not establish closure.

If an optional code-review workflow is available, it may help discover
candidates from the same resolved range. Its output does not replace the change
map, closure verification, evidence audit, or final severity calibration.

## Review lenses

Use the lenses as prompts for applicable closures. Record the risks and
exclusions that affect the verdict in the shared map.

| Lens | Question |
|---|---|
| Correctness/boundaries | Does the invariant hold for ordinary and reachable zero/empty/nil/boundary/invalid inputs? |
| Contract/consumers | Are callers, readers, receivers, interfaces, generated artifacts, and public error/result semantics aligned? |
| State/concurrency | Are transitions, linearization, shared ownership, stale generations, races, and exactly-once side effects defined? |
| Control/liveness | Can cancel/close/reject/timeout terminate independently of the work it controls? |
| Resource lifecycle | Does each resource have one effective cleanup owner on success, partial failure, cancel, panic, reset, and reuse? |
| Boundedness/scale | Are queues, retries, caches, retained state, logs/metrics, memory/disk/FDs, and work admission bounded? |
| Compatibility/security | Does the changed closure preserve the interpretation of released state on reads and later writes, supported migrations/restores, external contracts, and auth, tenant, and trust boundaries where applicable? Route affected compatibility boundaries through the shared upgrade contract. |
| Performance/operations | Did hot-path work, allocations, I/O, synchronization, startup, capacity, rollout, observability, or blast radius change materially? |
| Platform/delivery | Are build tags, OS/arch, CGo/native loading, generated files, packaging, and final committed artifacts correct? |
| Test architecture | Do tests prove distinct contracts with minimum deterministic data/setup, correct UT/BVT layer, isolation, cleanup, and real selection? |

## First-principles finding proof

Do not report “might.” For each candidate:

1. State the invariant and concrete reachable input/state.
2. Follow all guards, ownership transfers, defer/cancel/timer/retry/release paths.
3. Identify the user or operational impact, avoidable design cost, or missing
   required proof.
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
| config/API/protocol | parse/default -> producer -> every consumer -> compatibility with persisted/external data (no in-cluster old-peer fallback) |
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

Use the testing contract for fixture reuse, independent oracles, UT/BVT depth
and teardown. Apply the shared evidence checks to suspected overfit, lost test
coverage and performance claims. For a proposed optimization, check work and
results at the real entry point as well as the claimed faster phase.

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

Use the shared delivery record for the final decision and its evidence.
In self mode, verify the final diff and generated artifacts. For PR
publication, use the GitHub delivery reference for the template and readback.

For index-plugin changes, additionally run the linked section-9 candidate
searches and prove hook interfaces, registration/build tags, ISCP/CDC where
applicable, CPU-runnable tests, and public-path behavior. Candidate greps are not
standalone findings.
