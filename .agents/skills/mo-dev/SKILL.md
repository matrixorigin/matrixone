---
name: mo-dev
description: Develop and validate MatrixOne kernel changes with design-first gates, first-principles invariants, risk-proportional evidence, deterministic UT/BVT, and repository-specific CGo, operator/pipeline, planner, and index-plugin workflows. Use for MatrixOne production or test changes, bug fixes, feature/refactor design, kernel CI failures/hangs, and implementation review.
---

Compatibility: designed for Codex CLI and compatible agents on supported
MatrixOne development platforms. Use the Go version in `go.mod`. CGo work also
requires the repository-supported compiler and matching native artifacts.

## Outcome and core workflow

Produce the smallest general change that restores or introduces an explicit
contract, then prove every affected boundary with the cheapest valid evidence.
Rigor is coverage of contracts and unhappy paths, not diff size, command count,
or repeated full-suite runs.

1. **Resolve scope once.** Record base, head, merge-base, committed plus local
   changes, and untracked/delivery artifacts. Build the change map and per-closure
   R0-R3 classification from
   [validation-evidence.md](references/validation-evidence.md).
2. **Classify design work.** Apply the feature/major-refactor gate before
   implementation. Ordinary focused bug fixes are exempt from a design document,
   but never from invariant, ownership, cost, or validation reasoning.
3. **State the contract.** Write the problem evidence, invariant and negation,
   first owner, affected consumers, state transitions, unhappy paths, and
   performance/resource budget before choosing the patch.
4. **Load only applicable domain references.** Use the resource map below; do
   not load or restate every specialized workflow.
5. **Implement the narrowest complete closure.** Change the common ownership or
   protocol boundary, not one observed trace. Update consumers/reverse arcs and
   tests in the same functional unit.
6. **Validate by information value.** Reuse semantically valid evidence, run the
   cheapest discriminating checks first, and escalate only along mapped risk
   dimensions. Do not rerun equivalent author/CI work.
7. **Review and deliver.** Run `mo-self-review` on the final scope, resolve every
   blocker, inspect the delivery diff, and report the compact evidence record.

For diagnosis-only requests, stop after proving and explaining the cause; do not
turn diagnosis into an implementation without authorization.

## Resource map

Read a reference when its trigger applies:

| Need | Read |
|---|---|
| Change/risk map, efficient validation order, CI/local evidence reuse | [references/validation-evidence.md](references/validation-evidence.md) |
| Credible shared-state/lifecycle/synchronization/timing failure mode | [references/race-validation.md](references/race-validation.md) |
| Large/complex feature or major refactor; design document review (RFC optional) | [references/feature-design-review.md](references/feature-design-review.md) |
| UT/BVT purpose, orthogonality, fixture reuse, cost/flakiness, result and execution evidence | [references/testing-contract.md](references/testing-contract.md) |
| CGo compile/link/load/module errors, local wrapper, hang attribution, clean-baseline proof, GPU/cuVS/CUDA | [references/cgo-build-test.md](references/cgo-build-test.md) |
| `colexec`/`process`/pipeline lifecycle, typed terminal signals, distributed dispatch/receiver hangs | [references/operator-pipeline.md](references/operator-pipeline.md) |
| Planner/explain/rewrite correctness, public reachability, independent black-/white-box oracles | [references/counterexample-testing.md](references/counterexample-testing.md) |
| Vector/fulltext algorithm registry, build-tag registration, ISCP/CDC and index-plugin review | [references/index-plugin.md](references/index-plugin.md) |

## Enforcement gates

| Gate | Trigger | Required action |
|---|---|---|
| **G-FEATURE-DESIGN** | Feature/major refactor reaches the size threshold or any complexity trigger | Read the design contract; require and review an approved, versioned design document before implementation. A failed/missing design makes the review decision `REQUEST_CHANGES`; submit that GitHub review only when the task authorizes the mutation. |
| **G-CHANGE-MAP** | Before non-trivial implementation, validation, or review | Build one complete change map. Every changed hunk and contract must be covered; deep work is routed per closure rather than repeated over the whole diff. |
| **G-TEST-CONTRACT** | Production behavior changes or any UT/BVT is added, changed, removed, merged, or optimized | Read the testing contract. Map behavior to UT/BVT, inventory existing cases/fixtures, preserve all distinct oracles, and reject unnecessary data, sleeps, processes, or setup. |
| **G-CGO** | A selected package is CGo-direct/transitive, native/module/link/load errors occur, or GPU mode is involved | Read the CGo reference and use the controlled wrapper/appropriate GPU workflow. Diagnose the failing layer; do not change product code to mask environment failure. |
| **G-OPERATOR** | Editing/reviewing `colexec`, process signals, pipeline spool/protocol, or a distributed pipeline hang | Read the operator/pipeline reference before editing or concluding. Trace sender and receiver plus reset/cleanup terminal paths. |
| **G-COUNTEREXAMPLE** | Planner/explain/rewrite correctness or scenario-overfit risk | Read the counterexample reference. Define invariant/negation/reachability and use independent public and typed oracles where each proves a distinct claim. |
| **G-INDEXPLUGIN** | Index algorithm dispatch/plugin/registry/ISCP paths change | Read the index-plugin reference and its review section. New SQL/catalog per-algorithm dispatch is forbidden. |
| **G-EVIDENCE** | Before claiming pass/done or attributing a failure | Apply semantic evidence validity. Pending/empty selection/partial output is not a pass; a “pre-existing” claim requires the same failure at the verified clean baseline. |

## First-principles change rules

1. Start from the violated/missing invariant and the first owner of state or
   resources, not the proposed patch, issue spelling, or last stack frame.
2. Close success, error, cancellation, timeout, retry, partial initialization,
   reset/reuse, restart, and cleanup only where the mapped contract can reach
   them. Prove unreachable dimensions instead of testing them ritualistically.
3. Prefer deleting duplicated state/transitions/cleanup. Every new stateful
   component needs one effective owner, an explicit bound, admission/publication
   point, all terminal paths, and a generation/restart rule.
4. Keep mechanisms proportional. A local fix does not justify a framework,
   cache, worker, retry layer, global, extension point, or generic abstraction.
   Require multiple independent recurring needs, a stable contract, and lower
   total runtime/operational/testing/maintenance complexity.
5. Treat per-row/batch/message/transaction/query paths as hot until bounded.
   Account for allocations, copies, scans, synchronization, goroutines, I/O,
   logs, and metric cardinality; benchmark/profile only when cost can be
   material.
6. Avoid incident overfit. Do not encode issue IDs, one plan layout, exact data
   shape, or timing coincidence in production logic. Test the invariant,
   nearest control, and counterexample with minimum data and deterministic
   control.
7. Preserve user scope and permissions. Read-only review/diagnosis does not
   authorize production changes, GitHub review submission, or other external
   mutations.

## Efficient validation and completion

Use [validation-evidence.md](references/validation-evidence.md) as the source of
truth. In particular:

- prove exact package/test/case selection is non-empty;
- run focused evidence before broader owning-package/group evidence;
- validate a real consumer when an ownership/API boundary changes;
- add BVT/topology/restart/upgrade/race/GPU/performance only when the mapped
  contract requires that dimension;
- reuse exact-head CI or local evidence whose relevant semantic inputs and mode
  are unchanged; unrelated docs or PR metadata do not invalidate it;
- retain real terminal status and diagnose silence by polling the existing
  process, not by launching duplicates.

Ordinary pure-Go examples:

```bash
GOWORK=off go test -mod=readonly -list 'TestXxx' ./pkg/target
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s -run '^TestXxx$' ./pkg/target
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/target/...
GOWORK=off go vet -mod=readonly ./pkg/target/...
```

For CGo-direct/transitive packages, replace `go test` with:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/target/...
```

Before delivery, the record must show:

```text
□ resolved range/worktree scope and complete per-closure change map
□ design-gate decision and approved revision when triggered
□ invariant/root cause/owner/consumer and relevant unhappy paths closed
□ UT/BVT and specialized-domain decisions recorded
□ every required proof is validly reused or passed; gaps/pending work are explicit
□ generated/delivery artifacts, diff stat, and unintended files checked
□ `mo-self-review` has zero unresolved blockers
```

Never weaken assertions, add sleeps/retries/skips, broaden fixtures, or run an
unrelated package merely to make a checkbox green. Never claim “systematic”
from breadth alone: demonstrate one general contract closed across its relevant
state space with less total complexity than the credible alternatives.
