---
name: mo-dev
description: MatrixOne database kernel development - first-principles, performance-aware change design; controlled local CGo build/test setup; complete test matrices; counterexample-driven white-box/black-box validation; hung-test diagnosis; GPU builds (MO_CL_CUDA=1 / cuVS); operator lifecycle contracts (Call/Reset); pipeline protocol; and the vector/fulltext index-plugin framework. Use when modifying kernel production code or hot paths, fixing correctness/resource-lifecycle/concurrency defects, evaluating a new abstraction or framework, designing systematic non-overfit regressions for planner/explain/rewrite bugs, running CGo-transitive tests, diagnosing compile/link/load/vendor failures or silent hangs, changing colexec/process/compile pipelines, or adding/editing index algorithms.
---

Compatibility: designed for Codex CLI and compatible agents on supported MatrixOne development platforms. Use the Go version declared by `go.mod`; CGo work also requires GNU Make, a supported C/C++ toolchain, and matching pre-built thirdparties.

## Resource Map

Load only the reference needed for the task:

| Need | Read |
|------|------|
| CGo compile/link/load/module errors, controlled local test execution, layered matrices, hung tests, "pre-existing" claims, GPU/cuVS/CUDA | [references/cgo-build-test.md](references/cgo-build-test.md) |
| `colexec` operator edits, `process` signal types, pipeline spools, Call/Reset cleanup, hung tests, distributed pipeline hangs, remote dispatch/receiver registration | [references/operator-pipeline.md](references/operator-pipeline.md) |
| Systematic regression design, counterexamples, white-box/black-box validation, planner/explain/rewrite correctness, avoiding scenario overfit | [references/counterexample-testing.md](references/counterexample-testing.md) |
| Vector/fulltext index algorithm work, plugin registry, GPU-only algorithm registration, index-plugin review | [references/index-plugin.md](references/index-plugin.md) |

## Enforcement Gates

Consult the referenced material before acting:

| Gate | When | Action |
|------|------|--------|
| **G-MODIFY** | Before editing any `colexec` operator or `process` signal type | Read [operator-pipeline.md](references/operator-pipeline.md). |
| **G-CGO-ERR** | Any build/test returns module/vendor, header, link, `dyld`, or shared-library errors | Read [cgo-build-test.md](references/cgo-build-test.md) and identify the failing layer before changing code. |
| **G-GPU** | Before a GPU build/test (`MO_CL_CUDA=1`), or on CUDA/cuVS errors (`CONDA_PREFIX`, `nvcc`, `-lcuvs`/`-lcudart`, `unsupported index type: ivfpq\|cagra`) | Read [cgo-build-test.md](references/cgo-build-test.md) section 6. |
| **G-IDXPLUGIN** | Before adding/editing an index-algorithm plugin, OR adding any `switch`/`if` on an index **algo** name in `pkg/sql/{compile,plan}` or `pkg/catalog` | Read [index-plugin.md](references/index-plugin.md). Route through `pkg/indexplugin`; new algo switches are forbidden. |
| **G-IDXREVIEW** | Reviewing a diff that touches index-algorithm dispatch, `pkg/vectorindex/<algo>/plugin/`, `pkg/fulltext/plugin`, or `pkg/indexplugin` | Read [index-plugin.md](references/index-plugin.md) section 9 and run its greps. |
| **G-DONE** | Before declaring "done"/"complete"/"passes" | Apply the completion gate below. |
| **G-TEST-FAIL** | `go test` returns non-zero or hangs >10s | Route by evidence: module/header/link/load errors → [cgo-build-test.md](references/cgo-build-test.md); operator/lifecycle/channel hangs → [operator-pipeline.md](references/operator-pipeline.md); ordinary assertion/panic failures → trace the owning code and test directly. Do not load unrelated references by default. |
| **G-TEST-EVIDENCE** | A test command yields no final PASS/FAIL, returns a session/process identifier, or leaves a test process alive | Treat it as still running or failed; poll, inspect the process, and capture its real exit status/stack. |
| **G-PIPELINE-HANG** | A distributed query, DML, `LOAD DATA`, dispatch, or remote pipeline stalls | Read [operator-pipeline.md](references/operator-pipeline.md), collect synchronized stacks from every CN, and close the cross-CN registration/wait graph before attributing storage or network cause. |
| **G-COUNTEREXAMPLE** | Fixing a planner/explain/rewrite correctness bug, or asked for systematic, first-principles, non-overfit, white-box/black-box coverage | Read [counterexample-testing.md](references/counterexample-testing.md). State the invariant and its negation, prove public-path reachability for externally visible claims, and use independent black-box and white-box oracles when each proves a distinct necessary claim. |
| **G-DESIGN** | Before a non-trivial production-code change or proposing a new abstraction/framework, especially lifecycle, concurrency, cache, retry, or hot-path work | State the invariant, root cause, ownership boundary, relevant state transitions, and hot-path cost. Choose the smallest general change that closes the invariant. Justify any new abstraction or framework with multiple independent recurring needs, a stable shared contract, and a net reduction in total complexity. |

## Project Structure

```
pkg/sql/compile/       <- DAG compilation, operator instantiation, pipeline build, launch
pkg/sql/colexec/       <- execution operators (connector/dispatch/merge/join/scan/...)
pkg/vm/process/        <- execution context (Process), WaitRegister, signal channels
pkg/vm/pipeline/       <- Pipeline lifecycle management
pkg/container/         <- Batch/Vector/pSpool and other base data structures
pkg/frontend/          <- MySQL protocol compatibility layer
pkg/txn/               <- transaction management and MVCC
pkg/sql/plan/          <- query plan construction
cgo/                   <- CGo adapter layer (libmo.dylib / libmo.so)
```

Operator file convention under `pkg/sql/colexec/<op>/`:

- `types.go`: Arg struct definition
- `<op>.go`: main logic (`Prepare`/`Call`/`Reset`)
- Optional `sendfunc.go`/`dispatch.go` helpers

Key dependency chain: `compile` instantiates operators -> `colexec` executes -> `process` manages context and signals -> `container` carries data.

## Change Design Rules

Apply **G-DESIGN** before editing production code:

1. Work from the violated invariant and first owner of the state or resource. Do not start from the proposed patch or the last visible stack frame.
2. Close the complete relevant state space: success, error, cancellation, timeout, retry, reuse/reset, restart, and partial initialization. Fix the common ownership or protocol boundary instead of adding a branch for one observed trace.
3. Keep the mechanism proportional to the problem. A small or local defect does not by itself justify a framework, generic abstraction, or subsystem; a leak is only one example. Prefer an existing primitive or the narrowest ownership correction. Introduce shared machinery only when multiple independent recurring needs reveal a stable common contract and it reduces total code, state, runtime cost, and operational/cognitive complexity.
4. Treat row-, batch-, message-, transaction-, and query-frequency paths as performance-sensitive. Account for added allocations, scans, copies, locks/atomics, goroutines/channels, syscalls, I/O, logging, and metric cardinality. When cost could be material, compare a focused benchmark or profile before and after; do not move diagnostics onto the fast path without a bounded budget.
5. Avoid speculative flexibility. Do not add configuration, generic layers, background workers, caches, retries, global state, or extension points for hypothetical future needs. Make the smallest change that restores the general contract; let multiple concrete uses establish a stable variation axis before generalizing.
6. Avoid scenario overfit. Do not encode issue numbers, exact data shapes, timing coincidences, or one plan layout in production logic. Derive regression cases from the invariant and its nearby controls using [counterexample-testing.md](references/counterexample-testing.md).
7. Prefer deleting state, transitions, and duplicated cleanup over coordinating them with another layer. Every new stateful component must have an explicit owner, bound, initialization point, normal termination, error/cancel termination, and reuse/restart rule.

## Quick Test Commands

For ordinary package checks:

```bash
GOWORK=off go build -mod=readonly ./pkg/target/...
GOWORK=off go vet -mod=readonly ./pkg/target/...
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/target/...
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s -run '^TestXxx$' ./pkg/target/...
```

For CGo-transitive or CGo-direct packages, do not guess flags. Read [references/cgo-build-test.md](references/cgo-build-test.md).

For local CGo tests, prefer the controlled wrapper. It normalizes the
repository module/CGo/load paths; `GOFLAGS`, `GOEXPERIMENT`, `CC`, and `CXX`
remain caller-owned inputs and must be recorded when relevant:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/target/...
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=240s ./pkg/target/...
```

`MO_CL_CUDA=1` selects the GPU build, the same switch `make` uses: it adds the
CUDA/cuvs link flags and implies `-tags gpu`. It is required both for gpu-tagged
packages AND for any package once `cgo/libmo.so` has been built with CUDA, since
a CUDA libmo carries undefined `cu*` symbols that every test binary linking it
must resolve. The wrapper detects that libmo and says so rather than letting the
linker emit a page of `undefined reference to cuInit`:

```bash
MO_CL_CUDA=1 .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=300s ./pkg/vectorindex/metric/
```

Rule: "`go build` passes" does not mean "`go test` will pass." Test binaries link more CGo.

## Operator / Pipeline Rules

Read [references/operator-pipeline.md](references/operator-pipeline.md) before changing these paths.

- `Call()` processes one batch. It must not send terminal signals (`End`, `Error`, `Abort`).
- `Reset()` performs cleanup and notifies downstream completion.
- For typed signals, distinguish graceful completion from failure. After `EventEnd` is delivered, reclaim the spool with `ForceCleanupAfterTerminalSignal()` only after paired receiver cleanup returns.
- Use `Abort(cause)` for error/abort/cancel paths or terminal-delivery failure. Reserve `Close()` / `CloseWithTimeout()` for the legacy nil-batch protocol.
- When sending terminal signals into bounded channels, record terminal state even if the channel send fails.

## Index-Plugin Rules

Read [references/index-plugin.md](references/index-plugin.md) before touching vector/fulltext index algorithm dispatch.

- Work through `pkg/indexplugin.Get(algo)` and hook interfaces.
- Do not add new per-algorithm `switch` / `if IsXxxIndexAlgo || ...` in SQL/catalog layers.
- Do not import `pkg/sql/plan` or `pkg/sql/compile` from plugin packages.
- Register CPU-safe plugins in `pkg/indexplugin/all/all.go`; register GPU-only plugins in `all_gpu.go`.
- Keep `var _ AlgoPlugin` and `var _ Hooks` compile-time assertions intact.
- Add CPU-runnable unit tests for plan/schema/runtime hooks; GPU-gated BVT alone can fail coverage gates.

## Completion Gate

Before declaring any MatrixOne change done, map every changed artifact to its
owning validation. Do not run an unrelated Go package merely to fill a box.

```
□ changed artifacts and their direct/dependent validators are explicitly named
□ if Go is affected, each owning package pattern is named and
  `GOWORK=off go list -mod=readonly <patterns...>` proves the selection is non-empty
□ if Go is affected, GOWORK=off go build -mod=readonly <patterns...> -> exit 0
□ if Go is affected, GOWORK=off go vet -mod=readonly <patterns...>   -> exit 0
□ if Go is affected, GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s <patterns...> -> exit 0, no hangs
□ if non-Go artifacts are affected, their parser/linter plus the smallest
  behavior-level validation for the changed contract pass (for example shell
  syntax + script tests, Make dry-run + target test, or Docker check + relevant build)
□ at least one real dependent consumer is validated when the change crosses an ownership boundary
□ git diff --stat -> inspected, no unintended files
□ all evidence is newer than the last semantic edit/rebase and has a real exit code
```

Hang = failure. If `go test` produces >10s of no output, investigate instead of calling it slow.

Hard rule: never claim a failure is "pre-existing" without reproducing it at the correct clean baseline in an isolated worktree, using the same toolchain and dependency mode. Use [references/cgo-build-test.md](references/cgo-build-test.md) section 5.

## Common Diagnosis Shortcuts

| Symptom | First Place To Look |
|---------|---------------------|
| Test hangs near a repeatable deadline (for example 30s) | Locate the timer/deadline owner from stacks and the wait-for graph. `CloseWithTimeout` is one candidate, not a conclusion. Read [operator-pipeline.md](references/operator-pipeline.md). |
| Test hangs >5s, no output | Deadlock or blocking channel send. Check `done` channel and non-blocking `select`. |
| `context deadline exceeded` after 30s | Did all senders call `Reset()` and send typed terminal signals? |
| Primary CN waits in `receiveMsgAndForward` while remote CNs wait in `GetProcByUuid` | Trace scope placement and dispatch ownership through `PutProcIntoUuidMap`; compare exact cross-CN fan-out counts. Read [operator-pipeline.md](references/operator-pipeline.md). |
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `Undefined symbols` / `undefined symbol:` | Inspect the ordered native dependency graph and artifact freshness; read [cgo-build-test.md](references/cgo-build-test.md). |
| `cannot find -lmo` / `ld: library 'mo' not found` | Use the wrapper; for manual links, place `cgo` package `CgoLDFLAGS` after `-lmo`. Read [cgo-build-test.md](references/cgo-build-test.md). |
| `dyld`/loader searches a temporary `go-build.../lib` directory | A package-relative rpath was used for a temporary test binary; use the CGo test wrapper or absolute test rpaths. |
| Only linker warnings appear, no PASS/FAIL | Check the returned session and live test process; do not infer success from partial output. |
| `unsupported index type: ivfpq|cagra` | CPU binary lacks GPU plugin registration; read [index-plugin.md](references/index-plugin.md) and GPU notes. |

## Forbidden Patterns

1. Never send terminal signals (`End`, `Error`, `Abort`) from `Call()`.
2. Never use legacy `CloseWithTimeout()` to finish a typed-signal path; apply the graceful `EventEnd` or failure `Abort(cause)` contract above.
3. Never claim "pre-existing" from a dirty-tree rerun or `git stash`; reproduce at the correct clean baseline in an isolated worktree.
4. Never declare done without fresh test output.
5. Never assume `go build` success means `go test` will pass.
6. Never skip bottom-up testing: pure Go -> CGo-transitive -> CGo-direct.
7. Never add a per-algorithm `switch`/`if` on an index algo name in the SQL layer. Route through `indexplugin.Get(algo)`.
8. Never use distributable-binary relative rpaths as proof that a temporary `go test` binary can load its libraries.
9. Never use an exact `EXPLAIN` text snapshot or a white-box structural assertion as the only oracle for semantic correctness.
10. Never make a regression pass by adding sleeps, retries, skipped cases, or weaker assertions without proving that the product contract permits that behavior.
11. Never introduce a framework, generic abstraction, subsystem, background worker, cache, or retry layer merely because a small/local defect could be generalized. Require multiple independent recurring needs, a stable shared contract, and lower total complexity after including runtime, operations, testing, and maintenance.
12. Never add a potentially material per-row, per-batch, per-message, or per-query cost without a bounded cost analysis and, when material, a focused benchmark/profile. Unbounded logging or metric cardinality is always forbidden.
13. Never call a change systematic merely because it is broad; prove that it restores one general invariant across the relevant state transitions with less total complexity than the alternatives.
