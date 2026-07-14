---
name: mo-dev
description: MatrixOne database kernel development - deterministic CGo build/test setup, complete test matrices, hung-test diagnosis, GPU builds (MO_CL_CUDA=1 / cuVS), operator lifecycle contracts (Call/Reset), pipeline protocol, and the vector/fulltext index-plugin framework. Use when modifying kernel packages, running CGo-transitive tests, diagnosing compile/link/load/vendor failures or silent hangs, changing colexec/process/compile pipelines, or adding/editing index algorithms.
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  language: go
  cgo: true
---

Compatibility: designed for Codex CLI and compatible agents. Requires Go 1.22+, GNU Make, C/C++ toolchain (gcc/clang), and pre-built thirdparties.

## Resource Map

Load only the reference needed for the task:

| Need | Read |
|------|------|
| CGo compile/link/load/module errors, deterministic test execution, layered matrices, hung tests, "pre-existing" claims, GPU/cuVS/CUDA | [references/cgo-build-test.md](references/cgo-build-test.md) |
| `colexec` operator edits, `process` signal types, pipeline spools, Call/Reset cleanup, hung tests | [references/operator-pipeline.md](references/operator-pipeline.md) |
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
| **G-TEST-FAIL** | `go test` returns non-zero or hangs >10s | Read [operator-pipeline.md](references/operator-pipeline.md) section 4 and [cgo-build-test.md](references/cgo-build-test.md) section 4 before attributing cause. |
| **G-TEST-EVIDENCE** | A test command yields no final PASS/FAIL, returns a session/process identifier, or leaves a test process alive | Treat it as still running or failed; poll, inspect the process, and capture its real exit status/stack. |

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

## Quick Test Commands

For ordinary package checks:

```bash
go build ./pkg/target/...
go vet ./pkg/target/...
go test -v -count=1 -timeout 120s ./pkg/target/...
go test -v -count=1 -run TestXxx ./pkg/target/...
```

For CGo-transitive or CGo-direct packages, do not guess flags. Read [references/cgo-build-test.md](references/cgo-build-test.md).

For local CPU CGo tests, prefer the deterministic wrapper:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/target/...
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=240s ./pkg/target/...
```

Rule: "`go build` passes" does not mean "`go test` will pass." Test binaries link more CGo.

## Operator / Pipeline Rules

Read [references/operator-pipeline.md](references/operator-pipeline.md) before changing these paths.

- `Call()` processes one batch. It must not send terminal signals (`End`, `Error`, `Abort`).
- `Reset()` performs cleanup and notifies downstream completion.
- If explicit typed signals replaced implicit nil-batch end-of-stream, use `Abort()` instead of `CloseWithTimeout()`.
- `CloseWithTimeout()` waits for nil batches; with typed signals it can wait for something that will never arrive.
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

Before declaring any MatrixOne code change done, check all boxes:

```
□ go build ./pkg/.../modified_package...    -> exit 0
□ go vet ./pkg/.../modified_package...      -> exit 0
□ go test -v -count=1 ./pkg/.../...         -> exit 0, no hangs
□ git diff --stat                            -> inspected, no unintended files
□ Regression: at least one test from dependent package passes
□ all evidence is newer than the last semantic edit/rebase and has a real exit code
```

Hang = failure. If `go test` produces >10s of no output, investigate instead of calling it slow.

Hard rule: never claim a failure is "pre-existing" without proving it from clean HEAD. Use the clean-tree reproduction protocol in [references/cgo-build-test.md](references/cgo-build-test.md) section 5.

## Common Diagnosis Shortcuts

| Symptom | First Place To Look |
|---------|---------------------|
| Test hangs exactly 30s | `CloseWithTimeout` waiting for nil-batch that never arrives. Read [operator-pipeline.md](references/operator-pipeline.md). |
| Test hangs >5s, no output | Deadlock or blocking channel send. Check `done` channel and non-blocking `select`. |
| `context deadline exceeded` after 30s | Did all senders call `Reset()` and send typed terminal signals? |
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `Undefined symbols` / `undefined symbol:` | `CGO_LDFLAGS` and stale `libusearch_c`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `cannot find -lmo` / `ld: library 'mo' not found` | `-ldflags="-extldflags '-L... -lmo'"`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `dyld`/loader searches a temporary `go-build.../lib` directory | A package-relative rpath was used for a temporary test binary; use the CGo test wrapper or absolute test rpaths. |
| Only linker warnings appear, no PASS/FAIL | Check the returned session and live test process; do not infer success from partial output. |
| `unsupported index type: ivfpq|cagra` | CPU binary lacks GPU plugin registration; read [index-plugin.md](references/index-plugin.md) and GPU notes. |

## Forbidden Patterns

1. Never send terminal signals (`End`, `Error`, `Abort`) from `Call()`.
2. Never call `sp.CloseWithTimeout()` after switching to explicit typed terminal signals.
3. Never claim "pre-existing" without `git stash` proof.
4. Never declare done without fresh test output.
5. Never assume `go build` success means `go test` will pass.
6. Never skip bottom-up testing: pure Go -> CGo-transitive -> CGo-direct.
7. Never add a per-algorithm `switch`/`if` on an index algo name in the SQL layer. Route through `indexplugin.Get(algo)`.
8. Never use distributable-binary relative rpaths as proof that a temporary `go test` binary can load its libraries.
