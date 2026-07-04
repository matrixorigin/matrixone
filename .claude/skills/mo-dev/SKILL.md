---
name: mo-dev
description: MatrixOne database kernel development - CGo build/test environment setup, GPU builds (MO_CL_CUDA=1 / cuVS), operator lifecycle contracts (Call/Reset), pipeline protocol, layered testing strategy, and the vector/fulltext index-plugin framework (pkg/indexplugin AlgoPlugin registry). Use when modifying colexec operators, process signal types, compile pipeline construction, adding or editing an index algorithm (HNSW/IVFFLAT/IVF-PQ/CAGRA/fulltext), or debugging CGo link errors (undefined symbols, missing headers, library not found).
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
| CGo link/header/runtime errors, layered testing, "pre-existing" test claims, GPU/cuVS/CUDA builds | [references/cgo-build-test.md](references/cgo-build-test.md) |
| `colexec` operator edits, `process` signal types, pipeline spools, Call/Reset cleanup, hung tests | [references/operator-pipeline.md](references/operator-pipeline.md) |
| Vector/fulltext index algorithm work, plugin registry, GPU-only algorithm registration, index-plugin review | [references/index-plugin.md](references/index-plugin.md) |

## Enforcement Gates

Consult the referenced material before acting:

| Gate | When | Action |
|------|------|--------|
| **G-MODIFY** | Before editing any `colexec` operator or `process` signal type | Read [operator-pipeline.md](references/operator-pipeline.md). |
| **G-CGO-ERR** | Any build/test returns `file not found`, `Undefined symbols`/`undefined symbol:`, `dyld:`, or `error while loading shared libraries:` | Read [cgo-build-test.md](references/cgo-build-test.md) sections 2 and 4 before blaming code. |
| **G-GPU** | Before a GPU build/test (`MO_CL_CUDA=1`), or on CUDA/cuVS errors (`CONDA_PREFIX`, `nvcc`, `-lcuvs`/`-lcudart`, `unsupported index type: ivfpq\|cagra`) | Read [cgo-build-test.md](references/cgo-build-test.md) section 5. |
| **G-IDXPLUGIN** | Before adding/editing an index-algorithm plugin, OR adding any `switch`/`if` on an index **algo** name in `pkg/sql/{compile,plan}` or `pkg/catalog` | Read [index-plugin.md](references/index-plugin.md). Route through `pkg/indexplugin`; new algo switches are forbidden. |
| **G-IDXREVIEW** | Reviewing a diff that touches index-algorithm dispatch, `pkg/vectorindex/<algo>/plugin/`, `pkg/fulltext/plugin`, or `pkg/indexplugin` | Read [index-plugin.md](references/index-plugin.md) section 9 and run its greps. |
| **G-DONE** | Before declaring "done"/"complete"/"passes" | Apply the completion gate below. |
| **G-TEST-FAIL** | `go test` returns non-zero or hangs >10s | Read [operator-pipeline.md](references/operator-pipeline.md) section 4 and [cgo-build-test.md](references/cgo-build-test.md) section 4 before attributing cause. |

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
```

Hang = failure. If `go test` produces >10s of no output, investigate instead of calling it slow.

Hard rule: never claim a failure is "pre-existing" without proving it from clean HEAD. Use the stash protocol in [references/cgo-build-test.md](references/cgo-build-test.md) section 4.

## Common Diagnosis Shortcuts

| Symptom | First Place To Look |
|---------|---------------------|
| Test hangs exactly 30s | `CloseWithTimeout` waiting for nil-batch that never arrives. Read [operator-pipeline.md](references/operator-pipeline.md). |
| Test hangs >5s, no output | Deadlock or blocking channel send. Check `done` channel and non-blocking `select`. |
| `context deadline exceeded` after 30s | Did all senders call `Reset()` and send typed terminal signals? |
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `Undefined symbols` / `undefined symbol:` | `CGO_LDFLAGS` and stale `libusearch_c`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `cannot find -lmo` / `ld: library 'mo' not found` | `-ldflags="-extldflags '-L... -lmo'"`; read [cgo-build-test.md](references/cgo-build-test.md). |
| `unsupported index type: ivfpq|cagra` | CPU binary lacks GPU plugin registration; read [index-plugin.md](references/index-plugin.md) and GPU notes. |

## Forbidden Patterns

1. Never send terminal signals (`End`, `Error`, `Abort`) from `Call()`.
2. Never call `sp.CloseWithTimeout()` after switching to explicit typed terminal signals.
3. Never claim "pre-existing" without `git stash` proof.
4. Never declare done without fresh test output.
5. Never assume `go build` success means `go test` will pass.
6. Never skip bottom-up testing: pure Go -> CGo-transitive -> CGo-direct.
7. Never add a per-algorithm `switch`/`if` on an index algo name in the SQL layer. Route through `indexplugin.Get(algo)`.
