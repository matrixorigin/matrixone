---
name: mo-dev
description: MatrixOne database kernel development - CGo build/test environment setup, operator lifecycle contracts (Call/Reset), pipeline protocol, layered testing strategy. Use when modifying colexec operators, process signal types, compile pipeline construction, or debugging CGo link errors (undefined symbols, missing headers, library not found).
compatibility:
  agents: Codex CLI and compatible agents
  requires:
    - Go 1.22+
    - GNU Make
    - C/C++ toolchain (gcc/clang)
    - pre-built thirdparties
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  language: go
  cgo: true
---

## Enforcement Gates

This skill gates four decision points. Consult the corresponding section before acting:

| Gate | When | Action |
|------|------|--------|
| **G-MODIFY** | Before editing any `colexec` operator or `process` signal type | Read §3 (operator contract) + §7 (forbidden patterns) |
| **G-CGO-ERR** | Any build/test returns `file not found`, `Undefined symbols`/`undefined symbol:`, or `dyld:`/`error while loading shared libraries:` | Read §2.2 (symptom table) + §2.4 (stash protocol) |
| **G-DONE** | Before declaring "done"/"complete"/"passes" | Read §4.3 (completion gate) — all 5 boxes MUST be checked |
| **G-TEST-FAIL** | `go test` returns non-zero or hangs >10s | Read §5 (diagnosis) + §2.4 (stash protocol) before attributing cause |

---

## 1. Project Structure

```
pkg/sql/compile/       ← DAG compilation, operator instantiation, pipeline build, launch
pkg/sql/colexec/       ← execution operators (connector/dispatch/merge/join/scan/...)
pkg/vm/process/        ← execution context (Process), WaitRegister, signal channels
pkg/vm/pipeline/       ← Pipeline lifecycle management
pkg/container/         ← Batch/Vector/pSpool and other base data structures
pkg/frontend/          ← MySQL protocol compatibility layer
pkg/txn/               ← transaction management and MVCC
pkg/sql/plan/          ← query plan construction
cgo/                   ← CGo adapter layer (libmo.dylib / libmo.so)
```

**Operator file convention** under `pkg/sql/colexec/<op>/`:
- `types.go` — Arg struct definition
- `<op>.go` — main logic (Prepare/Call/Reset)
- Optional `sendfunc.go`/`dispatch.go` helpers

**Key dependency chain**: `compile` instantiates operators → `colexec` executes → `process` manages context and signals → `container` carries data.

---

## 2. Build, Test, Verify

### 2.1 Basic Commands

```bash
# Compile check
go build ./pkg/target/...

# Static analysis
go vet ./pkg/target/...

# Run tests (-count=1 disables cache)
go test -v -count=1 ./pkg/target/...

# Single test
go test -v -count=1 -run TestXxx ./pkg/target/...

# Timeout control (integration tests can be slow)
go test -v -count=1 -timeout 120s ./pkg/target/...
```

### 2.2 CGo Environment (Three-Layer Variables)

MO's CGo involves three independent layers: **compilation** (headers), **linking own lib** (`libmo`), and **third-party libs** (`libusearch_c`).

```makefile
# Makefile:203 — header paths for compilation
CGO_OPTS := CGO_CFLAGS="-I$(CGO_DIR) -I$(THIRDPARTIES_INSTALL_DIR)/include"

# Makefile:204-207 — link libmo (rpath varies by OS)
# Linux:   -Wl,-rpath,$$ORIGIN/lib
# macOS:   -Wl,-rpath,@executable_path/lib
GOLDFLAGS := -ldflags="-extldflags '-L$(CGO_DIR) -lmo -L$(THIRDPARTIES_INSTALL_DIR)/lib $(RPATH)'"
```

**Note**: Makefile does **not** set `CGO_LDFLAGS` — `libmo` link flags all go through `-ldflags` → `-extldflags`. However, the `usearch` Go module ships its own `#cgo LDFLAGS: -lusearch_c`, causing the linker to prefer system paths. `CGO_LDFLAGS` overrides this.

#### macOS vs Linux

| Aspect | macOS | Linux |
|--------|-------|-------|
| Dynamic library | `libmo.dylib` | `libmo.so` |
| Runtime path env | `DYLD_LIBRARY_PATH` | `LD_LIBRARY_PATH` |
| rpath flag | `-Wl,-rpath,@executable_path/lib` | `-Wl,-rpath,\$ORIGIN/lib` |
| C header flags | `CGO_CFLAGS="-I{root}/cgo -I{root}/thirdparties/install/include"` | Same |
| usearch link flags | `CGO_LDFLAGS="-L{root}/thirdparties/install/lib -lusearch_c"` | Same |

#### Full Test Commands

**macOS:**
```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

**Linux:**
```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

#### Symptom → Root Cause

| Symptom | Missing Variable | Root Cause |
|---------|-----------------|------------|
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS` | Compiler can't find thirdparties headers |
| `Undefined symbols: _usearch_hardware_acceleration_*` (macOS) or `undefined symbol:` (Linux) | `CGO_LDFLAGS` | usearch module's `#cgo LDFLAGS` found old `libusearch_c` |
| `ld: library 'mo' not found` (macOS) or `cannot find -lmo` (Linux) | `-ldflags="-extldflags '-L... -lmo'"` | Linker can't find `libmo` |
| `dyld: Library not loaded: libmo.dylib` (macOS) or `error while loading shared libraries: libmo.so` (Linux) | `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Runtime can't find dynamic library |

### 2.3 Layered Testing Strategy

| Layer | Example Packages | CGo Behavior | Variables Needed |
|-------|-----------------|-------------|-----------------|
| **Pure Go** | `pkg/vm/process`, `pkg/container/pSpool`, `pkg/vm/pipeline` | Zero CGo in transitive closure | None |
| **CGo-transitive** | `pkg/sql/colexec/connector`, `dispatch`, `merge` | Test binary links usearch Go module | `CGO_CFLAGS` + `CGO_LDFLAGS` |
| **CGo-direct** | `pkg/sql/compile` | Test binary directly links `libmo` + `libusearch_c` | All four: CGO_CFLAGS + CGO_LDFLAGS + ldflags + DYLD/LD_LIBRARY_PATH |
| **Integration** | `pkg/frontend`, cmd packages | Full MO binary, needs external services | All + services |

**Copy-paste per layer:**

Layer 1 (Pure Go):
```bash
go test -v -count=1 -timeout 120s ./pkg/vm/process/... ./pkg/container/pSpool/...
```

Layer 2 (CGo-transitive, macOS):
```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
go test -v -count=1 -timeout 120s ./pkg/sql/colexec/connector/... ./pkg/sql/colexec/dispatch/...
```

Layer 3 (CGo-direct, macOS):
```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -v -count=1 -timeout 120s ./pkg/sql/compile/...
```

#### Variable → Purpose

| Variable | What It Does | When Needed |
|---------|-------------|-------------|
| `CGO_CFLAGS` | Tells C compiler where to find headers | Package in transitive closure has CGo C code |
| `CGO_LDFLAGS` | Overrides usearch module's own `#cgo LDFLAGS` path | usearch Go module is linked |
| `-ldflags "-extldflags ..."` | Tells external linker where to find `libmo` + sets rpath | Package directly links `libmo` |
| `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Tells OS runtime loader where to find `.dylib` / `.so` | Test binary loads `libmo` at runtime |

**Why bottom-up matters**: Pure Go tests finish in seconds with zero env setup. If they fail, the problem is in your code — not CGo. Debug top-down wastes time chasing link errors when there's a real bug.

#### 2.3.1 `go build` vs `go test` — Not Equivalent

| Command | CGo Behavior |
|---------|-------------|
| `go build ./pkg/sql/colexec/connector/...` | May succeed without CGo flags (compiles package only, no test binary linking) |
| `go test ./pkg/sql/colexec/connector/...` | Compiles AND links test binary — full CGo path fires |

**Rule**: "`go build` passes" does not mean "`go test` will pass." Always verify with `go test`.

### 2.4 Stash Protocol — Verify "Pre-existing" Claims

**Hard rule**: Never claim a test failure is "pre-existing" without proving it.

```bash
# 1. Stash current changes
git stash

# 2. Run the same test from clean state
go test -v -count=1 -timeout 120s ./pkg/target/...

# 3. If it fails: genuinely pre-existing — document it
# 4. If it passes: YOUR code caused it — investigate
git stash pop
```

---

## 3. Operator Lifecycle Contracts

### 3.1 Call() vs Reset() — Hard Boundary

| Method | Purpose | Must NOT Do |
|--------|---------|-------------|
| `Call()` | Process one batch. Receive from upstream, compute, send downstream. | Send terminal signals (End/Error/Abort). That's `Reset()`'s job. |
| `Reset()` | Cleanup. Notify downstream the operator is done. | Block waiting for receiver acknowledgment. Use Abort(), not CloseWithTimeout(). |

**This is the single most common source of subtle bugs.** Violating it causes: premature pipeline termination, spurious timeouts, dead receivers.

### 3.2 PipelineSpool Lifecycle

| Method | Behavior | Use When |
|--------|---------|---------|
| `Close()` | Wait for all consumers to acknowledge end of stream (consumes nil batches from spool) | Graceful completion path |
| `CloseWithTimeout()` | Same as `Close()` but with timeout | Legacy cleanup — deprecated for typed signals |
| `ForceCleanup()` / `Abort()` | Synchronous release of all un-consumed slots. No waiting. Idempotent (`sync.Once`). | Cleanup path with explicit terminal signals |

**Critical rule**: If you replaced implicit nil-batch end-of-stream with explicit typed signals (EventEnd/EventError/EventAbort), use `Abort()` instead of `CloseWithTimeout()`. `CloseWithTimeout()` waits for nil batches that will never arrive.

### 3.3 Pipeline Signal Types (Explicit Protocol)

| Signal | Constructor | Meaning |
|--------|------------|---------|
| `EventData` | `NewDataSignal(batch)` | Normal data batch |
| `EventEnd` | `NewEndSignal()` | Graceful end of stream |
| `EventError` | `NewErrorSignal(err)` | Operator encountered an error |
| `EventAbort` | `NewAbortSignal()` | Forceful termination |

**Dual-protocol compatibility**: `GetNextBatch` handles both explicit typed signals AND legacy nil-batch convention (`content == nil`). Old operators using implicit nil-batch continue to work.

---

## 4. Post-Modification Verification

### 4.1 Test Freshness

Test output must be **from the current turn**. Verify:
1. `go test` command appears in current turn's bash calls
2. Exit code 0 (not "it compiled" or "trust me")
3. Timestamp is after the last `str_replace`/`write_file`

### 4.2 Completion Gate (G-DONE)

Before declaring any change "done", all 5 boxes must be checked:

```
□ go build ./pkg/.../modified_package...    → exit 0
□ go vet ./pkg/.../modified_package...      → exit 0
□ go test -v -count=1 ./pkg/.../...         → exit 0, no hangs
□ git diff --stat                            → inspected, no unintended files
□ Regression: at least one test from dependent package passes
```

**Hang = failure**: If `go test` produces >10s of no output → the test is hung, not "still running." Investigate.

---

## 5. Fault Diagnosis

| Symptom | Look At |
|---------|---------|
| Test hangs exactly 30s | `CloseWithTimeout` waiting for nil-batch that never arrives. Check if typed signals are being sent instead. |
| Test hangs >5s, no output | Deadlock or blocking channel send. Check: is `done` channel closed? Is `sendSignal` using a non-blocking `select`? |
| `context deadline exceeded` after 30s | `WaitingEndWithTimeout` timed out. Check: did all senders call `Reset()`? Did they send typed terminal signals? |
| `CGO_CFLAGS` not working | Run `go env CGO_CFLAGS` to verify. Use `export` (not inline without export) if the package has sub-packages. |

---

## 6. Common Pitfalls

### 6.1 Structural Changes — Check Both Ends
When changing the communication protocol between operators (connector ↔ merge, dispatch ↔ merge), both sender and receiver must be updated. A sender change without the corresponding receiver change causes deadlock.

**Identification**: exact 30s timeout in cleanup → `CloseWithTimeout` + typed signal mismatch.

### 6.2 CGo Link Errors — Verify the Third Party Build
CGo link errors (`Undefined symbols`, `cannot find -lmo`) are environment issues, not code bugs. The C shared libraries (`libmo.dylib`, `libusearch_c.dylib`) must be pre-built via `make cgo` and `make thirdparties`.

### 6.3 Pipeline Cleanup — Abort, Don't Wait
During pipeline cleanup, operators should use non-blocking or timeout-gated communication. Never block waiting for a receiver that may have already exited.

### 6.4 Channel Full Edge Cases
When sending terminal signals into a bounded channel, the send may fail because the channel is full. Ensure the terminal state is still recorded even when the channel send fails — the `done` channel must close regardless of delivery success.

---

## 7. Forbidden Patterns

1. **Never send terminal signals (End/Error/Abort) from `Call()`.** Only `Reset()` sends terminal signals.
2. **Never call `sp.CloseWithTimeout()` after switching to explicit typed terminal signals.** Use `sp.Abort()`.
3. **Never claim "pre-existing" without `git stash` proof.** Run the test on clean HEAD first.
4. **Never declare done without fresh test output.** All 5 completion gate boxes must be checked.
5. **Never assume `go build` success means `go test` will pass.** Build only compiles packages; test compiles AND links test binaries.
6. **Never skip bottom-up testing.** Start with pure Go packages, then CGo-transitive, then CGo-direct.
