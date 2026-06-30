# MatrixOne Development Skill (mo-dev)

## Trigger
Activated automatically when operating in the MatrixOne codebase.

---

## Enforcement Gates (READ FIRST)

This skill is not optional reference — activate it at these **mandatory checkpoints**:

| Gate | When | Action |
|------|------|--------|
| **G-MODIFY** | Before any `str_replace`/`write_file` on a `colexec` operator or `process` signal type | Read §3 (operator contract) + §7 (forbidden patterns) |
| **G-CGO-ERR** | Any build/test returns `file not found`, `Undefined symbols`/`undefined symbol:`, or `dyld:`/`error while loading shared libraries:` | Read §2.2 (symptom table) + §2.4 (stash protocol) |
| **G-DONE** | Before declaring "done"/"complete"/"passes" | Read §4.3 (completion gate) — all 5 boxes MUST be checked |
| **G-TEST-FAIL** | `go test` returns non-zero or hangs >10s | Read §5 (diagnosis) + §2.4 (stash protocol) before attributing cause |

**How to use**: At each gate, verify the corresponding section's conditions. If a condition fails, fix it before proceeding. Never skip a gate.

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
cgo/                   ← CGo adapter layer (libmo.dylib)
```

**Operator file convention**: under `pkg/sql/colexec/<op>/`:
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

MO's CGo involves **compilation** (headers), **linking** (own library `libmo`), and **third-party libraries** (`libusearch_c`) as three independent layers. Corresponding Makefile config:

```makefile
# Makefile:203 — header paths for compilation
CGO_OPTS := CGO_CFLAGS="-I$(CGO_DIR) -I$(THIRDPARTIES_INSTALL_DIR)/include"

# Makefile:204-207 — link libmo (rpath varies by OS)
# Linux:   -Wl,-rpath,$$ORIGIN/lib
# macOS:   -Wl,-rpath,@executable_path/lib
GOLDFLAGS := -ldflags="-extldflags '-L$(CGO_DIR) -lmo -L$(THIRDPARTIES_INSTALL_DIR)/lib $(RPATH)'"
```

**Note**: Makefile does **not** set `CGO_LDFLAGS` — `libmo` link flags all go through `-ldflags` → `-extldflags`. However, the `usearch` Go module ships its own `#cgo LDFLAGS: -lusearch_c`, causing the linker to prefer system paths which may find an outdated version missing required symbols. In that case, `CGO_LDFLAGS` is needed to point to the MO-compiled version.

#### macOS vs Linux — All Differences

| Aspect | macOS | Linux |
|--------|-------|-------|
| Dynamic library name | `libmo.dylib` | `libmo.so` |
| Third-party lib prefix | `libusearch_c.dylib` | `libusearch_c.so` |
| Linker flag (make static) | `gcc -dynamiclib` | `gcc -shared` |
| Runtime library path env | `DYLD_LIBRARY_PATH` | `LD_LIBRARY_PATH` |
| rpath in ldflags | `@executable_path/lib` | `$$ORIGIN/lib` (double-escape in Makefile) |
| `-rpath` flag | `-Wl,-rpath,@executable_path/lib` | `-Wl,-rpath,\$\$ORIGIN/lib` or `-Wl,-rpath,$ORIGIN/lib` in shell |
| C header flags | `CGO_CFLAGS="-I.../cgo -I.../thirdparties/install/include"` | **Same** |
| usearch link flags | `CGO_LDFLAGS="-L.../thirdparties/install/lib -lusearch_c"` | **Same** |

#### Full Test Environment Variables

**macOS:**
```bash
# 1. Headers (required for any package with CGo code)
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"

# 2. libusearch_c (required for packages needing usearch symbols)
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"

# 3. Runtime dynamic library (required when running tests)
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib:$DYLD_LIBRARY_PATH"

# go test one-liner:
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

**Linux:**
```bash
# 1. Headers
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"

# 2. libusearch_c
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"

# 3. Runtime dynamic library
export LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib:$LD_LIBRARY_PATH"

# go test one-liner:
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

#### Symptom → Root Cause Quick Reference

| Symptom (macOS) | Symptom (Linux) | Missing Variable | Root Cause |
|-----------------|-----------------|-----------------|------------|
| `fatal error: 'xxhash.h' file not found` | **Same error** | `CGO_CFLAGS` | Compiler can't find thirdparties headers |
| `Undefined symbols: _usearch_hardware_acceleration_*` | **Same error** (`undefined symbol:` prefix) | `CGO_LDFLAGS` | usearch module's `#cgo LDFLAGS` found an old `libusearch_c` without this symbol |
| `ld: library 'mo' not found` | `/usr/bin/ld: cannot find -lmo` | `-ldflags="-extldflags '-L... -lmo'"` | Linker can't find `libmo.dylib`/`libmo.so` |
| `dyld: Library not loaded: libmo.dylib` | `error while loading shared libraries: libmo.so` | `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Runtime can't find dynamic library |

### 2.3 Layered Testing Strategy

MO packages fall into four layers by CGo dependency depth. **Must validate bottom-up**.

#### Layer Map

| Layer | Example Packages | What Happens at `go test` | Variables Needed |
|-------|-----------------|--------------------------|-----------------|
| **Pure Go** | `pkg/vm/process`, `pkg/container/pSpool`, `pkg/vm/pipeline` | Compiles + links test binary. Zero CGo code in the transitive closure. | **None** |
| **CGo-transitive** | `pkg/sql/colexec/connector`, `dispatch`, `merge` | Test binary links `usearch` Go module, which has `#cgo LDFLAGS: -lusearch_c`. Full CGo compile + link path fires. | `CGO_CFLAGS` + `CGO_LDFLAGS` |
| **CGo-direct** | `pkg/sql/compile`, `pkg/sql/colexec/...` (some) | Test binary directly links `libmo` + `libusearch_c`. Both libraries must be findable at build-time AND loadable at run-time. | `CGO_CFLAGS` + `CGO_LDFLAGS` + `-ldflags "-extldflags ..."` + `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH` |
| **Integration** | `pkg/frontend`, cmd packages | Full MO binary. Needs external services (MySQL protocol, storage, etc). | All + services |

#### Copy-Paste Commands Per Layer

**Layer 1 — Pure Go** (nothing needed):
```bash
go test -v -count=1 -timeout 120s ./pkg/vm/process/... ./pkg/container/pSpool/...
```

**Layer 2 — CGo-transitive** (needs headers + usearch link):
```bash
# macOS
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
go test -v -count=1 -timeout 120s ./pkg/sql/colexec/connector/... ./pkg/sql/colexec/dispatch/...

# Linux
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
go test -v -count=1 -timeout 120s ./pkg/sql/colexec/connector/...
```

**Layer 3 — CGo-direct** (all four — macOS):
```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -v -count=1 -timeout 120s ./pkg/sql/compile/...
```

Layer 3 — Linux:
```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
export LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib'" \
  -v -count=1 -timeout 120s ./pkg/sql/compile/...
```

#### Variable → Purpose Quick Card

| Variable | What It Does | When Needed |
|---------|-------------|-------------|
| `CGO_CFLAGS` | Tells the C compiler where to find headers (`xxhash.h`, `mo.h`, etc.) | Anytime a package in the transitive closure has CGo C code |
| `CGO_LDFLAGS` | Tells the Go linker where to find `libusearch_c` (overrides the outdated path in usearch module's own `#cgo LDFLAGS`) | Anytime the usearch Go module is linked (most `colexec` packages) |
| `-ldflags "-extldflags ..."` | Tells the external linker where to find `libmo` + sets rpath for runtime | Only when the package directly links `libmo` (CGo-direct layer) |
| `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Tells the OS runtime loader where to find `.dylib` / `.so` files | Only when the test binary loads `libmo` (CGo-direct layer) |

**Why this order matters**: Pure Go package tests are fastest (seconds), and failures are 100% code issues. If pure Go fails, don't waste time debugging CGo link errors on upper layers.

#### 2.3.1 `go build` vs `go test` — Not the Same

| Command | What It Does | CGo Impact |
|---------|-------------|------------|
| `go build ./pkg/sql/colexec/connector/...` | Compiles the **package**, does not link a test binary | May succeed without CGo flags |
| `go test ./pkg/sql/colexec/connector/...` | Compiles and links a **test binary** including all transitive deps | May fail if transitive deps need CGo symbols |

**Consequence**: `go build` passing does NOT prove `go test` will pass. When adding a new import chain that reaches `cgo/`, `go build` may still succeed while `go test` breaks. Only `go test` output is authoritative.

### 2.4 Isolate Environment Issues (Stash Verification Protocol)

```bash
# Step 1: Save current changes
git stash

# Step 2: Run same test with same variables
CGO_CFLAGS="..." go test -count=1 ./failing/package/...

# Step 3a: Still fails → environment issue, unrelated to code changes
# Step 3b: Passes       → code change introduced the problem

# Step 4: Restore changes
git stash pop
```

**Hard rule**: A "pre-existing issue" claim without stash verification is not credible. When seeing `Undefined symbols` or `file not found`, never assert "this is pre-existing" — must prove it via stash.

---

## 3. Operator Lifecycle

MatrixOne `colexec` operators follow a three-phase lifecycle:

| Phase | Method | When Called | Constraint |
|-------|--------|-------------|------------|
| Init | `Prepare()` | After DAG compile, before first execution | Allocate Reg, init state |
| Execute | `Call()` | Called repeatedly as each batch of data arrives | Data forwarding only, **never send terminal signals** |
| Cleanup | `Reset()` | During Pipeline Cleanup phase | Release resources, **solely responsible for sending terminal signals** |

**Hard boundary between Call() and Reset()**: This is the most important operator contract in MO. `Call()` handles the "still has data" path. `Reset()` handles the "completely done" path. Confusing the two causes receivers to terminate early or dead-wait for timeout.

---

## 4. Code Change Workflow

### 4.1 Before Changing

1. Read target files to confirm current content
2. `grep` for all references to the symbol being changed (code + tests)
3. Confirm whether each reference is affected by the change
4. List packages to test manually (starting from bottom pure-Go packages)

### 4.2 After Changing (Strict Order)

```
go build ./pkg/A/... ./pkg/B/...    ← Compile (all changed packages)
go vet ./pkg/A/... ./pkg/B/...      ← Static check
go test -v -count=1 ./pkg/A/...     ← Unit tests (bottom-up)
go test -v -count=1 ./pkg/B/...
git diff --stat                      ← Confirm no unexpected file changes
go test -count=1 ./pkg/upstream/...  ← Regression tests
```

**Hard rule**: Test output must be fresh from this turn's `go test -count=1`. Results from previous turns are stale and cannot be used to justify "it passes."

**Freshness verification**: A test result is only valid if ALL of these hold:
1. The `go test -count=1` command ran in the **current turn** (not a previous turn)
2. The output includes the **test binary's own timestamp** (e.g., `ok  pkg/vm/process  0.123s`), not just a cached summary
3. If the test package is CGo-transitive, the command includes the full `CGO_CFLAGS` + `CGO_LDFLAGS` + `DYLD_LIBRARY_PATH` prefix

**Anti-pattern**: Seeing `go test` output in context and concluding "tests pass" without checking whether that output is from the current turn or a stale earlier turn.

### 4.3 Completion Gate

Before claiming any change is "done" or "complete", all of the following MUST be true:

```
□ All changed packages pass go build
□ All changed packages pass go vet
□ All changed packages pass go test -count=1 (fresh output from this turn)
□ No test hung >10s (each package must print "ok" or "FAIL" within expected time)
□ git diff --stat shows only intended files
□ Upstream regression tests pass or failures are confirmed pre-existing via stash
```

**Never claim done when any box is unchecked.** If a test fails, fix it; if an environment blocks testing, state it explicitly rather than implying it passed.

**Test hang is a failure**: A test that produces no output for >10s is either deadlocked or blocked. It is not "still running" — it is a bug. Common MO causes:
- `sp.CloseWithTimeout()` waiting for nil-batch that never arrives → exactly 30s hang
- goroutine blocked on channel send/receive with no timeout → infinite hang
- `context.TODO()` used where a real context with deadline was needed → infinite hang

---

## 5. Test Failure Diagnosis

### 5.1 Test Hangs With No Output (>10s)

```
→ goroutine blocked or deadlocked
→ Analyze the wait chain: who is blocking on select/recv?
→ Check whether the channel communication peer is working
```

### 5.2 Exact 30s Timeout

```
→ Some wait path triggered context/time.Sleep timeout
→ Usually waiting for a signal or data that will never arrive
→ Check whether upstream changed send timing
```

### 5.3 CGo Link Error

```
→ First, stash-verify it's not a code issue (see §2.4)
→ Environment: DYLD_LIBRARY_PATH, libmo.dylib version
→ Code: check whether cgo/lib.go was accidentally modified
```

### 5.4 Vet Error

Do not ignore. Packages that pass vet may still have runtime bugs, but packages that **fail** vet almost certainly have code problems.

---

## 6. MO Common Pitfalls

1. **Operators are modified in pairs**: connector/dispatch (send side) and merge (receive side) share a communication protocol. Changing one side must check the other.

2. **Protocol migration compatibility window**: When adding new signal types, the old path must remain compatible until all callers are migrated.

3. **Channel capacity**: `make(chan T, N)` — N determines buffer size. N=0 means senders drop data when receivers are not ready. Check whether channel capacity fits the use case.

4. **Resource ownership**: `Batch` in MO is a heavyweight object. Confirm who creates it, who holds the last reference, and who is responsible for `Clean()`.

---

## 7. Forbidden Patterns

These are HARD rules. Violating any of them has caused real bugs in past MO development sessions.

| # | Pattern | Why Forbidden |
|---|---------|---------------|
| 7.1 | Modifying `Call()` to send terminal signals | Violates the operator lifecycle contract (§3). Terminal signals go in `Reset()` only. |
| 7.2 | Using `sp.CloseWithTimeout()` after switching from implicit nil-batch to explicit signals | `CloseWithTimeout` waits for a nil-batch that will never arrive → guaranteed 30s timeout |
| 7.3 | Claiming a test failure is "pre-existing" without `git stash` proof | The failure may actually be caused by your change. §2.4 protocol is mandatory. |
| 7.4 | Adding `CGO_CFLAGS` directives to `cgo/lib.go` | This file is committed to the repo. Environment-specific paths belong in shell variables, not source files. |
| 7.5 | Declaring completion without fresh `go test -count=1` output in the current turn | Stale test results from earlier turns do not prove the current code works. |
| 7.6 | Changing protocol on one side of a sender/receiver pair without checking the other | Every sender has a corresponding receiver. connector↔merge, dispatch↔merge. |
