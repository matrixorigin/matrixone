---
name: mo-dev
description: MatrixOne database kernel development - CGo build/test environment setup, GPU builds (MO_CL_CUDA=1 / cuVS), operator lifecycle contracts (Call/Reset), pipeline protocol, layered testing strategy, and the vector/fulltext index-plugin framework (pkg/indexplugin AlgoPlugin registry). Use when modifying colexec operators, process signal types, compile pipeline construction, adding or editing an index algorithm (HNSW/IVFFLAT/IVF-PQ/CAGRA/fulltext), or debugging CGo link errors (undefined symbols, missing headers, library not found).
compatibility: Designed for Codex CLI and compatible agents. Requires Go 1.22+, GNU Make, C/C++ toolchain (gcc/clang), and pre-built thirdparties.
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  language: go
  cgo: true
---

## Enforcement Gates

This skill gates seven decision points. Consult the corresponding section before acting:

| Gate | When | Action |
|------|------|--------|
| **G-MODIFY** | Before editing any `colexec` operator or `process` signal type | Read §3 (operator contract) + §7 (forbidden patterns) |
| **G-CGO-ERR** | Any build/test returns `file not found`, `Undefined symbols`/`undefined symbol:`, or `dyld:`/`error while loading shared libraries:` | Read §2.2 (symptom table) + §2.4 (stash protocol) |
| **G-GPU** | Before a GPU build/test (`MO_CL_CUDA=1`), or on CUDA/cuVS errors (`CONDA_PREFIX`, `nvcc`, `-lcuvs`/`-lcudart`, `unsupported index type: ivfpq\|cagra`) | Read §2.5 (GPU build) |
| **G-IDXPLUGIN** | Before adding/editing an index-algorithm plugin, OR adding any `switch`/`if` that branches on an index **algo** name (`IsIvfIndexAlgo`, `== "ivfpq"`, `KeyType`, …) in `pkg/sql/{compile,plan}` or `pkg/catalog` | Read §8 (index-plugin framework) — route through the registry; new algo switches are forbidden |
| **G-IDXREVIEW** | When **reviewing** a diff (yours or `/code-review`) that touches index-algorithm dispatch, any `pkg/vectorindex/<algo>/plugin/` / `pkg/fulltext/plugin`, or `pkg/indexplugin` | Read §9 (index-plugin review checklist) — run each grep before approving |
| **G-DONE** | Before declaring "done"/"complete"/"passes" | Read §4.2 (completion gate) — all 5 boxes MUST be checked |
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

### 2.5 GPU Build (`MO_CL_CUDA=1`) — cuVS / CUDA

GPU support compiles the CUDA-backed vector index algorithms (**CAGRA**, **IVF-PQ**) into `libmo` and turns on the `gpu` Go build tag. **Linux x86_64 only** — the macOS `Makefile` branch (`Makefile:234-236`) carries no CUDA flags, so **macOS builds are always CPU-only**. Do not try to enable it on Darwin.

**Prerequisites (one-time):**
1. CUDA toolkit 12.0 / 13.0+ installed under `/usr/local/cuda`.
2. cuVS Go bindings installed via conda (`rapidsai/cuvs`), then the env **activated** so `CONDA_PREFIX` is exported:
   ```bash
   conda env create --name go -f conda/environments/go_cuda-130_arch-$(uname -m).yaml
   conda activate go     # exports CONDA_PREFIX — every GPU build/test hard-errors without it
   ```

**Build:**
```bash
MO_CL_CUDA=1 make -j8
```

**What `MO_CL_CUDA=1` flips** (vs. the default CPU build):

| Layer | CPU build | GPU build (`MO_CL_CUDA=1`) |
|-------|-----------|----------------------------|
| Go build tag | none | `-tags gpu` (`Makefile:224`) — registers CAGRA + IVF-PQ, compiles `*_gpu.go` |
| `cgo/` compiler | `gcc`/`clang` | `/usr/local/cuda/bin/nvcc` (`cgo/Makefile:31`, multi-arch `sm_75…sm_90`) |
| `libmo` objects | C objects only | + `cuda/*.o` + `cuvs/*.o` |
| Link flags | `-lusearch_c -lroaring` | + `-lcuvs -lcuvs_c -lcudart -lcuda -lrmm -lstdc++` |
| Header/lib roots | thirdparties only | + `$CONDA_PREFIX/{include,lib}`, `/usr/local/cuda/...` |

**Guardrails baked into the Makefiles:**
- **`CONDA_PREFIX env variable not found`** → conda env not activated. Run `conda activate <env>` first. Both `Makefile:217` and `cgo/Makefile:28` hard-error on this — it is *not* a code bug.
- **`libmo` is re-linked on every GPU build** (`cgo/Makefile` order-only `cuda_objs`/`cuvs_objs` prereqs). Deliberate: `mo-service` loads `libmo.so` **dynamically**, so a stale `.so` silently runs old C++ (hours of "my fix didn't take"). The old `rm -f cgo/libmo.so cgo/libmo.a` workaround is no longer needed.

**The `gpu` tag gates index-plugin registration — memorize this.** CAGRA and IVF-PQ register **only** under `//go:build gpu` (`pkg/indexplugin/all/all_gpu.go`). On a CPU binary their plugins are absent from the registry, so `CREATE INDEX … USING ivfpq|cagra` fails cleanly at plan-build with `unsupported index type: <algo>` — **before** any hidden table is created. This is intentional. Do **not** "fix" it by moving those imports into `all.go` (see §8.6).

**Prerequisite — the linked `libmo` must itself be GPU-built:** run `MO_CL_CUDA=1 make -j8 cgo` first. A CPU `libmo` lacks the `cuda/`+`cuvs/` objects, so `gpu`-tagged Go (which cgo-includes `cgo/cuvs/*.h` via `pkg/cuvs`) fails to link with undefined `cuvs_*` / `cuda*` symbols — this is a *stale-library* error, not a flag error, so re-check §2.2 before chasing `CGO_LDFLAGS`.

**GPU tests** — add `-tags gpu` plus the CUDA search paths (mirror the Makefile's `CUDA_CFLAGS`/`CUDA_LDFLAGS`, `Makefile:220-223`); Linux only:
```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include -I$CONDA_PREFIX/include -I/usr/local/cuda/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c -L$CONDA_PREFIX/lib -lcuvs -lcuvs_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib:$CONDA_PREFIX/lib:/usr/local/cuda/lib64" \
go test -tags gpu \
  -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib -fopenmp'" \
  -v -count=1 -timeout 300s ./pkg/vectorindex/ivfpq/...
```
The **authoritative** flag source is the `Makefile` (`CUDA_CFLAGS`/`CUDA_LDFLAGS`), not this snippet — if a GPU link error appears, diff your flags against those lines.

**Tag-split test files are a trap:** `*_gpu.go` / `//go:build gpu` tests (e.g. `pkg/vectorindex/ivfpq/search_test.go`, `model_test.go`) compile **only** under `-tags gpu`. A plain `go test ./pkg/vectorindex/ivfpq/...` runs the `//go:build !gpu` / `*_cpu.go` stubs instead. **"CPU tests pass" ≠ "GPU path tested."**

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
7. **Never add a per-algorithm `switch`/`if` on an index algo name in the SQL layer.** Route through `indexplugin.Get(algo)` — see §8.

---

## 8. Vector / Fulltext Index-Plugin Framework

### 8.1 Why this exists (and what NOT to undo)

Adding a vector index used to mean editing 8+ files across `pkg/sql/compile`,
`pkg/sql/plan`, `pkg/catalog`, and `pkg/vectorindex`, wired through ~6
switch/if-chain seams. Miss one seam → the algorithm silently misbehaves. The
`pkg/indexplugin` framework collapses those seams into **one registry lookup**:
an algorithm registers **one** `AlgoPlugin`, and the compiler enforces every
required hook exists.

**The guarded failure mode:** a change that re-introduces a per-algorithm
`switch algo` / `if IsXxxIndexAlgo || …` in the SQL layer, or otherwise bypasses
the registry, quietly re-opens the "forgot a seam" bug class. Work **through**
the framework — do not route around it.

### 8.2 Architecture

`pkg/indexplugin` is the framework. It must **not** import `pkg/sql/{compile,plan}`;
hook methods receive narrow `Context` interfaces the SQL layer satisfies.

```
pkg/indexplugin/
├── plugin.go          -- AlgoPlugin interface; Register/Get/All; IsVectorIndexAlgo/IsFullTextIndexAlgo/IsPluginAlgo
├── catalog/hooks.go   -- catalog.Hooks   (metadata: params, hidden tables, op/vector/pk types, quantization)
├── compile/hooks.go   -- compile.Hooks   (DDL execution: create/reindex/drop/restore) + CompileContext
├── plan/hooks.go      -- plan.Hooks      (schema defs + tablefunc + thin ANN redirects) + PlanBuilder/CompilerContext
├── idxcron/hooks.go   -- idxcron.Hooks   (scheduled-rebuild gating: Updatable)
└── all/
    ├── all.go         -- blank-imports CPU-safe plugins (fulltext, hnsw, ivfflat)
    └── all_gpu.go     -- //go:build gpu — blank-imports GPU-only plugins (cagra, ivfpq)

pkg/vectorindex/<algo>/plugin/   -- one dir per algorithm; assembles the hooks into an AlgoPlugin
pkg/fulltext/plugin/             -- fulltext is a plugin too (kind = fulltext, not vector)
```

**Dispatch flow** — every SQL-layer site that used to `switch algo` now does:

```go
p, ok := indexplugin.Get(algo)      // case-insensitive, trims whitespace
if ok { return p.Compile().HandleCreateIndex(cctx, defs) }
```

`indexplugin.IsVectorIndexAlgo(algo)` replaces the
`IsIvfIndexAlgo || IsHnswIndexAlgo || IsCagraIndexAlgo || IsIvfpqIndexAlgo`
chain. Use `IsFullTextIndexAlgo` for fulltext, `IsPluginAlgo` for "registered at
all (vector OR fulltext)". Live dispatch sites already exist in
`pkg/sql/plan/build_ddl.go` (`indexplugin.Get(...KeyType.ToString())` →
`unsupported index type: %s`) and `pkg/sql/plan/apply_indices.go`
(`p.Plan().CanApply`).

### 8.3 Registry contract — the compiler is the safety net

```go
// pkg/indexplugin/plugin.go
type AlgoPlugin interface {
    Algo() string                       // must == catalog.MoIndex<X>Algo.ToString() (lower-cased)
    Catalog() catalogplugin.Hooks
    Compile() compileplugin.Hooks
    Plan()    planplugin.Hooks
    Idxcron() idxcronplugin.Hooks
}
func Register(p AlgoPlugin)              // panics on duplicate Algo(); call from init()
func Get(algo string) (AlgoPlugin, bool)
```

Each plugin keeps compile-time interface assertions:

```go
var _ plugin.AlgoPlugin = (*Plugin)(nil)   // in <algo>/plugin/plugin.go
var _ Hooks = Hooks{}                        // in each hooks sub-package impl
```

If `AlgoPlugin` or a `Hooks` interface gains a method and a plugin isn't updated,
these lines **stop the build**. That compile error *is* the "did I miss a seam?"
answer. **Never delete or `//nolint` these assertions to green a build** —
implement the missing method.

### 8.4 Plugin package layout (canonical: IVF-PQ)

`pkg/vectorindex/ivfpq/plugin/plugin.go` is the **authoritative walkthrough** —
read its package doc before adding an algorithm. A full plugin (see
`pkg/vectorindex/ivfflat/plugin/`) is:

```
pkg/vectorindex/<algo>/plugin/
├── plugin.go            -- assembles the 4 Hooks into one Plugin; init() { plugin.Register(New()) }
├── runtime/runtime.go   -- catalog.Hooks impl  (algorithm metadata constants)
├── compile/compile.go   -- compile.Hooks impl  (CREATE/ALTER/DROP/REINDEX execution)
├── plan/
│   ├── plan.go          -- plan.Hooks: thin ApplyForSort/CanApply redirect (~10 LoC) into *plan.QueryBuilder
│   ├── schema.go        -- BuildSecondaryIndexDefs body (hidden-table TableDefs + IndexDefs)
│   └── tablefunc.go     -- <algo>_create / <algo>_search FUNCTION_SCAN builder registrations
├── idxcron/idxcron.go   -- idxcron.Hooks impl  (Updatable: scheduled-rebuild gating)
└── iscp/iscp.go         -- CDC / import sync (may be //go:build gpu for GPU algos)
```

**Which sub-package gets lifted code:** from `pkg/sql/compile/*.go` → `compile/`;
from `pkg/sql/plan/*.go` → `plan/`; algorithm-metadata constants → `runtime/`
(lifted code goes in the sub-package matching `pkg/sql/<layer>` of the call site).

### 8.5 Adding a new algorithm — steps

Follow `pkg/vectorindex/ivfpq/plugin/plugin.go`'s doc. Summary:

1. **Catalog constants.** Add `MoIndex<X>Algo` in
   `pkg/catalog/secondary_index_utils.go`; hidden-table-type constants in
   `pkg/catalog/types.go` (one per hidden table). A `tree.INDEX_TYPE_<X>` parser
   keyword only if it needs new CREATE INDEX syntax.
2. **Copy** `pkg/vectorindex/ivfpq/plugin/` → `pkg/vectorindex/<x>/plugin/`;
   rename packages/imports.
3. **Implement the four Hooks** (let the `var _ Hooks = …` assertions tell you
   what's missing):
   - `catalog.Hooks` — `HiddenTableTypes`, `ParamsFromTree`, `DefaultOptions`,
     `SupportedOpTypes`, `SupportedVectorTypes`, `SupportedPrimaryKeyTypes`,
     `SupportedIncludeColumnTypes`, `ValidQuantization`, `ExperimentalFlag`,
     `AlterTableCloneBehavior`, `RestoreBehavior`, `BuildSessionVars`,
     `ShouldTruncateHiddenTable`, `SyncDescriptor`.
   - `compile.Hooks` — `HandleCreateIndex`, `HandleReindex`,
     `ValidateReindexParams`, `HandleDropIndex`, `RestoreInitSQL`,
     `IdxcronMetadata`.
   - `plan.Hooks` — `BuildSecondaryIndexDefs`, `BuildFullTextIndexDefs`,
     `CanApply`, `ApplyForSort`.
   - `idxcron.Hooks` — `Updatable` (return "always yes" if the algo has no
     minimum-size / cadence constraint).
4. **ANN rewrite body** (only if the algo supports `ORDER BY <distfn>(col,v)
   LIMIT k`): add `applyIndicesForSortUsing<X>` + `prepare<X>IndexContext` in
   `pkg/sql/plan/apply_indices_<x>.go`, redirect methods on `*QueryBuilder` in
   `pkg/sql/plan/plugin_builder.go`, and the dispatch case in
   `pkg/sql/plan/apply_indices.go` (which calls `p.Plan().CanApply`).
5. **Register:** `init()` in `plugin.go` calls `plugin.Register(New())`. Add
   **one** blank-import line to `pkg/indexplugin/all/all.go` — **or**
   `all_gpu.go` if GPU-only (§8.6). That aggregator is the *only* wiring edit;
   `pkg/sql/plan` and `pkg/sql/compile` already blank-import `.../all`.
6. **SQL test** under `test/distributed/cases/vector/` exercising CREATE INDEX,
   `ORDER BY <distfn>(col,v) LIMIT k`, ALTER REINDEX, DROP INDEX, DROP TABLE.

> If the plugin compiles and every hook is implemented, you did **not** miss a
> dispatch site. Do not add manual dispatch "to be safe."

### 8.6 CPU vs GPU registration (ties to §2.5)

| File | Build tag | Registers | Rationale |
|------|-----------|-----------|-----------|
| `pkg/indexplugin/all/all.go` | (none) | fulltext, hnsw, ivfflat | CPU-safe algorithms |
| `pkg/indexplugin/all/all_gpu.go` | `//go:build gpu` | cagra, ivfpq | CUDA-backed table functions exist only under `gpu` |

CAGRA / IVF-PQ have `cagra_create` / `ivfpq_create` table functions implemented
**only** under `//go:build gpu`. Registering them on a CPU binary would let
`CREATE INDEX … USING cagra|ivfpq` proceed until the BUILD SQL fails mid-flight —
after hidden tables are created and DELETEs run. Gating registration behind the
`gpu` tag makes plan-build return `unsupported index type: <algo>` **before any
DDL side effect**. The `gpu` tag is enabled by `MO_CL_CUDA=1 make` (§2.5).

**Pairing rule:** an algo with `build_gpu.go` (`//go:build gpu`) must have a
`//go:build !gpu` CPU counterpart (`*_cpu.go` stub) so the package still compiles
on CPU, *and* its plugin blank-import belongs in `all_gpu.go`, never `all.go`.

### 8.7 Forbidden patterns (index-plugin)

1. **Never re-introduce per-algorithm dispatch in the SQL layer.** A new
   `switch idx.IndexAlgo`, `if catalog.IsIvfIndexAlgo(a) || …`, or `case
   MoIndex<X>Algo:` in `pkg/sql/{compile,plan}` / `pkg/catalog` re-opens the bug
   class the framework closed. Need a new per-algo decision? Add a **method to
   the relevant `Hooks` interface** (compiler forces every algo to answer) — not
   a switch.
2. **Never import `pkg/sql/plan` or `pkg/sql/compile` from a plugin package.**
   Those packages blank-import the plugins for `init()` registration → an import
   cycle. Plugins receive `compile.CompileContext` / `plan.PlanBuilder` /
   `plan.CompilerContext` and call *through* them. `plan/` bodies that need
   `*QueryBuilder` internals live in `pkg/sql/plan/apply_indices_<x>.go`, reached
   via the thin redirect (§8.5 step 4).
3. **Never register a GPU-only algo in `all.go`** — use `all_gpu.go` (§8.6).
4. **Never weaken the `var _ AlgoPlugin` / `var _ Hooks` assertions** to green a
   build. Implement the missing method.
5. **Keep runtime out of the plugin.** The plugin owns compile + plan + catalog +
   idxcron metadata only. Real build/search kernels stay in
   `pkg/vectorindex/<algo>/{build_gpu,search_gpu}.go`, invoked from
   `pkg/sql/colexec/table_function/<algo>_*.go`. Do not copy kernel logic in.
6. **Never pollute `pkg/indexplugin` with algorithm-specific code — it is
   interfaces + metadata only.** The framework package holds only the interfaces
   (`AlgoPlugin`, the `Hooks` + `Context` interfaces), the registry, and
   *algorithm-agnostic* shared helpers/metadata (e.g. `AlgoParamInt`,
   `IdxcronVarSpec` / `BuildIdxcronMetadata` — generic, zero algo branching).
   Every concrete hook body and per-algo constant lives in
   `pkg/vectorindex/<algo>/plugin/` (or `pkg/fulltext/plugin/`). Do **not** add a
   `switch algo` / `if IsXxxIndexAlgo`, a concrete `Hooks` implementation, or any
   algorithm-named symbol under `pkg/indexplugin/*`. The *only* files there that
   may name a concrete algorithm are the `all/` and `iscp/` aggregators — and
   only as blank imports.

### 8.8 Testing & completion (index-plugin)

Index-plugin changes have a **coverage trap**: the end-to-end BVT cases under
`test/distributed/cases/vector/` are GPU-gated (skipped in non-GPU CI), so
`plan.go` / `schema.go` read **0% coverage** and fail the 0.75 gate unless you
add CPU-runnable unit tests (tablefunc_test / runtime_test style — see
`pkg/vectorindex/ivfflat/plugin/{runtime,idxcron}/*_test.go`). On top of the §4.2
completion gate:

```
□ CPU unit tests exist for plan/schema/runtime hooks (not just GPU-gated BVT)  → run, exit 0
□ GPU-only hooks also run with `-tags gpu` per §2.5                            → exit 0
□ grep: NO new `switch algo` / `IsXxxIndexAlgo ||` added in pkg/sql/*
□ git diff --stat: the ONLY all.go/all_gpu.go edit is one blank import
```

**Known in-progress seams — do not add NEW ones.** A few DML-sync sites still
call `catalog.IsIvfIndexAlgo` directly (`pkg/sql/plan/build_dml_util.go`,
`pkg/sql/plan/bind_insert.go`) — the IVFFLAT-hardcoded resolution is mid-migration
into `plan.Hooks`. These are **legacy**, not a license to add more. When you
touch one, prefer moving it behind a hook; never model a *new* algorithm on them.

---

## 9. Reviewing an index-plugin change (G-IDXREVIEW)

Apply this when reviewing a diff — your own before "done", or via `/code-review`
— that touches index-algorithm dispatch, any `pkg/vectorindex/<algo>/plugin/`,
`pkg/fulltext/plugin`, or `pkg/indexplugin`. Each item maps to a §8.7 forbidden
pattern; the grep is the fast check. **Request changes if any check fails**, and
cite the §8.7 rule number in the finding.

1. **No new per-algo dispatch** (§8.7 #1). The diff adds no `switch …IndexAlgo`,
   `case MoIndex<X>Algo:`, or `IsIvfIndexAlgo || …` in `pkg/sql/{compile,plan}` /
   `pkg/catalog`. Legacy holdouts (`build_dml_util.go`, `bind_insert.go`) may
   remain — but **no new ones**.
   ```bash
   git diff -U0 pkg/sql pkg/catalog | grep -E '^\+' \
     | grep -E 'IsIvfIndexAlgo|IsHnswIndexAlgo|IsCagraIndexAlgo|IsIvfpqIndexAlgo|IndexAlgo *==|case .*Algo\b'
   ```
2. **No import cycle** (§8.7 #2). No plugin sub-package imports `pkg/sql/plan` or
   `pkg/sql/compile`.
   ```bash
   git diff pkg/vectorindex/*/plugin pkg/fulltext/plugin \
     | grep -E '^\+.*matrixorigin/matrixone/pkg/sql/(plan|compile)"'   # expect empty
   ```
3. **`indexplugin` not polluted** (§8.7 #6). No file under `pkg/indexplugin/`
   (except `all/`, `iscp/`) imports a concrete algo package or gains
   algorithm-named / `switch algo` code.
   ```bash
   git diff pkg/indexplugin \
     | grep -E '^\+.*(vectorindex/(ivfflat|ivfpq|cagra|hnsw)/plugin|fulltext/plugin)'  # only all*.go / iscp allowed
   ```
4. **GPU registration correct** (§8.6, §8.7 #3). A GPU-only algo's blank import is
   in `all_gpu.go` (`//go:build gpu`), not `all.go`; each `build_gpu.go` has a
   `//go:build !gpu` CPU counterpart.
5. **Assertions intact** (§8.7 #4). The diff does not delete / comment /
   `//nolint` any `var _ AlgoPlugin` or `var _ Hooks` line.
   ```bash
   git diff | grep -E '^-.*var _ (plugin\.)?(AlgoPlugin|Hooks)'   # expect empty
   ```
6. **Runtime kept out** (§8.7 #5). No build/search kernel logic copied into the
   plugin; it still calls `pkg/vectorindex/<algo>/{build_gpu,search_gpu}.go`.
7. **New-algo completeness** (§8.5, §8.8). If a new algorithm: blank-imported in
   `all/` or `all_gpu/`, has an SQL case under `test/distributed/cases/vector/`,
   and CPU unit tests cover the plan/schema/runtime hooks (not just GPU-gated
   BVT).
