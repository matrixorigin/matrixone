# CGo, Build, Test, And GPU Reference

## Contents

- [1. Basic Commands](#1-basic-commands)
- [2. CGo Environment (Four-Layer Model)](#2-cgo-environment-four-layer-model)
- [3. Layered Testing Strategy](#3-layered-testing-strategy)
- [4. Completion And Hang Diagnosis](#4-completion-and-hang-diagnosis)
- [5. Attribution And Clean-Tree Reproduction](#5-attribution-and-clean-tree-reproduction)
- [6. GPU Build (`MO_CL_CUDA=1`) -- cuVS / CUDA](#6-gpu-build-mo_cl_cuda1----cuvs--cuda)

## 1. Basic Commands

```bash
# Compile check
GOWORK=off go build -mod=readonly ./pkg/target/...

# Static analysis
GOWORK=off go vet -mod=readonly ./pkg/target/...

# Run tests (-count=1 disables cache)
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/target/...

# Single test
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s -run '^TestXxx$' ./pkg/target/...
```

## 2. CGo Environment (Four-Layer Model)

MO test execution has four independent layers. Diagnose them in order:

1. **Module resolution** — `go.mod`, vendor metadata, replacements, build tags.
2. **Compilation** — C/C++ headers and compiler flags.
3. **Linking** — `libmo`, third-party libraries, and external-linker flags.
4. **Runtime loading** — the test executable must locate every dynamic library.

Passing one layer does not prove the next one.

Platform handling is a contract, not a collection of machine-specific fixes:

- obtain target and host facts from `go env`, not usernames, Homebrew paths, or
  assumptions about the current CPU;
- require target OS/architecture to match the host for local CGo execution;
- use the repository Makefile's module mode and library roots as the source of
  truth;
- reject unsupported repository layouts explicitly (the current Make/CGo flag
  contract requires a repository path without whitespace);
- branch only on a supported platform capability (library format and loader),
  and fail explicitly for unsupported targets;
- keep distributable-binary and temporary-test loader rules separate.

```makefile
# Makefile -- header paths for compilation
CGO_OPTS := CGO_CFLAGS="-I$(CGO_DIR) -I$(THIRDPARTIES_INSTALL_DIR)/include"

# Makefile -- link libmo (rpath varies by OS)
# Linux:   -Wl,-rpath,$$ORIGIN/lib
# macOS:   -Wl,-rpath,@executable_path/lib
GOLDFLAGS := -ldflags="-extldflags '-L$(CGO_DIR) -lmo -L$(THIRDPARTIES_INSTALL_DIR)/lib $(RPATH)'"
```

Note: Makefile does **not** set `CGO_LDFLAGS`; `libmo` link flags all go through `-ldflags` -> `-extldflags`. `CGO_LDFLAGS` only needs to put the intended thirdparty directory on the search path. The test wrapper is different: because it injects `-lmo` even when the target does not import `cgo/lib.go`, it queries that package's `CgoLDFLAGS` with `go list` and appends the declared native dependency graph after `-lmo`. Do not maintain a second hard-coded dependency list in the wrapper; it will drift when `libmo` gains or removes a native dependency.

### macOS vs Linux

| Aspect | macOS | Linux |
|--------|-------|-------|
| Dynamic library | `libmo.dylib` | `libmo.so` |
| Runtime path env | `DYLD_LIBRARY_PATH` | `LD_LIBRARY_PATH` |
| Packaged-binary rpath | `-Wl,-rpath,@executable_path/lib` | `-Wl,-rpath,\$ORIGIN/lib` |
| Temporary `go test` rpath | absolute repository `cgo/` and thirdparty lib paths | same |
| CPU external-link addition | none | `-fopenmp` |
| C header flags | `CGO_CFLAGS="-I{root}/cgo -I{root}/thirdparties/install/include"` | Same |
| usearch search flags | `CGO_LDFLAGS="-L{root}/thirdparties/install/lib"` | Same |

### Controlled local CPU test command

Prefer the repository wrapper for arbitrary packages and test flags:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/target/...
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=240s ./pkg/target/...
```

It verifies host/target and CGo prerequisites, enforces the repository's
`GOWORK=off` and `-mod=readonly` contract, removes ambient CGo flag drift,
chooses the supported OS library/loader form, and gives temporary test
executables absolute rpaths. It is a local CPU-test entry point; GPU and static
cross-builds have different toolchain contracts and remain explicit workflows.
`GOFLAGS`, `GOEXPERIMENT`, `CC`, and `CXX` remain caller-owned inputs; record
them when attribution or reproducibility depends on them, and ensure native
artifacts were built from the same source generation.

### Why test rpaths differ from packaged binaries

`go test` runs an executable under a temporary `go-build...` directory.
`@executable_path/lib` or `$ORIGIN/lib` therefore points under that temporary
directory, not the repository. Those relative rpaths are appropriate only when
a distributable binary is installed beside its `lib/` directory. Use absolute
repository rpaths for local tests; keep the runtime environment variable as a
supplement, not the sole proof of loadability.

### Expanded commands

macOS:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib"
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
export GOWORK=off
libmo_deps=$(go list -mod=readonly -f '{{join .CgoLDFLAGS " "}}' ./cgo)
go test -mod=readonly -ldflags="-extldflags '-L$(pwd)/cgo -lmo $libmo_deps -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

Linux:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib"
export LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
export GOWORK=off
libmo_deps=$(go list -mod=readonly -f '{{join .CgoLDFLAGS " "}}' ./cgo)
go test -mod=readonly -ldflags="-extldflags '-fopenmp -L$(pwd)/cgo -lmo $libmo_deps -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

### Symptom -> Root Cause

| Symptom | Missing Variable | Root Cause |
|---------|-----------------|------------|
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS` | Compiler cannot find thirdparties headers |
| `Undefined symbols: _usearch_hardware_acceleration_*` (macOS) or `undefined symbol:` (Linux) | `CGO_LDFLAGS` | usearch module's `#cgo LDFLAGS` found old `libusearch_c` |
| `ld: library 'mo' not found` (macOS) or `cannot find -lmo` (Linux) | `-ldflags="-extldflags '-L... -lmo'"` | Linker cannot find `libmo` |
| Native symbols are unresolved from `libmo` | declared dependencies absent or placed before `-lmo` in `-extldflags` | A forced `libmo` link did not close its ordered dependency graph |
| Loader searches `go-build.../lib` | Test used packaged-binary relative rpath | Temporary executable resolves relative to its own directory |
| `dyld: Library not loaded` / `error while loading shared libraries` | Runtime path/rpath | Runtime cannot find a dynamic library; inspect every dependency, not only `libmo` |
| Inconsistent vendoring | Module mode/vendor metadata | Do not regenerate vendor or silently switch modes as a side effect of testing; use `GOWORK=off -mod=readonly` and report the mismatch |

## 3. Layered Testing Strategy

Classify the current dependency graph instead of assuming a package stays pure
Go or CGo-transitive forever:

```bash
# Empty output means the complete dependency graph is pure Go.
GOWORK=off go list -mod=readonly -deps -test \
  -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./pkg/target

# Empty output here, but non-empty dependency output above, means the target is
# CGo-transitive rather than CGo-direct.
GOWORK=off go list -mod=readonly \
  -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./pkg/target
```

| Layer | Example Packages | CGo Behavior | Variables Needed |
|-------|------------------|--------------|------------------|
| **Pure Go** | `pkg/common/moerr`, `pkg/pb/timestamp` | Zero CGo in transitive closure | None |
| **CGo-transitive** | `optools/testdata/mo_cgo_transitive`, `pkg/txn/client` | Target has no CGo files, but its test/dependencies force an external link | CGo compile/search flags plus any forcibly linked library closure |
| **CGo-direct** | `cgo`, packages reported with their own `CgoFiles` | Target itself compiles CGo and declares native libraries | All compile, ordered link, and runtime-load layers |
| **Integration** | `pkg/frontend`, cmd packages | Full MO binary, needs external services | All + services |

Layer 1, pure Go:

```bash
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/common/moerr/... ./pkg/pb/timestamp/...
```

Layer 2, CGo-transitive:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test \
  -v -count=1 -timeout 120s ./optools/testdata/mo_cgo_transitive
```

Layer 3, CGo-direct:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test \
  -v -count=1 -timeout 120s ./cgo
```

| Variable | What It Does | When Needed |
|----------|--------------|-------------|
| `CGO_CFLAGS` | Tells C compiler where to find headers | Package in transitive closure has CGo C code |
| `CGO_LDFLAGS` | Supplies deterministic native-library search roots to CGo packages | A dependency resolves native libraries outside system roots |
| `-ldflags "-extldflags ..."` | Tells external linker where to find `libmo`, closes its ordered CPU dependencies, and sets rpath | Package directly or forcibly links `libmo` |
| `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Tells OS runtime loader where to find `.dylib` / `.so` | Test binary loads `libmo` at runtime |

Bottom-up testing matters: pure Go tests finish in seconds with zero env setup. If they fail, the problem is in code, not CGo.

### `go build` vs `go test` Is Not Equivalent

| Command | CGo Behavior |
|---------|--------------|
| `go build ./pkg/sql/colexec/connector/...` | May succeed without CGo flags because it compiles package code only |
| `go test ./pkg/sql/colexec/connector/...` | Compiles and links a test binary; full CGo path fires |

Always verify with `go test`.

## 4. Completion And Hang Diagnosis

A command is complete only when its final exit status is known. Linker output,
partial logs, a yielded session ID, or silence is not success.

When a test produces no progress for more than 10 seconds:

1. Check whether `go test` and its test binary are still alive.
2. Poll the existing session instead of launching duplicate tests.
3. Capture goroutine stacks with the test timeout or `SIGQUIT` when safe.
4. Construct a wait-for graph from the blocked goroutine through locks,
   channels, callbacks, RPC, I/O, and the release event.
5. Terminate only test processes you started after capturing evidence.

On macOS, use `otool -L <test-binary>` and `otool -l <test-binary>`; on Linux,
use `ldd` and `readelf -d`. Loader errors often reveal dependencies one at a
time, so inspect the whole graph after the first missing library.

## 5. Attribution And Clean-Tree Reproduction

Never claim a test failure is pre-existing without proof at the correct clean
baseline. Do not use `git stash`: it omits untracked files by default, does not
remove committed PR changes, and can disturb the user's index or conflict on
restore.

First run the exact candidate command and record its exit status plus a stable
failure signature (failing test/package and causal error, not timestamps or temp
paths). Then run the same command at the baseline below:

```bash
# Choose HEAD for an uncommitted-only change, or the verified PR base/merge-base
# when the candidate includes commits. Record the resolved object ID.
baseline_ref=HEAD
baseline_parent=$(mktemp -d "${TMPDIR:-/tmp}/mo-baseline.XXXXXX")
baseline_dir="$baseline_parent/tree"
git worktree add --detach "$baseline_dir" "$baseline_ref"
baseline_log="$baseline_parent/baseline.log"
cleanup_baseline() {
  git worktree remove --force "$baseline_dir" 2>/dev/null || true
}
trap cleanup_baseline EXIT
trap 'exit 130' INT
trap 'exit 129' HUP
trap 'exit 143' TERM

# In the isolated worktree, recreate matching native artifacts and run the
# exact candidate command with the same Go/toolchain/module inputs.
baseline_status=0
(cd "$baseline_dir" && GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/target/...) \
  >"$baseline_log" 2>&1 || baseline_status=$?
printf 'baseline exit status: %s\n' "$baseline_status"
printf 'baseline log retained for signature comparison: %s\n' "$baseline_log"
cleanup_baseline
trap - EXIT INT HUP TERM
```

Keep cleanup and signal termination as separate responsibilities. The `EXIT`
trap owns worktree cleanup; each signal trap must terminate with its conventional
`128 + signal` status so a cancelled command cannot continue into baseline
attribution. This minimal oracle must report status 130, record exactly one
`cleanup` line, and never record `continued`:

```bash
signal_log=$(mktemp)
signal_status=0
sh -c '
  trap '\''printf "cleanup\\n" >> "$1"'\'' EXIT
  trap '\''exit 130'\'' INT
  kill -INT $$
  printf "continued\\n" >> "$1"
' sh "$signal_log" || signal_status=$?
test "$signal_status" -eq 130
test "$(cat "$signal_log")" = cleanup
rm -f "$signal_log"
```

A candidate failure is pre-existing evidence only when the baseline also fails
with the equivalent causal signature and the command, environment, platform,
and native dependency provenance match. Baseline success points to the
candidate; a different failure is inconclusive. Extract and record the causal
signature from the retained log before deleting the exact `baseline_parent`;
the cleanup above removes only the disposable worktree, so a tool invocation
cannot erase the evidence before comparison.

## 6. GPU Build (`MO_CL_CUDA=1`) -- cuVS / CUDA

GPU support compiles the CUDA-backed vector index algorithms (**CAGRA**, **IVF-PQ**) into `libmo` and turns on the `gpu` Go build tag. Linux x86_64 only. The macOS Makefile branch carries no CUDA flags, so macOS builds are CPU-only. Do not try to enable it on Darwin.

Prerequisites:

1. CUDA toolkit matching the versions pinned by
   `optools/images/gpu/Dockerfile` and
   `optools/images/gpu/go_cuda-130_arch-x86_64.yaml`, installed under the path
   expected by those files. Do not infer an unsupported version range.
2. cuVS Go bindings installed from the repository's Linux x86_64 environment
   file and the environment activated so `CONDA_PREFIX` is exported. Prefer the
   builder stage in `optools/images/gpu/Dockerfile` when a container runtime is
   available. Building does not require a GPU device; executing GPU workloads
   requires a compatible NVIDIA host/runtime.

```bash
conda env create --name go -f optools/images/gpu/go_cuda-130_arch-x86_64.yaml
conda activate go
```

Build:

```bash
MO_CL_CUDA=1 make -j8
```

What `MO_CL_CUDA=1` flips:

| Layer | CPU build | GPU build (`MO_CL_CUDA=1`) |
|-------|-----------|----------------------------|
| Go build tag | none | `-tags gpu` -- registers CAGRA + IVF-PQ, compiles `*_gpu.go` |
| `cgo/` compiler | `gcc`/`clang` | `/usr/local/cuda/bin/nvcc` |
| `libmo` objects | C objects only | + `cuda/*.o` + `cuvs/*.o` |
| Link flags | `-lusearch_c -lroaring` | + `-lcuvs -lcuvs_c -lcudart -lcuda -lrmm -lstdc++` |
| Header/lib roots | thirdparties only | + `$CONDA_PREFIX/{include,lib}`, `/usr/local/cuda/...` |

Guardrails:

- `CONDA_PREFIX env variable not found`: conda env not activated. Run `conda activate <env>` first. This is not a code bug.
- `libmo` is re-linked on every GPU build deliberately because `mo-service` loads `libmo.so` dynamically. A stale `.so` silently runs old C++.
- Always pass `-j8`. The cuVS/CUDA objects dominate a GPU build and a single-threaded `make` stalls the edit-build-test loop for minutes at a time.

The `gpu` tag gates index-plugin registration. CAGRA and IVF-PQ register only under `//go:build gpu` (`pkg/indexplugin/all/all_gpu.go`). On a CPU binary their plugins are absent from the registry, so `CREATE INDEX ... USING ivfpq|cagra` fails cleanly at plan-build with `unsupported index type: <algo>` before hidden table creation. Do not move those imports into `all.go`.

The linked `libmo` must itself be GPU-built:

```bash
MO_CL_CUDA=1 make -j8 cgo
```

GPU tests need `-tags gpu` plus CUDA search paths. Linux only:

```bash
gpu_package=./pkg/vectorindex/ivfpq/... # set to the affected GPU algorithm package
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include -I$CONDA_PREFIX/include -I/usr/local/cuda/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c -L$CONDA_PREFIX/lib -lcuvs -lcuvs_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib:$CONDA_PREFIX/lib:/usr/local/cuda/lib64" \
GOWORK=off go test -mod=readonly -tags gpu \
  -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib -Wl,-rpath,$CONDA_PREFIX/lib -Wl,-rpath,/usr/local/cuda/lib64 -fopenmp'" \
  -v -count=1 -timeout 300s "$gpu_package"
```

The authoritative flag source is the Makefile (`CUDA_CFLAGS` / `CUDA_LDFLAGS`), not this snippet. If a GPU link error appears, diff your flags against those lines.

Tag-split test files are a trap: `*_gpu.go` / `//go:build gpu` tests compile only under `-tags gpu`. A plain `go test ./pkg/vectorindex/ivfpq/...` runs `//go:build !gpu` / `*_cpu.go` stubs instead. CPU tests passing does not test the GPU path.

### The GPU suite is not green just because CI is

CI does not compile `//go:build gpu` files at all, so GPU-tagged tests are unmaintained by
default: nothing tells an author when one rots. A single local sweep in August 2026 found
**four** failing on `main`, each broken by an unrelated change months earlier —

| Test | Broken by |
|---|---|
| `TestIvfpqSearch` | the quantization base-type guard (#25095); its `IndexTableConfig` omits `parttype`, so `KeyPartType` defaulted to 0 and every `vecf32` query was rejected |
| `TestBuildCagraSecondaryIndexDef_OK`, `TestBuildIvfpqSecondaryIndexDef_OK` | mock catalog drift |
| `TestBatchArrayDistanceSync_GPU_InnerProduct` | an inner-product double negation |

Two consequences for anyone running GPU tests:

1. **A GPU failure is not automatically yours.** Prove ownership before fixing: revert your
   change (`git stash`, or `git checkout HEAD~1 -- <paths>` once committed) and re-run. Then
   check whether another branch already fixes it — `git log -S '<symbol>' --all --oneline`
   and `git branch --contains <commit>` — before patching it into your diff. Three of the
   four above were already fixed on other in-flight branches; duplicating those fixes would
   have produced merge conflicts for no gain.
2. **Run the whole GPU set, not just your package**, when touching shared vector code:

```bash
grep -rl '^//go:build gpu' --include='*_test.go' pkg/ | xargs -n1 dirname | sort -u
```

### mo-cgo-test merges the gpu tag into yours

`MO_CL_CUDA=1` makes the wrapper add `-tags gpu`. When the caller passes their own `-tags`,
it is **merged** (`-tags typecheck` becomes `-tags typecheck,gpu`, in whichever spelling was
used) rather than replaced or skipped.

It skipped it until August 2026, on the reasoning that `go test` keeps only the last `-tags`
and appending would discard the caller's. The effect was a false green:
`MO_CL_CUDA=1 mo-cgo-test -tags typecheck ./pkg/vectorindex/metric/` compiled **0** of that
package's 2 GPU test files while reporting a pass. `mo-cgo-test-tags-test` pins every form
with a stubbed `go`, so it needs no GPU, CUDA toolkit or built libmo.

The wrapper's CUDA support is itself branch-dependent: a checkout whose `mo-cgo-test`
predates it ignores `MO_CL_CUDA` entirely and fails to link a GPU-built `libmo` with
`undefined reference to cuMemcpyHtoD_v2` and `libcuda.so.1 not found`. Check with
`grep -c MO_CL_CUDA .agents/skills/mo-dev/scripts/mo-cgo-test` before concluding the tree is
broken; borrow a newer copy into the repo root if needed (it derives the repo from its own
location, so it must sit inside the worktree).

