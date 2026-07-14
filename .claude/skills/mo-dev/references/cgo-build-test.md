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

Note: Makefile does **not** set `CGO_LDFLAGS`; `libmo` link flags all go through `-ldflags` -> `-extldflags`. The `usearch` Go module ships its own `#cgo LDFLAGS: -lusearch_c`, so `CGO_LDFLAGS` only needs to put the intended thirdparty directory on the search path. Repeating `-lusearch_c` adds noise and can obscure the effective linker graph.

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

### Deterministic test command

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
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib" \
DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" GOWORK=off \
go test -mod=readonly -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

Linux:

```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" GOWORK=off \
go test -mod=readonly -ldflags="-extldflags '-fopenmp -L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

### Symptom -> Root Cause

| Symptom | Missing Variable | Root Cause |
|---------|-----------------|------------|
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS` | Compiler cannot find thirdparties headers |
| `Undefined symbols: _usearch_hardware_acceleration_*` (macOS) or `undefined symbol:` (Linux) | `CGO_LDFLAGS` | usearch module's `#cgo LDFLAGS` found old `libusearch_c` |
| `ld: library 'mo' not found` (macOS) or `cannot find -lmo` (Linux) | `-ldflags="-extldflags '-L... -lmo'"` | Linker cannot find `libmo` |
| Loader searches `go-build.../lib` | Test used packaged-binary relative rpath | Temporary executable resolves relative to its own directory |
| `dyld: Library not loaded` / `error while loading shared libraries` | Runtime path/rpath | Runtime cannot find a dynamic library; inspect every dependency, not only `libmo` |
| Inconsistent vendoring | Module mode/vendor metadata | Do not regenerate vendor or silently switch modes as a side effect of testing; use `GOWORK=off -mod=readonly` and report the mismatch |

## 3. Layered Testing Strategy

| Layer | Example Packages | CGo Behavior | Variables Needed |
|-------|------------------|--------------|------------------|
| **Pure Go** | `pkg/vm/process`, `pkg/container/pSpool`, `pkg/vm/pipeline` | Zero CGo in transitive closure | None |
| **CGo-transitive** | `pkg/sql/colexec/connector`, `dispatch`, `merge` | Test binary links usearch Go module | `CGO_CFLAGS` + `CGO_LDFLAGS` + runtime loader path |
| **CGo-direct** | `pkg/sql/compile` | Test binary directly links `libmo` + `libusearch_c` | All four: CGO_CFLAGS + CGO_LDFLAGS + ldflags + DYLD/LD_LIBRARY_PATH |
| **Integration** | `pkg/frontend`, cmd packages | Full MO binary, needs external services | All + services |

Layer 1, pure Go:

```bash
go test -v -count=1 -timeout 120s ./pkg/vm/process/... ./pkg/container/pSpool/...
```

Layer 2, CGo-transitive on macOS:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib"
export DYLD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib"
GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s ./pkg/sql/colexec/connector/... ./pkg/sql/colexec/dispatch/...
```

Layer 3, CGo-direct on macOS:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib"
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
GOWORK=off go test -mod=readonly -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib'" \
  -v -count=1 -timeout 120s ./pkg/sql/compile/...
```

| Variable | What It Does | When Needed |
|----------|--------------|-------------|
| `CGO_CFLAGS` | Tells C compiler where to find headers | Package in transitive closure has CGo C code |
| `CGO_LDFLAGS` | Overrides usearch module's own `#cgo LDFLAGS` path | usearch Go module is linked |
| `-ldflags "-extldflags ..."` | Tells external linker where to find `libmo` + sets rpath | Package directly links `libmo` |
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

Never claim a test failure is pre-existing without proof.

```bash
# 1. Stash current changes
git stash

# 2. Run the same test from clean state
go test -v -count=1 -timeout 120s ./pkg/target/...

# 3. If it fails: genuinely pre-existing -- document it
# 4. If it passes: YOUR code caused it -- investigate
git stash pop
```

## 6. GPU Build (`MO_CL_CUDA=1`) -- cuVS / CUDA

GPU support compiles the CUDA-backed vector index algorithms (**CAGRA**, **IVF-PQ**) into `libmo` and turns on the `gpu` Go build tag. Linux x86_64 only. The macOS Makefile branch carries no CUDA flags, so macOS builds are CPU-only. Do not try to enable it on Darwin.

Prerequisites:

1. CUDA toolkit 12.0 / 13.0+ installed under `/usr/local/cuda`.
2. cuVS Go bindings installed via conda and the env activated so `CONDA_PREFIX` is exported:

```bash
conda env create --name go -f conda/environments/go_cuda-130_arch-$(uname -m).yaml
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

The `gpu` tag gates index-plugin registration. CAGRA and IVF-PQ register only under `//go:build gpu` (`pkg/indexplugin/all/all_gpu.go`). On a CPU binary their plugins are absent from the registry, so `CREATE INDEX ... USING ivfpq|cagra` fails cleanly at plan-build with `unsupported index type: <algo>` before hidden table creation. Do not move those imports into `all.go`.

The linked `libmo` must itself be GPU-built:

```bash
MO_CL_CUDA=1 make -j8 cgo
```

GPU tests need `-tags gpu` plus CUDA search paths. Linux only:

```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include -I$CONDA_PREFIX/include -I/usr/local/cuda/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c -L$CONDA_PREFIX/lib -lcuvs -lcuvs_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib:$CONDA_PREFIX/lib:/usr/local/cuda/lib64" \
go test -tags gpu \
  -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib -fopenmp'" \
  -v -count=1 -timeout 300s ./pkg/vectorindex/ivfpq/...
```

The authoritative flag source is the Makefile (`CUDA_CFLAGS` / `CUDA_LDFLAGS`), not this snippet. If a GPU link error appears, diff your flags against those lines.

Tag-split test files are a trap: `*_gpu.go` / `//go:build gpu` tests compile only under `-tags gpu`. A plain `go test ./pkg/vectorindex/ivfpq/...` runs `//go:build !gpu` / `*_cpu.go` stubs instead. CPU tests passing does not test the GPU path.
