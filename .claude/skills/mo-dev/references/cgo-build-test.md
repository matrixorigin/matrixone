# CGo, Build, Test, And GPU Reference

## Contents

- [1. Basic Commands](#1-basic-commands)
- [2. CGo Environment (Three-Layer Variables)](#2-cgo-environment-three-layer-variables)
- [3. Layered Testing Strategy](#3-layered-testing-strategy)
- [4. Stash Protocol For "Pre-existing" Claims](#4-stash-protocol-for-pre-existing-claims)
- [5. GPU Build (`MO_CL_CUDA=1`) -- cuVS / CUDA](#5-gpu-build-mo_cl_cuda1----cuvs--cuda)

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

## 2. CGo Environment (Three-Layer Variables)

MO's CGo involves three independent layers: **compilation** (headers), **linking own lib** (`libmo`), and **third-party libs** (`libusearch_c`).

```makefile
# Makefile:203 -- header paths for compilation
CGO_OPTS := CGO_CFLAGS="-I$(CGO_DIR) -I$(THIRDPARTIES_INSTALL_DIR)/include"

# Makefile:204-207 -- link libmo (rpath varies by OS)
# Linux:   -Wl,-rpath,$$ORIGIN/lib
# macOS:   -Wl,-rpath,@executable_path/lib
GOLDFLAGS := -ldflags="-extldflags '-L$(CGO_DIR) -lmo -L$(THIRDPARTIES_INSTALL_DIR)/lib $(RPATH)'"
```

Note: Makefile does **not** set `CGO_LDFLAGS`; `libmo` link flags all go through `-ldflags` -> `-extldflags`. The `usearch` Go module ships its own `#cgo LDFLAGS: -lusearch_c`, so `CGO_LDFLAGS` is needed to force the intended thirdparty path.

### macOS vs Linux

| Aspect | macOS | Linux |
|--------|-------|-------|
| Dynamic library | `libmo.dylib` | `libmo.so` |
| Runtime path env | `DYLD_LIBRARY_PATH` | `LD_LIBRARY_PATH` |
| rpath flag | `-Wl,-rpath,@executable_path/lib` | `-Wl,-rpath,\$ORIGIN/lib` |
| C header flags | `CGO_CFLAGS="-I{root}/cgo -I{root}/thirdparties/install/include"` | Same |
| usearch link flags | `CGO_LDFLAGS="-L{root}/thirdparties/install/lib -lusearch_c"` | Same |

### Full Test Commands

macOS:

```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

Linux:

```bash
CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c" \
LD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,\$ORIGIN/lib'" \
  -v -count=1 -timeout 120s ./pkg/target/...
```

### Symptom -> Root Cause

| Symptom | Missing Variable | Root Cause |
|---------|-----------------|------------|
| `fatal error: 'xxhash.h' file not found` | `CGO_CFLAGS` | Compiler cannot find thirdparties headers |
| `Undefined symbols: _usearch_hardware_acceleration_*` (macOS) or `undefined symbol:` (Linux) | `CGO_LDFLAGS` | usearch module's `#cgo LDFLAGS` found old `libusearch_c` |
| `ld: library 'mo' not found` (macOS) or `cannot find -lmo` (Linux) | `-ldflags="-extldflags '-L... -lmo'"` | Linker cannot find `libmo` |
| `dyld: Library not loaded: libmo.dylib` (macOS) or `error while loading shared libraries: libmo.so` (Linux) | `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` | Runtime cannot find dynamic library |

## 3. Layered Testing Strategy

| Layer | Example Packages | CGo Behavior | Variables Needed |
|-------|------------------|--------------|------------------|
| **Pure Go** | `pkg/vm/process`, `pkg/container/pSpool`, `pkg/vm/pipeline` | Zero CGo in transitive closure | None |
| **CGo-transitive** | `pkg/sql/colexec/connector`, `dispatch`, `merge` | Test binary links usearch Go module | `CGO_CFLAGS` + `CGO_LDFLAGS` |
| **CGo-direct** | `pkg/sql/compile` | Test binary directly links `libmo` + `libusearch_c` | All four: CGO_CFLAGS + CGO_LDFLAGS + ldflags + DYLD/LD_LIBRARY_PATH |
| **Integration** | `pkg/frontend`, cmd packages | Full MO binary, needs external services | All + services |

Layer 1, pure Go:

```bash
go test -v -count=1 -timeout 120s ./pkg/vm/process/... ./pkg/container/pSpool/...
```

Layer 2, CGo-transitive on macOS:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
go test -v -count=1 -timeout 120s ./pkg/sql/colexec/connector/... ./pkg/sql/colexec/dispatch/...
```

Layer 3, CGo-direct on macOS:

```bash
export CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -lusearch_c"
export DYLD_LIBRARY_PATH="$(pwd)/cgo:$(pwd)/thirdparties/install/lib"
go test -ldflags="-extldflags '-L$(pwd)/cgo -lmo -L$(pwd)/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
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

## 4. Stash Protocol For "Pre-existing" Claims

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

## 5. GPU Build (`MO_CL_CUDA=1`) -- cuVS / CUDA

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
