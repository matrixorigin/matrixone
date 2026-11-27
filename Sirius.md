# Building Sirius with MatrixOne

Use the Sirius submodule at `thirdparties/sirius` and its **`mo` Pixi
environment** for MatrixOne integration. Run both Sirius and MatrixOne builds
through `pixi run --frozen -e mo` so they use the same CUDA libraries and
compilers.

## Profile and prerequisites

The manifest is [thirdparties/sirius/pixi.toml](thirdparties/sirius/pixi.toml);
the adjacent `pixi.lock` pins the complete environment.

| Setting | Value |
| --- | --- |
| Pixi environment | `mo` |
| Supported hosts | Native Linux x86-64 and ARM64/aarch64 |
| CUDA | 13.3 |
| cuDF | 26.10.02 |
| cuVS | 26.10.00 |
| RMM | 26.10 |
| UCX | 1.21.0 |
| C/C++ compiler | GCC 14, selected for the host architecture |
| CMake generator | Ninja |

Install Git and Pixi 0.84.2 or newer. Building MatrixOne also requires the Go
version specified by [go.mod](go.mod), currently Go 1.27.1 or newer. Use a
checkout path without whitespace. A compatible NVIDIA driver and GPU are
required to execute GPU queries and smoke tests.

The `default` Sirius environment selects a different CUDA version; explicitly
select `mo`. MatrixOne's standalone `optools/gpu/pixi.toml` currently supports
x86-64 only. On ARM64, use the Sirius `mo` profile for MO GPU-only builds too.

Pixi supplies CUDA, RAPIDS, compilers, and packaging tools. Keep the SDK and MO
on one activated prefix; do not mix their dependencies with a system CUDA or
another Conda/Pixi environment.

## Initialize the pinned sources

If you previously initialized Sirius at `third_party/sirius`, Git may leave
that directory behind after the move. Before initializing the new path, back
up any uncommitted changes and untracked files you want to keep from the old
checkout. Once you have verified your backup and initialized
`thirdparties/sirius`, you can remove the old directory.

Start in the MatrixOne repository root. Keep these exported variables in the
same shell for the commands below:

```sh
export MO_SRC="$(pwd -P)"
export SIRIUS_SRC="$MO_SRC/thirdparties/sirius"
# Capture MatrixOne's Go installation before Pixi activation changes PATH.
export MO_GO_ROOT="$(go env GOROOT)"

git submodule update --init thirdparties/sirius
git -C "$SIRIUS_SRC" submodule update --init --recursive \
  duckdb substrait cucascade tae-scanner

git -C "$SIRIUS_SRC" rev-parse HEAD
git -C "$MO_SRC" rev-parse HEAD:thirdparties/sirius
```

For a combined MO/Sirius build, the two revisions must match. Commit submodule
path and pin changes in MatrixOne before that build: SDK validation reads the
gitlink from **`HEAD`**, so staging a new pin alone is insufficient. Development
mode also enforces this revision check.

## Build the embedding SDK

When upgrading an existing checkout, check `CMAKE_HOME_DIRECTORY` in
`$SIRIUS_SRC/build/mo/CMakeCache.txt` if that file exists. If it points to
the old `duckdb` source directory, move `$SIRIUS_SRC/build/mo` to an unused
backup path before running the SDK build. The current task configures from
`$SIRIUS_SRC` and needs a fresh build directory for that transition.

```sh
cd "$SIRIUS_SRC"
pixi lock --check
pixi install --frozen -e mo

CMAKE_BUILD_PARALLEL_LEVEL=8 \
  pixi run --frozen -e mo mo-build-embedding-sdk

export SIRIUS_SDK="$SIRIUS_SRC/build/mo/embedding-sdk"
test -f "$SIRIUS_SDK/link.json"
test -f "$SIRIUS_SDK/link.rsp"
test -f "$SIRIUS_SDK/sirius_c.h"
```

The task configures `build/mo` and builds the `sirius_c_smoke` C consumer. Its
post-build step exports the SDK header, link response file, and `link.json`
with compiler paths, source revision, and artifact hashes. The SDK configure
task disables compiler cache launchers for C, C++, and CUDA.

The SDK contains absolute build paths. Regenerate it after changing the Sirius
sources, Pixi lockfile, environment, or checkout location.

On a host with a compatible GPU, run the native smoke test:

```sh
cd "$SIRIUS_SRC"
pixi run --frozen -e mo \
  ./build/mo/sirius_c_smoke \
  "$SIRIUS_SRC/test/cpp/operator/result.yaml"
```

The example YAML targets GPU 0 and reserves 2 GiB of GPU memory and 4 GiB of
host memory. Adjust the configuration for the machine before running it.

## Build MatrixOne with embedded Sirius and GPU operators

Use the same Sirius profile and Go installation for the full MatrixOne build:

```sh
pixi run --frozen --manifest-path "$SIRIUS_SRC/pixi.toml" -e mo sh -c '
  export GOROOT="$MO_GO_ROOT"
  export PATH="$GOROOT/bin:$PATH"
  cd "$MO_SRC"
  make -j8 build MO_SIRIUS=1 MO_CL_CUDA=1 \
    SIRIUS_SDK="$SIRIUS_SDK" \
    SIRIUS_BUILD_MODE=development
'
```

`MO_SIRIUS=1` links the embedding SDK. `MO_CL_CUDA=1` additionally enables MO's
GPU vector operators. The top-level `build` target builds native dependencies,
links `mo-service`, and packages the required libraries.

The command uses development mode for the published build-branch pin. For a
release, first merge the Sirius change into the integration branch and pin
the merged revision in MatrixOne. Fetch that branch inside the submodule,
then replace `SIRIUS_BUILD_MODE=development` with
`SIRIUS_MERGED_REF=origin/perf/embedded-mo-scan`. The default release mode
checks source cleanliness and verifies that the integration branch contains
the SDK revision.

Verify the package under the same profile:

```sh
pixi run --frozen --manifest-path "$SIRIUS_SRC/pixi.toml" -e mo sh -c '
  cd "$MO_SRC"
  python3 optools/sirius_sdk.py verify-package \
    --prepared .sirius-sdk --output lib --sdk "$SIRIUS_SDK"
'
"$MO_SRC/mo-service" -version
```

Keep `mo-service`, `lib/`, `dict/`, and `mocl_kernel64.fatbin` together when
deploying the combined GPU build. Package provenance is recorded in
`lib/sirius-provenance.json`. Runtime configuration and native integration tests
are described in the [embedded Sirius guide](docs/guides/sirius-embedded.md).

## Build MO GPU operators without the embedding SDK

This uses the same ARM64-capable toolchain but only builds MO's GPU operators;
it does not build or link the Sirius SDK. Run source initialization first;
SDK compilation is unnecessary for this command.

```sh
pixi run --frozen --manifest-path "$SIRIUS_SRC/pixi.toml" -e mo sh -c '
  export GOROOT="$MO_GO_ROOT"
  export PATH="$GOROOT/bin:$PATH"
  cd "$MO_SRC"
  make -j8 build MO_CL_CUDA=1 MO_SIRIUS=0
'
"$MO_SRC/mo-service" -version
```

This produces a local GPU binary that uses libraries from the activated Pixi
environment. Keep that environment available when running it. A successful
MO GPU-only build does not validate the embedding SDK or Sirius query execution.

## Build Sirius as a DuckDB extension

The DuckDB extension has its own submodules and build targets. Follow the
[Sirius standalone quick start](thirdparties/sirius/docs/README.md#quick-start).
