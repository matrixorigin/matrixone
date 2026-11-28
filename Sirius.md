# Building and running Sirius with MatrixOne

Use the Sirius submodule at `thirdparties/sirius` and its **`mo` Pixi
environment** for MatrixOne integration. Run both Sirius and MatrixOne builds
through `pixi run --frozen -e mo` so they use the same CUDA libraries and
compilers.

Embedded Sirius runs hinted queries inside `mo-service` using MO readers, a
native C ABI and bounded result batches. Unhinted SQL follows the ordinary MO
execution path. Embedding requires no sidecar, Flight certificates, manifest
resolver, changes to TN garbage collection or direct-TAE protection.

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
On an NVIDIA DGX Spark, you can pass `"$MO_SRC/etc/launch/sirius-dgx-spark.yaml"`
as the YAML argument. See [Configure and query](#configure-and-query) for the profile's
shared-memory budgets and CN settings.

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
`lib/sirius-provenance.json`.

## Configure and query

Add this section to the CN configuration selected by your launch manifest
(`etc/launch/cn.toml` for the default local launch). `native-config-path` names the
Sirius YAML configuration. Its GPU and pinned-host budgets must leave enough
room for every admitted input window and the result window.

On an NVIDIA DGX Spark (GB10), set `native-config-path` below to the absolute
path of the [DGX Spark profile](etc/launch/sirius-dgx-spark.yaml) on the
CN host. It budgets 48 GiB for GPU memory and 32 GiB for pinned host memory.
Both draw from the machine's 128 GB of unified memory. On other hardware,
size the budgets for its memory capacity and leave room for MatrixOne and the OS.

```toml
[cn.sirius]
enabled = true
native-config-path = "/absolute/path/to/sirius.yaml"
gpu-streams = 2
max-waiting-queries = 16
request-timeout = "15m"
cleanup-timeout = "30s"
```

Start the local service from the MatrixOne repository root.

```sh
./mo-service -launch etc/launch/launch.toml
```

Submit a SELECT through MO's MySQL endpoint using a table with columns `n` and `s`.

```sql
/*+ SIDECAR GPU */ SELECT n, s FROM example WHERE n > 0 ORDER BY n;
```

The existing statement hint selects embedded Sirius. Unsupported plans return
an eligibility error. Exact numeric support covers all 22 canonical TPC-H
queries. Prepared-query offload, direct TAE, multi-CN producers and concurrent
GPU queries are not enabled.

These six fields are the complete CN configuration schema. Omitted timeouts
use the values shown above. Unknown Sirius keys fail parsing even when
`enabled = false`; remove old backend, input-mode and transport settings.

Before upgrading an old Flight deployment, use its previous release to stop
admission, drain consumers and finish lease recovery. Only start this release
after the old owner has quiesced and recovered its state. This binary does not
open or repair old journals. Rollback uses the previous binary and its matching
configuration.

## Ownership and evidence

One GPU query executes at a time; at most sixteen competing requests wait
without readers. MO scan workers feed the existing bounded pipeline into a
synchronous publisher. Native credit is acquired before outgoing allocation.
Sirius coalesces batches on demand rather than buffering a complete table.
Input windows are 64 MiB per binding, and the result window is 64 MiB. Original
MO scan batches remain governed by the normal MO pipeline's bounds.

Result callbacks borrow a bounded Go copy until the callback returns. No query
result is collected by the adapter. Cancellation stops producers, joins their
pipelines and native work, then releases ownership. Fatal GPU failures seal the
runtime and can require restarting MO.

The terminal `Sirius embedded execution` log record contains the statement ID,
backend/input mode, stream count, source rows/bytes, first-row and wall latency,
terminal health and native execution statistics. Charged-byte statistics are
admission accounting, not physical process/GPU memory measurements. The record
does not contain SQL, credentials, row contents or object paths.

## Verify embedded execution locally

Set `MO_SIRIUS_TEST_CONFIG` to a valid, bounded Sirius YAML configuration and
run the native tests from the MatrixOne repository root through the build
environment.

```sh
pixi run --frozen --manifest-path "$SIRIUS_SRC/pixi.toml" -e mo sh -c '
  cd "$MO_SRC"
  export GOROOT="$MO_GO_ROOT" PATH="$MO_GO_ROOT/bin:$PATH"
  MO_SIRIUS=1 MO_CL_CUDA=1 MO_SIRIUS_TEST_CONFIG="/absolute/path/to/sirius.yaml" \
    .agents/skills/mo-dev/scripts/mo-cgo-test \
    -tags sirius_integration -count=1 -timeout=180s \
    -run "^(TestNativeBridgeDataAndCancellation|TestEmbeddedSiriusPublicMOReader)$" \
    ./pkg/sql/compile/siriusbridge ./pkg/tests/sqlintegration
'
```

The public test exercises MySQL results, NULLs, variable-width strings, filtering,
ordering/fetch, committed deletes and transaction-local writes at two streams.
The bridge test verifies native GPU work, cancellation and zero retained
input/result charges after cleanup. CPU-runnable tests separately prove that
blocked publication stops the table scan and that failed transfers release
their lease.

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
