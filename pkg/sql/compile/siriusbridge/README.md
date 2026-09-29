# Embedded Sirius bridge

This package owns the ABI-v1 engine/query/input/result handles from Sirius's
generated `sirius_c.h`. CN constructs the bridge and adapts it to the compiler's
backend interface; the bridge and compiler do not import each other.

This is the opt-in bridge milestone of the
[embedded Sirius migration](../../../../docs/design/sirius-embedded.md), not
the completed SQL offload. It can build and start an embedded runtime and run
the native bridge integration tests. The real MO-reader SQL path, TPC-H parity,
default cutover, and Flight retirement are separate later milestones. Do not
use a successful build or bridge smoke test as evidence that all 22 queries
work through embedded Sirius.

## Prerequisites

- Linux amd64, CGo, MatrixOne's normal build prerequisites, a compatible
  NVIDIA driver and GPU, and [Pixi](https://pixi.sh). CUDA, cuDF, cuVS, RMM,
  compilers, and `patchelf` come from Sirius's frozen `mo` Pixi environment;
  `/usr/local/cuda` is not required.
- The Sirius commit pinned by MatrixOne at `third_party/sirius`. For a release,
  that commit must be merged into `matrixorigin/sirius:upstream-dev-merge`; the
  `SIRIUS_MERGED_REF` used below must contain the exact SDK source revision.
  An unmerged pin requires an explicit development build and is not a release
  artifact.
- An absolute path to the MatrixOne checkout. Run the commands from the host;
  no sidecar checkout or container rebuild is needed.

## Build the combined MO/Sirius binary

First generate the SDK and its C smoke consumer from Sirius. This is a build
tree artifact, so regenerate it after changing Sirius source or its Pixi lock:

```sh
export MO_SRC=/absolute/path/to/matrixone
export SIRIUS_SRC="$MO_SRC/third_party/sirius"
git -C "$MO_SRC" submodule update --init third_party/sirius
cd "$SIRIUS_SRC"
git submodule update --init duckdb substrait cucascade tae-scanner vcpkg
pixi install --frozen -e mo
pixi run --frozen -e mo mo-build-embedding-sdk
```

Build MO's GPU native library and `mo-service` with the same activated Pixi
prefix. `MO_CL_CUDA=1` also includes MO cuVS, so this is the combined-process
profile; omit it only if MO's GPU vector operations are not needed.

```sh
cd "$SIRIUS_SRC"
pixi run --frozen -e mo sh -c '
  cd "$MO_SRC"
  MO_CL_CUDA=1 MO_SIRIUS=1 \
    SIRIUS_SDK="$SIRIUS_SRC/build/mo/extension/sirius/embedding-sdk" \
    SIRIUS_MERGED_REF=upstream-dev-merge \
    make -j8 build
'
```

`make build` builds MO's native dependencies before linking. The
`build-with-prebuilt-native` target is only for a stage that already has a
matching `cgo/libmo.so` and `thirdparties/install`; it is not the first-build
command. SDK preparation and packaging reject a source path or revision that
does not match MO's committed Sirius submodule pin. CPU-only builds remain
unchanged. Selecting `backend="embedded"` in
a binary without `MO_SIRIUS=1` returns an explicit configuration error.

The output is `mo-service` with an adjacent `lib/` directory (and MO's normal
`dict/`; combined GPU builds also stage `mocl_kernel64.fatbin`). Keep these
together when moving the binary. The NVIDIA driver stays on the host. Check
the package against the prepared SDK under the same Pixi provider:

```sh
cd "$SIRIUS_SRC"
pixi run --frozen -e mo sh -c '
  cd "$MO_SRC"
  python3 optools/sirius_sdk.py verify-package \
    --prepared .sirius-sdk --output lib \
    --sdk "$SIRIUS_SRC/build/mo/extension/sirius/embedding-sdk"
'
```

`lib/sirius-provenance.json` records the binary, Sirius source and SDK,
baseline native libraries, GPU-runtime closure, and packaged hashes. A release
build rejects a dirty Sirius tree or a revision outside `SIRIUS_MERGED_REF`.
For local work on an unmerged Sirius change, set
`SIRIUS_BUILD_MODE=development` on the MO build command, and do not distribute
its resulting package as a release.

## Configure a local runtime

Add this section to the CN file selected by your launch manifest (for the
default local launch, `etc/launch/cn.toml`). Use an absolute path to a Sirius
YAML configuration. The small `test/cpp/operator/result.yaml` in Sirius is a
known integration-test example, not a production capacity profile.

```toml
[cn.sirius]
enabled = true
backend = "embedded"
input-mode = "mo"
native-config-path = "/absolute/path/to/sirius/test/cpp/operator/result.yaml"
gpu-streams = 2
max-waiting-queries = 16
```

The example YAML selects one GPU (`device_id: 0`), reserves 2 GiB of GPU
capacity and 4 GiB of host capacity:

```yaml
sirius:
  topology:
    num_gpus: 1
  space:
    gpu:
      - device_id: 0
        memory_capacity: 2147483648
    host:
      - numa_id: 0
        memory_capacity: 4294967296
```

Choose capacities appropriate for the host before deployment. The embedded
route needs no Flight address,
certificates, resolver, direct-TAE leases, or TN `disable-gc` setting. The
default remains `backend="flight"` during coexistence; embedded selection is
explicit, and `input-mode="tae"` is rejected before native preparation.

After building, a local startup check is:

```sh
cd "$MO_SRC"
./mo-service -launch etc/launch/launch.toml
```

This checks configuration and native runtime startup only. No public SQL or
TPC-H command is provided for this bridge milestone: the MO-reader/result
operator is the next PR. Do not reinterpret an unoffloaded or fallback query
as an embedded pass.

## Verify the bridge on a GPU

Run the native integration package after the combined build. The test suite
uses two Sirius GPU streams and includes a same-process cuVS-before/Sirius/
cuVS-after check. It requires a working GPU and uses the test YAML above.

```sh
cd "$SIRIUS_SRC"
pixi run --frozen -e mo sh -c '
  cd "$MO_SRC"
  MO_CL_CUDA=1 MO_SIRIUS=1 \
    SIRIUS_SDK="$SIRIUS_SRC/build/mo/extension/sirius/embedding-sdk" \
    MO_SIRIUS_TEST_CONFIG="$SIRIUS_SRC/test/cpp/operator/result.yaml" \
    .agents/skills/mo-dev/scripts/mo-cgo-test \
    -count=1 -timeout=180s -tags sirius_integration \
    ./pkg/sql/compile/siriusbridge/
'
```

For the race check, repeat the test command with `-race`. The CGo test helper
requires a current prepared and packaged build; run the build command above
again after changing native or SDK inputs. This is a host/Pixi workflow, not a
Docker-image verification loop.

## Build and runtime failures

- `requires pixi run --frozen -e mo`: invoke MO's build inside Sirius's `mo`
  environment. Do not mix it with a system CUDA or another Pixi prefix.
- `release requires a clean SDK built from a merged Sirius SHA`: clean and
  rebuild Sirius at the merged revision, and point `SIRIUS_MERGED_REF` at a
  containing branch; use development mode only for local unmerged work.
- `missing prepared or packaged Sirius runtime closure` during tests: build
  `mo-service` before running `mo-cgo-test`.
- Native GPU startup failure: check the host driver/GPU and the selected device
  and memory capacities in the Sirius YAML. A fatal native/CUDA failure can
  seal admission and require restarting MO; do not reset the device in-process.

## Bridge contract and packaging

The native build requires Linux amd64 and CGo. The default is release
provenance: the SDK must record a clean source SHA reachable from the supplied
merged branch, plus hashes of its verified C consumer and complete
static/device-link artifacts. Local integration before merge requires explicit
`SIRIUS_BUILD_MODE=development`. The SDK supplies the C compiler verified
against CMake/Ninja's C smoke consumer, C++ linker, and complete ordered
response file. `MO_SIRIUS=1` sets both CGo `CC` and `CXX` to these SDK compilers
so C compilation uses the same sysroot as final linking. The SDK manifest
fingerprint also enters CGo's compile flags, invalidating Go's cache when
external SDK headers or archives change at the same filesystem path.
`MO_CL_CUDA=1` can be enabled independently; it does not enable Sirius
implicitly.

Packaging derives shared dependencies from the verified native consumer,
checks their hashes, and copies them with their original permissions into the
binary's adjacent `lib/`. It rewrites the binary search path to `$ORIGIN/lib`
and every reachable staged library's search path to `$ORIGIN`, including MO's
baseline native libraries supplied by `mo-stage-native-libs`. A combined GPU
build records the actual ELF dependency closure of `libmo` after native
publication, requires the same active Sirius Pixi prefix, and hashes any
additional user-space libraries (notably `libcudart` and `libcuvs_c`) before
Go linking. Packaging rechecks those hashes. Driver libraries, linker stubs,
libraries outside the shared prefix, and unverified baseline files remain
errors. Patching uses independent temporary copies, so hardlinks or symlinks
cannot modify the SDK or original baseline artifacts. It excludes host glibc
and the NVIDIA driver, verifies that every other resolved dependency is local
to the package, and records binary, SDK, baseline, and added GPU-runtime hashes
in `lib/sirius-provenance.json`. Neither native compilation nor a container
rebuild is performed by this packaging step.

Native configuration owns GPU selection and finite host/GPU/spill capacity.
Streams default to 2 (maximum 128), waiting queries to 16 (maximum 16). MO
input uses the existing MO statement snapshot and needs no new TAE storage
hooks.

Preparation validates typed read/output descriptors before starting any lazy
producer. For each MO range, a size-only pass precedes native input acquisition;
only granted credit permits compact clones, NULL bitmap allocation, synchronous
copy, and publication. Native acquisition is the authoritative hard limit
because it includes allocator rounding and per-column descriptors. A producer
must handle `IsNotNeeded` and stop on cancellation. Results retain their native
lease until the output callback finishes. The ABI copies result payloads into a
bounded Go buffer; CN copies vectors into the normal MO memory pool before
calling the existing output function. This is bounded transport, not zero-copy.

Before any C allocation, the bridge caps transient query descriptors (including
plan bytes, every repeated string copy, and conservative descriptor array
charges) at 64 MiB. This is separate from native metadata admission,
which defaults to 256 MiB and charges its own expansion/copy factors. Schemas
are limited to 1024 columns, output names to 1 MiB in total, and each read's
identity/schema metadata to a conservative 1 MiB envelope. The existing 16 MiB
plan limit remains available; native plan expansion is not double-counted in
the transient CGo arena budget.

Go reserves one of `1 + max-waiting-queries` slots before entering native
preparation or allocating that arena. Preparing calls and prepared/running
query owners share this limit; a slot returns only after query cleanup. The
default is 17, matching one active query and sixteen waiting requests.

Cancellation has an independent native call path. Close first cancels and
joins producers, in-flight calls and cancellation subscriptions; only then
does it destroy handles and release admitted resources. Failed cleanup keeps
ownership for a retry and seals new admission.

When the later SQL operator selects embedded execution, rejected MO reads
cannot fall back to native MO execution after selection.
