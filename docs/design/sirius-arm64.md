# Embedded Sirius on Linux ARM64

## Problem and contract

The local GB10 host runs Linux/aarch64, while MO's embedded Sirius bridge and
Sirius's `mo` Pixi feature only admit amd64. Sirius's ordinary CUDA profiles
already include Linux ARM64. This change extends the embedded build to native
Linux arm64 without changing the C ABI, query behavior, or resource lifecycle.

Both native consumers must use one frozen Sirius `mo` Pixi prefix, with native
ARM compilers and CUDA headers/libraries. An amd64 artifact must never satisfy
an arm64 provenance check. Driver libraries and linker stubs must never be
packaged. Unsupported hosts continue to fail before compilation.

## Decision and ownership

Use native ARM64 builds. Keeping amd64-only builds cannot run on this host;
emulating amd64 adds an unnecessary CPU and CUDA ABI boundary; using the system
CUDA installation would break the existing frozen-provider contract. CUDA and
RAPIDS support ARM SBSA (see NVIDIA's DGX Spark compilation guide and RAPIDS
ARM SBSA support notice). Package availability is proved by Pixi resolution.

Sirius owns the platform-specific `mo` compiler dependencies and lockfile.
MO's Make/shell GPU helpers select native compiler and CUDA target paths from
the host (aarch64 compiler, `targets/sbsa-linux` CUDA libraries), its bridge
admits linux/arm64, and SDK packaging checks native provenance against that
host. The standalone MO GPU profile remains amd64;
ARM builds use Sirius's shared profile. No runtime fallback or cross-compilation
is introduced. Preserve amd64's Haswell host ISA floor; ARM uses ARMv8-A.

The shared profile selects Ninja for MO's CMake dependencies, avoiding system
Make/Pixi Make jobserver incompatibility. MO's Go installation must meet its
repository version requirement; when Pixi supplies an older Go, select both
the host Go executable and its matching GOROOT for MO build/test commands.

The bridge already uses CGo-generated sizeof values and fixed-width integers.
Its bounded descriptor arena and little-endian input contract remain unchanged.
Native C smoke and bridge integration tests must prove the ABI on ARM.

Development builds can consume the locally modified pinned Sirius checkout.
Release delivery still requires the Sirius change to be committed and merged,
then the MO submodule pin advanced; this task does not waive that provenance gate.
This MatrixOne change leaves the Sirius submodule pin unchanged. The native ARM
Pixi profile and any Sirius revision update are separate delivery dependencies.
Rollback is the original pin/profile and architecture guard; no persisted data,
wire format, upgrade state, concurrency, or security contract changes.

## Validation and review

R2 closure: Sirius Pixi profile -> MO GPU helpers -> CGo bridge -> packaging.
R0 closure: platform documentation. Base/head for this local change is
1b66ff135c64ef1c642a297105bf599cd3173dee; no origin/main ref is available.
The pre-existing untracked `.codegraph/` is outside scope.

Use existing deterministic Make/shell and SDK fixture tests for both native
architectures, plus mismatched-prefix, missing-input and wrong-architecture
rejection. Check Go build-file selection for both architectures. Resolve and
install the frozen ARM profile, build its C smoke consumer, then link MO and
run the existing native bridge integration tests. SQL/BVT behavior is unchanged;
the existing native tests are the consumer proof for this build-only change.
Full GPU execution evidence remains required before claiming runnable support.

Design review (before implementation): PASS for the native-only architecture
extension above. Provider ownership, failure behavior, ABI and release gates
are preserved. Package resolution and native execution are acceptance evidence,
not assumptions. No unrelated runtime redesign is authorized.

References:
- https://docs.nvidia.com/dgx/dgx-spark-porting-guide/porting/compilation.html
- https://docs.rapids.ai/notices/rsn0016/

## Historical local acceptance evidence (2026-10-13)

Host: Linux/aarch64, NVIDIA GB10, driver 580.178.04. Sirius source pin:
955b6881afa6e92d0995f43e3fe9c383d86981c0 with local `pixi.toml`/`pixi.lock`
changes. Go: 1.27.2; native compiler: GCC 14.4.0; CUDA: 13.3.73.

- Frozen ARM `mo` resolution and installation passed; all existing environment
  package selections were preserved, with one new ARM MO platform.
- `mo-build-embedding-sdk` completed all 859 build steps. The AArch64
  `sirius_c_smoke` consumer passed (exit 0).
- `make -j8 build MO_SIRIUS=1 SIRIUS_BUILD_MODE=development` under the shared
  profile passed (exit 0), producing AArch64 `mo-service` and staged `lib/`.
  Package verification passed; the binary's RUNPATH is `$ORIGIN/lib`.
- `mo-cgo-test -v -count=1 -timeout=180s -tags sirius_integration
  ./pkg/sql/compile/siriusbridge` passed (exit 0), including
  `TestNativeBridgeDataAndCancellation`: scalar GPU result, empty/constant-null
  input, and cancellation of the native result wait and producer.
- Native-tag `go vet` passed with the same compilers and headers. Go file
  selection includes the native bridge on amd64/arm64 and the stub with CGo
  disabled. Formatting and both repository diff checks passed.
- 16 Make/shell GPU contract tests, 16 SDK tests, 6 synchronization tests, 8
  Sirius export tests, and wrapper tag checks for amd64/arm64 passed.
- A real MO cuVS `helper.o` compiled with native GCC/NVCC, SBSA headers and
  `-march=armv8-a` (exit 0).

Self-review: local change scope reviewed against the base above. Platform,
provider, native ABI, packaging, failure rejection, test isolation and release
gates are covered. No query semantics, state ownership or concurrency model
changed, so no new SQL BVT, race or performance suite was added. The configured
Go linter is not installed; full CI was not run. The optional
`MO_CL_CUDA=1` cuVS/Sirius coexistence runtime test was not run; acceptance here
is the embedded Sirius profile with MO's own GPU vector operations disabled.
Release delivery still needs a merged Sirius change and updated submodule pin.

## Validation with updated Sirius (2026-10-21)

The unchanged MatrixOne ARM implementation was validated in isolated snapshot
`eb1ca6c4fc`, based on `1362ffba70`, with Sirius
`13ad4bd00d79fdaf07b1d2ff7a2780a787b2056e`. The isolated Sirius checkout restored
the native ARM `mo` Pixi profile and resolved its lockfile. The snapshot recorded
that Sirius revision to satisfy SDK provenance checks; that pin update is not
part of this MatrixOne change.

The SDK completed all 895 build steps. Its C/GPU smoke consumer, the MatrixOne
development build and package verification, 38 toolchain/SDK/synchronization
unit tests, wrapper checks for both architectures, and all 32 bridge tests passed.

The existing public MO-reader integration test was enabled on ARM only in the
validation checkout. It failed three integer-overflow eligibility assertions:
hinted queries returned MySQL error 1690 (`data out of range`) instead of error
1105 containing `not eligible`. This run does not establish full public SQL
acceptance. The combined MO cuVS/Sirius runtime profile was not tested.

The committed Sirius pin remains
`06400cf48073e51955067bfd75c5c4defe955a72`; the isolated build is evidence for
the MatrixOne implementation with the stated native dependency, not an ARM
build validation of that unchanged pin.
