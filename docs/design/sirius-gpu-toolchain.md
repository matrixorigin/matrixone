# Pixi GPU toolchain for embedded Sirius

Design version: 2.

Tracking: [#28966](https://github.com/matrixorigin/matrixone/issues/28966).
The owner [superseded the earlier optional-provider decision](https://github.com/matrixorigin/matrixone/issues/28966#issuecomment-5793447039):
Pixi is the only supported GPU build provider in the final embedded shape.
Ordinary CPU-only MatrixOne builds remain independent of Pixi.
It refines section 7 of the MO-reader-only
[embedded Sirius design v2](https://github.com/aunjgr/matrixone/blob/d6e95dddbd/docs/design/sirius-embedded.md).
Implementation approval remains subject to this PR's CI and code-owner review.

## Contract

An ordinary CPU-only MatrixOne build has no Pixi or CUDA dependency.
`MO_CL_CUDA=1` enables MO's GPU vector operations and requires execution
inside a frozen Pixi environment; there is no system CUDA/Conda provider or
GPU toolchain JSON manifest. `MO_SIRIUS=1` independently enables the Sirius
SDK, which is built with Pixi. A combined process must build both GPU users
against the same activated Pixi prefix and retain host ownership of the
NVIDIA driver. GPU selection fails before compilation when Pixi activation or
required CUDA/cuVS inputs are missing; it never falls back to `/usr/local/cuda`.

## Ownership and layout

Pixi resolves and locks CUDA 13.3, cuVS 26.08.01 and its C API, compatible
cuDF/RMM, compilers, and packaging tools. Sirius has a dedicated MO profile;
its ordinary CUDA 13.2 and CUDA 12 profiles remain unaffected. MO's own Pixi
profile supports MO GPU-only builds without Sirius source. Combined builds
run under Sirius's MO profile, not a second environment. Every GPU invocation
uses `pixi run --frozen`; native provenance binds the Pixi project, environment,
prefix, and lockfile digest, so changing the installed profile invalidates
reusable native artifacts.

Make and the CGo test wrapper derive the compiler, NVCC, CUDA target
include/library directories and RAPIDS directories from the activated Pixi
prefix. The compiler prefix and CUDA target root are deliberately distinct.
No producer or consumer exports or parses a second GPU manifest. The CPU path
does not inspect Pixi.

The Sirius SDK's existing `link.json` records verified compiler paths, source
revision, link inputs, and artifact hashes; it does not claim to record a Pixi
lock identity. The MO bridge checks that those compilers belong to the active
Sirius `mo` Pixi prefix and records that prefix and its lockfile digest in its
own prepared provenance, without changing C ABI version 1. It records the
actual ELF dependency closure of the SDK consumer and `libmo`; release builds
also verify MO native provenance. Packaging rechecks source hashes and stages
the closure, including `libcuvs_c.so`. Different sources for the same SONAME
are rejected.

The distributed binary uses relative runtime paths beside its packaged
libraries. Driver libraries (`libcuda.so.1`, NVML) and linker stubs are never
packaged as runtime implementations or accepted through a runtime library
directory, including a symlink alias. The installed host driver and GPU device
access remain deployment requirements. Build tooling and the complete Pixi
environment are not required in the final runtime artifact.

## Delivery and alternatives

Two toolchain prerequisites can proceed independently: the Sirius Pixi/SDK
extension and MO's Pixi-only GPU build support. They share the activated
prefix contract, not a new JSON schema. The CGo bridge PR integrates their
merged revisions and owns combined packaging and the GPU coexistence test.
Direct-TAE storage protection is deferred; embedded MO-reader input needs no
new TAE or directory-lock code. No intermediate PR enables embedded execution
by default.

The deleted `go_cuda-133_arch-x86_64.yaml` is not converted at upgrade time:
`optools/gpu/pixi.toml` and its lock are the MO GPU-only source of truth. Sirius
independently locks its `mo` profile. To upgrade cuVS, update the compatible
CUDA/cuVS/RMM constraints and regenerate each affected Pixi lock; verify the
MO GPU-only build and the combined build against Sirius's one activated `mo`
prefix. The bridge rejects mixed prefixes, so differing standalone lockfiles
cannot silently provide libraries to one combined binary.

The rejected optional-provider design added a bespoke manifest exporter,
resolver, and two copies of GPU package identity. A system CUDA/Conda fallback
would preserve an incompatible build path and allow mixed provider selection.
Making Pixi mandatory for ordinary CPU-only MO builds would add an unrelated
dependency. Pixi is therefore mandatory only when building GPU-capable MO or
Sirius; combined builds use one activated environment and the SDK's existing
link provenance.

## Validation and rollout

- Contract tests cover missing/wrong Pixi activation, absent CUDA/cuVS inputs,
  native lock/prefix invalidation, and CPU builds without Pixi.
- Build CPU, MO-GPU-only, Sirius-only, and combined profiles. A build can
  succeed without GPU hardware; it is not evidence of GPU execution.
- Test packaging after relocation, with the build environment removed from
  runtime lookup. Verify every shipped dependency and reject stub leakage.
- The combined acceptance test executes MO cuVS, a Sirius query with two GPU
  streams, Sirius shutdown, and MO cuVS again in the same process. Record
  exact source and toolchain revisions.
- Existing CPU workflows remain unchanged. GPU builders use the frozen Pixi
  profile; no system CUDA symlink, container rebuild, or driver installation
  is needed for compilation. Historical system CUDA/Conda GPU builders must
  migrate before this PR merges.

The current host's driver availability is a separate execution gate. A missing
GPU cannot be converted into a passing coexistence test. Runtime package-size
improvement is measured after dependency staging; Pixi alone does not remove
libraries required by the workload.
