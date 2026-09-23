# Optional GPU toolchain for embedded Sirius

Design version: 1.

Tracking: [#28966](https://github.com/matrixorigin/matrixone/issues/28966).
The owner approved this prerequisite split in the
[#28966 decision record](https://github.com/matrixorigin/matrixone/issues/28966#issuecomment-5791713361).
It refines section 7 of the MO-reader-only
[embedded Sirius design v2](https://github.com/aunjgr/matrixone/blob/d6e95dddbd/docs/design/sirius-embedded.md).
Implementation approval remains subject to this PR's CI and code-owner review.

## Contract

An ordinary MatrixOne build has no Pixi or CUDA dependency. `MO_CL_CUDA=1`
continues to opt into MO's GPU vector operations. `MO_SIRIUS=1` independently
opts into the Sirius SDK. A combined process must link one compatible
CUDA/RAPIDS dependency set and retain host ownership of the NVIDIA driver.

The existing system CUDA plus Conda provider remains the default for MO GPU
builds. An explicit `GPU_TOOLCHAIN_MANIFEST` selects a validated toolchain
description; an invalid description fails before compilation and never falls
back to the system installation. The manifest supports MO GPU-only builds
without requiring Sirius source. Pixi is the reproducible provider for the
new profile, while Make consumes paths and provenance rather than depending
on the environment directory's spelling. A manifest selected on the Make
command line is passed to the parse-time resolver explicitly, including on
the older GNU Make used by supported CI hosts.

## Ownership and layout

Pixi resolves and locks CUDA 13.3, cuVS 26.08.01 and its C API, compatible
cuDF/RMM, compilers, and packaging tools. Sirius adds a dedicated MO profile;
its existing default CUDA 13.2 and CUDA 12 environments remain available.
Each build uses frozen lockfile installation. Native artifacts record the
selected package identities, toolchain versions and artifact hashes.

The compiler prefix and CUDA target include/library directories are separate
inputs. Conda-style `targets/x86_64-linux` layouts cannot be described by
substituting one path for every occurrence of `/usr/local/cuda`. All MO native
sub-builds and tests consume the same normalized description, including the
NVCC host compiler. Provider changes invalidate reusable GPU artifacts.

The Sirius SDK adds toolchain metadata without changing C ABI version 1.
The SDK's C consumer proves the Sirius link inputs; final MO packaging also
checks dependencies introduced by `libmo`, particularly `libcuvs_c.so`.
Different sources for the same runtime library identity are rejected.

The distributed binary uses relative runtime paths beside its packaged
libraries. Driver libraries (`libcuda.so.1`, NVML) and linker stubs are never
packaged as runtime implementations or accepted through a runtime library
directory, including a symlink alias. The installed host driver and GPU device
access remain deployment requirements. Build tooling and the complete Pixi
environment are not required in the final runtime artifact.

## Delivery and alternatives

Two toolchain prerequisites can proceed independently: the Sirius Pixi/SDK
extension and MO's configurable GPU toolchain. They freeze their manifest
contract together. The CGo bridge PR integrates their merged revisions and
owns combined packaging and the GPU coexistence test. Direct-TAE storage
protection is deferred; embedded MO-reader input needs no new TAE or
directory-lock code. No intermediate PR enables embedded execution by default.

Keeping separate Pixi and Conda providers in one binary leaves library
selection dependent on search order. Requiring a system toolkit prevents
native development without a system install. Making Pixi mandatory for every
MO build would expand CPU dependencies unnecessarily. The explicit optional
manifest preserves existing builds and gives the combined profile one
verifiable dependency authority.

## Validation and rollout

- Contract tests cover both system and Pixi layouts, invalid manifests,
  missing artifacts, compiler/package identity changes, and GPU artifact
  invalidation. CPU selection never invokes GPU discovery.
- Build CPU, MO-GPU-only, Sirius-only, and combined profiles. A build can
  succeed without GPU hardware; it is not evidence of GPU execution.
- Test packaging after relocation, with the build environment removed from
  runtime lookup. Verify every shipped dependency and reject stub leakage.
- The combined acceptance test executes MO cuVS, a Sirius query with two GPU
  streams, Sirius shutdown, and MO cuVS again in the same process. Record
  exact source and toolchain revisions.
- Retain legacy GPU-provider compatibility during rollout. Existing CPU
  workflows remain unchanged. No system symlink, container rebuild, or driver
  installation is needed to select the Pixi build profile.

The current host's driver availability is a separate execution gate. A missing
GPU cannot be converted into a passing coexistence test. Runtime package-size
improvement is measured after dependency staging; Pixi alone does not remove
libraries required by the workload.
