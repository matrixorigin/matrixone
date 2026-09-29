# Native GPU toolchain

Ordinary CPU-only MatrixOne builds do not require Pixi or CUDA. GPU builds
(`MO_CL_CUDA=1`) use Pixi as their only toolchain and dependency provider;
system CUDA/Conda GPU builds are no longer supported.

The Linux x86_64 profile provides CUDA 13.3, cuVS 26.08, RMM, and GCC 14
without a system CUDA installation or Sirius checkout:

```sh
cd optools/gpu
pixi run --frozen make -C ../.. MO_CL_CUDA=1 -j8
```

For a combined MO/Sirius build, use Sirius's frozen `mo` Pixi environment for
both components. Do not build MO under its standalone profile and Sirius under
another prefix. The embedding bridge PR will verify that the native MO
artifacts and Sirius SDK use the same activated prefix before packaging.

Make and Go test entrypoints require `PIXI_PROJECT_ROOT`,
`PIXI_ENVIRONMENT_NAME`, and `CONDA_PREFIX` from Pixi activation and fail if
required CUDA/cuVS inputs are missing. Native provenance includes the Pixi
prefix, selected environment, and lockfile digest. A changed lock forces a
full thirdparty and CGo rebuild because the compiler or sysroot can change at
the same paths.

The build uses separate compiler and CUDA target roots inside the Pixi prefix.
NVCC uses Pixi's host C++ compiler. Driver stubs are used only for linking;
they are excluded from runtime search paths.

From the MO repository root, use the following upgrade commands. Sirius's
`pixi.lock` covers several environments and platforms, including
packages MO does not use. Do not copy or subset it into MO's standalone lock.
For a cuVS/toolchain upgrade, first update and lock Sirius's `mo` profile,
then synchronize its shared direct constraints into MO and let Pixi resolve
MO's own smaller dependency graph:

```sh
python3 optools/gpu/sync_sirius_mo.py \
  --sirius-manifest /path/to/sirius/pixi.toml --check
python3 optools/gpu/sync_sirius_mo.py \
  --sirius-manifest /path/to/sirius/pixi.toml
cd optools/gpu && pixi lock --check
```

The helper updates MO's manifest and lock only when the shared constraints
change; it leaves MO-only dependencies in place. If the CUDA minor version
changes, the helper first requests the shown `pixi workspace platform edit`
command for MO's `__cuda` virtual package; rerun the helper after that edit.
Build MO under both its own GPU-only profile and Sirius's single `mo` profile,
then rerun the combined cuVS/Sirius test. The standalone MO lock is never used
to supply libraries
to the combined binary.

Pixi supplies user-space dependencies. GPU execution still requires compatible
NVIDIA hardware and a host driver. This PR configures builds and tests; the
combined Sirius packaging PR owns runtime dependency-closure staging and the
cuVS/Sirius coexistence test. Changing dependency managers does not by itself
reduce the bytes required by the deployed runtime.

Focused contract tests require Python and Make, with no GPU dependencies:

```sh
python3 -m unittest discover -s cgo -p 'test_gpu_toolchain.py' -v
python3 -m unittest discover -s optools/gpu -p 'test_sync_sirius_mo.py' -v
python3 -m unittest discover -s optools/images/gpu -p 'test_stage_runtime_libs.py' -v
```
