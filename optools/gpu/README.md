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
required CUDA/cuVS inputs are missing. Native provenance includes the Pixi prefix, selected
environment, and lockfile digest, so changing the profile invalidates cached
GPU artifacts.

The build uses separate compiler and CUDA target roots inside the Pixi prefix.
NVCC uses Pixi's host C++ compiler. Driver stubs
are used only for linking; they are excluded from runtime search paths.

Pixi supplies user-space dependencies. GPU execution still requires compatible
NVIDIA hardware and a host driver. This PR configures builds and tests; the
combined Sirius packaging PR owns runtime dependency-closure staging and the
cuVS/Sirius coexistence test. Changing dependency managers does not by itself
reduce the bytes required by the deployed runtime.

Focused contract tests require Python and Make, with no GPU dependencies:

```sh
python3 -m unittest discover -s cgo -p 'test_gpu_toolchain.py' -v
```
