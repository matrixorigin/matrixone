# Optional native GPU toolchain

Ordinary MatrixOne builds do not require Pixi, Python, CUDA, or RAPIDS. GPU
support remains opt-in with `MO_CL_CUDA=1`. The existing system CUDA installation
(`CUDA_PATH`, default `/usr/local/cuda`) plus activated `CONDA_PREFIX` remains
supported when `GPU_TOOLCHAIN_MANIFEST` is unset.

The optional Linux x86_64 Pixi profile provides CUDA 13.3, cuVS 26.08, RMM, and
GCC 14 without a system CUDA installation or Sirius checkout:

```sh
cd optools/gpu
pixi install --frozen
pixi run --frozen export-toolchain
cd ../..
MO_CL_CUDA=1 GPU_TOOLCHAIN_MANIFEST="$PWD/optools/gpu/toolchain.json" make -j8
```

`toolchain.json` is a local build input with absolute paths. Do not commit or
distribute it. Re-export after changing the Pixi lockfile or environment. A
Sirius SDK can export the same schema; use its `toolchain.json` when combining
MO and Sirius so the two components use one CUDA/RAPIDS installation.
Set `GPU_TOOLCHAIN_MANIFEST` in the environment before invoking Make, as in the
example above. A Make command-line assignment is rejected because older GNU
Make versions can omit it from the parse-time resolver's environment.

All GPU native sub-builds and Go test entrypoints resolve the manifest through
`cgo/mo_gpu_toolchain.py`. An explicit manifest must validate completely:
unsupported platform/version, missing files, changed lockfile, package mismatch,
or changed hashed artifact stops the build. It never falls back to system CUDA.
GPU artifact reuse includes this manifest fingerprint in native provenance.

The resolver exports separate compiler, CUDA target include/library, driver
stub, and RAPIDS paths. NVCC uses the manifest's host C++ compiler. Driver stubs
are used only for linking; they are excluded from runtime search paths.

Pixi supplies user-space dependencies. GPU execution still requires compatible
NVIDIA hardware and a host driver. This PR configures builds and tests; the
combined Sirius packaging PR owns runtime dependency-closure staging and the
cuVS/Sirius coexistence test. Changing dependency managers does not by itself
reduce the bytes required by the deployed runtime.

Focused tests require Python and Make, with no GPU dependencies:

```sh
python3 -m unittest discover -s cgo -p 'test_gpu_toolchain.py' -v
```
