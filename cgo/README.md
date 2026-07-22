MatrixOne CGO Kernel
===============================

This directory contains CGO source code for MatrixOne. Running `make` produces the core library files used by Go code.

On the Go side, the integration typically uses `mo.h` and links against the generated libraries:
```
mo.h
libmo.a / libmo.so
```

`mo.h` should remain pristine, containing only C function prototypes for Go to consume. Data passed between Go and C should be limited to standard types (int, float, double, pointers). Always specify explicit integer sizes (e.g., `int32_t`, `uint64_t`) and avoid platform-dependent types like `int` or `long`.

GPU Support (CUDA & cuVS)
-------------------------
The kernel supports GPU acceleration for certain operations (e.g., vector search) via NVIDIA CUDA and the cuVS library.

- **Build Flag:** GPU support is enabled by setting `MO_CL_CUDA=1` during the build.
- **Environment:** Requires a working CUDA installation and a Conda environment with `cuvs` and `rmm` installed.
- **Source Code:** GPU-specific code resides in the `cuda/` and `cuvs/` subdirectories.

Implementation Notes
--------------------

1. **Language:** Core kernel is Pure C. GPU extensions use C++ and CUDA, wrapped in a C-compatible interface.
2. **Memory Management:** Prefer using memory allocated and passed from Go. Minimize internal allocations in C/C++ code.
3. **Dependencies:** The base kernel depends only on `libc`, `libm`, and `libusearch`. GPU builds introduce dependencies on CUDA, `cuvs`, and `rmm`.
4. **Third-party Libraries:** If a third-party library is necessary, it should be built from source (see `thirdparties/` directory). C++ libraries must be fully wrapped in C before being exposed to Go.
