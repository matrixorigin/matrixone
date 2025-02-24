# Contributing

To keep the quality of the code high, we have a set of [guidelines](https://github.com/unum-cloud).

- [What's the procedure?](https://github.com/unum-cloud/awesome/blob/main/Workflow.md#organizing-software-development)
- [How to organize branches?](https://github.com/unum-cloud/awesome/blob/main/Workflow.md#branches)
- [How to style commits?](https://github.com/unum-cloud/awesome/blob/main/Workflow.md#commits)

## Navigating the Codebase

Primary kernels are implemented in header files under `include/simsimd/`:

- `dot.h` - dot products for real and complex vectors.
- `spatial.h` - spatial distances: L2, cosine distance.
- `binary.h` - binary distances: Hamming, Jaccard, etc.
- `probability.h` - probability metrics: KL-divergence, Jensen-Shannon, etc.
- `sparse.h` - sparse distances: weighted and normal set intersections.
- `curved.h` - bilinear forms for real and complex vectors, and Mahalanobis distance.

Bindings to other languages are in the respective directories:

- `python/lib.c` - Python bindings.
- `javascript/lib.c` - JavaScript bindings.
- `rust/lib.rs` - Rust bindings.
- `swift/SimSIMD.swift` - Swift bindings.

All tests, benchmarks, and examples are placed in the `scripts/` directory, if compatible with the toolchain of the implementation language.

## C and C++

To rerun experiments utilize the following command:

```sh
sudo apt install libopenblas-dev # BLAS installation is optional, but recommended for benchmarks
cmake -D CMAKE_BUILD_TYPE=Release -D SIMSIMD_BUILD_TESTS=1 -D SIMSIMD_BUILD_BENCHMARKS=1 -D SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS=1 -B build_release
cmake --build build_release --config Release
build_release/simsimd_bench
build_release/simsimd_bench --benchmark_filter=js
build_release/simsimd_test_run_time
build_release/simsimd_test_compile_time # no need to run this one, it's just a compile-time test
```

To utilize `f16` instructions, use GCC 12 or newer, or Clang 16 or newer.
To install them on Ubuntu 22.04, use:

```sh
sudo apt install gcc-12 g++-12
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 100
```

To compile with the default Apple Clang on MacOS, use:

```sh
brew install openblas
cmake -D CMAKE_BUILD_TYPE=Release \
      -D SIMSIMD_BUILD_TESTS=1 \
      -D SIMSIMD_BUILD_BENCHMARKS=1 \
      -D SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS=1 \
      -D CMAKE_PREFIX_PATH="$(brew --prefix openblas)" \
      -D CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES="$(brew --prefix openblas)/include" \
      -B build_release
cmake --build build_release --config Release
```

On MacOS it's recommended to use Homebrew and install Clang, as opposed to "Apple Clang".
Replacing the default compiler across the entire system is not recommended on MacOS, as it may break the system, but you can pass it as an environment variable:

```sh
brew install llvm openblas
cmake -D CMAKE_BUILD_TYPE=Release \
      -D SIMSIMD_BUILD_TESTS=1 \
      -D SIMSIMD_BUILD_BENCHMARKS=1 \
      -D SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS=1 \
      -D CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES="$(brew --prefix openblas)/include" \
      -D CMAKE_C_LINK_FLAGS="-L$(xcrun --sdk macosx --show-sdk-path)/usr/lib" \
      -D CMAKE_EXE_LINKER_FLAGS="-L$(xcrun --sdk macosx --show-sdk-path)/usr/lib" \
      -D CMAKE_C_COMPILER="$(brew --prefix llvm)/bin/clang" \
      -D CMAKE_CXX_COMPILER="$(brew --prefix llvm)/bin/clang++" \
      -D CMAKE_OSX_SYSROOT="$(xcrun --sdk macosx --show-sdk-path)" \
      -D CMAKE_OSX_DEPLOYMENT_TARGET=$(sw_vers -productVersion) \
      -B build_release
cmake --build build_release --config Release
```

When benchmarking, make sure to disable multi-threading in the BLAS library, as it may interfere with the results:

```sh
export OPENBLAS_NUM_THREADS=1 # for OpenBLAS
export MKL_NUM_THREADS=1 # for Intel MKL
export VECLIB_MAXIMUM_THREADS=1 # for Apple Accelerate
export BLIS_NUM_THREADS=1 # for BLIS
```

## Python

Testing:

```sh
pip install -e .                             # to install the package in editable mode
pip install pytest pytest-repeat tabulate    # testing dependencies
pytest scripts/test.py -s -x -Wd             # to run tests

# to check supported SIMD instructions:
python -c "import simsimd; print(simsimd.get_capabilities())" 
```

Here, `-s` will output the logs.
The `-x` will stop on the first failure.
The `-Wd` will silence overflows and runtime warnings.

When building on MacOS, same as with C/C++, use non-Apple Clang version:

```sh
brew install llvm
CC=$(brew --prefix llvm)/bin/clang CXX=$(brew --prefix llvm)/bin/clang++ pip install -e .
```

Benchmarking:

```sh
pip install numpy scipy scikit-learn                 # for comparison baselines
python scripts/bench_vectors.py                      # to run default benchmarks
python scripts/bench_vectors.py --n 1000 --ndim 1536 # batch size and dimensions
```

You can also benchmark against other libraries, filter the numeric types, and distance metrics:

```sh
$ python scripts/bench_vectors.py --help
> usage: bench.py [-h] [--ndim NDIM] [-n COUNT]
>                 [--metric {all,dot,spatial,binary,probability,sparse}]
>                 [--dtype {all,bin8,int8,uint16,uint32,float16,float32,float64,bfloat16,complex32,complex64,complex128}] 
>                 [--scipy] [--scikit] [--torch] [--tf] [--jax]
> 
> Benchmark SimSIMD vs. other libraries
> 
> optional arguments:
>   -h, --help            show this help message and exit
>   --ndim NDIM           Number of dimensions in vectors (default: 1536) For binary vectors (e.g., Hamming, Jaccard), this is the number of bits. In
>                         case of SimSIMD, the inputs will be treated at the bit-level. Other packages will be matching/comparing 8-bit integers. The
>                         volume of exchanged data will be identical, but the results will differ.
>   -n COUNT, --count COUNT
>                         Number of vectors per batch (default: 1) By default, when set to 1 the benchmark will generate many vectors of size (ndim, )
>                         and call the functions on pairs of single vectors: both directly, and through `cdist`. Alternatively, for larger batch sizes
>                         the benchmark will generate two matrices of size (n, ndim) and compute: - batch mode: (n) distances between vectors in
>                         identical rows of the two matrices, - all-pairs mode: (n^2) distances between all pairs of vectors in the two matrices via
>                         `cdist`.
>   --metric {all,dot,spatial,binary,probability,sparse}
>                         Distance metric to use, profiles everything by default
>   --dtype {all,bin8,int8,uint16,uint32,float16,float32,float64,bfloat16,complex32,complex64,complex128}
>                         Defines numeric types to benchmark, profiles everything by default
>   --scipy               Profile SciPy, must be installed
>   --scikit              Profile scikit-learn, must be installed
>   --torch               Profile PyTorch, must be installed
>   --tf                  Profile TensorFlow, must be installed
>   --jax                 Profile JAX, must be installed
```


Before merging your changes you may want to test your changes against the entire matrix of Python versions USearch supports.
For that you need the `cibuildwheel`, which is tricky to use on MacOS and Windows, as it would target just the local environment.
Still, if you have Docker running on any desktop OS, you can use it to build and test the Python bindings for all Python versions for Linux:

```sh
pip install cibuildwheel
cibuildwheel
cibuildwheel --platform linux                   # works on any OS and builds all Linux backends
cibuildwheel --platform linux --archs x86_64    # 64-bit x86, the most common on desktop and servers
cibuildwheel --platform linux --archs aarch64   # 64-bit Arm for mobile devices, Apple M-series, and AWS Graviton
cibuildwheel --platform linux --archs i686      # 32-bit Linux
cibuildwheel --platform macos                   # works only on MacOS
cibuildwheel --platform windows                 # works only on Windows
```

You may need root privileges for multi-architecture builds:

```sh
sudo $(which cibuildwheel) --platform linux
```

On Windows and MacOS, to avoid frequent path resolution issues, you may want to use:

```sh
python -m cibuildwheel --platform windows
```

## Rust

```sh
cargo test -p simsimd
cargo test -p simsimd -- --nocapture # To see the output
cargo bench
open target/criterion/report/index.html
```

## JavaScript

### NodeJS

If you don't have the environment configured, here are the [installation options](https://github.com/nvm-sh/nvm?tab=readme-ov-file#install--update-script) with different tools:

```sh
wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash # Linux
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash  # MacOS
```

Install dependencies:

```sh
nvm install 20
npm install -g typescript           # Install the TypeScript compiler globally
npm install --save-dev @types/node  # Install the Node.js type definitions as a dev dependency
```

Testing and benchmarking:

```sh
npm run build-js                    # Build the JavaScript code using TypeScript configurations
npm test                            # Run the test suite
npm run bench                       # Run the benchmark script
```

### Deno

If you don't have the environment configured, here are [installation options](https://docs.deno.com/runtime/getting_started/installation/) with different tools:

```sh
wget -qO- https://deno.land/x/install/install.sh | sh # Linux
curl -fsSL https://deno.land/install.sh | sh          # MacOS
irm https://deno.land/install.ps1 | iex               # Windows
```

Testing:

```sh
deno test --allow-read
```

### Bun

If you don't have the environment configured, here are the [installation options](https://bun.sh/docs/installation) with different tools:

```sh
wget -qO- https://bun.sh/install | bash   # for Linux
curl -fsSL https://bun.sh/install | bash  # for macOS and WSL
```

Testing:

```sh
bun install
bun test ./scripts/test.mjs
```

... wouldn't work for now.

## Swift

```sh
swift build && swift test -v
```

Running Swift on Linux requires a couple of extra steps, as the Swift compiler is not available in the default repositories.
Please get the most recent Swift tarball from the [official website](https://www.swift.org/install/).
At the time of writing, for 64-bit Arm CPU running Ubuntu 22.04, the following commands would work:

```bash
wget https://download.swift.org/swift-5.9.2-release/ubuntu2204-aarch64/swift-5.9.2-RELEASE/swift-5.9.2-RELEASE-ubuntu22.04-aarch64.tar.gz
tar xzf swift-5.9.2-RELEASE-ubuntu22.04-aarch64.tar.gz
sudo mv swift-5.9.2-RELEASE-ubuntu22.04-aarch64 /usr/share/swift
echo "export PATH=/usr/share/swift/usr/bin:$PATH" >> ~/.bashrc
source ~/.bashrc
```

You can check the available images on [`swift.org/download` page](https://www.swift.org/download/#releases).
For x86 CPUs, the following commands would work:

```bash
wget https://download.swift.org/swift-5.9.2-release/ubuntu2204/swift-5.9.2-RELEASE/swift-5.9.2-RELEASE-ubuntu22.04.tar.gz
tar xzf swift-5.9.2-RELEASE-ubuntu22.04.tar.gz
sudo mv swift-5.9.2-RELEASE-ubuntu22.04 /usr/share/swift
echo "export PATH=/usr/share/swift/usr/bin:$PATH" >> ~/.bashrc
source ~/.bashrc
```

Alternatively, on Linux, the official Swift Docker image can be used for builds and tests:

```bash
sudo docker run --rm -v "$PWD:/workspace" -w /workspace swift:5.9 /bin/bash -cl "swift build -c release --static-swift-stdlib && swift test -c release --enable-test-discovery"
```

## GoLang

```sh
cd golang
go test # To test
go test -run=^$ -bench=. -benchmem # To benchmark
```
