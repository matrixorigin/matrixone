#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module: bench.py

This script benchmarks the performance of SimSIMD against other libraries,
such as NumPy, SciPy, scikit-learn, PyTorch, TensorFlow, and JAX.
It can operate in 2 modes: 

    1. Batch mode
    2. All-Pairs mode.

It also provides necessary primitives for performance visualizations and
other benchmarking scripts, like `bench_vectors_live.py`.
"""
import os
import time
import argparse
from typing import List, Generator, Union
from dataclasses import dataclass


#! Before all else, ensure that we use only one thread for each library
os.environ["OMP_NUM_THREADS"] = "1"  # OpenMP
os.environ["MKL_NUM_THREADS"] = "1"  # MKL
os.environ["NUMEXPR_NUM_THREADS"] = "1"  # NumExpr
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"  # Accelerate
os.environ["OPENBLAS_NUM_THREADS"] = "1"  # OpenBLAS

# NumPy and SimSIMD are obligatory for benchmarking
import numpy as np
import simsimd as simd
import tabulate

# Set to ignore all floating-point errors
np.seterr(all="ignore")


metric_families = [
    "dot",  # Dot product
    "spatial",  # Euclidean and Cosine distance
    "binary",  # Hamming and Jaccard distance for binary vectors
    "probability",  # Jensen-Shannon and Kullback-Leibler divergences for probability distributions
    "sparse",  # Intersection of two sparse integer sets, with float/int weights
]
dtype_names = [
    "bin8",  #! Not supported by SciPy
    "int8",  #! Presented as supported, but overflows most of the time
    "uint16",
    "uint32",
    "float16",
    "float32",
    "float64",
    "bfloat16",  #! Not supported by NumPy
    "complex32",  #! Not supported by NumPy
    "complex64",
    "complex128",
]


@dataclass
class Kernel:
    """Data class to store information about a numeric kernel."""

    name: str
    dtype: str
    baseline_one_to_one_func: callable
    baseline_many_to_many_func: callable
    baseline_all_pairs_func: callable
    simsimd_func: callable
    simsimd_all_pairs_func: callable
    tensor_type: callable = np.array


def serial_cosine(a: List[float], b: List[float]) -> float:
    dot_product = sum(ai * bi for ai, bi in zip(a, b))
    norm_a = sum(ai * ai for ai in a) ** 0.5
    norm_b = sum(bi * bi for bi in b) ** 0.5
    if norm_a == 0 and norm_b == 0:
        return 1
    if dot_product == 0:
        return 0
    return dot_product / (norm_a * norm_b)


def serial_sqeuclidean(a: List[float], b: List[float]) -> float:
    return sum((ai - bi) ** 2 for ai, bi in zip(a, b))


def yield_kernels(
    metric_families: List[str],
    dtype_names: List[str],
    include_scipy: bool = False,
    include_scikit: bool = False,
    include_torch: bool = False,
    include_tf: bool = False,
    include_jax: bool = False,
) -> Generator[Kernel, None, None]:
    """Yield a list of kernels to latency."""

    if include_scipy:
        import scipy as sp
        import scipy.spatial.distance as spd
        import scipy.special as scs

    if include_scikit:
        import sklearn as sk
        import sklearn.metrics.pairwise as skp

    if include_torch:
        import torch
    if include_tf:
        # Disable TensorFlow warning messages
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # This hides INFO and WARNING messages

        import tensorflow as tf

        # This will show only ERROR messages, not WARNING messages.
        # Additionally, to filter out oneDNN related warnings, you might need to:
        tf.get_logger().setLevel("FATAL")

    if include_jax:
        import jax
        import jax.numpy as jnp

    # Define a few helper functions to wrap non-vectorized kernels
    def wrap_rows_batch_calls(baseline_one_to_one_func):
        """Wrap a function to apply it row-wise to rows of two matrices.
        It's needed as SciPy functions don't support batch processing out of the box."""

        def wrapped(A, B):
            for i in range(A.shape[0]):
                baseline_one_to_one_func(A[i], B[i])

        return wrapped

    def wrap_rows_all_pairs_calls(baseline_one_to_one_func):
        """Wrap a function to apply it row-wise to all possible pairs of rows of two matrices.
        It's needed as NumPy `vdot`-like functions don't support batch processing out of the box."""

        def wrapped(A, B):
            for i in range(A.shape[0]):
                for j in range(B.shape[0]):
                    baseline_one_to_one_func(A[i], B[j])

        return wrapped

    def raise_(ex):
        """Utility function to allow raising exceptions in lambda functions."""
        raise ex

    def for_dtypes(
        name: str,
        dtypes: List[str],
        baseline_one_to_one_func: callable,
        baseline_many_to_many_func: callable,
        baseline_all_pairs_func: callable,
        simsimd_func: callable,
        simsimd_all_pairs_func: callable,
        tensor_type: callable = np.array,
    ) -> list:
        """Filter out unsupported data types."""
        return [
            Kernel(
                name=name,
                baseline_one_to_one_func=baseline_one_to_one_func,
                baseline_many_to_many_func=baseline_many_to_many_func,
                baseline_all_pairs_func=baseline_all_pairs_func,
                simsimd_func=simsimd_func,
                simsimd_all_pairs_func=simsimd_all_pairs_func,
                tensor_type=tensor_type,
                dtype=dtype,
            )
            for dtype in dtypes
            if dtype in dtype_names
        ]

    if "dot" in metric_families:
        yield from for_dtypes(
            "numpy.dot",
            ["float64", "float32", "float16", "int8", "complex64", "complex128"],
            np.dot,
            lambda A, B: np.sum(A * B, axis=1),
            lambda A, B: np.dot(A, B.T),
            simd.dot,
            lambda A, B: simd.cdist(A, B, metric="dot"),
        )
        yield from for_dtypes(
            "numpy.dot",
            ["complex32"],
            lambda A, B: raise_(NotImplementedError("Not implemented for complex32")),
            lambda A, B: raise_(NotImplementedError("Not implemented for complex32")),
            lambda A, B: raise_(NotImplementedError("Not implemented for complex32")),
            lambda A, B: simd.dot(A, B, "complex32"),
            lambda A, B: simd.cdist(A, B, "complex32", metric="dot"),
        )
        yield from for_dtypes(
            "numpy.dot",
            ["bfloat16"],
            lambda A, B: raise_(NotImplementedError("Not implemented for bfloat16")),
            lambda A, B: raise_(NotImplementedError("Not implemented for bfloat16")),
            lambda A, B: raise_(NotImplementedError("Not implemented for bfloat16")),
            lambda A, B: simd.dot(A, B, "bfloat16"),
            lambda A, B: simd.cdist(A, B, "bfloat16", metric="dot"),
        )
        yield from for_dtypes(
            "numpy.vdot",
            ["complex64", "complex128"],
            np.vdot,
            wrap_rows_batch_calls(np.vdot),
            wrap_rows_all_pairs_calls(np.vdot),
            simd.vdot,
            lambda A, B: simd.cdist(A, B, metric="vdot"),
        )
    if "spatial" in metric_families:
        yield from for_dtypes(
            "serial.cosine",
            ["float64", "float32", "float16", "int8"],
            serial_cosine,
            wrap_rows_batch_calls(serial_cosine),
            lambda A, B: spd.cdist(A, B, "cosine"),
            simd.cosine,
            lambda A, B: simd.cdist(A, B, metric="cosine"),
        )
        yield from for_dtypes(
            "serial.sqeuclidean",
            ["float64", "float32", "float16", "int8"],
            serial_sqeuclidean,
            wrap_rows_batch_calls(serial_sqeuclidean),
            lambda A, B: spd.cdist(A, B, "sqeuclidean"),
            simd.sqeuclidean,
            lambda A, B: simd.cdist(A, B, metric="sqeuclidean"),
        )
    if "spatial" in metric_families and include_scipy:
        yield from for_dtypes(
            "scipy.cosine",
            ["float64", "float32", "float16", "int8"],
            spd.cosine,
            wrap_rows_batch_calls(spd.cosine),
            lambda A, B: spd.cdist(A, B, "cosine"),
            simd.cosine,
            lambda A, B: simd.cdist(A, B, metric="cosine"),
        )
        yield from for_dtypes(
            "scipy.cosine",
            ["bfloat16"],
            lambda A, B: raise_(NotImplementedError(f"Not implemented for bfloat16")),
            lambda A, B: raise_(NotImplementedError(f"Not implemented for bfloat16")),
            lambda A, B: raise_(NotImplementedError(f"Not implemented for bfloat16")),
            lambda A, B: simd.cosine(A, B, "bfloat16"),
            lambda A, B: simd.cdist(A, B, "bfloat16", metric="cosine"),
        )
        yield from for_dtypes(
            "scipy.sqeuclidean",
            ["float64", "float32", "float16", "int8"],
            spd.sqeuclidean,
            wrap_rows_batch_calls(spd.sqeuclidean),
            lambda A, B: spd.cdist(A, B, "sqeuclidean"),
            simd.sqeuclidean,
            lambda A, B: simd.cdist(A, B, metric="sqeuclidean"),
        )

    if "probability" in metric_families and include_scipy:
        yield from for_dtypes(
            "scipy.jensenshannon",
            ["float64", "float32", "float16"],
            spd.jensenshannon,
            wrap_rows_batch_calls(spd.jensenshannon),
            lambda A, B: spd.cdist(A, B, "jensenshannon"),
            simd.jensenshannon,
            lambda A, B: simd.cdist(A, B, metric="jensenshannon"),
        )
        yield from for_dtypes(
            "scipy.kl_div",
            ["float64", "float32", "float16"],
            scs.kl_div,
            wrap_rows_batch_calls(scs.kl_div),
            wrap_rows_all_pairs_calls(scs.kl_div),
            simd.kullbackleibler,
            lambda A, B: simd.cdist(A, B, metric="kullbackleibler"),
        )
    if "binary" in metric_families and include_scipy:
        yield from for_dtypes(
            "scipy.hamming",
            ["bin8"],
            spd.hamming,
            wrap_rows_batch_calls(spd.hamming),
            lambda A, B: spd.cdist(A, B, "hamming"),
            lambda A, B: simd.hamming(A, B, "bin8"),
            lambda A, B: simd.cdist(A, B, "bin8", metric="hamming"),
        )
        yield from for_dtypes(
            "scipy.jaccard",
            ["bin8"],
            spd.jaccard,
            wrap_rows_batch_calls(spd.jaccard),
            lambda A, B: spd.cdist(A, B, "jaccard"),
            lambda A, B: simd.jaccard(A, B, "bin8"),
            lambda A, B: simd.cdist(A, B, "bin8", metric="jaccard"),
        )
    if "spatial" in metric_families and include_scikit:
        yield from for_dtypes(
            "sklearn.cosine_similarity",
            ["float64", "float32", "float16", "int8"],
            lambda A, B: skp.cosine_similarity(A.reshape(1, len(A)), B.reshape(1, len(B))),
            lambda A, B: raise_(NotImplementedError("Not implemented for many-to-many")),
            skp.paired_cosine_distances,
            simd.cosine,
            lambda A, B: simd.cdist(A, B, metric="cosine"),
        )
        yield from for_dtypes(
            "sklearn.euclidean_distances",
            ["float64", "float32", "float16", "int8"],
            lambda A, B: skp.euclidean_distances(A.reshape(1, len(A)), B.reshape(1, len(B))),
            lambda A, B: raise_(NotImplementedError("Not implemented for many-to-many")),
            skp.paired_euclidean_distances,
            simd.sqeuclidean,
            lambda A, B: simd.cdist(A, B, metric="sqeuclidean"),
        )
    if "dot" in metric_families and include_tf:
        yield from for_dtypes(
            "tensorflow.tensordot",
            ["float64", "float32", "float16", "int8"],
            lambda A, B: tf.tensordot(A, B, axes=1).numpy(),
            lambda A, B: tf.reduce_sum(tf.multiply(A, B), axis=1).numpy(),
            lambda A, B: tf.tensordot(A, B.T, axes=1).numpy(),
            simd.dot,
            lambda A, B: simd.cdist(A, B, metric="dot"),
            tf.convert_to_tensor,
        )
    if "dot" in metric_families and include_jax:
        yield from for_dtypes(
            "jax.numpy.dot",
            ["float64", "float32", "float16", "int8"],
            lambda A, B: jnp.dot(A, B).block_until_ready(),
            lambda A, B: jnp.einsum("ij,ij->i", A, B).block_until_ready(),
            lambda A, B: jnp.dot(A, B.T).block_until_ready(),
            simd.dot,
            lambda A, B: simd.cdist(A, B, metric="dot"),
            jnp.array,
        )
    if "dot" in metric_families and include_torch:
        yield from for_dtypes(
            "torch.dot",
            ["float64", "float32", "float16", "int8"],
            lambda A, B: torch.dot(A, B).item(),
            lambda A, B: torch.bmm(A.unsqueeze(1), B.unsqueeze(2)).squeeze(),
            lambda A, B: torch.dot(A, B.T).item(),
            simd.dot,
            lambda A, B: simd.cdist(A, B, metric="dot"),
            torch.tensor,
        )


@dataclass
class Result:
    dtype: str
    name: str
    baseline_seconds: Union[float, Exception]
    simsimd_seconds: Union[float, Exception]
    bytes_per_vector: int
    distance_calculations: int


def random_matrix(count: int, ndim: int, dtype: str) -> np.ndarray:
    if dtype == "complex128":
        return (
            np.random.randn(count, ndim // 2).astype(np.float64)
            + 1j * np.random.randn(count, ndim // 2).astype(np.float64)
        ).view(np.complex128)
    if dtype == "complex64":
        return (
            np.random.randn(count, ndim // 2).astype(np.float32)
            + 1j * np.random.randn(count, ndim // 2).astype(np.float32)
        ).view(np.complex64)
    if dtype == "complex32":
        return np.random.randn(count, ndim).astype(np.float16)
    if dtype == "float64":
        return np.random.randn(count, ndim).astype(np.float64)
    if dtype == "float32":
        return np.random.randn(count, ndim).astype(np.float32)
    if dtype == "float16":
        return np.random.randn(count, ndim).astype(np.float16)
    if dtype == "bfloat16":
        return np.random.randint(0, high=256, size=(count, ndim), dtype=np.int16)
    if dtype == "int8":
        return np.random.randint(-100, high=100, size=(count, ndim), dtype=np.int8)
    if dtype == "bin8":
        return np.packbits(np.random.randint(0, high=2, size=(count, ndim), dtype=np.uint8), axis=0)


def latency(func, A, B, iterations: int = 1, warmup: int = 0) -> float:
    """Time the amount of time it takes to run a function and return the average time per run in seconds."""
    while warmup > 0:
        func(A, B)
        warmup -= 1
    start_time = time.time_ns()
    while iterations > 0:
        func(A, B)
        iterations -= 1
    end_time = time.time_ns()
    return (end_time - start_time) / 1e9


def yield_batch_results(
    count_vectors_per_matrix: int,
    ndim: int,
    kernels: List[Kernel],
    warmup: int = 0,
) -> Generator[Result, None, None]:
    # For each of the present data types, we may want to pre-generate several random matrices
    count_matrices_per_dtype = 16
    count_repetitions_per_matrix = 3  # This helps dampen the effect of time-measurement itself
    matrices_per_dtype = {}
    for kernel in kernels:
        if kernel.dtype in matrices_per_dtype:
            continue
        matrices = [
            random_matrix(count_vectors_per_matrix, ndim, kernel.dtype) for _ in range(count_matrices_per_dtype)
        ]
        if count_vectors_per_matrix == 1:
            matrices = [m.flatten() for m in matrices]
        matrices_per_dtype[kernel.dtype] = matrices

    # For each kernel, repeat benchmarks for each data type
    for kernel in kernels:
        matrices_numpy = matrices_per_dtype[kernel.dtype]
        matrices_converted = [kernel.tensor_type(m) for m in matrices_numpy]
        baseline_one_to_one_func = (
            kernel.baseline_one_to_one_func if count_vectors_per_matrix == 1 else kernel.baseline_many_to_many_func
        )
        simsimd_func = kernel.simsimd_func
        result = Result(
            kernel.dtype,
            kernel.name,
            baseline_seconds=0,
            simsimd_seconds=0,
            bytes_per_vector=matrices_numpy[0].nbytes // count_vectors_per_matrix,
            distance_calculations=count_vectors_per_matrix * count_matrices_per_dtype * count_repetitions_per_matrix,
        )

        # Try obtaining the baseline measurements
        try:
            for i in range(1, count_matrices_per_dtype):
                result.baseline_seconds += latency(
                    baseline_one_to_one_func,
                    matrices_converted[i - 1],
                    matrices_converted[i],
                    count_repetitions_per_matrix,
                    warmup,
                )
        except NotImplementedError as e:
            result.baseline_seconds = e
        except ValueError as e:
            result.baseline_seconds = e  #! This happens often during overflows
        except RuntimeError as e:
            result.baseline_seconds = e  #! This happens often during overflows
        except Exception as e:
            # This is an unexpected exception... once you face it, please report it
            raise RuntimeError(str(e) + " for %s(%s)" % (kernel.name, str(kernel.dtype))) from e

        # Try obtaining the SimSIMD measurements
        try:
            for i in range(1, count_matrices_per_dtype):
                result.simsimd_seconds += latency(
                    simsimd_func,
                    matrices_numpy[i - 1],
                    matrices_numpy[i],
                    count_repetitions_per_matrix,
                    warmup,
                )
        except NotImplementedError as e:
            result.simsimd_seconds = e
        except Exception as e:
            # This is an unexpected exception... once you face it, please report it
            raise RuntimeError(str(e) + " for %s(%s)" % (kernel.name, str(kernel.dtype))) from e

        yield result


def yield_all_pairs_results(
    count_vectors_per_matrix: int,
    ndim: int,
    kernels: List[Kernel],
    warmup: int = 0,
) -> Generator[Result, None, None]:
    # For each of the present data types, we may want to pre-generate several random matrices
    count_matrices_per_dtype = 16
    count_repetitions_per_matrix = 3  # This helps dampen the effect of time-measurement itself
    matrices_per_dtype = {}
    for kernel in kernels:
        if kernel.dtype in matrices_per_dtype:
            continue
        matrices = [
            random_matrix(count_vectors_per_matrix, ndim, kernel.dtype) for _ in range(count_matrices_per_dtype)
        ]
        matrices_per_dtype[kernel.dtype] = matrices

    # For each kernel, repeat benchmarks for each data type
    for kernel in kernels:
        matrices_numpy = matrices_per_dtype[kernel.dtype]
        matrices_converted = [kernel.tensor_type(m) for m in matrices_numpy]
        baseline_one_to_one_func = kernel.baseline_all_pairs_func
        simsimd_func = kernel.simsimd_all_pairs_func
        result = Result(
            kernel.dtype,
            kernel.name,
            baseline_seconds=0,
            simsimd_seconds=0,
            bytes_per_vector=matrices_numpy[0].nbytes // count_vectors_per_matrix,
            distance_calculations=(count_vectors_per_matrix**2)
            * count_matrices_per_dtype
            * count_repetitions_per_matrix,
        )

        # Try obtaining the baseline measurements
        try:
            for i in range(1, count_matrices_per_dtype):
                result.baseline_seconds += latency(
                    baseline_one_to_one_func,
                    matrices_converted[i - 1],
                    matrices_converted[i],
                    count_repetitions_per_matrix,
                    warmup,
                )
        except NotImplementedError as e:
            result.baseline_seconds = e
        except ValueError as e:
            result.baseline_seconds = e  #! This happens often during overflows
        except RuntimeError as e:
            result.baseline_seconds = e  #! This happens often during overflows
        except Exception as e:
            # This is an unexpected exception... once you face it, please report it
            raise RuntimeError(str(e) + " for %s(%s)" % (kernel.name, str(kernel.dtype))) from e

        # Try obtaining the SimSIMD measurements
        try:
            for i in range(1, count_matrices_per_dtype):
                result.simsimd_seconds += latency(
                    simsimd_func,
                    matrices_numpy[i - 1],
                    matrices_numpy[i],
                    count_repetitions_per_matrix,
                    warmup,
                )
        except NotImplementedError as e:
            result.simsimd_seconds = e
        except Exception as e:
            # This is an unexpected exception... once you face it, please report it
            raise RuntimeError(str(e) + " for %s(%s)" % (kernel.name, str(kernel.dtype))) from e

        yield result


def result_to_row(result: Result) -> List[str]:
    dtype_cell = f"`{result.dtype}`"
    name_cell = f"`{result.name}`"
    baseline_cell = "💥"
    simsimd_cell = "💥"
    improvement_cell = "🤷"

    if isinstance(result.baseline_seconds, float):
        ops_per_second = result.distance_calculations / result.baseline_seconds
        gbs_per_second = result.bytes_per_vector * ops_per_second / 1e9
        baseline_cell = f"{ops_per_second:,.0f} ops/s, {gbs_per_second:,.3f} GB/s"
    if isinstance(result.simsimd_seconds, float):
        ops_per_second = result.distance_calculations / result.simsimd_seconds
        gbs_per_second = result.bytes_per_vector * ops_per_second / 1e9
        simsimd_cell = f"{ops_per_second:,.0f} ops/s, {gbs_per_second:,.3f} GB/s"
    if isinstance(result.baseline_seconds, float) and isinstance(result.simsimd_seconds, float):
        improvement_cell = f"{result.baseline_seconds / result.simsimd_seconds:,.2f} x"

    return [dtype_cell, name_cell, baseline_cell, simsimd_cell, improvement_cell]


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Benchmark SimSIMD and other libraries")
    parser.add_argument(
        "--ndim",
        type=int,
        default=1536,
        help="""
            Number of dimensions in vectors (default: 1536)
                        
            For binary vectors (e.g., Hamming, Jaccard), this is the number of bits.
            In case of SimSIMD, the inputs will be treated at the bit-level.
            Other packages will be matching/comparing 8-bit integers.
            The volume of exchanged data will be identical, but the results will differ.
            """,
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=1,
        help="""
            Number of vectors per batch (default: 1)
            
            By default, when set to 1 the latency will generate many vectors of size (ndim, )
            and call the functions on pairs of single vectors: both directly, and through `cdist`.
            Alternatively, for larger batch sizes the latency will generate two matrices of 
            size (n, ndim) and compute:
            
            - batch mode: (n) distances between vectors in identical rows of the two matrices,
            - all-pairs mode: (n^2) distances between all pairs of vectors in the two matrices via `cdist`.
            """,
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "all-pairs"],
        default="batch",
        help="""Choose between 'batch' and 'all-pairs' mode (default: batch)
        
        In 'batch' mode, the latency will generate two matrices of size (n, ndim) 
        and compute (n) distances between vectors in identical rows of the two matrices.
        In 'all-pairs' mode, the latency will generate two matrices of size (n, ndim)
        and compute (n^2) distances between all pairs of vectors in the two matrices via `cdist`.
        """,
    )
    parser.add_argument(
        "--metric",
        choices=["all", *metric_families],
        default="all",
        help="Distance metric to use, profiles everything by default",
    )
    parser.add_argument(
        "--dtype",
        choices=["all", *dtype_names],
        default="all",
        help="Defines numeric types to latency, profiles everything by default",
    )
    parser.add_argument("--scipy", action="store_true", help="Profile SciPy, must be installed")
    parser.add_argument("--scikit", action="store_true", help="Profile scikit-learn, must be installed")
    parser.add_argument("--torch", action="store_true", help="Profile PyTorch, must be installed")
    parser.add_argument("--tf", action="store_true", help="Profile TensorFlow, must be installed")
    parser.add_argument("--jax", action="store_true", help="Profile JAX, must be installed")
    parser.add_argument(
        "--time-limit",
        type=float,
        default=1.0,
        help="Maximum time in seconds to run each latency (default: 1.0)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="""
        Number of warm-up runs before timing (default: 0)
        
        This will greatly affect the results for all heavy libraries relying on JIT compilation
        or lazy computational graphs (e.g., TensorFlow, PyTorch, JAX).
        """,
    )
    args = parser.parse_args()
    assert args.count > 0, "Number of vectors per batch must be greater than 0"
    assert args.ndim > 0, "Number of dimensions must be greater than 0"

    count = args.count
    ndim = args.ndim
    dtypes_profiled = set([args.dtype] if args.dtype != "all" else dtype_names)
    metric_families_profiled = set([args.metric] if args.metric != "all" else metric_families)

    print("# Benchmarking SimSIMD")
    print("- Vector dimensions:", ndim)
    print("- Vectors count:", count)
    print("- Metrics:", ", ".join(metric_families_profiled))
    print("- Datatypes:", ", ".join(dtypes_profiled))
    try:
        caps = [cap for cap, enabled in simd.get_capabilities().items() if enabled]
        print("- Hardware capabilities:", ", ".join(caps))

        # Log versions of SimSIMD, NumPy, SciPy, and scikit-learn
        print(f"- SimSIMD version: {simd.__version__}")
        print(f"- NumPy version: {np.__version__}")

        if args.scipy:
            import scipy as sp

            print(f"- SciPy version: {sp.__version__}")
        if args.scikit:
            import sklearn as sk

            print(f"- scikit-learn version: {sk.__version__}")
        if args.torch:
            import torch

            print(f"- PyTorch version: {torch.__version__}")
        if args.tf:
            import tensorflow as tf

            print(f"- TensorFlow version: {tf.__version__}")
        if args.jax:
            import jax

            print(f"- JAX version: {jax.__version__}")

        deps: dict = np.show_config(mode="dicts").get("Build Dependencies")
        print("-- NumPy BLAS dependency:", deps["blas"]["name"])
        print("-- NumPy LAPACK dependency:", deps["lapack"]["name"])
    except Exception as e:
        print(f"An error occurred: {e}")

    kernels: List[Kernel] = list(
        yield_kernels(
            metric_families_profiled,
            dtypes_profiled,
            include_scipy=args.scipy,
            include_scikit=args.scikit,
            include_torch=args.torch,
            include_tf=args.tf,
            include_jax=args.jax,
        )
    )

    results: Generator[Result, None, None] = []
    if args.mode == "batch":
        print("## Between Vectors in Two Matrices, Batch Size: {:,}".format(count))
        results = yield_batch_results(count, ndim, kernels)
    else:
        print("## Between All Pairs of Vectors (`cdist`), Batch Size: {:,}".format(count))
        results = yield_all_pairs_results(count, ndim, kernels)

    columns_headers = ["Data Type", "Method", "Baseline", "SimSIMD", "Improvement"]
    results_rows = []
    for result in results:
        result_row = result_to_row(result)
        results_rows.append(result_row)

    print(tabulate.tabulate(results_rows, headers=columns_headers))


if __name__ == "__main__":
    main()
