#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module: bench_vectors_live.py

This script visualizes the performance difference between SimSIMD and default
numerics libraries like NumPy and SimSIMD for the most common kernels.

One plot contains information about one distance function, but many data types
and libraries. For each benchmark, the baseline is the NumPy/SciPy distance function
applied to `float64` input data. The "x" axis represents the growth in the number of
dimensions of the input vectors, while the "y" axis represents the speedup factor
of every kernel against the baseline.
"""
import os
import argparse
from typing import List

import numpy as np

import perfplot

from bench_vectors import (
    metric_families,
    dtype_names,
    Kernel,
    yield_kernels,
    random_matrix,
)


def ndim_argument(value):
    if value == "default":
        return [2**k for k in range(16)]
    try:
        # Split the input string by commas and convert each part to an integer
        return [int(x) for x in value.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError("Value must be 'default' or a comma-separated list of integers")


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Visualize Performance Difference between SimSIMD and other libraries")
    parser.add_argument(
        "--ndim-min",
        type=int,
        default=2,
        help="",
    )
    parser.add_argument(
        "--ndim-max",
        type=int,
        default=1024 * 1024,
        help="",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        type=str,
        default=None,
        help="File to save the plot to (default: None, plot is shown live)",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=1,
        help="""Number of vectors per batch (default: 1)
            
        By default, when set to 1 the benchmark will generate many vectors of size (ndim, )
        and call the functions on pairs of single vectors: both directly, and through `cdist`.
        Alternatively, for larger batch sizes the benchmark will generate two matrices of 
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
        choices=metric_families,
        default="dot",
        help=f"Distance metric to use, profiles `dot` by default",
    )
    parser.add_argument(
        "--dtype",
        choices=dtype_names,
        default="float64",
        help=f"Defines numeric types to latency, profiles `float64` by default",
    )
    parser.add_argument("--scipy", action="store_true", help="Profile SciPy, must be installed")
    parser.add_argument("--scikit", action="store_true", help="Profile scikit-learn, must be installed")
    parser.add_argument("--torch", action="store_true", help="Profile PyTorch, must be installed")
    parser.add_argument("--tf", action="store_true", help="Profile TensorFlow, must be installed")
    parser.add_argument("--jax", action="store_true", help="Profile JAX, must be installed")

    args = parser.parse_args()
    assert args.count > 0, "Number of vectors per batch must be greater than 0"
    assert args.ndim_min > 0, "Number of dimensions must be greater than 0"
    assert args.ndim_max > 0, "Number of dimensions must be greater than 0"

    ndim_range = [args.ndim_min]
    while ndim_range[-1] <= args.ndim_max:
        ndim_range.append(ndim_range[-1] * 2)

    kernels: List[Kernel] = list(
        yield_kernels(
            [args.metric],
            dtype_names,
            include_scipy=args.scipy,
            include_scikit=args.scikit,
            include_torch=args.torch,
            include_tf=args.tf,
            include_jax=args.jax,
        )
    )
    if len(kernels) == 0:
        raise RuntimeError("No kernels found!")

    def precomputed_flops(ndim: int) -> int:
        if args.mode == "all-pairs":
            return ndim * (args.count**2)
        else:
            return ndim * (args.count)

    def generate_matrix(ndim: int) -> np.ndarray:
        if args.count == 1:
            return random_matrix(1, ndim, dtype=args.dtype).flatten()
        else:
            return random_matrix(args.count, ndim, dtype=args.dtype)

    def wrap_binary_function(function):
        def wrapped(A):
            return function(A, A)

        return wrapped

    kernel_labels: List[str] = []
    kernel_callables: List[callable] = []

    # Add SimSIMD kernels
    for kernel in kernels:
        if not kernel.name.startswith("numpy.") and not kernel.name.startswith("scipy."):
            continue
        _, _, function_name = kernel.name.partition(".")
        kernel_labels.append(f"simsimd.{function_name}<{kernel.dtype}>")
        if args.mode == "all-pairs":
            kernel_callables.append(wrap_binary_function(kernel.simsimd_all_pairs_func))
        else:
            kernel_callables.append(wrap_binary_function(kernel.simsimd_func))

    # Add other kernels
    for kernel in kernels:
        kernel_labels.append(f"{kernel.name}<{kernel.dtype}>")
        if args.mode == "all-pairs":
            kernel_callables.append(wrap_binary_function(kernel.baseline_all_pairs_func))
        elif args.count == 1:
            kernel_callables.append(wrap_binary_function(kernel.baseline_one_to_one_func))
        else:
            kernel_callables.append(wrap_binary_function(kernel.baseline_many_to_many_func))

    # Filter-out kernels that raise any exceptions
    safe_callables = []
    safe_labels = []
    for kernel_label, kernel_callable in zip(kernel_labels, kernel_callables):
        try:
            kernel_callable(generate_matrix(ndim_range[0]))
            safe_callables.append(kernel_callable)
            safe_labels.append(kernel_label)
        except:
            print(f"Skipping {kernel_label}")

    print(safe_callables)
    print(safe_labels)

    # Settings are mostly the same for live charts and exported ones
    profiler_settings = dict(
        setup=generate_matrix,
        kernels=safe_callables,
        labels=safe_labels,
        n_range=ndim_range,
        flops=precomputed_flops,
        xlabel="ndim",
        equality_check=None,  # bypass correctness check, we have tests for that
    )

    # Plot the results
    if args.output_path is not None:
        plot_fp = os.path.abspath(args.output_path)
        profiler_settings["filename"] = plot_fp
        profiler_settings["show_progress"] = False
        results = perfplot.bench(**profiler_settings)
        results.save(plot_fp, transparent=False, bbox_inches="tight", relative_to=0, logy="auto")
    else:
        profiler_settings.pop("flops")
        perfplot.live(**profiler_settings)


if __name__ == "__main__":
    main()
