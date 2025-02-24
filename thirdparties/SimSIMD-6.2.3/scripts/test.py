#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module: test.py

This module contains a suite of tests for the `simsimd` package.
It compares various SIMD kernels (like Dot-products, squared Euclidean, and Cosine distances) 
with their NumPy or baseline counterparts, testing accuracy for different data types including 
floating-point, integer, and complex numbers.

The tests cover:

- **Dense Vector Operations**: Tests for `float64`, `float32`, `float16` data types using metrics like `inner`, `sqeuclidean`, and `cosine`.
- **Brain Floating-Point Format (bfloat16)**: Tests for operations with the brain floating-point format not natively supported by NumPy.
- **Integer Operations**: Tests for `int8` data type, ensuring accuracy without overflow.
- **Bitwise Operations**: Tests for Hamming and Jaccard distances using bit arrays.
- **Complex Numbers**: Tests for complex dot products and vector dot products.
- **Batch Operations and Cross-Distance Computations**: Tests for batch processing and cross-distance computations using `cdist`.
- **Hardware Capabilities Verification**: Checks the availability of hardware capabilities and function pointers.

**Dependencies**:

- Python 3.x
- `numpy`
- `scipy`
- `pytest`
- `tabulate`
- `simsimd` package

**Usage**:

Run the tests using pytest:

    pytest test.py

Or run the script directly:

    python test.py

"""
import os
import math
import time
import platform
import collections
from typing import Dict, List

import tabulate
import pytest
import simsimd as simd


# NumPy is available on most platforms and is required for most tests.
# When using PyPy on some platforms NumPy has internal issues, that will
# raise a weird error, not an `ImportError`. That's why we intentionally
# use a naked `except:`. Necessary evil!
try:
    import numpy as np

    numpy_available = True

    baseline_inner = np.inner
    baseline_intersect = lambda x, y: len(np.intersect1d(x, y))
    baseline_bilinear = lambda x, y, z: x @ z @ y

    def baseline_fma(x, y, z, alpha, beta):
        xy_scaled = np.multiply((alpha * x), y)
        z_scaled = beta * z
        r = xy_scaled + z_scaled
        if np.issubdtype(x.dtype, np.integer):
            r = np.round(r)
        #! We need non-overflowing saturating addition for small integers, that NumPy lacks:
        #! https://stackoverflow.com/questions/29611185/avoid-overflow-when-adding-numpy-arrays
        if x.dtype == np.uint8:
            r = np.clip(r, 0, 255, out=r)
        elif x.dtype == np.int8:
            r = np.clip(r, -128, 127, out=r)
        return r.astype(x.dtype)

    def baseline_wsum(x, y, alpha, beta):
        x_scaled = alpha * x
        y_scaled = beta * y
        r = x_scaled + y_scaled
        if np.issubdtype(x.dtype, np.integer):
            r = np.round(r)
        #! We need non-overflowing saturating addition for small integers, that NumPy lacks:
        #! https://stackoverflow.com/questions/29611185/avoid-overflow-when-adding-numpy-arrays
        if x.dtype == np.uint8:
            r = np.clip(r, 0, 255, out=r)
        elif x.dtype == np.int8:
            r = np.clip(r, -128, 127, out=r)
        return r.astype(x.dtype)

except:
    # NumPy is not installed, most tests will be skipped
    numpy_available = False

    baseline_inner = lambda x, y: sum(x[i] * y[i] for i in range(len(x)))
    baseline_intersect = lambda x, y: len(set(x).intersection(y))

    def baseline_bilinear(x, y, z):
        result = 0
        for i in range(len(x)):
            for j in range(len(y)):
                result += x[i] * z[i][j] * y[j]
        return result

    def baseline_fma(x, y, z, alpha, beta):
        return [(alpha * xi) * yi + beta * zi for xi, yi, zi in zip(x, y, z)]

    def baseline_wsum(x, y, alpha, beta):
        return [(alpha * xi) + beta * yi for xi, yi in zip(x, y)]


# At the time of Python 3.12, SciPy doesn't support 32-bit Windows on any CPU,
# or 64-bit Windows on Arm. It also doesn't support `musllinux` distributions,
# like CentOS, RedHat OS, and many others.
try:
    import scipy.spatial.distance as spd

    scipy_available = True

    baseline_euclidean = lambda x, y: np.array(spd.euclidean(x, y))  #! SciPy returns a scalar
    baseline_sqeuclidean = spd.sqeuclidean
    baseline_cosine = spd.cosine
    baseline_jensenshannon = lambda x, y: spd.jensenshannon(x, y)
    baseline_hamming = lambda x, y: spd.hamming(x, y) * len(x)
    baseline_jaccard = spd.jaccard

    def baseline_mahalanobis(x, y, z):
        # If there was an error, or the value is NaN, we skip the test.
        try:
            result = spd.mahalanobis(x, y, z).astype(np.float64)
            if not np.isnan(result):
                return result
        except:
            pass
        pytest.skip(f"SciPy Mahalanobis distance returned {result} due to `sqrt` of a negative number")

except:
    # SciPy is not installed, some tests will be skipped
    scipy_available = False

    baseline_cosine = lambda x, y: 1.0 - np.dot(x, y) / (np.linalg.norm(x) * np.linalg.norm(y))
    baseline_euclidean = lambda x, y: np.array([np.sqrt(np.sum((x - y) ** 2))])
    baseline_sqeuclidean = lambda x, y: np.sum((x - y) ** 2)
    baseline_jensenshannon = lambda p, q: (np.sum((np.sqrt(p) - np.sqrt(q)) ** 2)) / 2
    baseline_hamming = lambda x, y: np.logical_xor(x, y).sum()

    def baseline_mahalanobis(x, y, z):
        diff = x - y
        return np.sqrt(diff @ z @ diff)

    def baseline_jaccard(x, y):
        intersection = np.logical_and(x, y).sum()
        union = np.logical_or(x, y).sum()
        return 0.0 if union == 0 else 1.0 - float(intersection) / float(union)

    def baseline_intersect(arr1, arr2):
        i, j, intersection = 0, 0, 0
        while i < len(arr1) and j < len(arr2):
            if arr1[i] == arr2[j]:
                intersection += 1
                i += 1
                j += 1
            elif arr1[i] < arr2[j]:
                i += 1
            else:
                j += 1
        return intersection


def is_running_under_qemu():
    return "SIMSIMD_IN_QEMU" in os.environ


def profile(callable, *args, **kwargs) -> tuple:
    before = time.perf_counter_ns()
    result = callable(*args, **kwargs)
    after = time.perf_counter_ns()
    return after - before, result


@pytest.fixture(scope="session")
def stats_fixture():
    """Session-scoped fixture that collects errors during tests."""
    results = dict()
    results["metric"] = []
    results["ndim"] = []
    results["dtype"] = []
    results["absolute_baseline_error"] = []
    results["relative_baseline_error"] = []
    results["absolute_simsimd_error"] = []
    results["relative_simsimd_error"] = []
    results["accurate_duration"] = []
    results["baseline_duration"] = []
    results["simsimd_duration"] = []
    results["warnings"] = []
    yield results

    # Group the errors by (metric, ndim, dtype) to calculate the mean and std error.
    grouped_errors = collections.defaultdict(
        lambda: {
            "absolute_baseline_error": [],
            "relative_baseline_error": [],
            "absolute_simsimd_error": [],
            "relative_simsimd_error": [],
            "accurate_duration": [],
            "baseline_duration": [],
            "simsimd_duration": [],
        }
    )
    for (
        metric,
        ndim,
        dtype,
        absolute_baseline_error,
        relative_baseline_error,
        absolute_simsimd_error,
        relative_simsimd_error,
        accurate_duration,
        baseline_duration,
        simsimd_duration,
    ) in zip(
        results["metric"],
        results["ndim"],
        results["dtype"],
        results["absolute_baseline_error"],
        results["relative_baseline_error"],
        results["absolute_simsimd_error"],
        results["relative_simsimd_error"],
        results["accurate_duration"],
        results["baseline_duration"],
        results["simsimd_duration"],
    ):
        key = (metric, ndim, dtype)
        grouped_errors[key]["absolute_baseline_error"].append(absolute_baseline_error)
        grouped_errors[key]["relative_baseline_error"].append(relative_baseline_error)
        grouped_errors[key]["absolute_simsimd_error"].append(absolute_simsimd_error)
        grouped_errors[key]["relative_simsimd_error"].append(relative_simsimd_error)
        grouped_errors[key]["accurate_duration"].append(accurate_duration)
        grouped_errors[key]["baseline_duration"].append(baseline_duration)
        grouped_errors[key]["simsimd_duration"].append(simsimd_duration)

    # Compute mean and the standard deviation for each task error
    final_results = []
    for key, errors in grouped_errors.items():
        n = len(errors["simsimd_duration"])

        # Mean and the standard deviation for errors
        baseline_errors = errors["relative_baseline_error"]
        simsimd_errors = errors["relative_simsimd_error"]
        #! On some platforms (like `cp312-musllinux_aarch64`) without casting via `float(x)`
        #! the subsequent `:.2e` string formatting code will fail due to:
        #! `TypeError: unsupported format string passed to numpy.ndarray.__format__`.
        baseline_mean = float(sum(baseline_errors)) / n
        simsimd_mean = float(sum(simsimd_errors)) / n
        baseline_std = math.sqrt(sum((x - baseline_mean) ** 2 for x in baseline_errors) / n)
        simsimd_std = math.sqrt(sum((x - simsimd_mean) ** 2 for x in simsimd_errors) / n)
        baseline_error_formatted = f"{baseline_mean:.2e} ± {baseline_std:.2e}"
        simsimd_error_formatted = f"{simsimd_mean:.2e} ± {simsimd_std:.2e}"

        # Log durations
        accurate_durations = errors["accurate_duration"]
        baseline_durations = errors["baseline_duration"]
        simsimd_durations = errors["simsimd_duration"]
        accurate_mean_duration = sum(accurate_durations) / n
        baseline_mean_duration = sum(baseline_durations) / n
        simsimd_mean_duration = sum(simsimd_durations) / n
        accurate_std_duration = math.sqrt(sum((x - accurate_mean_duration) ** 2 for x in accurate_durations) / n)
        baseline_std_duration = math.sqrt(sum((x - baseline_mean_duration) ** 2 for x in baseline_durations) / n)
        simsimd_std_duration = math.sqrt(sum((x - simsimd_mean_duration) ** 2 for x in simsimd_durations) / n)
        accurate_duration = f"{accurate_mean_duration:.2e} ± {accurate_std_duration:.2e}"
        baseline_duration = f"{baseline_mean_duration:.2e} ± {baseline_std_duration:.2e}"
        simsimd_duration = f"{simsimd_mean_duration:.2e} ± {simsimd_std_duration:.2e}"

        # Measure time improvement
        improvements = [baseline / simsimd for baseline, simsimd in zip(baseline_durations, simsimd_durations)]
        improvements_mean = sum(improvements) / n
        improvements_std = math.sqrt(sum((x - improvements_mean) ** 2 for x in improvements) / n)
        simsimd_speedup = f"{improvements_mean:.2f}x ± {improvements_std:.2f}x"

        # Calculate Improvement
        # improvement = abs(baseline_mean - simsimd_mean) / min(simsimd_mean, baseline_mean)
        # if baseline_mean < simsimd_mean:
        #     improvement *= -1
        # improvement_formatted = f"{improvement:+.2}x" if improvement != float("inf") else "N/A"

        final_results.append(
            (
                *key,
                baseline_error_formatted,
                simsimd_error_formatted,
                accurate_duration,
                baseline_duration,
                simsimd_duration,
                simsimd_speedup,
            )
        )

    # Sort results for consistent presentation
    final_results.sort(key=lambda x: (x[0], x[1], x[2]))

    # Output the final table after all tests are completed
    print("\n")
    print("Numerical Error Aggregation Report:")
    headers = [
        "Metric",
        "NDim",
        "DType",
        "Baseline Error",  # Printed as mean ± std deviation
        "SimSIMD Error",  # Printed as mean ± std deviation
        "Accurate Duration",  # Printed as mean ± std deviation
        "Baseline Duration",  # Printed as mean ± std deviation
        "SimSIMD Duration",  # Printed as mean ± std deviation
        "SimSIMD Speedup",
    ]
    print(tabulate.tabulate(final_results, headers=headers, tablefmt="pretty", showindex=True))

    # Show the additional grouped warnings
    warnings = results.get("warnings", [])
    warnings = sorted(warnings)
    warnings = [f"{name}: {message}" for name, message in warnings]
    if len(warnings) != 0:
        print("\nWarnings:")
        unique_warnings, warning_counts = np.unique(warnings, return_counts=True)
        for warning, count in zip(unique_warnings, warning_counts):
            print(f"- {count}x times: {warning}")


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Custom hook to ensure that the error aggregator runs even for failed tests."""
    if call.when == "call":
        item.test_result = call.excinfo is None


def collect_errors(
    metric: str,
    ndim: int,
    dtype: str,
    accurate_result: float,
    accurate_duration: float,
    baseline_result: float,
    baseline_duration: float,
    simsimd_result: float,
    simsimd_duration: float,
    stats,
):
    """Calculates and aggregates errors for a given test.

    What we want to know in the end of the day is:

    -   How much SimSIMD implementation is more/less accurate than baseline,
        when compared against the accurate result?
    -   TODO: How much faster is SimSIMD than the baseline kernel?
    -   TODO: How much faster is SimSIMD than the accurate kernel?
    """
    eps = np.finfo(accurate_result.dtype).resolution
    absolute_baseline_error = np.max(np.abs(baseline_result - accurate_result))
    relative_baseline_error = np.max(np.abs(baseline_result - accurate_result) / (np.abs(accurate_result) + eps))
    absolute_simsimd_error = np.max(np.abs(simsimd_result - accurate_result))
    relative_simsimd_error = np.max(np.abs(simsimd_result - accurate_result) / (np.abs(accurate_result) + eps))

    stats["metric"].append(metric)
    stats["ndim"].append(ndim)
    stats["dtype"].append(dtype)
    stats["absolute_baseline_error"].append(absolute_baseline_error)
    stats["relative_baseline_error"].append(relative_baseline_error)
    stats["absolute_simsimd_error"].append(absolute_simsimd_error)
    stats["relative_simsimd_error"].append(relative_simsimd_error)
    stats["accurate_duration"].append(accurate_duration)
    stats["baseline_duration"].append(baseline_duration)
    stats["simsimd_duration"].append(simsimd_duration)


def get_current_test():
    """Get's the current test filename, test name, and function name.
    Similar metadata can be obtained from the `request` fixture, but this
    solution uses environment variables."""
    full_name = os.environ.get("PYTEST_CURRENT_TEST").split(" ")[0]
    test_file = full_name.split("::")[0].split("/")[-1].split(".py")[0]
    test_name = full_name.split("::")[1]
    # The `test_name` may look like: "test_dense_i8[cosine-1536-24-50]"
    function_name = test_name.split("[")[0]
    return test_file, test_name, function_name


def collect_warnings(message: str, stats: dict):
    """Collects warnings for the final report."""
    _, _, function_name = get_current_test()
    stats["warnings"].append((function_name, message))


# For normalized distances we use the absolute tolerance, because the result is close to zero.
# For unnormalized ones (like squared Euclidean or Jaccard), we use the relative.
SIMSIMD_RTOL = 0.1
SIMSIMD_ATOL = 0.1

# We will run all the tests many times using different instruction sets under the hood.
available_capabilities: Dict[str, str] = simd.get_capabilities()
possible_x86_capabilities: List[str] = ["haswell", "ice", "skylake", "sapphire", "turin", "genoa", "sierra"]
possible_arm_capabilities: List[str] = [
    "neon",
    "neon_f16",
    "neon_bf16",
    "neon_i8",
    "sve",
    "sve_f16",
    "sve_bf16",
    "sve_i8",
]
possible_x86_capabilities: List[str] = [c for c in possible_x86_capabilities if available_capabilities[c]]
possible_arm_capabilities: List[str] = [c for c in possible_arm_capabilities if available_capabilities[c]]
possible_capabilities: List[str] = (
    possible_x86_capabilities if platform.machine() == "x86_64" else possible_arm_capabilities
)


def keep_one_capability(cap: str):
    assert cap in possible_capabilities
    for c in possible_capabilities:
        if c != cap:
            simd.disable_capability(c)
    simd.enable_capability(c)


def name_to_kernels(name: str):
    """
    Having a separate "helper" function to convert the kernel name is handy for PyTest decorators,
    that can't generally print non-trivial object (like function pointers) well.
    """
    if name == "inner":
        return baseline_inner, simd.inner
    elif name == "euclidean":
        return baseline_euclidean, simd.euclidean
    elif name == "sqeuclidean":
        return baseline_sqeuclidean, simd.sqeuclidean
    elif name == "cosine":
        return baseline_cosine, simd.cosine
    elif name == "bilinear":
        return baseline_bilinear, simd.bilinear
    elif name == "mahalanobis":
        return baseline_mahalanobis, simd.mahalanobis
    elif name == "jaccard":
        return baseline_jaccard, simd.jaccard
    elif name == "hamming":
        return baseline_hamming, simd.hamming
    elif name == "intersect":
        return baseline_intersect, simd.intersect
    elif name == "fma":
        return baseline_fma, simd.fma
    elif name == "wsum":
        return baseline_wsum, simd.wsum
    elif name == "jensenshannon":
        return baseline_jensenshannon, simd.jensenshannon
    else:
        raise ValueError(f"Unknown kernel name: {name}")


def f32_downcast_to_bf16(array):
    """Converts an array of 32-bit floats into 16-bit brain-floats."""
    array = np.asarray(array, dtype=np.float32)
    # NumPy doesn't natively support brain-float, so we need a trick!
    # Luckily, it's very easy to reduce the representation accuracy
    # by simply masking the low 16-bits of our 32-bit single-precision
    # numbers. We can also add `0x8000` to round the numbers.
    array_f32_rounded = ((array.view(np.uint32) + 0x8000) & 0xFFFF0000).view(np.float32)
    # To represent them as brain-floats, we need to drop the second halves.
    array_bf16 = np.right_shift(array_f32_rounded.view(np.uint32), 16).astype(np.uint16)
    return array_f32_rounded, array_bf16


def i8_downcast_to_i4(array):
    """Converts an array of 8-bit integers into 4-bit integers, packing 2 per byte."""
    array = np.asarray(array, dtype=np.int8)
    assert np.all(array >= -8) and np.all(array <= 7), "Input array must be in the range [-8, 7]"


def hex_array(arr):
    """Converts numerical array into a string of comma-separated hexadecimal values for debugging.
    Supports 1D and 2D arrays.
    """
    printer = np.vectorize(hex)
    strings = printer(arr)

    if strings.ndim == 1:
        return ", ".join(strings)
    else:
        return "\n".join(", ".join(row) for row in strings)


def test_pointers_availability():
    """Tests the availability of pre-compiled functions for compatibility with USearch."""
    assert simd.pointer_to_sqeuclidean("float64") != 0
    assert simd.pointer_to_cosine("float64") != 0
    assert simd.pointer_to_inner("float64") != 0

    assert simd.pointer_to_sqeuclidean("float32") != 0
    assert simd.pointer_to_cosine("float32") != 0
    assert simd.pointer_to_inner("float32") != 0

    assert simd.pointer_to_sqeuclidean("float16") != 0
    assert simd.pointer_to_cosine("float16") != 0
    assert simd.pointer_to_inner("float16") != 0

    assert simd.pointer_to_sqeuclidean("int8") != 0
    assert simd.pointer_to_cosine("int8") != 0
    assert simd.pointer_to_inner("int8") != 0

    assert simd.pointer_to_sqeuclidean("uint8") != 0
    assert simd.pointer_to_cosine("uint8") != 0
    assert simd.pointer_to_inner("uint8") != 0


def test_capabilities_list():
    """Tests the visibility of hardware capabilities."""
    assert "serial" in simd.get_capabilities()
    assert "neon" in simd.get_capabilities()
    assert "neon_f16" in simd.get_capabilities()
    assert "neon_bf16" in simd.get_capabilities()
    assert "neon_i8" in simd.get_capabilities()
    assert "sve" in simd.get_capabilities()
    assert "sve_f16" in simd.get_capabilities()
    assert "sve_bf16" in simd.get_capabilities()
    assert "sve_i8" in simd.get_capabilities()
    assert "haswell" in simd.get_capabilities()
    assert "ice" in simd.get_capabilities()
    assert "skylake" in simd.get_capabilities()
    assert "genoa" in simd.get_capabilities()
    assert "sapphire" in simd.get_capabilities()
    assert "turin" in simd.get_capabilities()
    assert simd.get_capabilities().get("serial") == 1

    # Check the toggle:
    previous_value = simd.get_capabilities().get("neon")
    simd.enable_capability("neon")
    assert simd.get_capabilities().get("neon") == 1
    if not previous_value:
        simd.disable_capability("neon")


def to_array(x, dtype=None):
    if numpy_available:
        y = np.array(x)
        if dtype is not None:
            y = y.astype(dtype)
        return y


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.parametrize(
    "function, expected_error, args, kwargs",
    [
        # Test missing positional arguments
        (simd.sqeuclidean, TypeError, (), {}),  # No arguments provided
        (simd.sqeuclidean, TypeError, (to_array([1.0]),), {}),  # Only one positional argument
        # Try missing type name
        (simd.sqeuclidean, ValueError, (to_array([1.0]), to_array([1.0]), "missing_dtype"), {}),
        # Test incorrect argument type
        (simd.sqeuclidean, TypeError, (to_array([1.0]), "invalid"), {}),  # Wrong type for second argument
        # Test invalid keyword argument name
        (simd.sqeuclidean, TypeError, (to_array([1.0]), to_array([1.0])), {"invalid_kwarg": "value"}),
        # Test wrong argument type for SIMD capability toggle
        (simd.enable_capability, TypeError, (123,), {}),  # Should expect a string
        (simd.disable_capability, TypeError, ([],), {}),  # Should expect a string
        # Test missing required argument for Mahalanobis
        (simd.mahalanobis, TypeError, (to_array([1.0]), to_array([1.0])), {}),  # Missing covariance matrix
        # Test missing required arguments for bilinear
        (simd.bilinear, TypeError, (to_array([1.0]),), {}),  # Missing second vector and metric tensor
        # Test passing too many arguments to a method
        (simd.cosine, TypeError, (to_array([1.0]), to_array([1.0]), to_array([1.0])), {}),  # Too many arguments
        (simd.cdist, TypeError, (to_array([[1.0]]), to_array([[1.0]]), "cos", "dos"), {}),  # Too many arguments
        # Same argument as both positional and keyword
        (simd.cdist, TypeError, (to_array([[1.0]]), to_array([[1.0]]), "cos"), {"metric": "cos"}),
        # Applying real metric to complex numbers - missing kernel
        (simd.cosine, LookupError, (to_array([1 + 2j]), to_array([1 + 2j])), {}),
        # Test incompatible vectors for cosine
        (simd.cosine, ValueError, (to_array([1.0]), to_array([1.0, 2.0])), {}),  # Different number of dimensions
        (simd.cosine, TypeError, (to_array([1.0]), to_array([1], "uint32")), {}),  # Floats and integers
        (simd.cosine, TypeError, (to_array([1]), to_array([1], "float16")), {}),  # Different floats
    ],
)
def test_invalid_argument_handling(function, expected_error, args, kwargs):
    """Test that functions raise TypeError when called with invalid arguments."""
    with pytest.raises(expected_error):
        function(*args, **kwargs)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float64", "float32", "float16"])
@pytest.mark.parametrize("metric", ["inner", "euclidean", "sqeuclidean", "cosine"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_dense(ndim, dtype, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Dot-products, squared Euclidean, and Cosine distances)
    with their NumPy or baseline counterparts, testing accuracy for IEEE standard floating-point types."""

    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()
    a = np.random.randn(ndim).astype(dtype)
    b = np.random.randn(ndim).astype(dtype)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)

    accurate_dt, accurate = profile(baseline_kernel, a.astype(np.float64), b.astype(np.float64))
    expected_dt, expected = profile(baseline_kernel, a, b)
    result_dt, result = profile(simd_kernel, a, b)
    result = np.array(result)

    np.testing.assert_allclose(result, expected.astype(np.float64), atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(metric, ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97])
@pytest.mark.parametrize(
    "dtypes",  # representation datatype and compute precision
    [
        ("float64", "float64"),
        ("float32", "float32"),
        ("float16", "float32"),  # otherwise NumPy keeps aggregating too much error
    ],
)
@pytest.mark.parametrize("metric", ["bilinear", "mahalanobis"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_curved(ndim, dtypes, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Bilinear Forms and Mahalanobis distances) for curved spaces
    with their NumPy or baseline counterparts, testing accuracy for IEEE standard floating-point types."""

    dtype, compute_dtype = dtypes
    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()

    # Let's generate some non-negative probability distributions
    a = np.abs(np.random.randn(ndim).astype(dtype))
    b = np.abs(np.random.randn(ndim).astype(dtype))
    a /= np.sum(a)
    b /= np.sum(b)

    # Let's compute the inverse of the covariance matrix, otherwise in the SciPy
    # implementation of the Mahalanobis we may face `sqrt` of a negative number.
    # We multiply the matrix by its transpose to get a positive-semi-definite matrix.
    c = np.abs(np.random.randn(ndim, ndim).astype(dtype))
    c = np.dot(c, c.T)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    accurate_dt, accurate = profile(
        baseline_kernel,
        a.astype(np.float64),
        b.astype(np.float64),
        c.astype(np.float64),
    )
    expected_dt, expected = profile(
        baseline_kernel,
        a.astype(compute_dtype),
        b.astype(compute_dtype),
        c.astype(compute_dtype),
    )
    result_dt, result = profile(simd_kernel, a, b, c)
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(metric, ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)


@pytest.mark.skipif(is_running_under_qemu(), reason="Complex math in QEMU fails")
@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97])
@pytest.mark.parametrize("dtype", ["complex128", "complex64"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_curved_complex(ndim, dtype, capability, stats_fixture):
    """Compares various SIMD kernels (like Bilinear Forms and Mahalanobis distances) for curved spaces
    with their NumPy or baseline counterparts, testing accuracy for complex IEEE standard floating-point types."""

    # Let's generate some uniform complex numbers
    np.random.seed()
    a = (np.random.randn(ndim) + 1.0j * np.random.randn(ndim)).astype(dtype)
    b = (np.random.randn(ndim) + 1.0j * np.random.randn(ndim)).astype(dtype)
    c = (np.random.randn(ndim, ndim) + 1.0j * np.random.randn(ndim, ndim)).astype(dtype)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels("bilinear")
    accurate_dt, accurate = profile(
        baseline_kernel,
        a.astype(np.complex128),
        b.astype(np.complex128),
        c.astype(np.complex128),
    )
    expected_dt, expected = profile(baseline_kernel, a, b, c)
    result_dt, result = profile(simd_kernel, a, b, c)
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(
        "bilinear", ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("metric", ["inner", "euclidean", "sqeuclidean", "cosine"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_dense_bf16(ndim, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Dot-products, squared Euclidean, and Cosine distances)
    with their NumPy or baseline counterparts, testing accuracy for the Brain-float format not
    natively supported by NumPy."""
    np.random.seed()
    a = np.random.randn(ndim).astype(np.float32)
    b = np.random.randn(ndim).astype(np.float32)

    a_f32_rounded, a_bf16 = f32_downcast_to_bf16(a)
    b_f32_rounded, b_bf16 = f32_downcast_to_bf16(b)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    accurate_dt, accurate = profile(baseline_kernel, a_f32_rounded.astype(np.float64), b_f32_rounded.astype(np.float64))
    expected_dt, expected = profile(baseline_kernel, a_f32_rounded, b_f32_rounded)
    result_dt, result = profile(simd_kernel, a_bf16, b_bf16, "bf16")
    result = np.array(result)

    np.testing.assert_allclose(
        result,
        expected,
        atol=SIMSIMD_ATOL,
        rtol=SIMSIMD_RTOL,
        err_msg=f"""
        First `f32` operand in hex:     {hex_array(a_f32_rounded.view(np.uint32))}
        Second `f32` operand in hex:    {hex_array(b_f32_rounded.view(np.uint32))}
        First `bf16` operand in hex:    {hex_array(a_bf16)}
        Second `bf16` operand in hex:   {hex_array(b_bf16)}
        """,
    )
    collect_errors(
        metric, ndim, "bfloat16", accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 16, 33])
@pytest.mark.parametrize("metric", ["bilinear", "mahalanobis"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_curved_bf16(ndim, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Bilinear Forms and Mahalanobis distances) for curved spaces
    with their NumPy or baseline counterparts, testing accuracy for the Brain-float format not
    natively supported by NumPy."""

    np.random.seed()

    # Let's generate some non-negative probability distributions
    a = np.abs(np.random.randn(ndim).astype(np.float32))
    b = np.abs(np.random.randn(ndim).astype(np.float32))
    a /= np.sum(a)
    b /= np.sum(b)

    # Let's compute the inverse of the covariance matrix, otherwise in the SciPy
    # implementation of the Mahalanobis we may face `sqrt` of a negative number.
    # We multiply the matrix by its transpose to get a positive-semi-definite matrix.
    c = np.abs(np.random.randn(ndim, ndim).astype(np.float32))
    c = np.dot(c, c.T)

    a_f32_rounded, a_bf16 = f32_downcast_to_bf16(a)
    b_f32_rounded, b_bf16 = f32_downcast_to_bf16(b)
    c_f32_rounded, c_bf16 = f32_downcast_to_bf16(c)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    accurate_dt, accurate = profile(
        baseline_kernel,
        a_f32_rounded.astype(np.float64),
        b_f32_rounded.astype(np.float64),
        c_f32_rounded.astype(np.float64),
    )
    expected_dt, expected = profile(baseline_kernel, a_f32_rounded, b_f32_rounded, c_f32_rounded)
    result_dt, result = profile(simd_kernel, a_bf16, b_bf16, c_bf16, "bf16")
    result = np.array(result)

    np.testing.assert_allclose(
        result,
        expected,
        atol=SIMSIMD_ATOL,
        rtol=SIMSIMD_RTOL,
        err_msg=f"""
        First `f32` operand in hex:     {hex_array(a_f32_rounded.view(np.uint32))}
        Second `f32` operand in hex:    {hex_array(b_f32_rounded.view(np.uint32))}
        First `bf16` operand in hex:    {hex_array(a_bf16)}
        Second `bf16` operand in hex:   {hex_array(b_bf16)}
        Matrix `bf16` operand in hex:    {hex_array(c_bf16)}
        Matrix `bf16` operand in hex:   {hex_array(c_bf16)}
        """,
    )
    collect_errors(
        metric, ndim, "bfloat16", accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["int8", "uint8"])
@pytest.mark.parametrize("metric", ["inner", "euclidean", "sqeuclidean", "cosine"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_dense_i8(ndim, dtype, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Dot-products, squared Euclidean, and Cosine distances)
    with their NumPy or baseline counterparts, testing accuracy for small integer types, that can't
    be directly processed with other tools without overflowing."""

    np.random.seed()
    if dtype == "int8":
        a = np.random.randint(-128, 127, size=(ndim), dtype=np.int8)
        b = np.random.randint(-128, 127, size=(ndim), dtype=np.int8)
    else:
        a = np.random.randint(0, 255, size=(ndim), dtype=np.uint8)
        b = np.random.randint(0, 255, size=(ndim), dtype=np.uint8)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)

    accurate_dt, accurate = profile(baseline_kernel, a.astype(np.float64), b.astype(np.float64))
    expected_dt, expected = profile(baseline_kernel, a.astype(np.int64), b.astype(np.int64))
    result_dt, result = profile(simd_kernel, a, b)
    result = np.array(result)

    if metric == "inner":
        assert round(float(result)) == round(float(expected)), f"Expected {expected}, but got {result}"
    else:
        np.testing.assert_allclose(
            result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL
        ), f"Expected {expected}, but got {result}"
    collect_errors(metric, ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)

    #! Fun fact: SciPy doesn't actually raise an `OverflowError` when overflow happens
    #! here, instead it raises `ValueError: math domain error` during the `sqrt` operation.
    try:
        expected_overflow = baseline_kernel(a, b)
        if np.isinf(expected_overflow):
            collect_warnings("Couldn't avoid overflow in SciPy", stats_fixture)
    except Exception as e:
        collect_warnings(f"Arbitrary error raised in SciPy: {e}", stats_fixture)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("metric", ["jaccard", "hamming"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_dense_bits(ndim, metric, capability, stats_fixture):
    """Compares various SIMD kernels (like Hamming and Jaccard/Tanimoto distances) for dense bit arrays
    with their NumPy or baseline counterparts, even though, they can't process sub-byte-sized scalars."""
    np.random.seed()
    a = np.random.randint(2, size=ndim).astype(np.uint8)
    b = np.random.randint(2, size=ndim).astype(np.uint8)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    accurate_dt, accurate = profile(baseline_kernel, a.astype(np.uint64), b.astype(np.uint64))
    expected_dt, expected = profile(baseline_kernel, a, b)
    result_dt, result = profile(simd_kernel, np.packbits(a), np.packbits(b), "bin8")
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(metric, ndim, "bin8", accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)

    # Aside from overriding the `dtype` parameter, we can also view as booleans
    result_dt, result = profile(simd_kernel, np.packbits(a).view(np.bool_), np.packbits(b).view(np.bool_))
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(metric, ndim, "bin8", accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float32", "float16"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_jensen_shannon(ndim, dtype, capability, stats_fixture):
    """Compares the simd.jensenshannon() function with scipy.spatial.distance.jensenshannon(), measuring the accuracy error for f16, and f32 types."""

    np.random.seed()
    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    a = np.abs(np.random.randn(ndim)).astype(dtype)
    b = np.abs(np.random.randn(ndim)).astype(dtype)
    a /= np.sum(a)
    b /= np.sum(b)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels("jensenshannon")
    accurate_dt, accurate = profile(baseline_kernel, a.astype(np.float64), b.astype(np.float64))
    expected_dt, expected = profile(baseline_kernel, a, b)
    result_dt, result = profile(simd_kernel, a, b)
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors(
        "jensenshannon", ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float32", "float16"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_cosine_zero_vector(ndim, dtype, capability):
    """Tests the simd.cosine() function with zero vectors, to catch division by zero errors."""
    a = np.zeros(ndim, dtype=dtype)
    b = (np.random.randn(ndim) + 1).astype(dtype)
    keep_one_capability(capability)

    result = simd.cosine(a, b)
    assert result == 1, f"Expected 1, but got {result}"

    result = simd.cosine(a, a)
    assert result == 0, f"Expected 0 distance from itself, but got {result}"

    result = simd.cosine(b, b)
    assert abs(result) < SIMSIMD_ATOL, f"Expected 0 distance from itself, but got {result}"

    # For the cosine, the output must not be negative!
    assert np.all(result >= 0), f"Negative result for cosine distance"


@pytest.mark.skip(reason="Lacks overflow protection: https://github.com/ashvardanian/SimSIMD/issues/206")  # TODO
@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float64", "float32", "float16"])
@pytest.mark.parametrize("metric", ["inner", "euclidean", "sqeuclidean", "cosine"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_overflow(ndim, dtype, metric, capability):
    """Tests if the floating-point kernels are capable of detecting overflow yield the same ±inf result."""

    np.random.seed()
    a = np.random.randn(ndim)
    b = np.random.randn(ndim)

    # Replace scalar at random position with infinity
    a[np.random.randint(ndim)] = np.inf
    a = a.astype(dtype)
    b = b.astype(dtype)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    result = simd_kernel(a, b)
    assert np.isinf(result), f"Expected ±inf, but got {result}"

    #! In the Euclidean (L2) distance, SciPy raises a `ValueError` from the underlying
    #! NumPy function: `ValueError: array must not contain infs or NaNs`.
    try:
        expected_overflow = baseline_kernel(a, b)
        if not np.isinf(expected_overflow):
            collect_warnings("Overflow not detected in SciPy", stats_fixture)
    except Exception as e:
        collect_warnings(f"Arbitrary error raised in SciPy: {e}", stats_fixture)


@pytest.mark.skip(reason="Lacks overflow protection: https://github.com/ashvardanian/SimSIMD/issues/206")  # TODO
@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [131072, 262144])
@pytest.mark.parametrize("metric", ["inner", "euclidean", "sqeuclidean", "cosine"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_overflow_i8(ndim, metric, capability):
    """Tests if the integral kernels are capable of detecting overflow yield the same ±inf result,
    as with 2^16 elements accumulating "u32(u16(u8)*u16(u8))+u32" products should overflow and the
    same is true for 2^17 elements with "i32(i15(i8))*i32(i15(i8))" products.
    """

    np.random.seed()
    a = np.full(ndim, fill_value=-128, dtype=np.int8)
    b = np.full(ndim, fill_value=-128, dtype=np.int8)

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(metric)
    expected = baseline_kernel(a, b)
    result = simd_kernel(a, b)
    assert np.isinf(result), f"Expected ±inf, but got {result}"

    try:
        expected_overflow = baseline_kernel(a, b)
        if not np.isinf(expected_overflow):
            collect_warnings("Overflow not detected in SciPy", stats_fixture)
    except Exception as e:
        collect_warnings(f"Arbitrary error raised in SciPy: {e}", stats_fixture)


@pytest.mark.skipif(is_running_under_qemu(), reason="Complex math in QEMU fails")
@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["complex128", "complex64"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_dot_complex(ndim, dtype, capability, stats_fixture):
    """Compares the simd.dot() and simd.vdot() against NumPy for complex numbers."""
    np.random.seed()
    a = (np.random.randn(ndim) + 1.0j * np.random.randn(ndim)).astype(dtype)
    b = (np.random.randn(ndim) + 1.0j * np.random.randn(ndim)).astype(dtype)

    keep_one_capability(capability)
    accurate_dt, accurate = profile(np.dot, a.astype(np.complex128), b.astype(np.complex128))
    expected_dt, expected = profile(np.dot, a, b)
    result_dt, result = profile(simd.dot, a, b)
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors("dot", ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)

    accurate_dt, accurate = profile(np.vdot, a.astype(np.complex128), b.astype(np.complex128))
    expected_dt, expected = profile(np.vdot, a, b)
    result_dt, result = profile(simd.vdot, a, b)
    result = np.array(result)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    collect_errors("vdot", ndim, dtype, accurate, accurate_dt, expected, expected_dt, result, result_dt, stats_fixture)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("dtype", ["uint16", "uint32"])
@pytest.mark.parametrize("first_length_bound", [10, 100, 1000])
@pytest.mark.parametrize("second_length_bound", [10, 100, 1000])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_intersect(dtype, first_length_bound, second_length_bound, capability):
    """Compares the simd.intersect() function with numpy.intersect1d."""

    if is_running_under_qemu() and (platform.machine() == "aarch64" or platform.machine() == "arm64"):
        pytest.skip("In QEMU `aarch64` emulation on `x86_64` the `intersect` function is not reliable")

    np.random.seed()

    a_length = np.random.randint(1, first_length_bound)
    b_length = np.random.randint(1, second_length_bound)
    a = np.random.randint(first_length_bound * 2, size=a_length, dtype=dtype)
    b = np.random.randint(second_length_bound * 2, size=b_length, dtype=dtype)

    # Remove duplicates, converting into sorted arrays
    a = np.unique(a)
    b = np.unique(b)

    keep_one_capability(capability)
    expected = baseline_intersect(a, b)
    result = simd.intersect(a, b)

    assert round(float(expected)) == round(float(result)), f"Missing {np.intersect1d(a, b)} from {a} and {b}"


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float64", "float32", "float16", "int8", "uint8"])
@pytest.mark.parametrize("kernel", ["fma"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_fma(ndim, dtype, kernel, capability, stats_fixture):
    """"""

    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()
    if np.issubdtype(np.dtype(dtype), np.integer):
        dtype_info = np.iinfo(np.dtype(dtype))
        a = np.random.randint(dtype_info.min, dtype_info.max, size=ndim, dtype=dtype)
        b = np.random.randint(dtype_info.min, dtype_info.max, size=ndim, dtype=dtype)
        c = np.random.randint(dtype_info.min, dtype_info.max, size=ndim, dtype=dtype)
        alpha = abs(np.random.randn(1).astype(np.float64).item()) / 512
        beta = abs(np.random.randn(1).astype(np.float64).item()) / 3
        atol = 1  # ? Allow at most one rounding error per vector
        rtol = 0
    else:
        a = np.random.randn(ndim).astype(dtype)
        b = np.random.randn(ndim).astype(dtype)
        c = np.random.randn(ndim).astype(dtype)
        alpha = np.random.randn(1).astype(np.float64).item()
        beta = np.random.randn(1).astype(np.float64).item()
        atol = SIMSIMD_ATOL
        rtol = SIMSIMD_RTOL

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(kernel)

    accurate_dt, accurate = profile(
        baseline_kernel,
        a.astype(np.float64),
        b.astype(np.float64),
        c.astype(np.float64),
        alpha=alpha,
        beta=beta,
    )
    expected_dt, expected = profile(baseline_kernel, a, b, c, alpha=alpha, beta=beta)
    result_dt, result = profile(simd_kernel, a, b, c, alpha=alpha, beta=beta)
    result = np.array(result)

    np.testing.assert_allclose(result, expected.astype(np.float64), atol=atol, rtol=rtol)
    collect_errors(
        kernel,
        ndim,
        dtype,
        accurate,
        accurate_dt,
        expected,
        expected_dt,
        result,
        result_dt,
        stats_fixture,
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float64", "float32", "float16", "int8", "uint8"])
@pytest.mark.parametrize("kernel", ["wsum"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_wsum(ndim, dtype, kernel, capability, stats_fixture):
    """"""

    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()
    if np.issubdtype(np.dtype(dtype), np.integer):
        dtype_info = np.iinfo(np.dtype(dtype))
        a = np.random.randint(dtype_info.min, dtype_info.max, size=ndim, dtype=dtype)
        b = np.random.randint(dtype_info.min, dtype_info.max, size=ndim, dtype=dtype)
        alpha = abs(np.random.randn(1).astype(np.float64).item()) / 2
        beta = abs(np.random.randn(1).astype(np.float64).item()) / 2
        atol = 1  # ? Allow at most one rounding error per vector
        rtol = 0
    else:
        a = np.random.randn(ndim).astype(dtype)
        b = np.random.randn(ndim).astype(dtype)
        alpha = np.random.randn(1).astype(np.float64).item()
        beta = np.random.randn(1).astype(np.float64).item()
        atol = SIMSIMD_ATOL
        rtol = SIMSIMD_RTOL

    keep_one_capability(capability)
    baseline_kernel, simd_kernel = name_to_kernels(kernel)

    accurate_dt, accurate = profile(
        baseline_kernel,
        a.astype(np.float64),
        b.astype(np.float64),
        alpha=alpha,
        beta=beta,
    )
    expected_dt, expected = profile(baseline_kernel, a, b, alpha=alpha, beta=beta)
    result_dt, result = profile(simd_kernel, a, b, alpha=alpha, beta=beta)
    result = np.array(result)

    np.testing.assert_allclose(result, expected.astype(np.float64), atol=atol, rtol=rtol)
    collect_errors(
        kernel,
        ndim,
        dtype,
        accurate,
        accurate_dt,
        expected,
        expected_dt,
        result,
        result_dt,
        stats_fixture,
    )


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("dtype", ["float64", "float32", "float16"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_batch(ndim, dtype, capability):
    """Compares the simd.simd.sqeuclidean() function with scipy.spatial.distance.sqeuclidean() for a batch of vectors, measuring the accuracy error for f16, and f32 types."""

    if dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()
    keep_one_capability(capability)

    # Distance between matrixes A (N x D scalars) and B (N x D scalars) is an array with N floats.
    A = np.random.randn(10, ndim).astype(dtype)
    B = np.random.randn(10, ndim).astype(dtype)
    result_np = [spd.sqeuclidean(A[i], B[i]) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrixes A (N x D scalars) and B (1 x D scalars) is an array with N floats.
    B = np.random.randn(1, ndim).astype(dtype)
    result_np = [spd.sqeuclidean(A[i], B[0]) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrixes A (1 x D scalars) and B (N x D scalars) is an array with N floats.
    A = np.random.randn(1, ndim).astype(dtype)
    B = np.random.randn(10, ndim).astype(dtype)
    result_np = [spd.sqeuclidean(A[0], B[i]) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrix A (N x D scalars) and array B (D scalars) is an array with N floats.
    A = np.random.randn(10, ndim).astype(dtype)
    B = np.random.randn(ndim).astype(dtype)
    result_np = [spd.sqeuclidean(A[i], B) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrix B (N x D scalars) and array A (D scalars) is an array with N floats.
    B = np.random.randn(10, ndim).astype(dtype)
    A = np.random.randn(ndim).astype(dtype)
    result_np = [spd.sqeuclidean(B[i], A) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(B, A)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrixes A (N x D scalars) and B (N x D scalars) in slices of bigger matrices.
    A_extended = np.random.randn(10, ndim + 11).astype(dtype)
    B_extended = np.random.randn(10, ndim + 13).astype(dtype)
    A = A_extended[:, 1 : 1 + ndim]
    B = B_extended[:, 3 : 3 + ndim]
    assert A.base is A_extended and B.base is B_extended
    assert A.__array_interface__["strides"] is not None and B.__array_interface__["strides"] is not None
    result_np = [spd.sqeuclidean(A[i], B[i]) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrixes A (N x D scalars) and B (N x D scalars) in a transposed matrix.
    #! This requires calling `np.ascontiguousarray()` to ensure the matrix is in the right format.
    A = np.random.randn(10, ndim).astype(dtype)
    B = np.ascontiguousarray(np.random.randn(ndim, 10).astype(dtype).T)
    result_np = [spd.sqeuclidean(A[i], B[i]) for i in range(10)]
    result_simd = np.array(simd.sqeuclidean(A, B)).astype(np.float64)
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)

    # Distance between matrixes A (N x D scalars) and B (N x D scalars) with a differnt output type.
    A = np.random.randn(10, ndim).astype(dtype)
    B = np.random.randn(10, ndim).astype(dtype)
    result_np = np.array([spd.sqeuclidean(A[i], B[i]) for i in range(10)]).astype(np.float32)
    result_simd = np.array(simd.sqeuclidean(A, B, out_dtype="float32"))
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    assert result_simd.dtype == result_np.dtype

    # Distance between matrixes A (N x D scalars) and B (N x D scalars) with a supplied output buffer.
    A = np.random.randn(10, ndim).astype(dtype)
    B = np.random.randn(10, ndim).astype(dtype)
    result_np = np.array([spd.sqeuclidean(A[i], B[i]) for i in range(10)]).astype(np.float32)
    result_simd = np.zeros(10, dtype=np.float32)
    assert simd.sqeuclidean(A, B, out=result_simd) is None
    assert np.allclose(result_simd, result_np, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    assert result_simd.dtype == result_np.dtype


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("input_dtype", ["float32", "float16"])
@pytest.mark.parametrize("out_dtype", [None, "float32", "int32"])
@pytest.mark.parametrize("metric", ["cosine", "sqeuclidean"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_cdist(ndim, input_dtype, out_dtype, metric, capability):
    """Compares the simd.cdist() function with scipy.spatial.distance.cdist(), measuring the accuracy error for f16, and f32 types using sqeuclidean and cosine metrics."""

    if input_dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()
    keep_one_capability(capability)

    # We will work with random matrices A (M x D) and B (N x D).
    # To test their ability to handle strided inputs, we are going to add one extra dimension.
    M, N = 10, 15
    A_extended = np.random.randn(M, ndim + 1).astype(input_dtype)
    B_extended = np.random.randn(N, ndim + 3).astype(input_dtype)
    A = A_extended[:, :ndim]
    B = B_extended[:, :ndim]

    if out_dtype is None:
        expected = spd.cdist(A, B, metric)
        result = simd.cdist(A, B, metric)
        #! Same functions can be used in-place, but SciPy doesn't support misaligned outputs
        expected_out = np.zeros((M, N))
        result_out_extended = np.zeros((M, N + 7))
        result_out = result_out_extended[:, :N]
        assert spd.cdist(A, B, metric, out=expected_out) is not None
        assert simd.cdist(A, B, metric, out=result_out) is None
    else:
        expected = spd.cdist(A, B, metric).astype(out_dtype)
        result = simd.cdist(A, B, metric, out_dtype=out_dtype)

        #! Same functions can be used in-place, but SciPy doesn't support misaligned outputs
        expected_out = np.zeros((M, N), dtype=np.float64)
        result_out_extended = np.zeros((M, N + 7), dtype=out_dtype)
        result_out = result_out_extended[:, :N]
        assert spd.cdist(A, B, metric, out=expected_out) is not None
        assert simd.cdist(A, B, metric, out=result_out) is None
        #! Moreover, SciPy supports only double-precision outputs, so we need to downcast afterwards.
        expected_out = expected_out.astype(out_dtype)

    # Assert they're close.
    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    np.testing.assert_allclose(result_out, expected_out, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("input_dtype", ["float32", "float16"])
@pytest.mark.parametrize("out_dtype", [None, "float32", "int32"])
@pytest.mark.parametrize("metric", ["cosine", "sqeuclidean"])
def test_cdist_itself(ndim, input_dtype, out_dtype, metric):
    """Compares the simd.cdist(A, A) function with scipy.spatial.distance.cdist(A, A), measuring the accuracy error for f16, and f32 types using sqeuclidean and cosine metrics."""

    if input_dtype == "float16" and is_running_under_qemu():
        pytest.skip("Testing low-precision math isn't reliable in QEMU")

    np.random.seed()

    A = np.random.randn(10, ndim + 1).astype(input_dtype)
    if out_dtype is None:
        expected = spd.cdist(A, A, metric)
        result = simd.cdist(A, A, metric=metric)
    else:
        expected = spd.cdist(A, A, metric).astype(out_dtype)
        result = simd.cdist(A, A, metric=metric, out_dtype=out_dtype)

    # Assert they're close.
    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("input_dtype", ["complex128", "complex64"])
@pytest.mark.parametrize("out_dtype", [None, "complex128", "complex64"])
@pytest.mark.parametrize("metric", ["dot", "vdot"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_cdist_complex(ndim, input_dtype, out_dtype, metric, capability):
    """Compares the simd.cdist() for complex numbers to pure NumPy complex dot-products, as SciPy has no such functionality.
    The goal is to make sure that addressing multi-component numbers is done properly in both real and imaginary parts.
    """

    np.random.seed()
    keep_one_capability(capability)

    # We will work with random matrices A (M x D) and B (N x D).
    # To test their ability to handle strided inputs, we are going to add one extra dimension.
    M, N = 10, 15
    A_extended = np.random.randn(M, ndim + 1).astype(input_dtype)
    B_extended = np.random.randn(N, ndim + 3).astype(input_dtype)
    A = A_extended[:, :ndim]
    B = B_extended[:, :ndim]
    C_extended = np.random.randn(M, N + 7).astype(out_dtype if out_dtype else np.complex128)
    C = C_extended[:, :N]

    #! Unlike the `np.dot`, the `np.vdot` flattens multi-dimensional inputs into 1D arrays.
    #! So to compreare the results we need to manually compute all the dot-products.
    expected = np.zeros((M, N), dtype=out_dtype if out_dtype else np.complex128)
    baseline_kernel = np.dot if metric == "dot" else np.vdot
    for i in range(M):
        for j in range(N):
            expected[i, j] = baseline_kernel(A[i], B[j])

    # Compute with SimSIMD:
    if out_dtype is None:
        result1d = simd.cdist(A[0], B[0], metric=metric)
        result2d = simd.cdist(A, B, metric=metric)
        assert simd.cdist(A, B, metric=metric, out=C) is None
    else:
        expected = expected.astype(out_dtype)
        result1d = simd.cdist(A[0], B[0], metric=metric, out_dtype=out_dtype)
        result2d = simd.cdist(A, B, metric=metric, out_dtype=out_dtype)
        assert simd.cdist(A, B, metric=metric, out_dtype=out_dtype, out=C) is None

    # Assert they're close.
    np.testing.assert_allclose(result1d, expected[0, 0], atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    np.testing.assert_allclose(result2d, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)
    np.testing.assert_allclose(C, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)


@pytest.mark.skipif(not numpy_available, reason="NumPy is not installed")
@pytest.mark.skipif(not scipy_available, reason="SciPy is not installed")
@pytest.mark.repeat(5)
@pytest.mark.parametrize("ndim", [11, 97, 1536])
@pytest.mark.parametrize("out_dtype", [None, "float32", "float16", "int8"])
@pytest.mark.parametrize("capability", possible_capabilities)
def test_cdist_hamming(ndim, out_dtype, capability):
    """Compares various SIMD kernels (like Hamming and Jaccard/Tanimoto distances) for dense bit arrays
    with their NumPy or baseline counterparts, even though, they can't process sub-byte-sized scalars."""
    np.random.seed()
    keep_one_capability(capability)

    # Create random matrices A (M x D) and B (N x D).
    M, N = 10, 15
    A = np.random.randint(2, size=(M, ndim)).astype(np.uint8)
    B = np.random.randint(2, size=(N, ndim)).astype(np.uint8)
    A_bits, B_bits = np.packbits(A, axis=1), np.packbits(B, axis=1)

    if out_dtype is None:
        # SciPy divides the Hamming distance by the number of dimensions, so we need to multiply it back.
        expected = spd.cdist(A, B, "hamming") * ndim
        result = simd.cdist(A_bits, B_bits, metric="hamming", dtype="bin8")
    else:
        expected = (spd.cdist(A, B, "hamming") * ndim).astype(out_dtype)
        result = simd.cdist(A_bits, B_bits, metric="hamming", dtype="bin8", out_dtype=out_dtype)

    np.testing.assert_allclose(result, expected, atol=SIMSIMD_ATOL, rtol=SIMSIMD_RTOL)


if __name__ == "__main__":
    pytest.main(
        [
            "-s",  # Print stdout
            "-x",  # Stop on first failure
            "-v",  # Verbose output
            "--tb=short",  # Short traceback format
        ]
    )
