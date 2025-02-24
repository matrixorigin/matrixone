from typing import Any, Union, Literal, Optional, TypeAlias

# A lot of annotation features a depend on the Python version:
# - `typing.TypeAlias` Type aliases are supported from Python 3.10 to 3.11
# - `type` Type statements are supported from Python 3.12, replacing `typing.TypeAlias`
# - `typing.Literal` Literal types are supported from Python 3.8
#
# We can't and shouldn't use a different `.pyi` file for every single Python version.
# So let's assume we are targeting Python 3.11 and we have NumPy available.
from numpy.typing import NDArray

_BufferType: TypeAlias = Union[NDArray[Any], memoryview]

_MetricType = Literal[
    "euclidean",
    "sqeuclidean",
    "inner",
    "dot",
    "cosine",
    "cos",
    "hamming",
    "jaccard",
    "kullbackleibler",
    "kl",
    "jensenshannon",
    "js",
    "intersection",
    "bilinear",
    "mahalanobis",
    "fma",
    "wsum",
]
_IntegralType = Literal[
    "bin8",
    # Signed integers
    "int8",
    "int16",
    "int32",
    "int64",
    # Unsigned integers
    "uint8",
    "uint16",
    "uint32",
    "uint64",
]
_FloatType = Literal[
    "f32",
    "float32",
    "f16",
    "float16",
    "f64",
    "float64",
    "bf16",  #! Not supported by NumPy
    "bfloat16",  #! Not supported by NumPy
]
_ComplexType = Literal[
    "complex32",  #! Not supported by NumPy
    "bcomplex32",  #! Not supported by NumPy
    "complex64",
    "complex128",
]

class DistancesTensor(memoryview): ...

# ---------------------------------------------------------------------

# Controlling SIMD capabilities
def get_capabilities() -> dict[str, bool]: ...
def enable_capability(capability: str, /) -> None: ...
def disable_capability(capability: str, /) -> None: ...

# Accessing function pointers
def pointer_to_euclidean(dtype: Union[_IntegralType, _FloatType], /) -> int: ...
def pointer_to_sqeuclidean(dtype: Union[_IntegralType, _FloatType], /) -> int: ...
def pointer_to_cosine(dtype: Union[_IntegralType, _FloatType], /) -> int: ...
def pointer_to_inner(dtype: Union[_FloatType, _ComplexType], /) -> int: ...
def pointer_to_dot(dtype: Union[_FloatType, _ComplexType], /) -> int: ...
def pointer_to_vdot(dtype: Union[_FloatType, _ComplexType], /) -> int: ...
def pointer_to_hamming(dtype: _IntegralType, /) -> int: ...
def pointer_to_jaccard(dtype: _IntegralType, /) -> int: ...
def pointer_to_jensenshannon(dtype: _FloatType, /) -> int: ...
def pointer_to_kullbackleibler(dtype: _FloatType, /) -> int: ...

# ---------------------------------------------------------------------

# All pairwise distances, similar to: `scipy.spatial.distance.cdist`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.cdist.html
def cdist(
    a: _BufferType,
    b: _BufferType,
    /,
    metric: _MetricType = "euclidean",
    *,
    threads: int = 1,
    dtype: Optional[Union[_IntegralType, _FloatType, _ComplexType]] = None,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType, _ComplexType] = "d",
) -> Optional[Union[float, complex, DistancesTensor]]: ...

# ---------------------------------------------------------------------
# Vector-vector dot products for real and complex numbers
# ---------------------------------------------------------------------

# Inner product, similar to: `numpy.inner`.
# https://numpy.org/doc/stable/reference/generated/numpy.inner.html
def inner(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[Union[_FloatType, _ComplexType]] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType, _ComplexType] = "d",
) -> Optional[Union[float, complex, DistancesTensor]]: ...

# Dot product, similar to: `numpy.dot`.
# https://numpy.org/doc/stable/reference/generated/numpy.dot.html
def dot(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[Union[_FloatType, _ComplexType]] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType, _ComplexType] = None,
) -> Optional[Union[float, complex, DistancesTensor]]: ...

# Vector-vector dot product for complex conjugates, similar to: `numpy.vdot`.
# https://numpy.org/doc/stable/reference/generated/numpy.vdot.html
def vdot(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[_ComplexType] = None,
    *,
    out: Optional[Union[float, complex, DistancesTensor]] = None,
    out_dtype: Optional[_ComplexType] = None,
) -> Optional[Union[complex, DistancesTensor]]: ...

# ---------------------------------------------------------------------
# Vector-vector spatial distance metrics for real and integer numbers
# ---------------------------------------------------------------------

# Vector-vector squared Euclidean distance, similar to: `scipy.spatial.distance.sqeuclidean`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.sqeuclidean.html
# https://numpy.org/doc/stable/reference/generated/numpy.linalg.norm.html
def sqeuclidean(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[Union[_IntegralType, _FloatType]] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# Vector-vector cosine distance, similar to: `scipy.spatial.distance.cosine`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.cosine.html
def cosine(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[Union[_IntegralType, _FloatType]] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# ---------------------------------------------------------------------
# Vector-vector similarity functions for binary vectors
# ---------------------------------------------------------------------

# Vector-vector Hamming distance, similar to: `scipy.spatial.distance.hamming`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.hamming.html
def hamming(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[_IntegralType] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# Vector-vector Jaccard distance, similar to: `scipy.spatial.distance.jaccard`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.jaccard.html
def jaccard(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[_IntegralType] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# ---------------------------------------------------------------------
# Vector-vector similarity between probability distributions
# ---------------------------------------------------------------------

# Vector-vector Jensen-Shannon distance, similar to: `scipy.spatial.distance.jensenshannon`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.jensenshannon.html
def jensenshannon(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[_FloatType] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# Vector-vector Kullback-Leibler divergence, similar to: `scipy.spatial.distance.kullback_leibler`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.kullback_leibler.html
def kullbackleibler(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[_FloatType] = None,
    *,
    out: Optional[_BufferType] = None,
    out_dtype: Union[_FloatType] = None,
) -> Optional[Union[float, DistancesTensor]]: ...

# ---------------------------------------------------------------------
# Vector-vector similarity between vectors in curved spaces
# ---------------------------------------------------------------------

# Vector-vector bilinear distance, similar to: `numpy.dot(a, metric_tensor @ vector2)`.
# https://numpy.org/doc/stable/reference/generated/numpy.dot.html
def bilinear(
    a: _BufferType,
    b: _BufferType,
    metric_tensor: _BufferType,
    /,
    dtype: Optional[_FloatType] = None,
) -> float: ...

# Vector-vector Mahalanobis distance, similar to: `scipy.spatial.distance.mahalanobis`.
# https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.mahalanobis.html
def mahalanobis(
    a: _BufferType,
    b: _BufferType,
    inverse_covariance: _BufferType,
    /,
    dtype: Optional[_FloatType] = None,
) -> float: ...

# ---------------------------------------------------------------------
# Vector-vector similarity between sparse vectors
# ---------------------------------------------------------------------

# Vector-vector intersection similarity, similar to: `numpy.intersect1d`.
# https://numpy.org/doc/stable/reference/generated/numpy.intersect1d.html
def intersection(array1: _BufferType, array2: _BufferType, /) -> float: ...

# ---------------------------------------------------------------------
# Vector-vector math: FMA, WSum
# ---------------------------------------------------------------------

# Vector-vector element-wise fused-multiply add.
def fma(
    a: _BufferType,
    b: _BufferType,
    c: _BufferType,
    /,
    dtype: Optional[Union[_FloatType, _IntegralType]] = None,
    *,
    alpha: float = 1,
    beta: float = 1,
    out: Optional[_BufferType] = None,
) -> Optional[DistancesTensor]: ...

# Vector-vector element-wise weighted sum.
def wum(
    a: _BufferType,
    b: _BufferType,
    /,
    dtype: Optional[Union[_FloatType, _IntegralType]] = None,
    *,
    alpha: float = 1,
    beta: float = 1,
    out: Optional[_BufferType] = None,
) -> Optional[DistancesTensor]: ...
