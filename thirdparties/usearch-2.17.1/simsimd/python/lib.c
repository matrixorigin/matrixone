/**
 *  @brief      Pure CPython bindings for SimSIMD.
 *  @file       lib.c
 *  @author     Ash Vardanian
 *  @date       January 1, 2023
 *  @copyright  Copyright (c) 2023
 *
 *  @section    Latency, Quality, and Arguments Parsing
 *
 *  The complexity of implementing high-quality CPython bindings is often underestimated.
 *  You can't use high-level wrappers like PyBind11 and NanoBind, and you shouldn't use
 *  SWIG-like messy toolchains. Most of them use expensive dynamic data-structures to map
 *  your callbacks to object/module properties, not taking advantage of the CPython API.
 *  They are prohibitively slow for low-latency operations like checking the length of a
 *  container, handling vectors, or strings.
 *
 *  Once you are down to the CPython API, there is a lot of boilerplate code to write and
 *  it's understandable that most people lazily use the `PyArg_ParseTupleAndKeywords` and
 *  `PyArg_ParseTuple` functions. Those, however, need to dynamically parse format specifier
 *  strings at runtime, which @b can't be fast by design! Moreover, they are not suitable
 *  for the Python's "Fast Calling Convention". In a typical scenario, a function is defined
 *  with `METH_VARARGS | METH_KEYWORDS` and has a signature like:
 *
 *  @code {.c}
 *      static PyObject* cdist(
 *          PyObject * self,
 *          PyObject * positional_args_tuple,
 *          PyObject * named_args_dict) {
 *          PyObject * a_obj, b_obj, metric_obj, out_obj, dtype_obj, out_dtype_obj, threads_obj;
 *          static char* names[] = {"a", "b", "metric", "threads", "dtype", "out_dtype", NULL};
 *          if (!PyArg_ParseTupleAndKeywords(
 *              positional_args_tuple, named_args_dict, "OO|s$Kss", names,
 *              &a_obj, &b_obj, &metric_str, &threads, &dtype_str, &out_dtype_str))
 *              return NULL;
 *          ...
 *  @endcode
 *
 *  This `cdist` example takes 2 positional, 1 postional or named, 3 named-only arguments.
 *  The alternative using the `METH_FASTCALL` is to use a function signature like:
 *
 *  @code {.c}
 *     static PyObject* cdist(
 *          PyObject * self,
 *          PyObject * const * args_c_array,    //! C array of `args_count` pointers
 *          Py_ssize_t const positional_args_count,   //! The `args_c_array` may be larger than this
 *          PyObject * args_names_tuple) {      //! May be smaller than `args_count`
 *          Py_ssize_t args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
 *          Py_ssize_t args_count = positional_args_count + args_names_count;
 *          ...
 *  @endcode
 *
 *  The positional elements are easy to access in that C array, but parsing the named arguments is tricky.
 *  There may be a case, when the call is ill-formed and more positional arguments are provided than needed.
 *
 *  @code {.py}
 *          cdist(a, b, "cos", "dos"):               //! positional_args_count == 4, args_names_count == 0
 *          cdist(a, b, "cos", metric="dos"):        //! positional_args_count == 3, args_names_count == 1
 *          cdist(a, b, metric="cos", metric="dos"): //! positional_args_count == 2, args_names_count == 2
 *  @endcode
 *
 *  If the same argument is provided twice, a @b `TypeError` is raised.
 *  If the argument is not found, a @b `KeyError` is raised.
 *
 *  https://ashvardanian.com/posts/discount-on-keyword-arguments-in-python/
 *
 *  @section    Buffer Protocol and NumPy Compatibility
 *
 *  Most modern Machine Learning frameworks struggle with the buffer protocol compatibility.
 *  At best, they provide zero-copy NumPy views of the underlying data, introducing unnecessary
 *  dependency on NumPy, a memory allocation for the wrapper, and a constraint on the supported
 *  numeric types. The last is a noticeable limitation, as both PyTorch and TensorFlow have
 *  richer type systems than NumPy.
 *
 *  You can't convert a PyTorch `Tensor` to a `memoryview` object.
 *  If you try to convert a `bf16` TensorFlow `Tensor` to a `memoryview` object, you will get an error:
 *
 *      ! ValueError: cannot include dtype 'E' in a buffer
 *
 *  Moreover, the CPython documentation and the NumPy documentation diverge on the format specificers
 *  for the `typestr` and `format` data-type descriptor strings, making the development error-prone.
 *  At this point, SimSIMD seems to be @b the_only_package that at least attempts to provide interoperability.
 *
 *  https://numpy.org/doc/stable/reference/arrays.interface.html
 *  https://pearu.github.io/array_interface_pytorch.html
 *  https://github.com/pytorch/pytorch/issues/54138
 *  https://github.com/pybind/pybind11/issues/1908
 */
#include <math.h>

#if defined(__linux__)
#ifdef _OPENMP
#include <omp.h>
#endif
#endif

#include <simsimd/simsimd.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

typedef struct TensorArgument {
    char *start;
    size_t dimensions;
    size_t count;
    size_t stride;
    int rank;
    simsimd_datatype_t datatype;
} TensorArgument;

typedef struct DistancesTensor {
    PyObject_HEAD                    //
        simsimd_datatype_t datatype; // Double precision real or complex numbers
    size_t dimensions;               // Can be only 1 or 2 dimensions
    Py_ssize_t shape[2];             // Dimensions of the tensor
    Py_ssize_t strides[2];           // Strides for each dimension
    simsimd_distance_t start[];      // Variable length data aligned to 64-bit scalars
} DistancesTensor;

static int DistancesTensor_getbuffer(PyObject *export_from, Py_buffer *view, int flags);
static void DistancesTensor_releasebuffer(PyObject *export_from, Py_buffer *view);

static PyBufferProcs DistancesTensor_as_buffer = {
    .bf_getbuffer = DistancesTensor_getbuffer,
    .bf_releasebuffer = DistancesTensor_releasebuffer,
};

static PyTypeObject DistancesTensorType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "simsimd.DistancesTensor",
    .tp_doc = "Zero-copy view of an internal tensor, compatible with NumPy",
    .tp_basicsize = sizeof(DistancesTensor),
    // Instead of using `simsimd_distance_t` for all the elements,
    // we use `char` to allow user to specify the datatype on `cdist`-like functions.
    .tp_itemsize = sizeof(char),
    .tp_as_buffer = &DistancesTensor_as_buffer,
};

/// @brief  Global variable that caches the CPU capabilities, and is computed just onc, when the module is loaded.
simsimd_capability_t static_capabilities = simsimd_cap_serial_k;

/// @brief Helper method to check for string equality.
/// @return 1 if the strings are equal, 0 otherwise.
int same_string(char const *a, char const *b) { return strcmp(a, b) == 0; }

/// @brief Helper method to check if a logical datatype is complex and should be represented as two scalars.
/// @return 1 if the datatype is complex, 0 otherwise.
int is_complex(simsimd_datatype_t datatype) {
    return datatype == simsimd_datatype_f32c_k || datatype == simsimd_datatype_f64c_k ||
           datatype == simsimd_datatype_f16c_k || datatype == simsimd_datatype_bf16c_k;
}

/// @brief Converts a Python-ic datatype string to a logical datatype, normalizing the format.
/// @return `simsimd_datatype_unknown_k` if the datatype is not supported, otherwise the logical datatype.
/// @see https://docs.python.org/3/library/struct.html#format-characters
/// @see https://numpy.org/doc/stable/reference/arrays.interface.html
/// @see https://github.com/pybind/pybind11/issues/1908
simsimd_datatype_t python_string_to_datatype(char const *name) {
    // Floating-point numbers:
    if (same_string(name, "float32") || same_string(name, "f32") || // SimSIMD-specific
        same_string(name, "f4") || same_string(name, "<f4") ||      // Sized float
        same_string(name, "f") || same_string(name, "<f"))          // Named type
        return simsimd_datatype_f32_k;
    else if (same_string(name, "float16") || same_string(name, "f16") || // SimSIMD-specific
             same_string(name, "f2") || same_string(name, "<f2") ||      // Sized float
             same_string(name, "e") || same_string(name, "<e"))          // Named type
        return simsimd_datatype_f16_k;
    else if (same_string(name, "float64") || same_string(name, "f64") || // SimSIMD-specific
             same_string(name, "f8") || same_string(name, "<f8") ||      // Sized float
             same_string(name, "d") || same_string(name, "<d"))          // Named type
        return simsimd_datatype_f64_k;
    //? The exact format is not defined, but TensorFlow uses 'E' for `bf16`?!
    else if (same_string(name, "bfloat16") || same_string(name, "bf16")) // SimSIMD-specific
        return simsimd_datatype_bf16_k;

    // Complex numbers:
    else if (same_string(name, "complex64") ||                                             // SimSIMD-specific
             same_string(name, "F4") || same_string(name, "<F4") ||                        // Sized complex
             same_string(name, "Zf") || same_string(name, "F") || same_string(name, "<F")) // Named type
        return simsimd_datatype_f32c_k;
    else if (same_string(name, "complex128") ||                                            // SimSIMD-specific
             same_string(name, "F8") || same_string(name, "<F8") ||                        // Sized complex
             same_string(name, "Zd") || same_string(name, "D") || same_string(name, "<D")) // Named type
        return simsimd_datatype_f64c_k;
    else if (same_string(name, "complex32") ||                                             // SimSIMD-specific
             same_string(name, "F2") || same_string(name, "<F2") ||                        // Sized complex
             same_string(name, "Ze") || same_string(name, "E") || same_string(name, "<E")) // Named type
        return simsimd_datatype_f16c_k;
    //? The exact format is not defined, but TensorFlow uses 'E' for `bf16`?!
    else if (same_string(name, "bcomplex32")) // SimSIMD-specific
        return simsimd_datatype_bf16c_k;

    //! Boolean values:
    else if (same_string(name, "bin8") || // SimSIMD-specific
             same_string(name, "?"))      // Named type
        return simsimd_datatype_b8_k;

    // Signed integers:
    else if (same_string(name, "int8") ||                                                       // SimSIMD-specific
             same_string(name, "i1") || same_string(name, "|i1") || same_string(name, "<i1") || // Sized integer
             same_string(name, "b") || same_string(name, "<b"))                                 // Named type
        return simsimd_datatype_i8_k;
    else if (same_string(name, "int16") ||                                                      // SimSIMD-specific
             same_string(name, "i2") || same_string(name, "|i2") || same_string(name, "<i2") || // Sized integer
             same_string(name, "h") || same_string(name, "<h"))                                 // Named type
        return simsimd_datatype_i16_k;

        //! On Windows the 32-bit and 64-bit signed integers will have different specifiers:
        //! https://github.com/pybind/pybind11/issues/1908
#if defined(_MSC_VER) || defined(__i386__)
    else if (same_string(name, "int32") ||                                                      // SimSIMD-specific
             same_string(name, "i4") || same_string(name, "|i4") || same_string(name, "<i4") || // Sized integer
             same_string(name, "l") || same_string(name, "<l"))                                 // Named type
        return simsimd_datatype_i32_k;
    else if (same_string(name, "int64") ||                                                      // SimSIMD-specific
             same_string(name, "i8") || same_string(name, "|i8") || same_string(name, "<i8") || // Sized integer
             same_string(name, "q") || same_string(name, "<q"))                                 // Named type
        return simsimd_datatype_i64_k;
#else // On Linux and macOS:
    else if (same_string(name, "int32") ||                                                      // SimSIMD-specific
             same_string(name, "i4") || same_string(name, "|i4") || same_string(name, "<i4") || // Sized integer
             same_string(name, "i") || same_string(name, "<i"))                                 // Named type
        return simsimd_datatype_i32_k;
    else if (same_string(name, "int64") ||                                                      // SimSIMD-specific
             same_string(name, "i8") || same_string(name, "|i8") || same_string(name, "<i8") || // Sized integer
             same_string(name, "l") || same_string(name, "<l"))                                 // Named type
        return simsimd_datatype_i64_k;
#endif

    // Unsigned integers:
    else if (same_string(name, "uint8") ||                                                      // SimSIMD-specific
             same_string(name, "u1") || same_string(name, "|u1") || same_string(name, "<u1") || // Sized integer
             same_string(name, "B") || same_string(name, "<B"))                                 // Named type
        return simsimd_datatype_u8_k;
    else if (same_string(name, "uint16") ||                                                     // SimSIMD-specific
             same_string(name, "u2") || same_string(name, "|u2") || same_string(name, "<u2") || // Sized integer
             same_string(name, "H") || same_string(name, "<H"))                                 // Named type
        return simsimd_datatype_u16_k;

        //! On Windows the 32-bit and 64-bit unsigned integers will have different specifiers:
        //! https://github.com/pybind/pybind11/issues/1908
#if defined(_MSC_VER) || defined(__i386__)
    else if (same_string(name, "uint32") ||                                                     // SimSIMD-specific
             same_string(name, "i4") || same_string(name, "|i4") || same_string(name, "<i4") || // Sized integer
             same_string(name, "L") || same_string(name, "<L"))                                 // Named type
        return simsimd_datatype_u32_k;
    else if (same_string(name, "uint64") ||                                                     // SimSIMD-specific
             same_string(name, "i8") || same_string(name, "|i8") || same_string(name, "<i8") || // Sized integer
             same_string(name, "Q") || same_string(name, "<Q"))                                 // Named type
        return simsimd_datatype_u64_k;
#else // On Linux and macOS:
    else if (same_string(name, "uint32") ||                                                     // SimSIMD-specific
             same_string(name, "u4") || same_string(name, "|u4") || same_string(name, "<u4") || // Sized integer
             same_string(name, "I") || same_string(name, "<I"))                                 // Named type
        return simsimd_datatype_u32_k;
    else if (same_string(name, "uint64") ||                                                     // SimSIMD-specific
             same_string(name, "u8") || same_string(name, "|u8") || same_string(name, "<u8") || // Sized integer
             same_string(name, "L") || same_string(name, "<L"))                                 // Named type
        return simsimd_datatype_u64_k;
#endif

    else
        return simsimd_datatype_unknown_k;
}

/// @brief Returns the Python string representation of a datatype for the buffer protocol.
/// @param dtype Logical datatype, can be complex.
/// @return "unknown" if the datatype is not supported, otherwise a string.
/// @see https://docs.python.org/3/library/struct.html#format-characters
char const *datatype_to_python_string(simsimd_datatype_t dtype) {
    switch (dtype) {
        // Floating-point numbers:
    case simsimd_datatype_f64_k: return "d";
    case simsimd_datatype_f32_k: return "f";
    case simsimd_datatype_f16_k: return "e";
    // Complex numbers:
    case simsimd_datatype_f64c_k: return "Zd";
    case simsimd_datatype_f32c_k: return "Zf";
    case simsimd_datatype_f16c_k: return "Ze";
    // Boolean values:
    case simsimd_datatype_b8_k: return "?";
    // Signed integers:
    case simsimd_datatype_i8_k: return "b";
    case simsimd_datatype_i16_k: return "h";
    case simsimd_datatype_i32_k: return "i";
    case simsimd_datatype_i64_k: return "q";
    // Unsigned integers:
    case simsimd_datatype_u8_k: return "B";
    case simsimd_datatype_u16_k: return "H";
    case simsimd_datatype_u32_k: return "I";
    case simsimd_datatype_u64_k: return "Q";
    // Other:
    default: return "unknown";
    }
}

/// @brief Estimate the number of bytes per element for a given datatype.
/// @param dtype Logical datatype, can be complex.
/// @return Zero if the datatype is not supported, positive integer otherwise.
size_t bytes_per_datatype(simsimd_datatype_t dtype) {
    switch (dtype) {
    case simsimd_datatype_f64_k: return sizeof(simsimd_f64_t);
    case simsimd_datatype_f32_k: return sizeof(simsimd_f32_t);
    case simsimd_datatype_f16_k: return sizeof(simsimd_f16_t);
    case simsimd_datatype_bf16_k: return sizeof(simsimd_bf16_t);
    case simsimd_datatype_f64c_k: return sizeof(simsimd_f64_t) * 2;
    case simsimd_datatype_f32c_k: return sizeof(simsimd_f32_t) * 2;
    case simsimd_datatype_f16c_k: return sizeof(simsimd_f16_t) * 2;
    case simsimd_datatype_bf16c_k: return sizeof(simsimd_bf16_t) * 2;
    case simsimd_datatype_b8_k: return sizeof(simsimd_b8_t);
    case simsimd_datatype_i8_k: return sizeof(simsimd_i8_t);
    case simsimd_datatype_u8_k: return sizeof(simsimd_u8_t);
    case simsimd_datatype_i16_k: return sizeof(simsimd_i16_t);
    case simsimd_datatype_u16_k: return sizeof(simsimd_u16_t);
    case simsimd_datatype_i32_k: return sizeof(simsimd_i32_t);
    case simsimd_datatype_u32_k: return sizeof(simsimd_u32_t);
    case simsimd_datatype_i64_k: return sizeof(simsimd_i64_t);
    case simsimd_datatype_u64_k: return sizeof(simsimd_u64_t);
    default: return 0;
    }
}

/// @brief Copy a distance to a target datatype, downcasting if necessary.
/// @return 1 if the cast was successful, 0 if the target datatype is not supported.
int cast_distance(simsimd_distance_t distance, simsimd_datatype_t target_dtype, void *target_ptr, size_t offset) {
    switch (target_dtype) {
    case simsimd_datatype_f64c_k: ((simsimd_f64_t *)target_ptr)[offset] = (simsimd_f64_t)distance; return 1;
    case simsimd_datatype_f64_k: ((simsimd_f64_t *)target_ptr)[offset] = (simsimd_f64_t)distance; return 1;
    case simsimd_datatype_f32c_k: ((simsimd_f32_t *)target_ptr)[offset] = (simsimd_f32_t)distance; return 1;
    case simsimd_datatype_f32_k: ((simsimd_f32_t *)target_ptr)[offset] = (simsimd_f32_t)distance; return 1;
    case simsimd_datatype_f16c_k: simsimd_f32_to_f16(distance, (simsimd_f16_t *)target_ptr + offset); return 1;
    case simsimd_datatype_f16_k: simsimd_f32_to_f16(distance, (simsimd_f16_t *)target_ptr + offset); return 1;
    case simsimd_datatype_bf16c_k: simsimd_f32_to_bf16(distance, (simsimd_bf16_t *)target_ptr + offset); return 1;
    case simsimd_datatype_bf16_k: simsimd_f32_to_bf16(distance, (simsimd_bf16_t *)target_ptr + offset); return 1;
    case simsimd_datatype_i8_k: ((simsimd_i8_t *)target_ptr)[offset] = (simsimd_i8_t)distance; return 1;
    case simsimd_datatype_u8_k: ((simsimd_u8_t *)target_ptr)[offset] = (simsimd_u8_t)distance; return 1;
    case simsimd_datatype_i16_k: ((simsimd_i16_t *)target_ptr)[offset] = (simsimd_i16_t)distance; return 1;
    case simsimd_datatype_u16_k: ((simsimd_u16_t *)target_ptr)[offset] = (simsimd_u16_t)distance; return 1;
    case simsimd_datatype_i32_k: ((simsimd_i32_t *)target_ptr)[offset] = (simsimd_i32_t)distance; return 1;
    case simsimd_datatype_u32_k: ((simsimd_u32_t *)target_ptr)[offset] = (simsimd_u32_t)distance; return 1;
    case simsimd_datatype_i64_k: ((simsimd_i64_t *)target_ptr)[offset] = (simsimd_i64_t)distance; return 1;
    case simsimd_datatype_u64_k: ((simsimd_u64_t *)target_ptr)[offset] = (simsimd_u64_t)distance; return 1;
    default: return 0;
    }
}

simsimd_metric_kind_t python_string_to_metric_kind(char const *name) {
    if (same_string(name, "euclidean") || same_string(name, "l2")) return simsimd_metric_euclidean_k;
    else if (same_string(name, "sqeuclidean") || same_string(name, "l2sq"))
        return simsimd_metric_sqeuclidean_k;
    else if (same_string(name, "dot") || same_string(name, "inner"))
        return simsimd_metric_dot_k;
    else if (same_string(name, "vdot"))
        return simsimd_metric_vdot_k;
    else if (same_string(name, "cosine") || same_string(name, "cos"))
        return simsimd_metric_cosine_k;
    else if (same_string(name, "jaccard"))
        return simsimd_metric_jaccard_k;
    else if (same_string(name, "kullbackleibler") || same_string(name, "kl"))
        return simsimd_metric_kl_k;
    else if (same_string(name, "jensenshannon") || same_string(name, "js"))
        return simsimd_metric_js_k;
    else if (same_string(name, "hamming"))
        return simsimd_metric_hamming_k;
    else if (same_string(name, "jaccard"))
        return simsimd_metric_jaccard_k;
    else if (same_string(name, "bilinear"))
        return simsimd_metric_bilinear_k;
    else if (same_string(name, "mahalanobis"))
        return simsimd_metric_mahalanobis_k;
    else
        return simsimd_metric_unknown_k;
}

/// @brief Check if a metric is commutative, i.e., if `metric(a, b) == metric(b, a)`.
/// @return 1 if the metric is commutative, 0 otherwise.
int kernel_is_commutative(simsimd_metric_kind_t kind) {
    switch (kind) {
    case simsimd_metric_kl_k: return 0;
    case simsimd_metric_bilinear_k: return 0; //? The kernel is commutative if only the matrix is symmetric
    default: return 1;
    }
}

static char const doc_enable_capability[] = //
    "Enable a specific SIMD kernel family.\n\n"
    "Args:\n"
    "    capability (str): The name of the SIMD feature to enable (e.g., 'haswell').";

static PyObject *api_enable_capability(PyObject *self, PyObject *cap_name_obj) {
    char const *cap_name = PyUnicode_AsUTF8(cap_name_obj);
    if (!cap_name) {
        PyErr_SetString(PyExc_TypeError, "Capability name must be a string");
        return NULL;
    }

    if (same_string(cap_name, "neon")) { static_capabilities |= simsimd_cap_neon_k; }
    else if (same_string(cap_name, "neon_f16")) { static_capabilities |= simsimd_cap_neon_f16_k; }
    else if (same_string(cap_name, "neon_bf16")) { static_capabilities |= simsimd_cap_neon_bf16_k; }
    else if (same_string(cap_name, "neon_i8")) { static_capabilities |= simsimd_cap_neon_i8_k; }
    else if (same_string(cap_name, "sve")) { static_capabilities |= simsimd_cap_sve_k; }
    else if (same_string(cap_name, "sve_f16")) { static_capabilities |= simsimd_cap_sve_f16_k; }
    else if (same_string(cap_name, "sve_bf16")) { static_capabilities |= simsimd_cap_sve_bf16_k; }
    else if (same_string(cap_name, "sve_i8")) { static_capabilities |= simsimd_cap_sve_i8_k; }
    else if (same_string(cap_name, "haswell")) { static_capabilities |= simsimd_cap_haswell_k; }
    else if (same_string(cap_name, "skylake")) { static_capabilities |= simsimd_cap_skylake_k; }
    else if (same_string(cap_name, "ice")) { static_capabilities |= simsimd_cap_ice_k; }
    else if (same_string(cap_name, "genoa")) { static_capabilities |= simsimd_cap_genoa_k; }
    else if (same_string(cap_name, "sapphire")) { static_capabilities |= simsimd_cap_sapphire_k; }
    else if (same_string(cap_name, "turin")) { static_capabilities |= simsimd_cap_turin_k; }
    else if (same_string(cap_name, "sierra")) { static_capabilities |= simsimd_cap_sierra_k; }
    else if (same_string(cap_name, "serial")) {
        PyErr_SetString(PyExc_ValueError, "Can't change the serial functionality");
        return NULL;
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Unknown capability");
        return NULL;
    }

    Py_RETURN_NONE;
}

static char const doc_disable_capability[] = //
    "Disable a specific SIMD kernel family.\n\n"
    "Args:\n"
    "    capability (str): The name of the SIMD feature to disable (e.g., 'haswell').";

static PyObject *api_disable_capability(PyObject *self, PyObject *cap_name_obj) {
    char const *cap_name = PyUnicode_AsUTF8(cap_name_obj);
    if (!cap_name) {
        PyErr_SetString(PyExc_TypeError, "Capability name must be a string");
        return NULL;
    }

    if (same_string(cap_name, "neon")) { static_capabilities &= ~simsimd_cap_neon_k; }
    else if (same_string(cap_name, "neon_f16")) { static_capabilities &= ~simsimd_cap_neon_f16_k; }
    else if (same_string(cap_name, "neon_bf16")) { static_capabilities &= ~simsimd_cap_neon_bf16_k; }
    else if (same_string(cap_name, "neon_i8")) { static_capabilities &= ~simsimd_cap_neon_i8_k; }
    else if (same_string(cap_name, "sve")) { static_capabilities &= ~simsimd_cap_sve_k; }
    else if (same_string(cap_name, "sve_f16")) { static_capabilities &= ~simsimd_cap_sve_f16_k; }
    else if (same_string(cap_name, "sve_bf16")) { static_capabilities &= ~simsimd_cap_sve_bf16_k; }
    else if (same_string(cap_name, "sve_i8")) { static_capabilities &= ~simsimd_cap_sve_i8_k; }
    else if (same_string(cap_name, "haswell")) { static_capabilities &= ~simsimd_cap_haswell_k; }
    else if (same_string(cap_name, "skylake")) { static_capabilities &= ~simsimd_cap_skylake_k; }
    else if (same_string(cap_name, "ice")) { static_capabilities &= ~simsimd_cap_ice_k; }
    else if (same_string(cap_name, "genoa")) { static_capabilities &= ~simsimd_cap_genoa_k; }
    else if (same_string(cap_name, "sapphire")) { static_capabilities &= ~simsimd_cap_sapphire_k; }
    else if (same_string(cap_name, "turin")) { static_capabilities &= ~simsimd_cap_turin_k; }
    else if (same_string(cap_name, "sierra")) { static_capabilities &= ~simsimd_cap_sierra_k; }
    else if (same_string(cap_name, "serial")) {
        PyErr_SetString(PyExc_ValueError, "Can't change the serial functionality");
        return NULL;
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Unknown capability");
        return NULL;
    }

    Py_RETURN_NONE;
}

static char const doc_get_capabilities[] = //
    "Get the current hardware SIMD capabilities as a dictionary of feature flags.\n"
    "On x86 includes: 'serial', 'haswell', 'skylake', 'ice', 'genoa', 'sapphire', 'turin'.\n"
    "On Arm includes: 'serial', 'neon', 'sve', 'sve2', and their extensions.\n";

static PyObject *api_get_capabilities(PyObject *self) {
    simsimd_capability_t caps = static_capabilities;
    PyObject *cap_dict = PyDict_New();
    if (!cap_dict) return NULL;

#define ADD_CAP(name) PyDict_SetItemString(cap_dict, #name, PyBool_FromLong((caps) & simsimd_cap_##name##_k))

    ADD_CAP(serial);
    ADD_CAP(neon);
    ADD_CAP(sve);
    ADD_CAP(neon_f16);
    ADD_CAP(sve_f16);
    ADD_CAP(neon_bf16);
    ADD_CAP(sve_bf16);
    ADD_CAP(neon_i8);
    ADD_CAP(sve_i8);
    ADD_CAP(haswell);
    ADD_CAP(skylake);
    ADD_CAP(ice);
    ADD_CAP(genoa);
    ADD_CAP(sapphire);
    ADD_CAP(turin);
    ADD_CAP(sierra);

#undef ADD_CAP

    return cap_dict;
}

/// @brief Unpacks a Python tensor object into a C structure.
/// @return 1 on success, 0 otherwise.
int parse_tensor(PyObject *tensor, Py_buffer *buffer, TensorArgument *parsed) {
    if (PyObject_GetBuffer(tensor, buffer, PyBUF_STRIDES | PyBUF_FORMAT) != 0) {
        PyErr_SetString(PyExc_TypeError, "arguments must support buffer protocol");
        return 0;
    }
    // In case you are debugging some new obscure format string :)
    // printf("buffer format is %s\n", buffer->format);
    // printf("buffer ndim is %d\n", buffer->ndim);
    // printf("buffer shape is %d\n", buffer->shape[0]);
    // printf("buffer shape is %d\n", buffer->shape[1]);
    // printf("buffer itemsize is %d\n", buffer->itemsize);
    parsed->start = buffer->buf;
    parsed->datatype = python_string_to_datatype(buffer->format);
    if (parsed->datatype == simsimd_datatype_unknown_k) {
        PyErr_Format(PyExc_ValueError, "Unsupported '%s' datatype specifier", buffer->format);
        PyBuffer_Release(buffer);
        return 0;
    }

    parsed->rank = buffer->ndim;
    if (buffer->ndim == 1) {
        if (buffer->strides[0] > buffer->itemsize) {
            PyErr_SetString(PyExc_ValueError, "Input vectors must be contiguous, check with `X.__array_interface__`");
            PyBuffer_Release(buffer);
            return 0;
        }
        parsed->dimensions = buffer->shape[0];
        parsed->count = 1;
        parsed->stride = 0;
    }
    else if (buffer->ndim == 2) {
        if (buffer->strides[1] > buffer->itemsize) {
            PyErr_SetString(PyExc_ValueError, "Input vectors must be contiguous, check with `X.__array_interface__`");
            PyBuffer_Release(buffer);
            return 0;
        }
        parsed->dimensions = buffer->shape[1];
        parsed->count = buffer->shape[0];
        parsed->stride = buffer->strides[0];
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Input tensors must be 1D or 2D");
        PyBuffer_Release(buffer);
        return 0;
    }

    return 1;
}

static int DistancesTensor_getbuffer(PyObject *export_from, Py_buffer *view, int flags) {
    DistancesTensor *tensor = (DistancesTensor *)export_from;
    size_t const total_items = tensor->shape[0] * tensor->shape[1];
    size_t const item_size = bytes_per_datatype(tensor->datatype);

    view->buf = &tensor->start[0];
    view->obj = (PyObject *)tensor;
    view->len = item_size * total_items;
    view->readonly = 0;
    view->itemsize = (Py_ssize_t)item_size;
    view->format = datatype_to_python_string(tensor->datatype);
    view->ndim = (int)tensor->dimensions;
    view->shape = &tensor->shape[0];
    view->strides = &tensor->strides[0];
    view->suboffsets = NULL;
    view->internal = NULL;

    Py_INCREF(tensor);
    return 0;
}

static void DistancesTensor_releasebuffer(PyObject *export_from, Py_buffer *view) {
    // This function MUST NOT decrement view->obj, since that is done automatically in PyBuffer_Release().
    // https://docs.python.org/3/c-api/typeobj.html#c.PyBufferProcs.bf_releasebuffer
}

static PyObject *implement_dense_metric( //
    simsimd_metric_kind_t metric_kind,   //
    PyObject *const *args, Py_ssize_t const positional_args_count, PyObject *args_names_tuple) {

    PyObject *return_obj = NULL;

    // This function accepts up to 5 arguments:
    PyObject *a_obj = NULL;         // Required object, positional-only
    PyObject *b_obj = NULL;         // Required object, positional-only
    PyObject *dtype_obj = NULL;     // Optional object, "dtype" keyword or positional
    PyObject *out_obj = NULL;       // Optional object, "out" keyword-only
    PyObject *out_dtype_obj = NULL; // Optional object, "out_dtype" keyword-only

    // Once parsed, the arguments will be stored in these variables:
    char const *dtype_str = NULL, *out_dtype_str = NULL;
    simsimd_datatype_t dtype = simsimd_datatype_unknown_k, out_dtype = simsimd_datatype_unknown_k;
    Py_buffer a_buffer, b_buffer, out_buffer;
    TensorArgument a_parsed, b_parsed, out_parsed;
    memset(&a_buffer, 0, sizeof(Py_buffer));
    memset(&b_buffer, 0, sizeof(Py_buffer));
    memset(&out_buffer, 0, sizeof(Py_buffer));

    // Parse the arguments
    Py_ssize_t const args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
    Py_ssize_t const args_count = positional_args_count + args_names_count;
    if (args_count < 2 || args_count > 5) {
        PyErr_Format(PyExc_TypeError, "Function expects 2-5 arguments, got %zd", args_count);
        return NULL;
    }
    if (positional_args_count > 3) {
        PyErr_Format(PyExc_TypeError, "Only first 3 arguments can be positional, received %zd", positional_args_count);
        return NULL;
    }

    // Positional-only arguments (first and second matrix)
    a_obj = args[0];
    b_obj = args[1];

    // Positional or keyword arguments (dtype)
    if (positional_args_count == 3) dtype_obj = args[2];

    // The rest of the arguments must be checked in the keyword dictionary:
    for (Py_ssize_t args_names_tuple_progress = 0, args_progress = positional_args_count;
         args_names_tuple_progress < args_names_count; ++args_progress, ++args_names_tuple_progress) {
        PyObject *const key = PyTuple_GetItem(args_names_tuple, args_names_tuple_progress);
        PyObject *const value = args[args_progress];
        if (PyUnicode_CompareWithASCIIString(key, "dtype") == 0 && !dtype_obj) { dtype_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out") == 0 && !out_obj) { out_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out_dtype") == 0 && !out_dtype_obj) { out_dtype_obj = value; }
        else {
            PyErr_Format(PyExc_TypeError, "Got unexpected keyword argument: %S", key);
            return NULL;
        }
    }

    // Convert `dtype_obj` to `dtype_str` and to `dtype`
    if (dtype_obj) {
        dtype_str = PyUnicode_AsUTF8(dtype_obj);
        if (!dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'dtype' to be a string");
            return NULL;
        }
        dtype = python_string_to_datatype(dtype_str);
        if (dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'dtype'");
            return NULL;
        }
    }

    // Convert `out_dtype_obj` to `out_dtype_str` and to `out_dtype`
    if (out_dtype_obj) {
        out_dtype_str = PyUnicode_AsUTF8(out_dtype_obj);
        if (!out_dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'out_dtype' to be a string");
            return NULL;
        }
        out_dtype = python_string_to_datatype(out_dtype_str);
        if (out_dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'out_dtype'");
            return NULL;
        }
    }

    // Convert `a_obj` to `a_buffer` and to `a_parsed`. Same for `b_obj` and `out_obj`.
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed)) return NULL;
    if (out_obj && !parse_tensor(out_obj, &out_buffer, &out_parsed)) return NULL;

    // Check dimensions
    if (a_parsed.dimensions != b_parsed.dimensions) {
        PyErr_SetString(PyExc_ValueError, "Vector dimensions don't match");
        goto cleanup;
    }
    if (a_parsed.count == 0 || b_parsed.count == 0) {
        PyErr_SetString(PyExc_ValueError, "Collections can't be empty");
        goto cleanup;
    }
    if (a_parsed.count > 1 && b_parsed.count > 1 && a_parsed.count != b_parsed.count) {
        PyErr_SetString(PyExc_ValueError, "Collections must have the same number of elements or just one element");
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype || //
        a_parsed.datatype == simsimd_datatype_unknown_k || b_parsed.datatype == simsimd_datatype_unknown_k) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }
    if (dtype == simsimd_datatype_unknown_k) dtype = a_parsed.datatype;

    // Inference order for the output type:
    // 1. `out_dtype` named argument, if defined
    // 2. `out.dtype` attribute, if `out` is passed
    // 3. double precision float (or its complex variant)
    if (out_dtype == simsimd_datatype_unknown_k) {
        if (out_obj) { out_dtype = out_parsed.datatype; }
        else { out_dtype = is_complex(dtype) ? simsimd_datatype_f64c_k : simsimd_datatype_f64_k; }
    }

    // Make sure the return datatype is complex if the input datatype is complex, and the same for real numbers
    if (out_dtype != simsimd_datatype_unknown_k) {
        if (is_complex(dtype) != is_complex(out_dtype)) {
            PyErr_SetString(
                PyExc_ValueError,
                "If the input datatype is complex, the return datatype must be complex, and same for real.");
            goto cleanup;
        }
    }

    // Check if the downcasting to provided datatype is supported
    {
        char returned_buffer_example[8];
        if (!cast_distance(0, out_dtype, &returned_buffer_example, 0)) {
            PyErr_SetString(PyExc_ValueError, "Exporting to the provided datatype is not supported");
            goto cleanup;
        }
    }

    // Look up the metric and the capability
    simsimd_metric_dense_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError,
            "Unsupported metric '%c' and datatype combination across vectors ('%s'/'%s' and '%s'/'%s') and "
            "`dtype` override ('%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            b_buffer.format ? b_buffer.format : "nil", datatype_to_python_string(b_parsed.datatype), //
            dtype_str ? dtype_str : "nil", datatype_to_python_string(dtype));
        goto cleanup;
    }

    // If the distance is computed between two vectors, rather than matrices, return a scalar
    int const dtype_is_complex = is_complex(dtype);
    if (a_parsed.rank == 1 && b_parsed.rank == 1) {
        simsimd_distance_t distances[2];
        metric(a_parsed.start, b_parsed.start, a_parsed.dimensions, distances);
        return_obj =         //
            dtype_is_complex //
                ? PyComplex_FromDoubles(distances[0], distances[1])
                : PyFloat_FromDouble(distances[0]);
        goto cleanup;
    }

    // In some batch requests we may be computing the distance from multiple vectors to one,
    // so the stride must be set to zero avoid illegal memory access
    if (a_parsed.count == 1) a_parsed.stride = 0;
    if (b_parsed.count == 1) b_parsed.stride = 0;

    // We take the maximum of the two counts, because if only one entry is present in one of the arrays,
    // all distances will be computed against that single entry.
    size_t const count_pairs = a_parsed.count > b_parsed.count ? a_parsed.count : b_parsed.count;
    size_t const components_per_pair = dtype_is_complex ? 2 : 1;
    size_t const count_components = count_pairs * components_per_pair;
    char *distances_start = NULL;
    size_t distances_stride_bytes = 0;

    // Allocate the output matrix if it wasn't provided
    if (!out_obj) {
        DistancesTensor *distances_obj =
            PyObject_NewVar(DistancesTensor, &DistancesTensorType, count_components * bytes_per_datatype(out_dtype));
        if (!distances_obj) {
            PyErr_NoMemory();
            goto cleanup;
        }

        // Initialize the object
        distances_obj->datatype = out_dtype;
        distances_obj->dimensions = 1;
        distances_obj->shape[0] = count_pairs;
        distances_obj->shape[1] = 1;
        distances_obj->strides[0] = bytes_per_datatype(out_dtype);
        distances_obj->strides[1] = 0;
        return_obj = (PyObject *)distances_obj;
        distances_start = (char *)&distances_obj->start[0];
        distances_stride_bytes = distances_obj->strides[0];
    }
    else {
        if (bytes_per_datatype(out_parsed.datatype) != bytes_per_datatype(out_dtype)) {
            PyErr_Format( //
                PyExc_LookupError,
                "Output tensor scalar type must be compatible with the output type ('%s' and '%s'/'%s')",
                datatype_to_python_string(out_dtype), out_buffer.format ? out_buffer.format : "nil",
                datatype_to_python_string(out_parsed.datatype));
            goto cleanup;
        }
        distances_start = (char *)&out_parsed.start[0];
        distances_stride_bytes = out_buffer.strides[0];
        //? Logic suggests to return `None` in in-place mode...
        //? SciPy decided differently.
        return_obj = Py_None;
    }

    // Compute the distances
    for (size_t i = 0; i < count_pairs; ++i) {
        simsimd_distance_t result[2];
        metric(                                   //
            a_parsed.start + i * a_parsed.stride, //
            b_parsed.start + i * b_parsed.stride, //
            a_parsed.dimensions,                  //
            (simsimd_distance_t *)&result);

        // Export out:
        cast_distance(result[0], out_dtype, distances_start + i * distances_stride_bytes, 0);
        if (dtype_is_complex) cast_distance(result[1], out_dtype, distances_start + i * distances_stride_bytes, 1);
    }

cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    PyBuffer_Release(&out_buffer);
    return return_obj;
}

static PyObject *implement_curved_metric( //
    simsimd_metric_kind_t metric_kind,    //
    PyObject *const *args, Py_ssize_t const positional_args_count, PyObject *args_names_tuple) {

    PyObject *return_obj = NULL;

    // This function accepts up to 6 arguments:
    PyObject *a_obj = NULL;     // Required object, positional-only
    PyObject *b_obj = NULL;     // Required object, positional-only
    PyObject *c_obj = NULL;     // Required object, positional-only
    PyObject *dtype_obj = NULL; // Optional object, "dtype" keyword or positional

    // Once parsed, the arguments will be stored in these variables:
    char const *dtype_str = NULL;
    simsimd_datatype_t dtype = simsimd_datatype_unknown_k;
    Py_buffer a_buffer, b_buffer, c_buffer;
    TensorArgument a_parsed, b_parsed, c_parsed;
    memset(&a_buffer, 0, sizeof(Py_buffer));
    memset(&b_buffer, 0, sizeof(Py_buffer));
    memset(&c_buffer, 0, sizeof(Py_buffer));

    // Parse the arguments
    Py_ssize_t const args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
    Py_ssize_t const args_count = positional_args_count + args_names_count;
    if (args_count < 3 || args_count > 6) {
        PyErr_Format(PyExc_TypeError, "Function expects 2-6 arguments, got %zd", args_count);
        return NULL;
    }
    if (positional_args_count > 4) {
        PyErr_Format(PyExc_TypeError, "Only first 4 arguments can be positional, received %zd", positional_args_count);
        return NULL;
    }

    // Positional-only arguments (first, second, and third matrix)
    a_obj = args[0];
    b_obj = args[1];
    c_obj = args[2];

    // Positional or keyword arguments (dtype)
    if (positional_args_count == 4) dtype_obj = args[3];

    // The rest of the arguments must be checked in the keyword dictionary:
    for (Py_ssize_t args_names_tuple_progress = 0, args_progress = positional_args_count;
         args_names_tuple_progress < args_names_count; ++args_progress, ++args_names_tuple_progress) {
        PyObject *const key = PyTuple_GetItem(args_names_tuple, args_names_tuple_progress);
        PyObject *const value = args[args_progress];
        if (PyUnicode_CompareWithASCIIString(key, "dtype") == 0 && !dtype_obj) { dtype_obj = value; }
        else {
            PyErr_Format(PyExc_TypeError, "Got unexpected keyword argument: %S", key);
            return NULL;
        }
    }

    // Convert `dtype_obj` to `dtype_str` and to `dtype`
    if (dtype_obj) {
        dtype_str = PyUnicode_AsUTF8(dtype_obj);
        if (!dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'dtype' to be a string");
            return NULL;
        }
        dtype = python_string_to_datatype(dtype_str);
        if (dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'dtype'");
            return NULL;
        }
    }

    // Convert `a_obj` to `a_buffer` and to `a_parsed`. Same for `b_obj` and `out_obj`.
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed) ||
        !parse_tensor(c_obj, &c_buffer, &c_parsed))
        return NULL;

    // Check dimensions
    if (a_parsed.rank != 1 || b_parsed.rank != 1) {
        PyErr_SetString(PyExc_ValueError, "First and second argument must be vectors");
        goto cleanup;
    }
    if (c_parsed.rank != 2) {
        PyErr_SetString(PyExc_ValueError, "Third argument must be a matrix (rank-2 tensor)");
        goto cleanup;
    }
    if (a_parsed.count == 0 || b_parsed.count == 0) {
        PyErr_SetString(PyExc_ValueError, "Collections can't be empty");
        goto cleanup;
    }
    if (a_parsed.count > 1 && b_parsed.count > 1 && a_parsed.count != b_parsed.count) {
        PyErr_SetString(PyExc_ValueError, "Collections must have the same number of elements or just one element");
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype || a_parsed.datatype != c_parsed.datatype ||
        a_parsed.datatype == simsimd_datatype_unknown_k || b_parsed.datatype == simsimd_datatype_unknown_k ||
        c_parsed.datatype == simsimd_datatype_unknown_k) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }
    if (dtype == simsimd_datatype_unknown_k) dtype = a_parsed.datatype;

    // Look up the metric and the capability
    simsimd_metric_curved_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError,
            "Unsupported metric '%c' and datatype combination across vectors ('%s'/'%s' and '%s'/'%s'), "
            "tensor ('%s'/'%s'), and `dtype` override ('%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            b_buffer.format ? b_buffer.format : "nil", datatype_to_python_string(b_parsed.datatype), //
            c_buffer.format ? c_buffer.format : "nil", datatype_to_python_string(c_parsed.datatype), //
            dtype_str ? dtype_str : "nil", datatype_to_python_string(dtype));
        goto cleanup;
    }

    // If the distance is computed between two vectors, rather than matrices, return a scalar
    int const dtype_is_complex = is_complex(dtype);
    simsimd_distance_t distances[2];
    metric(a_parsed.start, b_parsed.start, c_parsed.start, a_parsed.dimensions, &distances[0]);
    return_obj =         //
        dtype_is_complex //
            ? PyComplex_FromDoubles(distances[0], distances[1])
            : PyFloat_FromDouble(distances[0]);

cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    PyBuffer_Release(&c_buffer);
    return return_obj;
}

static PyObject *implement_sparse_metric( //
    simsimd_metric_kind_t metric_kind,    //
    PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "Function expects only 2 arguments");
        return NULL;
    }

    PyObject *return_obj = NULL;
    PyObject *a_obj = args[0];
    PyObject *b_obj = args[1];

    Py_buffer a_buffer, b_buffer;
    TensorArgument a_parsed, b_parsed;
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed)) return NULL;

    // Check dimensions
    if (a_parsed.rank != 1 || b_parsed.rank != 1) {
        PyErr_SetString(PyExc_ValueError, "First and second argument must be vectors");
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype && a_parsed.datatype != simsimd_datatype_unknown_k &&
        b_parsed.datatype != simsimd_datatype_unknown_k) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }

    simsimd_datatype_t dtype = a_parsed.datatype;
    simsimd_metric_sparse_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError, "Unsupported metric '%c' and datatype combination ('%s'/'%s' and '%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            b_buffer.format ? b_buffer.format : "nil", datatype_to_python_string(b_parsed.datatype));
        goto cleanup;
    }

    simsimd_distance_t distance;
    metric(a_parsed.start, b_parsed.start, a_parsed.dimensions, b_parsed.dimensions, &distance);
    return_obj = PyFloat_FromDouble(distance);

cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    return return_obj;
}

static PyObject *implement_cdist(                        //
    PyObject *a_obj, PyObject *b_obj, PyObject *out_obj, //
    simsimd_metric_kind_t metric_kind, size_t threads,   //
    simsimd_datatype_t dtype, simsimd_datatype_t out_dtype) {

    PyObject *return_obj = NULL;

    Py_buffer a_buffer, b_buffer, out_buffer;
    TensorArgument a_parsed, b_parsed, out_parsed;
    memset(&a_buffer, 0, sizeof(Py_buffer));
    memset(&b_buffer, 0, sizeof(Py_buffer));
    memset(&out_buffer, 0, sizeof(Py_buffer));

    // Error will be set by `parse_tensor` if the input is invalid
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed)) return NULL;
    if (out_obj && !parse_tensor(out_obj, &out_buffer, &out_parsed)) return NULL;

    // Check dimensions
    if (a_parsed.dimensions != b_parsed.dimensions) {
        PyErr_Format(PyExc_ValueError, "Vector dimensions don't match (%z != %z)", a_parsed.dimensions,
                     b_parsed.dimensions);
        goto cleanup;
    }
    if (a_parsed.count == 0 || b_parsed.count == 0) {
        PyErr_SetString(PyExc_ValueError, "Collections can't be empty");
        goto cleanup;
    }
    if (out_obj &&
        (out_parsed.rank != 2 || out_buffer.shape[0] != a_parsed.count || out_buffer.shape[1] != b_parsed.count)) {
        PyErr_Format(PyExc_ValueError, "Output tensor must have shape (%z, %z)", a_parsed.count, b_parsed.count);
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype || //
        a_parsed.datatype == simsimd_datatype_unknown_k || b_parsed.datatype == simsimd_datatype_unknown_k) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }
    if (dtype == simsimd_datatype_unknown_k) dtype = a_parsed.datatype;

    // Inference order for the output type:
    // 1. `out_dtype` named argument, if defined
    // 2. `out.dtype` attribute, if `out` is passed
    // 3. double precision float (or its complex variant)
    if (out_dtype == simsimd_datatype_unknown_k) {
        if (out_obj) { out_dtype = out_parsed.datatype; }
        else { out_dtype = is_complex(dtype) ? simsimd_datatype_f64c_k : simsimd_datatype_f64_k; }
    }

    // Make sure the return datatype is complex if the input datatype is complex, and the same for real numbers
    if (out_dtype != simsimd_datatype_unknown_k) {
        if (is_complex(dtype) != is_complex(out_dtype)) {
            PyErr_SetString(
                PyExc_ValueError,
                "If the input datatype is complex, the return datatype must be complex, and same for real.");
            goto cleanup;
        }
    }

    // Check if the downcasting to provided datatype is supported
    {
        char returned_buffer_example[8];
        if (!cast_distance(0, out_dtype, &returned_buffer_example, 0)) {
            PyErr_SetString(PyExc_ValueError, "Exporting to the provided datatype is not supported");
            goto cleanup;
        }
    }

    // Look up the metric and the capability
    simsimd_metric_dense_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError, "Unsupported metric '%c' and datatype combination ('%s'/'%s' and '%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            b_buffer.format ? b_buffer.format : "nil", datatype_to_python_string(b_parsed.datatype));
        goto cleanup;
    }

    // If the distance is computed between two vectors, rather than matrices, return a scalar
    int const dtype_is_complex = is_complex(dtype);
    if (a_parsed.rank == 1 && b_parsed.rank == 1) {
        simsimd_distance_t distances[2];
        metric(a_parsed.start, b_parsed.start, a_parsed.dimensions, distances);
        return_obj =         //
            dtype_is_complex //
                ? PyComplex_FromDoubles(distances[0], distances[1])
                : PyFloat_FromDouble(distances[0]);
        goto cleanup;
    }

#ifdef __linux__
#ifdef _OPENMP
    if (threads == 0) threads = omp_get_num_procs();
    omp_set_num_threads(threads);
#endif
#endif

    size_t const count_pairs = a_parsed.count * b_parsed.count;
    size_t const components_per_pair = dtype_is_complex ? 2 : 1;
    size_t const count_components = count_pairs * components_per_pair;
    char *distances_start = NULL;
    size_t distances_rows_stride_bytes = 0;
    size_t distances_cols_stride_bytes = 0;

    // Allocate the output matrix if it wasn't provided
    if (!out_obj) {

        DistancesTensor *distances_obj =
            PyObject_NewVar(DistancesTensor, &DistancesTensorType, count_components * bytes_per_datatype(out_dtype));
        if (!distances_obj) {
            PyErr_NoMemory();
            goto cleanup;
        }

        // Initialize the object
        distances_obj->datatype = out_dtype;
        distances_obj->dimensions = 2;
        distances_obj->shape[0] = a_parsed.count;
        distances_obj->shape[1] = b_parsed.count;
        distances_obj->strides[0] = b_parsed.count * bytes_per_datatype(distances_obj->datatype);
        distances_obj->strides[1] = bytes_per_datatype(distances_obj->datatype);
        return_obj = (PyObject *)distances_obj;
        distances_start = (char *)&distances_obj->start[0];
        distances_rows_stride_bytes = distances_obj->strides[0];
        distances_cols_stride_bytes = distances_obj->strides[1];
    }
    else {
        if (bytes_per_datatype(out_parsed.datatype) != bytes_per_datatype(out_dtype)) {
            PyErr_Format( //
                PyExc_LookupError,
                "Output tensor scalar type must be compatible with the output type ('%s' and '%s'/'%s')",
                datatype_to_python_string(out_dtype), out_buffer.format ? out_buffer.format : "nil",
                datatype_to_python_string(out_parsed.datatype));
            goto cleanup;
        }
        distances_start = (char *)&out_parsed.start[0];
        distances_rows_stride_bytes = out_buffer.strides[0];
        distances_cols_stride_bytes = out_buffer.strides[1];
        //? Logic suggests to return `None` in in-place mode...
        //? SciPy decided differently.
        return_obj = Py_None;
    }

    // Assuming most of our kernels are symmetric, we only need to compute the upper triangle
    // if we are computing all pairwise distances within the same set.
    int const is_symmetric = kernel_is_commutative(metric_kind) && a_parsed.start == b_parsed.start &&
                             a_parsed.stride == b_parsed.stride && a_parsed.count == b_parsed.count;
#pragma omp parallel for collapse(2)
    for (size_t i = 0; i < a_parsed.count; ++i)
        for (size_t j = 0; j < b_parsed.count; ++j) {
            if (is_symmetric && i > j) continue;

            // Export into an on-stack buffer and then copy to the output
            simsimd_distance_t result[2];
            metric(                                   //
                a_parsed.start + i * a_parsed.stride, //
                b_parsed.start + j * b_parsed.stride, //
                a_parsed.dimensions,                  //
                (simsimd_distance_t *)&result         //
            );

            // Export into both the lower and upper triangle
            if (1)
                cast_distance(result[0], out_dtype,
                              distances_start + i * distances_rows_stride_bytes + j * distances_cols_stride_bytes, 0);
            if (dtype_is_complex)
                cast_distance(result[1], out_dtype,
                              distances_start + i * distances_rows_stride_bytes + j * distances_cols_stride_bytes, 1);
            if (is_symmetric)
                cast_distance(result[0], out_dtype,
                              distances_start + j * distances_rows_stride_bytes + i * distances_cols_stride_bytes, 0);
            if (is_symmetric && dtype_is_complex)
                cast_distance(result[1], out_dtype,
                              distances_start + j * distances_rows_stride_bytes + i * distances_cols_stride_bytes, 1);
        }

cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    PyBuffer_Release(&out_buffer);
    return return_obj;
}

static PyObject *implement_pointer_access(simsimd_metric_kind_t metric_kind, PyObject *dtype_obj) {
    char const *dtype_name = PyUnicode_AsUTF8(dtype_obj);
    if (!dtype_name) {
        PyErr_SetString(PyExc_TypeError, "Data-type name must be a string");
        return NULL;
    }

    simsimd_datatype_t datatype = python_string_to_datatype(dtype_name);
    if (!datatype) { // Check the actual variable here instead of dtype_name
        PyErr_SetString(PyExc_ValueError, "Unsupported type");
        return NULL;
    }

    simsimd_kernel_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, datatype, static_capabilities, simsimd_cap_any_k, &metric, &capability);
    if (metric == NULL) {
        PyErr_SetString(PyExc_LookupError, "No such metric");
        return NULL;
    }

    return PyLong_FromUnsignedLongLong((unsigned long long)metric);
}

static char const doc_cdist[] = //
    "Compute pairwise distances between two sets of input matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First matrix.\n"
    "    b (NDArray): Second matrix.\n"
    "    metric (str, optional): Distance metric to use (e.g., 'sqeuclidean', 'cosine').\n"
    "    out (NDArray, optional): Output matrix to store the result.\n"
    "    dtype (Union[IntegralType, FloatType, ComplexType], optional): Override the presumed input type name.\n"
    "    out_dtype (Union[FloatType, ComplexType], optional): Result type, default is 'float64'.\n\n"
    "    threads (int, optional): Number of threads to use (default is 1).\n"
    "Returns:\n"
    "    DistancesTensor: Pairwise distances between all inputs.\n\n"
    "Equivalent to: `scipy.spatial.distance.cdist`.\n"
    "Signature:\n"
    "    >>> def cdist(a, b, /, metric, *, dtype, out, out_dtype, threads) -> Optional[DistancesTensor]: ...";

static PyObject *api_cdist( //
    PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count, PyObject *args_names_tuple) {

    // This function accepts up to 7 arguments - more than SciPy:
    // https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.cdist.html
    PyObject *a_obj = NULL;         // Required object, positional-only
    PyObject *b_obj = NULL;         // Required object, positional-only
    PyObject *metric_obj = NULL;    // Optional string, "metric" keyword or positional
    PyObject *out_obj = NULL;       // Optional object, "out" keyword-only
    PyObject *dtype_obj = NULL;     // Optional string, "dtype" keyword-only
    PyObject *out_dtype_obj = NULL; // Optional string, "out_dtype" keyword-only
    PyObject *threads_obj = NULL;   // Optional integer, "threads" keyword-only

    // Once parsed, the arguments will be stored in these variables:
    unsigned long long threads = 1;
    char const *dtype_str = NULL, *out_dtype_str = NULL;
    simsimd_datatype_t dtype = simsimd_datatype_unknown_k, out_dtype = simsimd_datatype_unknown_k;

    /// Same default as in SciPy:
    /// https://docs.scipy.org/doc/scipy-1.11.4/reference/generated/scipy.spatial.distance.cdist.html
    simsimd_metric_kind_t metric_kind = simsimd_metric_euclidean_k;
    char const *metric_str = NULL;

    // Parse the arguments
    Py_ssize_t const args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
    Py_ssize_t const args_count = positional_args_count + args_names_count;
    if (args_count < 2 || args_count > 7) {
        PyErr_Format(PyExc_TypeError, "Function expects 2-7 arguments, got %zd", args_count);
        return NULL;
    }
    if (positional_args_count > 3) {
        PyErr_Format(PyExc_TypeError, "Only first 3 arguments can be positional, received %zd", positional_args_count);
        return NULL;
    }

    // Positional-only arguments (first and second matrix)
    a_obj = args[0];
    b_obj = args[1];

    // Positional or keyword arguments (metric)
    if (positional_args_count == 3) metric_obj = args[2];

    // The rest of the arguments must be checked in the keyword dictionary:
    for (Py_ssize_t args_names_tuple_progress = 0, args_progress = positional_args_count;
         args_names_tuple_progress < args_names_count; ++args_progress, ++args_names_tuple_progress) {
        PyObject *const key = PyTuple_GetItem(args_names_tuple, args_names_tuple_progress);
        PyObject *const value = args[args_progress];
        if (PyUnicode_CompareWithASCIIString(key, "dtype") == 0 && !dtype_obj) { dtype_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out") == 0 && !out_obj) { out_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out_dtype") == 0 && !out_dtype_obj) { out_dtype_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "threads") == 0 && !threads_obj) { threads_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "metric") == 0 && !metric_obj) { metric_obj = value; }
        else {
            PyErr_Format(PyExc_TypeError, "Got unexpected keyword argument: %S", key);
            return NULL;
        }
    }

    // Convert `metric_obj` to `metric_str` and to `metric_kind`
    if (metric_obj) {
        metric_str = PyUnicode_AsUTF8(metric_obj);
        if (!metric_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'metric' to be a string");
            return NULL;
        }
        metric_kind = python_string_to_metric_kind(metric_str);
        if (metric_kind == simsimd_metric_unknown_k) {
            PyErr_SetString(PyExc_LookupError, "Unsupported metric");
            return NULL;
        }
    }

    // Convert `threads_obj` to `threads` integer
    if (threads_obj) threads = PyLong_AsSize_t(threads_obj);
    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_TypeError, "Expected 'threads' to be an unsigned integer");
        return NULL;
    }

    // Convert `dtype_obj` to `dtype_str` and to `dtype`
    if (dtype_obj) {
        dtype_str = PyUnicode_AsUTF8(dtype_obj);
        if (!dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'dtype' to be a string");
            return NULL;
        }
        dtype = python_string_to_datatype(dtype_str);
        if (dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'dtype'");
            return NULL;
        }
    }

    // Convert `out_dtype_obj` to `out_dtype_str` and to `out_dtype`
    if (out_dtype_obj) {
        out_dtype_str = PyUnicode_AsUTF8(out_dtype_obj);
        if (!out_dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'out_dtype' to be a string");
            return NULL;
        }
        out_dtype = python_string_to_datatype(out_dtype_str);
        if (out_dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'out_dtype'");
            return NULL;
        }
    }

    return implement_cdist(a_obj, b_obj, out_obj, metric_kind, threads, dtype, out_dtype);
}

static char const doc_l2_pointer[] = "Get (int) pointer to the `simsimd.l2` kernel.";
static PyObject *api_l2_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_l2_k, dtype_obj);
}
static char const doc_l2sq_pointer[] = "Get (int) pointer to the `simsimd.l2sq` kernel.";
static PyObject *api_l2sq_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_l2sq_k, dtype_obj);
}
static char const doc_cos_pointer[] = "Get (int) pointer to the `simsimd.cos` kernel.";
static PyObject *api_cos_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_cos_k, dtype_obj);
}
static char const doc_dot_pointer[] = "Get (int) pointer to the `simsimd.dot` kernel.";
static PyObject *api_dot_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_dot_k, dtype_obj);
}
static char const doc_vdot_pointer[] = "Get (int) pointer to the `simsimd.vdot` kernel.";
static PyObject *api_vdot_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_vdot_k, dtype_obj);
}
static char const doc_kl_pointer[] = "Get (int) pointer to the `simsimd.kl` kernel.";
static PyObject *api_kl_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_kl_k, dtype_obj);
}
static char const doc_js_pointer[] = "Get (int) pointer to the `simsimd.js` kernel.";
static PyObject *api_js_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_js_k, dtype_obj);
}
static char const doc_hamming_pointer[] = "Get (int) pointer to the `simsimd.hamming` kernel.";
static PyObject *api_hamming_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_hamming_k, dtype_obj);
}
static char const doc_jaccard_pointer[] = "Get (int) pointer to the `simsimd.jaccard` kernel.";
static PyObject *api_jaccard_pointer(PyObject *self, PyObject *dtype_obj) {
    return implement_pointer_access(simsimd_metric_jaccard_k, dtype_obj);
}

static char const doc_l2[] = //
    "Compute Euclidean (L2) distances between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First matrix or vector.\n"
    "    b (NDArray): Second matrix or vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `scipy.spatial.distance.euclidean`.\n"
    "Signature:\n"
    "    >>> def euclidean(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_l2(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                        PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_l2_k, args, positional_args_count, args_names_tuple);
}

static char const doc_l2sq[] = //
    "Compute squared Euclidean (L2) distances between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First matrix or vector.\n"
    "    b (NDArray): Second matrix or vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `scipy.spatial.distance.sqeuclidean`.\n"
    "Signature:\n"
    "    >>> def sqeuclidean(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_l2sq(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                          PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_l2sq_k, args, positional_args_count, args_names_tuple);
}

static char const doc_cos[] = //
    "Compute cosine (angular) distances between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First matrix or vector.\n"
    "    b (NDArray): Second matrix or vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `scipy.spatial.distance.cosine`.\n"
    "Signature:\n"
    "    >>> def cosine(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_cos(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                         PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_cos_k, args, positional_args_count, args_names_tuple);
}

static char const doc_dot[] = //
    "Compute the inner (dot) product between two matrices (real or complex).\n\n"
    "Args:\n"
    "    a (NDArray): First matrix or vector.\n"
    "    b (NDArray): Second matrix or vector.\n"
    "    dtype (Union[IntegralType, FloatType, ComplexType], optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (Union[FloatType, ComplexType], optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `numpy.inner`.\n"
    "Signature:\n"
    "    >>> def dot(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_dot(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                         PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_dot_k, args, positional_args_count, args_names_tuple);
}

static char const doc_vdot[] = //
    "Compute the conjugate dot product between two complex matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First complex matrix or vector.\n"
    "    b (NDArray): Second complex matrix or vector.\n"
    "    dtype (ComplexType, optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (Union[ComplexType], optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `numpy.vdot`.\n"
    "Signature:\n"
    "    >>> def vdot(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_vdot(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                          PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_vdot_k, args, positional_args_count, args_names_tuple);
}

static char const doc_kl[] = //
    "Compute Kullback-Leibler divergences between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First floating-point matrix or vector.\n"
    "    b (NDArray): Second floating-point matrix or vector.\n"
    "    dtype (FloatType, optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `scipy.special.kl_div`.\n"
    "Signature:\n"
    "    >>> def kl(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_kl(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                        PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_kl_k, args, positional_args_count, args_names_tuple);
}

static char const doc_js[] = //
    "Compute Jensen-Shannon divergences between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First floating-point matrix or vector.\n"
    "    b (NDArray): Second floating-point matrix or vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `scipy.spatial.distance.jensenshannon`.\n"
    "Signature:\n"
    "    >>> def kl(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_js(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                        PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_js_k, args, positional_args_count, args_names_tuple);
}

static char const doc_hamming[] = //
    "Compute Hamming distances between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First binary matrix or vector.\n"
    "    b (NDArray): Second binary matrix or vector.\n"
    "    dtype (IntegralType, optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Similar to: `scipy.spatial.distance.hamming`.\n"
    "Signature:\n"
    "    >>> def hamming(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_hamming(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                             PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_hamming_k, args, positional_args_count, args_names_tuple);
}

static char const doc_jaccard[] = //
    "Compute Jaccard distances (bitwise Tanimoto) between two matrices.\n\n"
    "Args:\n"
    "    a (NDArray): First binary matrix or vector.\n"
    "    b (NDArray): Second binary matrix or vector.\n"
    "    dtype (IntegralType, optional): Override the presumed input type name.\n"
    "    out (NDArray, optional): Vector for resulting distances. Allocates a new tensor by default.\n"
    "    out_dtype (FloatType, optional): Result type, default is 'float64'.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Similar to: `scipy.spatial.distance.jaccard`.\n"
    "Signature:\n"
    "    >>> def jaccard(a, b, /, dtype, *, out, out_dtype) -> Optional[DistancesTensor]: ...";

static PyObject *api_jaccard(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                             PyObject *args_names_tuple) {
    return implement_dense_metric(simsimd_metric_jaccard_k, args, positional_args_count, args_names_tuple);
}

static char const doc_bilinear[] = //
    "Compute the bilinear form between two vectors given a metric tensor.\n\n"
    "Args:\n"
    "    a (NDArray): First vector.\n"
    "    b (NDArray): Second vector.\n"
    "    metric_tensor (NDArray): The metric tensor defining the bilinear form.\n"
    "    dtype (FloatType, optional): Override the presumed input type name.\n\n"
    "Returns:\n"
    "    float: The bilinear form.\n\n"
    "Equivalent to: `numpy.dot` with a metric tensor.\n"
    "Signature:\n"
    "    >>> def bilinear(a, b, metric_tensor, /, dtype) -> float: ...";

static PyObject *api_bilinear(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                              PyObject *args_names_tuple) {
    return implement_curved_metric(simsimd_metric_bilinear_k, args, positional_args_count, args_names_tuple);
}

static char const doc_mahalanobis[] = //
    "Compute the Mahalanobis distance between two vectors given an inverse covariance matrix.\n\n"
    "Args:\n"
    "    a (NDArray): First vector.\n"
    "    b (NDArray): Second vector.\n"
    "    inverse_covariance (NDArray): The inverse of the covariance matrix.\n"
    "    dtype (FloatType, optional): Override the presumed input type name.\n\n"
    "Returns:\n"
    "    float: The Mahalanobis distance.\n\n"
    "Equivalent to: `scipy.spatial.distance.mahalanobis`.\n"
    "Signature:\n"
    "    >>> def mahalanobis(a, b, inverse_covariance, /, dtype) -> float: ...";

static PyObject *api_mahalanobis(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                                 PyObject *args_names_tuple) {
    return implement_curved_metric(simsimd_metric_mahalanobis_k, args, positional_args_count, args_names_tuple);
}

static char const doc_intersect[] = //
    "Compute the intersection of two sorted integer arrays.\n\n"
    "Args:\n"
    "    a (NDArray): First sorted integer array.\n"
    "    b (NDArray): Second sorted integer array.\n\n"
    "Returns:\n"
    "    float: The number of intersecting elements.\n\n"
    "Similar to: `numpy.intersect1d`."
    "Signature:\n"
    "    >>> def intersect(a, b, /) -> float: ...";

static PyObject *api_intersect(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
    return implement_sparse_metric(simsimd_metric_intersect_k, args, nargs);
}

static char const doc_fma[] = //
    "Fused-Multiply-Add between 3 input vectors.\n\n"
    "Args:\n"
    "    a (NDArray): First vector.\n"
    "    b (NDArray): Second vector.\n"
    "    c (NDArray): Third vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed numeric type name.\n"
    "    alpha (float, optional): First scale, 1.0 by default.\n"
    "    beta (float, optional): Second scale, 1.0 by default.\n"
    "    out (NDArray, optional): Vector for resulting distances.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `alpha * a * b + beta * c`.\n"
    "Signature:\n"
    "    >>> def fma(a, b, c, /, dtype, *, alpha, beta, out) -> Optional[DistancesTensor]: ...";

static PyObject *api_fma(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                         PyObject *args_names_tuple) {

    PyObject *return_obj = NULL;

    // This function accepts up to 5 arguments:
    PyObject *a_obj = NULL;     // Required object, positional-only
    PyObject *b_obj = NULL;     // Required object, positional-only
    PyObject *c_obj = NULL;     // Required object, positional-only
    PyObject *dtype_obj = NULL; // Optional object, "dtype" keyword or positional
    PyObject *out_obj = NULL;   // Optional object, "out" keyword-only
    PyObject *alpha_obj = NULL; // Optional object, "alpha" keyword-only
    PyObject *beta_obj = NULL;  // Optional object, "beta" keyword-only

    // Once parsed, the arguments will be stored in these variables:
    char const *dtype_str = NULL;
    simsimd_datatype_t dtype = simsimd_datatype_unknown_k;
    simsimd_distance_t alpha = 1, beta = 1;

    Py_buffer a_buffer, b_buffer, c_buffer, out_buffer;
    TensorArgument a_parsed, b_parsed, c_parsed, out_parsed;
    memset(&a_buffer, 0, sizeof(Py_buffer));
    memset(&b_buffer, 0, sizeof(Py_buffer));
    memset(&c_buffer, 0, sizeof(Py_buffer));
    memset(&out_buffer, 0, sizeof(Py_buffer));

    Py_ssize_t const args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
    Py_ssize_t const args_count = positional_args_count + args_names_count;
    if (args_count < 3 || args_count > 7) {
        PyErr_Format(PyExc_TypeError, "Function expects 3-7 arguments, got %zd", args_count);
        return NULL;
    }
    if (positional_args_count > 4) {
        PyErr_Format(PyExc_TypeError, "Only first 4 arguments can be positional, received %zd", positional_args_count);
        return NULL;
    }

    // Positional-only arguments (first and second matrix)
    a_obj = args[0];
    b_obj = args[1];
    c_obj = args[2];

    // Positional or keyword arguments (dtype)
    if (positional_args_count == 4) dtype_obj = args[3];

    // The rest of the arguments must be checked in the keyword dictionary:
    for (Py_ssize_t args_names_tuple_progress = 0, args_progress = positional_args_count;
         args_names_tuple_progress < args_names_count; ++args_progress, ++args_names_tuple_progress) {
        PyObject *const key = PyTuple_GetItem(args_names_tuple, args_names_tuple_progress);
        PyObject *const value = args[args_progress];
        if (PyUnicode_CompareWithASCIIString(key, "dtype") == 0 && !dtype_obj) { dtype_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out") == 0 && !out_obj) { out_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "alpha") == 0 && !alpha_obj) { alpha_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "beta") == 0 && !beta_obj) { beta_obj = value; }
        else {
            PyErr_Format(PyExc_TypeError, "Got unexpected keyword argument: %S", key);
            return NULL;
        }
    }

    // Convert `dtype_obj` to `dtype_str` and to `dtype`
    if (dtype_obj) {
        dtype_str = PyUnicode_AsUTF8(dtype_obj);
        if (!dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'dtype' to be a string");
            return NULL;
        }
        dtype = python_string_to_datatype(dtype_str);
        if (dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'dtype'");
            return NULL;
        }
    }

    // Convert `alpha_obj` to `alpha` and `beta_obj` to `beta`
    if (alpha_obj) alpha = PyFloat_AsDouble(alpha_obj);
    if (beta_obj) beta = PyFloat_AsDouble(beta_obj);
    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_TypeError, "Expected 'alpha' and 'beta' to be a float");
        return NULL;
    }

    // Convert `a_obj` to `a_buffer` and to `a_parsed`. Same for `b_obj` and `out_obj`.
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed) ||
        !parse_tensor(c_obj, &c_buffer, &c_parsed))
        return NULL;
    if (out_obj && !parse_tensor(out_obj, &out_buffer, &out_parsed)) return NULL;

    // Check dimensions
    if (a_parsed.rank != 1 || b_parsed.rank != 1 || c_parsed.rank != 1 || (out_obj && out_parsed.rank != 1)) {
        PyErr_SetString(PyExc_ValueError, "All tensors must be vectors");
        goto cleanup;
    }
    if (a_parsed.dimensions != b_parsed.dimensions || a_parsed.dimensions != c_parsed.dimensions ||
        (out_obj && a_parsed.dimensions != out_parsed.dimensions)) {
        PyErr_SetString(PyExc_ValueError, "Vector dimensions don't match");
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype || a_parsed.datatype == simsimd_datatype_unknown_k ||
        b_parsed.datatype == simsimd_datatype_unknown_k || c_parsed.datatype == simsimd_datatype_unknown_k ||
        (out_obj && out_parsed.datatype == simsimd_datatype_unknown_k)) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }
    if (dtype == simsimd_datatype_unknown_k) dtype = a_parsed.datatype;

    // Look up the metric and the capability
    simsimd_kernel_fma_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_metric_kind_t const metric_kind = simsimd_metric_fma_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError,
            "Unsupported metric '%c' and datatype combination across vectors ('%s'/'%s') and "
            "`dtype` override ('%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            dtype_str ? dtype_str : "nil", datatype_to_python_string(dtype));
        goto cleanup;
    }

    char *distances_start = NULL;
    size_t distances_stride_bytes = 0;

    // Allocate the output matrix if it wasn't provided
    if (!out_obj) {
        DistancesTensor *distances_obj =
            PyObject_NewVar(DistancesTensor, &DistancesTensorType, a_parsed.dimensions * bytes_per_datatype(dtype));
        if (!distances_obj) {
            PyErr_NoMemory();
            goto cleanup;
        }

        // Initialize the object
        distances_obj->datatype = dtype;
        distances_obj->dimensions = 1;
        distances_obj->shape[0] = a_parsed.dimensions;
        distances_obj->shape[1] = 1;
        distances_obj->strides[0] = bytes_per_datatype(dtype);
        distances_obj->strides[1] = 0;
        return_obj = (PyObject *)distances_obj;
        distances_start = (char *)&distances_obj->start[0];
        distances_stride_bytes = distances_obj->strides[0];
    }
    else {
        distances_start = (char *)&out_parsed.start[0];
        distances_stride_bytes = out_buffer.strides[0];
        //? Logic suggests to return `None` in in-place mode...
        //? SciPy decided differently.
        return_obj = Py_None;
    }

    metric(a_parsed.start, b_parsed.start, c_parsed.start, a_parsed.dimensions, alpha, beta, distances_start);
cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    PyBuffer_Release(&c_buffer);
    PyBuffer_Release(&out_buffer);
    return return_obj;
}

static char const doc_wsum[] = //
    "Weighted Sum of 2 input vectors.\n\n"
    "Args:\n"
    "    a (NDArray): First vector.\n"
    "    b (NDArray): Second vector.\n"
    "    dtype (Union[IntegralType, FloatType], optional): Override the presumed numeric type name.\n"
    "    alpha (float, optional): First scale, 1.0 by default.\n"
    "    beta (float, optional): Second scale, 1.0 by default.\n"
    "    out (NDArray, optional): Vector for resulting distances.\n\n"
    "Returns:\n"
    "    DistancesTensor: The distances if `out` is not provided.\n"
    "    None: If `out` is provided. Operation will per performed in-place.\n\n"
    "Equivalent to: `alpha * a + beta * b`.\n"
    "Signature:\n"
    "    >>> def wsum(a, b, /, dtype, *, alpha, beta, out) -> Optional[DistancesTensor]: ...";

static PyObject *api_wsum(PyObject *self, PyObject *const *args, Py_ssize_t const positional_args_count,
                          PyObject *args_names_tuple) {

    PyObject *return_obj = NULL;

    // This function accepts up to 5 arguments:
    PyObject *a_obj = NULL;     // Required object, positional-only
    PyObject *b_obj = NULL;     // Required object, positional-only
    PyObject *dtype_obj = NULL; // Optional object, "dtype" keyword or positional
    PyObject *out_obj = NULL;   // Optional object, "out" keyword-only
    PyObject *alpha_obj = NULL; // Optional object, "alpha" keyword-only
    PyObject *beta_obj = NULL;  // Optional object, "beta" keyword-only

    // Once parsed, the arguments will be stored in these variables:
    char const *dtype_str = NULL;
    simsimd_datatype_t dtype = simsimd_datatype_unknown_k;
    simsimd_distance_t alpha = 1, beta = 1;

    Py_buffer a_buffer, b_buffer, out_buffer;
    TensorArgument a_parsed, b_parsed, out_parsed;
    memset(&a_buffer, 0, sizeof(Py_buffer));
    memset(&b_buffer, 0, sizeof(Py_buffer));
    memset(&out_buffer, 0, sizeof(Py_buffer));

    Py_ssize_t const args_names_count = args_names_tuple ? PyTuple_Size(args_names_tuple) : 0;
    Py_ssize_t const args_count = positional_args_count + args_names_count;
    if (args_count < 2 || args_count > 6) {
        PyErr_Format(PyExc_TypeError, "Function expects 2-6 arguments, got %zd", args_count);
        return NULL;
    }
    if (positional_args_count > 3) {
        PyErr_Format(PyExc_TypeError, "Only first 3 arguments can be positional, received %zd", positional_args_count);
        return NULL;
    }

    // Positional-only arguments (first and second matrix)
    a_obj = args[0];
    b_obj = args[1];

    // Positional or keyword arguments (dtype)
    if (positional_args_count == 3) dtype_obj = args[2];

    // The rest of the arguments must be checked in the keyword dictionary:
    for (Py_ssize_t args_names_tuple_progress = 0, args_progress = positional_args_count;
         args_names_tuple_progress < args_names_count; ++args_progress, ++args_names_tuple_progress) {
        PyObject *const key = PyTuple_GetItem(args_names_tuple, args_names_tuple_progress);
        PyObject *const value = args[args_progress];
        if (PyUnicode_CompareWithASCIIString(key, "dtype") == 0 && !dtype_obj) { dtype_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "out") == 0 && !out_obj) { out_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "alpha") == 0 && !alpha_obj) { alpha_obj = value; }
        else if (PyUnicode_CompareWithASCIIString(key, "beta") == 0 && !beta_obj) { beta_obj = value; }
        else {
            PyErr_Format(PyExc_TypeError, "Got unexpected keyword argument: %S", key);
            return NULL;
        }
    }

    // Convert `dtype_obj` to `dtype_str` and to `dtype`
    if (dtype_obj) {
        dtype_str = PyUnicode_AsUTF8(dtype_obj);
        if (!dtype_str && PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "Expected 'dtype' to be a string");
            return NULL;
        }
        dtype = python_string_to_datatype(dtype_str);
        if (dtype == simsimd_datatype_unknown_k) {
            PyErr_SetString(PyExc_ValueError, "Unsupported 'dtype'");
            return NULL;
        }
    }

    // Convert `alpha_obj` to `alpha` and `beta_obj` to `beta`
    if (alpha_obj) alpha = PyFloat_AsDouble(alpha_obj);
    if (beta_obj) beta = PyFloat_AsDouble(beta_obj);
    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_TypeError, "Expected 'alpha' and 'beta' to be a float");
        return NULL;
    }

    // Convert `a_obj` to `a_buffer` and to `a_parsed`. Same for `b_obj` and `out_obj`.
    if (!parse_tensor(a_obj, &a_buffer, &a_parsed) || !parse_tensor(b_obj, &b_buffer, &b_parsed)) return NULL;
    if (out_obj && !parse_tensor(out_obj, &out_buffer, &out_parsed)) return NULL;

    // Check dimensions
    if (a_parsed.rank != 1 || b_parsed.rank != 1 || (out_obj && out_parsed.rank != 1)) {
        PyErr_SetString(PyExc_ValueError, "All tensors must be vectors");
        goto cleanup;
    }
    if (a_parsed.dimensions != b_parsed.dimensions || (out_obj && a_parsed.dimensions != out_parsed.dimensions)) {
        PyErr_SetString(PyExc_ValueError, "Vector dimensions don't match");
        goto cleanup;
    }

    // Check data types
    if (a_parsed.datatype != b_parsed.datatype || a_parsed.datatype == simsimd_datatype_unknown_k ||
        b_parsed.datatype == simsimd_datatype_unknown_k ||
        (out_obj && out_parsed.datatype == simsimd_datatype_unknown_k)) {
        PyErr_SetString(PyExc_TypeError,
                        "Input tensors must have matching datatypes, check with `X.__array_interface__`");
        goto cleanup;
    }
    if (dtype == simsimd_datatype_unknown_k) dtype = a_parsed.datatype;

    // Look up the metric and the capability
    simsimd_kernel_wsum_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_metric_kind_t const metric_kind = simsimd_metric_wsum_k;
    simsimd_find_kernel_punned(metric_kind, dtype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (!metric) {
        PyErr_Format( //
            PyExc_LookupError,
            "Unsupported metric '%c' and datatype combination across vectors ('%s'/'%s') and "
            "`dtype` override ('%s'/'%s')",
            metric_kind,                                                                             //
            a_buffer.format ? a_buffer.format : "nil", datatype_to_python_string(a_parsed.datatype), //
            dtype_str ? dtype_str : "nil", datatype_to_python_string(dtype));
        goto cleanup;
    }

    char *distances_start = NULL;
    size_t distances_stride_bytes = 0;

    // Allocate the output matrix if it wasn't provided
    if (!out_obj) {
        DistancesTensor *distances_obj =
            PyObject_NewVar(DistancesTensor, &DistancesTensorType, a_parsed.dimensions * bytes_per_datatype(dtype));
        if (!distances_obj) {
            PyErr_NoMemory();
            goto cleanup;
        }

        // Initialize the object
        distances_obj->datatype = dtype;
        distances_obj->dimensions = 1;
        distances_obj->shape[0] = a_parsed.dimensions;
        distances_obj->shape[1] = 1;
        distances_obj->strides[0] = bytes_per_datatype(dtype);
        distances_obj->strides[1] = 0;
        return_obj = (PyObject *)distances_obj;
        distances_start = (char *)&distances_obj->start[0];
        distances_stride_bytes = distances_obj->strides[0];
    }
    else {
        distances_start = (char *)&out_parsed.start[0];
        distances_stride_bytes = out_buffer.strides[0];
        //? Logic suggests to return `None` in in-place mode...
        //? SciPy decided differently.
        return_obj = Py_None;
    }

    metric(a_parsed.start, b_parsed.start, a_parsed.dimensions, alpha, beta, distances_start);
cleanup:
    PyBuffer_Release(&a_buffer);
    PyBuffer_Release(&b_buffer);
    PyBuffer_Release(&out_buffer);
    return return_obj;
}

// There are several flags we can use to define the functions:
// - `METH_O`: Single object argument
// - `METH_VARARGS`: Variable number of arguments
// - `METH_FASTCALL`: Fast calling convention
// - `METH_KEYWORDS`: Accepts keyword arguments, can be combined with `METH_FASTCALL`
//
// https://llllllllll.github.io/c-extension-tutorial/appendix.html#c.PyMethodDef.ml_flags
static PyMethodDef simsimd_methods[] = {
    // Introspecting library and hardware capabilities
    {"get_capabilities", (PyCFunction)api_get_capabilities, METH_NOARGS, doc_get_capabilities},
    {"enable_capability", (PyCFunction)api_enable_capability, METH_O, doc_enable_capability},
    {"disable_capability", (PyCFunction)api_disable_capability, METH_O, doc_disable_capability},

    // NumPy and SciPy compatible interfaces for dense vector representations
    // Each function can compute distances between:
    //  - A pair of vectors
    //  - A batch of vector pairs (two matrices of identical shape)
    //  - A matrix of vectors and a single vector
    {"l2", (PyCFunction)api_l2, METH_FASTCALL | METH_KEYWORDS, doc_l2},
    {"l2sq", (PyCFunction)api_l2sq, METH_FASTCALL | METH_KEYWORDS, doc_l2sq},
    {"kl", (PyCFunction)api_kl, METH_FASTCALL | METH_KEYWORDS, doc_kl},
    {"js", (PyCFunction)api_js, METH_FASTCALL | METH_KEYWORDS, doc_js},
    {"cos", (PyCFunction)api_cos, METH_FASTCALL | METH_KEYWORDS, doc_cos},
    {"dot", (PyCFunction)api_dot, METH_FASTCALL | METH_KEYWORDS, doc_dot},
    {"vdot", (PyCFunction)api_vdot, METH_FASTCALL | METH_KEYWORDS, doc_vdot},
    {"hamming", (PyCFunction)api_hamming, METH_FASTCALL | METH_KEYWORDS, doc_hamming},
    {"jaccard", (PyCFunction)api_jaccard, METH_FASTCALL | METH_KEYWORDS, doc_jaccard},

    // Aliases
    {"euclidean", (PyCFunction)api_l2, METH_FASTCALL | METH_KEYWORDS, doc_l2},
    {"sqeuclidean", (PyCFunction)api_l2sq, METH_FASTCALL | METH_KEYWORDS, doc_l2sq},
    {"cosine", (PyCFunction)api_cos, METH_FASTCALL | METH_KEYWORDS, doc_cos},
    {"inner", (PyCFunction)api_dot, METH_FASTCALL | METH_KEYWORDS, doc_dot},
    {"kullbackleibler", (PyCFunction)api_kl, METH_FASTCALL | METH_KEYWORDS, doc_kl},
    {"jensenshannon", (PyCFunction)api_js, METH_FASTCALL | METH_KEYWORDS, doc_js},

    // Conventional `cdist` interface for pairwise distances
    {"cdist", (PyCFunction)api_cdist, METH_FASTCALL | METH_KEYWORDS, doc_cdist},

    // Exposing underlying API for USearch `CompiledMetric`
    {"pointer_to_euclidean", (PyCFunction)api_l2_pointer, METH_O, doc_l2_pointer},
    {"pointer_to_sqeuclidean", (PyCFunction)api_l2sq_pointer, METH_O, doc_l2sq_pointer},
    {"pointer_to_cosine", (PyCFunction)api_cos_pointer, METH_O, doc_cos_pointer},
    {"pointer_to_inner", (PyCFunction)api_dot_pointer, METH_O, doc_dot_pointer},
    {"pointer_to_dot", (PyCFunction)api_dot_pointer, METH_O, doc_dot_pointer},
    {"pointer_to_vdot", (PyCFunction)api_vdot_pointer, METH_O, doc_vdot_pointer},
    {"pointer_to_kullbackleibler", (PyCFunction)api_kl_pointer, METH_O, doc_kl_pointer},
    {"pointer_to_jensenshannon", (PyCFunction)api_js_pointer, METH_O, doc_js_pointer},

    // Set operations
    {"intersect", (PyCFunction)api_intersect, METH_FASTCALL, doc_intersect},

    // Curved spaces
    {"bilinear", (PyCFunction)api_bilinear, METH_FASTCALL | METH_KEYWORDS, doc_bilinear},
    {"mahalanobis", (PyCFunction)api_mahalanobis, METH_FASTCALL | METH_KEYWORDS, doc_mahalanobis},

    // Vectorized operations
    {"fma", (PyCFunction)api_fma, METH_FASTCALL | METH_KEYWORDS, doc_fma},
    {"wsum", (PyCFunction)api_wsum, METH_FASTCALL | METH_KEYWORDS, doc_wsum},

    // Sentinel
    {NULL, NULL, 0, NULL}};

static char const doc_module[] = //
    "Portable mixed-precision BLAS-like vector math library for x86 and ARM.\n"
    "\n"
    "Performance Recommendations:\n"
    " - Avoid converting to NumPy arrays. SimSIMD works with any Tensor implementation\n"
    "   compatible with Python's Buffer Protocol, which can be coming from PyTorch, TensorFlow, etc.\n"
    " - In low-latency environments - provide the output array with the `out=` parameter\n"
    "   to avoid expensive memory allocations on the hot path.\n"
    " - On modern CPUs, if the application allows, prefer low-precision numeric types.\n"
    "   Whenever possible, use 'bf16' and 'f16' over 'f32'. Consider quantizing to 'i8'\n"
    "   and 'u8' for highest hardware compatibility and performance.\n"
    " - If you are only interested in relative proximity instead of the absolute distance\n"
    "   prefer simpler kernels, like the Squared Euclidean distance over the Euclidean distance.\n"
    " - Use row-major continuous matrix representations. Strides between rows won't have significant\n"
    "   impact on performance, but most modern HPC packages explicitly ban non-contiguous rows,\n"
    "   where the nearby matrix cells within a row have multi-byte gaps.\n"
    " - The CPython runtime has a noticeable overhead for function calls, so consider batching\n"
    "   kernel invokations. Many kernels can compute not only 1-to-1 distance between vectors,\n"
    "   but also 1-to-N and N-to-N distances between two batches of vectors packed into matrices.\n"
    "\n"
    "Example:\n"
    "    >>> import simsimd\n"
    "    >>> simsimd.l2(a, b)\n"
    "\n"
    "Mixed-precision 1-to-N example with numeric types missing in NumPy, but present in PyTorch:\n"
    "    >>> import simsimd\n"
    "    >>> import torch\n"
    "    >>> a = torch.randn(1536, dtype=torch.bfloat16)\n"
    "    >>> b = torch.randn((100, 1536), dtype=torch.bfloat16)\n"
    "    >>> c = torch.zeros(100, dtype=torch.float32)\n"
    "    >>> simsimd.l2(a, b, dtype='bfloat16', out=c)\n";

static PyModuleDef simsimd_module = {
    PyModuleDef_HEAD_INIT, .m_name = "SimSIMD", .m_doc = doc_module, .m_size = -1, .m_methods = simsimd_methods,
};

PyMODINIT_FUNC PyInit_simsimd(void) {
    PyObject *m;

    if (PyType_Ready(&DistancesTensorType) < 0) return NULL;

    m = PyModule_Create(&simsimd_module);
    if (m == NULL) return NULL;

    // Add version metadata
    {
        char version_str[64];
        snprintf(version_str, sizeof(version_str), "%d.%d.%d", SIMSIMD_VERSION_MAJOR, SIMSIMD_VERSION_MINOR,
                 SIMSIMD_VERSION_PATCH);
        PyModule_AddStringConstant(m, "__version__", version_str);
    }

    Py_INCREF(&DistancesTensorType);
    if (PyModule_AddObject(m, "DistancesTensor", (PyObject *)&DistancesTensorType) < 0) {
        Py_XDECREF(&DistancesTensorType);
        Py_XDECREF(m);
        return NULL;
    }

    static_capabilities = simsimd_capabilities();
    return m;
}
