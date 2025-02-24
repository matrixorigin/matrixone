package simsimd

/*
#cgo CFLAGS: -O3
#cgo LDFLAGS: -O3 -L. -lm
#define SIMSIMD_NATIVE_F16 (0)
#define SIMSIMD_NATIVE_BF16 (0)
#include "../include/simsimd/simsimd.h"
#include <stdlib.h>

inline static simsimd_f32_t cosine_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_cosine_k, simsimd_datatype_i8_k, simsimd_cap_any_k)(a, b, d, d); }
inline static simsimd_f32_t cosine_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_cosine_k, simsimd_datatype_f32_k, simsimd_cap_any_k)(a, b, d, d); }
inline static simsimd_f32_t inner_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_inner_k, simsimd_datatype_i8_k, simsimd_cap_any_k)(a, b, d, d); }
inline static simsimd_f32_t inner_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_inner_k, simsimd_datatype_f32_k, simsimd_cap_any_k)(a, b, d, d); }
inline static simsimd_f32_t sqeuclidean_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_sqeuclidean_k, simsimd_datatype_i8_k, simsimd_cap_any_k)(a, b, d, d); }
inline static simsimd_f32_t sqeuclidean_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t d) { return simsimd_metric_punned(simsimd_metric_sqeuclidean_k, simsimd_datatype_f32_k, simsimd_cap_any_k)(a, b, d, d); }
*/
import "C"

// CosineI8 computes the cosine distance between two i8 vectors using the most suitable SIMD instruction set available.
func CosineI8(a, b []int8) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.cosine_i8((*C.simsimd_i8_t)(&a[0]), (*C.simsimd_i8_t)(&b[0]), C.simsimd_size_t(len(a))))
}

// CosineF32 computes the cosine distance between two f32 vectors using the most suitable SIMD instruction set available.
func CosineF32(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.cosine_f32((*C.simsimd_f32_t)(&a[0]), (*C.simsimd_f32_t)(&b[0]), C.simsimd_size_t(len(a))))
}

// InnerI8 computes the inner-product similarity between two i8 vectors using the most suitable SIMD instruction set available.
func InnerI8(a, b []int8) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.inner_i8((*C.simsimd_i8_t)(&a[0]), (*C.simsimd_i8_t)(&b[0]), C.simsimd_size_t(len(a))))
}

// InnerF32 computes the inner-product similarity between two f32 vectors using the most suitable SIMD instruction set available.
func InnerF32(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.inner_f32((*C.simsimd_f32_t)(&a[0]), (*C.simsimd_f32_t)(&b[0]), C.simsimd_size_t(len(a))))
}

// SqEuclideanI8 computes the squared euclidean similarity between two i8 vectors using the most suitable SIMD instruction set available.
func SqEuclideanI8(a, b []int8) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.sqeuclidean_i8((*C.simsimd_i8_t)(&a[0]), (*C.simsimd_i8_t)(&b[0]), C.simsimd_size_t(len(a))))
}

// SqEuclideanF32 computes the squared euclidean similarity between two f32 vectors using the most suitable SIMD instruction set available.
func SqEuclideanF32(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("both vectors must have the same length")
	}

	return float32(C.sqeuclidean_f32((*C.simsimd_f32_t)(&a[0]), (*C.simsimd_f32_t)(&b[0]), C.simsimd_size_t(len(a))))
}
