//go:build amd64

// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"math"
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	dist, err := L2DistanceSq(v1, v2)
	if err != nil {
		return dist, err
	}

	return T(math.Sqrt(float64(dist))), nil
}
*/

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	dist, err := L2DistanceSq(v1, v2)
	if err != nil {
		return dist, err
	}

	return T(math.Sqrt(float64(dist))), nil
}

/*
func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	var sumOfSquares T
	for i := range v1 {
		diff := v1[i] - v2[i]
		sumOfSquares += diff * diff
	}
	return sumOfSquares, nil

}
*/

func SumFloat32x16(v archsimd.Float32x16) float32 {
	var arr [16]float32
	v.Store(&arr)
	var total float32
	for _, x := range arr {
		total += x
	}
	return total
}

func SumFloat32x8(v archsimd.Float32x8) float32 {
	var arr [8]float32
	v.Store(&arr)
	var total float32
	for _, x := range arr {
		total += x
	}
	return total
}

func SumFloat32x4(v archsimd.Float32x4) float32 {
	var arr [4]float32
	v.Store(&arr)
	var total float32
	for _, x := range arr {
		total += x
	}
	return total
}

func SumFloat64x8(v archsimd.Float64x8) float64 {
	var arr [8]float64
	v.Store(&arr)
	var total float64
	for _, x := range arr {
		total += x
	}
	return total
}

func SumFloat64x4(v archsimd.Float64x4) float64 {
	var arr [4]float64
	v.Store(&arr)
	var total float64
	for _, x := range arr {
		total += x
	}
	return total
}

func SumFloat64x2(v archsimd.Float64x2) float64 {
	var arr [2]float64
	v.Store(&arr)
	var total float64
	for _, x := range arr {
		total += x
	}
	return total
}

func L2DistanceSqFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sumSq float32
	i := 0
	n := len(a)

	// 1. AVX-512 Path (512-bit vectors, 16 elements)
	if archsimd.X86.AVX512() {
		sumVec := archsimd.Float32x16{}
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])
			diff := va.Sub(vb)
			sumVec = diff.MulAdd(diff, sumVec)
			i += 16
		}
		sumSq += SumFloat32x16(sumVec)
	}

	// 2. AVX2 Path (256-bit vectors, 8 elements)
	if archsimd.X86.AVX2() || archsim.X86.AVX() {
		sumVec := archsimd.Float32x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])
			diff := va.Sub(vb)
			sq := diff.Mul(diff)
			sumVec = sumVec.Add(sq)
			i += 8
		}
		sumSq += SumFloat32x8(sumVec)
	}

	// 4. Scalar Tail Path
	for ; i < n; i++ {
		diff := a[i] - b[i]
		sumSq += diff * diff
	}
	return sumSq, nil
}

func L2DistanceSqFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sumSq float64
	i := 0
	n := len(a)

	// 1. AVX-512 Path (512-bit vectors, 8 elements)
	if archsimd.X86.AVX512() {
		sumVec := archsimd.Float64x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])
			diff := va.Sub(vb)
			sumVec = diff.MulAdd(diff, sumVec)
			i += 8
		}
		sumSq += SumFloat64x8(sumVec)
	}

	// 2. AVX2 Path (256-bit vectors, 4 elements)
	if archsimd.X86.AVX2() || archsimd.X86.AVX() {
		sumVec := archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])
			diff := va.Sub(vb)
			sq := diff.Mul(diff)
			sumVec = sumVec.Add(sq)
			i += 4
		}
		sumSq += SumFloat64x4(sumVec)
	}

	// 4. Scalar Tail Path
	for ; i < n; i++ {
		diff := a[i] - b[i]
		sumSq += diff * diff
	}
	return sumSq, nil
}

// L2SquareDistanceUnrolled calculates the L2 square distance using loop unrolling.
// This optimization can improve performance for large vectors by reducing loop
// overhead and allowing for better instruction-level parallelism.
func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {

	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := L2DistanceSqFloat32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := L2DistanceSqFloat64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

// L1Distance calculates the L1 (Manhattan) distance between two vectors.
/*
func L1Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	var sum T
	for i := range v1 {
		sum += math.Abs(v1[i] - v2[i])
	}
	return sum, nil

}
*/

// L1Distance computes the Manhattan distance between two float32 slices.
func L1DistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var sum float32
	i := 0

	// 1. AVX-512 Path (16 elements per iteration)
	if archsimd.X86.AVX512() {
		acc := archsimd.LoadFloat32x16Slice(make([]float32, 16)) // Zero accumulator
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])
			// Calculate |va - vb| and add to accumulator
			d1 := va.Sub(vb)
			d2 := vb.Sub(va)
			absDiff := d1.Max(d2)
			acc = acc.Add(absDiff)
			i += 16
		}
		sum += SumFloat32x16(acc) // Horizontal sum of the vector
	}

	// 2. AVX2/AVX Path (8 elements per iteration)
	// Most modern archsimd implementations handle AVX2/AVX via Float32x8
	if i <= n-8 && archsimd.X86.AVX2() || archsimd.X86.AVX() {
		acc := archsimd.LoadFloat32x8Slice(make([]float32, 8))
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])
			d1 := va.Sub(vb)
			d2 := vb.Sub(va)
			absDiff := d1.Max(d2)
			acc = acc.Add(absDiff)
			i += 8
		}
		sum += SumFloat32x8(acc)
	}

	// 3. Scalar Tail (Process remaining elements)
	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 {
			val = -val
		}
		sum += val
	}

	return sum, nil
}

// L1Distance computes Manhattan distance for float64 vectors.
func L1DistanceFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var total float64
	i := 0

	// 1. AVX-512 Path: 512-bit registers (8 float64 elements)
	if archsimd.X86.AVX512() {
		acc := archsimd.Float64x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])
			// Calculate |va - vb| and accumulate
			d1 := va.Sub(vb)
			d2 := vb.Sub(va)
			absDiff := d1.Max(d2)
			acc = acc.Add(absDiff)
			i += 8
		}
		total += SumFloat64x8(acc) // Horizontal reduction
	}

	// 2. AVX2/AVX Path: 256-bit registers (4 float64 elements)
	if i <= n-4 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		acc := archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])
			d1 := va.Sub(vb)
			d2 := vb.Sub(va)
			absDiff := d1.Max(d2)
			acc = acc.Add(absDiff)
			i += 4
		}
		total += SumFloat64x4(acc)
	}

	// 3. Scalar Tail: Handle remaining 0-3 elements
	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 {
			val = -val
		}
		total += val
	}

	return total, nil
}

// L1DistanceUnrolled calculates the L1 distance using loop unrolling for optimization.
// It processes 8 elements per iteration to reduce loop overhead and improve performance
// on large vectors. It also uses an inline 'abs' for potential speed gains.
func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := L1DistanceFloat32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := L1DistanceFloat64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

// InnerProduct calculates the inner product (dot product) of two vectors.
// This is a clear, readable, and idiomatic Go implementation.
/*
func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	var sum T
	for i := range p {
		sum += p[i] * q[i]
	}

	return -sum, nil
}
*/

// InnerProduct computes the dot product of two float32 slices using SIMD.
func InnerProductFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var total float32
	i := 0

	// 1. AVX-512 Path: 16 float32 elements (512-bit) per iteration
	if archsimd.X86.AVX512() {
		acc := archsimd.Float32x16{} // Zero-initialized accumulator
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])

			// Compute element-wise multiplication and add to accumulator
			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 16
		}
		total += SumFloat32x16(acc) // Final horizontal sum of the 16 elements
	}

	// 2. AVX2/AVX Path: 8 float32 elements (256-bit) per iteration
	if i <= n-8 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		acc := archsimd.Float32x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])

			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 8
		}
		total += SumFloat32x8(acc)
	}

	// 3. Scalar Tail: Process remaining 0-7 elements
	for ; i < n; i++ {
		total += a[i] * b[i]
	}

	return -total, nil
}

// InnerProduct computes the dot product of two float64 slices using SIMD.
func InnerProductFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var total float64
	i := 0

	// 1. AVX-512 Path: 8 float64 elements (512-bit) per iteration
	if archsimd.X86.AVX512() {
		acc := archsimd.Float64x8{} // Initialized to zero
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])

			// Element-wise multiplication and accumulation
			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 8
		}
		total += SumFloat64x8(acc) // Final horizontal reduction
	}

	// 2. AVX2/AVX Path: 4 float64 elements (256-bit) per iteration
	if i <= n-4 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		acc := archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])

			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 4
		}
		total += SumFloat64x4(acc)
	}

	// 3. Scalar Tail: Process remaining elements
	for ; i < n; i++ {
		total += a[i] * b[i]
	}

	return -total, nil
}

// InnerProductUnrolled calculates the inner product using loop unrolling.
// This can significantly improve performance for large vectors by reducing
// loop overhead and enabling better CPU instruction scheduling.
func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {

	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := InnerProductFloat32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := InnerProductFloat64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func CosineDistanceF32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i, n := 0, len(a)

	// 1. AVX-512 (512-bit, 16 elements)
	if archsimd.X86.AVX512() {
		accDot, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 16
		}
		dot += SumFloat32x16(accDot)
		normA += SumFloat32x16(accA)
		normB += SumFloat32x16(accB)
	}

	// 2. AVX2/AVX (256-bit, 8 elements)
	if i <= n-8 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		accDot, accA, accB := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat32x8(accDot)
		normA += SumFloat32x8(accA)
		normB += SumFloat32x8(accB)
	}

	// 3. Scalar Tail
	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 {
		return 1.0, nil
	}

	similarity := float64(dot) / denominator
	return float32(1.0 - similarity), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i, n := 0, len(a)

	// 1. AVX-512 (512-bit, 8 elements)
	if archsimd.X86.AVX512() {
		accDot, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat64x8(accDot)
		normA += SumFloat64x8(accA)
		normB += SumFloat64x8(accB)
	}

	// 2. AVX2/AVX (256-bit, 4 elements)
	if i <= n-4 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		accDot, accA, accB := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 4
		}
		dot += SumFloat64x4(accDot)
		normA += SumFloat64x4(accA)
		normB += SumFloat64x4(accB)
	}

	// 3. Scalar Tail
	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 {
		return 1.0, nil
	}
	similarity := dot / denominator
	return 1.0 - similarity, nil
}

// CosineDistance calculates the cosine distance between two vectors using generics.
//
// Formula:
// Cosine Distance = 1 - Cosine Similarity
// Cosine Similarity = (v1 · v2) / (||v1|| * ||v2||)
//
// This implementation uses loop unrolling to optimize the calculation of the
// dot product (v1 · v2) and the squared L2 norms (||v1||², ||v2||²) in a single pass.
// This improves performance by reducing loop overhead and maximizing CPU cache efficiency.
func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {

	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := CosineDistanceF32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := CosineDistanceF64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func CosineSimilarityF32(a, b []float32) (float32, error) {
	if len(a) == 0 {
		// The distance is undefined for empty vectors. Returning 0 and no error is a common convention.
		return 0, nil
	}
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i, n := 0, len(a)

	// 1. AVX-512 (512-bit, 16 elements)
	if archsimd.X86.AVX512() {
		accDot, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 16
		}
		dot += SumFloat32x16(accDot)
		normA += SumFloat32x16(accA)
		normB += SumFloat32x16(accB)
	}

	// 2. AVX2/AVX (256-bit, 8 elements)
	if i <= n-8 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		accDot, accA, accB := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat32x8(accDot)
		normA += SumFloat32x8(accA)
		normB += SumFloat32x8(accB)
	}

	// 3. Scalar Tail
	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 {
		// This can happen if one or both vectors are all zeros.
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}

	similarity := float64(dot) / denominator
	return float32(similarity), nil
}

func CosineSimilarityF64(a, b []float64) (float64, error) {
	if len(a) == 0 {
		// The distance is undefined for empty vectors. Returning 0 and no error is a common convention.
		return 0, nil
	}

	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i, n := 0, len(a)

	// 1. AVX-512 (512-bit, 8 elements)
	if archsimd.X86.AVX512() {
		accDot, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat64x8(accDot)
		normA += SumFloat64x8(accA)
		normB += SumFloat64x8(accB)
	}

	// 2. AVX2/AVX (256-bit, 4 elements)
	if i <= n-4 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		accDot, accA, accB := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])
			accDot = accDot.Add(va.Mul(vb))
			accA = accA.Add(va.Mul(va))
			accB = accB.Add(vb.Mul(vb))
			i += 4
		}
		dot += SumFloat64x4(accDot)
		normA += SumFloat64x4(accA)
		normB += SumFloat64x4(accB)
	}

	// 3. Scalar Tail
	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 {
		// This can happen if one or both vectors are all zeros.
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	similarity := dot / denominator
	return similarity, nil
}

// CosineSimilarity calculates the cosine similarity between two vectors using generics.
//
// Formula:
// Cosine Distance = 1 - Cosine Similarity
// Cosine Similarity = (v1 · v2) / (||v1|| * ||v2||)
//
// This implementation uses loop unrolling to optimize the calculation of the
// dot product (v1 · v2) and the squared L2 norms (||v1||², ||v2||²) in a single pass.
// This improves performance by reducing loop overhead and maximizing CPU cache efficiency.
func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := CosineSimilarityF32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := CosineSimilarityF64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

// InnerProduct computes the dot product of two float32 slices using SIMD.
func SphericalDistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var total float32
	i := 0

	// 1. AVX-512 Path: 16 float32 elements (512-bit) per iteration
	if archsimd.X86.AVX512() {
		acc := archsimd.Float32x16{} // Zero-initialized accumulator
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])

			// Compute element-wise multiplication and add to accumulator
			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 16
		}
		total += SumFloat32x16(acc) // Final horizontal sum of the 16 elements
	}

	// 2. AVX2/AVX Path: 8 float32 elements (256-bit) per iteration
	if i <= n-8 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		acc := archsimd.Float32x8{}
		for i <= n-8 {
			va := archsimd.LoadFloat32x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat32x8Slice(b[i : i+8])

			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 8
		}
		total += SumFloat32x8(acc)
	}

	// 3. Scalar Tail: Process remaining 0-7 elements
	for ; i < n; i++ {
		total += a[i] * b[i]
	}

	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}

	theta := math.Acos(float64(total))
	//To scale the result to the range [0, 1], we divide by Pi.
	return float32(theta / math.Pi), nil
}

func SphericalDistanceFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n := len(a)
	var total float64
	i := 0

	// 1. AVX-512 Path: 8 float64 elements (512-bit) per iteration
	if archsimd.X86.AVX512() {
		acc := archsimd.Float64x8{} // Initialized to zero
		for i <= n-8 {
			va := archsimd.LoadFloat64x8Slice(a[i : i+8])
			vb := archsimd.LoadFloat64x8Slice(b[i : i+8])

			// Element-wise multiplication and accumulation
			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 8
		}
		total += SumFloat64x8(acc) // Final horizontal reduction
	}

	// 2. AVX2/AVX Path: 4 float64 elements (256-bit) per iteration
	if i <= n-4 && (archsimd.X86.AVX2() || archsimd.X86.AVX()) {
		acc := archsimd.Float64x4{}
		for i <= n-4 {
			va := archsimd.LoadFloat64x4Slice(a[i : i+4])
			vb := archsimd.LoadFloat64x4Slice(b[i : i+4])

			prod := va.Mul(vb)
			acc = acc.Add(prod)
			i += 4
		}
		total += SumFloat64x4(acc)
	}

	// 3. Scalar Tail: Process remaining elements
	for ; i < n; i++ {
		total += a[i] * b[i]
	}

	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}

	theta := math.Acos(total)
	//To scale the result to the range [0, 1], we divide by Pi.
	return theta / math.Pi, nil
}

// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
// NOTE: spherical distance between two points on a sphere is equal to the
// angular distance between the two points, scaled by pi.
// Refs:
// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {

	switch any(p).(type) {
	case []float32:
		_p := any(p).([]float32)
		_q := any(q).([]float32)
		ret, err := SphericalDistanceFloat32(_p, _q)
		return T(ret), err
	case []float64:
		_p := any(p).([]float64)
		_q := any(q).([]float64)
		ret, err := SphericalDistanceFloat64(_p, _q)
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {

	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}

	// Compute the norm of the vector
	var sumSquares float64
	for _, val := range v1 {
		sumSquares += float64(val) * float64(val)
	}
	norm := math.Sqrt(sumSquares)
	if norm == 0 {
		copy(normalized, v1)
		return nil
	}

	// Divide each element by the norm
	for i, val := range v1 {
		normalized[i] = T(float64(val) / norm)
	}

	return nil
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v {
		v[i] *= scale
	}
}
