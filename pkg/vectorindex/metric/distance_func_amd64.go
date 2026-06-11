//go:build amd64 && go1.26 && goexperiment.simd

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

var (
	hasAVX512 = archsimd.X86.AVX512()
)

// Reduction Helpers - Simple Store and Tree Sum for maximum throughput
func sumF32x16(v archsimd.Float32x16) float32 {
	var a [16]float32
	v.Store(&a)
	s0 := (a[0] + a[1]) + (a[2] + a[3])
	s1 := (a[4] + a[5]) + (a[6] + a[7])
	s2 := (a[8] + a[9]) + (a[10] + a[11])
	s3 := (a[12] + a[13]) + (a[14] + a[15])
	return (s0 + s1) + (s2 + s3)
}

func sumF64x8(v archsimd.Float64x8) float64 {
	var a [8]float64
	v.Store(&a)
	return (a[0] + a[1] + a[2] + a[3]) + (a[4] + a[5] + a[6] + a[7])
}

// L2 Distance Squared kernels
func L2DistanceSqFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float32
	i := 0

	if hasAVX512 && n >= 64 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
			d0 := archsimd.LoadFloat32x16Slice(as[0:16]).Sub(archsimd.LoadFloat32x16Slice(bs[0:16]))
			d1 := archsimd.LoadFloat32x16Slice(as[16:32]).Sub(archsimd.LoadFloat32x16Slice(bs[16:32]))
			d2 := archsimd.LoadFloat32x16Slice(as[32:48]).Sub(archsimd.LoadFloat32x16Slice(bs[32:48]))
			d3 := archsimd.LoadFloat32x16Slice(as[48:64]).Sub(archsimd.LoadFloat32x16Slice(bs[48:64]))

			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 64
		}
		sum += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		d0 := as[0] - bs[0]
		d1 := as[1] - bs[1]
		d2 := as[2] - bs[2]
		d3 := as[3] - bs[3]
		d4 := as[4] - bs[4]
		d5 := as[5] - bs[5]
		d6 := as[6] - bs[6]
		d7 := as[7] - bs[7]
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
		i += 8
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func InnerProductFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float32
	i := 0

	if hasAVX512 && n >= 64 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
			acc0 = archsimd.LoadFloat32x16Slice(as[0:16]).MulAdd(archsimd.LoadFloat32x16Slice(bs[0:16]), acc0)
			acc1 = archsimd.LoadFloat32x16Slice(as[16:32]).MulAdd(archsimd.LoadFloat32x16Slice(bs[16:32]), acc1)
			acc2 = archsimd.LoadFloat32x16Slice(as[32:48]).MulAdd(archsimd.LoadFloat32x16Slice(bs[32:48]), acc2)
			acc3 = archsimd.LoadFloat32x16Slice(as[48:64]).MulAdd(archsimd.LoadFloat32x16Slice(bs[48:64]), acc3)
			i += 64
		}
		total += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	return -total, nil
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if pf32, ok := any(v1).([]float32); ok {
		dist, err := L2DistanceSqFloat32(pf32, any(v2).([]float32))
		if err != nil {
			return 0, err
		}
		return T(math.Sqrt(float64(dist))), nil
	}
	if pf64, ok := any(v1).([]float64); ok {
		dist, err := L2DistanceSqFloat64(pf64, any(v2).([]float64))
		if err != nil {
			return 0, err
		}
		return T(math.Sqrt(dist)), nil
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func L2DistanceSqFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float64
	i := 0
	if hasAVX512 && n >= 32 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			d0 := archsimd.LoadFloat64x8Slice(as[0:8]).Sub(archsimd.LoadFloat64x8Slice(bs[0:8]))
			d1 := archsimd.LoadFloat64x8Slice(as[8:16]).Sub(archsimd.LoadFloat64x8Slice(bs[8:16]))
			d2 := archsimd.LoadFloat64x8Slice(as[16:24]).Sub(archsimd.LoadFloat64x8Slice(bs[16:24]))
			d3 := archsimd.LoadFloat64x8Slice(as[24:32]).Sub(archsimd.LoadFloat64x8Slice(bs[24:32]))
			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 32
		}
		sum += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		d0 := as[0] - bs[0]
		d1 := as[1] - bs[1]
		d2 := as[2] - bs[2]
		d3 := as[3] - bs[3]
		d4 := as[4] - bs[4]
		d5 := as[5] - bs[5]
		d6 := as[6] - bs[6]
		d7 := as[7] - bs[7]
		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
		i += 8
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func InnerProductFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float64
	i := 0
	if hasAVX512 && n >= 32 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = archsimd.LoadFloat64x8Slice(as[0:8]).MulAdd(archsimd.LoadFloat64x8Slice(bs[0:8]), acc0)
			acc1 = archsimd.LoadFloat64x8Slice(as[8:16]).MulAdd(archsimd.LoadFloat64x8Slice(bs[8:16]), acc1)
			acc2 = archsimd.LoadFloat64x8Slice(as[16:24]).MulAdd(archsimd.LoadFloat64x8Slice(bs[16:24]), acc2)
			acc3 = archsimd.LoadFloat64x8Slice(as[24:32]).MulAdd(archsimd.LoadFloat64x8Slice(bs[24:32]), acc3)
			i += 32
		}
		total += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	return -total, nil
}

func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L2DistanceSqFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L2DistanceSqFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := InnerProductFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := InnerProductFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func L1DistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var sum float32
	i := 0
	if hasAVX512 && n >= 64 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
			acc0 = acc0.Add(archsimd.LoadFloat32x16Slice(as[0:16]).Sub(archsimd.LoadFloat32x16Slice(bs[0:16])).Max(archsimd.LoadFloat32x16Slice(bs[0:16]).Sub(archsimd.LoadFloat32x16Slice(as[0:16]))))
			acc1 = acc1.Add(archsimd.LoadFloat32x16Slice(as[16:32]).Sub(archsimd.LoadFloat32x16Slice(bs[16:32])).Max(archsimd.LoadFloat32x16Slice(bs[16:32]).Sub(archsimd.LoadFloat32x16Slice(as[16:32]))))
			acc2 = acc2.Add(archsimd.LoadFloat32x16Slice(as[32:48]).Sub(archsimd.LoadFloat32x16Slice(bs[32:48])).Max(archsimd.LoadFloat32x16Slice(bs[32:48]).Sub(archsimd.LoadFloat32x16Slice(as[32:48]))))
			acc3 = acc3.Add(archsimd.LoadFloat32x16Slice(as[48:64]).Sub(archsimd.LoadFloat32x16Slice(bs[48:64])).Max(archsimd.LoadFloat32x16Slice(bs[48:64]).Sub(archsimd.LoadFloat32x16Slice(as[48:64]))))
			i += 64
		}
		sum += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	abs := func(x float32) float32 {
		return math.Float32frombits(math.Float32bits(x) &^ (1 << 31))
	}
	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		sum += abs(as[0]-bs[0]) + abs(as[1]-bs[1]) + abs(as[2]-bs[2]) + abs(as[3]-bs[3]) +
			abs(as[4]-bs[4]) + abs(as[5]-bs[5]) + abs(as[6]-bs[6]) + abs(as[7]-bs[7])
		i += 8
	}

	for ; i < n; i++ {
		sum += abs(a[i] - b[i])
	}
	return sum, nil
}

func L1DistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var sum float64
	i := 0
	if hasAVX512 && n >= 32 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = acc0.Add(archsimd.LoadFloat64x8Slice(as[0:8]).Sub(archsimd.LoadFloat64x8Slice(bs[0:8])).Max(archsimd.LoadFloat64x8Slice(bs[0:8]).Sub(archsimd.LoadFloat64x8Slice(as[0:8]))))
			acc1 = acc1.Add(archsimd.LoadFloat64x8Slice(as[8:16]).Sub(archsimd.LoadFloat64x8Slice(bs[8:16])).Max(archsimd.LoadFloat64x8Slice(bs[8:16]).Sub(archsimd.LoadFloat64x8Slice(as[8:16]))))
			acc2 = acc2.Add(archsimd.LoadFloat64x8Slice(as[16:24]).Sub(archsimd.LoadFloat64x8Slice(bs[16:24])).Max(archsimd.LoadFloat64x8Slice(bs[16:24]).Sub(archsimd.LoadFloat64x8Slice(as[16:24]))))
			acc3 = acc3.Add(archsimd.LoadFloat64x8Slice(as[24:32]).Sub(archsimd.LoadFloat64x8Slice(bs[24:32])).Max(archsimd.LoadFloat64x8Slice(bs[24:32]).Sub(archsimd.LoadFloat64x8Slice(as[24:32]))))
			i += 32
		}
		sum += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	abs := func(x float64) float64 {
		return math.Abs(x)
	}
	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		sum += abs(as[0]-bs[0]) + abs(as[1]-bs[1]) + abs(as[2]-bs[2]) + abs(as[3]-bs[3]) +
			abs(as[4]-bs[4]) + abs(as[5]-bs[5]) + abs(as[6]-bs[6]) + abs(as[7]-bs[7])
		i += 8
	}

	for ; i < n; i++ {
		sum += abs(a[i] - b[i])
	}
	return sum, nil
}

func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L1DistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L1DistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineDistanceF32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float32
	i := 0
	if n >= 16 && hasAVX512 {
		accD, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 16
		}
		dot, normA, normB = sumF32x16(accD), sumF32x16(accA), sumF32x16(accB)
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if den == 0 {
		return 1.0, nil
	}
	return float32(1.0 - float64(dot)/den), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float64
	i := 0
	if n >= 8 && hasAVX512 {
		accD, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF64x8(accD), sumF64x8(accA), sumF64x8(accB)
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	den := math.Sqrt(normA) * math.Sqrt(normB)
	if den == 0 {
		return 1.0, nil
	}
	return 1.0 - dot/den, nil
}

func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineDistanceF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineDistanceF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineSimilarityF32(a, b []float32) (float32, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float32
	i := 0
	if n >= 16 && hasAVX512 {
		accD, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 16
		}
		dot, normA, normB = sumF32x16(accD), sumF32x16(accA), sumF32x16(accB)
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	den := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if den == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity zero denominator")
	}
	return float32(float64(dot) / den), nil
}

func CosineSimilarityF64(a, b []float64) (float64, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float64
	i := 0
	if n >= 8 && hasAVX512 {
		accD, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF64x8(accD), sumF64x8(accA), sumF64x8(accB)
	}

	for i <= n-4 {
		// BCE Hint
		va := a[i : i+4 : i+4]
		vb := b[i : i+4 : i+4]
		dot += va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2] + va[3]*vb[3]
		normA += va[0]*va[0] + va[1]*va[1] + va[2]*va[2] + va[3]*va[3]
		normB += vb[0]*vb[0] + vb[1]*vb[1] + vb[2]*vb[2] + vb[3]*vb[3]
		i += 4
	}

	for ; i < n; i++ {
		dot, normA, normB = dot+a[i]*b[i], normA+a[i]*a[i], normB+b[i]*b[i]
	}
	den := math.Sqrt(normA) * math.Sqrt(normB)
	if den == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity zero denominator")
	}
	return dot / den, nil
}

func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineSimilarityF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineSimilarityF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func SphericalDistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float32
	i := 0
	if hasAVX512 && n >= 64 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
			acc0 = archsimd.LoadFloat32x16Slice(as[0:16]).MulAdd(archsimd.LoadFloat32x16Slice(bs[0:16]), acc0)
			acc1 = archsimd.LoadFloat32x16Slice(as[16:32]).MulAdd(archsimd.LoadFloat32x16Slice(bs[16:32]), acc1)
			acc2 = archsimd.LoadFloat32x16Slice(as[32:48]).MulAdd(archsimd.LoadFloat32x16Slice(bs[32:48]), acc2)
			acc3 = archsimd.LoadFloat32x16Slice(as[48:64]).MulAdd(archsimd.LoadFloat32x16Slice(bs[48:64]), acc3)
			i += 64
		}
		total += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}
	return float32(math.Acos(float64(total)) / math.Pi), nil
}

func SphericalDistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var total float64
	i := 0
	if hasAVX512 && n >= 32 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
			acc0 = archsimd.LoadFloat64x8Slice(as[0:8]).MulAdd(archsimd.LoadFloat64x8Slice(bs[0:8]), acc0)
			acc1 = archsimd.LoadFloat64x8Slice(as[8:16]).MulAdd(archsimd.LoadFloat64x8Slice(bs[8:16]), acc1)
			acc2 = archsimd.LoadFloat64x8Slice(as[16:24]).MulAdd(archsimd.LoadFloat64x8Slice(bs[16:24]), acc2)
			acc3 = archsimd.LoadFloat64x8Slice(as[24:32]).MulAdd(archsimd.LoadFloat64x8Slice(bs[24:32]), acc3)
			i += 32
		}
		total += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for i <= n-8 {
		// BCE Hint
		as := a[i : i+8 : i+8]
		bs := b[i : i+8 : i+8]
		total += as[0]*bs[0] + as[1]*bs[1] + as[2]*bs[2] + as[3]*bs[3] +
			as[4]*bs[4] + as[5]*bs[5] + as[6]*bs[6] + as[7]*bs[7]
		i += 8
	}

	for ; i < n; i++ {
		total += a[i] * b[i]
	}
	if total > 1.0 {
		total = 1.0
	} else if total < -1.0 {
		total = -1.0
	}
	return math.Acos(total) / math.Pi, nil
}

func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := SphericalDistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := SphericalDistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}
	var sumSquares float64
	for _, val := range v1 {
		sumSquares += float64(val) * float64(val)
	}
	norm := math.Sqrt(sumSquares)
	if norm == 0 {
		copy(normalized, v1)
		return nil
	}
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
