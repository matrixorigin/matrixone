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
	"os"
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// hasAVX512 gates the top kernel tier. TESTING-ONLY override: set
// MO_METRIC_NO_AVX512=1 to force AVX512-capable CPUs down to the scalar float
// kernels or the AVX2 (then scalar) narrow kernels, covering the lower tiers.
// Package-level var initializers run before every init() selector, so the
// override is seen by all kernel-selection init() funcs in this package.
var (
	hasAVX512 = archsimd.X86.AVX512() && os.Getenv("MO_METRIC_NO_AVX512") == ""
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
		prefix, end := l2DistanceSqFloat32AVX512(a, b)
		sum += prefix
		i = end
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
		prefix, end := innerProductFloat32AVX512(a, b)
		total += prefix
		i = end
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
		prefix, end := l2DistanceSqFloat64AVX512(a, b)
		sum += prefix
		i = end
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
		prefix, end := innerProductFloat64AVX512(a, b)
		total += prefix
		i = end
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
		prefix, end := l1DistanceFloat32AVX512(a, b)
		sum += prefix
		i = end
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
		prefix, end := l1DistanceFloat64AVX512(a, b)
		sum += prefix
		i = end
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
		dot, normA, normB, i = cosineAccumFloat32AVX512(a, b)
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
	return float32(cosineDistClamped(float64(dot), den)), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}
	var dot, normA, normB float64
	i := 0
	if n >= 8 && hasAVX512 {
		dot, normA, normB, i = cosineAccumFloat64AVX512(a, b)
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
	return cosineDistClamped(dot, den), nil
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
		dot, normA, normB, i = cosineAccumFloat32AVX512(a, b)
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
	// Clamp to [-1,1]: float32 accumulation can push the quotient a hair
	// outside (e.g. 1.000000119) and mirror the scalar CosineSimilarity.
	sim := float64(dot) / den
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return float32(sim), nil
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
		dot, normA, normB, i = cosineAccumFloat64AVX512(a, b)
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
	// Clamp to [-1,1]: float accumulation can push the quotient a hair
	// outside and mirror the scalar CosineSimilarity.
	sim := dot / den
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return sim, nil
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
		prefix, end := innerProductFloat32AVX512(a, b)
		total += prefix
		i = end
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
		prefix, end := innerProductFloat64AVX512(a, b)
		total += prefix
		i = end
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
