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

package moarray

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/gonum/blas/blas64"
)

// These functions are exposed externally via SQL API.

func Add[T types.RealNumbers](p, q []T) ([]T, error) {
	if len(p) != len(q) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(p), len(q))
	}

	i := 0
	n := len(p)
	x := make([]T, n)
	for i <= n-8 {

		// BCE hint
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]
		xx := x[i : i+8 : i+8]

		xx[0] = pp[0] + qq[0]
		xx[1] = pp[1] + qq[1]
		xx[2] = pp[2] + qq[2]
		xx[3] = pp[3] + qq[3]
		xx[4] = pp[4] + qq[4]
		xx[5] = pp[5] + qq[5]
		xx[6] = pp[6] + qq[6]
		xx[7] = pp[7] + qq[7]
		i += 8
	}

	for i < n {
		x[i] = p[i] + q[i]
		i++
	}
	return x, nil
}

func Subtract[T types.RealNumbers](p, q []T) ([]T, error) {
	if len(p) != len(q) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(p), len(q))
	}

	i := 0
	n := len(p)
	x := make([]T, n)
	for i <= n-8 {

		// BCE hint
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]
		xx := x[i : i+8 : i+8]

		xx[0] = pp[0] - qq[0]
		xx[1] = pp[1] - qq[1]
		xx[2] = pp[2] - qq[2]
		xx[3] = pp[3] - qq[3]
		xx[4] = pp[4] - qq[4]
		xx[5] = pp[5] - qq[5]
		xx[6] = pp[6] - qq[6]
		xx[7] = pp[7] - qq[7]
		i += 8
	}

	for i < n {
		x[i] = p[i] - q[i]
		i++
	}
	return x, nil
}

func Multiply[T types.RealNumbers](p, q []T) ([]T, error) {
	if len(p) != len(q) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(p), len(q))
	}

	i := 0
	n := len(p)
	x := make([]T, n)
	for i <= n-8 {

		// BCE hint
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]
		xx := x[i : i+8 : i+8]

		xx[0] = pp[0] * qq[0]
		xx[1] = pp[1] * qq[1]
		xx[2] = pp[2] * qq[2]
		xx[3] = pp[3] * qq[3]
		xx[4] = pp[4] * qq[4]
		xx[5] = pp[5] * qq[5]
		xx[6] = pp[6] * qq[6]
		xx[7] = pp[7] * qq[7]
		i += 8
	}

	for i < n {
		x[i] = p[i] * q[i]
		i++
	}
	return x, nil
}

func Divide[T types.RealNumbers](p, q []T) ([]T, error) {
	if len(p) != len(q) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(p), len(q))
	}

	// pre-check for division by zero
	for i := 0; i < len(q); i++ {
		if q[i] == 0 {
			return nil, moerr.NewDivByZeroNoCtx()
		}
	}

	i := 0
	n := len(p)
	x := make([]T, n)
	for i <= n-8 {

		// BCE hint
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]
		xx := x[i : i+8 : i+8]

		xx[0] = pp[0] / qq[0]
		xx[1] = pp[1] / qq[1]
		xx[2] = pp[2] / qq[2]
		xx[3] = pp[3] / qq[3]
		xx[4] = pp[4] / qq[4]
		xx[5] = pp[5] / qq[5]
		xx[6] = pp[6] / qq[6]
		xx[7] = pp[7] / qq[7]
		i += 8
	}

	for i < n {
		x[i] = p[i] / q[i]
		i++
	}
	return x, nil
}

/* ------------ [START] Performance critical functions. ------- */

func InnerProduct[T types.RealNumbers](v1, v2 []T) (float64, error) {

	ret, err := metric.InnerProduct(v1, v2)
	if err != nil {
		return 0, err
	}

	// Vector distances are a float32 domain (usearch/cuvs return float32, and this is the
	// per-row scalar twin of that index metric), so round a float64 base's result into that
	// domain too, keeping the scalar and the index in the same precision (#29040 / #29050).
	// This is float32-domain agreement, not bitwise equality -- see metric.RoundDistanceToElemDomain
	// for the residual float32-ULP boundary case. No-op for a float32 base.
	return metric.RoundDistanceToElemDomain(float64(ret)), err
}

// L1Distance returns the Manhattan distance sum|a-b|. Like its L2 siblings it checks the
// dimensions here so a mismatch surfaces as the user-facing ARRAY_INVALID_OP naming both
// dimensions, instead of the kernel's bare internal error (whose text also differs
// between the SIMD and scalar builds).
func L1Distance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.L1Distance[T](v1, v2)
	// Round a float64 base into the float32 distance domain so every vector distance is uniform
	// (see InnerProduct); no-op for a float32 base (#29040 / #29050).
	return metric.RoundDistanceToElemDomain(float64(ret)), err
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.L2Distance[T](v1, v2)
	// Round a float64 base into the float32 distance domain so the scalar and index agree in
	// that domain -- float32-domain agreement, not bitwise (see InnerProduct and
	// metric.RoundDistanceToElemDomain); no-op for a float32 base (#29040 / #29050).
	return metric.RoundDistanceToElemDomain(float64(ret)), err
}

// L2DistanceSq returns the squared L2 distance between two vectors.
// It is an optimized version of L2Distance used in Index Scan.
//
// Unlike the other scalar distances this returns the RAW float64 square and does NOT round into the
// float32 domain. IVF's entries query uses l2_distance_sq as its internal squared intermediate and
// then takes sqrt + rounds once in scoreFromQuantized (DistanceTransformIvfflat). Rounding the square
// here would make IVF compute float32(sqrt(float32(sq))) while the scalar l2_distance computes
// float32(sqrt(sq)) -- a boundary mismatch (e.g. [1.00000006,0,0] vs 0: 1.0 vs 1.0000001192092896)
// that changes projected values, predicates, and ordering. The final exposed L2 value is rounded by
// L2Distance / DistanceTransform* after the sqrt (#29040 / #29050).
func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.L2DistanceSq[T](v1, v2)
	return float64(ret), err
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.CosineDistance[T](v1, v2)
	// Round a float64 base into the float32 distance domain so the scalar and index agree in
	// that domain -- float32-domain agreement, not bitwise (see InnerProduct and
	// metric.RoundDistanceToElemDomain); no-op for a float32 base (#29040 / #29050).
	return metric.RoundDistanceToElemDomain(float64(ret)), err
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.CosineSimilarity[T](v1, v2)
	if err != nil {
		return 0, err
	}

	// Vector metrics are a float32 domain (see InnerProduct), so round the float64 cosine to
	// float32. This also subsumes the historical 1.0/-1.0 corner-case snap: gonum's f64 mat.Dot /
	// mat.Norm make cosine_similarity(a,a) read as 0.9999999999999998, but float32(that) == 1, so an
	// identical-vector similarity is exactly 1 without a special case. No-op for a float32 base.
	return metric.RoundDistanceToElemDomain(float64(ret)), nil
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	return metric.NormalizeL2(v1, normalized)
}

// L1Norm returns l1 distance to origin.
func L1Norm[T types.RealNumbers](v []T) (float64, error) {
	switch any(v).(type) {
	case []float32:
		_v := blas32.Vector{N: len(v), Inc: 1, Data: any(v).([]float32)}
		return float64(blas32.Asum(_v)), nil
	case []float64:
		_v := blas64.Vector{N: len(v), Inc: 1, Data: any(v).([]float64)}
		return blas64.Asum(_v), nil
	default:
		return 0, moerr.NewInternalErrorNoCtx("L1Norm type not supported")
	}
}

// L2Norm returns l2 distance to origin.
func L2Norm[T types.RealNumbers](v []T) (float64, error) {
	switch any(v).(type) {
	case []float32:
		_v := blas32.Vector{N: len(v), Inc: 1, Data: any(v).([]float32)}
		return float64(blas32.Nrm2(_v)), nil
	case []float64:
		_v := blas64.Vector{N: len(v), Inc: 1, Data: any(v).([]float64)}
		return blas64.Nrm2(_v), nil
	default:
		return 0, moerr.NewInternalErrorNoCtx("L2Norm type not supported")
	}
}

func ScalarOp[T types.RealNumbers](v []T, operation string, scalar float64) ([]T, error) {

	ret := make([]T, len(v))

	switch operation {
	case "+", "-":
		//TODO: optimize this in future.
		if operation == "+" {
			for i := range v {
				ret[i] = v[i] + T(scalar)
			}
		} else {
			for i := range v {
				ret[i] = v[i] - T(scalar)
			}
		}
	case "*", "/":
		var scale float64
		if operation == "/" {
			if scalar == 0 {
				return nil, moerr.NewDivByZeroNoCtx()
			}
			scale = float64(1) / scalar
		} else {
			scale = scalar
		}

		for i := range v {
			ret[i] = v[i] * T(scale)
		}
	default:
		return nil, moerr.NewInternalErrorNoCtx("scale_vector: invalid operation")
	}

	// check overflow
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, moerr.NewInternalErrorNoCtx("vector contains infinity values")
		}
	}
	return ret, nil
}

/* ------------ [END] Performance critical functions. ------- */

/* ------------ [START] mat.VecDense not supported functions ------- */

func Abs[T types.RealNumbers](v []T) (res []T, err error) {
	n := len(v)
	res = make([]T, n)
	for i := 0; i < n; i++ {
		res[i], err = momath.AbsSigned[T](v[i])
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func Sqrt[T types.RealNumbers](v []T) (res []float64, err error) {
	n := len(v)
	res = make([]float64, n)
	for i := 0; i < n; i++ {
		res[i], err = momath.Sqrt(float64(v[i]))
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func Summation[T types.RealNumbers](v []T) (float64, error) {
	n := len(v)
	var sum float64 = 0
	for i := 0; i < n; i++ {
		sum += float64(v[i])
	}
	return sum, nil
}

func Cast[I types.RealNumbers, O types.RealNumbers](in []I) (out []O, err error) {
	n := len(in)

	out = make([]O, n)
	for i := 0; i < n; i++ {
		out[i] = O(in[i])
	}

	return out, nil
}

/** Slice Array **/

// SubArrayFromLeft Slice from left to right, starting from 0
func SubArrayFromLeft[T types.RealNumbers](s []T, offset int64) []T {
	totalLen := int64(len(s))
	if offset > totalLen {
		return []T{}
	}
	return s[offset:]
}

// SubArrayFromRight Cut slices from right to left, starting from 1
func SubArrayFromRight[T types.RealNumbers](s []T, offset int64) []T {
	totalLen := int64(len(s))
	if offset > totalLen {
		return []T{}
	}
	return s[totalLen-offset:]
}

// SubArrayFromLeftWithLength Cut the slice with length from left to right, starting from 0
func SubArrayFromLeftWithLength[T types.RealNumbers](s []T, offset int64, length int64) []T {
	if offset < 0 {
		return []T{}
	}
	return subArrayOffsetLen(s, offset, length)
}

// SubArrayFromRightWithLength From right to left, cut the slice with length from 1
func SubArrayFromRightWithLength[T types.RealNumbers](s []T, offset int64, length int64) []T {
	return subArrayOffsetLen(s, -offset, length)
}

func subArrayOffsetLen[T types.RealNumbers](s []T, offset int64, length int64) []T {
	totalLen := int64(len(s))
	if offset < 0 {
		offset += totalLen
		if offset < 0 {
			return []T{}
		}
	}
	if offset >= totalLen {
		return []T{}
	}

	if length <= 0 {
		return []T{}
	} else {
		end := offset + length
		if end > totalLen {
			end = totalLen
		}
		return s[offset:end]
	}
}

/* ------------ [END] mat.VecDense not supported functions ------- */
