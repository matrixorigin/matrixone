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

	return float64(ret), err
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.L2Distance[T](v1, v2)
	return float64(ret), err
}

// L2DistanceSq returns the squared L2 distance between two vectors.
// It is an optimized version of L2Distance used in Index Scan
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
	return float64(ret), err
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (float64, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	ret, err := metric.CosineSimilarity[T](v1, v2)
	if err != nil {
		return 0, err
	}
	cosine := float64(ret)

	// NOTE: Downcast the float64 cosine_similarity to float32 and check if it is
	// 1.0 or -1.0 to avoid precision issue.
	//
	//  Example for corner case:
	// - cosine_similarity(a,a) = 1:
	// - Without downcasting check, we get the following results:
	//   cosine_similarity( [0.46323407, 23.498016, 563.923, 56.076736, 8732.958] ,
	//					    [0.46323407, 23.498016, 563.923, 56.076736, 8732.958] ) =   0.9999999999999998
	// - With downcasting, we get the following results:
	//   cosine_similarity( [0.46323407, 23.498016, 563.923, 56.076736, 8732.958] ,
	//					    [0.46323407, 23.498016, 563.923, 56.076736, 8732.958] ) =   1
	//
	//  Reason:
	// The reason for this check is
	// 1. gonums mat.Dot, mat.Norm returns float64. In other databases, we mostly do float32 operations.
	// 2. float64 operations are not exact.
	// mysql> select 76586261.65813679/(8751.35770370157 *8751.35770370157);
	//+-----------------------------------------------------------+
	//| 76586261.65813679 / (8751.35770370157 * 8751.35770370157) |
	//+-----------------------------------------------------------+
	//|                                            1.000000000000 |
	//+-----------------------------------------------------------+
	//mysql> select cast(76586261.65813679 as double)/(8751.35770370157 * 8751.35770370157);
	//+---------------------------------------------------------------------------+
	//| cast(76586261.65813679 as double) / (8751.35770370157 * 8751.35770370157) |
	//+---------------------------------------------------------------------------+
	//|                                                        0.9999999999999996 |
	//+---------------------------------------------------------------------------+
	// 3. We only need to handle the case for 1.0 and -1.0 with float32 precision.
	//    Rest of the cases can have float64 precision.

	cosinef32 := float32(cosine)
	if cosinef32 == 1 {
		cosine = 1
	} else if cosinef32 == -1 {
		cosine = -1
	}

	return cosine, nil
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
