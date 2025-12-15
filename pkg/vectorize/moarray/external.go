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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// These functions are exposed externally via SQL API.

func Add[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] + v2[i]
	}
	return res, nil
}

func Subtract[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] - v2[i]
	}
	return res, nil
}

func Multiply[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] * v2[i]
	}
	return res, nil
}

func Divide[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	// pre-check for division by zero
	if slices.Contains(v2, 0) {
		return nil, moerr.NewDivByZeroNoCtx()
	}

	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] / v2[i]
	}
	return res, nil
}

func AddScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] + v2
	}
	return res, nil
}

func SubtractScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] - v2
	}
	return res, nil
}

func MultiplyScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] * v2
	}
	return res, nil
}

func DivideScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	// pre-check for division by zero
	if v2 == 0 {
		return nil, moerr.NewDivByZeroNoCtx()
	}

	res := make([]T, len(v1))
	for i := range v1 {
		res[i] = v1[i] / v2
	}
	return res, nil
}

// Compare returns an integer comparing two arrays/vectors lexicographically.
// TODO: this function might not be correct. we need to compare using tolerance for float values.
// TODO: need to check if we need len(v1)==len(v2) check.
func Compare[T types.RealNumbers](v1, v2 []T) int {
	minLen := len(v1)
	if len(v2) < minLen {
		minLen = len(v2)
	}

	for i := 0; i < minLen; i++ {
		if v1[i] < v2[i] {
			return -1
		} else if v1[i] > v2[i] {
			return 1
		}
	}

	if len(v1) < len(v2) {
		return -1
	} else if len(v1) > len(v2) {
		return 1
	}
	return 0
}

/* ------------ [START] Performance critical functions. ------- */

func InnerProduct[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var sum T
	for i := range v1 {
		sum -= v1[i] * v2[i]
	}

	return sum, nil
}

func L1Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var sum T
	for i := range v1 {
		sum += T(math.Abs(float64(v1[i] - v2[i])))
	}

	return sum, nil
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var sumOfSquares T
	for i := range v1 {
		diff := v1[i] - v2[i]
		sumOfSquares += diff * diff
	}

	return T(math.Sqrt(float64(sumOfSquares))), nil
}

// L2DistanceSq returns the squared L2 distance between two vectors.
// It is an optimized version of L2Distance used in Index Scan
func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var sumOfSquares T
	for i := range v1 {
		diff := v1[i] - v2[i]
		sumOfSquares += diff * diff
	}

	return sumOfSquares, nil
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var norm1, norm2, dotProduct T

	for i := range v1 {
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
		dotProduct += v1[i] * v2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 0, nil
	}

	return 1 - dotProduct/T(math.Sqrt(float64(norm1*norm2))), nil
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	var norm1, norm2, dotProduct T

	for i := range v1 {
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
		dotProduct += v1[i] * v2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 0, nil
	}

	return dotProduct / T(math.Sqrt(float64(norm1*norm2))), nil
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}

	var norm T
	for _, val := range v1 {
		norm += val * val
	}

	if norm == 0 {
		return nil
	}

	norm = T(math.Sqrt(float64(norm)))
	for i, val := range v1 {
		normalized[i] = val / norm
	}

	return nil
}

// L1Norm returns l1 distance to origin.
func L1Norm[T types.RealNumbers](v []T) (T, error) {
	var sum T
	for _, val := range v {
		sum += T(math.Abs(float64(val)))
	}
	return sum, nil
}

// L2Norm returns l2 distance to origin.
func L2Norm[T types.RealNumbers](v []T) (T, error) {
	var sumOfSquares T
	for _, val := range v {
		sumOfSquares += val * val
	}
	return T(math.Sqrt(float64(sumOfSquares))), nil
}

func ScalarOp[T types.RealNumbers](v []T, operation string, scalar T) ([]T, error) {
	switch operation {
	case "+":
		return AddScalar(v, scalar)
	case "-":
		return SubtractScalar(v, scalar)
	case "*":
		return MultiplyScalar(v, scalar)
	case "/":
		return DivideScalar(v, scalar)
	default:
		return nil, moerr.NewInternalErrorNoCtx("scale_vector: invalid operation")
	}
}

/* ------------ [END] Performance critical functions. ------- */

/* ------------ [START] mat.VecDense not supported functions ------- */

func Abs[T types.RealNumbers](v []T) ([]T, error) {
	res := make([]T, len(v))
	for i, val := range v {
		res[i] = T(math.Abs(float64(val)))
	}
	return res, nil
}

func Sqrt[T types.RealNumbers](v []T) ([]T, error) {
	firstNegIdx := slices.IndexFunc(v, func(n T) bool {
		return n < 0
	})
	if firstNegIdx != -1 {
		return nil, moerr.NewInvalidArgNoCtx("Sqrt", v[firstNegIdx])
	}

	res := make([]T, len(v))
	for i, val := range v {
		res[i] = T(math.Sqrt(float64(val)))
	}
	return res, nil
}

func Summation[T types.RealNumbers](v []T) (T, error) {
	var sum T
	for _, val := range v {
		sum += val
	}
	return sum, nil
}

func Cast[I types.RealNumbers, O types.RealNumbers](in []I) (out []O, err error) {
	n := len(in)

	out = make([]O, n)
	for i := range in {
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
