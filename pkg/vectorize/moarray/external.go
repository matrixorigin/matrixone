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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/viterin/vek"
	"github.com/viterin/vek/vek32"
)

// These functions are exposed externally via SQL API.

func Add[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		res := vek32.Add(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := any(v2).([]float64)
		res := vek.Add(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("Add type not supported")
	}
}

func Subtract[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		res := vek32.Sub(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := any(v2).([]float64)
		res := vek.Sub(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("Subtract type not supported")
	}
}

func Multiply[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		res := vek32.Mul(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := any(v2).([]float64)
		res := vek.Mul(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("Multiply type not supported")
	}
}

func Divide[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	// pre-check for division by zero
	if slices.Contains(v2, 0) {
		return nil, moerr.NewDivByZeroNoCtx()
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		res := vek32.Div(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := any(v2).([]float64)
		res := vek.Div(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("Divide type not supported")
	}
}

func AddScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := float32(v2)
		res := vek32.AddNumber(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := float64(v2)
		res := vek.AddNumber(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("AddScalar type not supported")
	}
}

func SubtractScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := float32(v2)
		res := vek32.SubNumber(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := float64(v2)
		res := vek.SubNumber(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("SubtractScalar type not supported")
	}
}

func MultiplyScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := float32(v2)
		res := vek32.MulNumber(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := float64(v2)
		res := vek.MulNumber(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("MultiplyScalar type not supported")
	}
}

func DivideScalar[T types.RealNumbers](v1 []T, v2 T) ([]T, error) {
	// pre-check for division by zero
	if v2 == 0 {
		return nil, moerr.NewDivByZeroNoCtx()
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := float32(v2)
		res := vek32.DivNumber(_v1, _v2)
		return any(res).([]T), nil

	case []float64:
		_v2 := float64(v2)
		res := vek.DivNumber(_v1, _v2)
		return any(res).([]T), nil

	default:
		panic("DivideScalar type not supported")
	}
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

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		return T(vek32.Dot(_v1, _v2)), nil

	case []float64:
		_v2 := any(v2).([]float64)
		return T(vek.Dot(_v1, _v2)), nil

	default:
		panic("InnerProduct type not supported")
	}
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		return T(vek32.Distance(_v1, _v2)), nil

	case []float64:
		_v2 := any(v2).([]float64)
		return T(vek.Distance(_v1, _v2)), nil

	default:
		panic("L2Distance type not supported")
	}
}

// L2DistanceSq returns the squared L2 distance between two vectors.
// It is an optimized version of L2Distance used in Index Scan
func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		dist := vek32.Distance(_v1, _v2)
		return T(dist * dist), nil

	case []float64:
		_v2 := any(v2).([]float64)
		dist := vek.Distance(_v1, _v2)
		return T(dist * dist), nil

	default:
		panic("L2DistanceSq type not supported")
	}
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	cosine, err := CosineSimilarity(v1, v2)
	if err != nil {
		return 0, err
	}

	return 1 - cosine, nil
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_v2 := any(v2).([]float32)
		return T(vek32.CosineSimilarity(_v1, _v2)), nil

	case []float64:
		_v2 := any(v2).([]float64)
		return T(vek.CosineSimilarity(_v1, _v2)), nil

	default:
		panic("CosineSimilarity type not supported")
	}
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}

	switch _v1 := any(v1).(type) {
	case []float32:
		_normalized := any(normalized).([]float32)
		vek32.DivNumber_Into(_normalized, _v1, vek32.Norm(_v1))

	case []float64:
		_normalized := any(normalized).([]float64)
		vek.DivNumber_Into(_normalized, _v1, vek.Norm(_v1))

	default:
		panic("NormalizeL2 type not supported")
	}

	return nil
}

// L1Norm returns l1 distance to origin.
func L1Norm[T types.RealNumbers](v []T) (T, error) {
	switch _v := any(v).(type) {
	case []float32:
		return T(vek32.ManhattanNorm(_v)), nil

	case []float64:
		return T(vek.ManhattanNorm(_v)), nil

	default:
		panic("L1Norm type not supported")
	}
}

// L2Norm returns l2 distance to origin.
func L2Norm[T types.RealNumbers](v []T) (T, error) {
	switch _v := any(v).(type) {
	case []float32:
		return T(vek32.Norm(_v)), nil

	case []float64:
		return T(vek.Norm(_v)), nil

	default:
		panic("L2Norm type not supported")
	}
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

func Abs[T types.RealNumbers](v []T) (res []T, err error) {
	switch _v := any(v).(type) {
	case []float32:
		res = any(vek32.Abs(_v)).([]T)

	case []float64:
		res = any(vek.Abs(_v)).([]T)

	default:
		panic("Abs type not supported")
	}

	return
}

func Sqrt[T types.RealNumbers](v []T) (res []T, err error) {
	switch _v := any(v).(type) {
	case []float32:
		res = any(vek32.Sqrt(_v)).([]T)

	case []float64:
		res = any(vek.Sqrt(_v)).([]T)

	default:
		panic("Sqrt type not supported")
	}

	return
}

func Summation[T types.RealNumbers](v []T) (T, error) {
	switch _v := any(v).(type) {
	case []float32:
		return T(vek32.Sum(_v)), nil

	case []float64:
		return T(vek.Sum(_v)), nil

	default:
		panic("Sum type not supported")
	}
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
