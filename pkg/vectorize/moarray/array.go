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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"math"
)

//TODO: Check on optimization.
// 1. Should we return []T or *[]T
// 2. Should we accept v1 *[]T. v1 is a Slice, so I think, it should be pass by reference.
// 3. Later on, use tensor library to improve the performance (may be via GPU)

func Add[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] + v2[i]
	}
	return r, nil
}

func Subtract[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] - v2[i]
	}
	return r, nil
}

func Multiply[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] * v2[i]
	}
	return r, nil
}

func Divide[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		if v2[i] == 0 {
			return nil, moerr.NewDivByZeroNoCtx()
		}
		r[i] = v1[i] / v2[i]
	}
	return r, nil
}

// Compare returns an integer comparing two arrays/vectors lexicographically.
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

func Cast[I types.RealNumbers, O types.RealNumbers](in []I) (out []O) {
	n := len(in)

	out = make([]O, n)
	for i := 0; i < n; i++ {
		out[i] = O(in[i])
	}

	return out
}

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

func Summation[T types.RealNumbers](v []T) float64 {
	n := len(v)
	var sum T = 0
	for i := 0; i < n; i++ {
		sum += v[i]
	}
	return float64(sum)
}

func InnerProduct[T types.RealNumbers](v1, v2 []T) (float64, error) {

	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	n := len(v1)

	var sum T = 0
	for i := 0; i < n; i++ {
		sum += v1[i] * v2[i]
	}
	return float64(sum), nil
}

// L1Norm returns l1 distance to origin.
// The only time, this could throw error is when T = int8 (v[i] is -128)
func L1Norm[T types.RealNumbers](v []T) (float64, error) {
	n := len(v)

	var absVal T
	var err error
	var sum T = 0

	for i := 0; i < n; i++ {
		absVal, err = momath.AbsSigned[T](v[i])
		if err != nil {
			return 0, err
		}

		sum += absVal
	}
	return float64(sum), nil
}

// L2Norm returns l2 distance to origin.
func L2Norm[T types.RealNumbers](v []T) (float64, error) {
	n := len(v)

	var sqrVal T
	var sum T = 0

	for i := 0; i < n; i++ {
		sqrVal = v[i] * v[i]
		sum += sqrVal
	}

	// using math.Sqrt instead of momath.Sqrt() because argument of Sqrt will never be negative for real numbers.
	return math.Sqrt(float64(sum)), nil
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (float32, error) {

	if len(v1) != len(v2) {
		return 0, moerr.NewArrayInvalidOpNoCtx(len(v1), len(v2))
	}

	n := len(v1)

	var innerProduct T = 0
	var normV1 T = 0
	var normV2 T = 0
	for i := 0; i < n; i++ {
		innerProduct += v1[i] * v2[i]
		normV1 += v1[i] * v1[i]
		normV2 += v2[i] * v2[i]
	}

	// using math.Sqrt instead of momath.Sqrt() because argument of Sqrt will never be negative for real numbers.
	// casting it to float32, because cosine_similarity is between 1 and -1.
	return float32(float64(innerProduct) / math.Sqrt(float64(normV1*normV2))), nil
}
