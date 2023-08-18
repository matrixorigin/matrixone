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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
)

//TODO: Check on optimization.
// 1. Should we return []T or *[]T
// 2. Should we accept v1 *[]T. v1 is a Slice, so I think, it should be pass by reference.
// 3. Later on, use tensor library to improve the performance (may be via GPU)

func Add[T types.RealNumbers](v1, v2 []T) []T {
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] + v2[i]
	}
	return r
}

func Subtract[T types.RealNumbers](v1, v2 []T) []T {
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] - v2[i]
	}
	return r
}

func Multiply[T types.RealNumbers](v1, v2 []T) []T {
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] * v2[i]
	}
	return r
}

func Divide[T types.RealNumbers](v1, v2 []T) []T {
	n := len(v1)
	r := make([]T, n)
	for i := 0; i < n; i++ {
		r[i] = v1[i] / v2[i]
	}
	return r
}

func Compare[T types.RealNumbers](v1, v2 []T) int {
	if len(v1) != len(v2) {
		// NOTE: We will not compare Arrays with different dimension.
		panic(moerr.NewInvalidArgNoCtx("dimension mismatch", fmt.Sprintf("%d != %d", len(v1), len(v2))))
	}
	for i := 0; i < len(v1); i++ {
		if v1[i] == v2[i] {
			continue
		} else if v1[i] > v2[i] {
			return +1
		} else {
			return -1
		}
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
	var sum float64 = 0
	for i := 0; i < n; i++ {
		sum = sum + float64(v[i])
	}
	return sum
}

func InnerProduct[T types.RealNumbers](v1, v2 []T) (float64, error) {

	if len(v1) != len(v2) {
		return 0, moerr.NewInvalidArgNoCtx("dimension mismatch", fmt.Sprintf("%d != %d", len(v1), len(v2)))
	}

	n := len(v1)

	var productVal T
	var sum float64 = 0

	for i := 0; i < n; i++ {
		productVal = v1[i] * v2[i]
		sum = sum + float64(productVal)
	}
	return sum, nil
}

func L1Norm[T types.RealNumbers](v []T) (float64, error) {
	n := len(v)

	var absVal T
	var err error
	var sum float64 = 0

	for i := 0; i < n; i++ {
		absVal, err = momath.AbsSigned[T](v[i])
		if err != nil {
			return 0, err
		}

		sum = sum + float64(absVal)
	}
	return sum, nil
}

func L2Norm[T types.RealNumbers](v []T) (float64, error) {
	n := len(v)

	var sqrVal T
	var sum float64 = 0

	for i := 0; i < n; i++ {
		sqrVal = v[i] * v[i]
		sum = sum + float64(sqrVal)
	}

	return momath.Sqrt(sum)
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (float32, error) {

	if len(v1) != len(v2) {
		return 0, moerr.NewInvalidArgNoCtx("dimension mismatch", fmt.Sprintf("%d != %d", len(v1), len(v2)))
	}

	a, err := InnerProduct[T](v1, v2)
	if err != nil {
		return 0, err
	}

	b, err := L2Norm[T](v1)
	if err != nil {
		return 0, err
	}

	c, err := L2Norm[T](v2)
	if err != nil {
		return 0, err
	}

	sum := a / (b * c)
	return float32(sum), nil
}
