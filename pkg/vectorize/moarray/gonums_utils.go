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
	"gonum.org/v1/gonum/mat"
)

// ToDualGonumVectors converts two moarray to two gonum vectors. This is mostly used by
// binary vector operations in external.go. Since we do 2 cast's in one loop, it is more efficient.
func ToDualGonumVectors[T types.RealNumbers](arr1, arr2 []T) (vec1 *mat.VecDense, vec2 *mat.VecDense, err error) {
	if len(arr1) != len(arr2) {
		return nil, nil, moerr.NewArrayInvalidOpNoCtx(len(arr1), len(arr2))
	}

	n := len(arr1)
	_arr1 := make([]float64, n)
	_arr2 := make([]float64, n)

	for i := 0; i < n; i++ {
		_arr1[i] = float64(arr1[i])
		_arr2[i] = float64(arr2[i])
	}

	return mat.NewVecDense(n, _arr1), mat.NewVecDense(n, _arr2), nil
}

func ToGonumVector[T types.RealNumbers](arr1 []T) *mat.VecDense {

	n := len(arr1)
	_arr1 := make([]float64, n)

	for i := 0; i < n; i++ {
		_arr1[i] = float64(arr1[i])
	}

	return mat.NewVecDense(n, _arr1)
}

func ToGonumVectors[T types.RealNumbers](arrays ...[]T) (res []*mat.VecDense, err error) {

	n := len(arrays)
	if n == 0 {
		return res, nil
	}

	array0Dim := len(arrays[0])
	for i := 1; i < n; i++ {
		if len(arrays[i]) != array0Dim {
			return nil, moerr.NewArrayInvalidOpNoCtx(array0Dim, len(arrays[i]))
		}
	}

	res = make([]*mat.VecDense, n)

	for i, arr := range arrays {
		res[i] = ToGonumVector[T](arr)
	}
	return res, nil
}

func ToMoArray[T types.RealNumbers](vec *mat.VecDense) (arr []T) {
	n := vec.Len()
	arr = make([]T, n)
	for i := 0; i < n; i++ {
		arr[i] = T(vec.AtVec(i))
	}
	return
}

func ToMoArrays[T types.RealNumbers](vecs []*mat.VecDense) [][]T {
	moVectors := make([][]T, len(vecs))
	for i, vec := range vecs {
		moVectors[i] = ToMoArray[T](vec)
	}
	return moVectors
}
