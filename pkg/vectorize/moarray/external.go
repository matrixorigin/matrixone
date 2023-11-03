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
	"gonum.org/v1/gonum/mat"
	"math"
)

// These functions are exposed externally via SQL API.

func Add[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return nil, err
	}

	vec1.AddVec(vec1, vec2)
	return ToMoArray[T](vec1), nil
}

func Subtract[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return nil, err
	}

	vec1.SubVec(vec1, vec2)
	return ToMoArray[T](vec1), nil
}

func Multiply[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return nil, err
	}

	vec1.MulElemVec(vec1, vec2)
	return ToMoArray[T](vec1), nil
}

func Divide[T types.RealNumbers](v1, v2 []T) ([]T, error) {
	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return nil, err
	}

	// pre-check for division by zero
	for i := 0; i < vec2.Len(); i++ {
		if vec2.AtVec(i) == 0 {
			return nil, moerr.NewDivByZeroNoCtx()
		}
	}

	vec1.DivElemVec(vec1, vec2)
	return ToMoArray[T](vec1), nil
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

func InnerProduct[T types.RealNumbers](v1, v2 []T) (float64, error) {

	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return 0, err
	}

	return mat.Dot(vec1, vec2), nil
}

func L2Distance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return 0, err
	}

	diff := mat.NewVecDense(vec1.Len(), nil)
	diff.SubVec(vec1, vec2)

	return math.Sqrt(mat.Dot(diff, diff)), nil
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (float64, error) {
	cosineSimilarity, err := CosineSimilarity[T](v1, v2)
	if err != nil {
		return 0, err
	}

	return 1 - cosineSimilarity, nil
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (float64, error) {

	vec1, vec2, err := ToDualGonumVectors[T](v1, v2)
	if err != nil {
		return 0, err
	}

	dotProduct := mat.Dot(vec1, vec2)

	normVec1 := mat.Norm(vec1, 2)
	normVec2 := mat.Norm(vec2, 2)

	if normVec1 == 0 || normVec2 == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine_similarity: one of the vectors is zero")
	}

	cosineSimilarity := dotProduct / (normVec1 * normVec2)

	// Handle precision issues. Clamp the cosine_similarity to the range [-1, 1].
	if cosineSimilarity > 1.0 {
		cosineSimilarity = 1.0
	} else if cosineSimilarity < -1.0 {
		cosineSimilarity = -1.0
	}

	return cosineSimilarity, nil
}

func NormalizeL2[T types.RealNumbers](v1 []T) ([]T, error) {

	vec := ToGonumVector[T](v1)

	norm := mat.Norm(vec, 2)
	if norm == 0 {
		return nil, moerr.NewInternalErrorNoCtx("normalize_l2: cannot normalize a zero vector")
	}

	vec.ScaleVec(1/norm, vec)

	return ToMoArray[T](vec), nil
}

// L1Norm returns l1 distance to origin.
func L1Norm[T types.RealNumbers](v []T) (float64, error) {
	vec := ToGonumVector[T](v)

	return mat.Norm(vec, 1), nil
}

// L2Norm returns l2 distance to origin.
func L2Norm[T types.RealNumbers](v []T) (float64, error) {
	vec := ToGonumVector[T](v)

	return mat.Norm(vec, 2), nil
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

/* ------------ [END] mat.VecDense not supported functions ------- */

//TODO: Check on optimization.
// 1. Should we return []T or *[]T
// 2. Should we accept v1 *[]T. v1 is a Slice, so I think, it should be pass by reference.
