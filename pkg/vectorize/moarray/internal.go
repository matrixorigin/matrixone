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

import "gonum.org/v1/gonum/mat"

// These functions are use internally by the kmeans algorithm and vector index etc. They are not exposed externally.

// NormalizeGonumVector normalizes a vector in place.
// Note that this function is used by the kmeans algorithm. Here, if we get a zero vector, we do not normalize it and
// return it directly. This is because the zero vector is a valid vector in the kmeans algorithm.
func NormalizeGonumVector(vector *mat.VecDense) {
	norm := mat.Norm(vector, 2)
	if norm != 0 {
		vector.ScaleVec(1/norm, vector)
	}
}

func NormalizeGonumVectors(vectors []*mat.VecDense) {
	for i := range vectors {
		NormalizeGonumVector(vectors[i])
	}
}

//// NormalizeMoVecf64 is used only in test functions.
//func NormalizeMoVecf64(vector []float64) []float64 {
//	res := ToGonumVector[float64](vector)
//	//NormalizeGonumVector(res)
//	return ToMoArray[float64](res)
//}
