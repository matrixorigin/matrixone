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

package elkans

import "gonum.org/v1/gonum/mat"

func ToGonumsVectors(vectors [][]float64) []*mat.VecDense {
	res := make([]*mat.VecDense, len(vectors))
	for i, vec := range vectors {
		res[i] = mat.NewVecDense(len(vec), vec)
	}
	return res
}

func ToGonumsVector(vectors []float64) *mat.VecDense {
	return mat.NewVecDense(len(vectors), vectors)
}

func ToMOArrays(vectors []*mat.VecDense) [][]float64 {
	moVectors := make([][]float64, len(vectors))
	for i, vec := range vectors {
		moVectors[i] = vec.RawVector().Data
	}
	return moVectors
}
