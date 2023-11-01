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

func Normalize(vectors [][]float64) []*mat.VecDense {
	normalizedVectors := make([]*mat.VecDense, len(vectors))

	for i, vec := range vectors {
		v := mat.NewVecDense(len(vec), vec)
		//// Do L2 normalization, ie Euclidean norm
		//norm := mat.Norm(v, 2)
		//
		//if norm != 0 {
		//	v.ScaleVec(1/norm, v)
		//}
		normalizedVectors[i] = v
	}

	return normalizedVectors
}
