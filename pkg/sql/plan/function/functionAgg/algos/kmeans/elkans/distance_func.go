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

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"gonum.org/v1/gonum/mat"
)

func L2Distance(v1, v2 *mat.VecDense) float64 {
	diff := mat.NewVecDense(v1.Len(), nil)
	diff.SubVec(v1, v2)
	return mat.Norm(diff, 2)
}

func AngularDistance(v1, v2 *mat.VecDense) float64 {
	// Calculate dot product
	dotProduct := mat.Dot(v1, v2)

	// Calculate norms
	normV1 := mat.Norm(v1, 2)
	normV2 := mat.Norm(v2, 2)

	// Compute cosine similarity
	cosineSimilarity := dotProduct / (normV1 * normV2)

	// Handle potential floating-point inaccuracies that might push
	// cosineSimilarity slightly above 1 or below -1
	if cosineSimilarity > 1.0 {
		cosineSimilarity = 1.0
	} else if cosineSimilarity < -1.0 {
		cosineSimilarity = -1.0
	}

	return 1 - cosineSimilarity
}

// resolveDistanceFn returns the distance function corresponding to the distance type
// Distance function should satisfy triangle inequality.
// We use
// - L2Distance distance for L2
// - AngularDistance for InnerProduct and CosineDistance
// Ref: https://github.com/pgvector/pgvector/blob/c599f92b52d20d1555654803f8f9a4955a97bc11/src/ivfkmeans.c#L155
func resolveDistanceFn(distType kmeans.DistanceType) (kmeans.DistanceFunction, error) {
	var distanceFunction kmeans.DistanceFunction
	switch distType {
	case kmeans.L2:
		distanceFunction = L2Distance
	case kmeans.InnerProduct, kmeans.CosineDistance:
		distanceFunction = AngularDistance
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
