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
	"math"
)

// L2Distance is used for L2Distance distance in Euclidean Kmeans.
func L2Distance(v1, v2 *mat.VecDense) float64 {
	diff := mat.NewVecDense(v1.Len(), nil)
	diff.SubVec(v1, v2)
	return mat.Norm(diff, 2)
}

// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
// NOTE: spherical distance between two points on a sphere is equal to the
// angular distance between the two points, scaled by pi.
// Refs:
// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
// PGVector implementation:
// - vector_spherical_distance definition
// https://github.com/pgvector/pgvector/blob/12aecfb4f593796d6038210d43ba483af7750a00/src/vector.c#L694
// - vector_spherical_distance defined for vector_ip_ops and vector_cosine_ops
// https://github.com/pgvector/pgvector/commit/6df7fa05b243860ed1a3553730246921dc5cb917#diff-89e3a35184b6d3fb74c532f4ac2d56148ea03b0258fd09150c069880244aa609R209
// https://github.com/pgvector/pgvector/commit/6df7fa05b243860ed1a3553730246921dc5cb917#diff-89e3a35184b6d3fb74c532f4ac2d56148ea03b0258fd09150c069880244aa609R201
// - distance_func index is 3 for each of vector_xx_ops
// https://github.com/pgvector/pgvector/commit/6df7fa05b243860ed1a3553730246921dc5cb917#diff-eb91a572fe32e712db27991468bc8b40de22334c3efbd46b975f7dde4157d920R16
// - distance_func resolved inside kmeans
// https://github.com/pgvector/pgvector/commit/6df7fa05b243860ed1a3553730246921dc5cb917#diff-9e86fad100806332720f35abdf3a55232a2b851d3d745703137a899d28f4f9e5R195
// https://github.com/pgvector/pgvector/commit/6df7fa05b243860ed1a3553730246921dc5cb917#diff-9e86fad100806332720f35abdf3a55232a2b851d3d745703137a899d28f4f9e5R259
func SphericalDistance(v1, v2 *mat.VecDense) float64 {
	// Compute the dot product of the two vectors.
	// The dot product of two vectors is a measure of their similarity,
	// and it can be used to calculate the angle between them.
	dp := mat.Dot(v1, v2)

	// Handle precision issues: Clamp the dot product to the range [-1, 1].
	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	// The acos() function is the inverse cosine function.
	// It returns the angle in radians whose cosine is dp.
	theta := math.Acos(dp)

	//To scale the result to the range [0, 1], we divide by Pi.
	return theta / math.Pi

	// NOTE:
	// Cosine distance is a measure of the similarity between two vectors. [Not satisfy triangle inequality]
	// Angular distance is a measure of the angular separation between two points. [Satisfy triangle inequality]
	// Spherical distance is a measure of the spatial separation between two points on a sphere. [Satisfy triangle inequality]
}

// resolveDistanceFn returns the distance function corresponding to the distance type
// Distance function should satisfy triangle inequality.
// We use
// - L2Distance distance for L2Distance
// - SphericalDistance for InnerProduct and CosineDistance
// Ref: https://github.com/pgvector/pgvector/blob/c599f92b52d20d1555654803f8f9a4955a97bc11/src/ivfkmeans.c#L155
func resolveDistanceFn(distType kmeans.DistanceType) (kmeans.DistanceFunction, error) {
	var distanceFunction kmeans.DistanceFunction
	switch distType {
	case kmeans.L2Distance:
		distanceFunction = L2Distance
	case kmeans.InnerProduct, kmeans.CosineDistance:
		distanceFunction = SphericalDistance
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
