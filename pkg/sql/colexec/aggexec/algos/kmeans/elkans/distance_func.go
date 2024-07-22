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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec/algos/kmeans"
	"gonum.org/v1/gonum/mat"
)

// L2Distance is used for L2Distance distance in Euclidean Kmeans.
func L2Distance(v1, v2 *mat.VecDense) float64 {
	diff := mat.NewVecDense(v1.Len(), nil)
	diff.SubVec(v1, v2)
	return mat.Norm(diff, 2)
}

//// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
//// NOTE: spherical distance between two points on a sphere is equal to the
//// angular distance between the two points, scaled by pi.
//// Refs:
//// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
//func SphericalDistance(v1, v2 *mat.VecDense) float64 {
//	// Compute the dot product of the two vectors.
//	// The dot product of two vectors is a measure of their similarity,
//	// and it can be used to calculate the angle between them.
//	dp := mat.Dot(v1, v2)
//
//	// Prevent NaN with acos with loss of precision.
//	if dp > 1.0 {
//		dp = 1.0
//	} else if dp < -1.0 {
//		dp = -1.0
//	}
//
//	theta := math.Acos(dp)
//
//	//To scale the result to the range [0, 1], we divide by Pi.
//	return theta / math.Pi
//
//	// NOTE:
//	// Cosine distance is a measure of the similarity between two vectors. [Not satisfy triangle inequality]
//	// Angular distance is a measure of the angular separation between two points. [Satisfy triangle inequality]
//	// Spherical distance is a measure of the spatial separation between two points on a sphere. [Satisfy triangle inequality]
//}

// resolveDistanceFn returns the distance function corresponding to the distance type
// Distance function should satisfy triangle inequality.
// We use
// - L2Distance distance for L2Distance
// - SphericalDistance for InnerProduct and CosineDistance
func resolveDistanceFn(distType kmeans.DistanceType) (kmeans.DistanceFunction, error) {
	var distanceFunction kmeans.DistanceFunction
	switch distType {
	case kmeans.L2Distance:
		distanceFunction = L2Distance
	//case kmeans.InnerProduct, kmeans.CosineDistance:
	//	distanceFunction = SphericalDistance
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
