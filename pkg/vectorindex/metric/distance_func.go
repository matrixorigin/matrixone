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

package metric

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"gonum.org/v1/gonum/mat"
)

// L2Distance is used for L2Distance distance in Euclidean Kmeans.
func L2Distance(v1, v2 *mat.VecDense) float64 {
	diff := mat.NewVecDense(v1.Len(), nil)
	diff.SubVec(v1, v2)
	return mat.Norm(diff, 2)
}

// L2Distance is used for L2Distance distance in Euclidean Kmeans.
func L2DistanceSq(v1, v2 *mat.VecDense) float64 {
	fv1 := v1.RawVector().Data
	fv2 := v2.RawVector().Data

	var sumOfSquares float64
	var difference float64
	for i := range fv1 {
		difference = fv1[i] - fv2[i]
		sumOfSquares += difference * difference
	}
	return sumOfSquares
}

// InnerProduct is used for InnerProduct distance in Spherical Kmeans.
func InnerProduct(v1, v2 *mat.VecDense) float64 {
	// return negative inner product
	return -mat.Dot(v1, v2)
}

// CosineDistance is used for CosineDistance distance in Spherical Kmeans.
func CosineDistance(v1, v2 *mat.VecDense) float64 {
	similarity := mat.Dot(v1, v2) / (mat.Norm(v1, 2) * mat.Norm(v2, 2))
	similarity = min(max(similarity, -1), 1)
	return 1 - similarity
}

// L1Distance is used for L1Distance distance in Manhattan Kmeans.
func L1Distance(v1, v2 *mat.VecDense) float64 {
	diff := mat.NewVecDense(v1.Len(), nil)
	diff.SubVec(v1, v2)
	return mat.Norm(diff, 1)
}

//// IMPORTANT: Spherical Kmeans is not implemented yet.  We cannot use InnerProduct and CosineDistance as similiarity
//// to estimate the centroid by Elkans Kmeans.
//
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

// IMPORTANT: Elkans Kmeans always use L2Distance for clustering.  After getting the centroids, we can use other distance function
// specified by user to assign vector to corresponding centroids (CENTROIDX JOIN / ProductL2).
func ResolveKmeansDistanceFn(metric MetricType) (DistanceFunction, error) {
	var distanceFunction DistanceFunction
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2Distance
	case Metric_InnerProduct:
		distanceFunction = L2Distance
	case Metric_CosineDistance:
		distanceFunction = L2Distance
	case Metric_L1Distance:
		distanceFunction = L2Distance
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}

// ResolveDistanceFn is used for similarity score for search and assign vector to centroids (CENTROIDX JOIN / ProductL2).
// IMPORTANT: Don't use it for Elkans Kmeans
func ResolveDistanceFn(metric MetricType) (DistanceFunction, error) {
	var distanceFunction DistanceFunction
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2DistanceSq
	case Metric_InnerProduct:
		distanceFunction = InnerProduct
	case Metric_CosineDistance:
		distanceFunction = CosineDistance
	case Metric_L1Distance:
		distanceFunction = L1Distance
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
