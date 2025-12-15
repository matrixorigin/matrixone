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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
)

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.L2Distance(v1, v2)
}

func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.L2DistanceSq(v1, v2)
}

func L1Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.L1Distance(v1, v2)
}

func InnerProduct[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.InnerProduct(v1, v2)
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.CosineDistance(v1, v2)
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.CosineSimilarity(v1, v2)
}

// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
// NOTE: spherical distance between two points on a sphere is equal to the
// angular distance between the two points, scaled by pi.
// Refs:
// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
func SphericalDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	// Compute the dot product of the two vectors.
	// The dot product of two vectors is a measure of their similarity,
	// and it can be used to calculate the angle between them.
	var dp T
	for i := range v1 {
		dp += v1[i] * v2[i]
	}

	// Prevent NaN with acos with loss of precision.
	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	theta := math.Acos(float64(dp))

	//To scale the result to the range [0, 1], we divide by Pi.
	return T(theta / math.Pi), nil
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	return moarray.NormalizeL2(v1, normalized)
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v {
		v[i] *= scale
	}
}

// IMPORTANT: Elkans Kmeans always use L2Distance for dense vector or images.  After getting the centroids, we can use other distance function
// specified by user to assign vector to corresponding centroids (CENTROIDX JOIN / ProductL2).

func ResolveKmeansDistanceFn[T types.RealNumbers](metric MetricType, spherical bool) (DistanceFunction[T], bool, error) {
	if spherical {
		return ResolveKmeansDistanceFnForSparse[T](metric)
	}
	return ResolveKmeansDistanceFnForDense[T](metric)
}

func ResolveKmeansDistanceFnForDense[T types.RealNumbers](metric MetricType) (DistanceFunction[T], bool, error) {
	var distanceFunction DistanceFunction[T]
	normalize := false
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_L2sqDistance:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_InnerProduct:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_CosineDistance:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_L1Distance:
		distanceFunction = L2Distance[T]
		normalize = false
	default:
		return nil, normalize, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, normalize, nil
}

// IMPORTANT: Spherical Kmeans always use Spherical Distance / Cosine Similarity for Sparse vector or text embedding (TD-IDF).
// After getting the centroids, we can use other distance function
// specified by user to assign vector to corresponding centroids (CENTROIDX JOIN / ProductL2).
func ResolveKmeansDistanceFnForSparse[T types.RealNumbers](metric MetricType) (DistanceFunction[T], bool, error) {
	var distanceFunction DistanceFunction[T]
	normalize := false
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_L2sqDistance:
		distanceFunction = L2Distance[T]
		normalize = false
	case Metric_InnerProduct:
		distanceFunction = SphericalDistance[T]
		normalize = true
	case Metric_CosineDistance:
		distanceFunction = SphericalDistance[T]
		normalize = true
	case Metric_L1Distance:
		distanceFunction = L2Distance[T]
		normalize = false
	default:
		return nil, normalize, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, normalize, nil
}

// ResolveDistanceFn is used for similarity score for search and assign vector to centroids (CENTROIDX JOIN / ProductL2).
// IMPORTANT: Don't use it for Elkans Kmeans
func ResolveDistanceFn[T types.RealNumbers](metric MetricType) (DistanceFunction[T], error) {
	var distanceFunction DistanceFunction[T]
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2DistanceSq[T]
	case Metric_L2sqDistance:
		distanceFunction = L2DistanceSq[T]
	case Metric_InnerProduct:
		distanceFunction = InnerProduct[T]
	case Metric_CosineDistance:
		distanceFunction = CosineDistance[T]
	case Metric_L1Distance:
		distanceFunction = L1Distance[T]
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
