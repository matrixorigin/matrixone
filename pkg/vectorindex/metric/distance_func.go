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
	"github.com/viterin/vek"
	"github.com/viterin/vek/vek32"
)

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return moarray.L2Distance(v1, v2)
}

func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)
		dist := vek32.Distance(_v1, _v2)

		return T(dist * dist), nil

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)
		dist := vek.Distance(_v1, _v2)

		return T(dist * dist), nil

	default:
		return 0, moerr.NewInternalErrorNoCtx("L2DistanceSq type not supported")
	}
}

func L1Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		return T(vek32.ManhattanDistance(_v1, _v2)), nil

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		return T(vek.ManhattanDistance(_v1, _v2)), nil

	default:
		return 0, moerr.NewInternalErrorNoCtx("L1Distance type not supported")
	}
}

func InnerProduct[T types.RealNumbers](v1, v2 []T) (T, error) {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		return T(-vek32.Dot(_v1, _v2)), nil

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		return T(-vek.Dot(_v1, _v2)), nil

	default:
		return 0, moerr.NewInternalErrorNoCtx("InnerProduct type not supported")
	}
}

func CosineDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		return T(1 - vek32.CosineSimilarity(_v1, _v2)), nil

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		return T(1 - vek.CosineSimilarity(_v1, _v2)), nil

	default:
		return 0, moerr.NewInternalErrorNoCtx("CosineDistance type not supported")
	}
}

func CosineSimilarity[T types.RealNumbers](v1, v2 []T) (T, error) {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		return T(vek32.CosineSimilarity(_v1, _v2)), nil

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		return T(vek.CosineSimilarity(_v1, _v2)), nil

	default:
		return 0, moerr.NewInternalErrorNoCtx("CosineSimilarity type not supported")
	}
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
	dp := float64(0)

	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		dp = float64(vek32.Dot(_v1, _v2))

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		dp = vek.Dot(_v1, _v2)

	default:
		return 0, moerr.NewInternalErrorNoCtx("SphericalDistance type not supported")
	}

	// Prevent NaN with acos with loss of precision.
	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	theta := math.Acos(dp)

	//To scale the result to the range [0, 1], we divide by Pi.
	return T(theta / math.Pi), nil
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_normalized := any(normalized).([]float32)

		copy(_normalized, _v1)
		vek32.DivNumber_Inplace(_normalized, vek32.Norm(_v1))

	case []float64:
		_v1 := any(v1).([]float64)
		_normalized := any(normalized).([]float64)

		copy(_normalized, _v1)
		vek.DivNumber_Inplace(_normalized, vek.Norm(_v1))

	default:
		return moerr.NewInternalErrorNoCtx("NormalizeL2 type not supported")
	}

	return nil
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
