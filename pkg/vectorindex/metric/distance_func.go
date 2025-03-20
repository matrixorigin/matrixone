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
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/gonum/blas/blas64"
	"gonum.org/v1/gonum/mat"
)

func L2DistanceBlas[T types.RealNumbers](v1, v2 []T) float64 {
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		diff := blas32.Vector{
			N:    len(_v1),
			Inc:  1,
			Data: make([]float32, len(_v1)),
		}

		for i := range _v1 {
			diff.Data[i] = _v1[i] - _v2[i]
		}
		return float64(blas32.Nrm2(diff))

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		diff := blas64.Vector{
			N:    len(_v1),
			Inc:  1,
			Data: make([]float64, len(_v1)),
		}

		for i := range _v1 {
			diff.Data[i] = _v1[i] - _v2[i]
		}
		return blas64.Nrm2(diff)
	default:
		fmt.Printf("type. %T\n", v1)
		panic("type mismatch")
	}

	return float64(0)

}

func L2DistanceSqBlas[T types.RealNumbers](v1, v2 []T) float64 {
	var sumOfSquares float64
	switch any(v1).(type) {
	case []float32:
		_v1 := any(v1).([]float32)
		_v2 := any(v2).([]float32)

		for i := range _v1 {
			diff := float64(_v1[i] - _v2[i])
			sumOfSquares += diff * diff
		}
		return sumOfSquares

	case []float64:
		_v1 := any(v1).([]float64)
		_v2 := any(v2).([]float64)

		for i := range _v1 {
			diff := float64(_v1[i] - _v2[i])
			sumOfSquares += diff * diff
		}
		return sumOfSquares
	}

	return float64(0)

}

func InnerProductBlas[T types.RealNumbers](v1, v2 []T) float64 {
	switch any(v1).(type) {
	case []float32:
		_v1 := blas32.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float32)}
		_v2 := blas32.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float32)}

		return float64(-blas32.Dot(_v1, _v2))

	case []float64:
		_v1 := blas64.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float64)}
		_v2 := blas64.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float64)}
		return -blas64.Dot(_v1, _v2)
	}

	return float64(0)

}

func CosineDistanceBlas[T types.RealNumbers](v1, v2 []T) float64 {
	switch any(v1).(type) {
	case []float32:
		_v1 := blas32.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float32)}
		_v2 := blas32.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float32)}

		score := blas32.Dot(_v1, _v2) / (blas32.Nrm2(_v1) * blas32.Nrm2(_v2))
		return float64(1 - score)

	case []float64:
		_v1 := blas64.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float64)}
		_v2 := blas64.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float64)}
		score := blas64.Dot(_v1, _v2) / (blas64.Nrm2(_v1) * blas64.Nrm2(_v2))
		return 1 - score
	}

	return float64(0)

}

func SphericalDistanceBlas[T types.RealNumbers](v1, v2 []T) float64 {
	dp := float64(0)

	switch any(v1).(type) {
	case []float32:
		_v1 := blas32.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float32)}
		_v2 := blas32.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float32)}
		dp = float64(blas32.Dot(_v1, _v2))

	case []float64:
		_v1 := blas64.Vector{N: len(v1), Inc: 1, Data: any(v1).([]float64)}
		_v2 := blas64.Vector{N: len(v2), Inc: 1, Data: any(v2).([]float64)}
		dp = blas64.Dot(_v1, _v2)
	}

	// Prevent NaN with acos with loss of precision.
	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	theta := math.Acos(dp)

	//To scale the result to the range [0, 1], we divide by Pi.
	return theta / math.Pi
}

// blas32.Vector or blas64.Vector
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

// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
// NOTE: spherical distance between two points on a sphere is equal to the
// angular distance between the two points, scaled by pi.
// Refs:
// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
func SphericalDistance(v1, v2 *mat.VecDense) float64 {
	// Compute the dot product of the two vectors.
	// The dot product of two vectors is a measure of their similarity,
	// and it can be used to calculate the angle between them.
	dp := mat.Dot(v1, v2)

	// Prevent NaN with acos with loss of precision.
	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	theta := math.Acos(dp)

	//To scale the result to the range [0, 1], we divide by Pi.
	return theta / math.Pi

	// NOTE:
	// Cosine distance is a measure of the similarity between two vectors. [Not satisfy triangle inequality]
	// Angular distance is a measure of the angular separation between two points. [Satisfy triangle inequality]
	// Spherical distance is a measure of the spatial separation between two points on a sphere. [Satisfy triangle inequality]
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
		distanceFunction = L2DistanceBlas[T]
		normalize = false
	case Metric_InnerProduct:
		distanceFunction = L2DistanceBlas[T]
		normalize = false
	case Metric_CosineDistance:
		distanceFunction = L2DistanceBlas[T]
		normalize = false
	case Metric_L1Distance:
		distanceFunction = L2DistanceBlas[T]
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
		distanceFunction = L2DistanceBlas[T]
		normalize = false
	case Metric_InnerProduct:
		distanceFunction = SphericalDistanceBlas[T]
		normalize = true
	case Metric_CosineDistance:
		distanceFunction = SphericalDistanceBlas[T]
		normalize = true
	case Metric_L1Distance:
		distanceFunction = L2DistanceBlas[T]
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
		distanceFunction = L2DistanceSqBlas[T]
	case Metric_InnerProduct:
		distanceFunction = InnerProductBlas[T]
	case Metric_CosineDistance:
		distanceFunction = CosineDistanceBlas[T]
	case Metric_L1Distance:
		distanceFunction = L2DistanceBlas[T]
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	return distanceFunction, nil
}
