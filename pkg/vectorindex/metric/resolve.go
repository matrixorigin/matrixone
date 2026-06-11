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

// NOTE: This file is intentionally UNTAGGED (no //go:build constraint).
// The distance KERNELS live in build-tag alternatives — distance_func.go
// (scalar, !(amd64 && goexperiment.simd)) and distance_func_amd64.go (SIMD,
// amd64 && go1.26 && goexperiment.simd) — so only one compiles per build. The
// resolver / orchestration helpers below are build-tag-independent (they just
// pick and call a kernel), so they must NOT live in a tagged file, or they
// would vanish on the SIMD build and break every caller (kmeans / brute_force /
// ivf). They belong here, where they compile in every build.

package metric

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

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
		// Elkans Kmeans always uses true L2Distance regardless of user metric.
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
// IMPORTANT: Don't use it for Elkans Kmeans.
// NOTE: Metric_L2Distance returns L2DistanceSq (squared distance). Callers that need true L2
// must apply sqrt to each result afterwards (as GoPairWiseDistance does).
func ResolveDistanceFn[T types.RealNumbers](metric MetricType) (DistanceFunction[T], error) {
	var distanceFunction DistanceFunction[T]
	switch metric {
	case Metric_L2Distance:
		distanceFunction = L2DistanceSq[T] // caller must sqrt; see function doc above
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

func GoPairWiseDistance[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
) ([]float32, error) {
	distFn, err := ResolveDistanceFn[T](metric)
	if err != nil {
		return nil, err
	}

	nX := len(x)
	nY := len(y)
	res := make([]float32, nX*nY)
	for i := 0; i < nX; i++ {
		for j := 0; j < nY; j++ {
			d, err := distFn(x[i], y[j])
			if err != nil {
				return nil, err
			}
			res[i*nY+j] = float32(d)
		}
	}

	if metric == Metric_L2Distance {
		for i := range res {
			res[i] = float32(math.Sqrt(float64(res[i])))
		}
	}

	return res, nil
}
