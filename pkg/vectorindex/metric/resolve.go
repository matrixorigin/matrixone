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

// resolveRealKernel picks the float32/float64 metric kernel (returning the value
// in its own type T). It is the f32/f64 half of ResolveDistanceFn.
func resolveRealKernel[T types.RealNumbers](metric MetricType) (DistanceFunction[T], error) {
	switch metric {
	case Metric_L2Distance:
		return L2DistanceSq[T], nil // caller must sqrt; squared distance
	case Metric_L2sqDistance:
		return L2DistanceSq[T], nil
	case Metric_InnerProduct:
		return InnerProduct[T], nil
	case Metric_CosineDistance:
		return CosineDistance[T], nil
	case Metric_L1Distance:
		return L1Distance[T], nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

// ResolveDistanceFn is the single distance resolver for search / assign-to-
// centroid (CENTROIDX JOIN / ProductL2), brute force, pairwise and topn. It
// works for any storage element type T (types.ArrayElement) and returns the
// distance in a caller-chosen result type R (types.RealNumbers): pass
// R=float32 for the common path and R=float64 only where f64 precision is
// needed (f64 input, topn ordering values). f32/f64 use the metric kernels;
// bf16/f16/int8/uint8 use the native narrow kernels (which compute in
// float32/int64 and are cast to R — casting their float64 down to float32 is
// bit-identical to a native-float32 kernel, since the intermediate is exact).
//
// IMPORTANT: Don't use it for Elkans Kmeans (use ResolveKmeansDistanceFn).
// NOTE: Metric_L2Distance returns squared L2; callers needing true L2 sqrt the
// result (as GoPairWiseDistance does).
func ResolveDistanceFn[T types.ArrayElement, R types.RealNumbers](metric MetricType) (func(a, b []T) (R, error), error) {
	// Each case resolves the CONCRETE element kernel, then rebinds it to
	// func([]T,...)(R,error) ONCE here (not per call). When R already equals the
	// kernel's native result type (e.g. f32 input with R=float32, or a narrow
	// kernel's float64 with R=float64), the kernel IS that type — return it
	// directly, so the hot loop is a single direct call exactly like before. Only
	// when R differs (a cast is genuinely needed, e.g. narrow float64 -> float32)
	// do we add a thin casting wrapper.
	switch any(*new(T)).(type) {
	case float32:
		fn, err := resolveRealKernel[float32](metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(fn).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []float32) (R, error) { d, e := fn(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	case float64:
		fn, err := resolveRealKernel[float64](metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(fn).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []float64) (R, error) { d, e := fn(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	case types.BF16:
		k, err := resolveBF16Kernel(metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(k).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []types.BF16) (R, error) { d, e := k(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	case types.Float16:
		k, err := resolveF16Kernel(metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(k).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []types.Float16) (R, error) { d, e := k(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	case int8:
		k, err := resolveInt8Kernel(metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(k).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []int8) (R, error) { d, e := k(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	case uint8:
		k, err := resolveUint8Kernel(metric)
		if err != nil {
			return nil, err
		}
		if f, ok := any(k).(func(a, b []T) (R, error)); ok {
			return f, nil
		}
		w := func(a, b []uint8) (R, error) { d, e := k(a, b); return R(d), e }
		return any(w).(func(a, b []T) (R, error)), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("ResolveDistanceFn: unsupported element type")
	}
}

func GoPairWiseDistance[T types.ArrayElement](
	x [][]T,
	y [][]T,
	metric MetricType,
) ([]float32, error) {
	distFn, err := ResolveDistanceFn[T, float32](metric)
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
			res[i*nY+j] = d
		}
	}

	if metric == Metric_L2Distance {
		for i := range res {
			res[i] = float32(math.Sqrt(float64(res[i])))
		}
	}

	return res, nil
}
