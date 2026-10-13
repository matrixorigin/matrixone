//go:build !((amd64 || arm64) && go1.27 && goexperiment.simd)

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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// Non-SIMD distance kernels. Each wraps the build-tag-independent loop-unrolled implementation in
// unrolled_func.go; the SIMD build (distance_func_amd64.go / distance_func_arm64.go) provides its own.

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return L2DistanceUnrolled(v1, v2)
}

func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	return L2DistanceSqUnrolled(p, q)
}

func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	return L1DistanceUnrolled(p, q)
}

func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	return InnerProductUnrolled(p, q)
}

func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	return CosineDistanceUnrolled(p, q)
}

func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	return CosineSimilarityUnrolled(p, q)
}

func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	return SphericalDistanceUnrolled(p, q)
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v {
		v[i] *= scale
	}
}
