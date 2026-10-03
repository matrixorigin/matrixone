// Copyright 2026 Matrix Origin
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
)

// Universal (build-tag-independent) loop-unrolled scalar distance kernels. They compute in source
// order in T's own type, so the non-SIMD kernels (distance_func.go) wrap them and the SIMD recompute
// calls them when a lane sum returns non-finite.

// L2DistanceUnrolled returns the true L2 distance (sqrt of the squared distance).
func L2DistanceUnrolled[T types.RealNumbers](v1, v2 []T) (T, error) {
	sq, err := L2DistanceSqUnrolled(v1, v2)
	if err != nil {
		return 0, err
	}
	return L2FromSquared(sq)
}

// L2DistanceSqUnrolled returns the squared L2 distance, summed in source order, unrolled by 8.
func L2DistanceSqUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	for i <= n-8 {
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]

		d0 := pp[0] - qq[0]
		d1 := pp[1] - qq[1]
		d2 := pp[2] - qq[2]
		d3 := pp[3] - qq[3]
		d4 := pp[4] - qq[4]
		d5 := pp[5] - qq[5]
		d6 := pp[6] - qq[6]
		d7 := pp[7] - qq[7]

		sum += (d0*d0 + d1*d1) + (d2*d2 + d3*d3) + (d4*d4 + d5*d5) + (d6*d6 + d7*d7)
		i += 8
	}

	for i < n {
		diff := p[i] - q[i]
		sum += diff * diff
		i++
	}

	return sum, nil
}

// L1DistanceUnrolled returns the L1 (Manhattan) distance, summed in source order, unrolled by 8.
func L1DistanceUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	abs := func(x T) T {
		if x < 0 {
			return -x
		}
		return x
	}

	for i <= n-8 {
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]

		sum += abs(pp[0] - qq[0])
		sum += abs(pp[1] - qq[1])
		sum += abs(pp[2] - qq[2])
		sum += abs(pp[3] - qq[3])
		sum += abs(pp[4] - qq[4])
		sum += abs(pp[5] - qq[5])
		sum += abs(pp[6] - qq[6])
		sum += abs(pp[7] - qq[7])
		i += 8
	}

	for i < n {
		sum += abs(p[i] - q[i])
		i++
	}

	return sum, nil
}

// InnerProductUnrolled returns the inner-product distance (-dot) summed in source order, unrolled by 8.
func InnerProductUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	for i <= n-8 {
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]

		sum += pp[0]*qq[0] +
			pp[1]*qq[1] +
			pp[2]*qq[2] +
			pp[3]*qq[3] +
			pp[4]*qq[4] +
			pp[5]*qq[5] +
			pp[6]*qq[6] +
			pp[7]*qq[7]
		i += 8
	}

	for i < n {
		sum += p[i] * q[i]
		i++
	}

	return -sum, nil
}

// CosineDistanceUnrolled returns 1 - cosine similarity, dot and squared norms summed in one pass,
// unrolled by 4, with the float64 norm recompute for subnormal/overflow norms.
func CosineDistanceUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var (
		dotProduct T
		normV1Sq   T
		normV2Sq   T
	)

	n := len(p)
	i := 0

	for i <= n-4 {
		pp := p[i : i+4 : i+4]
		qq := q[i : i+4 : i+4]

		dotProduct += pp[0]*qq[0] + pp[1]*qq[1] + pp[2]*qq[2] + pp[3]*qq[3]
		normV1Sq += pp[0]*pp[0] + pp[1]*pp[1] + pp[2]*pp[2] + pp[3]*pp[3]
		normV2Sq += qq[0]*qq[0] + qq[1]*qq[1] + qq[2]*qq[2] + qq[3]*qq[3]
		i += 4
	}

	for i < n {
		dotProduct += p[i] * q[i]
		normV1Sq += p[i] * p[i]
		normV2Sq += q[i] * q[i]
		i++
	}

	dot := float64(dotProduct)
	denominator := math.Sqrt(float64(normV1Sq)) * math.Sqrt(float64(normV2Sq))
	if !cosineNormsOK(float64(normV1Sq), float64(normV2Sq), smallestNormalOf[T]()) {
		var normP, normQ float64
		var ok bool
		if dot, normP, normQ, ok = cosineRecomputeF64(p, q); !ok {
			return T(0), moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude overflows the float64 domain")
		}
		denominator = math.Sqrt(normP) * math.Sqrt(normQ)
	}

	if denominator == 0 {
		if anyNonZero(p) && anyNonZero(q) {
			return T(0), moerr.NewInternalErrorNoCtx("cosine distance: vector magnitude underflows the element domain")
		}
		return 1.0, nil
	}

	similarity := dot / denominator

	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	distance := 1.0 - similarity

	return T(distance), nil
}

// CosineSimilarityUnrolled returns the cosine similarity, computed like CosineDistanceUnrolled.
func CosineSimilarityUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var (
		dotProduct T
		normV1Sq   T
		normV2Sq   T
	)

	n := len(p)
	i := 0

	for i <= n-4 {
		pp := p[i : i+4 : i+4]
		qq := q[i : i+4 : i+4]

		dotProduct += pp[0]*qq[0] + pp[1]*qq[1] + pp[2]*qq[2] + pp[3]*qq[3]
		normV1Sq += pp[0]*pp[0] + pp[1]*pp[1] + pp[2]*pp[2] + pp[3]*pp[3]
		normV2Sq += qq[0]*qq[0] + qq[1]*qq[1] + qq[2]*qq[2] + qq[3]*qq[3]
		i += 4
	}

	for i < n {
		dotProduct += p[i] * q[i]
		normV1Sq += p[i] * p[i]
		normV2Sq += q[i] * q[i]
		i++
	}

	dot := float64(dotProduct)
	denominator := math.Sqrt(float64(normV1Sq)) * math.Sqrt(float64(normV2Sq))
	if !cosineNormsOK(float64(normV1Sq), float64(normV2Sq), smallestNormalOf[T]()) {
		var normP, normQ float64
		var ok bool
		if dot, normP, normQ, ok = cosineRecomputeF64(p, q); !ok {
			return T(0), moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude overflows the float64 domain")
		}
		denominator = math.Sqrt(normP) * math.Sqrt(normQ)
	}

	if denominator == 0 {
		if anyNonZero(p) && anyNonZero(q) {
			return T(0), moerr.NewInternalErrorNoCtx("cosine similarity: vector magnitude underflows the element domain")
		}
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}

	similarity := dot / denominator

	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	return T(similarity), nil
}

// SphericalDistanceUnrolled returns acos(clamp(dot))/pi, the dot summed in source order, unrolled by 8.
func SphericalDistanceUnrolled[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	dp := T(0)
	n := len(p)
	i := 0

	for i <= n-8 {
		pp := p[i : i+8 : i+8]
		qq := q[i : i+8 : i+8]

		dp += pp[0]*qq[0] +
			pp[1]*qq[1] +
			pp[2]*qq[2] +
			pp[3]*qq[3] +
			pp[4]*qq[4] +
			pp[5]*qq[5] +
			pp[6]*qq[6] +
			pp[7]*qq[7]
		i += 8
	}

	for i < n {
		dp += p[i] * q[i]
		i++
	}

	if dp > 1.0 {
		dp = 1.0
	} else if dp < -1.0 {
		dp = -1.0
	}

	theta := math.Acos(float64(dp))

	return T(theta / math.Pi), nil
}
