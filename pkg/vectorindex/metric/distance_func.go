//go:build !(go1.26 && amd64)

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
)

/*
func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	dist, err := L2DistanceSq(v1, v2)
	if err != nil {
		return dist, err
	}

	return T(math.Sqrt(float64(dist))), nil
}
*/

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	dist, err := L2DistanceSq(v1, v2)
	if err != nil {
		return dist, err
	}

	return T(math.Sqrt(float64(dist))), nil
}

/*
func L2DistanceSq[T types.RealNumbers](v1, v2 []T) (T, error) {
	var sumOfSquares T
	for i := range v1 {
		diff := v1[i] - v2[i]
		sumOfSquares += diff * diff
	}
	return sumOfSquares, nil

}
*/

// L2SquareDistanceUnrolled calculates the L2 square distance using loop unrolling.
// This optimization can improve performance for large vectors by reducing loop
// overhead and allowing for better instruction-level parallelism.
func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	// Process the bulk of the data in chunks of 8.
	for i <= n-8 {
		// BCE Hint
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

	// Handle the remaining elements if the vector size is not a multiple of 8.
	for i < n {
		diff := p[i] - q[i]
		sum += diff * diff
		i++
	}

	return sum, nil
}

// L1Distance calculates the L1 (Manhattan) distance between two vectors.
/*
func L1Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	var sum T
	for i := range v1 {
		sum += math.Abs(v1[i] - v2[i])
	}
	return sum, nil

}
*/

// L1DistanceUnrolled calculates the L1 distance using loop unrolling for optimization.
// It processes 8 elements per iteration to reduce loop overhead and improve performance
// on large vectors. It also uses an inline 'abs' for potential speed gains.
func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	// Helper function for inline absolute value.
	// A good compiler might inline this automatically.
	abs := func(x T) T {
		if x < 0 {
			return -x
		}
		return x
	}

	// Process the bulk of the data in chunks of 8.
	for i <= n-8 {
		// BCE Hint
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

	// Handle the remaining 0 to 7 elements.
	for i < n {
		sum += abs(p[i] - q[i])
		i++
	}

	return sum, nil
}

// InnerProduct calculates the inner product (dot product) of two vectors.
// This is a clear, readable, and idiomatic Go implementation.
/*
func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	var sum T
	for i := range p {
		sum += p[i] * q[i]
	}

	return -sum, nil
}
*/

// InnerProductUnrolled calculates the inner product using loop unrolling.
// This can significantly improve performance for large vectors by reducing
// loop overhead and enabling better CPU instruction scheduling.
func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum T
	n := len(p)
	i := 0

	// Process the bulk of the data in chunks of 8.
	for i <= n-8 {
		// BCE Hint
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

	// Handle the remaining 0 to 7 elements.
	for i < n {
		sum += p[i] * q[i]
		i++
	}

	return -sum, nil
}

// CosineDistance calculates the cosine distance between two vectors using generics.
//
// Formula:
// Cosine Distance = 1 - Cosine Similarity
// Cosine Similarity = (v1 · v2) / (||v1|| * ||v2||)
//
// This implementation uses loop unrolling to optimize the calculation of the
// dot product (v1 · v2) and the squared L2 norms (||v1||², ||v2||²) in a single pass.
// This improves performance by reducing loop overhead and maximizing CPU cache efficiency.
func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) == 0 {
		// The distance is undefined for empty vectors. Returning 0 and no error is a common convention.
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

	// Process the bulk of the data in chunks of 4.
	// Unrolling by 4 provides a good balance between performance gain and code readability.
	// We calculate all three components in one loop to improve data locality.
	for i <= n-4 {
		// BCE Hint
		pp := p[i : i+4 : i+4]
		qq := q[i : i+4 : i+4]

		dotProduct += pp[0]*qq[0] + pp[1]*qq[1] + pp[2]*qq[2] + pp[3]*qq[3]
		normV1Sq += pp[0]*pp[0] + pp[1]*pp[1] + pp[2]*pp[2] + pp[3]*pp[3]
		normV2Sq += qq[0]*qq[0] + qq[1]*qq[1] + qq[2]*qq[2] + qq[3]*qq[3]
		i += 4
	}

	// Handle the remaining 0 to 3 elements.
	for i < n {
		dotProduct += p[i] * q[i]
		normV1Sq += p[i] * p[i]
		normV2Sq += q[i] * q[i]
		i++
	}

	// The denominator is the product of the L2 norms (Euclidean lengths).
	// We must cast to float64 to use the standard library's math.Sqrt.
	denominator := math.Sqrt(float64(normV1Sq)) * math.Sqrt(float64(normV2Sq))

	// Handle the edge case of a zero-magnitude vector. If the denominator is zero,
	// the cosine similarity is undefined. A distance of 1.0 is a common convention,
	// implying the vectors are maximally dissimilar (orthogonal).
	if denominator == 0 {
		// This can happen if one or both vectors are all zeros.
		return 1.0, nil
	}

	// Calculate cosine similarity.
	similarity := float64(dotProduct) / denominator

	// handle precision issues. Clamp the cosine simliarity to the range [-1, 1].
	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	// Cosine distance is 1 minus the similarity.
	// The result is cast back to the original type T.
	distance := 1.0 - similarity

	return T(distance), nil
}

// CosineSimilarity calculates the cosine similarity between two vectors using generics.
//
// Formula:
// Cosine Distance = 1 - Cosine Similarity
// Cosine Similarity = (v1 · v2) / (||v1|| * ||v2||)
//
// This implementation uses loop unrolling to optimize the calculation of the
// dot product (v1 · v2) and the squared L2 norms (||v1||², ||v2||²) in a single pass.
// This improves performance by reducing loop overhead and maximizing CPU cache efficiency.
func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) == 0 {
		// The distance is undefined for empty vectors. Returning 0 and no error is a common convention.
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

	// Process the bulk of the data in chunks of 4.
	// Unrolling by 4 provides a good balance between performance gain and code readability.
	// We calculate all three components in one loop to improve data locality.
	for i <= n-4 {
		// BCE Hint
		pp := p[i : i+4 : i+4]
		qq := q[i : i+4 : i+4]

		dotProduct += pp[0]*qq[0] + pp[1]*qq[1] + pp[2]*qq[2] + pp[3]*qq[3]
		normV1Sq += pp[0]*pp[0] + pp[1]*pp[1] + pp[2]*pp[2] + pp[3]*pp[3]
		normV2Sq += qq[0]*qq[0] + qq[1]*qq[1] + qq[2]*qq[2] + qq[3]*qq[3]
		i += 4
	}

	// Handle the remaining 0 to 3 elements.
	for i < n {
		dotProduct += p[i] * q[i]
		normV1Sq += p[i] * p[i]
		normV2Sq += q[i] * q[i]
		i++
	}

	// The denominator is the product of the L2 norms (Euclidean lengths).
	// We must cast to float64 to use the standard library's math.Sqrt.
	denominator := math.Sqrt(float64(normV1Sq)) * math.Sqrt(float64(normV2Sq))

	if denominator == 0 {
		// This can happen if one or both vectors are all zeros.
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}

	// Calculate cosine similarity.
	similarity := float64(dotProduct) / denominator

	// handle precision issues. Clamp the cosine simliarity to the range [-1, 1].
	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	return T(similarity), nil
}

// SphericalDistance is used for InnerProduct and CosineDistance in Spherical Kmeans.
// NOTE: spherical distance between two points on a sphere is equal to the
// angular distance between the two points, scaled by pi.
// Refs:
// https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version
func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	if len(p) != len(q) {
		return T(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	// Compute the dot product of the two vectors.
	// The dot product of two vectors is a measure of their similarity,
	// and it can be used to calculate the angle between them.
	dp := T(0)

	n := len(p)
	i := 0

	// Process the bulk of the data in chunks of 8.
	for i <= n-8 {
		// BCE Hint
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

	// Handle the remaining 0 to 7 elements.
	for i < n {
		dp += p[i] * q[i]
		i++
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

	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}

	// Compute the norm of the vector
	var sumSquares float64
	for _, val := range v1 {
		sumSquares += float64(val) * float64(val)
	}
	norm := math.Sqrt(sumSquares)
	if norm == 0 {
		copy(normalized, v1)
		return nil
	}

	// Divide each element by the norm
	for i, val := range v1 {
		normalized[i] = T(float64(val) / norm)
	}

	return nil
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v {
		v[i] *= scale
	}
}
