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
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// stableFloatPrec is large enough to retain the complete exponent span of a
// float64 product plus the carry bits from a vector-sized reduction.  The
// exact path is used only when a float64 accumulator could overflow before
// cancellation; ordinary embeddings stay on the allocation-free path.
const stableFloatPrec = 8192

// StableL2DistanceSq returns a squared L2 distance in float64.  It scales the
// differences before squaring so a finite norm does not overflow merely
// because an intermediate square is too large.  The final result is allowed
// to be +Inf when the mathematical squared distance is outside float64.
func StableL2DistanceSq[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	scale := 0.0
	for i := range p {
		d := float64(p[i]) - float64(q[i])
		ad := math.Abs(d)
		if math.IsInf(ad, 0) {
			return math.Inf(1), nil
		}
		if ad > scale {
			scale = ad
		}
	}
	if scale == 0 {
		return 0, nil
	}

	sum := 0.0
	for i := range p {
		d := (float64(p[i]) - float64(q[i])) / scale
		sum += d * d
	}
	return scaleTimes(scale, sum, 2), nil
}

// StableL2Distance returns a true L2 distance in float64.  It deliberately
// does not compute sqrt(StableL2DistanceSq): the squared result can be outside
// float64 while the distance itself is representable.
func StableL2Distance[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	scale := 0.0
	for i := range p {
		d := float64(p[i]) - float64(q[i])
		ad := math.Abs(d)
		if math.IsInf(ad, 0) {
			return math.Inf(1), nil
		}
		if ad > scale {
			scale = ad
		}
	}
	if scale == 0 {
		return 0, nil
	}

	sum := 0.0
	for i := range p {
		d := (float64(p[i]) - float64(q[i])) / scale
		sum += d * d
	}
	return scaleTimes(scale, math.Sqrt(sum), 1), nil
}

// StableL1Distance returns the Manhattan distance in float64.  All terms are
// non-negative, but scaling also prevents an early partial sum from becoming
// Inf before the final result is known.
func StableL1Distance[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	scale := 0.0
	for i := range p {
		d := math.Abs(float64(p[i]) - float64(q[i]))
		if math.IsInf(d, 0) {
			return math.Inf(1), nil
		}
		if d > scale {
			scale = d
		}
	}
	if scale == 0 {
		return 0, nil
	}

	sum := 0.0
	for i := range p {
		sum += math.Abs(float64(p[i])-float64(q[i])) / scale
	}
	return scaleTimes(scale, sum, 1), nil
}

// StableInnerProduct returns MatrixOne's negated dot-product convention.  A
// compensated float64 reduction handles normal cancellation. If a product
// could overflow, underflow, or lose precision as a subnormal, or a partial
// sum could overflow, the exact binary-float path accumulates the finite input
// values before conversion back to float64.
func StableInnerProduct[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	maxTerm := 0.0
	needsExact := false
	for i := range p {
		a := float64(p[i])
		b := float64(q[i])
		if math.IsNaN(a) || math.IsNaN(b) {
			return math.NaN(), nil
		}
		if a == 0 || b == 0 {
			continue
		}
		ab := math.Abs(a)
		bb := math.Abs(b)
		if ab > math.MaxFloat64/bb {
			needsExact = true
			continue
		}
		term := math.Abs(a * b)
		if term == 0 || term < math.SmallestNonzeroFloat64*(1<<52) {
			needsExact = true
			continue
		}
		if term > maxTerm {
			maxTerm = term
		}
	}
	if maxTerm > math.MaxFloat64/float64(maxInt(1, len(p))) {
		needsExact = true
	}

	var dot float64
	if needsExact {
		dot = exactInnerProduct(p, q)
	} else {
		var correction float64
		for i := range p {
			term := float64(p[i]) * float64(q[i])
			t := dot + term
			if math.Abs(dot) >= math.Abs(term) {
				correction += (dot - t) + term
			} else {
				correction += (term - t) + dot
			}
			dot = t
		}
		dot += correction
	}
	return -dot, nil
}

// StableCosineDistance computes cosine distance after independently scaling
// both vectors.  This avoids both squared-norm overflow and denominator
// overflow while preserving the existing zero-vector convention.
func StableCosineDistance[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	if len(p) == 0 {
		return 0, nil
	}

	sim, zero, err := stableCosineSimilarity(p, q)
	if err != nil {
		return 0, err
	}
	if zero {
		return 1, nil
	}
	return 1 - sim, nil
}

// StableCosineSimilarity preserves the metric package's zero-vector error,
// while StableCosineDistance uses the historical distance=1 convention.
func StableCosineSimilarity[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	if len(p) == 0 {
		return 0, nil
	}

	sim, zero, err := stableCosineSimilarity(p, q)
	if err != nil {
		return 0, err
	}
	if zero {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	return sim, nil
}

func stableCosineSimilarity[T types.RealNumbers](p, q []T) (float64, bool, error) {
	scaleP := stableMaxAbs(p)
	scaleQ := stableMaxAbs(q)
	if scaleP == 0 || scaleQ == 0 {
		return 0, true, nil
	}

	var dot, dotCorrection float64
	normP, normQ := 0.0, 0.0
	for i := range p {
		a := float64(p[i]) / scaleP
		b := float64(q[i]) / scaleQ
		term := a * b
		t := dot + term
		if math.Abs(dot) >= math.Abs(term) {
			dotCorrection += (dot - t) + term
		} else {
			dotCorrection += (term - t) + dot
		}
		dot = t
		normP += a * a
		normQ += b * b
	}
	dot += dotCorrection

	// The scaled squared norms are at most the vector dimension, so their
	// product is safe in float64. Taking one square root avoids introducing a
	// spurious one-ulp error for identical vectors (sqrt(2)*sqrt(2) can round
	// to 2.0000000000000004).
	denominator := math.Sqrt(normP * normQ)
	if denominator == 0 {
		return 0, true, nil
	}
	sim := dot / denominator
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return sim, false, nil
}

// StableSphericalDistance is the wide-result counterpart of SphericalDistance
// used by the balanced k-means path.  Zero vectors retain acos(0)/pi = 0.5.
func StableSphericalDistance[T types.RealNumbers](p, q []T) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	if len(p) == 0 {
		return 0, nil
	}
	sim, zero, err := stableCosineSimilarity(p, q)
	if err != nil {
		return 0, err
	}
	if zero {
		return 0.5, nil
	}
	return math.Acos(sim) / math.Pi, nil
}

func StableL1Norm[T types.RealNumbers](v []T) (float64, error) {
	scale := stableMaxAbs(v)
	if scale == 0 {
		return 0, nil
	}
	sum := 0.0
	for _, value := range v {
		sum += math.Abs(float64(value)) / scale
	}
	return scaleTimes(scale, sum, 1), nil
}

func StableL2Norm[T types.RealNumbers](v []T) (float64, error) {
	scale := stableMaxAbs(v)
	if scale == 0 {
		return 0, nil
	}
	sum := 0.0
	for _, value := range v {
		x := float64(value) / scale
		sum += x * x
	}
	return scaleTimes(scale, math.Sqrt(sum), 1), nil
}

func StableSummation[T types.RealNumbers](v []T) (float64, error) {
	maxAbs := stableMaxAbs(v)
	if maxAbs == 0 {
		return 0, nil
	}

	needsExact := maxAbs > math.MaxFloat64/float64(maxInt(1, len(v)))
	if needsExact {
		values := make([]float64, len(v))
		for i, value := range v {
			values[i] = float64(value)
		}
		return exactSummation(values), nil
	}

	var sum, correction float64
	for _, value := range v {
		x := float64(value)
		t := sum + x
		if math.Abs(sum) >= math.Abs(x) {
			correction += (sum - t) + x
		} else {
			correction += (x - t) + sum
		}
		sum = t
	}
	return sum + correction, nil
}

// StableMean returns the arithmetic mean in float64. Like StableSummation it
// uses a compensated reduction for ordinary values, but its exact path divides
// while the accumulator is still a big.Float so a finite mean is not lost when
// the unscaled sum itself is outside float64.
func StableMean[T types.RealNumbers](v []T) (float64, error) {
	maxAbs := stableMaxAbs(v)
	if maxAbs == 0 {
		return 0, nil
	}

	if maxAbs > math.MaxFloat64/float64(maxInt(1, len(v))) {
		values := make([]float64, len(v))
		for i, value := range v {
			values[i] = float64(value)
		}
		return exactMean(values), nil
	}

	var sum, correction float64
	for _, value := range v {
		x := float64(value)
		t := sum + x
		if math.Abs(sum) >= math.Abs(x) {
			correction += (sum - t) + x
		} else {
			correction += (x - t) + sum
		}
		sum = t
	}
	return (sum + correction) / float64(len(v)), nil
}

// StableDistanceFn returns a float64-result metric function for SQL and wide
// k-means consumers.  Existing ResolveDistanceFn APIs intentionally remain
// typed and keep their historical result types.
func StableDistanceFn[T types.RealNumbers](metricType MetricType) (func([]T, []T) (float64, error), error) {
	switch metricType {
	case Metric_L2Distance:
		return StableL2Distance[T], nil
	case Metric_L2sqDistance:
		return StableL2DistanceSq[T], nil
	case Metric_InnerProduct:
		return StableInnerProduct[T], nil
	case Metric_CosineDistance:
		return StableCosineDistance[T], nil
	case Metric_L1Distance:
		return StableL1Distance[T], nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

// StableKmeansDistanceFn mirrors ResolveKmeansDistanceFn's dense/spherical
// semantics, but keeps the distance wide until k-means has compared points.
func StableKmeansDistanceFn[T types.RealNumbers](metricType MetricType, spherical bool) (func([]T, []T) (float64, error), bool, error) {
	switch metricType {
	case Metric_L2Distance, Metric_L2sqDistance, Metric_InnerProduct, Metric_CosineDistance, Metric_L1Distance:
	default:
		return nil, false, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
	if spherical {
		switch metricType {
		case Metric_InnerProduct, Metric_CosineDistance:
			return StableSphericalDistance[T], true, nil
		}
	}
	return StableL2Distance[T], false, nil
}

func StableNormalizeL2[T types.RealNumbers](v, normalized []T) error {
	if len(v) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}
	scale := stableMaxAbs(v)
	if scale == 0 {
		copy(normalized, v)
		return nil
	}

	sum := 0.0
	for _, value := range v {
		x := float64(value) / scale
		sum += x * x
	}
	unitNorm := math.Sqrt(sum)
	for i, value := range v {
		normalized[i] = T((float64(value) / scale) / unitNorm)
	}
	return nil
}

func stableMaxAbs[T types.RealNumbers](v []T) float64 {
	maxAbs := 0.0
	for _, value := range v {
		abs := math.Abs(float64(value))
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	return maxAbs
}

func scaleTimes(scale, value float64, power int) float64 {
	if scale == 0 || value == 0 {
		return 0
	}
	mantissa, exponent := math.Frexp(scale)
	if power == 1 {
		return math.Ldexp(value*mantissa, exponent)
	}
	return math.Ldexp(value*mantissa*mantissa, 2*exponent)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func exactSummation(values []float64) float64 {
	sum := new(big.Float).SetPrec(stableFloatPrec)
	for _, value := range values {
		var term big.Float
		term.SetPrec(stableFloatPrec).SetFloat64(value)
		sum.Add(sum, &term)
	}
	result, _ := sum.Float64()
	return result
}

func exactMean(values []float64) float64 {
	sum := new(big.Float).SetPrec(stableFloatPrec)
	for _, value := range values {
		var term big.Float
		term.SetPrec(stableFloatPrec).SetFloat64(value)
		sum.Add(sum, &term)
	}
	count := new(big.Float).SetPrec(stableFloatPrec).SetInt64(int64(len(values)))
	sum.Quo(sum, count)
	result, _ := sum.Float64()
	return result
}

func exactInnerProduct[T types.RealNumbers](p, q []T) float64 {
	sum := new(big.Float).SetPrec(stableFloatPrec)
	for i := range p {
		var left, right, product big.Float
		left.SetPrec(stableFloatPrec).SetFloat64(float64(p[i]))
		right.SetPrec(stableFloatPrec).SetFloat64(float64(q[i]))
		product.Mul(&left, &right)
		sum.Add(sum, &product)
	}
	result, _ := sum.Float64()
	return result
}
