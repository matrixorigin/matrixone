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

	scale, hasNaN, hasInf := stableDifferenceScale(p, q)
	if hasNaN {
		return math.NaN(), nil
	}
	if hasInf {
		return math.Inf(1), nil
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

	scale, hasNaN, hasInf := stableDifferenceScale(p, q)
	if hasNaN {
		return math.NaN(), nil
	}
	if hasInf {
		return math.Inf(1), nil
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

	scale, hasNaN, hasInf := stableDifferenceScale(p, q)
	if hasNaN {
		return math.NaN(), nil
	}
	if hasInf {
		return math.Inf(1), nil
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
	hasPosInf, hasNegInf := false, false
	for i := range p {
		a := float64(p[i])
		b := float64(q[i])
		term := a * b
		if math.IsNaN(a) || math.IsNaN(b) || math.IsNaN(term) {
			return math.NaN(), nil
		}
		if math.IsInf(term, 1) {
			if math.IsInf(a, 0) || math.IsInf(b, 0) {
				hasPosInf = true
			} else {
				needsExact = true
			}
			continue
		}
		if math.IsInf(term, -1) {
			if math.IsInf(a, 0) || math.IsInf(b, 0) {
				hasNegInf = true
			} else {
				needsExact = true
			}
			continue
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
		absTerm := math.Abs(term)
		if absTerm == 0 || absTerm < math.SmallestNonzeroFloat64*(1<<52) {
			needsExact = true
			continue
		}
		if absTerm > maxTerm {
			maxTerm = absTerm
		}
	}
	if hasPosInf || hasNegInf {
		if hasPosInf && hasNegInf {
			return math.NaN(), nil
		}
		if hasPosInf {
			return math.Inf(-1), nil
		}
		return math.Inf(1), nil
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
			if !isFiniteStableFloat(t) {
				needsExact = true
				break
			}
			if math.Abs(dot) >= math.Abs(term) {
				correction += (dot - t) + term
			} else {
				correction += (term - t) + dot
			}
			if !isFiniteStableFloat(correction) {
				needsExact = true
				break
			}
			dot = t
		}
		if !needsExact {
			dot += correction
			if !isFiniteStableFloat(dot) {
				needsExact = true
			}
		}
		if needsExact {
			dot = exactInnerProduct(p, q)
		}
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
	summaryP := summarizeStableValues(p)
	summaryQ := summarizeStableValues(q)
	if summaryP.hasNaN || summaryP.hasPosInf || summaryP.hasNegInf ||
		summaryQ.hasNaN || summaryQ.hasPosInf || summaryQ.hasNegInf {
		return math.NaN(), false, nil
	}
	scaleP := summaryP.maxAbs
	scaleQ := summaryQ.maxAbs
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
	summary := summarizeStableValues(v)
	if summary.hasNaN {
		return math.NaN(), nil
	}
	if summary.hasPosInf || summary.hasNegInf {
		return math.Inf(1), nil
	}
	scale := summary.maxAbs
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
	summary := summarizeStableValues(v)
	if summary.hasNaN {
		return math.NaN(), nil
	}
	if summary.hasPosInf || summary.hasNegInf {
		return math.Inf(1), nil
	}
	scale := summary.maxAbs
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
	summary := summarizeStableValues(v)
	if summary.hasNaN || (summary.hasPosInf && summary.hasNegInf) {
		return math.NaN(), nil
	}
	if summary.hasPosInf {
		return math.Inf(1), nil
	}
	if summary.hasNegInf {
		return math.Inf(-1), nil
	}
	maxAbs := summary.maxAbs
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
		if !isFiniteStableFloat(t) {
			return exactSummation(stableFloat64Values(v)), nil
		}
		if math.Abs(sum) >= math.Abs(x) {
			correction += (sum - t) + x
		} else {
			correction += (x - t) + sum
		}
		if !isFiniteStableFloat(correction) {
			return exactSummation(stableFloat64Values(v)), nil
		}
		sum = t
	}
	result := sum + correction
	if !isFiniteStableFloat(result) {
		return exactSummation(stableFloat64Values(v)), nil
	}
	return result, nil
}

// StableMean returns the arithmetic mean in float64. Like StableSummation it
// uses a compensated reduction for ordinary values, but its exact path divides
// while the accumulator is still a big.Float so a finite mean is not lost when
// the unscaled sum itself is outside float64.
func StableMean[T types.RealNumbers](v []T) (float64, error) {
	summary := summarizeStableValues(v)
	if summary.hasNaN || (summary.hasPosInf && summary.hasNegInf) {
		return math.NaN(), nil
	}
	if summary.hasPosInf {
		return math.Inf(1), nil
	}
	if summary.hasNegInf {
		return math.Inf(-1), nil
	}
	maxAbs := summary.maxAbs
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
		if !isFiniteStableFloat(t) {
			return exactMean(stableFloat64Values(v)), nil
		}
		if math.Abs(sum) >= math.Abs(x) {
			correction += (sum - t) + x
		} else {
			correction += (x - t) + sum
		}
		if !isFiniteStableFloat(correction) {
			return exactMean(stableFloat64Values(v)), nil
		}
		sum = t
	}
	result := (sum + correction) / float64(len(v))
	if !isFiniteStableFloat(result) {
		return exactMean(stableFloat64Values(v)), nil
	}
	return result, nil
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
	summary := summarizeStableValues(v)
	if summary.hasNaN || summary.hasPosInf || summary.hasNegInf {
		norm := math.NaN()
		if !summary.hasNaN {
			norm = math.Inf(1)
		}
		for i, value := range v {
			normalized[i] = T(float64(value) / norm)
		}
		return nil
	}
	scale := summary.maxAbs
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

type stableValueSummary struct {
	maxAbs    float64
	hasNaN    bool
	hasPosInf bool
	hasNegInf bool
}

func summarizeStableValues[T types.RealNumbers](v []T) stableValueSummary {
	summary := stableValueSummary{}
	for _, value := range v {
		x := float64(value)
		switch {
		case math.IsNaN(x):
			summary.hasNaN = true
		case math.IsInf(x, 1):
			summary.hasPosInf = true
			summary.maxAbs = math.Inf(1)
		case math.IsInf(x, -1):
			summary.hasNegInf = true
			summary.maxAbs = math.Inf(1)
		default:
			if abs := math.Abs(x); abs > summary.maxAbs {
				summary.maxAbs = abs
			}
		}
	}
	return summary
}

func stableDifferenceScale[T types.RealNumbers](p, q []T) (scale float64, hasNaN, hasInf bool) {
	for i := range p {
		d := float64(p[i]) - float64(q[i])
		switch {
		case math.IsNaN(d):
			hasNaN = true
		case math.IsInf(d, 0):
			hasInf = true
		default:
			if ad := math.Abs(d); ad > scale {
				scale = ad
			}
		}
	}
	return scale, hasNaN, hasInf
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

func isFiniteStableFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func stableFloat64Values[T types.RealNumbers](v []T) []float64 {
	values := make([]float64, len(v))
	for i, value := range v {
		values[i] = float64(value)
	}
	return values
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
