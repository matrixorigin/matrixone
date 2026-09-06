// Copyright 2021 - 2022 Matrix Origin
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

package aggexec

import (
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type varStdDevExec[
	T float64 | types.Decimal128,
	A types.Ints | types.UInts | types.Floats | types.Decimal64 | types.Decimal128] struct {
	aggExec
	isVar       bool
	isPop       bool
	legacyState bool
	a2f         func(A, int32) float64
	f2t         func(float64, int32) (T, error)
}

func numericToFloat64[A types.Ints | types.UInts | types.Floats](a A, scale int32) float64 {
	return float64(a)
}

func float64ToResult(f float64, scale int32) (float64, error) {
	return f, nil
}

func dec64ToF(d types.Decimal64, scale int32) float64 {
	return types.Decimal64ToFloat64(d, scale)
}

func dec128ToF(d types.Decimal128, scale int32) float64 {
	return types.Decimal128ToFloat64(d, scale)
}

func fToDec128(f float64, scale int32) (types.Decimal128, error) {
	return types.Decimal128FromFloat64(f, 38, scale)
}

func VarStdDevReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		scale := max(int32(12), typs[0].Scale)
		scale = min(scale, typs[0].Scale+6)
		return types.New(types.T_decimal128, 38, scale)
	default:
		return types.T_float64.ToType()
	}
}

func (exec *varStdDevExec[T, A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *varStdDevExec[T, A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *varStdDevExec[T, A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	if exec.legacyState {
		return exec.batchFillLegacy(offset, groups, vectors)
	}

	vec := vectors[0]
	scale := exec.aggInfo.argTypes[0].Scale
	lastX := -1
	var cnts []int64
	var means []float64
	var variances []float64
	var varianceExponents []int64
	var origins []A
	hasExactOrigin := hasExactVarianceOrigin(exec.aggInfo.argTypes[0].Oid)

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := uint64(i) + uint64(offset)
		if vec.IsNull(idx) {
			continue
		}

		x, y := exec.getXY(grp - 1)
		if x != lastX {
			lastX = x
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[0])
			means = vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[1])
			variances = vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[2])
			varianceExponents = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[3])
			if hasExactOrigin {
				origins = vector.MustFixedColNoTypeCheck[A](exec.state[x].vecs[4])
			}
		}

		val := vector.GetFixedAtNoTypeCheck[A](vec, int(idx))
		fv := exec.a2f(val, scale)
		if hasExactOrigin {
			if cnts[y] == 0 {
				origins[y] = val
				fv = 0
			} else {
				var err error
				fv, err = exactVarianceDeviationToFloat64(
					val, origins[y], exec.aggInfo.argTypes[0].Oid, scale)
				if err != nil {
					return err
				}
			}
		}
		mean, variance, varianceExponent, count, err := updateVarianceState(
			means[y], variances[y], varianceExponents[y], cnts[y], fv)
		if err != nil {
			return err
		}

		means[y] = mean
		variances[y] = variance
		varianceExponents[y] = varianceExponent
		cnts[y] = count
	}
	return nil
}

func (exec *varStdDevExec[T, A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *varStdDevExec[T, A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*varStdDevExec[T, A])
	if exec.legacyState != other.legacyState {
		return moerr.NewInternalErrorNoCtx(
			"cannot merge variance aggregate states with different wire layouts")
	}
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}
	if exec.legacyState {
		return exec.batchMergeLegacy(other, offset, groups)
	}

	hasExactOrigin := hasExactVarianceOrigin(exec.aggInfo.argTypes[0].Oid)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[0])
		cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[0])
		means1 := vector.MustFixedColNoTypeCheck[float64](exec.state[x1].vecs[1])
		means2 := vector.MustFixedColNoTypeCheck[float64](other.state[x2].vecs[1])
		variances1 := vector.MustFixedColNoTypeCheck[float64](exec.state[x1].vecs[2])
		variances2 := vector.MustFixedColNoTypeCheck[float64](other.state[x2].vecs[2])
		varianceExponents1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[3])
		varianceExponents2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[3])
		mean2 := means2[y2]
		if hasExactOrigin {
			origins1 := vector.MustFixedColNoTypeCheck[A](exec.state[x1].vecs[4])
			origins2 := vector.MustFixedColNoTypeCheck[A](other.state[x2].vecs[4])
			if cnts1[y1] == 0 {
				origins1[y1] = origins2[y2]
			} else if cnts2[y2] != 0 {
				delta, err := exactVarianceDeviationToFloat64(
					origins2[y2], origins1[y1], exec.aggInfo.argTypes[0].Oid,
					exec.aggInfo.argTypes[0].Scale)
				if err != nil {
					return err
				}
				mean2 += delta
			}
		}
		mean, variance, varianceExponent, count, err := mergeVarianceState(
			means1[y1], variances1[y1], varianceExponents1[y1], cnts1[y1],
			mean2, variances2[y2], varianceExponents2[y2], cnts2[y2],
		)
		if err != nil {
			return err
		}

		means1[y1] = mean
		variances1[y1] = variance
		varianceExponents1[y1] = varianceExponent
		cnts1[y1] = count
	}
	return nil
}

// The legacy representation is count, sum, sum-of-squares. It is retained
// only while a remote pipeline can still be executed by a pre-v35 CN.
func (exec *varStdDevExec[T, A]) batchFillLegacy(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	scale := exec.aggInfo.argTypes[0].Scale
	lastX := -1
	var cnts []int64
	var sums, sumsqs []float64
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := uint64(i) + uint64(offset)
		if vec.IsNull(idx) {
			continue
		}
		x, y := exec.getXY(grp - 1)
		if x != lastX {
			lastX = x
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[0])
			sums = vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[1])
			sumsqs = vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[2])
		}
		fv := exec.a2f(vector.GetFixedAtNoTypeCheck[A](vec, int(idx)), scale)
		sums[y] += fv
		sumsqs[y] += fv * fv
		cnts[y]++
	}
	return nil
}

func (exec *varStdDevExec[T, A]) batchMergeLegacy(other *varStdDevExec[T, A], offset int, groups []uint64) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[0])
		cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[0])
		sums1 := vector.MustFixedColNoTypeCheck[float64](exec.state[x1].vecs[1])
		sums2 := vector.MustFixedColNoTypeCheck[float64](other.state[x2].vecs[1])
		sumsqs1 := vector.MustFixedColNoTypeCheck[float64](exec.state[x1].vecs[2])
		sumsqs2 := vector.MustFixedColNoTypeCheck[float64](other.state[x2].vecs[2])
		sums1[y1] += sums2[y2]
		sumsqs1[y1] += sumsqs2[y2]
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

// decimalDeviationToFloat64 converts a difference, rather than either full
// decimal operand, to float64. That preserves small variation around a large
// decimal offset (for example DECIMAL(30,6) values near 1e12).
//
// The exact decimal subtraction can overflow even when both operands and the
// final variance are representable: DECIMAL(38,20) values +9e17 and -9e17
// have an unscaled difference wider than Decimal128. In that exceptional
// case, use the finite float64 operands instead. This fallback is only used
// when the exact subtraction cannot be represented, so it does not affect
// the small-deviation path above.
func decimalDeviationToFloat64[A types.Ints | types.UInts | types.Floats | types.Decimal64 | types.Decimal128](value, origin A, oid types.T, scale int32) (float64, error) {
	switch oid {
	case types.T_decimal64:
		value64 := any(value).(types.Decimal64)
		origin64 := any(origin).(types.Decimal64)
		delta, deltaScale, err := value64.Sub(origin64, scale, scale)
		if err != nil {
			return types.Decimal64ToFloat64(value64, scale) - types.Decimal64ToFloat64(origin64, scale), nil
		}
		return types.Decimal64ToFloat64(delta, deltaScale), nil
	case types.T_decimal128:
		value128 := any(value).(types.Decimal128)
		origin128 := any(origin).(types.Decimal128)
		delta, deltaScale, err := value128.Sub(origin128, scale, scale)
		if err != nil {
			return types.Decimal128ToFloat64(value128, scale) - types.Decimal128ToFloat64(origin128, scale), nil
		}
		return types.Decimal128ToFloat64(delta, deltaScale), nil
	default:
		return 0, moerr.NewInternalErrorNoCtxf("unsupported decimal type %v", oid)
	}
}

func hasExactVarianceOrigin(oid types.T) bool {
	switch oid {
	case types.T_int64, types.T_uint64, types.T_bit,
		types.T_decimal64, types.T_decimal128:
		return true
	default:
		return false
	}
}

// exactVarianceDeviationToFloat64 converts an exact numeric difference only
// after subtracting the operands in their native domain. This preserves small
// integer variation above 2^53 while still allowing the stable floating-point
// recurrence to represent very large signed and unsigned ranges.
func exactVarianceDeviationToFloat64[A types.Ints | types.UInts | types.Floats | types.Decimal64 | types.Decimal128](
	value, origin A, oid types.T, scale int32,
) (float64, error) {
	switch oid {
	case types.T_int64:
		return signedInt64Deviation(
			any(value).(int64), any(origin).(int64)), nil
	case types.T_uint64, types.T_bit:
		return unsignedUint64Deviation(
			any(value).(uint64), any(origin).(uint64)), nil
	case types.T_decimal64, types.T_decimal128:
		return decimalDeviationToFloat64(value, origin, oid, scale)
	default:
		return 0, moerr.NewInternalErrorNoCtxf(
			"unsupported exact variance type %v", oid)
	}
}

func signedInt64Deviation(value, origin int64) float64 {
	if value >= origin {
		return float64(nonNegativeSignedInt64Difference(value, origin))
	}
	return -float64(nonNegativeSignedInt64Difference(origin, value))
}

func nonNegativeSignedInt64Difference(upper, lower int64) uint64 {
	if lower >= 0 || upper < 0 {
		return uint64(upper - lower)
	}
	// Avoid overflowing int64 when the operands straddle zero. The adjusted
	// negation also handles MinInt64 without taking abs(MinInt64).
	return uint64(upper) + uint64(-(lower + 1)) + 1
}

func unsignedUint64Deviation(value, origin uint64) float64 {
	if value >= origin {
		return float64(value - origin)
	}
	return -float64(origin - value)
}

func (exec *varStdDevExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

// scaledVariance stores value * 2^exponent. The common path keeps exponent
// zero, so ordinary inputs use direct floating-point arithmetic. Exponent
// scaling is activated only when a square or weighted sum would overflow or
// underflow. This lets STDDEV retain a finite result even when its square is
// outside float64's range.
type scaledVariance struct {
	value    float64
	exponent int64
}

func normalizeScaledVariance(value float64, exponent int64) scaledVariance {
	if value == 0 || math.IsInf(value, 0) || math.IsNaN(value) {
		return scaledVariance{value: value}
	}
	fraction, adjustment := math.Frexp(value)
	return scaledVariance{value: fraction, exponent: exponent + int64(adjustment)}
}

func scaleVariance(state scaledVariance, factor float64) scaledVariance {
	if state.value == 0 || factor == 0 {
		return scaledVariance{}
	}
	if state.exponent == 0 {
		product := state.value * factor
		if !math.IsInf(product, 0) && (product != 0 || math.IsNaN(product)) {
			return scaledVariance{value: product}
		}
	}
	if math.IsInf(state.value, 0) || math.IsNaN(state.value) {
		return scaledVariance{value: state.value * factor}
	}
	valueFraction, valueExponent := math.Frexp(state.value)
	factorFraction, factorExponent := math.Frexp(factor)
	return normalizeScaledVariance(
		valueFraction*factorFraction,
		state.exponent+int64(valueExponent)+int64(factorExponent),
	)
}

func squareAndScaleVariance(value float64, valueExponent int64, factor float64) scaledVariance {
	if value == 0 || factor == 0 {
		return scaledVariance{}
	}
	if valueExponent == 0 {
		square := value * value
		product := square * factor
		if !math.IsInf(square, 0) && !math.IsInf(product, 0) &&
			(product != 0 || math.IsNaN(product)) {
			return scaledVariance{value: product}
		}
	}
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return scaledVariance{value: value * value * factor}
	}
	valueFraction, adjustment := math.Frexp(value)
	factorFraction, factorExponent := math.Frexp(factor)
	return normalizeScaledVariance(
		valueFraction*valueFraction*factorFraction,
		2*(valueExponent+int64(adjustment))+int64(factorExponent),
	)
}

func addScaledVariances(left, right scaledVariance) scaledVariance {
	if left.value == 0 {
		return right
	}
	if right.value == 0 {
		return left
	}
	if left.exponent == 0 && right.exponent == 0 {
		sum := left.value + right.value
		if !math.IsInf(sum, 0) {
			return scaledVariance{value: sum}
		}
	}
	if math.IsInf(left.value, 0) || math.IsInf(right.value, 0) ||
		math.IsNaN(left.value) || math.IsNaN(right.value) {
		return scaledVariance{value: left.value + right.value}
	}

	left = normalizeScaledVariance(left.value, left.exponent)
	right = normalizeScaledVariance(right.value, right.exponent)
	if left.exponent < right.exponent {
		left, right = right, left
	}
	shift := right.exponent - left.exponent
	sum := left.value + math.Ldexp(right.value, int(shift))
	return normalizeScaledVariance(sum, left.exponent)
}

func varianceDifference(left, right float64) (float64, int64) {
	difference := left - right
	if !math.IsInf(difference, 0) || math.IsInf(left, 0) || math.IsInf(right, 0) {
		return difference, 0
	}
	// Both operands are finite but their difference is not. Halving before the
	// subtraction retains the difference as a scaled value.
	return left*0.5 - right*0.5, 1
}

func scaledVarianceFloat64(state scaledVariance) float64 {
	return math.Ldexp(state.value, int(state.exponent))
}

func scaledVarianceSqrt(state scaledVariance) float64 {
	if state.value == 0 || math.IsInf(state.value, 0) || math.IsNaN(state.value) {
		return math.Sqrt(state.value)
	}
	state = normalizeScaledVariance(state.value, state.exponent)
	if state.exponent&1 != 0 {
		state.value *= 2
		state.exponent--
	}
	return math.Ldexp(math.Sqrt(state.value), int(state.exponent/2))
}

// updateVarianceState uses Welford's recurrence with a normalized population
// variance. The exponent is zero for normal values; only exceptional squares
// use the scaled representation.
func updateVarianceState(
	mean, variance float64,
	varianceExponent, count int64,
	value float64,
) (float64, float64, int64, int64, error) {
	nextCount := count + 1
	delta := value - mean
	nextMean := mean + delta/float64(nextCount)
	if varianceExponent == 0 && !math.IsInf(delta, 0) {
		weight := float64(count) / float64(nextCount)
		residual := value - nextMean
		product := delta * residual
		if !math.IsInf(product, 0) {
			correction := product / float64(nextCount)
			nextVariance := variance*weight + correction
			if !math.IsInf(nextVariance, 0) &&
				(nextVariance != 0 || (variance == 0 && (count == 0 || delta == 0))) {
				return nextMean, nextVariance, 0, nextCount, nil
			}
		}
	}
	delta, deltaExponent := varianceDifference(value, mean)
	return updateVarianceStateScaled(
		mean, variance, varianceExponent, count, value,
		nextCount, delta, deltaExponent, nextMean,
	)
}

func updateVarianceStateScaled(
	mean, variance float64,
	varianceExponent, count int64,
	value float64,
	nextCount int64,
	delta float64,
	deltaExponent int64,
	nextMean float64,
) (float64, float64, int64, int64, error) {
	if deltaExponent != 0 {
		oldWeight := float64(count) / float64(nextCount)
		nextMean = mean*oldWeight + value/float64(nextCount)
	}

	oldVariance := scaleVariance(
		scaledVariance{value: variance, exponent: varianceExponent},
		float64(count)/float64(nextCount),
	)
	correctionFactor := float64(count) / float64(nextCount)
	correctionFactor /= float64(nextCount)
	correction := squareAndScaleVariance(delta, deltaExponent, correctionFactor)
	nextVariance := addScaledVariances(oldVariance, correction)
	return nextMean, nextVariance.value, nextVariance.exponent, nextCount, nil
}

// mergeVarianceState uses Chan's parallel-variance merge formula with the
// same scaled normalized-variance representation as updateVarianceState.
func mergeVarianceState(
	mean1, variance1 float64,
	varianceExponent1, count1 int64,
	mean2, variance2 float64,
	varianceExponent2, count2 int64,
) (float64, float64, int64, int64, error) {
	if count2 == 0 {
		return mean1, variance1, varianceExponent1, count1, nil
	}
	if count1 == 0 {
		return mean2, variance2, varianceExponent2, count2, nil
	}

	count := count1 + count2
	weight1 := float64(count1) / float64(count)
	weight2 := float64(count2) / float64(count)
	delta := mean2 - mean1
	mean := mean1 + delta*weight2
	if varianceExponent1 == 0 && varianceExponent2 == 0 && !math.IsInf(delta, 0) {
		left := variance1 * weight1
		right := variance2 * weight2
		square := delta * delta
		if !math.IsInf(square, 0) {
			correction := square * weight1 * weight2
			variance := left + right + correction
			if !math.IsInf(variance, 0) &&
				(variance != 0 || (variance1 == 0 && variance2 == 0 && delta == 0)) {
				return mean, variance, 0, count, nil
			}
		}
	}
	delta, deltaExponent := varianceDifference(mean2, mean1)
	return mergeVarianceStateScaled(
		mean1, variance1, varianceExponent1,
		mean2, variance2, varianceExponent2,
		count, weight1, weight2, delta, deltaExponent, mean,
	)
}

func mergeVarianceStateScaled(
	mean1, variance1 float64,
	varianceExponent1 int64,
	mean2, variance2 float64,
	varianceExponent2 int64,
	count int64,
	weight1, weight2 float64,
	delta float64,
	deltaExponent int64,
	mean float64,
) (float64, float64, int64, int64, error) {
	if deltaExponent != 0 {
		mean = mean1*weight1 + mean2*weight2
	}

	left := scaleVariance(
		scaledVariance{value: variance1, exponent: varianceExponent1}, weight1)
	right := scaleVariance(
		scaledVariance{value: variance2, exponent: varianceExponent2}, weight2)
	correction := squareAndScaleVariance(delta, deltaExponent, weight1*weight2)
	variance := addScaledVariances(addScaledVariances(left, right), correction)
	return mean, variance.value, variance.exponent, count, nil
}

func (exec *varStdDevExec[T, A]) getResult(variance float64, varianceExponent, cnt int64) (T, error) {
	state := scaledVariance{value: variance, exponent: varianceExponent}
	if !exec.isPop {
		state = scaleVariance(state, float64(cnt)/float64(cnt-1))
	}

	var result float64
	if exec.isVar {
		result = scaledVarianceFloat64(state)
	} else {
		result = scaledVarianceSqrt(state)
	}

	z, err := exec.f2t(result, exec.aggInfo.retType.Scale)
	return z, err
}

func (exec *varStdDevExec[T, A]) getLegacyResult(sum, sumsq float64, cnt int64) (T, error) {
	avg := sum / float64(cnt)
	denominator := float64(cnt)
	if !exec.isPop {
		denominator--
	}
	result := sumsq/denominator - avg*avg*float64(cnt)/denominator
	if result < 0 {
		result = 0
	}
	if !exec.isVar {
		result = math.Sqrt(result)
	}
	return exec.f2t(result, exec.aggInfo.retType.Scale)
}

func (exec *varStdDevExec[T, A]) Flush() (_ []*vector.Vector, retErr error) {
	if exec.legacyState {
		return exec.flushLegacy()
	}
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i := range vecs {
		var err error
		vecs[i], err = exec.allocation.newVector(resultType)
		if err != nil {
			return nil, err
		}
		if err := vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
			return nil, err
		}
	}

	if exec.IsDistinct() {
		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				cnt := int64(exec.state[i].argCnt[j])
				if cnt <= 1 {
					// cnt == 1 && exec is samp
					if cnt == 0 || !exec.isPop {
						if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
							return nil, err
						}
						continue
					}
					z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
					if err := vector.AppendFixed(vecs[i], z, false, exec.mp); err != nil {
						return nil, err
					}
					continue
				} else {
					mean := float64(0)
					variance := float64(0)
					varianceExponent := int64(0)
					seen := int64(0)
					var origin A
					hasExactOrigin := hasExactVarianceOrigin(exec.aggInfo.argTypes[0].Oid)
					err := exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						fv := exec.a2f(*ptr, exec.aggInfo.argTypes[0].Scale)
						if hasExactOrigin {
							if seen == 0 {
								origin = *ptr
								fv = 0
							} else {
								var derr error
								fv, derr = exactVarianceDeviationToFloat64(
									*ptr, origin, exec.aggInfo.argTypes[0].Oid,
									exec.aggInfo.argTypes[0].Scale)
								if derr != nil {
									return derr
								}
							}
						}
						var fnerr error
						mean, variance, varianceExponent, seen, fnerr = updateVarianceState(
							mean, variance, varianceExponent, seen, fv)
						if fnerr != nil {
							return fnerr
						}
						return nil
					})
					if err != nil {
						return nil, err
					}

					z, err := exec.getResult(variance, varianceExponent, seen)
					if err != nil {
						return nil, err
					}
					if err := vector.AppendFixed(vecs[i], z, false, exec.mp); err != nil {
						return nil, err
					}
				}
			}
		}
	} else {
		for i := range vecs {
			cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[i].vecs[0])
			variances := vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[2])
			varianceExponents := vector.MustFixedColNoTypeCheck[int64](exec.state[i].vecs[3])
			for j, cnt := range cnts {
				if cnt <= 1 {
					// cnt == 1 && exec is samp
					if cnt == 0 || !exec.isPop {
						if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
							return nil, err
						}
						continue
					}
					z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
					if err := vector.AppendFixed(vecs[i], z, false, exec.mp); err != nil {
						return nil, err
					}
				} else {
					result, err := exec.getResult(variances[j], varianceExponents[j], cnt)
					if err != nil {
						return nil, err
					}
					if err := vector.AppendFixed(vecs[i], result, false, exec.mp); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return vecs, nil
}

func (exec *varStdDevExec[T, A]) flushLegacy() (_ []*vector.Vector, retErr error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i := range vecs {
		var err error
		vecs[i], err = exec.allocation.newVector(resultType)
		if err != nil {
			return nil, err
		}
		if err = vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
			return nil, err
		}
	}
	for i := range vecs {
		for j := 0; j < int(exec.state[i].length); j++ {
			var cnt int64
			if exec.IsDistinct() {
				cnt = int64(exec.state[i].argCnt[j])
			} else {
				cnt = vector.MustFixedColNoTypeCheck[int64](exec.state[i].vecs[0])[j]
			}
			if cnt <= 1 {
				if cnt == 0 || !exec.isPop {
					if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
						return nil, err
					}
					continue
				}
				z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
				if err := vector.AppendFixed(vecs[i], z, false, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			sum, sumsq := 0.0, 0.0
			if exec.IsDistinct() {
				err := exec.state[i].iter(uint16(j), func(k []byte) error {
					value := exec.a2f(*util.UnsafeFromBytes[A](k[kAggArgPrefixSz:]), exec.aggInfo.argTypes[0].Scale)
					sum += value
					sumsq += value * value
					return nil
				})
				if err != nil {
					return nil, err
				}
			} else {
				sum = vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[1])[j]
				sumsq = vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[2])[j]
			}
			result, err := exec.getLegacyResult(sum, sumsq, cnt)
			if err != nil {
				return nil, err
			}
			if err := vector.AppendFixed(vecs[i], result, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func makeVarStdDevExec(mp *mpool.MPool,
	isVar bool, isPop bool,
	aggID int64, isDistinct bool, param types.Type, legacyStates ...bool) AggFuncExec {
	legacyState := len(legacyStates) > 0 && legacyStates[0]
	switch param.Oid {
	case types.T_int8:
		return newVarStdDevExec[float64, int8](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_int16:
		return newVarStdDevExec[float64, int16](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_int32:
		return newVarStdDevExec[float64, int32](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_int64:
		return newVarStdDevExec[float64, int64](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_uint8:
		return newVarStdDevExec[float64, uint8](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_uint16:
		return newVarStdDevExec[float64, uint16](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_uint32:
		return newVarStdDevExec[float64, uint32](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_uint64:
		return newVarStdDevExec[float64, uint64](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_bit:
		return newVarStdDevExec[float64, uint64](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_float32:
		return newVarStdDevExec[float64, float32](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_float64:
		return newVarStdDevExec[float64, float64](mp, isVar, isPop, aggID, isDistinct, param, numericToFloat64, float64ToResult, legacyState)
	case types.T_decimal64:
		return newVarStdDevExec[types.Decimal128, types.Decimal64](mp, isVar, isPop, aggID, isDistinct, param, dec64ToF, fToDec128, legacyState)
	case types.T_decimal128:
		return newVarStdDevExec[types.Decimal128, types.Decimal128](mp, isVar, isPop, aggID, isDistinct, param, dec128ToF, fToDec128, legacyState)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for var/stddev", param.Oid))
	}
}

func newVarStdDevExec[T float64 | types.Decimal128, A types.Ints | types.UInts | types.Floats | types.Decimal64 | types.Decimal128](mp *mpool.MPool, isVar bool, isPop bool, aggID int64, isDistinct bool, param types.Type, a2f func(A, int32) float64, f2t func(float64, int32) (T, error), legacyState bool) AggFuncExec {
	var exec varStdDevExec[T, A]
	exec.mp = mp
	exec.isVar = isVar
	exec.isPop = isPop
	exec.legacyState = legacyState
	exec.a2f = a2f
	exec.f2t = f2t

	retType := VarStdDevReturnType([]types.Type{param})
	stateTypes := []types.Type{
		types.T_int64.ToType(),
		types.T_float64.ToType(),
		types.T_float64.ToType(),
	}
	if !legacyState {
		stateTypes = append(stateTypes, types.T_int64.ToType())
	}
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    retType,
		stateTypes: stateTypes,
		emptyNull:  false,
		saveArg:    isDistinct,
	}
	if !legacyState && hasExactVarianceOrigin(param.Oid) {
		exec.aggInfo.stateTypes = append(exec.aggInfo.stateTypes, param)
	}
	return &exec
}

func makeVarPopExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, legacyState ...bool) AggFuncExec {
	return makeVarStdDevExec(mp, true, true, aggID, isDistinct, param, len(legacyState) > 0 && legacyState[0])
}

func makeVarSampleExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, legacyState ...bool) AggFuncExec {
	return makeVarStdDevExec(mp, true, false, aggID, isDistinct, param, len(legacyState) > 0 && legacyState[0])
}

func makeStdDevPopExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, legacyState ...bool) AggFuncExec {
	return makeVarStdDevExec(mp, false, true, aggID, isDistinct, param, len(legacyState) > 0 && legacyState[0])
}

func makeStdDevSampleExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, legacyState ...bool) AggFuncExec {
	return makeVarStdDevExec(mp, false, false, aggID, isDistinct, param, len(legacyState) > 0 && legacyState[0])
}
