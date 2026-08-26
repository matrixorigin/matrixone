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
		return AvgReturnType(typs)
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
	var origins []A
	isDecimal := exec.aggInfo.argTypes[0].Oid == types.T_decimal64 || exec.aggInfo.argTypes[0].Oid == types.T_decimal128

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
			if isDecimal {
				origins = vector.MustFixedColNoTypeCheck[A](exec.state[x].vecs[3])
			}
		}

		val := vector.GetFixedAtNoTypeCheck[A](vec, int(idx))
		fv := exec.a2f(val, scale)
		if isDecimal {
			if cnts[y] == 0 {
				origins[y] = val
				fv = 0
			} else {
				var err error
				fv, err = decimalDeviationToFloat64(val, origins[y], exec.aggInfo.argTypes[0].Oid, scale)
				if err != nil {
					return err
				}
			}
		}
		mean, variance, count, err := updateVarianceState(means[y], variances[y], cnts[y], fv)
		if err != nil {
			return err
		}

		means[y] = mean
		variances[y] = variance
		cnts[y] = count
	}
	return nil
}

func (exec *varStdDevExec[T, A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *varStdDevExec[T, A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*varStdDevExec[T, A])
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}
	if exec.legacyState {
		return exec.batchMergeLegacy(other, offset, groups)
	}

	isDecimal := exec.aggInfo.argTypes[0].Oid == types.T_decimal64 || exec.aggInfo.argTypes[0].Oid == types.T_decimal128
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
		mean2 := means2[y2]
		if isDecimal {
			origins1 := vector.MustFixedColNoTypeCheck[A](exec.state[x1].vecs[3])
			origins2 := vector.MustFixedColNoTypeCheck[A](other.state[x2].vecs[3])
			if cnts1[y1] == 0 {
				origins1[y1] = origins2[y2]
			} else if cnts2[y2] != 0 {
				delta, err := decimalDeviationToFloat64(origins2[y2], origins1[y1], exec.aggInfo.argTypes[0].Oid, exec.aggInfo.argTypes[0].Scale)
				if err != nil {
					return err
				}
				mean2 += delta
			}
		}
		mean, variance, count, err := mergeVarianceState(
			means1[y1], variances1[y1], cnts1[y1],
			mean2, variances2[y2], cnts2[y2],
		)
		if err != nil {
			return err
		}

		means1[y1] = mean
		variances1[y1] = variance
		cnts1[y1] = count
	}
	return nil
}

// The legacy representation is count, sum, sum-of-squares. It is retained
// only while a remote pipeline can still be executed by a pre-v30 CN.
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

func (exec *varStdDevExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

// updateVarianceState uses Welford's online algorithm and stores the
// normalized population variance rather than M2. Unlike M2, this state stays
// finite whenever the requested VAR_POP result is finite.
func updateVarianceState(mean, variance float64, count int64, value float64) (float64, float64, int64, error) {
	nextCount := count + 1
	delta := value - mean
	if err := float64OfCheck(0, 0, delta); err != nil {
		return 0, 0, 0, err
	}
	nextMean := mean + delta/float64(nextCount)
	if err := float64OfCheck(0, 0, nextMean); err != nil {
		return 0, 0, 0, err
	}
	// delta*(value-nextMean) is the M2 increment. Divide it by nextCount
	// before multiplying so a finite final variance does not overflow in an
	// intermediate product.
	increment := scaledProductQuotient(delta, value-nextMean, float64(nextCount))
	nextVariance := variance*float64(count)/float64(nextCount) + increment
	if err := float64OfCheck(0, 0, nextVariance); err != nil {
		return 0, 0, 0, err
	}
	return nextMean, nextVariance, nextCount, nil
}

// scaledProductQuotient calculates a*b/c by separately combining the
// mantissas and exponents. It avoids overflowing at a*b when the quotient is
// representable.
func scaledProductQuotient(a, b, c float64) float64 {
	if a == 0 || b == 0 {
		return 0
	}
	ma, ea := math.Frexp(a)
	mb, eb := math.Frexp(b)
	mc, ec := math.Frexp(c)
	return math.Ldexp((ma*mb)/mc, ea+eb-ec)
}

// mergeVarianceState uses Chan's parallel-variance merge formula with a
// normalized population-variance state.
func mergeVarianceState(mean1, variance1 float64, count1 int64, mean2, variance2 float64, count2 int64) (float64, float64, int64, error) {
	if count2 == 0 {
		return mean1, variance1, count1, nil
	}
	if count1 == 0 {
		return mean2, variance2, count2, nil
	}

	count := count1 + count2
	delta := mean2 - mean1
	if err := float64OfCheck(0, 0, delta); err != nil {
		return 0, 0, 0, err
	}
	mean := mean1 + delta*float64(count2)/float64(count)
	if err := float64OfCheck(0, 0, mean); err != nil {
		return 0, 0, 0, err
	}
	weight1 := float64(count1) / float64(count)
	weight2 := float64(count2) / float64(count)
	correction := scaledProductQuotient(delta, delta*weight1, 1/weight2)
	variance := variance1*weight1 + variance2*weight2 + correction
	if err := float64OfCheck(0, 0, variance); err != nil {
		return 0, 0, 0, err
	}
	return mean, variance, count, nil
}

func (exec *varStdDevExec[T, A]) getResult(variance float64, cnt int64) (T, error) {
	result := variance
	if !exec.isPop {
		result *= float64(cnt) / float64(cnt-1)
	}
	// Variance is non-negative by construction. A negative result can only be a
	// rounding artifact in a merged state; SQL variance must never be negative.
	if result < 0 {
		result = 0
	}

	if !exec.isVar {
		result = math.Sqrt(result)
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
					m2 := float64(0)
					seen := int64(0)
					var origin A
					isDecimal := exec.aggInfo.argTypes[0].Oid == types.T_decimal64 || exec.aggInfo.argTypes[0].Oid == types.T_decimal128
					err := exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						fv := exec.a2f(*ptr, exec.aggInfo.argTypes[0].Scale)
						if isDecimal {
							if seen == 0 {
								origin = *ptr
								fv = 0
							} else {
								var derr error
								fv, derr = decimalDeviationToFloat64(*ptr, origin, exec.aggInfo.argTypes[0].Oid, exec.aggInfo.argTypes[0].Scale)
								if derr != nil {
									return derr
								}
							}
						}
						var fnerr error
						mean, m2, seen, fnerr = updateVarianceState(mean, m2, seen, fv)
						if fnerr != nil {
							return fnerr
						}
						return nil
					})
					if err != nil {
						return nil, err
					}

					z, err := exec.getResult(m2, seen)
					if err != nil {
						return nil, err
					}
					vector.AppendFixed(vecs[i], z, false, exec.mp)
				}
			}
		}
	} else {
		for i := range vecs {
			cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[i].vecs[0])
			variances := vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[2])
			for j, cnt := range cnts {
				if cnt <= 1 {
					// cnt == 1 && exec is samp
					if cnt == 0 || !exec.isPop {
						vector.AppendNull(vecs[i], exec.mp)
						continue
					}
					z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
					vector.AppendFixed(vecs[i], z, false, exec.mp)
				} else {
					result, err := exec.getResult(variances[j], cnt)
					if err != nil {
						return nil, err
					}
					vector.AppendFixed(vecs[i], result, false, exec.mp)
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
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    retType,
		stateTypes: []types.Type{types.T_int64.ToType(), types.T_float64.ToType(), types.T_float64.ToType()},
		emptyNull:  false,
		saveArg:    isDistinct,
	}
	if !legacyState && (param.Oid == types.T_decimal64 || param.Oid == types.T_decimal128) {
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
