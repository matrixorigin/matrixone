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
	isVar bool
	isPop bool
	a2f   func(A, int32) float64
	f2t   func(float64, int32) (T, error)
}

const varianceNearZeroToleranceInULP = 8.0

func simpleA2F[A types.Ints | types.UInts | types.Floats](a A, scale int32) float64 {
	return float64(a)
}

func simpleF2T(f float64, scale int32) (float64, error) {
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

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(grp - 1)
			cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[0])
			sums := vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[1])
			sumsqs := vector.MustFixedColNoTypeCheck[float64](exec.state[x].vecs[2])
			val := vector.GetFixedAtNoTypeCheck[A](vectors[0], int(idx))
			fv := exec.a2f(val, exec.aggInfo.argTypes[0].Scale)
			s := sums[y] + fv
			if err := float64OfCheck(0, 0, s); err != nil {
				return err
			}

			s2 := sumsqs[y] + fv*fv
			if err := float64OfCheck(0, 0, s2); err != nil {
				return err
			}

			sums[y] = s
			sumsqs[y] = s2
			cnts[y] += 1
		}
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
		result := sums1[y1] + sums2[y2]
		if err := float64OfCheck(0, 0, result); err != nil {
			return err
		}
		resultsq := sumsqs1[y1] + sumsqs2[y2]
		if err := float64OfCheck(0, 0, resultsq); err != nil {
			return err
		}

		sums1[y1] = result
		sumsqs1[y1] = resultsq
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *varStdDevExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

// clampVarianceNearZero rounds cancellation noise to zero by using an ULP-based tolerance.
func clampVarianceNearZero(variance, part1, part2 float64) float64 {
	if math.IsNaN(variance) || math.IsInf(variance, 0) {
		return variance
	}
	scale := math.Max(math.Abs(part1), math.Abs(part2))
	if scale == 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return variance
	}

	ulp := math.Nextafter(scale, math.Inf(1)) - scale
	if ulp <= 0 || math.IsNaN(ulp) || math.IsInf(ulp, 0) {
		return variance
	}
	if math.Abs(variance) <= varianceNearZeroToleranceInULP*ulp {
		return 0
	}
	return variance
}

func (exec *varStdDevExec[T, A]) getResult(s float64, s2 float64, cnt int64) (T, error) {
	var result float64
	avg := s / float64(cnt)
	if exec.isPop {
		part1 := s2 / float64(cnt)
		part2 := avg * avg
		result = clampVarianceNearZero(part1-part2, part1, part2)
	} else {
		denominator := float64(cnt - 1)
		part1 := s2 / denominator
		part2 := avg * avg * float64(cnt) / denominator
		result = clampVarianceNearZero(part1-part2, part1, part2)
	}

	if !exec.isVar {
		result = math.Sqrt(result)
	}

	z, err := exec.f2t(result, exec.aggInfo.retType.Scale)
	return z, err
}

func (exec *varStdDevExec[T, A]) Flush() ([]*vector.Vector, error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = vector.NewOffHeapVecWithType(resultType)
		vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
	}

	if exec.IsDistinct() {
		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				if exec.state[i].argCnt[j] == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				} else if exec.state[i].argCnt[j] == 1 {
					z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
					vector.AppendFixed(vecs[i], z, false, exec.mp)
				} else {
					cnt := int64(exec.state[i].argCnt[j])
					s := float64(0)
					s2 := float64(0)
					err := exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						fv := exec.a2f(*ptr, exec.aggInfo.argTypes[0].Scale)
						s += fv
						if fnerr := float64OfCheck(0, 0, s); fnerr != nil {
							return fnerr
						}
						s2 += fv * fv
						if fnerr := float64OfCheck(0, 0, s2); fnerr != nil {
							return fnerr
						}
						return nil
					})
					if err != nil {
						return nil, err
					}

					z, err := exec.getResult(s, s2, cnt)
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
			sums := vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[1])
			sumsqs := vector.MustFixedColNoTypeCheck[float64](exec.state[i].vecs[2])
			for j, cnt := range cnts {
				if cnt == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				} else if cnt == 1 {
					z, _ := exec.f2t(0, exec.aggInfo.retType.Scale)
					vector.AppendFixed(vecs[i], z, false, exec.mp)
				} else {
					result, err := exec.getResult(sums[j], sumsqs[j], cnt)
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

func makeVarStdDevExec(mp *mpool.MPool,
	isVar bool, isPop bool,
	aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	switch param.Oid {
	case types.T_int8:
		return newVarStdDevExec[float64, int8](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_int16:
		return newVarStdDevExec[float64, int16](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_int32:
		return newVarStdDevExec[float64, int32](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_int64:
		return newVarStdDevExec[float64, int64](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_uint8:
		return newVarStdDevExec[float64, uint8](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_uint16:
		return newVarStdDevExec[float64, uint16](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_uint32:
		return newVarStdDevExec[float64, uint32](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_uint64:
		return newVarStdDevExec[float64, uint64](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_bit:
		return newVarStdDevExec[float64, uint64](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_float32:
		return newVarStdDevExec[float64, float32](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_float64:
		return newVarStdDevExec[float64, float64](mp, isVar, isPop, aggID, isDistinct, param, simpleA2F, simpleF2T)
	case types.T_decimal64:
		return newVarStdDevExec[types.Decimal128, types.Decimal64](mp, isVar, isPop, aggID, isDistinct, param, dec64ToF, fToDec128)
	case types.T_decimal128:
		return newVarStdDevExec[types.Decimal128, types.Decimal128](mp, isVar, isPop, aggID, isDistinct, param, dec128ToF, fToDec128)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for var/stddev", param.Oid))
	}
}

func newVarStdDevExec[T float64 | types.Decimal128, A types.Ints | types.UInts | types.Floats | types.Decimal64 | types.Decimal128](mp *mpool.MPool, isVar bool, isPop bool, aggID int64, isDistinct bool, param types.Type, a2f func(A, int32) float64, f2t func(float64, int32) (T, error)) AggFuncExec {
	var exec varStdDevExec[T, A]
	exec.mp = mp
	exec.isVar = isVar
	exec.isPop = isPop
	exec.a2f = a2f
	exec.f2t = f2t

	avgTyp := AvgReturnType([]types.Type{param})
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    avgTyp,
		stateTypes: []types.Type{types.T_int64.ToType(), types.T_float64.ToType(), types.T_float64.ToType()},
		emptyNull:  false,
		saveArg:    isDistinct,
	}
	return &exec
}

func makeVarPopExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeVarStdDevExec(mp, true, true, aggID, isDistinct, param)
}

func makeVarSampleExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeVarStdDevExec(mp, true, false, aggID, isDistinct, param)
}

func makeStdDevPopExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeVarStdDevExec(mp, false, true, aggID, isDistinct, param)
}

func makeStdDevSampleExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeVarStdDevExec(mp, false, false, aggID, isDistinct, param)
}
