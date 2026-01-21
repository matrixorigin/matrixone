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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// XXX:
//
// This returned type thing definitely belongs to plan, function.
// However, exec cannot import plan, function due to circular dependency.
// Also this is the reason for all those RegisterAgg crap.   This is the
// weirdest thing I've ever seen.  We need to untangle all this mess.
//
// See list_agg.go, we need to remove dependency of plan on exec.
//

func AvgReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 38, s)
	case types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 38, s)
	default:
		return types.T_float64.ToType()
	}
}

func SumReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_float32, types.T_float64:
		return types.T_float64.ToType()
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_year:
		return types.T_int64.ToType()
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
		return types.T_uint64.ToType()
	case types.T_decimal64:
		return types.New(types.T_decimal128, 38, typs[0].Scale)
	case types.T_decimal128:
		return types.New(types.T_decimal128, 38, typs[0].Scale)
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for sum", typs[0]))
}

func int64OfCheck(v1, v2, sum int64) error {
	if (v1 > 0 && v2 > 0 && sum <= 0) || (v1 < 0 && v2 < 0 && sum >= 0) {
		return moerr.NewOutOfRangeNoCtxf("int64", "(%d + %d)", v1, v2)
	}
	return nil
}

func uint64OfCheck(v1, v2, sum uint64) error {
	if sum < v1 || sum < v2 {
		return moerr.NewOutOfRangeNoCtxf("uint64", "(%d + %d)", v1, v2)
	}
	return nil
}

func float64OfCheck(v1, v2, sum float64) error {
	// MySQL behavior: SUM() aggregation allows overflow to +Infinity without error
	// This matches MySQL 8.0 where SUM() silently returns +Infinity on overflow
	return nil
}

type sumAvgExec[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats] struct {
	aggExec
	isSum   bool
	ofCheck func(T, T, T) error
}

func (exec *sumAvgExec[T, A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumAvgExec[T, A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *sumAvgExec[T, A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
			sumVec := exec.state[x].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[T](sumVec)
			val := vector.GetFixedAtNoTypeCheck[A](vectors[0], int(idx))
			result := sums[y] + T(val)
			if err := exec.ofCheck(sums[y], T(val), result); err != nil {
				return err
			}

			if exec.isSum {
				if sumVec.IsNull(uint64(y)) {
					sumVec.UnsetNull(uint64(y))
				}
				sums[y] = result
			} else {
				sums[y] = result
				cntVec := exec.state[x].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				cnts[y] += 1
			}
		}
	}
	return nil
}

func (exec *sumAvgExec[T, A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumAvgExec[T, A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*sumAvgExec[T, A])
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		sumVec1 := exec.state[x1].vecs[0]
		sumVec2 := other.state[x2].vecs[0]
		sums1 := vector.MustFixedColNoTypeCheck[T](sumVec1)
		sums2 := vector.MustFixedColNoTypeCheck[T](sumVec2)

		if exec.isSum {
			if sumVec2.IsNull(uint64(y2)) {
				continue
			} else if sumVec1.IsNull(uint64(y1)) {
				sumVec1.UnsetNull(uint64(y1))
				sums1[y1] = sums2[y2]
			} else {
				result := sums1[y1] + sums2[y2]
				if err := exec.ofCheck(sums1[y1], sums2[y2], result); err != nil {
					return err
				}
				sums1[y1] = result
			}
		} else {
			cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
			cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])
			result := sums1[y1] + sums2[y2]
			if err := exec.ofCheck(sums1[y1], sums2[y2], result); err != nil {
				return err
			}
			sums1[y1] = result
			cnts1[y1] += cnts2[y2]
		}
	}
	return nil
}

func (exec *sumAvgExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumAvgExec[T, A]) Flush() ([]*vector.Vector, error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))

	if exec.IsDistinct() {
		for i := range vecs {
			vecs[i] = vector.NewOffHeapVecWithType(resultType)
			vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
		}
		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				if exec.state[i].argCnt[j] == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				} else {
					sum := T(0)
					xcnt := 0
					err := exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						tmp := sum + T(*ptr)
						if err := exec.ofCheck(sum, T(*ptr), tmp); err != nil {
							return err
						}
						sum = tmp
						xcnt++
						return nil
					})

					if err != nil {
						return nil, err
					}
					if int(exec.state[i].argCnt[j]) != xcnt {
						panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", xcnt, j, exec.state[i].argCnt[j]))
					}

					if exec.isSum {
						vector.AppendFixed(vecs[i], sum, false, exec.mp)
					} else {
						vector.AppendFixed(vecs[i], float64(sum)/float64(exec.state[i].argCnt[j]), false, exec.mp)
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[T](sumVec)

			// transfer sumVec
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil

			if !exec.isSum {
				// hack: avgs will reuse sums slice, float64 and int64 are the same size.
				avgs := util.UnsafeSliceCast[float64](sums)
				cntVec := exec.state[i].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg := float64(sums[j]) / float64(cnt)
						avgs[j] = avg
					}
				}
				// free cntVec
				cntVec.Free(exec.mp)
				exec.state[i].vecs[1] = nil
			}

			// Fix result type.   note that for avg, the result type is
			// float64, for any int/uint type, the sum type is int64/uint64.
			// they are different types but SAME SIZE.   Let's just fix the
			// result type and be happy.
			*sumVec.GetType() = resultType

			// done transfer,
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

type sumAvgDecExec[A types.Decimal64 | types.Decimal128] struct {
	aggExec
	isSum bool
}

func (exec *sumAvgDecExec[A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumAvgDecExec[A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *sumAvgDecExec[A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	var err error
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
			sumVec := exec.state[x].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)
			var val types.Decimal128
			if exec.aggInfo.argTypes[0].Oid == types.T_decimal64 {
				val64 := vector.GetFixedAtNoTypeCheck[types.Decimal64](vectors[0], int(idx))
				val = types.Decimal128FromDecimal64(val64, exec.aggInfo.argTypes[0].Scale)
			} else {
				val = vector.GetFixedAtNoTypeCheck[types.Decimal128](vectors[0], int(idx))
			}

			if exec.isSum {
				if sumVec.IsNull(uint64(y)) {
					sumVec.UnsetNull(uint64(y))
					sums[y] = val
				} else {
					if sums[y], err = sums[y].Add128(val); err != nil {
						return err
					}
				}
			} else {
				if sums[y], err = sums[y].Add128(val); err != nil {
					return err
				}
				cntVec := exec.state[x].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				cnts[y] += 1
			}
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumAvgDecExec[A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	var err error
	other := next.(*sumAvgDecExec[A])
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		sumVec1 := exec.state[x1].vecs[0]
		sumVec2 := other.state[x2].vecs[0]
		sums1 := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec1)
		sums2 := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec2)

		if exec.isSum {
			if sumVec2.IsNull(uint64(y2)) {
				continue
			} else if sumVec1.IsNull(uint64(y1)) {
				sumVec1.UnsetNull(uint64(y1))
				sums1[y1] = sums2[y2]
			} else {
				if sums1[y1], err = sums1[y1].Add128(sums2[y2]); err != nil {
					return err
				}
			}
		} else {
			if sums1[y1], err = sums1[y1].Add128(sums2[y2]); err != nil {
				return err
			}
			cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
			cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])
			cnts1[y1] += cnts2[y2]
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func decAvg(sum types.Decimal128, count int64, argScale, resultScale int32) types.Decimal128 {
	cnt128 := types.Decimal128FromInt64(count)
	a, s, _ := sum.Div(cnt128, argScale, 0)
	a, _ = a.Scale(resultScale - s)
	return a
}

func (exec *sumAvgDecExec[A]) Flush() ([]*vector.Vector, error) {
	var err error
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))

	if exec.IsDistinct() {
		for i := range vecs {
			vecs[i] = vector.NewOffHeapVecWithType(resultType)
			vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
		}

		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				if exec.state[i].argCnt[j] == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				} else {
					var sum types.Decimal128
					xcnt := 0

					err = exec.state[i].iter(uint16(j), func(k []byte) error {
						var val types.Decimal128
						var fnerr error
						if exec.aggInfo.argTypes[0].Oid == types.T_decimal64 {
							ptr := util.UnsafeFromBytes[types.Decimal64](k[kAggArgPrefixSz:])
							val = types.Decimal128FromDecimal64(*ptr, exec.aggInfo.argTypes[0].Scale)
						} else {
							ptr := util.UnsafeFromBytes[types.Decimal128](k[kAggArgPrefixSz:])
							val = *ptr
						}
						if sum, fnerr = sum.Add128(val); fnerr != nil {
							return fnerr
						}
						xcnt++
						return nil
					})

					if err != nil {
						return nil, err
					}
					if int(exec.state[i].argCnt[j]) != xcnt {
						panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", xcnt, j, exec.state[i].argCnt[j]))
					}

					if exec.isSum {
						vector.AppendFixed(vecs[i], sum, false, exec.mp)
					} else {
						avg := decAvg(sum, int64(exec.state[i].argCnt[j]), exec.aggInfo.argTypes[0].Scale, resultType.Scale)
						vector.AppendFixed(vecs[i], avg, false, exec.mp)
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)

			if !exec.isSum {
				cntVec := exec.state[i].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg := decAvg(sums[j], cnt, exec.aggInfo.argTypes[0].Scale, resultType.Scale)
						vector.SetFixedAtNoTypeCheck(sumVec, j, avg)
					}
				}
				cntVec.Free(exec.mp)
				exec.state[i].vecs[1] = nil
			}

			// Fix resulit scale
			sumVec.GetType().Scale = resultType.Scale

			// transfer sumVec
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

func makeSumAvgExec(
	mp *mpool.MPool, isSum bool,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {

	switch param.Oid {
	case types.T_int8:
		return newSumAvgExec[int64, int8](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int16:
		return newSumAvgExec[int64, int16](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_year:
		return newSumAvgExec[int64, int16](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int32:
		return newSumAvgExec[int64, int32](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int64:
		return newSumAvgExec[int64, int64](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint8:
		return newSumAvgExec[uint64, uint8](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint16:
		return newSumAvgExec[uint64, uint16](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint32:
		return newSumAvgExec[uint64, uint32](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint64:
		return newSumAvgExec[uint64, uint64](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_bit:
		return newSumAvgExec[uint64, uint64](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_float32:
		return newSumAvgExec[float64, float32](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_float64:
		return newSumAvgExec[float64, float64](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_decimal64:
		return newSumAvgDecExec[types.Decimal64](mp, isSum, aggID, isDistinct, param)
	case types.T_decimal128:
		return newSumAvgDecExec[types.Decimal128](mp, isSum, aggID, isDistinct, param)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for sum/avg", param.Oid))
	}
}

func newSumAvgExec[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats](mp *mpool.MPool, ofCheck func(T, T, T) error, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumAvgExec[T, A]
	exec.mp = mp
	exec.isSum = isSum
	exec.ofCheck = ofCheck
	var rt types.Type
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	if isSum {
		rt = sumTyp
	} else {
		rt = avgTyp
	}
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  isSum,
		saveArg:    isDistinct,
	}

	if isSum {
		exec.aggInfo.stateTypes = []types.Type{sumTyp}
	} else {
		exec.aggInfo.stateTypes = []types.Type{sumTyp, types.T_int64.ToType()}
	}
	return &exec
}

func newSumAvgDecExec[A types.Decimal64 | types.Decimal128](mp *mpool.MPool, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumAvgDecExec[A]
	exec.mp = mp
	exec.isSum = isSum
	var rt types.Type
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	if isSum {
		rt = sumTyp
	} else {
		rt = avgTyp
	}

	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  isSum,
		saveArg:    isDistinct,
	}

	if isSum {
		exec.aggInfo.stateTypes = []types.Type{sumTyp}
	} else {
		exec.aggInfo.stateTypes = []types.Type{sumTyp, types.T_int64.ToType()}
	}

	return &exec
}
