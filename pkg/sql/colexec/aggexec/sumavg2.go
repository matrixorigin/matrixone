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
		return types.New(types.T_decimal128, 18, s)
	case types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 18, s)
	default:
		return types.T_float64.ToType()
	}
}

func SumReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_float32, types.T_float64:
		return types.T_float64.ToType()
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return types.T_int64.ToType()
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
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
	if math.IsInf(sum, 0) {
		return moerr.NewOutOfRangeNoCtxf("float64", "(%f + %f)", v1, v2)
	}
	return nil
}

type countSum[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats] struct {
	sum   T
	count int64
}

func (cs *countSum[T, A]) add(val A, ofCheck func(T, T, T) error) error {
	sumT := cs.sum + T(val)
	if err := ofCheck(cs.sum, T(val), sumT); err != nil {
		return err
	}
	cs.sum = sumT
	cs.count += 1
	return nil
}

func (cs *countSum[T, A]) addMany(val []A, ofCheck func(T, T, T) error) error {
	for _, v := range val {
		if err := cs.add(v, ofCheck); err != nil {
			return err
		}
	}
	return nil
}

func (cs *countSum[T, A]) avg() (float64, bool) {
	if cs.count == 0 {
		return 0, false
	}
	return float64(cs.sum) / float64(cs.count), true
}

func (cs *countSum[T, A]) merge(other *countSum[T, A], ofCheck func(T, T, T) error) error {
	sumT := cs.sum + other.sum
	if err := ofCheck(cs.sum, other.sum, sumT); err != nil {
		return err
	}
	cs.sum = sumT
	cs.count += other.count
	return nil
}

type countSumDec struct {
	sum   types.Decimal128
	count int64
}

func (cs *countSumDec) add64(val types.Decimal64, scale int32) error {
	var err error
	dv := types.Decimal128FromDecimal64(val, scale)
	cs.sum, err = cs.sum.Add128(dv)
	cs.count += 1
	return err
}

func (cs *countSumDec) add128(val types.Decimal128) error {
	var err error
	cs.sum, err = cs.sum.Add128(val)
	cs.count += 1
	return err
}

func (cs *countSumDec) addMany64(val []types.Decimal64, scale int32) error {
	var err error
	for _, v := range val {
		cs.sum, err = cs.sum.Add128(types.Decimal128FromDecimal64(v, scale))
		if err != nil {
			return err
		}
		cs.count += 1
	}
	return nil
}

func (cs *countSumDec) addMany128(val []types.Decimal128, scale int32) error {
	var err error
	for _, v := range val {
		cs.sum, err = cs.sum.Add128(v)
		if err != nil {
			return err
		}
		cs.count += 1
	}
	return nil
}

func (cs *countSumDec) merge(other *countSumDec) error {
	var err error
	cs.sum, err = cs.sum.Add128(other.sum)
	cs.count += other.count
	return err
}

// avg = sum / count, and scale from argScale to resultScale.
func (cs *countSumDec) avg(argScale, resultScale int32) (types.Decimal128, bool) {
	if cs.count == 0 {
		return types.Decimal128{}, false
	}
	cnt128 := types.Decimal128FromInt64(cs.count)
	a, s, _ := cs.sum.Div(cnt128, argScale, 0)
	a, _ = a.Scale(resultScale - s)
	return a, true
}

type sumAvgExec[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats] struct {
	isSum   bool
	ofCheck func(T, T, T) error
	aggExec[countSum[T, A]]
}

type sumAvgDecExec struct {
	isSum bool
	aggExec[countSumDec]
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
			pt := exec.GetState(grp - 1)
			val := vector.GetFixedAtNoTypeCheck[A](vectors[0], int(idx))
			err := pt.add(val, exec.ofCheck)
			if err != nil {
				return err
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
		pt1 := exec.GetState(grp - 1)
		pt2 := other.GetState(uint64(offset + i))
		pt1.merge(pt2, exec.ofCheck)
	}
	return nil
}

func (exec *sumAvgExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumAvgExec[T, A]) Flush() ([]*vector.Vector, error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = vector.NewOffHeapVecWithType(resultType)
		vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
	}

	if exec.IsDistinct() {
		for i := range vecs {
			ptrs := exec.state[i].getPtrLenSlice()
			for j := range ptrs {
				args := mpool.PtrLenToSlice[A](&ptrs[j])
				if len(args) == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				}

				var st countSum[T, A]
				st.addMany(args, exec.ofCheck)
				if exec.isSum {
					vector.AppendFixed(vecs[i], st.sum, false, exec.mp)
				} else {
					f64, _ := st.avg()
					vector.AppendFixed(vecs[i], f64, false, exec.mp)
				}
			}
		}
	} else {
		for i := range vecs {
			ss := exec.state[i].getStateSlice()
			for j := range ss {
				if ss[j].count == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				}

				if exec.isSum {
					vector.AppendFixed(vecs[i], ss[j].sum, false, exec.mp)
				} else {
					f64, _ := ss[j].avg()
					vector.AppendFixed(vecs[i], f64, false, exec.mp)
				}
			}
		}
	}
	return vecs, nil
}

func (exec *sumAvgDecExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumAvgDecExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *sumAvgDecExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
			pt := exec.GetState(grp - 1)
			if exec.aggInfo.argTypes[0].Oid == types.T_decimal64 {
				val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vectors[0], int(idx))
				err := pt.add64(val, exec.aggInfo.argTypes[0].Scale)
				if err != nil {
					return err
				}
			} else {
				val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vectors[0], int(idx))
				err := pt.add128(val)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (exec *sumAvgDecExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumAvgDecExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*sumAvgDecExec)
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		pt1 := exec.GetState(grp - 1)
		pt2 := other.GetState(uint64(offset + i))
		pt1.merge(pt2)
	}
	return nil
}

func (exec *sumAvgDecExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumAvgDecExec) Flush() ([]*vector.Vector, error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = vector.NewOffHeapVecWithType(resultType)
		vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
	}

	if exec.IsDistinct() {
		for i := range vecs {
			ptrs := exec.state[i].getPtrLenSlice()
			for j := range ptrs {
				if ptrs[j].Len() == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				}

				var st countSumDec
				if exec.aggInfo.argTypes[0].Oid == types.T_decimal64 {
					args := mpool.PtrLenToSlice[types.Decimal64](&ptrs[j])
					st.addMany64(args, exec.aggInfo.argTypes[0].Scale)
				} else {
					args := mpool.PtrLenToSlice[types.Decimal128](&ptrs[j])
					st.addMany128(args, exec.aggInfo.argTypes[0].Scale)
				}

				if exec.isSum {
					vector.AppendFixed(vecs[i], st.sum, false, exec.mp)
				} else {
					avgVal, _ := st.avg(exec.aggInfo.argTypes[0].Scale, resultType.Scale)
					vector.AppendFixed(vecs[i], avgVal, false, exec.mp)
				}
			}
		}
	} else {
		for i := range vecs {
			ss := exec.state[i].getStateSlice()
			for j := range ss {
				if ss[j].count == 0 {
					vector.AppendNull(vecs[i], exec.mp)
					continue
				}

				if exec.isSum {
					vector.AppendFixed(vecs[i], ss[j].sum, false, exec.mp)
				} else {
					avgVal, _ := ss[j].avg(exec.aggInfo.argTypes[0].Scale, resultType.Scale)
					vector.AppendFixed(vecs[i], avgVal, false, exec.mp)
				}
			}
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
	case types.T_float32:
		return newSumAvgExec[float64, float32](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_float64:
		return newSumAvgExec[float64, float64](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_decimal64:
		return newSumAvgDecExec(mp, isSum, aggID, isDistinct, param)
	case types.T_decimal128:
		return newSumAvgDecExec(mp, isSum, aggID, isDistinct, param)
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
	if isSum {
		rt = SumReturnType([]types.Type{param})
	} else {
		rt = AvgReturnType([]types.Type{param})
	}
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  false,
		usePtrLen:  isDistinct, // sum/avg (distinct X) needs to store varlen value
		entrySize:  countSumSize[T, A](),
	}
	return &exec
}

func newSumAvgDecExec(mp *mpool.MPool, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumAvgDecExec
	exec.mp = mp
	exec.isSum = isSum
	var rt types.Type
	if isSum {
		rt = SumReturnType([]types.Type{param})
	} else {
		rt = AvgReturnType([]types.Type{param})
	}
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  false,
		usePtrLen:  isDistinct, // sum/avg (distinct X) needs to store varlen value
		entrySize:  countSumDecSize(),
	}
	return &exec
}
