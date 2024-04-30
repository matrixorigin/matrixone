// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sort"
)

var MedianSupportedType = []types.T{
	types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
}

func MedianReturnType(args []types.Type) types.Type {
	if args[0].IsDecimal() {
		return types.New(types.T_decimal128, 38, args[0].Scale+1)
	}
	return types.T_float64.ToType()
}

type numeric interface {
	types.Ints | types.UInts | types.Floats
}

type medianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R float64 | types.Decimal128] struct {
	singleAggInfo
	singleAggExecExtraInformation
	distinctHash
	arg sFixedArg[T]
	ret aggFuncResult[R]

	// groups stores the values of the column for each group.
	// todo: it has a problem that same as the `clusterCentersExec.groupData` in `cluster_centers.go`
	groups []*vector.Vector
}

func newMedianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R float64 | types.Decimal128](mg AggMemoryManager, info singleAggInfo) medianColumnExecSelf[T, R] {
	s := medianColumnExecSelf[T, R]{
		singleAggInfo: info,
		ret:           initFixedAggFuncResult[R](mg, info.retType, info.emptyNull),
	}
	if info.IsDistinct() {
		s.distinctHash = newDistinctHash(mg.Mp(), false)
	}
	return s
}

func (exec *medianColumnExecSelf[T, R]) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	oldLength := len(exec.groups)
	if cap(exec.groups) >= oldLength+more {
		exec.groups = exec.groups[:oldLength+more]
	} else {
		exec.groups = append(exec.groups, make([]*vector.Vector, more)...)
	}

	for i, j := oldLength, len(exec.groups); i < j; i++ {
		exec.groups[i] = exec.ret.mg.GetVector(exec.singleAggInfo.argType)
	}
	return exec.ret.grows(more)
}

func (exec *medianColumnExecSelf[T, R]) PreAllocateGroups(more int) error {
	if len(exec.groups) == 0 {
		exec.groups = make([]*vector.Vector, 0, more)
	} else {
		oldLength := len(exec.groups)
		exec.groups = append(exec.groups, make([]*vector.Vector, more)...)
		exec.groups = exec.groups[:oldLength]
	}

	return exec.ret.preAllocate(more)
}

func (exec *medianColumnExecSelf[T, R]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	value := vector.MustFixedCol[T](vectors[0])[row]

	return vectorAppendWildly(exec.groups[groupIndex], exec.ret.mp, value)
}

func (exec *medianColumnExecSelf[T, R]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors)
	}

	if vectors[0].IsConst() {
		exec.ret.setGroupNotEmpty(groupIndex)
		value := vector.MustFixedCol[T](vectors[0])[0]
		return vector.AppendMultiFixed[T](exec.groups[0], value, false, vectors[0].Length(), exec.ret.mp)
	}

	exec.arg.prepare(vectors[0])
	mustNotEmpty := false
	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		v, null := exec.arg.w.GetValue(i)
		if null {
			continue
		}
		mustNotEmpty = true
		if err := vectorAppendWildly(exec.groups[groupIndex], exec.ret.mp, v); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConst() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
			return err
		}

		exec.ret.setGroupNotEmpty(groupIndex)
		value := vector.MustFixedCol[T](vectors[0])[0]
		return vector.AppendMultiFixed[T](exec.groups[groupIndex], value, false, vectors[0].Length(), exec.ret.mp)
	}

	needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
	if err != nil {
		return err
	}
	exec.arg.prepare(vectors[0])
	mustNotEmpty := false
	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		if !needs[i] {
			continue
		}

		v, null := exec.arg.w.GetValue(i)
		if null {
			continue
		}
		mustNotEmpty = true
		if err = vectorAppendWildly(exec.groups[groupIndex], exec.ret.mp, v); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

	if vectors[0].IsConst() {
		value := vector.MustFixedCol[T](vectors[0])[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIndex := groups[i] - 1
				exec.ret.setGroupNotEmpty(int(groupIndex))
				if err := vectorAppendWildly(
					exec.groups[groupIndex],
					exec.ret.mp, value); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, null := exec.arg.w.GetValue(i)
			if !null {
				groupIndex := groups[idx] - 1
				exec.ret.setGroupNotEmpty(int(groupIndex))

				if err := vectorAppendWildly(exec.groups[groupIndex], exec.ret.mp, v); err != nil {
					return err
				}
			}
		}
		idx++
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vectors[0].IsConst() {
		value := vector.MustFixedCol[T](vectors[0])[0]
		for i := 0; i < len(groups); i++ {
			if needs[i] && groups[i] != GroupNotMatched {
				groupIndex := groups[i] - 1
				exec.ret.setGroupNotEmpty(int(groupIndex))
				if err = vectorAppendWildly(
					exec.groups[groupIndex],
					exec.ret.mp, value); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if needs[idx] && groups[idx] != GroupNotMatched {
			v, null := exec.arg.w.GetValue(i)
			if !null {
				groupIndex := groups[idx] - 1
				exec.ret.setGroupNotEmpty(int(groupIndex))
				if err = vectorAppendWildly(exec.groups[groupIndex], exec.ret.mp, v); err != nil {
					return err
				}
			}
		}
		idx++
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Merge(other *medianColumnExecSelf[T, R], groupIdx1, groupIdx2 int) error {
	if exec.IsDistinct() {
		return exec.distinctHash.merge(&other.distinctHash)
	}
	if other.groups[groupIdx2].Length() == 0 {
		return nil
	}
	vs := vector.MustFixedCol[T](other.groups[groupIdx2])
	return vector.AppendFixedList[T](exec.groups[groupIdx1], vs, nil, exec.ret.mp)
}

func (exec *medianColumnExecSelf[T, R]) BatchMerge(next *medianColumnExecSelf[T, R], offset int, groups []uint64) error {
	for i, group := range groups {
		if group != GroupNotMatched {
			if err := exec.Merge(next, int(group)-1, i+offset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Free() {
	if exec.ret.mg == nil {
		return
	}
	for _, v := range exec.groups {
		if v == nil {
			continue
		}
		if v.NeedDup() {
			v.Free(exec.ret.mp)
		} else {
			exec.ret.mg.PutVector(v)
		}
	}
	exec.ret.free()
	exec.distinctHash.free()
}

type medianColumnNumericExec[T numeric] struct {
	medianColumnExecSelf[T, float64]
}

func newMedianColumnNumericExec[T numeric](mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	return &medianColumnNumericExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, float64](mg, info),
	}
}

type medianColumnDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	medianColumnExecSelf[T, types.Decimal128]
}

func newMedianColumnDecimalExec[T types.Decimal64 | types.Decimal128](mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	return &medianColumnDecimalExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, types.Decimal128](mg, info),
	}
}

func newMedianExecutor(mg AggMemoryManager, info singleAggInfo) (AggFuncExec, error) {
	if info.distinct {
		return nil, moerr.NewNotSupportedNoCtx("median in distinct mode")
	}

	switch info.argType.Oid {
	case types.T_bit:
		return newMedianColumnNumericExec[uint64](mg, info), nil
	case types.T_int8:
		return newMedianColumnNumericExec[int8](mg, info), nil
	case types.T_int16:
		return newMedianColumnNumericExec[int16](mg, info), nil
	case types.T_int32:
		return newMedianColumnNumericExec[int32](mg, info), nil
	case types.T_int64:
		return newMedianColumnNumericExec[int64](mg, info), nil
	case types.T_uint8:
		return newMedianColumnNumericExec[uint8](mg, info), nil
	case types.T_uint16:
		return newMedianColumnNumericExec[uint16](mg, info), nil
	case types.T_uint32:
		return newMedianColumnNumericExec[uint32](mg, info), nil
	case types.T_uint64:
		return newMedianColumnNumericExec[uint64](mg, info), nil
	case types.T_float32:
		return newMedianColumnNumericExec[float32](mg, info), nil
	case types.T_float64:
		return newMedianColumnNumericExec[float64](mg, info), nil
	case types.T_decimal64:
		return newMedianColumnDecimalExec[types.Decimal64](mg, info), nil
	case types.T_decimal128:
		return newMedianColumnDecimalExec[types.Decimal128](mg, info), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type for median()")
}

func (exec *medianColumnNumericExec[T]) Merge(next AggFuncExec, groupIdx1 int, groupIdx2 int) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnNumericExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnNumericExec[T]) Flush() (*vector.Vector, error) {
	vs := exec.ret.values
	for i := range exec.groups {
		rows := exec.groups[i].Length()
		if rows == 0 {
			vs[i] = 0
			continue
		}

		exec.ret.empty[i] = false
		sort.Sort(generateSortableSlice(vector.MustFixedCol[T](exec.groups[i])))
		srcs := vector.MustFixedCol[T](exec.groups[i])
		if rows&1 == 1 {
			vs[i] = float64(srcs[rows>>1])
		} else {
			vs[i] = float64(srcs[rows>>1-1]+srcs[rows>>1]) / 2
		}
	}
	return exec.ret.flush(), nil
}

func (exec *medianColumnDecimalExec[T]) Merge(next AggFuncExec, groupIdx1 int, groupIdx2 int) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnDecimalExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnDecimalExec[T]) Flush() (*vector.Vector, error) {
	var err error
	vs := exec.ret.values
	argIsDecimal128 := exec.singleAggInfo.argType.Oid == types.T_decimal128

	for i := range exec.groups {
		rows := exec.groups[i].Length()
		if rows == 0 {
			continue
		}

		exec.ret.empty[i] = false
		sort.Sort(generateSortableSlice2(vector.MustFixedCol[T](exec.groups[i])))
		if argIsDecimal128 {
			srcs := vector.MustFixedCol[types.Decimal128](exec.groups[i])
			if rows&1 == 1 {
				if vs[i], err = srcs[rows>>1].Scale(1); err != nil {
					return nil, err
				}
			} else {
				v1, v2 := srcs[rows>>1-1], srcs[rows>>1]
				if vs[i], err = v1.Add128(v2); err != nil {
					return nil, err
				}
				if vs[i].Sign() {
					// scale(1) here because we set the result scale to be arg.Scale+1
					if vs[i], err = vs[i].Minus().Scale(1); err != nil {
						return nil, err
					}
					vs[i] = vs[i].Right(1).Minus()
				} else {
					if vs[i], err = vs[i].Scale(1); err != nil {
						return nil, err
					}
					vs[i] = vs[i].Right(1)
				}
			}

		} else {
			srcs := vector.MustFixedCol[types.Decimal64](exec.groups[i])
			if rows&1 == 1 {
				if vs[i], err = FromD64ToD128(srcs[rows>>1]).Scale(1); err != nil {
					return nil, err
				}
			} else {
				v1, v2 := FromD64ToD128(srcs[rows>>1-1]), FromD64ToD128(srcs[rows>>1])
				if vs[i], err = v1.Add128(v2); err != nil {
					return nil, err
				}
				if vs[i].Sign() {
					if vs[i], err = vs[i].Minus().Scale(1); err != nil {
						return nil, err
					}
					vs[i] = vs[i].Right(1).Minus()
				} else {
					if vs[i], err = vs[i].Scale(1); err != nil {
						return nil, err
					}
					vs[i] = vs[i].Right(1)
				}
			}
		}
	}
	return exec.ret.flush(), nil
}

type numericSlice[T numeric] []T

func (s numericSlice[T]) Len() int {
	return len(s)
}
func (s numericSlice[T]) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s numericSlice[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type decimal64Slice []types.Decimal64
type decimal128Slice []types.Decimal128

func (s decimal64Slice) Len() int { return len(s) }
func (s decimal64Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal64Slice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s decimal128Slice) Len() int { return len(s) }
func (s decimal128Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal128Slice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func generateSortableSlice[T numeric](vs []T) sort.Interface {
	return numericSlice[T](vs)
}

func generateSortableSlice2[T types.Decimal64 | types.Decimal128](vs []T) sort.Interface {
	temp := any(vs)
	if d64, ok := temp.([]types.Decimal64); ok {
		return decimal64Slice(d64)
	}
	if d128, ok := temp.([]types.Decimal128); ok {
		return decimal128Slice(d128)
	}
	panic("unsupported type")
}
