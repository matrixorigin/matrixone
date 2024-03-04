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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// singleAggInfo contains the basic information of single column agg.
type singleAggInfo struct {
	aggID    int64
	distinct bool
	argType  types.Type
	retType  types.Type

	// emptyNull indicates that whether we should return null for a group without any input value.
	emptyNull bool
}

func (info singleAggInfo) String() string {
	return fmt.Sprintf("{aggID: %d, argType: %s, retType: %s}", info.aggID, info.argType.String(), info.retType.String())
}

func (info singleAggInfo) AggID() int64 {
	return info.aggID
}

func (info singleAggInfo) IsDistinct() bool {
	return info.distinct
}

func (info singleAggInfo) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{info.argType}, info.retType
}

func (info singleAggInfo) getEncoded() *EncodedBasicInfo {
	return &EncodedBasicInfo{
		Id:         info.aggID,
		IsDistinct: info.distinct,
		Args:       []types.Type{info.argType},
		Ret:        info.retType,
	}
}

type singleAggOptimizedInfo struct {
	// modify it to `acceptNull` later.
	receiveNull bool
}

type singleAggExecOptimized struct {
	partialGroup  int
	partialResult any
}

func (optimized *singleAggExecOptimized) SetPreparedResult(partialResult any, groupIndex int) {
	optimized.partialGroup = groupIndex
	optimized.partialResult = partialResult
}

// the executors of single column agg.
// singleAggFuncExec1 receives a fixed size type except string and returns a fixed size type except string.
// singleAggFuncExec2 receives a fixed size type except string and returns a byte type.
// singleAggFuncExec3 receives a byte type and returns a fixed size type except string.
// singleAggFuncExec4 receives a byte type and returns a byte type.
type singleAggFuncExec1[from, to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sFixedArg[from]
	ret    aggFuncResult[to]
	groups []SingleAggFromFixedRetFixed[from, to]

	hashmaps []*hashmap.StrHashMap

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromFixedRetFixed[from, to]
}
type singleAggFuncExec2[from types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sFixedArg[from]
	ret    aggFuncBytesResult
	groups []SingleAggFromFixedRetVar[from]

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromFixedRetVar[from]
}
type singleAggFuncExec3[to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sBytesArg
	ret    aggFuncResult[to]
	groups []SingleAggFromVarRetFixed[to]

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromVarRetFixed[to]
}
type singleAggFuncExec4 struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sBytesArg
	ret    aggFuncBytesResult
	groups []SingleAggFromVarRetVar

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromVarRetVar
}

func (exec *singleAggFuncExec1[from, to]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	nm func() SingleAggFromFixedRetFixed[from, to]) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.ret = initFixedAggFuncResult[to](mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromFixedRetFixed[from, to], 0, 1)
	exec.gGroup = nm
}

func (exec *singleAggFuncExec1[from, to]) GroupGrow(more int) error {
	moreGroup := make([]SingleAggFromFixedRetFixed[from, to], more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()
		moreGroup[i].Init()
	}
	exec.groups = append(exec.groups, moreGroup...)

	if exec.IsDistinct() {
		for i := 0; i < more; i++ {
			strMap, err := hashmap.NewStrMap(true, 0, 0, exec.ret.mp)
			if err != nil {
				return err
			}
			exec.hashmaps = append(exec.hashmaps, strMap)
		}
	}

	return exec.ret.grows(more)
}

func (exec *singleAggFuncExec1[from, to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].FillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].Fill(vector.MustFixedCol[from](vec)[row], getter, setter)
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()

	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			var value from
			if exec.receiveNull {
				exec.groups[groupIndex].Fills(value, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].Fills(vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].FillNull(getter, setter)
				} else {
					exec.groups[groupIndex].Fill(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].Fill(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].Fill(v, getter, setter)
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].FillNull(getter, setter)
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].Fill(value, getter, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].FillNull(getter, setter)
					} else {
						exec.groups[groupIdx].Fill(v, getter, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].Fill(v, getter, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].Fill(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) DistinctFill(groupIndex int, row int, vectors []*vector.Vector) error {

	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	isNew, err := exec.hashmaps[groupIndex].Insert(vectors, row)
	if err != nil {
		return err
	}
	if !isNew {
		return nil
	}

	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].FillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].Fill(vector.MustFixedCol[from](vec)[row], getter, setter)
	return nil
}

func (exec *singleAggFuncExec1[from, to]) DistinctBulkFill(groupIndex int, vectors []*vector.Vector) error {
	for i := range vectors {
		if err := exec.DistinctFill(groupIndex, i, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) DistinctBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if !exec.IsDistinct() {
		return nil
	}

	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	shouldBeFilled := make([]bool, len(groups))

	for vecIdx, idx := uint64(offset), 0; vecIdx < uint64(offset+len(groups)); vecIdx++ {
		if groups[vecIdx] != GroupNotMatched {
			groupIdx := int(groups[idx] - 1)
			isNew, err := exec.hashmaps[groupIdx].Insert(vectors, int(vecIdx))
			if err != nil {
				return err
			}
			if isNew {
				shouldBeFilled[idx] = true
			}
		}
		idx++
	}

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if shouldBeFilled[i] {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].FillNull(getter, setter)
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if shouldBeFilled[i] {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].Fill(value, getter, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if shouldBeFilled[idx] {
					v, null := exec.arg.w.GetValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].FillNull(getter, setter)
					} else {
						exec.groups[groupIdx].Fill(v, getter, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if shouldBeFilled[idx] {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].Fill(v, getter, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if shouldBeFilled[idx] {
			v, _ := exec.arg.w.GetValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].Fill(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec1[from, to])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleAggFuncExec1[from, to])
	setter := exec.ret.aggSet
	getter1, getter2 := exec.ret.aggGet, other.ret.aggGet

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset
		exec.ret.groupToSet = groupIdx1
		other.ret.groupToSet = groupIdx2

		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].Fill(exec.partialResult.(from), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.Flush(getter, setter)
	}
	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec1[from, to]) Free() {
	exec.ret.free()
}

func (exec *singleAggFuncExec2[from]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	nm func() SingleAggFromFixedRetVar[from]) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromFixedRetVar[from], 0, 1)
	exec.gGroup = nm
}

func (exec *singleAggFuncExec2[from]) GroupGrow(more int) error {
	moreGroup := make([]SingleAggFromFixedRetVar[from], more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()
		moreGroup[i].Init()
	}
	exec.groups = append(exec.groups, moreGroup...)
	return exec.ret.grows(more)
}

func (exec *singleAggFuncExec2[from]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].FillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].Fill(vector.MustFixedCol[from](vec)[row], getter, setter)
	return nil
}

func (exec *singleAggFuncExec2[from]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			var value from
			if exec.receiveNull {
				exec.groups[groupIndex].Fills(value, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].Fills(vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].FillNull(getter, setter)
				} else {
					exec.groups[groupIndex].Fill(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].Fill(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].Fill(v, getter, setter)
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].FillNull(getter, setter)
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].Fill(value, getter, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].FillNull(getter, setter)
					} else {
						exec.groups[groupIdx].Fill(v, getter, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].Fill(v, getter, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].Fill(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec2[from])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *singleAggFuncExec2[from]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleAggFuncExec2[from])
	setter := exec.ret.aggSet
	getter1, getter2 := exec.ret.aggGet, other.ret.aggGet

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset
		exec.ret.groupToSet = groupIdx1
		other.ret.groupToSet = groupIdx2

		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].Fill(exec.partialResult.(from), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.Flush(getter, setter)
	}
	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec2[from]) Free() {
	exec.ret.free()
}

func (exec *singleAggFuncExec3[to]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	nm func() SingleAggFromVarRetFixed[to]) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.ret = initFixedAggFuncResult[to](mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromVarRetFixed[to], 0, 1)
	exec.gGroup = nm
}

func (exec *singleAggFuncExec3[to]) GroupGrow(more int) error {
	moreGroup := make([]SingleAggFromVarRetFixed[to], more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()
		moreGroup[i].Init()
	}
	exec.groups = append(exec.groups, moreGroup...)
	return exec.ret.grows(more)
}

func (exec *singleAggFuncExec3[to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].FillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].FillBytes(vec.GetBytesAt(row), getter, setter)
	return nil
}

func (exec *singleAggFuncExec3[to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].Fills(nil, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].Fills(vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].FillNull(getter, setter)
				} else {
					exec.groups[groupIndex].FillBytes(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].FillBytes(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].FillBytes(v, getter, setter)
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].FillNull(getter, setter)
					}
				}
			}
			return nil
		}

		value := vec.GetBytesAt(0)
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].FillBytes(value, getter, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetStrValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].FillNull(getter, setter)
					} else {
						exec.groups[groupIdx].FillBytes(v, getter, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].FillBytes(v, getter, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].FillBytes(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec3[to])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *singleAggFuncExec3[to]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleAggFuncExec3[to])
	setter := exec.ret.aggSet
	getter1, getter2 := exec.ret.aggGet, other.ret.aggGet

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset
		exec.ret.groupToSet = groupIdx1
		other.ret.groupToSet = groupIdx2

		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].FillBytes(exec.partialResult.([]byte), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.Flush(getter, setter)
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec3[to]) Free() {
	exec.ret.free()
}

func (exec *singleAggFuncExec4) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	nm func() SingleAggFromVarRetVar) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromVarRetVar, 0, 1)
	exec.gGroup = nm
}

func (exec *singleAggFuncExec4) GroupGrow(more int) error {
	moreGroup := make([]SingleAggFromVarRetVar, more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()
		moreGroup[i].Init()
	}
	exec.groups = append(exec.groups, moreGroup...)
	return exec.ret.grows(more)
}

func (exec *singleAggFuncExec4) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].FillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].FillBytes(vec.GetBytesAt(row), getter, setter)
	return nil
}

func (exec *singleAggFuncExec4) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].Fills(nil, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].Fills(vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].FillNull(getter, setter)
				} else {
					exec.groups[groupIndex].FillBytes(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].FillBytes(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].FillBytes(v, getter, setter)
	}
	return nil
}

func (exec *singleAggFuncExec4) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].FillNull(getter, setter)
					}
				}
			}
			return nil
		}

		value := vec.GetBytesAt(0)
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].FillBytes(value, getter, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetStrValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].FillNull(getter, setter)
					} else {
						exec.groups[groupIdx].FillBytes(v, getter, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].FillBytes(v, getter, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].FillBytes(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec4) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec4)
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *singleAggFuncExec4) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleAggFuncExec4)
	setter := exec.ret.aggSet
	getter1, getter2 := exec.ret.aggGet, other.ret.aggGet

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset
		exec.ret.groupToSet = groupIdx1
		other.ret.groupToSet = groupIdx2

		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *singleAggFuncExec4) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].FillBytes(exec.partialResult.([]byte), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.Flush(getter, setter)
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec4) Free() {
	exec.ret.free()
}
