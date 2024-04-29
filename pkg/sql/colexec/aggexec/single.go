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

func (info singleAggInfo) eq(other singleAggInfo) bool {
	return info.aggID == other.aggID &&
		info.distinct == other.distinct &&
		info.argType.Eq(other.argType) &&
		info.retType.Eq(other.retType) &&
		info.emptyNull == other.emptyNull
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

type singleAggExecExtraInformation struct {
	partialGroup  int
	partialResult any
}

func (optimized *singleAggExecExtraInformation) SetExtraInformation(partialResult any, groupIndex int) error {
	optimized.partialGroup = groupIndex
	optimized.partialResult = partialResult
	return nil
}

// the executors of single column agg.
// singleAggFuncExec1 receives a fixed size type except string and returns a fixed size type except string.
// singleAggFuncExec2 receives a fixed size type except string and returns a byte type.
// singleAggFuncExec3 receives a byte type and returns a fixed size type except string.
// singleAggFuncExec4 receives a byte type and returns a byte type.
type singleAggFuncExec1[from, to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecExtraInformation
	distinctHash

	arg    sFixedArg[from]
	ret    aggFuncResult[to]
	groups []SingleAggFromFixedRetFixed[from, to]

	initGroup SingleAggInit1[from, to]
	fill      SingleAggFill1[from, to]
	fillNull  SingleAggFillNull1[from, to]
	fills     SingleAggFills1[from, to]
	merge     SingleAggMerge1[from, to]
	flush     SingleAggFlush1[from, to]

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromFixedRetFixed[from, to]
}
type singleAggFuncExec2[from types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecExtraInformation
	distinctHash

	arg    sFixedArg[from]
	ret    aggFuncBytesResult
	groups []SingleAggFromFixedRetVar[from]

	initGroup SingleAggInit2[from]
	fill      SingleAggFill2[from]
	fillNull  SingleAggFillNull2[from]
	fills     SingleAggFills2[from]
	merge     SingleAggMerge2[from]
	flush     SingleAggFlush2[from]

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromFixedRetVar[from]
}
type singleAggFuncExec3[to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecExtraInformation
	distinctHash

	arg    sBytesArg
	ret    aggFuncResult[to]
	groups []SingleAggFromVarRetFixed[to]

	initGroup SingleAggInit3[to]
	fill      SingleAggFill3[to]
	fillNull  SingleAggFillNull3[to]
	fills     SingleAggFills3[to]
	merge     SingleAggMerge3[to]
	flush     SingleAggFlush3[to]

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromVarRetFixed[to]
}
type singleAggFuncExec4 struct {
	singleAggInfo
	singleAggOptimizedInfo
	singleAggExecExtraInformation
	distinctHash

	arg    sBytesArg
	ret    aggFuncBytesResult
	groups []SingleAggFromVarRetVar

	initGroup SingleAggInit4
	fill      SingleAggFill4
	fillNull  SingleAggFillNull4
	fills     SingleAggFills4
	merge     SingleAggMerge4
	flush     SingleAggFlush4

	// method to new the private structure for group growing.
	gGroup func() SingleAggFromVarRetVar
}

func (exec *singleAggFuncExec1[from, to]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	impl aggImplementation) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.ret = initFixedAggFuncResult[to](mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromFixedRetFixed[from, to], 0, 1)
	exec.gGroup = impl.generator.(func() SingleAggFromFixedRetFixed[from, to])
	exec.fills = impl.fills.(SingleAggFills1[from, to])
	exec.fill = impl.fill.(SingleAggFill1[from, to])
	exec.merge = impl.merge.(SingleAggMerge1[from, to])
	if impl.fillNull != nil {
		exec.fillNull = impl.fillNull.(SingleAggFillNull1[from, to])
	}
	if impl.flush != nil {
		exec.flush = impl.flush.(SingleAggFlush1[from, to])
	}
	if impl.init != nil {
		exec.initGroup = impl.init.(SingleAggInit1[from, to])
	}

	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), opt.receiveNull)
	}
}

func (exec *singleAggFuncExec1[from, to]) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	if err := exec.ret.grows(more); err != nil {
		return err
	}

	setter := exec.ret.aggSet
	oldLength := len(exec.groups)
	exec.groups = append(exec.groups, make([]SingleAggFromFixedRetFixed[from, to], more)...)
	for i, j := oldLength, len(exec.groups); i < j; i++ {
		exec.groups[i] = exec.gGroup()
	}

	if exec.initGroup != nil {
		for i, j := oldLength, len(exec.groups); i < j; i++ {
			exec.ret.groupToSet = i
			if err := exec.initGroup(exec.groups[i], setter, exec.singleAggInfo.argType, exec.singleAggInfo.retType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); !need || err != nil {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fillNull(exec.groups[groupIndex], getter, setter)
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	return exec.fill(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[row], getter, setter)
}

func (exec *singleAggFuncExec1[from, to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	length := vectors[0].Length()
	if length == 0 {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors, length)
	}

	vec := vectors[0]
	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			var value from
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fills(exec.groups[groupIndex], value, true, length, getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fills(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					if err := exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		} else {
			mustNotEmpty := false
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					mustNotEmpty = true
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
			if mustNotEmpty {
				exec.ret.setGroupNotEmpty(groupIndex)
			}
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
			return err
		}
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

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
						exec.ret.setGroupNotEmpty(groupIdx)
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return nil
						}
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
				exec.ret.setGroupNotEmpty(groupIdx)
				if err := exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return nil
				}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if null {
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					} else {
						if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
							return err
						}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
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
			exec.ret.setGroupNotEmpty(groupIdx)
			if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec1[from, to])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	if err := exec.merge(
		exec.groups[groupIdx1],
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet); err != nil {
		return err
	}
	return exec.distinctHash.merge(&other.distinctHash)
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

		exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
		if err := exec.merge(
			exec.groups[groupIdx1],
			other.groups[groupIdx2],
			getter1, getter2,
			setter); err != nil {
			return err
		}
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExec1[from, to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		if vs, ok := exec.partialResult.(from); ok {
			exec.ret.setGroupNotEmpty(exec.partialGroup)
			if err := exec.fill(exec.groups[exec.partialGroup], vs, exec.ret.aggGet, exec.ret.aggSet); err != nil {
				return nil, err
			}
		}
	}

	if exec.flush == nil {
		return exec.ret.flush(), nil
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec1[from, to]) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *singleAggFuncExec2[from]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	agg aggImplementation) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromFixedRetVar[from], 0, 1)
	exec.gGroup = agg.generator.(func() SingleAggFromFixedRetVar[from])

	exec.fills = agg.fills.(SingleAggFills2[from])
	exec.fill = agg.fill.(SingleAggFill2[from])
	exec.merge = agg.merge.(SingleAggMerge2[from])
	if agg.fillNull != nil {
		exec.fillNull = agg.fillNull.(SingleAggFillNull2[from])
	}
	if agg.flush != nil {
		exec.flush = agg.flush.(SingleAggFlush2[from])
	}
	if agg.init != nil {
		exec.initGroup = agg.init.(SingleAggInit2[from])
	}

	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), opt.receiveNull)
	}
}

func (exec *singleAggFuncExec2[from]) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	if err := exec.ret.grows(more); err != nil {
		return err
	}

	setter := exec.ret.aggSet
	oldLength := len(exec.groups)
	exec.groups = append(exec.groups, make([]SingleAggFromFixedRetVar[from], more)...)
	for i, j := oldLength, len(exec.groups); i < j; i++ {
		exec.groups[i] = exec.gGroup()
	}
	if exec.initGroup != nil {
		for i, j := oldLength, len(exec.groups); i < j; i++ {
			exec.ret.groupToSet = i
			if err := exec.initGroup(exec.groups[i], setter, exec.singleAggInfo.argType, exec.singleAggInfo.retType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); !need || err != nil {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fillNull(exec.groups[groupIndex], getter, setter)
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	return exec.fill(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[row], getter, setter)
}

func (exec *singleAggFuncExec2[from]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	if length == 0 {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors, length)
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			var value from
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fills(exec.groups[groupIndex], value, true, length, getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fills(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					if err := exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		} else {
			mustNotEmpty := false
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					mustNotEmpty = true
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
			if mustNotEmpty {
				exec.ret.setGroupNotEmpty(groupIndex)
			}
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
			return err
		}
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

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
						exec.ret.setGroupNotEmpty(groupIdx)
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return nil
						}
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
				exec.ret.setGroupNotEmpty(groupIdx)
				if err := exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return nil
				}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if null {
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					} else {
						if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
							return err
						}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
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
			exec.ret.setGroupNotEmpty(groupIdx)
			if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec2[from])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	if err := exec.merge(
		exec.groups[groupIdx1],
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet); err != nil {
		return err
	}
	return exec.distinctHash.merge(&other.distinctHash)
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

		exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
		if err := exec.merge(
			exec.groups[groupIdx1],
			other.groups[groupIdx2],
			getter1, getter2,
			setter); err != nil {
			return err
		}
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExec2[from]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		if vs, ok := exec.partialResult.(from); ok {
			exec.ret.setGroupNotEmpty(exec.partialGroup)
			if err := exec.fill(exec.groups[exec.partialGroup], vs, exec.ret.aggGet, exec.ret.aggSet); err != nil {
				return nil, err
			}
		}
	}

	if exec.flush == nil {
		return exec.ret.flush(), nil
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	}
	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec2[from]) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *singleAggFuncExec3[to]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	impl aggImplementation) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.ret = initFixedAggFuncResult[to](mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromVarRetFixed[to], 0, 1)
	exec.gGroup = impl.generator.(func() SingleAggFromVarRetFixed[to])

	exec.fills = impl.fills.(SingleAggFills3[to])
	exec.fill = impl.fill.(SingleAggFill3[to])
	exec.merge = impl.merge.(SingleAggMerge3[to])
	if impl.fillNull != nil {
		exec.fillNull = impl.fillNull.(SingleAggFillNull3[to])
	}
	if impl.flush != nil {
		exec.flush = impl.flush.(SingleAggFlush3[to])
	}
	if impl.init != nil {
		exec.initGroup = impl.init.(SingleAggInit3[to])
	}

	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), opt.receiveNull)
	}
}

func (exec *singleAggFuncExec3[to]) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	if err := exec.ret.grows(more); err != nil {
		return err
	}

	setter := exec.ret.aggSet
	oldLength := len(exec.groups)
	exec.groups = append(exec.groups, make([]SingleAggFromVarRetFixed[to], more)...)
	for i, j := oldLength, len(exec.groups); i < j; i++ {
		exec.groups[i] = exec.gGroup()
	}

	if exec.initGroup != nil {
		for i, j := oldLength, len(exec.groups); i < j; i++ {
			exec.ret.groupToSet = i
			if err := exec.initGroup(exec.groups[i], setter, exec.singleAggInfo.argType, exec.singleAggInfo.retType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); !need || err != nil {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fillNull(exec.groups[groupIndex], getter, setter)
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	return exec.fill(exec.groups[groupIndex], vec.GetBytesAt(row), getter, setter)
}

func (exec *singleAggFuncExec3[to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	if length == 0 {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors, length)
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fills(exec.groups[groupIndex], nil, true, length, getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fills(exec.groups[groupIndex], vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					if err := exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		} else {
			mustNotEmpty := false
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					mustNotEmpty = true
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
			if mustNotEmpty {
				exec.ret.setGroupNotEmpty(groupIndex)
			}
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
			return err
		}
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

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
						exec.ret.setGroupNotEmpty(groupIdx)
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return nil
						}
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
				exec.ret.setGroupNotEmpty(groupIdx)
				if err := exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return nil
				}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if null {
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					} else {
						if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
							return err
						}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
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
			exec.ret.setGroupNotEmpty(groupIdx)
			if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec3[to])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	if err := exec.merge(
		exec.groups[groupIdx1],
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet); err != nil {
		return err
	}
	return exec.distinctHash.merge(&other.distinctHash)
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

		exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
		if err := exec.merge(
			exec.groups[groupIdx1],
			other.groups[groupIdx2],
			getter1, getter2,
			setter); err != nil {
			return err
		}
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExec3[to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		if vs, ok := exec.partialResult.([]byte); ok {
			exec.ret.setGroupNotEmpty(exec.partialGroup)
			if err := exec.fill(exec.groups[exec.partialGroup], vs, exec.ret.aggGet, exec.ret.aggSet); err != nil {
				return nil, err
			}
		}
	}

	if exec.flush == nil {
		return exec.ret.flush(), nil
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec3[to]) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *singleAggFuncExec4) init(
	mg AggMemoryManager,
	info singleAggInfo,
	opt singleAggOptimizedInfo,
	impl aggImplementation) {

	exec.singleAggInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
	exec.groups = make([]SingleAggFromVarRetVar, 0, 1)
	exec.gGroup = impl.generator.(func() SingleAggFromVarRetVar)
	exec.fills = impl.fills.(SingleAggFills4)
	exec.fill = impl.fill.(SingleAggFill4)
	exec.merge = impl.merge.(SingleAggMerge4)
	if impl.fillNull != nil {
		exec.fillNull = impl.fillNull.(SingleAggFillNull4)
	}
	if impl.flush != nil {
		exec.flush = impl.flush.(SingleAggFlush4)
	}
	if impl.init != nil {
		exec.initGroup = impl.init.(SingleAggInit4)
	}

	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), opt.receiveNull)
	}
}

func (exec *singleAggFuncExec4) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	if err := exec.ret.grows(more); err != nil {
		return err
	}

	setter := exec.ret.aggSet
	oldLength := len(exec.groups)
	exec.groups = append(exec.groups, make([]SingleAggFromVarRetVar, more)...)
	for i, j := oldLength, len(exec.groups); i < j; i++ {
		exec.groups[i] = exec.gGroup()
	}

	if exec.initGroup != nil {
		for i, j := oldLength, len(exec.groups); i < j; i++ {
			exec.ret.groupToSet = i
			if err := exec.initGroup(exec.groups[i], setter, exec.singleAggInfo.argType, exec.singleAggInfo.retType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *singleAggFuncExec4) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); !need || err != nil {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fillNull(exec.groups[groupIndex], getter, setter)
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	return exec.fill(exec.groups[groupIndex], vec.GetBytesAt(row), getter, setter)
}

func (exec *singleAggFuncExec4) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	if length == 0 {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors, length)
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fills(exec.groups[groupIndex], nil, true, length, getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fills(exec.groups[groupIndex], vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			exec.ret.setGroupNotEmpty(groupIndex)
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					if err := exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		} else {
			mustNotEmpty := false
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					mustNotEmpty = true
					if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
			if mustNotEmpty {
				exec.ret.setGroupNotEmpty(groupIndex)
			}
		}
		return nil
	}

	exec.ret.setGroupNotEmpty(groupIndex)
	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		if err := exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
			return err
		}
	}
	return nil
}

func (exec *singleAggFuncExec4) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

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
						exec.ret.setGroupNotEmpty(groupIdx)
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
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
				exec.ret.setGroupNotEmpty(groupIdx)
				if err := exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return err
				}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if null {
						if err := exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					} else {
						if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
							return err
						}
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
					exec.ret.setGroupNotEmpty(groupIdx)
					if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
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
			exec.ret.setGroupNotEmpty(groupIdx)
			if err := exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec4) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExec4)
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	if err := exec.merge(
		exec.groups[groupIdx1], other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet); err != nil {
		return err
	}
	return exec.distinctHash.merge(&other.distinctHash)
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

		exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
		if err := exec.merge(
			exec.groups[groupIdx1], other.groups[groupIdx2],
			getter1, getter2,
			setter); err != nil {
			return err
		}
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExec4) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		if vs, ok := exec.partialResult.([]byte); ok {
			exec.ret.setGroupNotEmpty(exec.partialGroup)
			if err := exec.fill(exec.groups[exec.partialGroup], vs, exec.ret.aggGet, exec.ret.aggSet); err != nil {
				return nil, err
			}
		}
	}

	if exec.flush == nil {
		return exec.ret.flush(), nil
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			if err := exec.flush(group, getter, setter); err != nil {
				return nil, err
			}
		}
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec4) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}
