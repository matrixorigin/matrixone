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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// singleAggTypeInfo is the type info of single column agg.
type singleAggTypeInfo struct {
	argType types.Type
	retType types.Type
}

func (info singleAggTypeInfo) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{info.argType}, info.retType
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
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sFixedArg[from]
	ret    aggFuncResult[to]
	groups []singleAggPrivateStructure1[from, to]

	// method to new the private structure for group growing.
	gGroup func() singleAggPrivateStructure1[from, to]
}
type singleAggFuncExec2[from types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sFixedArg[from]
	ret    aggFuncBytesResult
	groups []singleAggPrivateStructure2[from]
}
type singleAggFuncExec3[to types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sBytesArg
	ret    aggFuncResult[to]
	groups []singleAggPrivateStructure3[to]
}
type singleAggFuncExec4 struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    sBytesArg
	ret    aggFuncBytesResult
	groups []singleAggPrivateStructure4
}

func (exec *singleAggFuncExec1[from, to]) Init(
	proc *process.Process,
	info singleAggTypeInfo,
	opt singleAggOptimizedInfo,
	nm func() singleAggPrivateStructure1[from, to]) {

	exec.singleAggTypeInfo = info
	exec.singleAggOptimizedInfo = opt
	exec.ret = initFixedAggFuncResult[to](proc, info.retType)
	exec.groups = make([]singleAggPrivateStructure1[from, to], 0, 1)
	exec.gGroup = nm
}

func (exec *singleAggFuncExec1[from, to]) GroupGrow(more int) error {
	moreGroup := make([]singleAggPrivateStructure1[from, to], more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()
		moreGroup[i].init()
	}
	exec.groups = append(exec.groups, moreGroup...)
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
			exec.groups[groupIndex].fillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].fill(vector.MustFixedCol[from](vec)[row], getter, setter)
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
				exec.groups[groupIndex].fills(value, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].fillNull(getter, setter)
				} else {
					exec.groups[groupIndex].fill(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].fill(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].fill(v, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
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
				exec.groups[groupIdx].fill(value, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
					} else {
						exec.groups[groupIdx].fill(v, getter, setter)
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
					exec.groups[groupIdx].fill(v, getter, setter)
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
			exec.groups[groupIdx].fill(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fill(exec.partialResult.(from), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(getter, setter)
	}

	// if it was ordered window, should clean the nulls.

	// if it was count, should clean the nulls.
	// todo: this can be a method of the agg like 'nullsIfEmpty' or something.

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec1[from, to]) Free() {
	exec.ret.free()
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
			exec.groups[groupIndex].fillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].fill(vector.MustFixedCol[from](vec)[row], getter, setter)
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
				exec.groups[groupIndex].fills(value, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vector.MustFixedCol[from](vec)[0], false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].fillNull(getter, setter)
				} else {
					exec.groups[groupIndex].fill(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].fill(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].fill(v, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
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
				exec.groups[groupIdx].fill(value, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
					} else {
						exec.groups[groupIdx].fill(v, getter, setter)
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
					exec.groups[groupIdx].fill(v, getter, setter)
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
			exec.groups[groupIdx].fill(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fill(exec.partialResult.(from), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(getter, setter)
	}

	// copy from singleAggFuncExec1's Flush()
	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec2[from]) Free() {
	exec.ret.free()
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
			exec.groups[groupIndex].fillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].fillBytes(vec.GetBytesAt(row), getter, setter)
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
				exec.groups[groupIndex].fills(nil, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].fillNull(getter, setter)
				} else {
					exec.groups[groupIndex].fillBytes(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].fillBytes(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].fillBytes(v, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
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
				exec.groups[groupIdx].fillBytes(value, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
					} else {
						exec.groups[groupIdx].fillBytes(v, getter, setter)
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
					exec.groups[groupIdx].fillBytes(v, getter, setter)
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
			exec.groups[groupIdx].fillBytes(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fillBytes(exec.partialResult.([]byte), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(getter, setter)
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec3[to]) Free() {
	exec.ret.free()
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
			exec.groups[groupIndex].fillNull(getter, setter)
		}
		return nil
	}

	exec.groups[groupIndex].fillBytes(vec.GetBytesAt(row), getter, setter)
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
				exec.groups[groupIndex].fills(nil, true, length, getter, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vec.GetBytesAt(0), false, length, getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].fillNull(getter, setter)
				} else {
					exec.groups[groupIndex].fillBytes(v, getter, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].fillBytes(v, getter, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].fillBytes(v, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
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
				exec.groups[groupIdx].fillBytes(value, getter, setter)
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
						exec.groups[groupIdx].fillNull(getter, setter)
					} else {
						exec.groups[groupIdx].fillBytes(v, getter, setter)
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
					exec.groups[groupIdx].fillBytes(v, getter, setter)
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
			exec.groups[groupIdx].fillBytes(v, getter, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec4) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fillBytes(exec.partialResult.([]byte), exec.ret.aggGet, exec.ret.aggSet)
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(getter, setter)
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec4) Free() {
	exec.ret.free()
}
