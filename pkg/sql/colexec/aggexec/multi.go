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

type multiAggInfo struct {
	aggID    int64
	distinct bool
	argTypes []types.Type
	retType  types.Type

	// emptyNull indicates that whether we should return null for a group without any input value.
	emptyNull bool
}

func (info multiAggInfo) String() string {
	args := "[" + info.argTypes[0].String()
	for i := 1; i < len(info.argTypes); i++ {
		args += ", " + info.argTypes[i].String()
	}
	args += "]"
	return fmt.Sprintf("{aggID: %d, argTypes: %s, retType: %s}", info.aggID, args, info.retType.String())
}

func (info multiAggInfo) AggID() int64 {
	return info.aggID
}

func (info multiAggInfo) IsDistinct() bool {
	return info.distinct
}

func (info multiAggInfo) TypesInfo() ([]types.Type, types.Type) {
	return info.argTypes, info.retType
}

func (info multiAggInfo) getEncoded() *EncodedBasicInfo {
	return &EncodedBasicInfo{
		Id:         info.aggID,
		IsDistinct: info.distinct,
		Args:       info.argTypes,
		Ret:        info.retType,
	}
}

// multiAggFuncExec1 and multiAggFuncExec2 are the executors of multi columns agg.
// 1's return type is a fixed length type.
// 2's return type is bytes.
type multiAggFuncExec1[T types.FixedSizeTExceptStrType] struct {
	multiAggInfo

	args   []mArg1[T]
	ret    aggFuncResult[T]
	groups []MultiAggRetFixed[T]

	// method to new the private structure for group growing.
	gGroup func() MultiAggRetFixed[T]
}
type multiAggFuncExec2 struct {
	multiAggInfo

	args   []mArg2
	ret    aggFuncBytesResult
	groups []MultiAggRetVar

	// method to new the private structure for group growing.
	gGroup func() MultiAggRetVar
}

func (exec *multiAggFuncExec1[T]) init(
	mg AggMemoryManager,
	info multiAggInfo,
	nm func() MultiAggRetFixed[T]) {

	exec.multiAggInfo = info
	exec.args = make([]mArg1[T], len(info.argTypes))
	exec.ret = initFixedAggFuncResult[T](mg, info.retType, info.emptyNull)
	exec.groups = make([]MultiAggRetFixed[T], 0, 1)
	exec.gGroup = nm
	exec.args = make([]mArg1[T], len(info.argTypes))
	for i := range exec.args {
		exec.args[i] = newArgumentOfMultiAgg1[T](info.argTypes[i])

		t := nm()
		exec.args[i].cacheFill(t.GetWhichFill(i), t.GetWhichFillNull(i).(func(MultiAggRetFixed[T])))
	}
}

func (exec *multiAggFuncExec1[T]) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	setter := exec.ret.aggSet
	moreGroup := make([]MultiAggRetFixed[T], more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()

		exec.ret.groupToSet = i + len(exec.groups)
		moreGroup[i].Init(setter, exec.argTypes, exec.retType)
	}
	exec.groups = append(exec.groups, moreGroup...)
	return nil
}

func (exec *multiAggFuncExec1[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	var err error
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
		if err = arg.doRowFill(exec.groups[groupIndex], uint64(row)); err != nil {
			return err
		}
	}
	exec.ret.groupToSet = groupIndex
	if exec.groups[groupIndex].Valid() {
		exec.ret.setGroupNotEmpty(groupIndex)
		exec.groups[groupIndex].Eval(exec.ret.aggGet, exec.ret.aggSet)
	}

	return nil
}

func (exec *multiAggFuncExec1[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	var err error
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	exec.ret.groupToSet = groupIndex
	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		for _, arg := range exec.args {
			if err = arg.doRowFill(exec.groups[groupIndex], i); err != nil {
				return err
			}
		}
		if exec.groups[groupIndex].Valid() {
			exec.ret.setGroupNotEmpty(groupIndex)
			exec.groups[groupIndex].Eval(getter, setter)
		}
	}

	return nil
}

func (exec *multiAggFuncExec1[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	var err error
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
	}

	for idx, i, j := 0, uint64(offset), uint64(offset+len(groups)); i < j; i++ {
		if groups[idx] != GroupNotMatched {
			groupIdx := int(groups[idx] - 1)
			for _, arg := range exec.args {
				if err = arg.doRowFill(exec.groups[groupIdx], i); err != nil {
					return err
				}
			}
			exec.ret.groupToSet = groupIdx
			if exec.groups[groupIdx].Valid() {
				exec.ret.setGroupNotEmpty(groupIdx)
				exec.groups[groupIdx].Eval(getter, setter)
			}

		}
		idx++
	}

	return nil
}

func (exec *multiAggFuncExec1[T]) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("unimplemented SetPreparedResult for multiAggFuncExec1")
}

func (exec *multiAggFuncExec1[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*multiAggFuncExec1[T])
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *multiAggFuncExec1[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*multiAggFuncExec1[T])
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
		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *multiAggFuncExec1[T]) Flush() (*vector.Vector, error) {
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			group.Flush(getter, setter)
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			group.Flush(getter, setter)
		}
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec1[T]) Free() {
	exec.ret.free()
}

func (exec *multiAggFuncExec2) init(
	mg AggMemoryManager,
	info multiAggInfo,
	nm func() MultiAggRetVar) {

	exec.multiAggInfo = info
	exec.args = make([]mArg2, len(info.argTypes))
	exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
	exec.groups = make([]MultiAggRetVar, 0, 1)
	exec.gGroup = nm
	exec.args = make([]mArg2, len(info.argTypes))
	for i := range exec.args {
		exec.args[i] = newArgumentOfMultiAgg2(info.argTypes[i])

		t := nm()
		exec.args[i].cacheFill(t.GetWhichFill(i), t.GetWhichFillNull(i).(func(MultiAggRetVar)))
	}
}

func (exec *multiAggFuncExec2) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	setter := exec.ret.aggSet
	moreGroup := make([]MultiAggRetVar, more)
	for i := 0; i < more; i++ {
		moreGroup[i] = exec.gGroup()

		exec.ret.groupToSet = i + len(exec.groups)
		moreGroup[i].Init(setter, exec.argTypes, exec.retType)
	}
	exec.groups = append(exec.groups, moreGroup...)
	return nil
}

func (exec *multiAggFuncExec2) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	var err error
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
		if err = arg.doRowFill(exec.groups[groupIndex], uint64(row)); err != nil {
			return err
		}
	}
	exec.ret.groupToSet = groupIndex
	if exec.groups[groupIndex].Valid() {
		exec.ret.setGroupNotEmpty(groupIndex)
		exec.groups[groupIndex].Eval(exec.ret.aggGet, exec.ret.aggSet)
	}

	return nil
}

func (exec *multiAggFuncExec2) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	var err error
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
	}

	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	exec.ret.groupToSet = groupIndex

	// todo: can do optimization here once all the vectors were constant.

	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		for _, arg := range exec.args {
			if err = arg.doRowFill(exec.groups[groupIndex], i); err != nil {
				return err
			}
		}
		if exec.groups[groupIndex].Valid() {
			exec.ret.setGroupNotEmpty(groupIndex)
			exec.groups[groupIndex].Eval(getter, setter)
		}
	}

	return nil
}

func (exec *multiAggFuncExec2) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	var err error
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, arg := range exec.args {
		arg.prepare(vectors[i])
	}

	for idx, i, j := 0, uint64(offset), uint64(offset+len(groups)); i < j; i++ {
		if groups[idx] != GroupNotMatched {
			groupIdx := int(groups[idx] - 1)
			for _, arg := range exec.args {
				if err = arg.doRowFill(exec.groups[groupIdx], i); err != nil {
					return err
				}
			}
			exec.ret.groupToSet = groupIdx
			if exec.groups[groupIdx].Valid() {
				exec.ret.setGroupNotEmpty(groupIdx)
				exec.groups[groupIdx].Eval(getter, setter)
			}

		}
		idx++
	}

	return nil
}

func (exec *multiAggFuncExec2) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("unimplemented SetPreparedResult for multiAggFuncExec2")
}

func (exec *multiAggFuncExec2) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*multiAggFuncExec2)
	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2

	exec.ret.mergeEmpty(other.ret.basicResult, groupIdx1, groupIdx2)
	exec.groups[groupIdx1].Merge(
		other.groups[groupIdx2],
		exec.ret.aggGet, other.ret.aggGet,
		exec.ret.aggSet)
	return nil
}

func (exec *multiAggFuncExec2) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*multiAggFuncExec2)
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
		exec.groups[groupIdx1].Merge(
			other.groups[groupIdx2],
			getter1, getter2,
			setter)
	}
	return nil
}

func (exec *multiAggFuncExec2) Flush() (*vector.Vector, error) {
	var err error
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	if exec.ret.emptyBeNull {
		for i, group := range exec.groups {
			if exec.ret.groupIsEmpty(i) {
				continue
			}
			exec.ret.groupToSet = i
			if err = group.Flush(getter, setter); err != nil {
				return nil, err
			}
		}
	} else {
		for i, group := range exec.groups {
			exec.ret.groupToSet = i
			if err = group.Flush(getter, setter); err != nil {
				return nil, err
			}
		}
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec2) Free() {
	exec.ret.free()
}

func (exec *multiAggFuncExec1[T]) fills(groupIndex int, row uint64) error {
	return nil
}
