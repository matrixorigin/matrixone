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
)

type multiAggTypeInfo struct {
	argTypes []types.Type
	retType  types.Type
}

func (info multiAggTypeInfo) TypesInfo() ([]types.Type, types.Type) {
	return info.argTypes, info.retType
}

type multiAggOptimizedInfo struct {
	// receiveNull indicates whether the agg function receives null value.
	// if it was false, the rows with any null value will be ignored.
	receiveNull bool
}

// multiAggFuncExec1 and multiAggFuncExec2 are the executors of multi columns agg.
// 1's return type is a fixed length type.
// 2's return type is bytes.
type multiAggFuncExec1[T types.FixedSizeTExceptStrType] struct {
	multiAggTypeInfo
	multiAggOptimizedInfo

	args   []aggArg
	ret    aggFuncResult[T]
	groups []multiAggPrivateStructure1[T]
}
type multiAggFuncExec2 struct {
	multiAggTypeInfo
	multiAggOptimizedInfo

	args   []aggArg
	ret    aggFuncBytesResult
	groups []multiAggPrivateStructure2
}

func (exec *multiAggFuncExec1[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	for i := range vectors {
		exec.args[i].Prepare(vectors[i])
		if err := exec.fill(groupIndex, i, row, 1); err != nil {
			return err
		}
	}
	return nil
}

func (exec *multiAggFuncExec1[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for i := range vectors {
		exec.args[i].Prepare(vectors[i])
		if err := exec.fill(groupIndex, i, 0, vectors[i].Length()); err != nil {
			return err
		}
	}
	return nil
}

func (exec *multiAggFuncExec1[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	// todo: need a quick deal to avoid many switches.
	return nil
}

func (exec *multiAggFuncExec1[T]) SetPreparedResult(partialResult any, groupIndex int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec1")
}

func (exec *multiAggFuncExec1[T]) Flush() (*vector.Vector, error) {
	for i, group := range exec.groups {
		exec.ret.set(i, group.flush())
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec1[T]) Free() {
	exec.ret.free()
}

// should prepare the aggArg before calling this function.
// we will get values from the aggArg directly.
func (exec *multiAggFuncExec1[T]) fill(groupIndex int, columnIdx int, offset int, length int) error {
	switch exec.argTypes[columnIdx].Oid {
	case types.T_int8:
		// todo: maybe we can cache the fill function. but for some agg functions like 'group_concat',
		//  the number of fill function was unlimited. It will be a bad idea ?
		//  but for the whole query, the cache was still a small number.
		//
		arg := exec.args[columnIdx].(*aggFuncArg[int8])
		fill := exec.groups[groupIndex].getFillWhich(columnIdx).(func(int8))
		fillNull := exec.groups[groupIndex].getFillNullWhich(columnIdx).(func())
		fills := exec.groups[groupIndex].getFillsWhich(columnIdx).(func(int8, bool, int))
		return ff1[int8](arg, offset, length, fill, fillNull, fills)
	}
	return nil
}

func (exec *multiAggFuncExec2) fill(groupIndex int, columnIdx int, offset int, length int) error {
	// codes like the multiAggFuncExec1 above.
	return nil
}

func ff1[T types.FixedSizeTExceptStrType](
	source *aggFuncArg[T], offset int, length int,
	fill func(T), fillNull func(), fills func(T, bool, int)) error {

	// todo: check the const here and we can call the fills.
	// if source.w.IsConst() {

	if source.w.WithAnyNullValue() {
		for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
			v, null := source.w.GetValue(i)
			if null {
				fillNull()
			} else {
				fill(v)
			}
		}
		return nil
	}

	for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
		v, _ := source.w.GetValue(i)
		fill(v)
	}

	return nil
}

func ff2(
	source aggFuncBytesArg, offset int, length int,
	fill func([]byte), fillNull func(), fills func([]byte, bool, int)) error {

	if source.w.WithAnyNullValue() {
		for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
			v, null := source.w.GetStrValue(i)
			if null {
				fillNull()
			} else {
				fill(v)
			}
		}
		return nil
	}

	for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
		v, _ := source.w.GetStrValue(i)
		fill(v)
	}

	return nil
}
