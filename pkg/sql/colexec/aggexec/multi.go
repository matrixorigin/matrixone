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

// multiAggFuncExec1 and multiAggFuncExec2 are the executors of multi columns agg.
// 1's return type is a fixed length type.
// 2's return type is bytes.
type multiAggFuncExec1[T types.FixedSizeTExceptStrType] struct {
	multiAggTypeInfo

	args   []mArg1[T]
	ret    aggFuncResult[T]
	groups []multiAggPrivateStructure1[T]
}
type multiAggFuncExec2 struct {
	multiAggTypeInfo

	args   []mArg2
	ret    aggFuncBytesResult
	groups []multiAggPrivateStructure2
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
	exec.groups[groupIndex].eval(exec.ret.aggGet, exec.ret.aggSet)

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
		exec.groups[groupIndex].eval(getter, setter)
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
			exec.groups[groupIdx].eval(getter, setter)

		}
		idx++
	}

	return nil
}

func (exec *multiAggFuncExec1[T]) SetPreparedResult(_ any, _ int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec1")
}

func (exec *multiAggFuncExec1[T]) Flush() (*vector.Vector, error) {
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(getter, setter)
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec1[T]) Free() {
	exec.ret.free()
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
	exec.groups[groupIndex].eval(exec.ret.aggGet, exec.ret.aggSet)

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
	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		for _, arg := range exec.args {
			if err = arg.doRowFill(exec.groups[groupIndex], i); err != nil {
				return err
			}
		}
		exec.groups[groupIndex].eval(getter, setter)
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
			exec.groups[groupIdx].eval(getter, setter)

		}
		idx++
	}

	return nil
}

func (exec *multiAggFuncExec2) SetPreparedResult(_ any, _ int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec2")
}

func (exec *multiAggFuncExec2) Flush() (*vector.Vector, error) {
	var err error
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		if err = group.flush(getter, setter); err != nil {
			return nil, err
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
