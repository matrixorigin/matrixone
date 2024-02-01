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

type multiAggExecOptimized struct {
}

// multiAggFuncExec1 and multiAggFuncExec2 are the executors of multi columns agg.
// 1's return type is a fixed length type.
// 2's return type is bytes.
type multiAggFuncExec1[T types.FixedSizeTExceptStrType] struct {
	multiAggTypeInfo
	multiAggExecOptimized

	args   []aggArg
	ret    aggFuncResult[T]
	groups []multiAggPrivateStructure1[T]
}
type multiAggFuncExec2 struct {
	multiAggTypeInfo
	multiAggExecOptimized

	args   []aggArg
	ret    aggFuncBytesResult
	groups []multiAggPrivateStructure2
}

func (exec *multiAggFuncExec1[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec1[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec1[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec1[T]) SetPreparedResult(_ any, _ int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec1")
}

func (exec *multiAggFuncExec1[T]) Flush() (*vector.Vector, error) {
	setter := exec.ret.aggSet
	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		group.flush(setter)
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec1[T]) Free() {
	exec.ret.free()
}

func (exec *multiAggFuncExec2) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec2) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec2) BatchFill(offset int, groups []uint64, vector []*vector.Vector) error {
	return nil
}

func (exec *multiAggFuncExec2) SetPreparedResult(_ any, _ int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec2")
}

func (exec *multiAggFuncExec2) Flush() (*vector.Vector, error) {
	var err error
	setter := exec.ret.aggSet

	for i, group := range exec.groups {
		exec.ret.groupToSet = i
		if err = group.flush(setter); err != nil {
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

func ff1[T types.FixedSizeTExceptStrType](
	source aggArg, row uint64) error {

	_arg := source.(*aggFuncArg[T])

	v, null := _arg.w.GetValue(row)
	if null {
		_arg.fillNull()
	} else {
		_arg.fill(v)
	}

	return nil
}

func ff2(
	source aggArg, row uint64) error {

	_arg := source.(*aggFuncBytesArg)

	v, null := _arg.w.GetStrValue(row)
	if null {
		_arg.fillNull()
	} else {
		_arg.fill(v)
	}

	return nil
}

var multiFill = map[types.T]func(arg aggArg, row uint64) error{
	types.T_bool: ff1[bool],

	types.T_int8:    ff1[int8],
	types.T_int16:   ff1[int16],
	types.T_int32:   ff1[int32],
	types.T_int64:   ff1[int64],
	types.T_uint8:   ff1[uint8],
	types.T_uint16:  ff1[uint16],
	types.T_uint32:  ff1[uint32],
	types.T_uint64:  ff1[uint64],
	types.T_float32: ff1[float32],
	types.T_float64: ff1[float64],
	types.T_uuid:    ff1[types.Uuid],

	types.T_date:      ff1[int32],
	types.T_datetime:  ff1[int64],
	types.T_time:      ff1[types.Time],
	types.T_timestamp: ff1[types.Timestamp],
	types.T_interval:  ff1[types.IntervalType],

	types.T_decimal64:  ff1[types.Decimal64],
	types.T_decimal128: ff1[types.Decimal128],
	types.T_decimal256: ff1[types.Decimal256],

	types.T_char:      ff2,
	types.T_varchar:   ff2,
	types.T_text:      ff2,
	types.T_json:      ff2,
	types.T_blob:      ff2,
	types.T_binary:    ff2,
	types.T_varbinary: ff2,

	types.T_enum:    ff1[types.Enum],
	types.T_Rowid:   ff1[types.Rowid],
	types.T_Blockid: ff1[types.Blockid],

	types.T_array_float32: ff2,
	types.T_array_float64: ff2,
}
