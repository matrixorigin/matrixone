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
)

type multiAggTypeInfo struct {
	argTypes []types.Type
	retType  types.Type
}

func (info multiAggTypeInfo) TypesInfo() ([]types.Type, types.Type) {
	return info.argTypes, info.retType
}

type multiAggOptimizedInfo struct {
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
	for i := range vectors {
		exec.args[i].Prepare(vectors[i])
	}

	var groupIdx int
	var err error
	var i = 0
	for rowIdx, end := offset, offset+len(groups); rowIdx < end; rowIdx++ {
		if groups[i] != GroupNotMatched {
			groupIdx = int(groups[i] - 1)

			for colIdx := range vectors {
				if err = exec.fill(groupIdx, colIdx, rowIdx, 1); err != nil {
					return err
				}
			}
		}
		i++
	}

	return nil
}

func (exec *multiAggFuncExec1[T]) SetPreparedResult(_ any, _ int) {
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

func (exec *multiAggFuncExec2) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	for i := range vectors {
		exec.args[i].Prepare(vectors[i])
		if err := exec.fill(groupIndex, i, row, 1); err != nil {
			return err
		}
	}
	return nil
}

func (exec *multiAggFuncExec2) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for i := range vectors {
		exec.args[i].Prepare(vectors[i])
		if err := exec.fill(groupIndex, i, 0, vectors[i].Length()); err != nil {
			return err
		}
	}
	return nil
}

func (exec *multiAggFuncExec2) BatchFill(offset int, groups []uint64, vector []*vector.Vector) error {
	for i := range vector {
		exec.args[i].Prepare(vector[i])
	}

	var groupIdx int
	var err error
	var i = 0
	for rowIdx, end := offset, offset+len(groups); rowIdx < end; rowIdx++ {
		if groups[i] != GroupNotMatched {
			groupIdx = int(groups[i] - 1)

			for colIdx := range vector {
				if err = exec.fill(groupIdx, colIdx, rowIdx, 1); err != nil {
					return err
				}
			}
		}
		i++
	}

	return nil
}

func (exec *multiAggFuncExec2) SetPreparedResult(_ any, _ int) {
	panic("unimplemented SetPreparedResult for multiAggFuncExec2")
}

func (exec *multiAggFuncExec2) Flush() (*vector.Vector, error) {
	var err error
	for i, group := range exec.groups {
		if err = exec.ret.set(i, group.flush()); err != nil {
			return nil, err
		}
	}
	return exec.ret.flush(), nil
}

func (exec *multiAggFuncExec2) Free() {
	exec.ret.free()
}

// should prepare the aggArg before calling this function.
// we will get values from the aggArg directly.
func (exec *multiAggFuncExec1[T]) fill(groupIndex int, columnIdx int, offset int, length int) error {
	if !exec.args[columnIdx].Cached() {
		exec.args[columnIdx].CacheFill(
			exec.groups[groupIndex].getFillWhich(columnIdx),
			exec.groups[groupIndex].getFillNullWhich(columnIdx),
			exec.groups[groupIndex].getFillsWhich(columnIdx),
		)
	}

	f, ok := multiFill[exec.argTypes[columnIdx].Oid]
	if !ok {
		return moerr.NewInternalErrorNoCtx("unsupported argument type %s for multi agg", exec.argTypes[columnIdx].String())
	}
	return f(exec.args[columnIdx], offset, length)
}

func (exec *multiAggFuncExec2) fill(groupIndex int, columnIdx int, offset int, length int) error {
	if !exec.args[columnIdx].Cached() {
		exec.args[columnIdx].CacheFill(
			exec.groups[groupIndex].getFillWhich(columnIdx),
			exec.groups[groupIndex].getFillNullWhich(columnIdx),
			exec.groups[groupIndex].getFillsWhich(columnIdx),
		)
	}

	f, ok := multiFill[exec.argTypes[columnIdx].Oid]
	if !ok {
		return moerr.NewInternalErrorNoCtx("unsupported argument type %s for multi agg", exec.argTypes[columnIdx].String())
	}

	return f(exec.args[columnIdx], offset, length)
}

func ff1[T types.FixedSizeTExceptStrType](
	source aggArg, offset int, length int) error {

	// todo: if we set the _fill, _fillNull as the field of the aggArg, we can avoid the type assertion.
	_arg := source.(*aggFuncArg[T])
	_fill := _arg.fill
	_fillNull := _arg.fillNull
	//_fills := fills.(func(T, bool, int))

	// todo: check the const here and we can call the fills.
	// if source.w.IsConst() {

	if _arg.w.WithAnyNullValue() {
		for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
			v, null := _arg.w.GetValue(i)
			if null {
				_fillNull()
			} else {
				_fill(v)
			}
		}
		return nil
	}

	for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
		v, _ := _arg.w.GetValue(i)
		_fill(v)
	}

	return nil
}

func ff2(
	source aggArg, offset int, length int) error {

	_arg := source.(*aggFuncBytesArg)
	_fill := _arg.fill
	_fillNull := _arg.fillNull
	//_fills := fills.(func([]byte, bool, int))

	if _arg.w.WithAnyNullValue() {
		for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
			v, null := _arg.w.GetStrValue(i)
			if null {
				_fillNull()
			} else {
				_fill(v)
			}
		}
		return nil
	}

	for i, j := uint64(offset), uint64(offset+length); i < j; i++ {
		v, _ := _arg.w.GetStrValue(i)
		_fill(v)
	}

	return nil
}

var multiFill = map[types.T]func(arg aggArg, offset int, length int) error{
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
