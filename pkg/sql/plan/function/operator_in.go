// Copyright 2022 Matrix Origin
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

package function

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"sort"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type TGenericOfIn interface {
	constraints.Integer | constraints.Float | bool | types.Uuid |
		types.Time | types.Timestamp | types.Date | types.Datetime | types.Decimal64 | types.Decimal128 | types.Decimal256 | types.MoYear
}

type opOperatorFixedIn[T TGenericOfIn] struct {
	ready     bool
	hasNull   bool
	mp        map[T]bool
	accounted bool
	scratch   []byte
	count     int
}

type opOperatorStrIn struct {
	ready     bool
	hasNull   bool
	mp        map[string]bool
	accounted bool
	scratch   []byte
	count     int
}

func compareInValues[T TGenericOfIn](left, right T) int {
	switch value := any(left).(type) {
	case uint8:
		return cmp.Compare(value, any(right).(uint8))
	case uint16:
		return cmp.Compare(value, any(right).(uint16))
	case uint32:
		return cmp.Compare(value, any(right).(uint32))
	case uint64:
		return cmp.Compare(value, any(right).(uint64))
	case int8:
		return cmp.Compare(value, any(right).(int8))
	case int16:
		return cmp.Compare(value, any(right).(int16))
	case int32:
		return cmp.Compare(value, any(right).(int32))
	case int64:
		return cmp.Compare(value, any(right).(int64))
	case float32:
		return compareInFloat64(float64(value), float64(any(right).(float32)))
	case float64:
		return compareInFloat64(value, any(right).(float64))
	case bool:
		other := any(right).(bool)
		if value == other {
			return 0
		}
		if !value {
			return -1
		}
		return 1
	case types.Uuid:
		return types.CompareUuid(value, any(right).(types.Uuid))
	case types.Time:
		return cmp.Compare(value, any(right).(types.Time))
	case types.Timestamp:
		return cmp.Compare(value, any(right).(types.Timestamp))
	case types.Date:
		return cmp.Compare(value, any(right).(types.Date))
	case types.Datetime:
		return cmp.Compare(value, any(right).(types.Datetime))
	case types.Decimal64:
		return value.Compare(any(right).(types.Decimal64))
	case types.Decimal128:
		return value.Compare(any(right).(types.Decimal128))
	case types.Decimal256:
		return value.Compare(any(right).(types.Decimal256))
	case types.MoYear:
		return cmp.Compare(value, any(right).(types.MoYear))
	default:
		panic("unsupported IN value type")
	}
}

func compareInFloat64(left, right float64) int {
	leftNaN := math.IsNaN(left)
	rightNaN := math.IsNaN(right)
	switch {
	case leftNaN && rightNaN:
		return cmp.Compare(math.Float64bits(left), math.Float64bits(right))
	case leftNaN:
		return 1
	case rightNaN:
		return -1
	default:
		return cmp.Compare(left, right)
	}
}

func (op *opOperatorFixedIn[T]) initAccounted(
	tuple *vector.Vector,
	result vector.FunctionResultWrapper,
) error {
	op.hasNull = false
	count := 0
	parameter := vector.GenerateFunctionFixedTypeParameter[T](tuple)
	for row := uint64(0); row < uint64(tuple.Length()); row++ {
		_, isNull := parameter.GetValue(row)
		if isNull {
			op.hasNull = true
		} else {
			count++
		}
	}
	var zero T
	elementSize := int(unsafe.Sizeof(zero))
	if count > math.MaxInt/elementSize {
		return mpool.ErrAllocationAccountInvalid
	}
	scratch, selected, err := result.ResizeFunctionScratch(count * elementSize)
	if err != nil {
		return err
	}
	if !selected {
		return mpool.ErrAllocationAccountInvalid
	}
	values := util.UnsafeSliceCast[T](scratch)[:count]
	write := 0
	for row := uint64(0); row < uint64(tuple.Length()); row++ {
		value, isNull := parameter.GetValue(row)
		if !isNull {
			values[write] = value
			write++
		}
	}
	slices.SortFunc(values, compareInValues[T])
	op.accounted = true
	op.scratch = scratch
	op.count = count
	op.ready = true
	return nil
}

func (op *opOperatorFixedIn[T]) containsAccounted(value T) bool {
	values := util.UnsafeSliceCast[T](op.scratch)[:op.count]
	idx, found := slices.BinarySearchFunc(values, value, compareInValues[T])
	return found && values[idx] == value
}

func (op *opOperatorStrIn) initAccounted(
	tuple *vector.Vector,
	result vector.FunctionResultWrapper,
) error {
	op.hasNull = false
	count := 0
	payloadSize := 0
	parameter := vector.GenerateFunctionStrParameter(tuple)
	for row := uint64(0); row < uint64(tuple.Length()); row++ {
		value, isNull := parameter.GetStrValue(row)
		if isNull {
			op.hasNull = true
			continue
		}
		if len(value) > math.MaxInt-payloadSize {
			return mpool.ErrAllocationAccountInvalid
		}
		payloadSize += len(value)
		count++
	}
	if count > math.MaxInt/prefixScratchEntrySize ||
		payloadSize > math.MaxInt-count*prefixScratchEntrySize {
		return mpool.ErrAllocationAccountInvalid
	}
	total := count*prefixScratchEntrySize + payloadSize
	if uint64(total) > math.MaxUint32 {
		return mpool.ErrAllocationAccountInvalid
	}
	scratch, selected, err := result.ResizeFunctionScratch(total)
	if err != nil {
		return err
	}
	if !selected {
		return mpool.ErrAllocationAccountInvalid
	}
	entries := prefixScratchEntries{data: scratch, count: count}
	payloadOffset := count * prefixScratchEntrySize
	write := 0
	for row := uint64(0); row < uint64(tuple.Length()); row++ {
		value, isNull := parameter.GetStrValue(row)
		if isNull {
			continue
		}
		entry := scratch[write*prefixScratchEntrySize:]
		binary.LittleEndian.PutUint32(entry, uint32(payloadOffset))
		binary.LittleEndian.PutUint32(entry[4:], uint32(len(value)))
		payloadOffset += copy(scratch[payloadOffset:], value)
		write++
	}
	sort.Sort(entries)
	op.accounted = true
	op.scratch = scratch
	op.count = count
	op.ready = true
	return nil
}

func (op *opOperatorStrIn) containsAccounted(value []byte) bool {
	entries := prefixScratchEntries{data: op.scratch, count: op.count}
	idx := sort.Search(op.count, func(idx int) bool {
		return bytes.Compare(entries.value(idx), value) >= 0
	})
	return idx < op.count && bytes.Equal(entries.value(idx), value)
}

func newOpOperatorFixedIn[T TGenericOfIn]() *opOperatorFixedIn[T] {
	op := new(opOperatorFixedIn[T])
	op.ready = false
	return op
}

func newOpOperatorStrIn() *opOperatorStrIn {
	op := new(opOperatorStrIn)
	op.ready = false
	return op
}

func (op *opOperatorFixedIn[T]) init(tuple *vector.Vector) {
	op.ready = true
	op.hasNull = false

	if tuple.IsConstNull() {
		op.hasNull = true
		op.mp = make(map[T]bool)
		return
	}
	p := vector.GenerateFunctionFixedTypeParameter[T](tuple)

	if tuple.IsConst() {
		v, null := p.GetValue(0)
		if null {
			op.hasNull = true
			op.mp = make(map[T]bool)
			return
		}
		op.mp = make(map[T]bool, 1)
		op.mp[v] = true
		return
	}

	op.mp = make(map[T]bool, tuple.Length())
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetValue(i)
		if null {
			op.hasNull = true
			continue
		}
		op.mp[v] = true
	}
}

func (op *opOperatorStrIn) init(tuple *vector.Vector) {
	op.ready = true
	op.hasNull = false

	if tuple.IsConstNull() {
		op.hasNull = true
		op.mp = make(map[string]bool)
		return
	}
	p := vector.GenerateFunctionStrParameter(tuple)

	if tuple.IsConst() {
		v, null := p.GetStrValue(0)
		if null {
			op.hasNull = true
			op.mp = make(map[string]bool)
			return
		}
		op.mp = make(map[string]bool, 1)
		op.mp[string(v)] = true
		return
	}

	op.mp = make(map[string]bool)
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetStrValue(i)
		if null {
			op.hasNull = true
			continue
		}
		op.mp[string(v)] = true
	}
}

func (op *opOperatorFixedIn[T]) ensureInitialized(
	tuple *vector.Vector,
	result vector.FunctionResultWrapper,
) error {
	if op.ready {
		return nil
	}
	if result.HasFunctionScratch() {
		return op.initAccounted(tuple, result)
	}
	op.init(tuple)
	return nil
}

func (op *opOperatorStrIn) ensureInitialized(
	tuple *vector.Vector,
	result vector.FunctionResultWrapper,
) error {
	if op.ready {
		return nil
	}
	if result.HasFunctionScratch() {
		return op.initAccounted(tuple, result)
	}
	op.init(tuple)
	return nil
}

func (op *opOperatorFixedIn[T]) contains(value T) bool {
	if op.accounted {
		return op.containsAccounted(value)
	}
	return op.mp[value]
}

func (op *opOperatorStrIn) contains(value []byte) bool {
	if op.accounted {
		return op.containsAccounted(value)
	}
	return op.mp[string(value)]
}

func (op *opOperatorFixedIn[T]) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := op.ensureInitialized(parameters[1], result); err != nil {
		return err
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.contains(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorFixedIn[T]) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := op.ensureInitialized(parameters[1], result); err != nil {
		return err
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.contains(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := op.ensureInitialized(parameters[1], result); err != nil {
		return err
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.contains(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := op.ensureInitialized(parameters[1], result); err != nil {
		return err
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.contains(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}
