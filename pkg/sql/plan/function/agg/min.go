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

package agg

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterMin(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint8.ToType(), false, true), newAggMin[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint16.ToType(), false, true), newAggMin[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint32.ToType(), false, true), newAggMin[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggMin[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int8.ToType(), false, true), newAggMin[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int16.ToType(), false, true), newAggMin[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int32.ToType(), false, true), newAggMin[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggMin[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float32.ToType(), false, true), newAggMin[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggMin[float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_date.ToType(), types.T_date.ToType(), false, true), newAggMin[types.Date])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_datetime.ToType(), types.T_datetime.ToType(), false, true), newAggMin[types.Datetime])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_timestamp.ToType(), types.T_timestamp.ToType(), false, true), newAggMin[types.Timestamp])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_time.ToType(), types.T_time.ToType(), false, true), newAggMin[types.Time])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bool.ToType(), types.T_bool.ToType(), false, true), newAggMinBool)
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_bit.ToType(), false, true), newAggMin[uint64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		MinReturnType,
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggMinDecimal64
			case types.T_decimal128:
				return newAggMinDecimal128
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return newAggBytesMin
			case types.T_uuid:
				return newAggUuidMin
			default:
				panic("unexpect type for min()")
			}
		})
}

var MinSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
}

func MinReturnType(typs []types.Type) types.Type {
	return typs[0]
}

type aggMin[from canCompare] struct{}

func newAggMin[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMin[from]{}
}

type aggMinDecimal64 struct{}

func newAggMinDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64] {
	return aggMinDecimal64{}
}

type aggMinDecimal128 struct{}

func newAggMinDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggMinDecimal128{}
}

type aggBytesMin struct {
	isEmpty bool
}

func newAggBytesMin() aggexec.SingleAggFromVarRetVar {
	return &aggBytesMin{}
}

type aggUuidMin struct {
	isEmpty bool
}

func newAggUuidMin() aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid] {
	return &aggUuidMin{}
}

type aggMinBool struct{}

func newAggMinBool() aggexec.SingleAggFromFixedRetFixed[bool, bool] {
	return aggMinBool{}
}

func (a aggMin[from]) Marshal() []byte  { return nil }
func (a aggMin[from]) Unmarshal([]byte) {}
func (a aggMin[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) error {
	set(getMaxValue[from]().(from))
	return nil
}
func (a aggMin[from]) Fill(value from, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if value < get() {
		set(value)
	}
	return nil
}
func (a aggMin[from]) FillNull(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	return nil
}
func (a aggMin[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if !isNull && value < get() {
		set(value)
	}
	return nil
}
func (a aggMin[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, from], get1, get2 aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if get1() < get2() {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMin[from]) Flush(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	return nil
}

func getMaxValue[T canCompare]() interface{} {
	var t T
	var tt interface{} = &t
	switch tt.(type) {
	case *uint8:
		return uint8(math.MaxUint8)
	case *uint16:
		return uint16(math.MaxUint16)
	case *uint32:
		return uint32(math.MaxUint32)
	case *uint64:
		return uint64(math.MaxUint64)
	case *int8:
		return int8(math.MaxInt8)
	case *int16:
		return int16(math.MaxInt16)
	case *int32:
		return int32(math.MaxInt32)
	case *int64:
		return int64(math.MaxInt64)
	case *float32:
		return float32(math.MaxFloat32)
	case *float64:
		return math.MaxFloat64
	case *types.Date:
		return types.Date(math.MaxInt32)
	case *types.Datetime:
		return types.Datetime(math.MaxInt64)
	case *types.Timestamp:
		return types.Timestamp(math.MaxInt64)
	case *types.Time:
		return types.Time(math.MaxInt64)
	}
	panic("unexpected type")
}

func (a aggMinDecimal64) Marshal() []byte  { return nil }
func (a aggMinDecimal64) Unmarshal([]byte) {}
func (a aggMinDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) error {
	set(types.Decimal64Max)
	return nil
}
func (a aggMinDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(get()) < 0 {
		set(value)
	}
	return nil
}
func (a aggMinDecimal64) FillNull(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	return nil
}
func (a aggMinDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if !isNull && value.Compare(get()) < 0 {
		set(value)
	}
	return nil
}
func (a aggMinDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64], get1, get2 aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if get1().Compare(get2()) < 0 {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMinDecimal64) Flush(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	return nil
}

func (a aggMinDecimal128) Marshal() []byte  { return nil }
func (a aggMinDecimal128) Unmarshal([]byte) {}
func (a aggMinDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128Max)
	return nil
}
func (a aggMinDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(get()) < 0 {
		set(value)
	}
	return nil
}
func (a aggMinDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}
func (a aggMinDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if !isNull && value.Compare(get()) < 0 {
		set(value)
	}
	return nil
}
func (a aggMinDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if get1().Compare(get2()) < 0 {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMinDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}

func (a *aggBytesMin) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggBytesMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBytesMin) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}
func (a *aggBytesMin) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	if a.isEmpty {
		a.isEmpty = false
		return set(value)
	}
	if bytes.Compare(value, get()) < 0 {
		return set(value)
	}
	return nil
}
func (a *aggBytesMin) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}
func (a *aggBytesMin) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	if isNull {
		return nil
	}
	if a.isEmpty {
		a.isEmpty = false
		return set(value)
	}

	if !isNull && bytes.Compare(value, get()) < 0 {
		return set(value)
	}
	return nil
}
func (a *aggBytesMin) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	next := other.(*aggBytesMin)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		return set(get2())
	}
	if !a.isEmpty && !next.isEmpty {
		if bytes.Compare(get1(), get2()) > 0 {
			return set(get2())
		}
	}
	return nil
}
func (a *aggBytesMin) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}

func (a *aggUuidMin) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggUuidMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggUuidMin) Init(setter aggexec.AggSetter[types.Uuid], arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}
func (a *aggUuidMin) Fill(value types.Uuid, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if value.Compare(get()) < 0 {
			set(value)
		}
	}
	return nil
}
func (a *aggUuidMin) FillNull(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	return nil
}
func (a *aggUuidMin) Fills(value types.Uuid, isNull bool, count int, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	if isNull {
		return nil
	}
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if !isNull && value.Compare(get()) < 0 {
			set(value)
		}
	}
	return nil
}
func (a *aggUuidMin) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid], get1, get2 aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	next := other.(*aggUuidMin)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		set(get2())
	} else if !a.isEmpty && !next.isEmpty {
		if get1().Compare(get2()) > 0 {
			set(get2())
		}
	}
	return nil
}
func (a *aggUuidMin) Flush(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	return nil
}

func (a aggMinBool) Marshal() []byte     { return nil }
func (a aggMinBool) Unmarshal(bs []byte) {}
func (a aggMinBool) Init(setter aggexec.AggSetter[bool], arg types.Type, ret types.Type) error {
	setter(true)
	return nil
}
func (a aggMinBool) Fill(value bool, get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if !value {
		set(false)
	}
	return nil
}
func (a aggMinBool) FillNull(get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	return nil
}
func (a aggMinBool) Fills(value bool, isNull bool, count int, get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if isNull {
		return nil
	}
	if !value {
		set(false)
	}
	return nil
}
func (a aggMinBool) Merge(other aggexec.SingleAggFromFixedRetFixed[bool, bool], get1, get2 aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if !get2() {
		set(false)
	}
	return nil
}
func (a aggMinBool) Flush(get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	return nil
}
