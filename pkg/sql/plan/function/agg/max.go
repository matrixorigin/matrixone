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

func RegisterMax(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint8.ToType(), false, true), newAggMax[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint16.ToType(), false, true), newAggMax[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint32.ToType(), false, true), newAggMax[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggMax[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int8.ToType(), false, true), newAggMax[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int16.ToType(), false, true), newAggMax[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int32.ToType(), false, true), newAggMax[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggMax[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float32.ToType(), false, true), newAggMax[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggMax[float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_date.ToType(), types.T_date.ToType(), false, true), newAggMax[types.Date])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_datetime.ToType(), types.T_datetime.ToType(), false, true), newAggMax[types.Datetime])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_timestamp.ToType(), types.T_timestamp.ToType(), false, true), newAggMax[types.Timestamp])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_time.ToType(), types.T_time.ToType(), false, true), newAggMax[types.Time])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bool.ToType(), types.T_bool.ToType(), false, true), newAggMaxBool)
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_bit.ToType(), false, true), newAggMax[uint64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		MaxReturnType,
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggMaxDecimal64
			case types.T_decimal128:
				return newAggMaxDecimal128
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return newAggBytesMax
			case types.T_uuid:
				return newAggUuidMax
			default:
				panic("unexpect type for max()")
			}
		})
}

var MaxSupportedTypes = []types.T{
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

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

type aggMax[from canCompare] struct{}

func newAggMax[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMax[from]{}
}

type aggMaxDecimal64 struct{}

func newAggMaxDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64] {
	return aggMaxDecimal64{}
}

type aggMaxDecimal128 struct{}

func newAggMaxDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggMaxDecimal128{}
}

type aggBytesMax struct {
	isEmpty bool
}

func newAggBytesMax() aggexec.SingleAggFromVarRetVar {
	return &aggBytesMax{}
}

type aggUuidMax struct {
	isEmpty bool
}

func newAggUuidMax() aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid] {
	return &aggUuidMax{}
}

type aggMaxBool struct{}

func newAggMaxBool() aggexec.SingleAggFromFixedRetFixed[bool, bool] {
	return aggMaxBool{}
}

func (a aggMax[from]) Marshal() []byte  { return nil }
func (a aggMax[from]) Unmarshal([]byte) {}
func (a aggMax[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) error {
	set(getMinValue[from]().(from))
	return nil
}
func (a aggMax[from]) Fill(value from, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if value > get() {
		set(value)
	}
	return nil
}
func (a aggMax[from]) FillNull(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	return nil
}
func (a aggMax[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if !isNull && value > get() {
		set(value)
	}
	return nil
}
func (a aggMax[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, from], get1, get2 aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	if get1() > get2() {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMax[from]) Flush(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) error {
	return nil
}

func getMinValue[T canCompare]() interface{} {
	var t T
	var tt interface{} = &t
	switch tt.(type) {
	case *uint8, *uint16, *uint32, *uint64,
		*types.Date, *types.Datetime, *types.Timestamp, *types.Time:
		return T(0)
	case *int8:
		return int8(math.MinInt8)
	case *int16:
		return int16(math.MinInt16)
	case *int32:
		return int32(math.MinInt32)
	case *int64:
		return int64(math.MinInt64)
	case *float32:
		return float32(-math.MaxFloat32)
	case *float64:
		return -math.MaxFloat64
	}
	panic("unexpected type")
}

func (a aggMaxDecimal64) Marshal() []byte  { return nil }
func (a aggMaxDecimal64) Unmarshal([]byte) {}
func (a aggMaxDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) error {
	set(types.Decimal64Min)
	return nil
}
func (a aggMaxDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(get()) > 0 {
		set(value)
	}
	return nil
}
func (a aggMaxDecimal64) FillNull(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	return nil
}
func (a aggMaxDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if !isNull && value.Compare(get()) > 0 {
		set(value)
	}
	return nil
}
func (a aggMaxDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64], get1, get2 aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	if get1().Compare(get2()) > 0 {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMaxDecimal64) Flush(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) error {
	return nil
}

func (a aggMaxDecimal128) Marshal() []byte  { return nil }
func (a aggMaxDecimal128) Unmarshal([]byte) {}
func (a aggMaxDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128Min)
	return nil
}
func (a aggMaxDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(get()) > 0 {
		set(value)
	}
	return nil
}
func (a aggMaxDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}
func (a aggMaxDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if !isNull && value.Compare(get()) > 0 {
		set(value)
	}
	return nil
}
func (a aggMaxDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if get1().Compare(get2()) > 0 {
		set(get1())
	} else {
		set(get2())
	}
	return nil
}
func (a aggMaxDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}

func (a *aggBytesMax) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggBytesMax) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBytesMax) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}
func (a *aggBytesMax) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	if a.isEmpty {
		a.isEmpty = false
		return set(value)
	}
	if bytes.Compare(value, get()) > 0 {
		return set(value)
	}
	return nil
}
func (a *aggBytesMax) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}
func (a *aggBytesMax) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	if isNull {
		return nil
	}
	if a.isEmpty {
		a.isEmpty = false
		return set(value)
	}
	if !isNull && bytes.Compare(value, get()) > 0 {
		return set(value)
	}
	return nil
}
func (a *aggBytesMax) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	next := other.(*aggBytesMax)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		return set(get2())
	} else if !a.isEmpty && !next.isEmpty {
		if bytes.Compare(get1(), get2()) < 0 {
			return set(get2())
		}
	}
	return nil
}
func (a *aggBytesMax) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}

func (a *aggUuidMax) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggUuidMax) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggUuidMax) Init(setter aggexec.AggSetter[types.Uuid], arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}
func (a *aggUuidMax) Fill(value types.Uuid, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if value.Compare(get()) > 0 {
			set(value)
		}
	}
	return nil
}
func (a *aggUuidMax) FillNull(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	return nil
}
func (a *aggUuidMax) Fills(value types.Uuid, isNull bool, count int, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	if isNull {
		return nil
	}
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if !isNull && value.Compare(get()) > 0 {
			set(value)
		}
	}
	return nil
}
func (a *aggUuidMax) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid], get1, get2 aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	next := other.(*aggUuidMax)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		set(get2())
	} else if !a.isEmpty && !next.isEmpty {
		if get1().Compare(get2()) < 0 {
			set(get2())
		}
	}
	return nil
}
func (a *aggUuidMax) Flush(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	return nil
}

func (a aggMaxBool) Marshal() []byte     { return nil }
func (a aggMaxBool) Unmarshal(bs []byte) {}
func (a aggMaxBool) Init(setter aggexec.AggSetter[bool], arg types.Type, ret types.Type) error {
	setter(false)
	return nil
}
func (a aggMaxBool) Fill(value bool, get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if value {
		set(true)
	}
	return nil
}
func (a aggMaxBool) FillNull(get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	return nil
}
func (a aggMaxBool) Fills(value bool, isNull bool, count int, get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if isNull {
		return nil
	}
	if value {
		set(true)
	}
	return nil
}
func (a aggMaxBool) Merge(other aggexec.SingleAggFromFixedRetFixed[bool, bool], get1, get2 aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	if get2() {
		set(true)
	}
	return nil
}
func (a aggMaxBool) Flush(get aggexec.AggGetter[bool], set aggexec.AggSetter[bool]) error {
	return nil
}
