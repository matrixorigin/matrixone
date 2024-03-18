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

package agg2

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterMin(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int8.ToType(), false, true), newAggMin[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int16.ToType(), false, true), newAggMin[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int32.ToType(), false, true), newAggMin[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggMin[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint8.ToType(), false, true), newAggMin[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint16.ToType(), false, true), newAggMin[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint32.ToType(), false, true), newAggMin[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggMin[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float32.ToType(), false, true), newAggMin[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggMin[float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_date.ToType(), types.T_date.ToType(), false, true), newAggMin[types.Date])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_datetime.ToType(), types.T_datetime.ToType(), false, true), newAggMin[types.Datetime])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_timestamp.ToType(), types.T_timestamp.ToType(), false, true), newAggMin[types.Timestamp])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			switch t[0].Oid {
			case types.T_decimal64, types.T_decimal128:
				return t[0]
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return t[0]
			case types.T_uuid:
				return t[0]
			default:
				panic("unexpect type for min()")
			}
		},
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggMinDecimal64
			case types.T_decimal128:
				return newAggMinDecimal128
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return newaggBytesMin
			case types.T_uuid:
				return newaggUuidMin
			default:
				panic("unexpect type for min()")
			}
		})
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


type aggBytesMin struct{
	isEmpty bool
}

func newaggBytesMin() aggexec.SingleAggFromVarRetVar {
	return &aggBytesMin{}
}

type aggUuidMin struct {
	isEmpty bool
}

func newaggUuidMin() aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid] {
	return &aggUuidMin{}
}

func (a aggMin[from]) Marshal() []byte  { return nil }
func (a aggMin[from]) Unmarshal([]byte) {}
func (a aggMin[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) {
	set(getMaxValue[from]().(from))
}
func (a aggMin[from]) Fill(value from, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if value < get() {
		set(value)
	}
}
func (a aggMin[from]) FillNull(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}
func (a aggMin[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if !isNull && value < get() {
		set(value)
	}
}
func (a aggMin[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, from], get1, get2 aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if get1() < get2() {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMin[from]) Flush(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}

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
	}
	panic("unexpected type")
}

func (a aggMinDecimal64) Marshal() []byte  { return nil }
func (a aggMinDecimal64) Unmarshal([]byte) {}
func (a aggMinDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) {
	set(types.Decimal64(math.MaxUint64))
}
func (a aggMinDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if value.Compare(get()) < 0 {
		set(value)
	}
}
func (a aggMinDecimal64) FillNull(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
}
func (a aggMinDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if !isNull && value.Compare(get()) < 0 {
		set(value)
	}
}
func (a aggMinDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64], get1, get2 aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if get1().Compare(get2()) < 0 {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMinDecimal64) Flush(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
}

func (a aggMinDecimal128) Marshal() []byte  { return nil }
func (a aggMinDecimal128) Unmarshal([]byte) {}
func (a aggMinDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) {
	set(types.Decimal128{B0_63: math.MaxUint64, B64_127: math.MaxUint64})
}
func (a aggMinDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if value.Compare(get()) < 0 {
		set(value)
	}
}
func (a aggMinDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}
func (a aggMinDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if !isNull && value.Compare(get()) < 0 {
		set(value)
	}
}
func (a aggMinDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if get1().Compare(get2()) < 0 {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMinDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}

func (a *aggBytesMin) Marshal() []byte  { return types.EncodeBool(&a.isEmpty) }
func (a *aggBytesMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBytesMin) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) {
	a.isEmpty = true
}
func (a *aggBytesMin) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if a.isEmpty {
		a.isEmpty = false
		_ = set(value)
	} else {
		if bytes.Compare(value, get()) < 0 {
			_ = set(value)
		}
	}
}
func (a *aggBytesMin) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
func (a *aggBytesMin) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if isNull {
		return
	}
	if a.isEmpty {
		a.isEmpty = false
		_ = set(value)
	} else {
		if !isNull && bytes.Compare(value, get()) < 0 {
			_ = set(value)
		}
	}
}
func (a *aggBytesMin) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	next := other.(*aggBytesMin)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		_ = set(get2())
	} else if !a.isEmpty && !next.isEmpty {
		if bytes.Compare(get1(), get2()) > 0 {
			_ = set(get2())
		}
	}
}
func (a *aggBytesMin) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}

func (a *aggUuidMin) Marshal() []byte  { return types.EncodeBool(&a.isEmpty) }
func (a *aggUuidMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs)}
func (a *aggUuidMin) Init(setter aggexec.AggSetter[types.Uuid], arg types.Type, ret types.Type) {
	a.isEmpty = true
}
func (a *aggUuidMin) Fill(value types.Uuid, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) {
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if value.Compare(get()) < 0 {
			set(value)
		}
	}
}
func (a *aggUuidMin) FillNull(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) {}
func (a *aggUuidMin) Fills(value types.Uuid, isNull bool, count int, get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) {
	if isNull {
		return
	}
	if a.isEmpty {
		a.isEmpty = false
		set(value)
	} else {
		if !isNull && value.Compare(get()) < 0 {
			set(value)
		}
	}
}
func (a *aggUuidMin) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid], get1, get2 aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) {
	next := other.(*aggUuidMin)
	if a.isEmpty && !next.isEmpty {
		a.isEmpty = false
		set(get2())
	} else if !a.isEmpty && !next.isEmpty {
		if get1().Compare(get2()) > 0 {
			set(get2())
		}
	}
}
func (a *aggUuidMin) Flush(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) {}