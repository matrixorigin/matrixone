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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterAnyValue(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bool.ToType(), types.T_bool.ToType(), false, true), newAggAnyValue[bool])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_bit.ToType(), false, true), newAggAnyValue[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint8.ToType(), false, true), newAggAnyValue[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint16.ToType(), false, true), newAggAnyValue[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint32.ToType(), false, true), newAggAnyValue[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggAnyValue[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int8.ToType(), false, true), newAggAnyValue[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int16.ToType(), false, true), newAggAnyValue[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int32.ToType(), false, true), newAggAnyValue[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggAnyValue[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float32.ToType(), false, true), newAggAnyValue[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggAnyValue[float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_date.ToType(), types.T_date.ToType(), false, true), newAggAnyValue[types.Date])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_datetime.ToType(), types.T_datetime.ToType(), false, true), newAggAnyValue[types.Datetime])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_timestamp.ToType(), types.T_timestamp.ToType(), false, true), newAggAnyValue[types.Timestamp])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_time.ToType(), types.T_time.ToType(), false, true), newAggAnyValue[types.Time])
	aggexec.RegisterFlexibleSingleAgg(aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			switch t[0].Oid {
			case types.T_decimal64, types.T_decimal128:
				return t[0]
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return t[0]
			case types.T_Rowid, types.T_enum:
				return t[0]
			default:
				panic("unexpected type for any_value()")
			}
		},
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggAnyValue[types.Decimal64]
			case types.T_decimal128:
				return newAggAnyValue[types.Decimal128]
			case types.T_Rowid:
				return newAggAnyValue[types.Rowid]
			case types.T_enum:
				return newAggAnyValue[types.Enum]
			case types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
				return newAggAnyBytesValue
			default:
				panic("unexpected type for any_value()")
			}
		})
}

func newAggAnyValue[T types.FixedSizeTExceptStrType]() aggexec.SingleAggFromFixedRetFixed[T, T] {
	return &aggAnyValue[T]{}
}

type aggAnyValue[T types.FixedSizeTExceptStrType] struct {
	has bool
}

func (a *aggAnyValue[T]) Marshal() []byte       { return types.EncodeBool(&a.has) }
func (a *aggAnyValue[T]) Unmarshal(data []byte) { a.has = types.DecodeBool(data) }
func (a *aggAnyValue[T]) Init(setter aggexec.AggSetter[T], arg, ret types.Type) {
	a.has = false
}
func (a *aggAnyValue[T]) Fill(value T, get aggexec.AggGetter[T], set aggexec.AggSetter[T]) {
	if !a.has {
		a.has = true
		set(value)
	}
}
func (a *aggAnyValue[T]) FillNull(get aggexec.AggGetter[T], set aggexec.AggSetter[T]) {
}
func (a *aggAnyValue[T]) Fills(value T, isNull bool, count int, get aggexec.AggGetter[T], set aggexec.AggSetter[T]) {
	if !isNull && !a.has {
		a.has = true
		set(value)
	}
}
func (a *aggAnyValue[T]) Merge(other aggexec.SingleAggFromFixedRetFixed[T, T], get1, get2 aggexec.AggGetter[T], set aggexec.AggSetter[T]) {
	next := other.(*aggAnyValue[T])
	if !a.has && next.has {
		a.has = true
		set(get2())
	}
}
func (a *aggAnyValue[T]) Flush(get aggexec.AggGetter[T], set aggexec.AggSetter[T]) {}

type aggAnyBytesValue struct {
	has bool
}

func newAggAnyBytesValue() aggexec.SingleAggFromVarRetVar {
	return &aggAnyBytesValue{}
}

func (a *aggAnyBytesValue) Marshal() []byte { return types.EncodeBool(&a.has) }
func (a *aggAnyBytesValue) Unmarshal(data []byte) {
	a.has = types.DecodeBool(data)
}
func (a *aggAnyBytesValue) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) {
	a.has = false
}
func (a *aggAnyBytesValue) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if !a.has {
		a.has = true
		_ = set(value)
	}
}
func (a *aggAnyBytesValue) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
}
func (a *aggAnyBytesValue) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if !isNull && !a.has {
		a.has = true
		_ = set(value)
	}
}
func (a *aggAnyBytesValue) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	next := other.(*aggAnyBytesValue)
	if !a.has && next.has {
		a.has = true
		_ = set(get2())
	}
}
func (a *aggAnyBytesValue) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
