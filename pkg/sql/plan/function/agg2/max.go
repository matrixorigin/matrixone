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
	"math"
)

func RegisterMax(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int8.ToType(), false, true), newAggMax[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int16.ToType(), false, true), newAggMax[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int32.ToType(), false, true), newAggMax[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggMax[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint8.ToType(), false, true), newAggMax[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint16.ToType(), false, true), newAggMax[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint32.ToType(), false, true), newAggMax[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggMax[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float32.ToType(), false, true), newAggMax[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggMax[float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_date.ToType(), types.T_date.ToType(), false, true), newAggMax[types.Date])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_datetime.ToType(), types.T_datetime.ToType(), false, true), newAggMax[types.Datetime])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_timestamp.ToType(), types.T_timestamp.ToType(), false, true), newAggMax[types.Timestamp])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			switch t[0].Oid {
			case types.T_decimal64, types.T_decimal128:
				return t[0]
			default:
				panic("unexpect type for max()")
			}
		},
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggMaxDecimal64
			case types.T_decimal128:
				return newAggMaxDecimal128
			default:
				panic("unexpect type for max()")
			}
		})
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

func (a aggMax[from]) Marshal() []byte  { return nil }
func (a aggMax[from]) Unmarshal([]byte) {}
func (a aggMax[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) {
	set(getMinValue[from]().(from))
}
func (a aggMax[from]) Fill(value from, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if value > get() {
		set(value)
	}
}
func (a aggMax[from]) FillNull(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}
func (a aggMax[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if !isNull && value > get() {
		set(value)
	}
}
func (a aggMax[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, from], get1, get2 aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if get1() > get2() {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMax[from]) Flush(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}

func getMinValue[T canCompare]() interface{} {
	var t T
	var tt interface{} = &t
	switch tt.(type) {
	case *uint8, *uint16, *uint32, *uint64,
		*types.Date, *types.Datetime, *types.Timestamp:
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
func (a aggMaxDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) {
	set(types.Decimal64(0))
}
func (a aggMaxDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if value.Compare(get()) > 0 {
		set(value)
	}
}
func (a aggMaxDecimal64) FillNull(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
}
func (a aggMaxDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if !isNull && value.Compare(get()) > 0 {
		set(value)
	}
}
func (a aggMaxDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64], get1, get2 aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
	if get1().Compare(get2()) > 0 {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMaxDecimal64) Flush(get aggexec.AggGetter[types.Decimal64], set aggexec.AggSetter[types.Decimal64]) {
}

func (a aggMaxDecimal128) Marshal() []byte  { return nil }
func (a aggMaxDecimal128) Unmarshal([]byte) {}
func (a aggMaxDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
}
func (a aggMaxDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if value.Compare(get()) > 0 {
		set(value)
	}
}
func (a aggMaxDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {

}
func (a aggMaxDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if !isNull && value.Compare(get()) > 0 {
		set(value)
	}
}
func (a aggMaxDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if get1().Compare(get2()) > 0 {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMaxDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}
