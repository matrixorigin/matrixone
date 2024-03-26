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

func RegisterAvg(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		AvgReturnType,
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggAvgDecimal64
			case types.T_decimal128:
				return newAggAvgDecimal128
			default:
				panic("unexpected type for avg()")
			}
		})
}

var AvgSupportedTypes = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func AvgReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 18, s)
	case types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 18, s)
	default:
		return types.T_float64.ToType()
	}
}

type aggAvg[from numeric] struct {
	count int64
}

func newAggAvg[from numeric]() aggexec.SingleAggFromFixedRetFixed[from, float64] {
	return &aggAvg[from]{}
}

type aggAvgDecimal64 struct {
	count    int64
	argScale int32
}

func newAggAvgDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return &aggAvgDecimal64{}
}

type aggAvgDecimal128 struct {
	count    int64
	argScale int32
}

func newAggAvgDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return &aggAvgDecimal128{}
}

func (a *aggAvg[from]) Marshal() []byte       { return types.EncodeInt64(&a.count) }
func (a *aggAvg[from]) Unmarshal(data []byte) { a.count = types.DecodeInt64(data) }
func (a *aggAvg[from]) Init(set aggexec.AggSetter[float64], arg, ret types.Type) {
	set(0)
	a.count = 0
}
func (a *aggAvg[from]) Fill(value from, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	set(get() + float64(value))
	a.count++
}
func (a *aggAvg[from]) FillNull(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {}
func (a *aggAvg[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if isNull {
		return
	}
	set(get() + float64(value)*float64(count))
	a.count += int64(count)
}
func (a *aggAvg[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, float64], get1, get2 aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	next := other.(*aggAvg[from])
	set(get1() + get2())
	a.count += next.count
}
func (a *aggAvg[from]) Flush(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if a.count != 0 {
		set(get() / float64(a.count))
	}
}

func (a *aggAvgDecimal64) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeInt32(&a.argScale)...)
	return bs
}
func (a *aggAvgDecimal64) Unmarshal(data []byte) {
	a.count = types.DecodeInt64(data[:8])
	a.argScale = types.DecodeInt32(data[8:])
}
func (a *aggAvgDecimal64) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.count = 0
	a.argScale = arg.Scale
}
func (a *aggAvgDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, _ := get().Add64(value)
	set(r)
	a.count++
}
func (a *aggAvgDecimal64) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}
func (a *aggAvgDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if isNull {
		return
	}
	v := types.Decimal128{B0_63: uint64(value), B64_127: 0}
	if value.Sign() {
		v.B64_127 = ^v.B64_127
	}
	r, _, _ := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
	r, _ = get().Add128(r)
	set(r)
	a.count += int64(count)
}
func (a *aggAvgDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	next := other.(*aggAvgDecimal64)
	r, _ := get1().Add128(get2())
	set(r)
	a.count += next.count
}
func (a *aggAvgDecimal64) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if a.count != 0 {
		v, _, _ := get().Div(types.Decimal128{B0_63: uint64(a.count), B64_127: 0}, a.argScale, 0)
		set(v)
	}
}

func (a *aggAvgDecimal128) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeInt32(&a.argScale)...)
	return bs
}
func (a *aggAvgDecimal128) Unmarshal(data []byte) {
	a.count = types.DecodeInt64(data[:8])
	a.argScale = types.DecodeInt32(data[8:])
}
func (a *aggAvgDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.count = 0
	a.argScale = arg.Scale
}
func (a *aggAvgDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, _ := get().Add128(value)
	set(r)
	a.count++
}
func (a *aggAvgDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}
func (a *aggAvgDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if isNull {
		return
	}
	r, _, _ := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
	r, _ = get().Add128(r)
	set(r)
	a.count += int64(count)
}
func (a *aggAvgDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	next := other.(*aggAvgDecimal128)
	r, _ := get1().Add128(get2())
	set(r)
	a.count += next.count
}
func (a *aggAvgDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if a.count != 0 {
		v, _, _ := get().Div(types.Decimal128{B0_63: uint64(a.count), B64_127: 0}, a.argScale, 0)
		set(v)
	}
}
