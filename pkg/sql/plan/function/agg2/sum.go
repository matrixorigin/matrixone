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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterSum(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_uint64.ToType(), false, true), newAggSum[uint64, uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_int64.ToType(), false, true), newAggSum[int8, int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_int64.ToType(), false, true), newAggSum[int16, int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_int64.ToType(), false, true), newAggSum[int32, int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_int64.ToType(), false, true), newAggSum[int64, int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint64.ToType(), false, true), newAggSum[uint8, uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint64.ToType(), false, true), newAggSum[uint16, uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint64.ToType(), false, true), newAggSum[uint32, uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggSum[uint64, uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float64.ToType(), false, true), newAggSum[float32, float64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggSum[float64, float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		SumReturnType,
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggSumDecimal64
			case types.T_decimal128:
				return newAggSumDecimal128
			default:
				panic("unexpect type for sum()")
			}
		})
}

var (
	SumSupportedTypes = []types.T{
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	SumReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_float32, types.T_float64:
			return types.T_float64.ToType()
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.T_int64.ToType()
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.T_uint64.ToType()
		case types.T_bit:
			return types.T_uint64.ToType()
		case types.T_decimal64:
			return types.New(types.T_decimal128, 38, typs[0].Scale)
		case types.T_decimal128:
			return types.New(types.T_decimal128, 38, typs[0].Scale)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for sum", typs[0]))
	}
)

var _ aggexec.SingleAggFromFixedRetFixed[int32, int64] = aggSum[int32, int64]{}
var _ aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] = aggSumDecimal64{}

type aggSum[from numeric, to numericWithMaxScale] struct{}

func newAggSum[from numeric, to numericWithMaxScale]() aggexec.SingleAggFromFixedRetFixed[from, to] {
	return aggSum[from, to]{}
}

type aggSumDecimal64 struct {
	argScale int32
}

func newAggSumDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return aggSumDecimal64{}
}

type aggSumDecimal128 struct {
	argScale int32
}

func newAggSumDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggSumDecimal128{}
}

func (a aggSum[from, to]) Marshal() []byte        { return nil }
func (a aggSum[from, to]) Unmarshal(bytes []byte) {}
func (a aggSum[from, to]) Init(aggexec.AggSetter[to], types.Type, types.Type) error {
	return nil
}
func (a aggSum[from, to]) Fill(value from, get aggexec.AggGetter[to], set aggexec.AggSetter[to]) error {
	set(get() + to(value))
	return nil
}
func (a aggSum[from, to]) FillNull(get aggexec.AggGetter[to], set aggexec.AggSetter[to]) error {
	return nil
}
func (a aggSum[from, to]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[to], set aggexec.AggSetter[to]) error {
	if !isNull {
		set(get() + to(value)*to(count))
	}
	return nil
}
func (a aggSum[from, to]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, to], getter1, getter2 aggexec.AggGetter[to], set aggexec.AggSetter[to]) error {
	set(getter1() + getter2())
	return nil
}
func (a aggSum[from, to]) Flush(get aggexec.AggGetter[to], setter aggexec.AggSetter[to]) error {
	return nil
}

func (a aggSumDecimal64) Marshal() []byte     { return types.EncodeInt32(&a.argScale) }
func (a aggSumDecimal64) Unmarshal(bs []byte) { a.argScale = types.DecodeInt32(bs) }
func (a aggSumDecimal64) Init(set aggexec.AggSetter[types.Decimal128], arg types.Type, ret types.Type) error {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.argScale = arg.Scale
	return nil
}
func (a aggSumDecimal64) Fill(from types.Decimal64, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	r, err := get().Add64(from)
	if err != nil {
		return err
	}
	set(r)
	return nil
}
func (a aggSumDecimal64) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}
func (a aggSumDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		v := types.Decimal128{B0_63: uint64(value), B64_127: 0}
		if value.Sign() {
			v.B64_127 = ^v.B64_127
		}
		r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		r, err = get().Add128(r)
		if err != nil {
			return err
		}
		set(r)
	}
	return nil
}
func (a aggSumDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := get1().Add128(get2())
	if err != nil {
		return err
	}
	setter(r)
	return nil
}
func (a aggSumDecimal64) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}

func (a aggSumDecimal128) Marshal() []byte     { return types.EncodeInt32(&a.argScale) }
func (a aggSumDecimal128) Unmarshal(bs []byte) { a.argScale = types.DecodeInt32(bs) }
func (a aggSumDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg types.Type, ret types.Type) error {
	a.argScale = arg.Scale
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	return nil
}
func (a aggSumDecimal128) Fill(from types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	r, err := get().Add128(from)
	if err != nil {
		return err
	}
	set(r)
	return nil
}
func (a aggSumDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}
func (a aggSumDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		r, err = get().Add128(r)
		if err != nil {
			return err
		}
		set(r)
	}
	return nil
}
func (a aggSumDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := get1().Add128(get2())
	if err != nil {
		return err
	}
	setter(r)
	return nil
}
func (a aggSumDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) error {
	return nil
}
