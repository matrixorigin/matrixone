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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterSum1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[uint64, uint64],
			FillAggSum1[uint64, uint64], nil, FillsAggSum1[uint64, uint64],
			MergeAggSum1[uint64, uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[int8, int64],
			FillAggSum1[int8, int64], nil, FillsAggSum1[int8, int64],
			MergeAggSum1[int8, int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[int16, int64],
			FillAggSum1[int16, int64], nil, FillsAggSum1[int16, int64],
			MergeAggSum1[int16, int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[int32, int64],
			FillAggSum1[int32, int64], nil, FillsAggSum1[int32, int64],
			MergeAggSum1[int32, int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[int64, int64],
			FillAggSum1[int64, int64], nil, FillsAggSum1[int64, int64],
			MergeAggSum1[int64, int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[uint8, uint64],
			FillAggSum1[uint8, uint64], nil, FillsAggSum1[uint8, uint64],
			MergeAggSum1[uint8, uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[uint16, uint64],
			FillAggSum1[uint16, uint64], nil, FillsAggSum1[uint16, uint64],
			MergeAggSum1[uint16, uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[uint32, uint64],
			FillAggSum1[uint32, uint64], nil, FillsAggSum1[uint32, uint64],
			MergeAggSum1[uint32, uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[uint64, uint64],
			FillAggSum1[uint64, uint64], nil, FillsAggSum1[uint64, uint64],
			MergeAggSum1[uint64, uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[float32, float64],
			FillAggSum1[float32, float64], nil, FillsAggSum1[float32, float64],
			MergeAggSum1[float32, float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), SumReturnType, false, true),
			aggexec.GenerateEmptyContextFromFixedToFixed[float64, float64],
			FillAggSum1[float64, float64], nil, FillsAggSum1[float64, float64],
			MergeAggSum1[float64, float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), SumReturnType, false, true),
			newAggSumDecimal64,
			FillAggSumDecimal64, nil, FillsAggSumDecimal64,
			MergeAggSumDecimal64,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), SumReturnType, false, true),
			newAggSumDecimal128,
			FillAggSumDecimal128, nil, FillsAggSumDecimal128,
			MergeAggSumDecimal128,
			nil,
		))
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

func FillAggSum1[from numeric, to numericWithMaxScale](
	exec aggexec.SingleAggFromFixedRetFixed[from, to], value from, getter aggexec.AggGetter[to], setter aggexec.AggSetter[to]) error {
	setter(getter() + to(value))
	return nil
}
func FillsAggSum1[from numeric, to numericWithMaxScale](
	exec aggexec.SingleAggFromFixedRetFixed[from, to],
	value from, isNull bool, count int, getter aggexec.AggGetter[to], setter aggexec.AggSetter[to]) error {
	if !isNull {
		setter(getter() + to(value)*to(count))
	}
	return nil
}
func MergeAggSum1[from numeric, to numericWithMaxScale](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, to], getter1, getter2 aggexec.AggGetter[to], setter aggexec.AggSetter[to]) error {
	setter(getter1() + getter2())
	return nil
}

type aggSumDecimal64 struct {
	argScale int32
}

func newAggSumDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return &aggSumDecimal64{}
}

func (a *aggSumDecimal64) Marshal() []byte     { return types.EncodeInt32(&a.argScale) }
func (a *aggSumDecimal64) Unmarshal(bs []byte) { a.argScale = types.DecodeInt32(bs) }
func (a *aggSumDecimal64) Init(set aggexec.AggSetter[types.Decimal128], arg types.Type, ret types.Type) error {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.argScale = arg.Scale
	return nil
}

func FillAggSumDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], value types.Decimal64, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := getter().Add64(value)
	setter(r)
	return err
}

func FillsAggSumDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	value types.Decimal64, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		a := exec.(*aggSumDecimal64)

		v := types.Decimal128{B0_63: uint64(value), B64_127: 0}
		if value.Sign() {
			v.B64_127 = ^v.B64_127
		}
		r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		r, err = getter().Add128(r)
		setter(r)
		return err
	}
	return nil
}
func MergeAggSumDecimal64(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], getter1, getter2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := getter1().Add128(getter2())
	setter(r)
	return err
}

type aggSumDecimal128 struct {
	argScale int32
}

func newAggSumDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return &aggSumDecimal128{}
}

func (a *aggSumDecimal128) Marshal() []byte     { return types.EncodeInt32(&a.argScale) }
func (a *aggSumDecimal128) Unmarshal(bs []byte) { a.argScale = types.DecodeInt32(bs) }
func (a *aggSumDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg types.Type, ret types.Type) error {
	a.argScale = arg.Scale
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	return nil
}

func FillAggSumDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], value types.Decimal128, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := getter().Add128(value)
	setter(r)
	return err
}

func FillsAggSumDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		a := exec.(*aggSumDecimal128)
		r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		r, err = getter().Add128(r)
		setter(r)
		return err
	}
	return nil
}

func MergeAggSumDecimal128(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], getter1, getter2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	r, err := getter1().Add128(getter2())
	setter(r)
	return err
}
