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
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterSum2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[uint64],
		aggSumFill[uint64, uint64], aggSumFills[uint64, uint64], aggSumMerge[uint64, uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[int64],
		aggSumFill[int8, int64], aggSumFills[int8, int64], aggSumMerge[int8, int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[int64],
		aggSumFill[int16, int64], aggSumFills[int16, int64], aggSumMerge[int16, int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[int64],
		aggSumFill[int32, int64], aggSumFills[int32, int64], aggSumMerge[int32, int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[int64],
		aggSumInt64Fill, aggSumInt64Fills, aggSumInt64Merge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[uint64],
		aggSumFill[uint8, uint64], aggSumFills[uint8, uint64], aggSumMerge[uint8, uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[uint64],
		aggSumFill[uint16, uint64], aggSumFills[uint16, uint64], aggSumMerge[uint16, uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[uint64],
		aggSumFill[uint32, uint64], aggSumFills[uint32, uint64], aggSumMerge[uint32, uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[uint64],
		aggSumFill[uint64, uint64], aggSumFills[uint64, uint64], aggSumMerge[uint64, uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[float64],
		aggSumFill[float32, float64], aggSumFills[float32, float64], aggSumMerge[float32, float64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), SumReturnType, true),
		nil,
		nil,
		aggSumInitResult[float64],
		aggSumFill[float64, float64], aggSumFills[float64, float64], aggSumMerge[float64, float64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), SumReturnType, true),
		aggSumOfDecimalInitCommonContext,
		nil,
		aggSumOfDecimalInitResult,
		aggSumOfDecimal128Fill, aggSumOfDecimal128Fills, aggSumOfDecimal128Merge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), SumReturnType, true),
		aggSumOfDecimalInitCommonContext,
		nil,
		aggSumOfDecimalInitResult,
		aggSumOfDecimal64Fill, aggSumOfDecimal64Fills, aggSumOfDecimal64Merge, nil)
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
		panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for sum", typs[0]))
	}
)

type aggSumDecimal struct {
	argScale int32
}

func (a *aggSumDecimal) Size() int64 {
	return 4 // int32
}

func (a *aggSumDecimal) Marshal() []byte                { return types.EncodeInt32(&a.argScale) }
func (a *aggSumDecimal) MarshalBinary() ([]byte, error) { return a.Marshal(), nil }
func (a *aggSumDecimal) Unmarshal(bs []byte)            { a.argScale = types.DecodeInt32(bs) }
func aggSumOfDecimalInitCommonContext(
	resultType types.Type, parameters ...types.Type,
) aggexec.AggCommonExecContext {
	return &aggSumDecimal{argScale: parameters[0].Scale}
}
func aggSumOfDecimalInitResult(
	resultType types.Type, parameters ...types.Type) types.Decimal128 {
	return types.Decimal128{B0_63: 0, B64_127: 0}
}

func aggSumOfDecimal64Fill(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter().Add64(value)
	resultSetter(r)
	return err
}
func aggSumOfDecimal64Fills(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := commonContext.(*aggSumDecimal)
	v := types.Decimal128{B0_63: uint64(value), B64_127: 0}
	if value.Sign() {
		v.B64_127 = ^v.B64_127
	}
	r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
	if err != nil {
		return err
	}
	r, err = resultGetter().Add128(r)
	resultSetter(r)
	return err
}
func aggSumOfDecimal64Merge(
	ctx1, ctx2 aggexec.AggGroupExecContext,
	commonContext aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter1().Add128(resultGetter2())
	resultSetter(r)
	return err
}

func aggSumOfDecimal128Fill(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter().Add128(value)
	resultSetter(r)
	return err
}
func aggSumOfDecimal128Fills(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := commonContext.(*aggSumDecimal)
	r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
	if err != nil {
		return err
	}
	r, err = resultGetter().Add128(r)
	resultSetter(r)
	return err
}
func aggSumOfDecimal128Merge(
	ctx1, ctx2 aggexec.AggGroupExecContext,
	commonContext aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter1().Add128(resultGetter2())
	resultSetter(r)
	return err
}

func aggSumInitResult[to numericWithMaxScale](
	_ types.Type, parameters ...types.Type) to {
	return to(0)
}
func aggSumFill[from numeric, to numericWithMaxScale](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[to], resultSetter aggexec.AggSetter[to]) error {
	resultSetter(resultGetter() + to(value))
	return nil
}
func aggSumFills[from numeric, to numericWithMaxScale](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[to], resultSetter aggexec.AggSetter[to]) error {
	resultSetter(resultGetter() + to(value)*to(count))
	return nil
}
func aggSumMerge[from numeric, to numericWithMaxScale](
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[to],
	resultSetter aggexec.AggSetter[to]) error {
	resultSetter(resultGetter1() + resultGetter2())
	return nil
}

// Specialized SUM functions for int64 with overflow checking
// These match MySQL 8.0 strict mode behavior

func addInt64SumWithOverflowCheck(v1, v2 int64) (int64, error) {
	result := v1 + v2
	// Overflow detection: same as SELECT arithmetic
	// Note: use >= and <= to catch the case where result wraps to exactly 0
	if (v1 > 0 && v2 > 0 && result <= 0) || (v1 < 0 && v2 < 0 && result >= 0) {
		return 0, moerr.NewOutOfRangef(context.Background(), "int64", "SUM overflow in (%d + %d)", v1, v2)
	}
	return result, nil
}

func mulInt64SumWithOverflowCheck(v1, v2 int64) (int64, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}
	if v1 == math.MinInt64 && v2 == -1 || v2 == math.MinInt64 && v1 == -1 {
		return 0, moerr.NewOutOfRangef(context.Background(), "int64", "SUM overflow in (%d * %d)", v1, v2)
	}
	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(context.Background(), "int64", "SUM overflow in (%d * %d)", v1, v2)
	}
	return result, nil
}

func aggSumInt64Fill(
	execContext aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value int64, isEmpty bool,
	resultGetter aggexec.AggGetter[int64], resultSetter aggexec.AggSetter[int64]) error {

	result, err := addInt64SumWithOverflowCheck(resultGetter(), value)
	if err != nil {
		return err
	}
	resultSetter(result)
	return nil
}

func aggSumInt64Fills(
	execContext aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value int64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[int64], resultSetter aggexec.AggSetter[int64]) error {

	// First: value * count
	product, err := mulInt64SumWithOverflowCheck(value, int64(count))
	if err != nil {
		return err
	}

	// Then: result + product
	result, err := addInt64SumWithOverflowCheck(resultGetter(), product)
	if err != nil {
		return err
	}
	resultSetter(result)
	return nil
}

func aggSumInt64Merge(
	ctx1, ctx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[int64],
	resultSetter aggexec.AggSetter[int64]) error {

	result, err := addInt64SumWithOverflowCheck(resultGetter1(), resultGetter2())
	if err != nil {
		return err
	}
	resultSetter(result)
	return nil
}
