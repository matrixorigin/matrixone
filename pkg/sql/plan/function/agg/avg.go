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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterAvg2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[uint64], aggAvgFills[uint64], aggAvgMerge[uint64], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[int8], aggAvgFills[int8], aggAvgMerge[int8], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[int16], aggAvgFills[int16], aggAvgMerge[int16], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[int32], aggAvgFills[int32], aggAvgMerge[int32], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[int64], aggAvgFills[int64], aggAvgMerge[int64], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[uint8], aggAvgFills[uint8], aggAvgMerge[uint8], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[uint16], aggAvgFills[uint16], aggAvgMerge[uint16], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[uint32], aggAvgFills[uint32], aggAvgMerge[uint32], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[uint64], aggAvgFills[uint64], aggAvgMerge[uint64], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[float32], aggAvgFills[float32], aggAvgMerge[float32], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), AvgReturnType, true),
		nil, generateAggAvgContext, aggAvgInitResult,
		aggAvgFill[float64], aggAvgFills[float64], aggAvgMerge[float64], aggAvgFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), AvgReturnType, true),
		generateAggAvgDecimalCommonContext, generateAggAvgContext, aggAvgOfDecimalInitResult,
		aggAvgOfDecimal64Fill, aggAvgOfDecimal64Fills, aggAvgOfDecimalMerge, aggAvgOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), AvgReturnType, true),
		generateAggAvgDecimalCommonContext, generateAggAvgContext, aggAvgOfDecimalInitResult,
		aggAvgOfDecimal128Fill, aggAvgOfDecimal128Fills, aggAvgOfDecimalMerge, aggAvgOfDecimalFlush)
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

type aggAvgContext int64

func (a *aggAvgContext) Marshal() []byte     { return types.EncodeInt64((*int64)(a)) }
func (a *aggAvgContext) Unmarshal(bs []byte) { *a = aggAvgContext(types.DecodeInt64(bs)) }
func generateAggAvgContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	c := aggAvgContext(0)
	return &c
}

func aggAvgInitResult(_ types.Type, _ ...types.Type) float64 {
	return 0
}
func aggAvgFill[from numeric](
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	*(groupCtx.(*aggAvgContext))++
	resultSetter(resultGetter() + float64(value))
	return nil
}
func aggAvgFills[from numeric](
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	*(groupCtx.(*aggAvgContext)) += aggAvgContext(count)
	resultSetter(resultGetter() + float64(value)*float64(count))
	return nil
}
func aggAvgMerge[from numeric](
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	*(groupCtx1.(*aggAvgContext)) += *(groupCtx2.(*aggAvgContext))
	resultSetter(resultGetter1() + resultGetter2())
	return nil
}
func aggAvgFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	count := *(groupCtx.(*aggAvgContext))
	if count == 0 {
		resultSetter(0)
	} else {
		resultSetter(resultGetter() / float64(count))
	}
	return nil
}

type aggAvgDecimalCommonCtx int32

func (a *aggAvgDecimalCommonCtx) Marshal() []byte { return types.EncodeInt32((*int32)(a)) }
func (a *aggAvgDecimalCommonCtx) Unmarshal(bs []byte) {
	*a = aggAvgDecimalCommonCtx(types.DecodeInt32(bs))
}
func generateAggAvgDecimalCommonContext(_ types.Type, parameters ...types.Type) aggexec.AggCommonExecContext {
	c := aggAvgDecimalCommonCtx(parameters[0].Scale)
	return &c
}

func aggAvgOfDecimalInitResult(_ types.Type, _ ...types.Type) types.Decimal128 {
	return types.Decimal128{B0_63: 0, B64_127: 0}
}
func aggAvgOfDecimalMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	*groupCtx1.(*aggAvgContext) += *groupCtx2.(*aggAvgContext)
	r, err := resultGetter1().Add128(resultGetter2())
	resultSetter(r)
	return err
}
func aggAvgOfDecimalFlush(
	groupCtx aggexec.AggGroupExecContext,
	commonCtx aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	count := *(groupCtx.(*aggAvgContext))
	argScale := *(commonCtx.(*aggAvgDecimalCommonCtx))

	if count == 0 {
		resultSetter(types.Decimal128{B0_63: 0, B64_127: 0})
		return nil
	}
	v, _, err := resultGetter().Div(types.Decimal128{B0_63: uint64(count), B64_127: 0}, int32(argScale), 0)
	resultSetter(v)
	return err
}

func aggAvgOfDecimal64Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	*groupCtx.(*aggAvgContext)++
	r, err := resultGetter().Add64(value)
	resultSetter(r)
	return err
}
func aggAvgOfDecimal64Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	argScale := *(commonCtx.(*aggAvgDecimalCommonCtx))
	*groupCtx.(*aggAvgContext) += aggAvgContext(count)

	v := aggexec.FromD64ToD128(value)
	r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, int32(argScale), 0)
	if err != nil {
		return err
	}
	r, err = resultGetter().Add128(r)
	resultSetter(r)
	return err
}

func aggAvgOfDecimal128Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	*groupCtx.(*aggAvgContext)++
	r, err := resultGetter().Add128(value)
	resultSetter(r)
	return err
}
func aggAvgOfDecimal128Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	argScale := *(commonCtx.(*aggAvgDecimalCommonCtx))
	*groupCtx.(*aggAvgContext) += aggAvgContext(count)

	r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, int32(argScale), 0)
	if err != nil {
		return err
	}
	r, err = resultGetter().Add128(r)
	resultSetter(r)
	return err
}
