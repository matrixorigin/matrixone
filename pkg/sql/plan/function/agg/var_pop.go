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
	"math"
)

func RegisterVarPop2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint64], aggVarPopFills[uint64], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int8], aggVarPopFills[int8], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int16], aggVarPopFills[int16], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int32], aggVarPopFills[int32], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int64], aggVarPopFills[int64], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint8], aggVarPopFills[uint8], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint16], aggVarPopFills[uint16], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint32], aggVarPopFills[uint32], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint64], aggVarPopFills[uint64], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[float32], aggVarPopFills[float32], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[float64], aggVarPopFills[float64], aggVarPopMerge, aggVarPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarPopReturnType, true),
		generateAggVarPopOfDecimalCommonContext, generateAggVarPopOfDecimalGroupContext, aggVarPopOfDecimalInitResult,
		aggVarPopOfDecimal64Fill, aggVarPopOfDecimal64Fills, aggVarPopOfDecimalMerge, aggVarPopOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarPopReturnType, true),
		generateAggVarPopOfDecimalCommonContext, generateAggVarPopOfDecimalGroupContext, aggVarPopOfDecimalInitResult,
		aggVarPopOfDecimal128Fill, aggVarPopOfDecimal128Fills, aggVarPopOfDecimalMerge, aggVarPopOfDecimalFlush)
}

var VarPopSupportedParameters = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func VarPopReturnType(typs []types.Type) types.Type {
	if typs[0].IsDecimal() {
		s := int32(12)
		if typs[0].Scale > s {
			s = typs[0].Scale
		}
		return types.New(types.T_decimal128, 38, s)
	}
	return types.New(types.T_float64, 0, 0)
}

// we use the math formula `var_pop = (sum(x^2) - sum(x)^2/n) / n` to calculate the variance of population.
// so we need to store the sum of x and sum of x^2, and the count of x.
// we use the agg result to store the sum of x^2, so we only need to store the sum of x and the count of x.
type aggVarPopGroupContext struct {
	sum   float64
	count int64
}

func generateAggVarPopGroupContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &aggVarPopGroupContext{
		sum:   0,
		count: 0,
	}
}
func (a *aggVarPopGroupContext) Marshal() []byte {
	bs := types.EncodeFloat64(&a.sum)
	bs = append(bs, types.EncodeInt64(&a.count)...)
	return bs
}
func (a *aggVarPopGroupContext) Unmarshal(bs []byte) {
	a.sum = types.DecodeFloat64(bs[:8])
	a.count = types.DecodeInt64(bs[8:])
}

func aggVarPopInitResult(_ types.Type, _ ...types.Type) float64 {
	return 0
}
func aggVarPopFill[from numeric](
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarPopGroupContext)
	a.sum += float64(value)
	a.count++
	resultSetter(resultGetter() + math.Pow(float64(value), 2))
	return nil
}
func aggVarPopFills[from numeric](
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarPopGroupContext)
	a.sum += float64(value) * float64(count)
	a.count += int64(count)
	resultSetter(resultGetter() + math.Pow(float64(value), 2)*float64(count))
	return nil
}
func aggVarPopMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	if isEmpty2 {
		return nil
	}
	a1 := groupCtx1.(*aggVarPopGroupContext)
	a2 := groupCtx2.(*aggVarPopGroupContext)
	a1.sum += a2.sum
	a1.count += a2.count
	resultSetter(resultGetter1() + resultGetter2())
	return nil
}
func aggVarPopFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarPopGroupContext)
	if a.count <= 1 {
		resultSetter(0)
	} else {
		avg := a.sum / float64(a.count)
		resultSetter(resultGetter()/float64(a.count) - math.Pow(avg, 2))
	}
	return nil
}

// var_pop(decimal) uses the same formula as var_pop(numeric),
// but for considering the overflow of decimal^2, we set an overflow flag here because the var_pop(an overflow decimal) = 0.
type aggVarPopOfDecimalGroupContext struct {
	count    int64
	overflow bool
	sum      types.Decimal128
}

func generateAggVarPopOfDecimalGroupContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &aggVarPopOfDecimalGroupContext{
		count:    0,
		overflow: false,
		sum:      types.Decimal128{},
	}
}
func (a *aggVarPopOfDecimalGroupContext) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeBool(&a.overflow)...)
	bs = append(bs, types.EncodeDecimal128(&a.sum)...)
	return bs
}
func (a *aggVarPopOfDecimalGroupContext) Unmarshal(bs []byte) {
	a.count = types.DecodeInt64(bs[:8])
	a.overflow = types.DecodeBool(bs[8:9])
	a.sum = types.DecodeDecimal128(bs[9:])
}

type aggVarPopOfDecimalCommonContext struct {
	argScale    int32
	resultScale int32
}

func generateAggVarPopOfDecimalCommonContext(ret types.Type, parameters ...types.Type) aggexec.AggCommonExecContext {
	return &aggVarPopOfDecimalCommonContext{
		argScale:    parameters[0].Scale,
		resultScale: ret.Scale,
	}
}
func (a *aggVarPopOfDecimalCommonContext) Marshal() []byte {
	bs := types.EncodeInt32(&a.argScale)
	bs = append(bs, types.EncodeInt32(&a.resultScale)...)
	return bs
}
func (a *aggVarPopOfDecimalCommonContext) Unmarshal(bs []byte) {
	a.argScale = types.DecodeInt32(bs[:4])
	a.resultScale = types.DecodeInt32(bs[4:])
}

func aggVarPopOfDecimalInitResult(_ types.Type, _ ...types.Type) types.Decimal128 {
	return types.Decimal128{
		B0_63:   0,
		B64_127: 0,
	}
}

func aggVarPopOfDecimal128Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarPopOfDecimalGroupContext)
	b := commonCtx.(*aggVarPopOfDecimalCommonContext)

	if !a.overflow {
		a.count++
		newSum, newPow2, overflow := getNewValueSumAndNewPower2(a.sum, resultGetter(), value, b.argScale, 1)
		if !overflow {
			a.sum = newSum
			resultSetter(newPow2)
			return nil
		}
		a.overflow = true
		return nil
	}
	return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
}
func aggVarPopOfDecimal128Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarPopOfDecimalGroupContext)
	b := commonCtx.(*aggVarPopOfDecimalCommonContext)

	if !a.overflow {
		a.count += int64(count)
		newSum, newPow2, overflow := getNewValueSumAndNewPower2(a.sum, resultGetter(), value, b.argScale, count)
		if !overflow {
			a.sum = newSum
			resultSetter(newPow2)
			return nil
		}
		if a.count == 1 {
			a.overflow = true
			return nil
		}
	}
	return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
}
func aggVarPopOfDecimalMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a1 := groupCtx1.(*aggVarPopOfDecimalGroupContext)
	a2 := groupCtx2.(*aggVarPopOfDecimalGroupContext)

	if a2.count == 0 {
		return nil
	}
	if a1.count == 0 {
		a1.count = a2.count
		a1.overflow = a2.overflow
		a1.sum = a2.sum
		resultSetter(resultGetter2())
		return nil
	}

	if a1.overflow || a2.overflow {
		return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}

	var err error
	var newPow2 types.Decimal128
	a1.count += a2.count
	a1.sum, err = a1.sum.Add128(a2.sum)
	if err == nil {
		newPow2, err = resultGetter1().Add128(resultGetter2())
	}
	if err != nil {
		if a1.count == 1 {
			a1.overflow = true
			return nil
		}
		return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}
	resultSetter(newPow2)
	return nil
}
func aggVarPopOfDecimalFlush(
	groupCtx aggexec.AggGroupExecContext,
	commonCtx aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarPopOfDecimalGroupContext)
	b := commonCtx.(*aggVarPopOfDecimalCommonContext)

	r, err := getVarianceFromSumPowCount(a.sum, resultGetter(), a.count, b.argScale)
	if err != nil {
		return err
	}
	resultSetter(r)
	return nil
}

func aggVarPopOfDecimal64Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	return aggVarPopOfDecimal128Fill(groupCtx, commonCtx, aggexec.FromD64ToD128(value), isEmpty, resultGetter, resultSetter)
}
func aggVarPopOfDecimal64Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	return aggVarPopOfDecimal128Fills(groupCtx, commonCtx, aggexec.FromD64ToD128(value), count, isEmpty, resultGetter, resultSetter)
}

func getNewValueSumAndNewPower2(
	oldSum types.Decimal128, oldPow2 types.Decimal128, value types.Decimal128, valueScale int32, count int) (
	newSum types.Decimal128, newPow2 types.Decimal128, outOfRange bool) {

	count128 := types.Decimal128{B0_63: uint64(count), B64_127: 0}
	valueMulCount, valueMulCountScale, err := value.Mul(count128, valueScale, 0)
	if err != nil {
		return oldSum, oldPow2, true
	}

	newSum, err = oldSum.Add128(valueMulCount)
	if err != nil {
		return oldSum, oldPow2, true
	}

	newPow2, _, err = valueMulCount.Mul(value, valueMulCountScale, valueScale)
	if err != nil {
		return oldSum, oldPow2, true
	}

	newPow2, err = newPow2.Add128(oldPow2)
	return newSum, newPow2, err != nil
}

func getVarianceFromSumPowCount(
	sum types.Decimal128, pow types.Decimal128, count int64, argScale int32) (types.Decimal128, error) {
	if count <= 1 {
		return types.Decimal128{B0_63: 0, B64_127: 0}, nil
	}

	_, powScale, _ := types.Decimal128{}.Mul(types.Decimal128{}, argScale, argScale)

	avg, avgScale, err := sum.Div(types.Decimal128{B0_63: uint64(count), B64_127: 0}, argScale, 0)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	part1, part1Scale, err := pow.Div(types.Decimal128{B0_63: uint64(count), B64_127: 0}, powScale, 0)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	part2, part2Scale, err := avg.Mul(avg, avgScale, avgScale)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	result, _, err := part1.Sub(part2, part1Scale, part2Scale)
	return result, err
}
