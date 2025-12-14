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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterVarSample2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint64], aggVarSampleFills[uint64], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int8], aggVarSampleFills[int8], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int16], aggVarSampleFills[int16], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int32], aggVarSampleFills[int32], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int64], aggVarSampleFills[int64], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint8], aggVarSampleFills[uint8], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint16], aggVarSampleFills[uint16], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint32], aggVarSampleFills[uint32], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint64], aggVarSampleFills[uint64], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[float32], aggVarSampleFills[float32], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[float64], aggVarSampleFills[float64], aggVarSampleMerge, aggVarSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarSampleReturnType, true),
		generateAggVarSampleOfDecimalCommonContext, generateAggVarSampleOfDecimalGroupContext, aggVarSampleOfDecimalInitResult,
		aggVarSampleOfDecimal64Fill, aggVarSampleOfDecimal64Fills, aggVarSampleOfDecimalMerge, aggVarSampleOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarSampleReturnType, true),
		generateAggVarSampleOfDecimalCommonContext, generateAggVarSampleOfDecimalGroupContext, aggVarSampleOfDecimalInitResult,
		aggVarSampleOfDecimal128Fill, aggVarSampleOfDecimal128Fills, aggVarSampleOfDecimalMerge, aggVarSampleOfDecimalFlush)
}

var VarSampleSupportedParameters = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func VarSampleReturnType(typs []types.Type) types.Type {
	if typs[0].IsDecimal() {
		s := int32(12)
		if typs[0].Scale > s {
			s = typs[0].Scale
		}
		return types.New(types.T_decimal128, 38, s)
	}
	return types.New(types.T_float64, 0, 0)
}

// we use the math formula `var_samp = (sum(x^2) - sum(x)^2/n) / (n-1)` to calculate the sample variance.
// so we need to store the sum of x and sum of x^2, and the count of x.
// we use the agg result to store the sum of x^2, so we only need to store the sum of x and the count of x.
type aggVarSampleGroupContext struct {
	sum   float64
	count int64
}

func generateAggVarSampleGroupContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &aggVarSampleGroupContext{
		sum:   0,
		count: 0,
	}
}

func (a *aggVarSampleGroupContext) Size() int64 {
	return 16 // float64 + int64
}

func (a *aggVarSampleGroupContext) Marshal() []byte {
	bs := types.EncodeFloat64(&a.sum)
	bs = append(bs, types.EncodeInt64(&a.count)...)
	return bs
}
func (a *aggVarSampleGroupContext) MarshalBinary() ([]byte, error) { return a.Marshal(), nil }
func (a *aggVarSampleGroupContext) Unmarshal(bs []byte) {
	a.sum = types.DecodeFloat64(bs[:8])
	a.count = types.DecodeInt64(bs[8:])
}

func aggVarSampleInitResult(_ types.Type, _ ...types.Type) float64 {
	return 0
}
func aggVarSampleFill[from numeric](
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarSampleGroupContext)
	a.sum += float64(value)
	a.count++
	resultSetter(resultGetter() + math.Pow(float64(value), 2))
	return nil
}
func aggVarSampleFills[from numeric](
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[float64], resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarSampleGroupContext)
	a.sum += float64(value) * float64(count)
	a.count += int64(count)
	resultSetter(resultGetter() + math.Pow(float64(value), 2)*float64(count))
	return nil
}
func aggVarSampleMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	if isEmpty2 {
		return nil
	}
	a1 := groupCtx1.(*aggVarSampleGroupContext)
	a2 := groupCtx2.(*aggVarSampleGroupContext)
	a1.sum += a2.sum
	a1.count += a2.count
	resultSetter(resultGetter1() + resultGetter2())
	return nil
}
func aggVarSampleFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	a := groupCtx.(*aggVarSampleGroupContext)
	if a.count <= 1 {
		resultSetter(0)
	} else {
		avg := a.sum / float64(a.count)
		resultSetter((resultGetter()/float64(a.count) - math.Pow(avg, 2)) * float64(a.count) / float64(a.count-1))
	}
	return nil
}

// var_samp(decimal) uses the same formula as var_samp(numeric),
// but for considering the overflow of decimal^2, we set an overflow flag here because the var_samp(an overflow decimal) = 0.
type aggVarSampleOfDecimalGroupContext struct {
	count    int64
	overflow bool
	sum      types.Decimal128
}

func generateAggVarSampleOfDecimalGroupContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &aggVarSampleOfDecimalGroupContext{
		count:    0,
		overflow: false,
		sum:      types.Decimal128{},
	}
}

func (a *aggVarSampleOfDecimalGroupContext) Size() int64 {
	return 32
}

func (a *aggVarSampleOfDecimalGroupContext) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeBool(&a.overflow)...)
	bs = append(bs, types.EncodeDecimal128(&a.sum)...)
	return bs
}
func (a *aggVarSampleOfDecimalGroupContext) MarshalBinary() ([]byte, error) { return a.Marshal(), nil }
func (a *aggVarSampleOfDecimalGroupContext) Unmarshal(bs []byte) {
	a.count = types.DecodeInt64(bs[:8])
	a.overflow = types.DecodeBool(bs[8:9])
	a.sum = types.DecodeDecimal128(bs[9:])
}

type aggVarSampleOfDecimalCommonContext struct {
	argScale    int32
	resultScale int32
}

func generateAggVarSampleOfDecimalCommonContext(ret types.Type, parameters ...types.Type) aggexec.AggCommonExecContext {
	return &aggVarSampleOfDecimalCommonContext{
		argScale:    parameters[0].Scale,
		resultScale: ret.Scale,
	}
}

func (a *aggVarSampleOfDecimalCommonContext) Size() int64 {
	return 8
}

func (a *aggVarSampleOfDecimalCommonContext) Marshal() []byte {
	bs := types.EncodeInt32(&a.argScale)
	bs = append(bs, types.EncodeInt32(&a.resultScale)...)
	return bs
}
func (a *aggVarSampleOfDecimalCommonContext) MarshalBinary() ([]byte, error) { return a.Marshal(), nil }
func (a *aggVarSampleOfDecimalCommonContext) Unmarshal(bs []byte) {
	a.argScale = types.DecodeInt32(bs[:4])
	a.resultScale = types.DecodeInt32(bs[4:])
}

func aggVarSampleOfDecimalInitResult(_ types.Type, _ ...types.Type) types.Decimal128 {
	return types.Decimal128{
		B0_63:   0,
		B64_127: 0,
	}
}

func aggVarSampleOfDecimal128Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarSampleOfDecimalGroupContext)
	b := commonCtx.(*aggVarSampleOfDecimalCommonContext)

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
func aggVarSampleOfDecimal128Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarSampleOfDecimalGroupContext)
	b := commonCtx.(*aggVarSampleOfDecimalCommonContext)

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
func aggVarSampleOfDecimalMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a1 := groupCtx1.(*aggVarSampleOfDecimalGroupContext)
	a2 := groupCtx2.(*aggVarSampleOfDecimalGroupContext)

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
func aggVarSampleOfDecimalFlush(
	groupCtx aggexec.AggGroupExecContext,
	commonCtx aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := groupCtx.(*aggVarSampleOfDecimalGroupContext)
	b := commonCtx.(*aggVarSampleOfDecimalCommonContext)

	r, err := getVarianceSampleFromSumPowCount(a.sum, resultGetter(), a.count, b.argScale)
	if err != nil {
		return err
	}
	resultSetter(r)
	return nil
}

func aggVarSampleOfDecimal64Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	return aggVarSampleOfDecimal128Fill(groupCtx, commonCtx, aggexec.FromD64ToD128(value), isEmpty, resultGetter, resultSetter)
}
func aggVarSampleOfDecimal64Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	return aggVarSampleOfDecimal128Fills(groupCtx, commonCtx, aggexec.FromD64ToD128(value), count, isEmpty, resultGetter, resultSetter)
}

func getVarianceSampleFromSumPowCount(
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
	// var_pop = part1 - part2
	varPop, _, err := part1.Sub(part2, part1Scale, part2Scale)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	// var_samp = var_pop * n / (n-1)
	countMinus1 := types.Decimal128{B0_63: uint64(count - 1), B64_127: 0}
	countD128 := types.Decimal128{B0_63: uint64(count), B64_127: 0}
	result, _, err := varPop.Mul(countD128, part1Scale, 0)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	result, _, err = result.Div(countMinus1, part1Scale, 0)
	return result, err
}
