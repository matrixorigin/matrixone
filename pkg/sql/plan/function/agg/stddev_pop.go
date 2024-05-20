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
	"math"
)

func RegisterStdDevPop2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint64], aggVarPopFills[uint64], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int8], aggVarPopFills[int8], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int16], aggVarPopFills[int16], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int32], aggVarPopFills[int32], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[int64], aggVarPopFills[int64], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint8], aggVarPopFills[uint8], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint16], aggVarPopFills[uint16], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint32], aggVarPopFills[uint32], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[uint64], aggVarPopFills[uint64], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[float32], aggVarPopFills[float32], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarPopReturnType, true),
		nil, generateAggVarPopGroupContext, aggVarPopInitResult,
		aggVarPopFill[float64], aggVarPopFills[float64], aggVarPopMerge, aggStdDevPopFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarPopReturnType, true),
		generateAggVarPopOfDecimalCommonContext, generateAggVarPopOfDecimalGroupContext, aggVarPopOfDecimalInitResult,
		aggVarPopOfDecimal64Fill, aggVarPopOfDecimal64Fills, aggVarPopOfDecimalMerge, aggStdDevPopOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarPopReturnType, true),
		generateAggVarPopOfDecimalCommonContext, generateAggVarPopOfDecimalGroupContext, aggVarPopOfDecimalInitResult,
		aggVarPopOfDecimal128Fill, aggVarPopOfDecimal128Fills, aggVarPopOfDecimalMerge, aggStdDevPopOfDecimalFlush)
}

func aggStdDevPopFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	if err := aggVarPopFlush(groupCtx, nil, resultGetter, resultSetter); err != nil {
		return err
	}
	resultSetter(math.Sqrt(resultGetter()))
	return nil
}

func aggStdDevPopOfDecimalFlush(
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
	if r.B0_63 == 0 && r.B64_127 == 0 {
		resultSetter(r)
		return nil
	}
	r, err = types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(r, b.resultScale)),
		38, b.resultScale)
	resultSetter(r)
	return err
}
