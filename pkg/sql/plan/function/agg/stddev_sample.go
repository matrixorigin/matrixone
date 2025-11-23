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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterStdDevSample2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint64], aggVarSampleFills[uint64], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int8], aggVarSampleFills[int8], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int16], aggVarSampleFills[int16], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int32], aggVarSampleFills[int32], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[int64], aggVarSampleFills[int64], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint8], aggVarSampleFills[uint8], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint16], aggVarSampleFills[uint16], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint32], aggVarSampleFills[uint32], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[uint64], aggVarSampleFills[uint64], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[float32], aggVarSampleFills[float32], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarSampleReturnType, true),
		nil, generateAggVarSampleGroupContext, aggVarSampleInitResult,
		aggVarSampleFill[float64], aggVarSampleFills[float64], aggVarSampleMerge, aggStdDevSampleFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarSampleReturnType, true),
		generateAggVarSampleOfDecimalCommonContext, generateAggVarSampleOfDecimalGroupContext, aggVarSampleOfDecimalInitResult,
		aggVarSampleOfDecimal64Fill, aggVarSampleOfDecimal64Fills, aggVarSampleOfDecimalMerge, aggStdDevSampleOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarSampleReturnType, true),
		generateAggVarSampleOfDecimalCommonContext, generateAggVarSampleOfDecimalGroupContext, aggVarSampleOfDecimalInitResult,
		aggVarSampleOfDecimal128Fill, aggVarSampleOfDecimal128Fills, aggVarSampleOfDecimalMerge, aggStdDevSampleOfDecimalFlush)
}

func aggStdDevSampleFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggGetter[float64],
	resultSetter aggexec.AggSetter[float64]) error {
	if err := aggVarSampleFlush(groupCtx, nil, resultGetter, resultSetter); err != nil {
		return err
	}
	resultSetter(math.Sqrt(resultGetter()))
	return nil
}

func aggStdDevSampleOfDecimalFlush(
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
