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

func RegisterSum2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), SumReturnType, false, true),
		aggSumOfDecimal64InitCommonContext,
		nil,
		aggSumOfDecimal64InitResult,
		aggSumOfDecimal64Fill,
		aggSumOfDecimal64Fills,
		aggSumOfDecimal64Merge,
		nil,
	)
}

func aggSumOfDecimal64InitCommonContext(
	resultType types.Type, parameters ...types.Type,
) aggexec.AggCommonExecContext {
	return &aggSumDecimal64{argScale: parameters[0].Scale}
}
func aggSumOfDecimal64InitResult(
	resultType types.Type, parameters ...types.Type) types.Decimal128 {
	return types.Decimal128{B0_63: 0, B64_127: 0}
}
func aggSumOfDecimal64Fill(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal64,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter().Add64(value)
	resultSetter(r)
	return err
}
func aggSumOfDecimal64Fills(
	execContext aggexec.AggGroupExecContext, commonContext aggexec.AggCommonExecContext,
	value types.Decimal64, count int,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	a := commonContext.(*aggSumDecimal64)
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
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	r, err := resultGetter1().Add128(resultGetter2())
	resultSetter(r)
	return err
}
