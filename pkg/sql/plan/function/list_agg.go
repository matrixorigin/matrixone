// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var supportedAggregateFunctions = []FuncNew{
	{
		functionId: MAX,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggMaxSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggMaxReturnType,
			},
		},
	},

	{
		functionId: MIN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggMinSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggMinxReturnType,
			},
		},
	},

	{
		functionId: SUM,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggSumSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggSumReturnType,
			},
		},
	},

	{
		functionId: AVG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggAvgSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggAvgReturnType,
			},
		},
	},

	{
		functionId: COUNT,
		class:      plan.Function_AGG | plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggCountReturnType,
			},
		},
	},

	{
		functionId: STARCOUNT,
		class:      plan.Function_AGG | plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggCountReturnType,
			},
		},
	},

	{
		functionId: APPROX_COUNT,
		class:      plan.Function_AGG | plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggCountReturnType,
			},
		},
	},

	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggBitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggBitAndReturnType,
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggBitOrSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggBitOrReturnType,
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggBitXorSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggBitXorReturnType,
			},
		},
	},

	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggVarianceSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggVarianceReturnType,
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggStdDevSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggStdDevReturnType,
			},
		},
	},

	{
		functionId: APPROX_COUNT_DISTINCT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.ApproxCountReturnType,
			},
		},
	},

	{
		functionId: ANY_VALUE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggAnyValueSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggAnyValueReturnType,
			},
		},
	},

	{
		functionId: MEDIAN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggMedianSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggMedianReturnType,
			},
		},
	},

	{
		functionId: GROUP_CONCAT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return newCheckResultWithSuccess(0)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggGroupConcatReturnType,
			},
		},
	},
}
