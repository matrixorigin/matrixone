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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/agg2"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg"
)

var supportedAggregateFunctions = []FuncNew{
	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggBitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggBitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "bit_and",
					aggNew: functionAgg.NewAggBitAnd,
				},
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggBitOrSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggBitOrReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "bit_or",
					aggNew: functionAgg.NewAggBitOr,
				},
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggBitXorSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggBitXorReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "bit_xor",
					aggNew: functionAgg.NewAggBitXor,
				},
			},
		},
	},
	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggVarianceSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggVarianceReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "var_pop",
					aggNew: functionAgg.NewAggVarPop,
				},
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggStdDevSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggStdDevReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "stddev_pop",
					aggNew: functionAgg.NewAggStdDevPop,
				},
			},
		},
	},

	{
		functionId: MEDIAN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggMedianSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggMedianReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "median",
					aggNew: functionAgg.NewAggMedian,
				},
			},
		},
	},

	{
		functionId: CLUSTER_CENTERS,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) > 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggClusterCentersReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:    "cluster_centers",
					aggNew: functionAgg.NewAggClusterCenters,
				},
			},
		},
	},
}

var supportedAggInNewFramework = []FuncNew{
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
				retType:    functionAgg.AggCountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "count",
					aggRegister: agg2.RegisterCountColumn,
				},
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
				retType:    functionAgg.AggCountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "count(*)",
					aggRegister: agg2.RegisterCountStar,
				},
			},
		},
	},

	{
		functionId: MIN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggMinSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggMinReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "min",
					aggRegister: agg2.RegisterMin,
				},
			},
		},
	},

	{
		functionId: MAX,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggMaxSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggMaxReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "max",
					aggRegister: agg2.RegisterMax,
				},
			},
		},
	},

	{
		functionId: SUM,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggSumSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggSumReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "sum",
					aggRegister: agg2.RegisterSum,
				},
			},
		},
	},

	{
		functionId: AVG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggAvgSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggAvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg",
					aggRegister: agg2.RegisterAvg,
				},
			},
		},
	},

	{
		functionId: GROUP_CONCAT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) > 0 {
				kk := make([]types.Type, len(inputs))
				needCast := false
				for i, in := range inputs {
					if in.IsVarlen() {
						kk[i] = in
					} else {
						needCast = true
						kk[i] = types.T_text.ToType()
					}
				}
				if needCast {
					return newCheckResultWithCast(0, kk)
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggGroupConcatReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "group_concat",
					aggRegister: agg2.RegisterGroupConcat,
				},
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
					return newCheckResultWithCast(0, []types.Type{types.T_uint64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggApproxCountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "approx_count",
					aggRegister: agg2.RegisterApproxCount,
				},
			},
		},
	},

	{
		functionId: ANY_VALUE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, functionAgg.AggAnyValueSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    functionAgg.AggAnyValueReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "any_value",
					aggRegister: agg2.RegisterAnyValue,
				},
			},
		},
	},
}
