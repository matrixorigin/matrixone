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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/agg"
)

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
				retType:    aggexec.CountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "count",
					aggRegister: agg.RegisterCountColumn,
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
				retType:    aggexec.CountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "count(*)",
					aggRegister: agg.RegisterCountStar,
				},
			},
		},
	},

	{
		functionId: MIN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.MinSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.MinReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "min",
					aggRegister: agg.RegisterMin2,
				},
			},
		},
	},

	{
		functionId: MAX,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.MaxSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.MaxReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "max",
					aggRegister: agg.RegisterMax2,
				},
			},
		},
	},

	{
		functionId: SUM,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.SumReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "sum",
					aggRegister: agg.RegisterSum2,
				},
			},
		},
	},

	{
		functionId: AVG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg",
					aggRegister: agg.RegisterAvg2,
				},
			},
		},
	},

	{
		functionId: AVG_TW_CACHE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgTwCacheSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgTwCacheReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg_tw_cache",
					aggRegister: agg.RegisterAvgTwCache,
				},
			},
		},
	},

	{
		functionId: AVG_TW_RESULT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgTwResultSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgTwResultReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg_tw_result",
					aggRegister: agg.RegisterAvgTwResult,
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
					if in.Oid == types.T_any {
						needCast = true
						kk[i] = types.T_text.ToType()
						continue
					}
					if !aggexec.IsGroupConcatSupported(in) {
						return newCheckResultWithFailure(failedAggParametersWrong)
					}

					kk[i] = in
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
				retType:    aggexec.GroupConcatReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "group_concat",
					aggRegister: agg.RegisterGroupConcat,
				},
			},
		},
	},

	{
		functionId: APPROX_COUNT,
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
				retType:    aggexec.CountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "approx_count",
					aggRegister: agg.RegisterApproxCount,
				},
			},
		},
	},

	// todo: it's a better way to rewrite `approx_count_distinct` to `approx_count(distinct col)`?. not sure.
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
				retType:    aggexec.CountReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "approx_count_distinct",
					aggRegister: agg.RegisterApproxCount,
				},
			},
		},
	},

	{
		functionId: ANY_VALUE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AnyValueSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AnyValueReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "any_value",
					aggRegister: agg.RegisterAnyValue2,
				},
			},
		},
	},

	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_and",
					aggRegister: agg.RegisterBitAnd2,
				},
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_or",
					aggRegister: agg.RegisterBitOr2,
				},
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_xor",
					aggRegister: agg.RegisterBitXor2,
				},
			},
		},
	},

	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.VarPopSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.VarPopReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "var_pop",
					aggRegister: agg.RegisterVarPop2,
				},
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.VarPopSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.VarPopReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "stddev_pop",
					aggRegister: agg.RegisterStdDevPop2,
				},
			},
		},
	},

	{
		functionId: MEDIAN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggexec.MedianSupportedType)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.MedianReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "median",
					aggRegister: agg.RegisterMedian,
				},
			},
		},
	},

	{
		functionId: CLUSTER_CENTERS,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return fixedUnaryAggTypeCheck(inputs, aggexec.ClusterCentersSupportTypes)
			}
			if len(inputs) == 2 && inputs[1].IsVarlen() {
				result := fixedUnaryAggTypeCheck(inputs[:1], aggexec.ClusterCentersSupportTypes)
				if result.status == succeedMatched {
					return result
				}
				if result.status == succeedWithCast {
					result.finalType = append(result.finalType, types.T_varchar.ToType())
					return result
				}
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.ClusterCentersReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "cluster_centers",
					aggRegister: agg.RegisterClusterCenters,
				},
			},
		},
	},

	// function `BITMAP_CONSTRUCT_AGG`
	{
		functionId: BITMAP_CONSTRUCT_AGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitmapConstructSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       agg.BitmapConstructSupportedTypes,
				retType:    agg.BitmapConstructReturnType,

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_construct_agg",
					aggRegister: agg.RegisterBitmapConstruct2,
				},
			},
		},
	},

	// function `BITMAP_OR_AGG`
	{
		functionId: BITMAP_OR_AGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitmapOrSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       agg.BitmapOrSupportedTypes,
				retType:    agg.BitmapOrReturnType,

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_or_agg",
					aggRegister: agg.RegisterBitmapOr2,
				},
			},
		},
	},
}
