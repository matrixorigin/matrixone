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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/agg2"
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
				retType:    aggexec.CountReturnType,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.MinSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.MinReturnType,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.MaxSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.MaxReturnType,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.SumReturnType,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.AvgSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.AvgReturnType,
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
					aggRegister: agg2.RegisterGroupConcat,
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
					aggRegister: agg2.RegisterApproxCount,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.AnyValueSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.AnyValueReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "any_value",
					aggRegister: agg2.RegisterAnyValue,
				},
			},
		},
	},

	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg2.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_and",
					aggRegister: agg2.RegisterBitAnd,
				},
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg2.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_or",
					aggRegister: agg2.RegisterBitOr,
				},
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg2.BitAndSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.BitAndReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_xor",
					aggRegister: agg2.RegisterBitXor,
				},
			},
		},
	},

	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg2.VarPopSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.VarPopReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "var_pop",
					aggRegister: agg2.RegisterVarPop,
				},
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg2.VarPopSupportedParameters)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg2.VarPopReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "stddev_pop",
					aggRegister: agg2.RegisterStdVarPop,
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
					aggRegister: agg2.RegisterMedian,
				},
			},
		},
	},

	{
		functionId: CLUSTER_CENTERS,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggexec.ClusterCentersSupportTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.ClusterCentersReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "cluster_centers",
					aggRegister: agg2.RegisterClusterCenters,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.BitmapConstructSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       agg2.BitmapConstructSupportedTypes,
				retType:    agg2.BitmapConstructReturnType,

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_construct_agg",
					aggRegister: agg2.RegisterBitmapConstruct,
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
			return fixedUnaryAggTypeCheck(inputs, agg2.BitmapOrSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       agg2.BitmapOrSupportedTypes,
				retType:    agg2.BitmapConstructReturnType,

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_or_agg",
					aggRegister: agg2.RegisterBitmapOr,
				},
			},
		},
	},
}
