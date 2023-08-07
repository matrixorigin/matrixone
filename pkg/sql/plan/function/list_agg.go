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
			return fixedUnaryAggTypeCheck(inputs, agg.MaxSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.MaxReturnType,
				specialId:  agg.AggregateMax,
			},
		},
	},

	{
		functionId: MIN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.MinSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.MinReturnType,
				specialId:  agg.AggregateMin,
			},
		},
	},

	{
		functionId: SUM,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.SumSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.SumReturnType,
				specialId:  agg.AggregateSum,
			},
		},
	},

	{
		functionId: AVG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgReturnType,
				specialId:  agg.AggregateAvg,
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
				retType:    agg.CountReturnType,
				specialId:  agg.AggregateCount,
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
				retType:    agg.CountReturnType,
				specialId:  agg.AggregateStarCount,
			},
		},
	},

	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitAndSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitAndReturnType,
				specialId:  agg.AggregateBitAnd,
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitOrSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitOrReturnType,
				specialId:  agg.AggregateBitOr,
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.BitXorSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.BitXorReturnType,
				specialId:  agg.AggregateBitXor,
			},
		},
	},

	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.VarianceSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.VarianceReturnType,
				specialId:  agg.AggregateVariance,
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.StdDevPopSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.StdDevPopReturnType,
				specialId:  agg.AggregateStdDevPop,
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
				specialId:  agg.AggregateApproxCountDistinct,
			},
		},
	},

	{
		functionId: ANY_VALUE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AnyValueSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AnyValueReturnType,
				specialId:  agg.AggregateAnyValue,
			},
		},
	},

	{
		functionId: MEDIAN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.MedianSupported)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.MedianReturnType,
				specialId:  agg.AggregateMedian,
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
				retType:    agg.GroupConcatReturnType,
				specialId:  agg.AggregateGroupConcat,
			},
		},
	},
}
