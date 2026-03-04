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

var supportedWindowInNewFramework = []FuncNew{
	{
		functionId: RANK,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "rank",
					aggRegister: agg.RegisterRank,
				},
			},
		},
	},
	{
		functionId: ROW_NUMBER,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "row_number",
					aggRegister: agg.RegisterRowNumber,
				},
			},
		},
	},
	{
		functionId: DENSE_RANK,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "dense_rank",
					aggRegister: agg.RegisterDenseRank,
				},
			},
		},
	},
	{
		functionId: PERCENT_RANK,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "percent_rank",
					aggRegister: agg.RegisterPercentRank,
				},
			},
		},
	},
	{
		functionId: NTILE,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "ntile",
					aggRegister: agg.RegisterNtile,
				},
			},
		},
	},
	{
		functionId: CUME_DIST,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.CumeDistReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "cume_dist",
					aggRegister: agg.RegisterCumeDist,
				},
			},
		},
	},
	// LAG window function
	{
		functionId: LAG,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// LAG(expr) or LAG(expr, offset) or LAG(expr, offset, default)
			if len(inputs) >= 1 && len(inputs) <= 3 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(parameters []types.Type) types.Type {
					// Return type is the same as the first parameter
					if len(parameters) > 0 {
						return parameters[0]
					}
					return types.T_any.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "lag",
					aggRegister: agg.RegisterLag,
				},
			},
		},
	},
	// LEAD window function
	{
		functionId: LEAD,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// LEAD(expr) or LEAD(expr, offset) or LEAD(expr, offset, default)
			if len(inputs) >= 1 && len(inputs) <= 3 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(parameters []types.Type) types.Type {
					// Return type is the same as the first parameter
					if len(parameters) > 0 {
						return parameters[0]
					}
					return types.T_any.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "lead",
					aggRegister: agg.RegisterLead,
				},
			},
		},
	},
	// FIRST_VALUE window function
	{
		functionId: FIRST_VALUE,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(parameters []types.Type) types.Type {
					if len(parameters) > 0 {
						return parameters[0]
					}
					return types.T_any.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "first_value",
					aggRegister: agg.RegisterFirstValue,
				},
			},
		},
	},
	// LAST_VALUE window function
	{
		functionId: LAST_VALUE,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(parameters []types.Type) types.Type {
					if len(parameters) > 0 {
						return parameters[0]
					}
					return types.T_any.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "last_value",
					aggRegister: agg.RegisterLastValue,
				},
			},
		},
	},
	// NTH_VALUE window function
	{
		functionId: NTH_VALUE,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// NTH_VALUE(expr, n)
			if len(inputs) == 2 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(parameters []types.Type) types.Type {
					if len(parameters) > 0 {
						return parameters[0]
					}
					return types.T_any.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "nth_value",
					aggRegister: agg.RegisterNthValue,
				},
			},
		},
	},
}
