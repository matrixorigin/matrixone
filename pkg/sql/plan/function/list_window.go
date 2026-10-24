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
				aggName:    "rank",
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
				aggName:    "row_number",
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
				aggName:    "dense_rank",
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
				aggName: "percent_rank",
			},
		},
	},
	{
		functionId: NTILE,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 && inputs[0].Oid.IsInteger() {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.NtileReturnType,
				aggName:    "ntile",
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
				aggName:    "cume_dist",
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
				return checkValueWindowDomain(inputs)
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
				aggName: "lag",
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
				return checkValueWindowDomain(inputs)
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
				aggName: "lead",
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
				return checkValueWindowDomain(inputs)
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
				aggName: "first_value",
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
				return checkValueWindowDomain(inputs)
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
				aggName: "last_value",
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
				return checkValueWindowDomain(inputs)
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
				aggName: "nth_value",
			},
		},
	},
	// MRANK window function (issue #29947): 1-based ascending competition
	// rank of the current non-NULL value among the last N physical rows.
	{
		functionId: MRANK,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// MRANK(expr, n): the value domain followed by an INTEGER row
			// count. A non-integer count (1.5, string, temporal) is a bind
			// error, mirroring NTILE's bucket count; the count's VALUE
			// (>= 1, non-NULL) is an execution-time check named to mrank.
			if len(inputs) == 2 && IsLocalRankValueDomain(&inputs[0]) && inputs[1].Oid.IsInteger() {
				return checkValueWindowDomain(inputs)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggName:    "mrank",
			},
		},
	},
	// CUMRANK window function (issue #29947): cumulative competition rank
	// from the partition start through the current row.
	{
		functionId: CUMRANK,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 && IsLocalRankValueDomain(&inputs[0]) {
				return checkValueWindowDomain(inputs)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggName:    "cumrank",
			},
		},
	},
	// TMRANK window function (issue #29947): time-bounded trailing
	// competition rank over RANGE-interval membership. The catalog registers
	// only the INTERNAL three-argument shape TMRANK(v, count, unit) the
	// binder's conversion case publishes: the public two-argument form
	// TMRANK(v, INTERVAL n unit) is rewritten during binding (like date_add)
	// and is never resolved here. The count slot is the window-normalized
	// (possibly null-marked) int64 constant or the retained direct prepared
	// magnitude source; the unit slot is the normalized int64 unit code or
	// the raw unit-string literal. No implicit cast is inserted on either
	// slot: the count slot's domain must reach the operator for the
	// string-vs-numeric window grammar dispatch, and the unit slot's literal
	// kind marks raw versus normalized.
	{
		functionId: TMRANK,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 3 && IsLocalRankValueDomain(&inputs[0]) &&
				(inputs[2].Oid == types.T_int64 || inputs[2].Oid.IsMySQLString()) {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggName:    "tmrank",
			},
		},
	},
}

// IsLocalRankValueDomain reports whether a v argument of the local
// sliding-window rank family is comparable by the frame kernel: the numeric
// and temporal domains of types.IsNumericOrTemporal(). Strings stay out of
// scope pending the collation decision (#29947 decision D3) and produce a
// bind-time invalid-argument error.
func IsLocalRankValueDomain(t *types.Type) bool {
	return t.IsNumericOrTemporal()
}

// checkValueWindowDomain gives domainless NULL the same concrete transport
// type as SELECT NULL. Typed inputs retain their complete declared type.
func checkValueWindowDomain(inputs []types.Type) checkResult {
	if inputs[0].Oid == types.T_any {
		targets := append([]types.Type(nil), inputs...)
		targets[0] = types.T_text.ToType()
		return newCheckResultWithCast(0, targets)
	}
	return newCheckResultWithSuccess(0)
}
