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
	// Technical-indicator window kernels (issue #29949; docs/design/
	// technical_indicator_window_kernels.md, section 4). WIN_VALUE-class
	// registrations evaluated by the window operator's indicator state
	// machine: no aggexec kernel and no aggName. The window binder enforces
	// the section-4 OVER surface (required ORDER BY, explicit-frame
	// rejection, constant-integer period in [1, 1024]); argument domains are
	// shaped here and values are row-admitted at execution.
	{
		functionId: SMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: WMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: TRIMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: EMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: WILDER,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: KAMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: DEMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: TEMA,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: T3,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: MMAXDRAWDOWN,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorValueArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_float64.ToType()
				},
			},
		},
	},
	{
		functionId: CUMPOSITIVESTREAK,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkIndicatorStreakArgs,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType: func(_ []types.Type) types.Type {
					return types.T_int64.ToType()
				},
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

	// EWM_MEAN window function -- exponentially weighted mean.
	{
		functionId: EWM_MEAN,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    ewmMeanFamilyTypeCheck,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    ewmWindowReturnType,
				aggName:    "ewmmean",
			},
		},
	},
	// EWM_VAR window function -- exponentially weighted variance.
	{
		functionId: EWM_VAR,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    ewmMeanFamilyTypeCheck,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    ewmWindowReturnType,
				aggName:    "ewmvar",
			},
		},
	},
	// EWM_STD window function -- exponentially weighted standard deviation.
	{
		functionId: EWM_STD,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    ewmMeanFamilyTypeCheck,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    ewmWindowReturnType,
				aggName:    "ewmstd",
			},
		},
	},
	// EWM_COV window function -- exponentially weighted covariance.
	{
		functionId: EWM_COV,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    ewmCovFamilyTypeCheck,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    ewmWindowReturnType,
				aggName:    "ewmcov",
			},
		},
	},
	// EWM_CORR window function -- exponentially weighted correlation.
	{
		functionId: EWM_CORR,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn:    ewmCovFamilyTypeCheck,
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    ewmWindowReturnType,
				aggName:    "ewmcorr",
			},
		},
	},
}

// checkIndicatorValueArgs shapes the (x, n) arguments of the ten period
// indicator kernels: a numeric series argument and an integer period.
// checkIndicatorValueArgs admits exactly the two-argument indicator shapes
// down the window binder's section-4 validation. Value and period domains
// are decided there with the pinned diagnostics ("argument 1 of <fn> must
// be numeric", "period argument of <fn> must be a constant integer in
// [1, 1024]"); a type-shaped rejection here would surface the generic
// function-parameters error instead and shadow them. The executor mirrors
// both domains per row (indicatorRowValue, validateIndicatorPeriods).
func checkIndicatorValueArgs(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 2 {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// checkIndicatorStreakArgs shapes the single (x) argument of
// cumPositiveStreak; the numeric domain is decided by the window binder's
// section-4 validation and mirrored per row by indicatorRowValue.
func checkIndicatorStreakArgs(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// IsLocalRankValueDomain reports whether a v argument of the local
// sliding-window rank family is comparable by the frame kernel: the numeric
// and temporal domains of types.IsNumericOrTemporal(). Strings stay out of
// scope pending the collation decision (#29947 decision D3) and produce a
// bind-time invalid-argument error.
func IsLocalRankValueDomain(t *types.Type) bool {
	return t.IsNumericOrTemporal()
}

// ewmWindowReturnType: every ewm statistic emits float64 (§4).
func ewmWindowReturnType(_ []types.Type) types.Type {
	return types.T_float64.ToType()
}

// ewmMeanFamilyTypeCheck enforces the §4 signature table for
// ewmMean/ewmVar/ewmStd: arg1 the sample (numeric, cast to float64), arg2
// the decay (numeric alpha or the 'halflife' string tag with h at arg3),
// then up to three constant policies (adjust, ignoreNA, minPeriods).
// Arity: 2..5 with a numeric decay, 3..6 with the tag. Prepared markers
// (T_any) pass through; semantic values are validated by the binder
// (literals) and at the per-execution configuration boundary.
func ewmMeanFamilyTypeCheck(overloads []overload, inputs []types.Type) checkResult {
	return ewmTypeCheck(inputs, 1)
}

// ewmCovFamilyTypeCheck enforces the §4 signature table for
// ewmCov/ewmCorr: arg1 x, arg2 y (numeric, cast to float64), decay at
// arg3. Arity: 3..6 with a numeric decay, 4..7 with the tag.
func ewmCovFamilyTypeCheck(overloads []overload, inputs []types.Type) checkResult {
	return ewmTypeCheck(inputs, 2)
}

// ewmNumericArgDomain classifies the numeric domains an ewm argument may
// carry through the bind-established arg cast to float64 (§4): every
// alpha, sample and tag-form half-life domain shares one classification,
// with decimal256 included (§8 limitation 3 — if the shared cast path
// rejects decimal256 anywhere, escalate, do not narrow silently).
func ewmNumericArgDomain(oid types.T) bool {
	switch oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64,
		types.T_decimal128, types.T_decimal256:
		return true
	}
	return false
}

func ewmTypeCheck(inputs []types.Type, valueArgs int) checkResult {
	if len(inputs) <= valueArgs {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	finalTypes := append([]types.Type(nil), inputs...)
	needCast := false
	// value arguments: numeric samples, lowered to float64 (§4 cast rule).
	// A domainless NULL sample binds as the T_any literal and receives
	// the same float64 NULL transport, so the NULL-policy rows of §5/§6
	// publish instead of failing the dispatch's sample-transport check.
	for i := 0; i < valueArgs; i++ {
		if inputs[i].Oid == types.T_any {
			finalTypes[i] = types.T_float64.ToType()
			needCast = true
			continue
		}
		if !ewmNumericArgDomain(inputs[i].Oid) {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		finalTypes[i] = types.T_float64.ToType()
		needCast = true
	}
	// decay position: numeric alpha, the 'halflife' tag, or a prepare-time
	// marker (T_text/T_any) whose resolved kind — alpha, tag, or the
	// policies behind it — is validated at every execution boundary
	// (§4; §9 Invariant 12)
	hAfter := 0
	switch inputs[valueArgs].Oid {
	case types.T_any:
	case types.T_text:
		// placeholder may stand alone; a numeric or marker position right
		// behind it is the tag-form half-life (the same shared numeric
		// classification as the literal tag form), everything else defers
		// to the per-execution boundary
		if len(inputs) > valueArgs+1 {
			h := inputs[valueArgs+1].Oid
			if h == types.T_text || ewmNumericArgDomain(h) {
				hAfter = 1
			}
		}
	case types.T_varchar, types.T_char:
		hAfter = 1
		if len(inputs) <= valueArgs+1 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		h := inputs[valueArgs+1].Oid
		if h != types.T_any && h != types.T_text && !ewmNumericArgDomain(h) {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	default:
		// numeric alpha decay shares the same classification
		if !ewmNumericArgDomain(inputs[valueArgs].Oid) {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	// policy positions: adjust/ignoreNA boolean, minPeriods integer
	firstPolicy := valueArgs + 1 + hAfter
	lastPolicy := firstPolicy + 2
	if len(inputs) > lastPolicy+1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	for i := firstPolicy; i < len(inputs); i++ {
		switch inputs[i].Oid {
		case types.T_any, types.T_text: // prepare-time markers resolve at execution
		case types.T_bool, types.T_int8:
			// adjust / ignoreNA
		case types.T_int16, types.T_int32, types.T_int64,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_float32, types.T_float64, types.T_decimal64,
			types.T_decimal128, types.T_decimal256:
			// permit an integer-valued constant expression to flow; the
			// per-execution boundary enforces the resolved kinds' values
		default:
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	if needCast {
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithSuccess(0)
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
