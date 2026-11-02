// Copyright 2025 MatrixOrigin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// indicatorPeriodNames are the ten (x, n) indicator kernels; the eleventh
// registered name, cumpositiveStreak, takes a single (x) argument.
var indicatorPeriodNames = []string{
	"sma", "wma", "trima", "ema", "wilder", "kama", "dema", "tema", "t3", "mmaxdrawdown",
}

// TestIndicatorRegistryBits pins the registration class and flag bits of the
// eleven technical-indicator window kernels (issue #29949, design section 5.1):
// window-function WIN_VALUE class, indicator-family member, frame ignoring,
// and not aggregate, not order-only, on the mis-usage axes the optimizer and
// the binder consult.
func TestIndicatorRegistryBits(t *testing.T) {
	all := append(append([]string(nil), indicatorPeriodNames...), "cumpositivestreak")
	for _, name := range all {
		require.True(t, function.GetFunctionIsWinFunByName(name), name)
		require.True(t, function.GetFunctionIsWinValueFunByName(name), name)
		require.True(t, function.GetFunctionIsWinIndicatorFunByName(name), name)
		require.True(t, function.GetFunctionIgnoresWindowFrameByName(name), name)
		require.False(t, function.GetFunctionIsWinOrderFunByName(name), name)
		require.False(t, function.GetFunctionIsAggregateByName(name), name)
	}
	// the flag surface is otherwise unchanged: ordinary aggregates and
	// order-only window functions are not indicators, and the pre-existing
	// frame-ignoring functions keep their behavior.
	require.False(t, function.GetFunctionIsWinIndicatorFunByName("sum"))
	require.False(t, function.GetFunctionIsWinIndicatorFunByName("lag"))
	require.False(t, function.GetFunctionIgnoresWindowFrameByName("row_number"))
	require.True(t, function.GetFunctionIgnoresWindowFrameByName("lag"))
	require.True(t, function.GetFunctionIgnoresWindowFrameByName("lead"))

	// the information_schema view-maintenance helpers keep their own
	// scalar-internal registration identity: an id collision (an indicator
	// registered on their numeric ids) silently swaps their overloads and
	// makes the maintenance SQL fail with a window diagnostic; pinned here
	// after the 603/604 collision was caught by the isolated live startup.
	require.False(t, function.GetFunctionIsWinFunByName("mo_view_definition"))
	require.False(t, function.GetFunctionIsWinFunByName("mo_view_check_option"))
	require.False(t, function.GetFunctionIsWinIndicatorFunByName("mo_view_definition"))
	require.False(t, function.GetFunctionIsWinIndicatorFunByName("mo_view_check_option"))
	require.False(t, function.GetFunctionIsWinIndicatorFunById(function.MO_VIEW_DEFINITION))
	require.False(t, function.GetFunctionIsWinIndicatorFunById(function.MO_VIEW_CHECK_OPTION))
}

// TestIndicatorDeduceNotNullablePins pins the nullability contract of issue
// #29949 design section 4: the ten period kernels emit leading warmup NULLs
// and are never proven not-nullable; cumPositiveStreak is dense and is.
func TestIndicatorDeduceNotNullablePins(t *testing.T) {
	for _, fid := range []int32{
		function.SMA, function.WMA, function.TRIMA, function.EMA, function.WILDER,
		function.KAMA, function.DEMA, function.TEMA, function.T3, function.MMAXDRAWDOWN,
	} {
		require.True(t, function.GetFunctionIsWinIndicatorFunById(fid))
		require.False(t, function.DeduceNotNullable(int64(fid)<<32, nil))
	}
	require.True(t, function.GetFunctionIsWinIndicatorFunById(function.CUMPOSITIVESTREAK))
	require.True(t, function.DeduceNotNullable(int64(function.CUMPOSITIVESTREAK)<<32, nil))
	// also non-indicator ids report false on the indicator surface
	require.False(t, function.GetFunctionIsWinIndicatorFunById(function.SUM))
}

func buildIndicatorPlan(t *testing.T, sql string) error {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	_, err = BuildPlan(ctx, stmt, false)
	return err
}

// TestIndicatorBinderSurface pins the section-4 SQL surface through the
// binder: every entry name resolves as a window function, ORDER BY is
// required with the function-naming diagnostic, explicit frames are
// rejected with the period diagnostic, frame-less calls without OVER report
// the generic window diagnostic instead of the 20105 not-supported error,
// and the constant period domain is enforced exactly at the [1, 1024]
// boundary.
func TestIndicatorBinderSurface(t *testing.T) {
	for _, name := range indicatorPeriodNames {
		require.NoError(t, buildIndicatorPlan(t,
			"select "+name+"(a, 3) over (order by a) from select_test.bind_select"), name)
		require.NoError(t, buildIndicatorPlan(t,
			"select "+name+"(a, 1) over (partition by a order by a) from select_test.bind_select"), name)
	}
	require.NoError(t, buildIndicatorPlan(t,
		"select cumpositivestreak(a) over (order by a) from select_test.bind_select"))

	// ordering is part of the definition
	err := buildIndicatorPlan(t, "select ema(a, 3) over () from select_test.bind_select")
	require.ErrorContains(t, err, "window function 'ema' requires an ORDER BY expression: series order is part of the indicator's definition")

	// explicit frames are never silently ignored
	err = buildIndicatorPlan(t, "select ema(a, 3) over (order by a rows between unbounded preceding and current row) from select_test.bind_select")
	require.ErrorContains(t, err, "window frame is not applicable to ema: the period is given by ema's parameter")
	err = buildIndicatorPlan(t, "select kama(a, 3) over (order by a range between unbounded preceding and current row) from select_test.bind_select")
	require.ErrorContains(t, err, "window frame is not applicable to kama: the period is given by kama's parameter")

	// a call without OVER reports the generic window diagnostic, not the
	// availability error of unregistered names
	err = buildIndicatorPlan(t, "select ema(a, 3) from select_test.bind_select")
	require.ErrorContains(t, err, "Window function 'ema' requires an OVER clause")
	require.NotContains(t, err.Error(), "not supported")

	// the period argument must fold to a constant integer in [1, 1024]
	periodError := "period argument of ema must be a constant integer in [1, 1024]"
	require.NoError(t, buildIndicatorPlan(t, "select ema(a, 1024) over (order by a) from select_test.bind_select"))
	err = buildIndicatorPlan(t, "select ema(a, 1025) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 0) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, -3) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, a) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 1.5) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, null) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)

	// section 4's foldable-integer domain: deterministic constant period
	// expressions are folded once at bind time and validated by their
	// evaluated integer value, at every integer width and through casts
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, abs(3)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, 2+1) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, 1+1*1) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, -(-3)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select sma(a, cast(3 as tinyint)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select sma(a, cast(1000 as smallint)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select wma(a, cast(3 as unsigned)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select wma(a, abs(abs(1+1))) over (order by a) from select_test.bind_select"))
	// folded values outside [1, 1024], non-integer folds, out-of-range
	// narrowing casts and non-constant targets all reject with the same
	// pinned diagnostic
	err = buildIndicatorPlan(t, "select ema(a, abs(1025)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 512*3) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 2-3) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, abs(1.5)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, cast(3 as double)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, cast(1000 as tinyint)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, cast(-1 as unsigned)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, abs(a)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 4/2) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)

	// constant periods fold through the shared deterministic constant
	// expression evaluator, so any deterministic scalar outside the base
	// binder's automatic fold set is admitted by its evaluated integer
	// (review case: LENGTH('abc') and GREATEST(2,3), plus MOD)
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, length('abc')) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select ema(a, greatest(2, 3)) over (order by a) from select_test.bind_select"))
	require.NoError(t, buildIndicatorPlan(t,
		"select sma(a, mod(7, 2)) over (order by a) from select_test.bind_select"))
	// non-constant scalar inputs, evaluated values outside the domain and
	// warning-evaluated constants still reject with the pinned diagnostic
	err = buildIndicatorPlan(t, "select ema(a, length(a)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, greatest(2000, 3)) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)
	err = buildIndicatorPlan(t, "select ema(a, 1/0) over (order by a) from select_test.bind_select")
	require.ErrorContains(t, err, periodError)

	// argument count and type rejections (registry surface)
	require.Error(t, buildIndicatorPlan(t, "select ema(a) over (order by a) from select_test.bind_select"))
	require.Error(t, buildIndicatorPlan(t, "select ema(a, 3, 3) over (order by a) from select_test.bind_select"))
	require.Error(t, buildIndicatorPlan(t, "select ema('text', 3) over (order by a) from select_test.bind_select"))
	require.Error(t, buildIndicatorPlan(t, "select cumpositivestreak(a, 3) over (order by a) from select_test.bind_select"))
	require.Error(t, buildIndicatorPlan(t, "select cumpositivestreak('text') over (order by a) from select_test.bind_select"))
}
