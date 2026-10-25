// Copyright 2026 Matrix Origin
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

package plan

// Binder-level proof for the local sliding-window ranks of issue #29947: the
// published plan shapes (D18 raw/normalized publication), the D14/D20 width
// normalization matrix through real SQL, the D21 ORDER BY key domain, the D4
// explicit-frame rejection, the D22 admission boundary (control functions,
// non-foldable and indirect prepared sources) and the prepared-marker
// re-materialization (D19/D16 owners).

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// findLocalRankWindowSpec walks window-spec lists (PlanFunctionExpr covers
// project/filter/agg lists but not WinSpecList) and returns the bound
// WindowSpec of a local rank function, or nil.
func findLocalRankWindowSpec(queryPlan *planpb.Plan, name string) *planpb.WindowSpec {
	var find func(*planpb.Expr) *planpb.WindowSpec
	find = func(expr *planpb.Expr) *planpb.WindowSpec {
		if expr == nil {
			return nil
		}
		if w := expr.GetW(); w != nil {
			if w.Name == name {
				return w
			}
			if found := find(w.WindowFunc); found != nil {
				return found
			}
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		return nil
	}
	query := queryPlan.GetQuery()
	if query == nil {
		return nil
	}
	for _, node := range query.Nodes {
		for _, spec := range node.WinSpecList {
			if found := find(spec); found != nil {
				return found
			}
		}
	}
	return nil
}

func bindLocalRankSQL(t *testing.T, sql string) (*planpb.Plan, error) {
	return runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
}

// bindTmRankWidthSQL binds `tmrank(o_totalprice, <width>) OVER (ORDER BY
// o_orderdate)` and returns the window function's published arguments, or the
// bind error.
func bindTmRankWidthSQL(t *testing.T, widthSQL string) ([]*planpb.Expr, error) {
	pn, err := bindLocalRankSQL(t,
		"select tmrank(o_totalprice, "+widthSQL+") over (order by o_orderdate) from orders")
	if err != nil {
		return nil, err
	}
	spec := findLocalRankWindowSpec(pn, "tmrank")
	require.NotNil(t, spec, pn.String())
	return spec.GetWindowFunc().GetF().GetArgs(), nil
}

// TestLocalRankBindPublishesInternalShape pins the bound shapes: the value
// argument stays a column reference, mrank's width stays its integer literal,
// tmrank's INTERVAL width is converted to the internal (count, unit-code)
// pair, every spec stays frameless, and the return type is the shared
// SingleWindowReturnType (uint64).
func TestLocalRankBindPublishesInternalShape(t *testing.T) {
	t.Run("mrank", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"select mrank(n_nationkey, 3) over (order by n_nationkey) from nation")
		require.NoError(t, err)
		spec := findLocalRankWindowSpec(pn, "mrank")
		require.NotNil(t, spec, pn.String())
		require.Nil(t, spec.Frame)
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Len(t, args, 2)
		require.NotNil(t, args[0].GetCol(), spec.String())
		require.Equal(t, int32(types.T_int64), args[1].Typ.Id)
		require.Equal(t, int64(3), args[1].GetLit().GetI64Val())
		require.Equal(t, int32(types.T_uint64), spec.GetWindowFunc().Typ.Id)
	})
	t.Run("cumrank with partition", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"select cumrank(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation")
		require.NoError(t, err)
		spec := findLocalRankWindowSpec(pn, "cumrank")
		require.NotNil(t, spec, pn.String())
		require.Nil(t, spec.Frame)
		require.Len(t, spec.PartitionBy, 1)
		require.Len(t, spec.GetWindowFunc().GetF().GetArgs(), 1)
	})
	t.Run("tmrank converts the INTERVAL width", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"select tmrank(o_totalprice, interval 3 day) over (order by o_orderdate) from orders")
		require.NoError(t, err)
		spec := findLocalRankWindowSpec(pn, "tmrank")
		require.NotNil(t, spec, pn.String())
		require.Nil(t, spec.Frame)
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Len(t, args, 3, spec.String())
		require.NotNil(t, args[0].GetCol(), spec.String())
		require.Equal(t, int32(types.T_int64), args[1].Typ.Id)
		require.Equal(t, int64(3), args[1].GetLit().GetI64Val())
		require.Equal(t, int32(types.T_int64), args[2].Typ.Id)
		require.Equal(t, int64(types.Day), args[2].GetLit().GetI64Val())
		require.Equal(t, int32(types.T_uint64), spec.GetWindowFunc().Typ.Id)
	})
}

// TestLocalRankBindTmRankWidthMatrix pins the width normalization matrix
// through real SQL (D14/D15/D20): string versus numeric magnitudes, foldable
// ripple with the shared int64 CAST rounding, the zero-width boundary, the
// NULL-literal rejection and the compound string's NULL-arithmetic outcome.
func TestLocalRankBindTmRankWidthMatrix(t *testing.T) {
	requireWidth := func(t *testing.T, widthSQL string, wantCount int64, wantUnit types.IntervalType) {
		args, err := bindTmRankWidthSQL(t, widthSQL)
		require.NoError(t, err, widthSQL)
		require.Len(t, args, 3)
		require.NotNil(t, args[1].GetLit(), widthSQL)
		require.Equal(t, wantCount, args[1].GetLit().GetI64Val(), widthSQL)
		require.Equal(t, int64(wantUnit), args[2].GetLit().GetI64Val(), widthSQL)
	}
	requireReject := func(t *testing.T, widthSQL, errText string) {
		args, err := bindTmRankWidthSQL(t, widthSQL)
		require.Error(t, err, widthSQL)
		require.Nil(t, args)
		require.Contains(t, err.Error(), errText, widthSQL)
	}

	requireWidth(t, "interval 3 day", 3, types.Day)
	// The MySQL-string grammar keeps '1.5' DAY integral at one day (D20).
	requireWidth(t, "interval '1.5' day", 1, types.Day)
	// A numeric decimal magnitude quantizes to microseconds (1.5 days).
	requireWidth(t, "interval 1.5 day", 129600000000, types.MicroSecond)
	// A foldable spine evaluates through the shared int64 CAST: sin(1) day
	// rounds to a 1-day width; an explicitly INT-typed negative-then-rounded
	// magnitude (CAST 0-sin(9) AS SIGNED rounds -0.41 to 0) publishes the
	// accepted zero-width control.
	requireWidth(t, "interval sin(1) day", 1, types.Day)
	requireWidth(t, "interval cast(0 - sin(9) as signed) day", 0, types.Day)

	// The compound string's fractional second keeps the NULL-arithmetic
	// publication: a null-marked count rides with a valid unit code.
	args, err := bindTmRankWidthSQL(t, "interval '00:01.5' minute_second")
	require.NoError(t, err)
	require.Len(t, args, 3)
	require.NotNil(t, args[1].GetLit())
	require.True(t, args[1].GetLit().Isnull, "compound fraction must null-mark the count")
	unitCode := args[2].GetLit().GetI64Val()
	require.Greater(t, unitCode, int64(types.IntervalTypeInvalid))
	require.Less(t, unitCode, int64(types.IntervalTypeMax))

	// 0-SIN(1) is a negative float fold and is refused with the shared
	// frame-bound illegal-width error (the reviewer-approved witness). The
	// same-nature 0-SIN(9) fold is negative pre-rounding as well: the
	// fast-path sign check fires before any rounding, so it shares the
	// negative rejection; only the explicit INT cast above realizes the
	// zero-width acceptance.
	requireReject(t, "interval (0 - sin(1)) day", "negative, NULL or of non-integral type")
	requireReject(t, "interval (0 - sin(9)) day", "negative, NULL or of non-integral type")
	// A literal NULL magnitude is refused outright.
	requireReject(t, "interval null day", "negative, NULL or of non-integral type")
}

// TestLocalRankBindTmRankAdmissionBoundary pins the D22 static admission
// walk through real SQL: column-reference magnitudes, indirect prepared
// forms and volatile or effectful function sources (rand/sleep, direct or
// nested -- a literal argument shape does not make them constant-foldable)
// never reach the eager evaluation, while the registry-proven pure control
// keeps folding.
func TestLocalRankBindTmRankAdmissionBoundary(t *testing.T) {
	requireReject := func(t *testing.T, widthSQL, errText string) {
		args, err := bindTmRankWidthSQL(t, widthSQL)
		require.Error(t, err, widthSQL)
		require.Nil(t, args)
		require.Contains(t, err.Error(), errText, widthSQL)
	}
	requireReject(t,
		"interval o_orderkey day",
		"a constant, foldable expression or direct parameter")
	requireReject(t,
		"interval (3 + o_orderkey) day",
		"a constant, foldable expression or direct parameter")
	requireReject(t,
		"interval rand() day",
		"a constant, foldable expression or direct parameter")
	requireReject(t,
		"interval sleep(0) day",
		"a constant, foldable expression or direct parameter")
	requireReject(t,
		"interval (1 + rand()) day",
		"a constant, foldable expression or direct parameter")

	// Positive control: the registry-proven pure spine still folds through the
	// shared pipeline (the prepared marker's raw retention keeps its own
	// dedicated pin in TestLocalRankBindPreparedWidth).
	args, err := bindTmRankWidthSQL(t, "interval sin(1) day")
	require.NoError(t, err)
	require.Len(t, args, 3)
	require.Equal(t, int64(1), args[1].GetLit().GetI64Val())
}

// TestLocalRankBindTmRankMoCtlRejected pins the D22 tier-1 boundary through
// real SQL: a control function in tmrank's width — direct or nested — is
// refused with the frame-bound gate's exact message, and the frame-bound
// route still produces the identical surface (message-parity between the two
// routes of the shared owner).
func TestLocalRankBindTmRankMoCtlRejected(t *testing.T) {
	bindErr := func(t *testing.T, sql string) error {
		pn, err := bindLocalRankSQL(t, sql)
		require.Nil(t, pn)
		return err
	}
	direct := bindErr(t,
		"select tmrank(o_totalprice, interval mo_ctl('cn','Task','health:time') day) "+
			"over (order by o_orderdate) from orders")
	nested := bindErr(t,
		"select tmrank(o_totalprice, interval (3 + mo_ctl('cn','Task','health:time')) day) "+
			"over (order by o_orderdate) from orders")
	frameBound := bindErr(t,
		"select sum(o_totalprice) over (order by o_orderdate "+
			"range interval mo_ctl('cn','Task','health:time') day preceding) from orders")
	for _, err := range []error{direct, nested, frameBound} {
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%s", err)
		require.Contains(t, err.Error(), "mo_ctl or fault_inject is not allowed in a window frame bound")
	}
	// The two routes share one owner, so their messages are identical.
	require.Equal(t, direct.Error(), frameBound.Error())
}

// TestLocalRankBindTmRankOrderKeyDomain pins the D21 ORDER BY key domain: one
// temporal key of DATE/TIME/DATETIME/TIMESTAMP; integer or varchar keys, two
// keys, and the frameless empty ORDER BY are refused with the named
// diagnostic; a partitioned tmrank with the valid key still binds.
func TestLocalRankBindTmRankOrderKeyDomain(t *testing.T) {
	bindTM := func(t *testing.T, overSQL string) error {
		pn, err := bindLocalRankSQL(t,
			"select tmrank(o_totalprice, interval 3 day) over "+overSQL+" from orders")
		if err != nil {
			return err
		}
		require.NotNil(t, findLocalRankWindowSpec(pn, "tmrank"), pn.String())
		return nil
	}
	require.NoError(t, bindTM(t, "(order by o_orderdate)"))
	require.NoError(t, bindTM(t, "(partition by o_custkey order by o_orderdate)"))

	for _, over := range []string{
		"(order by o_orderkey)",
		"(order by o_orderdate, o_orderkey)",
		"(order by o_orderstatus)",
		"()",
	} {
		err := bindTM(t, over)
		require.Error(t, err, over)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError), "%s: %s", over, err)
		require.Contains(t, err.Error(),
			"tmrank window order key requires exactly one ORDER BY expression of DATE, TIME, DATETIME or TIMESTAMP type",
			over)
	}
}

// TestLocalRankBindFrameRejection pins D4: an explicit frame on any of the
// three operators is refused (their frames come from the arguments).
func TestLocalRankBindFrameRejection(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{name: "mrank rows frame", sql: "select mrank(n_nationkey, 3) over (order by n_nationkey rows 2 preceding) from nation"},
		{name: "cumrank range frame", sql: "select cumrank(n_nationkey) over (order by n_nationkey range between unbounded preceding and current row) from nation"},
		{name: "tmrank rows frame", sql: "select tmrank(o_totalprice, interval 3 day) over (order by o_orderdate rows unbounded preceding) from orders"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pn, err := bindLocalRankSQL(t, tc.sql)
			require.Error(t, err, tc.sql)
			require.Nil(t, pn)
			require.Contains(t, err.Error(), "window comes from its arguments and does not accept an explicit frame")
		})
	}
}

// TestLocalRankBindArityAndDomain pins the catalog gates: the value domain is
// numeric or temporal, the width is an INTEGER, arities are exact, and the
// public three-argument tmrank spelling (the internal shape) is refused.
func TestLocalRankBindArityAndDomain(t *testing.T) {
	requireBindFails := func(t *testing.T, sql, errText string) {
		pn, err := bindLocalRankSQL(t, sql)
		require.Error(t, err, sql)
		require.Nil(t, pn)
		require.Contains(t, err.Error(), errText, sql)
	}
	// The grammar productions fix the argument shapes (two for mrank, one for
	// cumrank): wrong-count spellings are parse rejections pinned by the
	// parser package's window_local_rank_parser_test.go. The catalog gates
	// own every shape-valid rejection below.
	requireBindFails(t, "select mrank(n_nationkey, 1.5) over (order by n_nationkey) from nation", "function mrank")
	requireBindFails(t, "select mrank(n_nationkey, '3') over (order by n_nationkey) from nation", "function mrank")
	requireBindFails(t, "select mrank(n_name, 3) over (order by n_nationkey) from nation", "function mrank")
	requireBindFails(t, "select cumrank(n_name) over (order by n_nationkey) from nation", "function cumrank")

	// The grammar mandates tmrank's INTERVAL width, so every direct wrong-shape
	// spelling is a parse rejection pinned by the parser package's
	// TestLocalRankWindowArityRejectedAtParse. The binder-level conversion
	// refusal is the re-bind defense (a raw shape can reach the binder with
	// types already bound), so pin it through the direct binder call the
	// find_in_set internal-shape test established.
	_, err := bindFuncExprImplByPlanExpr(context.Background(), nil, "tmrank",
		[]*Expr{lit64(1), lit64(3)}, false, nil, nil, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tmrank function needs one value argument and one INTERVAL width")
	_, err = bindFuncExprImplByPlanExpr(context.Background(), nil, "tmrank",
		[]*Expr{lit64(1), lit64(3), lit64(5)}, false, nil, nil, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tmrank function needs one value argument and one INTERVAL width")

	// Positive domain coverage: typed-NULL value, decimal value and timestamp
	// ORDER BY keys; the width argument may be any INTEGER type.
	for _, sql := range []string{
		"select mrank(cast(null as double), 3) over (order by n_nationkey) from nation",
		"select cumrank(o_totalprice) over (order by o_orderdate) from orders",
		"select tmrank(o_orderdate, interval 3 day) over (order by o_orderdate) from orders",
	} {
		pn, err := bindLocalRankSQL(t, sql)
		require.NoError(t, err, sql)
		require.NotNil(t, pn)
	}
}

// TestLocalRankBindWindowIntervalSourceAdmitted pins the static shape-walk of
// the admission boundary (tier 2): literal constants, the direct prepared
// marker and deterministic foldable spines are admitted; column references,
// markers hidden inside arithmetic, volatile functions and any other shape
// are not.
func TestLocalRankBindWindowIntervalSourceAdmitted(t *testing.T) {
	col := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	marker := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	fold := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: args,
		}}}
	}
	// bindFunctionNode resolves one call through the real overload resolver,
	// so the walk sees exactly the node shape production binding produces
	// (function id, overload and any cast/target-type wrapping included).
	bindFunctionNode := func(name string, args ...*planpb.Expr) *planpb.Expr {
		t.Helper()
		bound, err := bindFuncExprImplByPlanExpr(context.Background(), nil, name,
			args, false, nil, nil, false, nil)
		require.NoError(t, err, name)
		return bound
	}

	require.True(t, windowIntervalBindSourceAdmitted(lit64(3)))
	require.True(t, windowIntervalBindSourceAdmitted(marker))
	require.True(t, windowIntervalBindSourceAdmitted(bindFunctionNode("sin", lit64(1))))
	require.True(t, windowIntervalBindSourceAdmitted(bindFunctionNode("+", lit64(1), lit64(2))))
	require.False(t, windowIntervalBindSourceAdmitted(col))
	require.False(t, windowIntervalBindSourceAdmitted(fold("sin", col)))
	require.False(t, windowIntervalBindSourceAdmitted(fold("+", lit64(1), marker)))
	require.False(t, windowIntervalBindSourceAdmitted(fold("+", lit64(1), col)))
	// Volatile and effectful functions are refused regardless of their
	// foldable-looking argument shapes: rand() takes no argument at all and
	// sleep(0) takes a literal, yet neither is a constant-foldable spine, so
	// refusing them keeps the eager bind-time evaluation deterministic and
	// side-effect free -- also when the volatile source is nested inside
	// admitted fold-switch arithmetic.
	randNode := bindFunctionNode("rand")
	require.False(t, windowIntervalBindSourceAdmitted(randNode))
	require.False(t, windowIntervalBindSourceAdmitted(bindFunctionNode("sleep", lit64(0))))
	require.False(t, windowIntervalBindSourceAdmitted(
		bindFunctionNode("+", lit64(1), randNode)))
	require.False(t, windowIntervalBindSourceAdmitted(&planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{lit64(1)}}},
	}))
}

// TestLocalRankBindTmRankWidthNilProcSafety pins the tier-1 boundary and the
// admission walk running BEFORE any evaluation: bindTmRankWidthArgument and
// resetWindowIntervalExpr reject a control function with a nil proc (the nil
// proc would fault the eager evaluator, so only a pre-evaluation refusal can
// return cleanly), mirroring window_binder_moctl_test.go's pattern.
func TestLocalRankBindTmRankWidthNilProcSafety(t *testing.T) {
	ctrl := func(name string) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: []*planpb.Expr{{
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "cn"}}},
			}},
		}}}
	}
	intervalOf := func(source *planpb.Expr) *Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_interval)},
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
				source,
				{Typ: planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "day"}}}},
			}}},
		}
	}
	ctx := context.Background()

	for _, name := range []string{"mo_ctl", "fault_inject"} {
		// Direct and nested control calls are refused before evaluation.
		for _, source := range []*planpb.Expr{
			ctrl(name),
			{Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "+"},
				Args: []*planpb.Expr{
					{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
					ctrl(name),
				},
			}}},
		} {
			_, _, err := bindTmRankWidthArgument(ctx, nil, intervalOf(source), "tmrank")
			require.Error(t, err, name)
			require.Contains(t, err.Error(), "not allowed in a window frame bound")

			_, err = resetWindowIntervalExpr(ctx, nil, intervalOf(source))
			require.Error(t, err, name)
			require.Contains(t, err.Error(), "not allowed in a window frame bound")
		}
	}

	// The static walk also refuses before evaluation: a column-reference
	// magnitude and a volatile magnitude both never reach the nil proc.
	col := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	_, _, err := bindTmRankWidthArgument(ctx, nil, intervalOf(col), "tmrank")
	require.Error(t, err)
	require.Contains(t, err.Error(), "a constant, foldable expression or direct parameter")

	boundFuncExpr := func(name string, args ...*planpb.Expr) *planpb.Expr {
		t.Helper()
		bound, bErr := bindFuncExprImplByPlanExpr(ctx, nil, name, args,
			false, nil, nil, false, nil)
		require.NoError(t, bErr, name)
		return bound
	}
	for _, source := range []*planpb.Expr{
		boundFuncExpr("rand"),
		boundFuncExpr("sleep", lit64(0)),
	} {
		_, _, err := bindTmRankWidthArgument(ctx, nil, intervalOf(source), "tmrank")
		require.Error(t, err)
		require.Contains(t, err.Error(),
			"a constant, foldable expression or direct parameter")
	}

	// An admitted literal source reaches the proc==nil guard: the const-path
	// carries the named "must be a constant" diagnostic rather than crashing.
	lit := lit64(3)
	_, _, err = bindTmRankWidthArgument(ctx, nil, intervalOf(lit), "tmrank")
	require.Error(t, err)
	require.Contains(t, err.Error(), "interval width must be a constant")
}

// lit64 builds a non-NULL int64 literal for the admission-boundary probes.
func lit64(v int64) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: v}}},
	}
}

// TestLocalRankBindPreparedWidth pins D18's raw publication and D19's
// re-materialization for a direct prepared magnitude: at PREPARE the retained
// raw pair travels as (marker, unit-string literal); after EXECUTE-parameter
// specialization the saved plan carries the re-materialized normalized pair,
// and the window spec still has no frame.
func TestLocalRankBindPreparedWidth(t *testing.T) {
	ctx := context.Background()
	prepared, err := bindLocalRankSQL(t,
		"prepare stmt_tmrank from "+
			"select tmrank(o_totalprice, interval ? day) over (order by o_orderdate) from orders")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().GetPlan()
	require.NotNil(t, preparedPlan)

	spec := findLocalRankWindowSpec(preparedPlan, "tmrank")
	require.NotNil(t, spec, preparedPlan.String())
	require.Nil(t, spec.Frame)
	args := spec.GetWindowFunc().GetF().GetArgs()
	require.Len(t, args, 3, spec.String())
	require.Equal(t, int32(types.T_decimal64), args[0].Typ.Id, "value argument type")
	require.NotNil(t, args[1].GetP(), "prepared magnitude must stay the raw marker")
	require.True(t, types.T(args[2].Typ.Id).IsMySQLString(), "raw unit literal stays a string")

	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
		preparedPlan, []any{ParamValue{
			Value:         "3",
			SourceType:    types.T_int64.ToType(),
			HasSourceType: true,
		}})
	require.NoError(t, err)
	filledSpec := findLocalRankWindowSpec(filled, "tmrank")
	require.NotNil(t, filledSpec, filled.String())
	require.Nil(t, filledSpec.Frame)
	fargs := filledSpec.GetWindowFunc().GetF().GetArgs()
	require.Len(t, fargs, 3, filledSpec.String())
	require.Equal(t, int32(types.T_int64), fargs[1].Typ.Id)
	require.Equal(t, int64(3), fargs[1].GetLit().GetI64Val())
	// The fill re-materializes the magnitude but keeps the retained raw pair:
	// the unit literal stays the raw MySQL-string form the operator's
	// materialization adapter consumes (decision D18).
	require.True(t, types.T(fargs[2].Typ.Id).IsMySQLString(), filledSpec.String())
	require.NotEmpty(t, fargs[2].GetLit().GetSval())
}

// TestLocalRankPreparedFillDistinguishesTypedWidths pins D19's source-domain
// reconstruction for the two spelling families the pre-PR review noted were
// unpinned: a DECIMAL parameter keeps the NUMERIC domain (the 1.5 DAY
// magnitude quantizes to 129600000000 MICROSECONDS, thirty hours) while a
// VARCHAR parameter keeps the MySQL string grammar (the integral prefix
// parse reads '1.5' DAY as one DAY), and the shared normalization observes
// both magnitudes from the retained raw pair.
func TestLocalRankPreparedFillDistinguishesTypedWidths(t *testing.T) {
	ctx := context.Background()

	fill := func(source types.Type) *planpb.WindowSpec {
		prepared, err := bindLocalRankSQL(t,
			"prepare stmt_tmrank from "+
				"select tmrank(o_totalprice, interval ? day) over (order by o_orderdate) from orders")
		require.NoError(t, err)
		preparedPlan := prepared.GetDcl().GetPrepare().GetPlan()
		require.NotNil(t, preparedPlan)

		filled, _, fillErr := FillValuesOfParamsInPlanWithSpecialization(ctx,
			preparedPlan, []any{ParamValue{
				Value:         "1.5",
				SourceType:    source,
				HasSourceType: true,
			}})
		require.NoError(t, fillErr)
		spec := findLocalRankWindowSpec(filled, "tmrank")
		require.NotNil(t, spec, filled.String())
		return spec
	}

	// The fill rebuilds the count slot in its source domain: DECIMAL for the
	// numeric parameter, VARCHAR for the text parameter, each with the
	// retained raw MySQL-string unit literal (D18).
	numSpec := fill(types.T_decimal64.ToType())
	numArgs := numSpec.GetWindowFunc().GetF().GetArgs()
	require.Len(t, numArgs, 3, numSpec.String())
	require.Equal(t, int32(types.T_decimal64), numArgs[1].Typ.Id, numSpec.String())
	require.True(t, types.T(numArgs[2].Typ.Id).IsMySQLString(), numSpec.String())

	textSpec := fill(types.T_varchar.ToType())
	textArgs := textSpec.GetWindowFunc().GetF().GetArgs()
	require.Len(t, textArgs, 3, textSpec.String())
	require.Equal(t, int32(types.T_varchar), textArgs[1].Typ.Id, textSpec.String())
	require.True(t, types.T(textArgs[2].Typ.Id).IsMySQLString(), textSpec.String())

	// The binding owner distinguishes the two reconstructed source domains
	// exactly as it distinguishes the constant spellings: the DECIMAL source
	// quantizes through the MICROSECOND count (1.5 DAY = 129600000000 us,
	// thirty-six hours), the VARCHAR source takes MySQL's integral prefix (1 DAY).
	decBits, decErr := types.Decimal64FromFloat64(1.5, 10, 1)
	require.NoError(t, decErr)
	rawPair := func(source *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_interval)},
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{source,
				{Typ: planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
						Value: &planpb.Literal_Sval{Sval: "day"}}}},
			}}},
		}
	}
	decSource := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_decimal64), Scale: 1},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: int64(decBits)}}}},
	}
	normalized, normalizeErr := resetWindowIntervalExpr(ctx, nil, rawPair(decSource))
	require.NoError(t, normalizeErr)
	numList := normalized.GetList().List
	require.Equal(t, int32(types.T_int64), numList[0].Typ.Id, normalized.String())
	require.Equal(t, int64(129600000000), numList[0].GetLit().GetI64Val(), normalized.String())
	require.Equal(t, int64(types.MicroSecond), numList[1].GetLit().GetI64Val(), normalized.String())

	textSource := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1.5"}}},
	}
	normalizedText, textErr := resetWindowIntervalExpr(ctx, nil, rawPair(textSource))
	require.NoError(t, textErr)
	textList := normalizedText.GetList().List
	require.Equal(t, int32(types.T_int64), textList[0].Typ.Id, normalizedText.String())
	require.Equal(t, int64(1), textList[0].GetLit().GetI64Val(), normalizedText.String())
	require.Equal(t, int64(types.Day), textList[1].GetLit().GetI64Val(), normalizedText.String())
}

// requireBindFailsPM runs PREPARE through the real binder and requires the
// named bind failure, so the preservation pins below cannot pass vacuously.
func requireBindFailsPM(t *testing.T, prepareSQL string, message string) {
	t.Helper()
	_, err := bindLocalRankSQL(t, prepareSQL)
	require.Error(t, err, "expected a bind error for: %s", prepareSQL)
	require.ErrorContains(t, err, message)
}

// fillMrankPreparedWidth fills the prepared plan's single parameter and
// returns the mrank window spec of the filled plan.
func fillMrankPreparedWidth(
	t *testing.T,
	prepared *planpb.Plan,
	source types.Type,
	value string,
) *planpb.WindowSpec {
	t.Helper()
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
		prepared, []any{ParamValue{
			Value:         value,
			SourceType:    source,
			HasSourceType: true,
		}})
	require.NoError(t, err)
	spec := findLocalRankWindowSpec(filled, "mrank")
	require.NotNil(t, spec, filled.String())
	return spec
}

// TestLocalRankPreparedMrankWidthMarker pins the r12 review finding's
// contract: MRANK's bare prepared width marker binds in the INTEGER domain
// (NTILE parity, design §3.1 "any integer-typed expression ... as with
// NTILE's bucket count"), the plan publishes cast(ParamRef -> int64) exactly
// as a prepared ntile does, the EXECUTE fill lands inside the retention cast,
// and invalid values are left to the operator's named execution-time width
// check. The ordinary rejections stay intact: non-integer ordinary widths,
// marker-less constant widths, value-slot markers and arithmetic width
// markers keep the ordinary overload behavior, and the explicit
// CAST(? AS UNSIGNED) prepared route keeps publishing the unsigned domain.
func TestLocalRankPreparedMrankWidthMarker(t *testing.T) {
	t.Run("bare width marker binds in the integer domain like ntile", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"prepare stmt_mrank from "+
				"select mrank(o_totalprice, ?) over (order by o_orderdate) from orders")
		require.NoError(t, err)
		prepared := pn.GetDcl().GetPrepare().GetPlan()
		require.NotNil(t, prepared)
		spec := findLocalRankWindowSpec(prepared, "mrank")
		require.NotNil(t, spec, prepared.String())
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Len(t, args, 2, spec.String())
		// The value argument keeps its ordinary column domain.
		require.Equal(t, int32(types.T_decimal64), args[0].Typ.Id, spec.String())
		require.NotNil(t, args[0].GetCol(), spec.String())
		// The width marker publishes under the int64 retention cast (the same
		// shape a prepared NTILE count publishes).
		require.Equal(t, int32(types.T_int64), args[1].Typ.Id, spec.String())
		cast := args[1].GetF()
		require.NotNil(t, cast, spec.String())
		require.Equal(t, int32(types.T_int64), args[1].GetF().Args[1].Typ.Id, spec.String())
		require.NotNil(t, cast.Args[0].GetP(), "the cast must retain the raw ParamRef")

		// The EXECUTE fill substitutes inside the retention cast.
		filledSpec := fillMrankPreparedWidth(t, prepared, types.T_int64.ToType(), "3")
		fargs := filledSpec.GetWindowFunc().GetF().GetArgs()
		require.Len(t, fargs, 2, filledSpec.String())
		require.Equal(t, int32(types.T_int64), fargs[1].Typ.Id, filledSpec.String())
		require.NotNil(t, fargs[1].GetF(), filledSpec.String())
		require.Equal(t, int32(types.T_int64), fargs[1].GetF().Args[1].Typ.Id, filledSpec.String())

		// A zero fill is plan-correct: the operator's named execution-time
		// width check owns the invalid value (N >= 1), not the filler.
		zeroSpec := fillMrankPreparedWidth(t, prepared, types.T_int64.ToType(), "0")
		require.Equal(t, int32(types.T_int64),
			zeroSpec.GetWindowFunc().GetF().GetArgs()[1].Typ.Id, zeroSpec.String())
	})

	t.Run("value-slot marker still refuses the ordinary value domain", func(t *testing.T) {
		requireBindFailsPM(t,
			"prepare stmt_mrank_value from "+
				"select mrank(?, 3) over (order by o_orderdate) from orders",
			"function mrank")
	})

	t.Run("marker-less constants keep the ordinary overload check", func(t *testing.T) {
		requireBindFailsPM(t,
			"prepare stmt_mrank_frac from "+
				"select mrank(o_totalprice, 1.5) over (order by o_orderdate) from orders",
			"function mrank")
		requireBindFailsPM(t,
			"select mrank(n_nationkey, '3') over (order by n_nationkey) from nation",
			"function mrank")
		requireBindFailsPM(t,
			"select mrank(n_nationkey, 1.5) over (order by n_nationkey) from nation",
			"function mrank")
	})

	t.Run("marker arithmetic keeps the generic prepared-numeric route", func(t *testing.T) {
		// ? + 1 is not a bare marker, so it takes the ordinary generic path:
		// prepared arithmetic types the expression in the integer domain, so
		// the width check passes and the plan publishes an int64-typed width.
		// This route predates the r12 fix (the generic prepared-numeric
		// machinery owns it); the pin guards that the r12 dispatch leaves it
		// untouched.
		pn, err := bindLocalRankSQL(t,
			"prepare stmt_mrank_arith from "+
				"select mrank(o_totalprice, ? + 1) over (order by o_orderdate) from orders")
		require.NoError(t, err)
		spec := findLocalRankWindowSpec(pn.GetDcl().GetPrepare().GetPlan(), "mrank")
		require.NotNil(t, spec, pn.String())
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Equal(t, int32(types.T_int64), args[1].Typ.Id, spec.String())
	})

	t.Run("explicit unsigned cast route publishes its own domain", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"prepare stmt_mrank_u from "+
				"select mrank(o_totalprice, cast(? as unsigned)) over (order by o_orderdate) from orders")
		require.NoError(t, err)
		prepared := pn.GetDcl().GetPrepare().GetPlan()
		require.NotNil(t, prepared)
		spec := findLocalRankWindowSpec(prepared, "mrank")
		require.NotNil(t, spec, prepared.String())
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Equal(t, int32(types.T_uint64), args[1].Typ.Id, spec.String())
		filledSpec := fillMrankPreparedWidth(t, prepared, types.T_int64.ToType(), "3")
		fargs := filledSpec.GetWindowFunc().GetF().GetArgs()
		require.Equal(t, int32(types.T_uint64), fargs[1].Typ.Id, filledSpec.String())
	})

	t.Run("prepared ntile keeps its own marker path", func(t *testing.T) {
		pn, err := bindLocalRankSQL(t,
			"prepare stmt_ntile_pin from "+
				"select ntile(?) over (order by o_totalprice) from orders")
		require.NoError(t, err)
		spec := findLocalRankWindowSpec(pn.GetDcl().GetPrepare().GetPlan(), "ntile")
		require.NotNil(t, spec)
		args := spec.GetWindowFunc().GetF().GetArgs()
		require.Equal(t, int32(types.T_int64), args[0].Typ.Id, spec.String())
		require.NotNil(t, args[0].GetF(), spec.String())
	})
}
