// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedNumericFallbackMetadataSurvivesProtoRoundTrip(t *testing.T) {
	original := &planpb.Expr{
		Typ:                                 planpb.Type{Id: int32(types.T_float64)},
		PreparedNumericFallback:             true,
		PreparedNumericParamPos:             0,
		PreparedNumericFallbackSource:       true,
		PreparedNumericFallbackSourceNodeId: 7,
		PreparedNumericFallbackSourceColPos: 2,
	}
	payload, err := proto.Marshal(original)
	require.NoError(t, err)

	var restored planpb.Expr
	require.NoError(t, proto.Unmarshal(payload, &restored))
	require.True(t, restored.PreparedNumericFallback)
	require.Equal(t, int32(0), restored.PreparedNumericParamPos)
	require.True(t, restored.PreparedNumericFallbackSource)
	require.Equal(t, int32(7), restored.PreparedNumericFallbackSourceNodeId)
	require.Equal(t, int32(2), restored.PreparedNumericFallbackSourceColPos)
	require.Zero(t, restored.AuxId,
		"prepared numeric provenance must not be encoded as an executor memo id")
}

func TestPreparedScalarNumericOverloadsUseDoubleDomain(t *testing.T) {
	for _, name := range []string{"abs", "sleep"} {
		t.Run(name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t,
				fmt.Sprintf("prepare stmt_%s from 'select %s(?)'", name, name))
			require.NoError(t, err)

			fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, name)
			require.NotNil(t, fn)
			require.Len(t, fn.GetF().Args, 1)
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
			require.Zero(t, fn.GetF().Args[0].AuxId,
				"deferred overload metadata must not consume executor AuxId space")
			cast := fn.GetF().Args[0].GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.Func.GetObjName())
			require.NotNil(t, cast.Args[0].GetP())
			require.Equal(t, int32(types.T_float64), cast.Args[1].Typ.Id)
		})
	}

	t.Run("ordinary column keeps native overload", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_column from 'select abs(n_regionkey) from nation'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})

	t.Run("parameter nested in arithmetic", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_expr from 'select abs(? + 0)'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})

	t.Run("scalar literal keeps native overload", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_scalar_literal from 'select abs((select n_regionkey from nation where n_regionkey = 0))'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})
}

func TestContainsPreparedParamExprVariants(t *testing.T) {
	param := tree.NewParamExpr(0)
	literal := tree.NewNumVal(int64(1), "1", false, tree.P_int64)
	binder := &baseBinder{sysCtx: context.Background()}
	hasParam := func(expr tree.Expr) bool {
		found, err := binder.hasPreparedNumericParamExprs([]tree.Expr{expr}, 0)
		require.NoError(t, err)
		return found
	}
	function := func(name string, args ...tree.Expr) tree.Expr {
		return &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
			Exprs: args,
		}
	}

	tests := []struct {
		name string
		expr tree.Expr
		want bool
	}{
		{name: "parameter", expr: param, want: true},
		{name: "binary", expr: tree.NewBinaryExpr(tree.PLUS, literal, param), want: true},
		{name: "unary", expr: tree.NewUnaryExpr(tree.UNARY_MINUS, param), want: true},
		{name: "parenthesized", expr: tree.NewParentExpr(param), want: true},
		{name: "function", expr: function("abs", param), want: true},
		{name: "cast", expr: tree.NewCastExpr(param, tree.TYPE_DOUBLE), want: true},
		{name: "bit cast", expr: tree.NewBitCastExpr(param, tree.TYPE_LONG), want: true},
		{name: "if condition", expr: function("if", param, literal, literal), want: false},
		{name: "if result", expr: function("if", literal, param, literal), want: true},
		{name: "case operand", expr: tree.NewCaseExpr(param, nil, literal), want: false},
		{name: "case when condition", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(param, literal)}, nil), want: false},
		{name: "case when value", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(literal, param)}, nil), want: true},
		{name: "case else", expr: tree.NewCaseExpr(nil, nil, param), want: true},
		{name: "tuple", expr: &tree.Tuple{Exprs: tree.Exprs{literal, param}}, want: true},
		{name: "scalar subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: param}}}, false), want: true},
		{name: "scalar literal subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: literal}}}, false), want: false},
		{name: "exists subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: param}}}, true), want: false},
		{name: "literal", expr: literal, want: false},
		{name: "empty case", expr: tree.NewCaseExpr(nil, nil, nil), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hasParam(tc.expr))
		})
	}
}

func TestPreparedNumericAstWalkersCoverSelectAndCastShapes(t *testing.T) {
	param := tree.NewParamExpr(0)
	literal := tree.NewNumVal(int64(1), "1", false, tree.P_int64)

	doubleCast := tree.NewCastExpr(param, tree.TYPE_DOUBLE)
	integerCast := tree.NewCastExpr(param, tree.TYPE_LONG)
	require.True(t, containsExplicitFloatCast(doubleCast))
	require.False(t, containsExplicitFloatCast(integerCast))
	require.True(t, containsExplicitFloatCast(tree.NewBitCastExpr(doubleCast, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewBinaryExpr(tree.PLUS, integerCast, doubleCast)))
	require.True(t, containsExplicitFloatCast(tree.NewUnaryExpr(tree.UNARY_MINUS, doubleCast)))
	require.True(t, containsExplicitFloatCast(tree.NewParentExpr(doubleCast)))
	require.True(t, containsExplicitFloatCast(&tree.FuncExpr{Exprs: tree.Exprs{doubleCast}}))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(doubleCast, nil, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil,
		[]*tree.When{tree.NewWhen(doubleCast, literal)}, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil,
		[]*tree.When{tree.NewWhen(literal, doubleCast)}, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil, nil, doubleCast)))
	require.True(t, containsExplicitFloatCast(&tree.Tuple{Exprs: tree.Exprs{doubleCast}}))
	require.True(t, containsExplicitFloatCast(tree.NewSubquery(
		&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: doubleCast}}}, false)))
	require.False(t, containsExplicitFloatCast(literal))
	require.True(t, containsExplicitFloatCasts(tree.Exprs{integerCast, doubleCast}))
	require.False(t, containsExplicitFloatCasts(tree.Exprs{integerCast}))
}

func TestPreparedNumericPlanHelpersCoverDeferredAndIntegerPaths(t *testing.T) {
	floatType := planpb.Type{Id: int32(types.T_float64)}
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_P{
			P: &planpb.ParamRef{Pos: pos},
		}}
	}
	literalExpr := func(value string) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: value},
		}}}
	}
	functionExpr := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name}, Args: args,
		}}}
	}

	deferredArg := functionExpr("cast", paramExpr(0), floatTypeExprForTest())
	deferredArg.PreparedNumericFallback = true
	deferredArg.PreparedNumericParamPos = 0
	deferredAbs := functionExpr("abs", deferredArg)
	queryPlan := &Plan{Plan: &Plan_Query{Query: &Query{
		Steps: []int32{0},
		Nodes: []*Node{{NodeType: planpb.Node_PROJECT, ProjectList: []*planpb.Expr{deferredAbs}}},
	}}}
	require.True(t, PreparedPlanHasDeferredNumericFunction(queryPlan))
	deferredSubqueryArg := &planpb.Expr{
		Typ:                     floatType,
		PreparedNumericFallback: true,
		PreparedNumericParamPos: 0,
		Expr:                    &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: deferredArg}},
	}
	deferredSubqueryAbs := functionExpr("abs", deferredSubqueryArg)
	queryPlan.GetQuery().Nodes = []*Node{{NodeType: planpb.Node_PROJECT, ProjectList: []*planpb.Expr{deferredSubqueryAbs}}}
	require.True(t, PreparedPlanHasDeferredNumericFunction(queryPlan))
	require.False(t, PreparedPlanHasDeferredNumericFunction(nil))
	require.False(t, PreparedPlanHasDeferredNumericFunction(&Plan{}))
	require.False(t, PreparedPlanHasDeferredNumericFunction(&Plan{Plan: &Plan_Query{
		Query: &Query{Steps: []int32{0}, Nodes: []*Node{{ProjectList: []*planpb.Expr{
			functionExpr("abs", literalExpr("1")),
		}}}},
	}}))

	// Exercise the ResetParamRefRule's exact integer/DECIMAL specialization,
	// including signed, signed-range unsigned, wide unsigned, invalid, NULL,
	// and missing protocol-kind values.
	newLiteralParam := func(value string, isNull bool) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: isNull, Value: &planpb.Literal_Sval{Sval: value},
		}}}
	}
	for _, tc := range []struct {
		name  string
		value string
		want  types.T
	}{
		{name: "negative", value: "-3", want: types.T_int64},
		{name: "signed range", value: "3", want: types.T_int64},
		{name: "wide unsigned", value: "9223372036854775808", want: types.T_uint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam(tc.value, false)})
			rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
			bound, ok := rule.typedIntegerParamExpr(0)
			require.True(t, ok)
			require.Equal(t, int32(tc.want), bound.Typ.Id)
		})
	}
	for _, tc := range []struct {
		name  string
		param *planpb.Expr
		kinds []vector.PrepareParamKind
		pos   int32
	}{
		{name: "invalid text", param: newLiteralParam("bad", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "null", param: newLiteralParam("", true), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "empty", param: newLiteralParam("", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "wrong kind", param: newLiteralParam("1", false), kinds: []vector.PrepareParamKind{vector.PrepareParamFloat}},
		{name: "bad position", param: newLiteralParam("1", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}, pos: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{tc.param})
			rule.SetParamKinds(tc.kinds)
			bound, ok := rule.typedIntegerParamExpr(tc.pos)
			require.False(t, ok)
			require.Nil(t, bound)
		})
	}

	rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("-7", false)})
	rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	rebound, changed, err := rule.rebindPreparedIntegerExpr(paramExpr(0))
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(types.T_int64), rebound.Typ.Id)
	_, changed, err = rule.rebindPreparedIntegerExpr(nil)
	require.NoError(t, err)
	require.False(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(literalExpr("1"))
	require.NoError(t, err)
	require.False(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{paramExpr(0), literalExpr("1")}},
	}})
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{literalExpr("1")}},
	}})
	require.NoError(t, err)
	require.False(t, changed)

	decimalCases := []struct {
		value string
		want  types.T
	}{
		{value: "123456789012.3456", want: types.T_decimal64},
		{value: "1234567890123456789012345678901234.5678", want: types.T_decimal128},
		{value: "12345678901234567890123456789012345.6789", want: types.T_decimal256},
	}
	for _, decimalCase := range decimalCases {
		decimalRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
			newLiteralParam(decimalCase.value, false),
		})
		decimalRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamDecimal})
		decimal, ok, err := decimalRule.typedDecimalParamExpr(0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int32(decimalCase.want), decimal.Typ.Id)
	}
	decimalRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		newLiteralParam("12345678901234567890123456789012345.6789", false),
	})
	decimalRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamDecimal})
	decimal, ok, err := decimalRule.typedDecimalParamExpr(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(types.T_decimal256), decimal.Typ.Id)
	require.True(t, decimalRule.allDecimalParamRefs(paramExpr(0)))
	require.False(t, decimalRule.allDecimalParamRefs(literalExpr("1")))
	rebound, changed, err = decimalRule.rebindPreparedDecimalExpr(paramExpr(0))
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(types.T_decimal256), rebound.Typ.Id)
	_, changed, err = decimalRule.rebindPreparedDecimalExpr(nil)
	require.NoError(t, err)
	require.False(t, changed)
	rule = NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("7", false)})
	rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	rebound, changed, err = rule.rebindPreparedIntegerExpr(deferredSubqueryArg)
	require.NoError(t, err)
	require.True(t, changed)
	if child := rebound.GetSub().Child; child.GetF() != nil {
		require.Equal(t, int64(7), child.GetF().Args[0].GetLit().GetI64Val())
	} else {
		require.Equal(t, int64(7), child.GetLit().GetI64Val())
	}

	// Cover the selective numeric-value walkers used by ABS's CASE/IF forms.
	ifExpr := functionExpr("if", literalExpr("0"), paramExpr(0), literalExpr("1"))
	caseExpr := functionExpr("case", paramExpr(0), literalExpr("1"), literalExpr("2"))
	caseValueExpr := functionExpr("case", literalExpr("0"), paramExpr(0), literalExpr("2"))
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
		want bool
	}{
		{name: "nil", expr: nil, want: false},
		{name: "direct", expr: paramExpr(0), want: true},
		{name: "if result", expr: ifExpr, want: true},
		{name: "case condition", expr: caseExpr, want: false},
		{name: "case value", expr: caseValueExpr, want: true},
		{name: "literal", expr: literalExpr("1"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, got := preparedNumericValueParamPosition(tc.expr)
			require.Equal(t, tc.want, got)
		})
	}
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(ifExpr, positions)
	collectNumericValueParamPositions(caseValueExpr, positions)
	collectNumericValueParamPositions(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{paramExpr(1)}},
	}}, positions)
	require.Contains(t, positions, int32(0))
	require.Contains(t, positions, int32(1))

	integerRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("1", false)})
	integerRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	require.True(t, integerRule.allIntegerParamRefs(paramExpr(0)))
	require.False(t, integerRule.allIntegerParamRefs(literalExpr("1")))
	integerRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamFloat})
	require.False(t, integerRule.allIntegerParamRefs(paramExpr(0)))
}

func floatTypeExprForTest() *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}}
}

func TestPreparedScalarNumericOverloadsCoverSubqueryAndExactInteger(t *testing.T) {
	ctx := context.Background()

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_exact from 'select abs(?)'")
	require.NoError(t, err)
	queryPlan := prepared.GetDcl().GetPrepare().Plan
	filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn := findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, int64(-9007199254740993), fn.GetF().Args[0].GetLit().GetI64Val())

	for _, sql := range []string{
		"prepare stmt_abs_nested_arithmetic from 'select abs(? + 0)'",
		"prepare stmt_abs_nested_if from 'select abs(if(1, ?, 0))'",
		"prepare stmt_abs_nested_case from 'select abs(case when 1 then ? else 0 end)'",
		"prepare stmt_abs_nested_scalar from 'select abs((select ?))'",
	} {
		prepared, err = runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		filled, err = FillValuesOfParamsInPlan(ctx, prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
		})
		require.NoError(t, err)
		fn = findPlanFunctionExpr(filled, "abs")
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_int64), fn.Typ.Id, sql)
		require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id, sql)
	}

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_subquery from 'select abs((select ?))'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	require.True(t, PreparedPlanHasDeferredNumericFunction(prepared.GetDcl().GetPrepare().Plan))

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_case_condition from 'select abs(case when ? then n_regionkey else n_regionkey end) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	// The marker only controls CASE flow. It must not force the BIGINT result
	// branches through the deferred DOUBLE overload.
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_if_condition from 'select abs(if(?, n_regionkey, n_regionkey)) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_explicit_double from 'select abs(cast(? as double))'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	// An explicit DOUBLE cast is a user-requested precision boundary and must
	// not be specialized back to an integer overload.
	require.Equal(t, int32(types.T_float64), fn.Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_decimal from 'select abs(?)'")
	require.NoError(t, err)
	filled, err = FillValuesOfParamsInPlan(ctx, prepared.GetDcl().GetPrepare().Plan, []any{
		ParamValue{
			Value:            "12345678901234567890123456789012345.6789",
			PrepareParamKind: vector.PrepareParamDecimal,
		},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	// DECIMAL values must bypass the prepare-time DOUBLE fallback as well;
	// otherwise high-precision values are rounded before ABS sees them.
	require.Equal(t, int32(types.T_decimal256), fn.Typ.Id)
	decimalArg := fn.GetF().Args[0]
	require.Equal(t, int32(types.T_decimal256), decimalArg.Typ.Id)
	require.Equal(t, "cast", decimalArg.GetF().Func.GetObjName())
	require.Equal(t, "12345678901234567890123456789012345.6789", decimalArg.GetF().Args[0].GetLit().GetSval())

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_nested_scalar_round from 'select abs((select round(? + 0) from nation limit 1))'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(queryPlan))
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	round := findPlanFunctionExpr(filled, "round")
	require.NotNil(t, round, "the scalar subquery projection must remain after rebinding")
	require.Equal(t, int32(types.T_int64), round.Typ.Id)
	require.NotNil(t, round.GetF().Args[0].GetF())
	require.Equal(t, int32(types.T_int64), round.GetF().Args[0].Typ.Id)
}

func TestBindFuncExprImplByPlanExpr_CaseDifferentDecimalScale(t *testing.T) {
	ctx := context.Background()

	condExpr := makePlan2BoolConstExprWithType(false)
	thenExpr := makeDecimal128ConstExpr("-58140.00", 23, 2)
	elseExpr := makeDecimal128ConstExpr("-408180.5580000", 38, 7)

	result, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{condExpr, thenExpr, elseExpr})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(38), result.Typ.Width)
	require.Equal(t, int32(7), result.Typ.Scale)

	funcExpr := result.GetF()
	require.NotNil(t, funcExpr)
	require.Len(t, funcExpr.Args, 3)

	arg1 := funcExpr.Args[1]
	require.True(t, isCastExpr(arg1), "THEN value should be cast when CASE decimal branch scales differ")
	require.Equal(t, int32(types.T_decimal128), arg1.Typ.Id)
	require.Equal(t, int32(38), arg1.Typ.Width)
	require.Equal(t, int32(7), arg1.Typ.Scale)
	require.False(t, isCastExpr(funcExpr.Args[2]), "ELSE value already has the common decimal scale")
}

func TestBuildPreparedCaseConditionParameter(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select case when ? then v else -v end from (select 1 as v) t", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx, queryPlan))

	caseExpr := findPlanFunctionExpr(queryPlan, "case")
	require.NotNil(t, caseExpr)
	require.Len(t, caseExpr.GetF().Args, 3)
	requirePreparedCaseCondition(t, caseExpr.GetF().Args[0], true)

	for _, test := range []struct {
		name    string
		value   any
		wantNil bool
		want    string
	}{
		{name: "true", value: "1", want: "1"},
		{name: "false", value: "0", want: "0"},
		{name: "null", value: nil, wantNil: true},
		{name: "binary true", value: ParamValue{Value: "1", IsBin: true}, want: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{test.value})
			require.NoError(t, err)
			filledCase := findPlanFunctionExpr(filled, "case")
			require.NotNil(t, filledCase)
			requirePreparedCaseCondition(t, filledCase.GetF().Args[0], false)

			conditionArg := filledCase.GetF().Args[0].GetF().Args[0]
			if test.wantNil {
				require.True(t, conditionArg.GetLit().GetIsnull())
			} else {
				require.Equal(t, test.want, conditionArg.GetLit().GetSval())
				require.Equal(t, test.name == "binary true", conditionArg.GetLit().GetIsBin())
			}
		})
	}
}

func requirePreparedCaseCondition(t *testing.T, condition *planpb.Expr, hasParam bool) {
	t.Helper()
	require.Equal(t, int32(types.T_bool), condition.Typ.Id)
	cast := condition.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.NotEmpty(t, cast.Args)
	if hasParam {
		require.NotNil(t, cast.Args[0].GetP())
		require.Equal(t, int32(0), cast.Args[0].GetP().Pos)
	} else {
		require.NotNil(t, cast.Args[0].GetLit())
	}
}

func findPlanFunctionExpr(queryPlan *planpb.Plan, name string) *planpb.Expr {
	var find func(*planpb.Expr) *planpb.Expr
	find = func(expr *planpb.Expr) *planpb.Expr {
		if expr == nil {
			return nil
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.GetObjName() == name {
				return expr
			}
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

	if query := queryPlan.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			for _, exprs := range [][]*planpb.Expr{
				node.ProjectList,
				node.FilterList,
				node.OnList,
				node.AggList,
				node.GroupBy,
			} {
				for _, expr := range exprs {
					if found := find(expr); found != nil {
						return found
					}
				}
			}
		}
	}
	return nil
}
