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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func makePreparedDecimalComparisonColumn(typ types.Type) *planpb.Expr {
	return &planpb.Expr{
		Typ: makePlan2Type(&typ),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}
}

func makePreparedDecimalComparisonParam(pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
	}
}

func requirePreparedDecimalComparisonArgs(
	t *testing.T,
	expr *planpb.Expr,
	want types.Type,
	paramPos int,
) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 2)
	for _, arg := range fn.Args {
		require.Equal(t, int32(want.Oid), arg.Typ.Id)
		require.Equal(t, want.Width, arg.Typ.Width)
		require.Equal(t, want.Scale, arg.Typ.Scale)
	}

	cast := fn.Args[paramPos].GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.Len(t, cast.Args, 2)
	require.NotNil(t, cast.Args[0].GetP())
}

func TestPreparedDecimalBinaryComparisonsDeriveParamType(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 20, 4),
	}
	operators := []string{"=", "<=>", "<>", "<", "<=", ">", ">="}

	for _, decimalType := range decimalTypes {
		for _, operator := range operators {
			for _, paramLeft := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/param_left=%t", decimalType.Oid.String(), operator, paramLeft)
				t.Run(name, func(t *testing.T) {
					column := makePreparedDecimalComparisonColumn(decimalType)
					param := makePreparedDecimalComparisonParam(0)
					args := []*planpb.Expr{column, param}
					paramPos := 1
					if paramLeft {
						args = []*planpb.Expr{param, column}
						paramPos = 0
					}

					expr, err := BindFuncExprImplByPlanExpr(ctx, operator, args)
					require.NoError(t, err)
					requirePreparedDecimalComparisonArgs(t, expr, decimalType, paramPos)
				})
			}
		}
	}
}

func TestDecimalStringColumnComparisonsKeepMySQLCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	stringColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
	}

	expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		stringColumn,
	})
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.Equal(t, int32(types.T_float64), arg.Typ.Id)
	}
}

func findPreparedDecimalComparisonFunction(expr *planpb.Expr, name string) *planpb.Expr {
	if expr == nil {
		return nil
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == name {
			return expr
		}
		for _, arg := range fn.Args {
			if found := findPreparedDecimalComparisonFunction(arg, name); found != nil {
				return found
			}
		}
	}
	return nil
}

func findPreparedDecimalComparisonInPlan(queryPlan *planpb.Plan, name string) *planpb.Expr {
	query := queryPlan.GetQuery()
	if query == nil {
		return nil
	}
	for _, node := range query.Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
			node.AggList,
			node.GroupBy,
		} {
			for _, expr := range exprs {
				if found := findPreparedDecimalComparisonFunction(expr, name); found != nil {
					return found
				}
			}
		}
	}
	return nil
}

func planExprContainsPreparedDecimalParam(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if planExprContainsPreparedDecimalParam(arg) {
				return true
			}
		}
	}
	return false
}

func firstPreparedDecimalComparisonLiteral(expr *planpb.Expr) *planpb.Literal {
	if expr == nil {
		return nil
	}
	if literal := expr.GetLit(); literal != nil {
		return literal
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if literal := firstPreparedDecimalComparisonLiteral(arg); literal != nil {
				return literal
			}
		}
	}
	return nil
}

func TestPreparedDecimalComparisonPlannerReplacementAndReuse(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_cmp from 'select p_partkey from part where p_retailprice <=> ?'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	original := findPreparedDecimalComparisonInPlan(prepare.Plan, "<=>")
	require.NotNil(t, original)
	requirePreparedDecimalComparisonArgs(t, original, decimalType, 1)

	for _, value := range []any{
		nil,
		"9007199254740992.0001",
		"9007199254740993.0001",
	} {
		filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{value})
		require.NoError(t, err)
		require.NotSame(t, prepare.Plan, filled)

		comparison := findPreparedDecimalComparisonInPlan(filled, "<=>")
		require.NotNil(t, comparison)
		for _, arg := range comparison.GetF().Args {
			require.True(t, types.T(arg.Typ.Id).IsDecimal())
			require.Equal(t, decimalType.Scale, arg.Typ.Scale)
		}
		require.False(t, planExprContainsPreparedDecimalParam(comparison))

		literal := firstPreparedDecimalComparisonLiteral(comparison.GetF().Args[1])
		require.NotNil(t, literal)
		if value == nil {
			require.True(t, literal.Isnull)
		} else {
			require.False(t, literal.Isnull)
			require.Equal(t, value, literal.GetSval())
		}

		require.True(t, planExprContainsPreparedDecimalParam(original))
	}
}

func TestPlanDetectsRuntimeDecimalDomainsThatRequireFullRebuild(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	queries := []string{
		"select p_partkey from part where p_retailprice in (?, 9007199254740992.0001)",
		"select p_partkey from part where p_retailprice = ? or p_retailprice = 9007199254740992.0001",
		"select p_partkey from part where ? in (p_retailprice, ?)",
		"select p_partkey from part where p_retailprice between ? and 9007199254740993",
		"select p_partkey from part where (p_retailprice, p_partkey) in ((?, 3), (?, 5))",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, "prepare decimal_runtime_domain from '"+query+"'")
			require.NoError(t, err)
			hasRuntimeDomain, err := PlanHasExactDecimalComparisonParam(
				context.Background(), logicPlan.GetDcl().GetPrepare().Plan)
			require.NoError(t, err)
			require.True(t, hasRuntimeDomain)
		})
	}
}

func TestExactDecimalRuntimeDomainCollectsOnlyParticipatingParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_projection from 'select ? from part where p_retailprice = ? or p_partkey = ?'",
	)
	require.NoError(t, err)
	positions, err := ExactDecimalComparisonParamPositions(
		context.Background(), logicPlan.GetDcl().GetPrepare().Plan)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, positions)
}

func TestPreparedDecimalComparisonUsesActualStringValueDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_cmp_value from 'select p_partkey from part where p_retailprice = ?'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)

	for _, tc := range []struct {
		value     string
		wantValue string
		wantScale int32
	}{
		{value: "0x10", wantValue: "0", wantScale: 0},
		{value: "1+2", wantValue: "1", wantScale: 0},
		{value: "1 2", wantValue: "1", wantScale: 0},
		{value: "9007199254740992.00014", wantValue: "9007199254740992.00014", wantScale: 5},
	} {
		t.Run(tc.value, func(t *testing.T) {
			changed, err := PlanHasExactDecimalComparisonParam(context.Background(), prepare.Plan)
			require.NoError(t, err)
			require.True(t, changed)
			filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{tc.value})
			require.NoError(t, err)
			comparison := findPreparedDecimalComparisonInPlan(filled, "=")
			if tc.wantScale > decimalType.Scale {
				require.Nil(t, comparison)
				return
			}
			require.NotNil(t, comparison)
			value, ok := decimalCastSourceString(comparison.GetF().Args[1])
			require.True(t, ok)
			require.Equal(t, tc.wantValue, value)
			for _, arg := range comparison.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal())
			}
		})
	}
}

func TestPreparedDecimalComparisonDetectionTraversesNestedExpressions(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	comparison, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType), makePreparedDecimalComparisonParam(0),
	})
	require.NoError(t, err)
	nested, err := BindFuncExprImplByPlanExpr(ctx, "or", []*planpb.Expr{
		comparison, makePlan2BoolConstExprWithType(false),
	})
	require.NoError(t, err)
	query := &planpb.Query{Steps: []int32{0}, Nodes: []*planpb.Node{{
		NodeType: planpb.Node_FILTER, FilterList: []*planpb.Expr{nested},
	}}}

	for _, tc := range []struct {
		name string
		plan *planpb.Plan
	}{
		{name: "query", plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: DeepCopyQuery(query)}}},
		{name: "ctas", plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Query: DeepCopyQuery(query)}}}},
		{name: "set", plan: &planpb.Plan{Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
			DclType: planpb.DataControl_SET_VARIABLES,
			Control: &planpb.DataControl_SetVariables{SetVariables: &planpb.SetVariables{
				Items: []*planpb.SetVariablesItem{{Value: DeepCopyExpr(nested)}},
			}},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			found, err := PlanHasExactDecimalComparisonParam(ctx, tc.plan)
			require.NoError(t, err)
			require.True(t, found)
			var filled *planpb.Plan
			if tc.plan.GetDcl() != nil {
				filled, err = FillExactDecimalComparisonParamsInPlan(ctx, tc.plan, []any{"9007199254740992.00014"})
			} else {
				filled, err = FillValuesOfParamsInPlan(ctx, tc.plan, []any{"9007199254740992.00014"})
			}
			require.NoError(t, err)
			if ddl := filled.GetDdl(); ddl != nil {
				filled = &planpb.Plan{Plan: &planpb.Plan_Query{Query: ddl.Query}}
			}
			if dcl := filled.GetDcl(); dcl != nil {
				rewritten := dcl.GetSetVariables().Items[0].Value
				require.False(t, planExprContainsPreparedDecimalParam(
					findPreparedDecimalComparisonFunction(rewritten, "=")))
				return
			}
			require.False(t, planExprContainsPreparedDecimalParam(
				findPreparedDecimalComparisonInPlan(filled, "=")))
		})
	}
}

func TestFillPreparedDecimalComparisonPreservesPlanMessages(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	comparison, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType), makePreparedDecimalComparisonParam(0),
	})
	require.NoError(t, err)
	wantSend := []planpb.MsgHeader{{MsgTag: 17, MsgType: 3}}
	wantRecv := []planpb.MsgHeader{{MsgTag: 23, MsgType: 4}}
	prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_FILTER,
			FilterList:  []*planpb.Expr{comparison},
			SendMsgList: wantSend,
			RecvMsgList: wantRecv,
		}},
	}}}

	filled, err := FillValuesOfParamsInPlan(ctx, prepared, []any{"9007199254740992.00014"})
	require.NoError(t, err)
	require.Equal(t, wantSend, filled.GetQuery().Nodes[0].SendMsgList)
	require.Equal(t, wantRecv, filled.GetQuery().Nodes[0].RecvMsgList)
}

func TestFillExactDecimalComparisonLeavesOtherParamsForRuntimeTyping(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	comparison, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType), makePreparedDecimalComparisonParam(1),
	})
	require.NoError(t, err)
	ordinaryParam := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_PROJECT,
			ProjectList: []*planpb.Expr{ordinaryParam},
			FilterList:  []*planpb.Expr{comparison},
		}},
	}}}

	filled, err := FillExactDecimalComparisonParamsInPlan(ctx, prepared, []any{
		ParamValue{Value: "5"}, ParamValue{Value: "9007199254740992.0001"},
	})
	require.NoError(t, err)
	require.NotNil(t, filled.GetQuery().Nodes[0].ProjectList[0].GetP())
	require.False(t, planExprContainsPreparedDecimalParam(
		findPreparedDecimalComparisonInPlan(filled, "=")))
	require.NotNil(t, prepared.GetQuery().Nodes[0].ProjectList[0].GetP())
	require.True(t, planExprContainsPreparedDecimalParam(
		findPreparedDecimalComparisonInPlan(prepared, "=")))
}

func TestFillExactDecimalComparisonPromotesFloatToExactDecimalDomain(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	comparison, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType), makePreparedDecimalComparisonParam(0),
	})
	require.NoError(t, err)
	prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:   planpb.Node_FILTER,
			FilterList: []*planpb.Expr{comparison},
		}},
	}}}

	filled, err := FillExactDecimalComparisonParamsInPlan(ctx, prepared, []any{
		ParamValue{Value: "9007199254740992", PrepareParamKind: vector.PrepareParamFloat},
	})
	require.NoError(t, err)
	rewritten := findPreparedDecimalComparisonInPlan(filled, "=")
	require.NotNil(t, rewritten)
	for _, arg := range rewritten.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), arg.String())
	}
	require.False(t, planExprContainsPreparedDecimalParam(rewritten))
}

func TestFillExactDecimalComparisonPreservesExactNumericProtocolDomain(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, test := range []struct {
		name  string
		value ParamValue
	}{
		{name: "signed integer", value: ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger}},
		{name: "unsigned integer", value: ParamValue{Value: uint64(9007199254740993), PrepareParamKind: vector.PrepareParamInteger}},
		{name: "boolean", value: ParamValue{Value: true, PrepareParamKind: vector.PrepareParamBoolean}},
	} {
		t.Run(test.name, func(t *testing.T) {
			comparison, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType), makePreparedDecimalComparisonParam(0),
			})
			require.NoError(t, err)
			prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0}, Nodes: []*planpb.Node{{NodeType: planpb.Node_FILTER, FilterList: []*planpb.Expr{comparison}}},
			}}}
			filled, err := FillExactDecimalComparisonParamsInPlan(ctx, prepared, []any{test.value})
			require.NoError(t, err)
			rewritten := findPreparedDecimalComparisonInPlan(filled, "=")
			require.NotNil(t, rewritten)
			for _, arg := range rewritten.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
			}
			require.False(t, planExprContainsPreparedDecimalParam(rewritten))
		})
	}
}

func TestDecimalScalarSubqueryPreservesConstantAndParamDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	literalPlan, err := runOneStmt(
		mock,
		t,
		"select p_partkey from part where p_retailprice = (select '9007199254740992.0001')",
	)
	require.NoError(t, err)
	literalComparison := findPreparedDecimalComparisonInPlan(literalPlan, "=")
	require.NotNil(t, literalComparison)
	for _, arg := range literalComparison.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal())
	}

	foldablePlan, err := runOneStmt(
		mock,
		t,
		"select p_partkey from part where p_retailprice = (select concat('9007199254740992.000', '1'))",
	)
	require.NoError(t, err)
	foldableComparison := findPreparedDecimalComparisonInPlan(foldablePlan, "=")
	require.NotNil(t, foldableComparison)
	for _, arg := range foldableComparison.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal())
	}

	preparedPlan, err := runOneStmt(
		mock,
		t,
		"prepare scalar_decimal from 'select p_partkey from part where p_retailprice = (select ?)'",
	)
	require.NoError(t, err)
	prepared := preparedPlan.GetDcl().GetPrepare().Plan
	found, err := PlanHasExactDecimalComparisonParam(context.Background(), prepared)
	require.NoError(t, err)
	require.True(t, found)

	filled, err := FillExactDecimalComparisonParamsInPlan(
		context.Background(), prepared, []any{"9007199254740992.00014"})
	require.NoError(t, err)
	require.Nil(t, findPreparedDecimalComparisonInPlan(filled, "="))
}

func TestPreparedSetExactDecimalComparisonVisitsTransientQuery(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare set_decimal from 'set @answer=(select count(*) from part where p_retailprice=?)'",
	)
	require.NoError(t, err)
	prepared := logicPlan.GetDcl().GetPrepare().Plan
	setVars := prepared.GetDcl().GetSetVariables()
	require.NotNil(t, setVars)
	require.NotNil(t, setVars.GetQuery())

	found, err := PlanHasExactDecimalComparisonParam(context.Background(), prepared)
	require.NoError(t, err)
	require.True(t, found)
	filled, err := FillExactDecimalComparisonParamsInPlan(
		context.Background(), prepared, []any{"9007199254740992.00014"})
	require.NoError(t, err)
	require.False(t, planExprContainsPreparedDecimalParam(
		findPreparedDecimalComparisonInPlan(
			&planpb.Plan{Plan: &planpb.Plan_Query{Query: filled.GetDcl().GetSetVariables().GetQuery()}}, "=")))
}

func TestDecimalBetweenAndNotBetweenUseOneSourceDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	for _, tc := range []struct {
		name        string
		predicate   string
		prepared    bool
		expectExact bool
		expectedID  types.T
	}{
		{name: "prepared_between", predicate: "p_retailprice between ? and ?", prepared: true, expectExact: true, expectedID: types.T_decimal128},
		{name: "prepared_not_between", predicate: "p_retailprice not between ? and ?", prepared: true, expectExact: true, expectedID: types.T_decimal128},
		{name: "mixed_prepared_literal_between", predicate: "p_retailprice between ? and '9007199254740992.99995'", prepared: true, expectExact: true, expectedID: types.T_float64},
		{name: "mixed_prepared_literal_not_between", predicate: "p_retailprice not between ? and '9007199254740992.99995'", prepared: true, expectExact: true, expectedID: types.T_float64},
		{name: "literal_between", predicate: "p_retailprice between '9007199254740992.00005' and '9007199254740992.99995'", expectedID: types.T_float64},
		{name: "literal_not_between", predicate: "p_retailprice not between '9007199254740992.00005' and '9007199254740992.99995'", expectedID: types.T_float64},
		{name: "cast_between", predicate: "p_retailprice between cast('9007199254740992.00005' as char) and cast('9007199254740992.99995' as char)", expectedID: types.T_float64},
		{name: "cast_not_between", predicate: "p_retailprice not between cast('9007199254740992.00005' as char) and cast('9007199254740992.99995' as char)", expectedID: types.T_float64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "select p_partkey from part where " + tc.predicate
			if tc.prepared {
				sql = "prepare decimal_range from '" + strings.ReplaceAll(sql, "'", "''") + "'"
			}
			logicPlan, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			planToInspect := logicPlan
			if tc.prepared {
				planToInspect = logicPlan.GetDcl().GetPrepare().Plan
				found, err := PlanHasExactDecimalComparisonParam(context.Background(), planToInspect)
				require.NoError(t, err)
				require.Equal(t, tc.expectExact, found)
			}
			comparisons := 0
			for _, op := range []string{"<", "<=", ">", ">="} {
				if comparison := findPreparedDecimalComparisonInPlan(planToInspect, op); comparison != nil {
					comparisons++
					for _, arg := range comparison.GetF().Args {
						require.Equal(t, int32(tc.expectedID), arg.Typ.Id)
					}
				}
			}
			require.Equal(t, 2, comparisons)
		})
	}
}

func TestPreparedDecimalMultiInMaterializesEveryTextParam(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	for _, op := range []string{"in", "not in"} {
		t.Run(op, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t,
				"prepare decimal_multi from 'select p_partkey from part where p_retailprice "+op+" (?,?)'")
			require.NoError(t, err)
			prepared := logicPlan.GetDcl().GetPrepare().Plan
			found, err := PlanHasExactDecimalComparisonParam(context.Background(), prepared)
			require.NoError(t, err)
			require.True(t, found)

			filled, err := FillExactDecimalComparisonParamsInPlan(context.Background(), prepared, []any{
				"9007199254740992.00014", "9007199254740992.99994",
			})
			require.NoError(t, err)
			for _, node := range filled.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					require.False(t, planExprContainsPreparedDecimalParam(filter))
				}
			}
		})
	}
}

func TestPreparedDecimalGroupsUseExecutionCommonDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	for _, test := range []struct {
		name       string
		predicate  string
		params     []any
		expectedID types.T
	}{
		{
			name: "between text float", predicate: "p_retailprice between ? and ?",
			params:     []any{"9007199254740992.00005", ParamValue{Value: "9007199254740992", PrepareParamKind: vector.PrepareParamFloat}},
			expectedID: types.T_float64,
		},
		{
			name: "between float text", predicate: "p_retailprice between ? and ?",
			params: []any{ParamValue{Value: "9007199254740992", PrepareParamKind: vector.PrepareParamFloat},
				"9007199254740992.99995"},
			expectedID: types.T_float64,
		},
		{
			name: "in text float", predicate: "p_retailprice in (?,?)",
			params:     []any{"9007199254740992.0001", ParamValue{Value: "9007199254740992", PrepareParamKind: vector.PrepareParamFloat}},
			expectedID: types.T_float64,
		},
		{
			name: "not in text float", predicate: "p_retailprice not in (?,?)",
			params:     []any{"9007199254740992.0001", ParamValue{Value: "9007199254740992", PrepareParamKind: vector.PrepareParamFloat}},
			expectedID: types.T_float64,
		},
		{
			name: "in text integer", predicate: "p_retailprice in (?,?)",
			params:     []any{"9007199254740992.0001", ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger}},
			expectedID: types.T_decimal128,
		},
		{
			name: "in integer text", predicate: "p_retailprice in (?,?)",
			params: []any{ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
				"9007199254740992.0001"},
			expectedID: types.T_decimal128,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t,
				"prepare decimal_group from 'select p_partkey from part where "+test.predicate+"'")
			require.NoError(t, err)
			filled, err := FillExactDecimalComparisonParamsInPlan(
				context.Background(), logicPlan.GetDcl().GetPrepare().Plan, test.params)
			require.NoError(t, err)
			comparisons := 0
			var check func(*planpb.Expr)
			check = func(expr *planpb.Expr) {
				if expr == nil || expr.GetF() == nil {
					return
				}
				if isDecimalComparisonOperator(expr.GetF().Func.GetObjName()) {
					comparisons++
					for _, arg := range expr.GetF().Args {
						require.Equal(t, int32(test.expectedID), arg.Typ.Id)
					}
				}
				for _, arg := range expr.GetF().Args {
					check(arg)
				}
			}
			for _, node := range filled.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					check(filter)
				}
			}
			require.Equal(t, 2, comparisons)
		})
	}
}
