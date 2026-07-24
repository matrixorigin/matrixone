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

package plan

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/smartystreets/goconvey/convey"
)

func TestExpr_1(t *testing.T) {
	convey.Convey("selectAndStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		params := []bool{false, true}
		input := []string{"select 0 and 1 from dual;",
			"select false and 1 from dual;",
			"select false and true from dual;",
			"select 0 and true from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "and")
			for j, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_Lit)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				constB, ok := exprC.Lit.Value.(*plan.Literal_Bval)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[j])
			}
		}
	})
}

func TestExpr_2(t *testing.T) {
	convey.Convey("selectORStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		params := []bool{false, true}
		input := []string{"select 0 or 1 from dual;",
			"select false or 1 from dual;",
			"select false or true from dual;",
			"select 0 or true from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "or")
			for j, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_Lit)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				constB, ok := exprC.Lit.Value.(*plan.Literal_Bval)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[j])
			}
		}
	})
}

func TestExpr_3(t *testing.T) {
	convey.Convey("selectNotStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		params := []bool{false, false, true, true}
		input := []string{"select not 0 from dual;",
			"select not false from dual;",
			"select not 1 from dual;",
			"select not true from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "not")
			for _, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_Lit)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				constB, ok := exprC.Lit.Value.(*plan.Literal_Bval)
				if !ok {
					t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[i])
			}
		}
	})
}

func TestExpr_4(t *testing.T) {
	convey.Convey("selectEqualStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 = 1 from dual;",
			"select 1 = 1 from dual;",
			"select true = false from dual;",
			"select true = 1 from dual;",
			"select 0 = false from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "=")
		}
	})
}

func TestExpr_5(t *testing.T) {
	convey.Convey("selectLessStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 < 1 from dual;",
			"select 1 < 1 from dual;",
			"select 1 < 0 from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<")
		}
	})
}

func TestExpr_6(t *testing.T) {
	convey.Convey("selectLessEqualStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 <= 1 from dual;",
			"select 1 <= 1 from dual;",
			"select 1 <= 0 from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<=")
		}
	})
}

func TestExpr_7(t *testing.T) {
	convey.Convey("selectGreatStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 > 1 from dual;",
			"select 1 > 1 from dual;",
			"select 1 > 0 from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, ">")
		}
	})
}

func TestExpr_8(t *testing.T) {
	convey.Convey("selectGreatEqualStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 >= 1 from dual;",
			"select 1 >= 1 from dual;",
			"select 1 >= 0 from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, ">=")
		}
	})
}

func TestExpr_9(t *testing.T) {
	convey.Convey("selectGreatEqualStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 != 1 from dual;",
			"select 1 != 1 from dual;",
			"select 1 != 0 from dual;",
			"select 0 <> 1 from dual;",
			"select 1 <> 1 from dual;",
			"select 1 <> 0 from dual;"}

		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<>")
		}
	})
}

func TestExpr_A(t *testing.T) {
	convey.Convey("selectAndStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 < 1 and 1 > 0 from dual;",
			"select 0 < 1 or 1 > 0 from dual;",
			"select not 0 < 1 from dual;"}
		name := []string{"and", "or", "not"}
		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, name[i])
			for _, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
			}
		}
	})
}

func TestExpr_B(t *testing.T) {
	convey.Convey("selectAndStmt succ", t, func() {
		mock := NewMockOptimizer(false)
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 < 1 and 1 > 0 && not false from dual;"}
		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", moerr.NewInternalError(mock.ctxt.GetContext(), "the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "and")
			for _, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
			}
		}
	})
}

func TestConvertBitConstantToJSONPreservesBitType(t *testing.T) {
	mock := NewMockOptimizer(false)
	pl, err := runOneExprStmt(mock, t, "select convert(cast(b'1' as bit(1)), json)")
	require.NoError(t, err)

	query := pl.GetQuery()
	require.NotNil(t, query)
	require.Len(t, query.Nodes, 2)
	require.Len(t, query.Nodes[1].ProjectList, 1)

	cast := query.Nodes[1].ProjectList[0].GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.ObjName)
	require.Len(t, cast.Args, 2)
	require.Equal(t, int32(types.T_bit), cast.Args[0].Typ.Id)
	require.Equal(t, int32(1), cast.Args[0].Typ.Width)
}

func TestConstantFoldBitCastPreservesBitType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		literal string
		width   int32
	}{
		{name: "bit1", literal: "b'1'", width: 1},
		{name: "bit8", literal: "b'11111111'", width: 8},
		{name: "bit9", literal: "b'100001010'", width: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			pl, err := runOneExprStmt(mock, t, fmt.Sprintf("select convert(cast(%s as bit(%d)), json)", tc.literal, tc.width))
			require.NoError(t, err)

			cast := pl.GetQuery().Nodes[1].ProjectList[0].GetF()
			require.NotNil(t, cast)
			require.Len(t, cast.Args, 2)

			node := &plan.Node{ProjectList: []*plan.Expr{DeepCopyExpr(cast.Args[0])}}
			rule.NewConstantFold(false).Apply(node, nil, mock.CurrentContext().GetProcess())

			folded := node.ProjectList[0]
			require.Equal(t, int32(types.T_bit), folded.Typ.Id)
			require.Equal(t, tc.width, folded.Typ.Width)
			require.NotNil(t, folded.GetLit())
		})
	}
}

func TestConvertBitConstantToJSONAfterConstantFold(t *testing.T) {
	for _, tc := range []struct {
		name    string
		literal string
		width   int32
		payload string
	}{
		{name: "bit1", literal: "b'1'", width: 1, payload: `"AQ=="`},
		{name: "bit8", literal: "b'11111111'", width: 8, payload: `"/w=="`},
		{name: "bit9", literal: "b'100001010'", width: 9, payload: `"AQo="`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			pl, err := runOneExprStmt(mock, t, fmt.Sprintf("select convert(cast(%s as bit(%d)), json)", tc.literal, tc.width))
			require.NoError(t, err)

			expr := DeepCopyExpr(pl.GetQuery().Nodes[1].ProjectList[0])
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, mock.CurrentContext().GetProcess(), false, true)
			require.NoError(t, err)

			vec, free, err := colexec.GetReadonlyResultFromExpression(
				mock.CurrentContext().GetProcess(), folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, types.T_json, vec.GetType().Oid)
			value := types.DecodeJson(vec.GetBytesAt(0))
			require.Equal(t, bytejson.TpCodeBlob, value.Type)
			require.Equal(t, tc.payload, value.String())
		})
	}
}

func TestEnumToJSONQuotesDisplayValueDuringBinding(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enumValues string
		nullable   bool
	}{
		{name: "text label", enumValues: "alpha,beta"},
		{name: "json-looking label", enumValues: `{"a":1},1`},
		{name: "nullable column", enumValues: "alpha,beta", nullable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			column := mock.ctxt.tables["nation"].Cols[1]
			column.Typ = plan.Type{
				Id:          int32(types.T_enum),
				Enumvalues:  tc.enumValues,
				NotNullable: !tc.nullable,
			}

			for _, sql := range []string{
				"select convert(n_name, json) from nation",
				"select cast(n_name as json) from nation",
			} {
				pl, err := runOneExprStmt(mock, t, sql)
				require.NoError(t, err, sql)

				expr := pl.GetQuery().Nodes[1].ProjectList[0]
				require.Equal(t, int32(types.T_json), expr.Typ.Id, sql)
				quoted := expr.GetF()
				require.NotNil(t, quoted, sql)
				require.Equal(t, "json_quote", quoted.Func.ObjName, sql)
				require.Len(t, quoted.Args, 1, sql)
				displayValue := quoted.Args[0].GetF()
				require.NotNil(t, displayValue, sql)
				require.Equal(t, moEnumCastIndexToValueFun, displayValue.Func.ObjName, sql)
			}
		})
	}
}

func TestEnumDisplayValueToJSONUsesJSONQuoteInPlannerCasts(t *testing.T) {
	ctx := NewMockCompilerContext(true).GetContext()
	displayExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: moEnumCastIndexToValueFun},
		}},
	}
	jsonType := plan.Type{Id: int32(types.T_json)}

	expr, err := makePlan2CastExpr(ctx, DeepCopyExpr(displayExpr), jsonType)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), expr.Typ.Id)
	require.Equal(t, "json_quote", expr.GetF().Func.ObjName)

	expr, err = forceAssignmentCastExpr(ctx, DeepCopyExpr(displayExpr), jsonType)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), expr.Typ.Id)
	require.Equal(t, "json_quote", expr.GetF().Func.ObjName)

	expr, err = forceCastExpr2(ctx, DeepCopyExpr(displayExpr), types.T_json.ToType(), &plan.Expr{Typ: jsonType})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), expr.Typ.Id)
	require.Equal(t, "json_quote", expr.GetF().Func.ObjName)
}

func runOneExprStmt(opt Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	ctx := opt.CurrentContext()

	var pl *plan.Plan
	for _, ast := range stmts {
		pl, err = BuildPlan(ctx, ast, false)
		if err != nil {
			return nil, err
		}
	}
	return pl, nil
}

func TestMakeTimeBinaryLiteralBindAndExecute(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		wantNull bool
	}{
		{name: "hex hour", sql: "select cast(maketime(X'0102', 0, 0) as varchar)", want: "258:00:00"},
		{name: "hex hour empty", sql: "select cast(maketime(X'', 0, 0) as varchar)", want: "00:00:00"},
		{name: "hex hour max int64", sql: "select cast(maketime(X'7FFFFFFFFFFFFFFF', 0, 0) as varchar)", want: "838:59:59"},
		{name: "hex hour max int64 plus one", sql: "select cast(maketime(X'8000000000000000', 0, 0) as varchar)", want: "838:59:59"},
		{name: "hex hour uint64 overflow", sql: "select cast(maketime(X'FFFFFFFFFFFFFFFF', 0, 0) as varchar)", want: "838:59:59"},
		{name: "hex hour wider than uint64", sql: "select cast(maketime(X'FFFFFFFFFFFFFFFFFF', 0, 0) as varchar)", want: "838:59:59"},
		{name: "hex hour wide leading zeros", sql: "select cast(maketime(X'000000000000000001', 0, 0) as varchar)", want: "01:00:00"},
		{name: "hex minute", sql: "select cast(maketime(12, X'01', 0) as varchar)", want: "12:01:00"},
		{name: "hex minute overflow", sql: "select cast(maketime(12, X'FFFFFFFFFFFFFFFFFF', 0) as varchar)", wantNull: true},
		{name: "hex minute wide leading zeros", sql: "select cast(maketime(12, X'000000000000000001', 0) as varchar)", want: "12:01:00"},
		{name: "hex second", sql: "select cast(maketime(12, 0, X'01') as varchar)", want: "12:00:01"},
		{name: "hex second wide leading zeros", sql: "select cast(maketime(12, 0, X'000000000000000001') as varchar)", want: "12:00:01"},
		{name: "hex second wide leading zero max", sql: "select cast(maketime(12, 0, X'00000000000000003B') as varchar)", want: "12:00:59"},
		{name: "hex second wider overflow", sql: "select cast(maketime(12, 0, X'010000000000000000') as varchar)", wantNull: true},
		{name: "bit second", sql: "select cast(maketime(12, 0, B'00000001') as varchar)", want: "12:00:01"},
		{name: "binary string second", sql: "select cast(maketime(12, 0, cast('01' as binary(2))) as varchar)", want: "12:00:01"},
		{name: "empty string second coerces to zero", sql: "select cast(maketime(12, 34, '') as varchar)", want: "12:34:00.000000"},
		{name: "nonnumeric string second coerces to zero", sql: "select cast(maketime(12, 34, 'foo') as varchar)", want: "12:34:00.000000"},
		{name: "plain strings", sql: "select cast(maketime('12.7', '15.8', '30.9') as varchar)", want: "12:15:30.900000"},
		{name: "decimal second", sql: "select cast(maketime(12, 34, cast('56.789012' as decimal(20, 6))) as varchar)", want: "12:34:56.789012"},
		{name: "decimal minute below half", sql: "select cast(maketime(12, cast('59.49999999999999999999' as decimal(30, 20)), cast('0' as decimal(2, 1))) as varchar)", want: "12:59:00.0"},
		{name: "decimal minute at half", sql: "select cast(maketime(12, cast('58.5' as decimal(3, 1)), 0) as varchar)", want: "12:59:00"},
		{name: "decimal64 minute below half", sql: "select cast(maketime(12, cast('58.499999999999999' as decimal(17, 15)), 0) as varchar)", want: "12:58:00"},
		{name: "decimal minute rounds out of range", sql: "select maketime(12, cast('59.5' as decimal(3, 1)), 0)", wantNull: true},
		{name: "decimal hour below half", sql: "select cast(maketime(cast('12.49999999999999999999' as decimal(30, 20)), 0, 0) as varchar)", want: "12:00:00"},
		{name: "negative decimal hour at half", sql: "select cast(maketime(cast('-12.5' as decimal(3, 1)), 0, 0) as varchar)", want: "-13:00:00"},
		{name: "decimal hour positive overflow", sql: "select cast(maketime(cast('99999999999999999999999999999999999999' as decimal(38, 0)), 0, 0) as varchar)", want: "838:59:59"},
		{name: "decimal hour negative overflow", sql: "select cast(maketime(cast('-99999999999999999999999999999999999999' as decimal(38, 0)), 0, 0) as varchar)", want: "-838:59:59"},
		{name: "decimal256 hour below half", sql: "select cast(maketime(cast('12.499999999999999999999999999999' as decimal(65, 30)), 0, 0) as varchar)", want: "12:00:00"},
		{name: "decimal256 hour at half", sql: "select cast(maketime(cast('12.500000000000000000000000000000' as decimal(65, 30)), 0, 0) as varchar)", want: "13:00:00"},
		{name: "decimal256 minute below half", sql: "select cast(maketime(12, cast('58.499999999999999999999999999999' as decimal(65, 30)), 0) as varchar)", want: "12:58:00"},
		{name: "decimal256 minute at half", sql: "select cast(maketime(12, cast('58.500000000000000000000000000000' as decimal(65, 30)), 0) as varchar)", want: "12:59:00"},
		{name: "decimal256 hour positive overflow", sql: "select cast(maketime(cast('99999999999999999999999999999999999999999999999999999999999999999' as decimal(65, 0)), 0, 0) as varchar)", want: "838:59:59"},
		{name: "decimal256 hour negative overflow", sql: "select cast(maketime(cast('-99999999999999999999999999999999999999999999999999999999999999999' as decimal(65, 0)), 0, 0) as varchar)", want: "-838:59:59"},
		{name: "safe exponent underflow", sql: "select cast(maketime(12, 34, '1e-5000') as varchar)", want: "12:34:00.000000"},
		{name: "zero mantissa huge exponent", sql: "select cast(maketime(12, 34, '0e5000') as varchar)", want: "12:34:00.000000"},
		{name: "wide zero mantissa", sql: "select cast(maketime(12, 34, '" + strings.Repeat("0", 4097) + "') as varchar)", want: "12:34:00.000000"},
		{name: "wide leading zero second", sql: "select cast(maketime(12, 34, '" + strings.Repeat("0", 4096) + "1') as varchar)", want: "12:34:01.000000"},
		{name: "wide trailing fractional zero second", sql: "select cast(maketime(12, 34, '1." + strings.Repeat("0", 4097) + "') as varchar)", want: "12:34:01.000000"},
		{name: "wide zero padding canceled by exponent", sql: "select cast(maketime(12, 34, '0." + strings.Repeat("0", 4096) + "1e4097') as varchar)", want: "12:34:01.000000"},
		{name: "wide significant fractional second", sql: "select cast(maketime(12, 34, '1." + strings.Repeat("1", 4096) + "') as varchar)", want: "12:34:01.111111"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			pl, err := runOneExprStmt(mock, t, test.sql)
			require.NoError(t, err)

			query := pl.GetQuery()
			require.NotNil(t, query)
			expr := query.Nodes[1].ProjectList[0]
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			if test.wantNull {
				require.True(t, result.GetNulls().Contains(0))
				return
			}
			require.False(t, result.GetNulls().Contains(0))
			require.Equal(t, test.want, result.GetStringAt(0))
		})
	}
}

func TestMakeTimeExtremeExactSecondBindAndExecute(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"exponent", "select maketime(12, 34, '1e" + strings.Repeat("9", 8192) + "')"},
		{"mantissa", "select maketime(12, 34, '" + strings.Repeat("9", 4097) + "')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			pl, err := runOneExprStmt(mock, t, test.sql)
			require.NoError(t, err)

			expr := pl.GetQuery().Nodes[1].ProjectList[0]
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			require.True(t, result.GetNulls().Contains(0))
		})
	}
}

func makeTimeExpr(s string, p int32) *plan.Expr {
	dt, _ := types.ParseTime(s, 0)
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_time),
			Scale: p,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Timeval{
					Timeval: int64(dt),
				},
			},
		},
	}
}

func makeDateExpr(s string) *plan.Expr {
	dt, _ := types.ParseDateCast(s)
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_date),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Dateval{
					Dateval: int32(dt),
				},
			},
		},
	}
}

func makeTimestampExpr(s string, p int32, loc *time.Location) *plan.Expr {
	dt, _ := types.ParseTimestamp(loc, s, p)
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_timestamp),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Timestampval{
					Timestampval: int64(dt),
				},
			},
		},
	}
}
func makeDatetimeExpr(s string, p int32) *plan.Expr {
	dt, _ := types.ParseDatetime(s, p)
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_datetime),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Datetimeval{
					Datetimeval: int64(dt),
				},
			},
		},
	}
}

func TestTime(t *testing.T) {
	s := "12:34:56"
	e := makeTimeExpr(s, 0)
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(testutil.NewProc(t), e)
	require.NoError(t, err)
	r, err := executor.Eval(testutil.NewProc(t), []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, r.Length())
}

func TestDatetime(t *testing.T) {
	s := "2019-12-12 12:34:56"
	e := makeDatetimeExpr(s, 0)
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(testutil.NewProc(t), e)
	require.NoError(t, err)
	r, err := executor.Eval(testutil.NewProc(t), []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, r.Length())
}
func TestTimestamp(t *testing.T) {
	s := "2019-12-12 12:34:56"
	e := makeTimestampExpr(s, 0, time.Local)
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(testutil.NewProc(t), e)
	require.NoError(t, err)
	r, err := executor.Eval(testutil.NewProc(t), []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, r.Length())
}
func TestDate(t *testing.T) {
	s := "2019-12-12"
	e := makeDateExpr(s)
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(testutil.NewProc(t), e)
	require.NoError(t, err)
	r, err := executor.Eval(testutil.NewProc(t), []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, r.Length())
}
