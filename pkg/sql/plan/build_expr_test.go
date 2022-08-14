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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/smartystreets/goconvey/convey"
)

func TestExpr_1(t *testing.T) {
	convey.Convey("selectAndStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "and")
			for j, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_C)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				constB, ok := exprC.C.Value.(*plan.Const_Bval)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[j])
			}
		}
	})
}

func TestExpr_2(t *testing.T) {
	convey.Convey("selectORStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "or")
			for j, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_C)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				constB, ok := exprC.C.Value.(*plan.Const_Bval)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[j])
			}
		}
	})
}

func TestExpr_3(t *testing.T) {
	convey.Convey("selectNotStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "not")
			for _, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
				exprC, ok := arg.Expr.(*plan.Expr_C)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				constB, ok := exprC.C.Value.(*plan.Const_Bval)
				if !ok {
					t.Fatalf("%+v", errors.New("the parse expr type is not right"))
				}
				convey.So(constB.Bval, convey.ShouldEqual, params[i])
			}
		}
	})
}

func TestExpr_4(t *testing.T) {
	convey.Convey("selectEqualStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "=")
		}
	})
}

func TestExpr_5(t *testing.T) {
	convey.Convey("selectLessStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<")
		}
	})
}

func TestExpr_6(t *testing.T) {
	convey.Convey("selectLessEqualStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<=")
		}
	})
}

func TestExpr_7(t *testing.T) {
	convey.Convey("selectGreatStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, ">")
		}
	})
}

func TestExpr_8(t *testing.T) {
	convey.Convey("selectGreatEqualStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, ">=")
		}
	})
}

func TestExpr_9(t *testing.T) {
	convey.Convey("selectGreatEqualStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "<>")
		}
	})
}

func TestExpr_A(t *testing.T) {
	convey.Convey("selectAndStmt succ", t, func() {
		mock := NewMockOptimizer()
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
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
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
		mock := NewMockOptimizer()
		// var params []bool = []bool{false, false, true, true}
		input := []string{"select 0 < 1 and 1 > 0 && not false from dual;"}
		for i := 0; i < len(input); i++ {
			pl, err := runOneExprStmt(mock, t, input[i])
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query, ok := pl.Plan.(*plan.Plan_Query)
			if !ok {
				t.Fatalf("%+v", errors.New("return type is not right"))
			}
			expr := query.Query.Nodes[1].ProjectList[0]
			exprF, ok := expr.Expr.(*plan.Expr_F)
			if !ok {
				t.Fatalf("%+v", errors.New("the parse expr type is not right"))
			}
			convey.So(expr.Typ.Id, convey.ShouldEqual, types.T_bool)
			convey.So(exprF.F.Func.ObjName, convey.ShouldEqual, "and")
			for _, arg := range exprF.F.Args {
				convey.So(arg.Typ.Id, convey.ShouldEqual, types.T_bool)
			}
		}
	})
}

func runOneExprStmt(opt Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(sql)
	if err != nil {
		return nil, err
	}
	ctx := opt.CurrentContext()

	var pl *plan.Plan
	for _, ast := range stmts {
		pl, err = BuildPlan(ctx, ast)
		if err != nil {
			return nil, err
		}
	}
	return pl, nil
}
