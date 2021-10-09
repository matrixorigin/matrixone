// Copyright 2021 Matrix Origin
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

package tree

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
	_ "github.com/pingcap/parser/test_driver"
	"github.com/pingcap/parser/types"
	"go/constant"
	"math"
	"reflect"
	"testing"
)

/**
https://github.com/pingcap/parser/blob/master/docs/quickstart.md
*/
func TestParser(t *testing.T) {
	p := parser.New()

	//insert into tbl2 select col_1c,max(col_1b), "K" from tbl1;
	//insert into tbl2 values (1,2),(3,4);
	sql := "create table table20 (`" + "` int);"

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Errorf("parser parse failed.error:%v", err)
		return
	}

	for _, sn := range stmtNodes {
		switch n := sn.(type) {
		case *ast.SelectStmt:
			ss := transformSelectStmtToSelect(n)

			fmt.Printf("ss %v\n",ss.Limit != nil )

		case *ast.SetOprStmt:
			ss := transformSetOprStmtToSelectStatement(n)
			if uc,ok := ss.(*Select); !ok{

			}else{
				fmt.Printf("all %v\n",uc.Limit != nil)
			}
		case *ast.InsertStmt:
			ss := transformInsertStmtToInsert(n)

			fmt.Printf("ss %v\n",ss.Rows != nil)
		case *ast.CreateTableStmt:
			ss := transformCreateTableStmtToCreateTable(n)

			fmt.Printf("ss %v\n",ss.IfNotExists)
		case *ast.CreateDatabaseStmt:
			ss := transformCreateDatabaseStmtToCreateDatabase(n)

			fmt.Printf("ss %v\n",ss.IfNotExists)
		case *ast.DropDatabaseStmt:
			ss := transformDropDatabaseStmtToDropDatabase(n)

			fmt.Printf("ss %v\n",ss.IfExists)
		case *ast.DeleteStmt:
			ss := transformDeleteStmtToDelete(n)

			fmt.Printf("ss %v \n",ss.String())
		case *ast.UpdateStmt:
			ss := transformUpdateStmtToUpdate(n)

			fmt.Printf("ss %v \n",ss.String())
		case *ast.LoadDataStmt:
			ss := transformLoadDataStmtToLoad(n)

			fmt.Printf("ss %v \n",ss.String())
		case *ast.BeginStmt:
			ss := transformBeginStmtToBeginTransaction(n)

			fmt.Printf("ss %v \n",ss.String())
		case *ast.ShowStmt:
			s := transformShowStmtToShow(n)
			ss := s.(*ShowIndex)

			fmt.Printf("ss %v \n",ss.showImpl)
		case *ast.ExplainStmt:
			s := transformExplainStmtToExplain(n)

			ss := s.(*ExplainStmt)

			fmt.Printf("ss %v \n",ss.Statement != nil)
		case *ast.CreateIndexStmt:
			s := transformCreateIndexStmtToCreateIndex(n)

			fmt.Printf("s %v \n",s.IfNotExists)
		case *ast.DropIndexStmt:
			s := transformDropIndexStmtToDropIndex(n)

			fmt.Printf("s %v \n",s.IfExists)
		case *ast.CreateUserStmt:
			s := transformCreatUserStmtToCreateUser(n)

			fmt.Printf("s %v \n",s.IfNotExists)
		case *ast.DropUserStmt:
			s := transformDropUserStmtToDropUser(n)

			fmt.Printf("s %v \n",s.IfExists)
		case *ast.RevokeStmt:
			s := transformRevokeStmtToRevoke(n)

			fmt.Printf("s %v \n",s.Users != nil)
		case *ast.RevokeRoleStmt:
			s := transformRevokeRoleStmtToRevoke(n)

			fmt.Printf("s %v \n",s.Users != nil)
		}

	}
}

func Test_transformDatumToNumVal(t *testing.T) {
	type args struct {
		datum *test_driver.Datum
	}

	t1 := test_driver.NewDatum(math.MaxInt64)
	t2 := test_driver.NewDatum(math.MinInt64)
	t3 := test_driver.NewDatum(nil)
	//this case is unwanted. Datum.SetUint64 is wrong.
	t4 := test_driver.NewDatum(math.MaxUint64 / 2)
	t5 := test_driver.NewDatum(0)
	t6 := test_driver.NewDatum(math.MaxFloat32)
	t7 := test_driver.NewDatum(-math.MaxFloat32)
	t8 := test_driver.NewDatum(math.MaxFloat64)
	t9 := test_driver.NewDatum(-math.MaxFloat64)

	s := "a string"
	t10 := test_driver.NewDatum(s)
	t11 := test_driver.NewDatum(true)
	t12 := test_driver.NewDatum(false)

	tests := []struct {
		name string
		args args
		want *NumVal
	}{
		{"t1", args{&t1}, NewNumVal(constant.MakeInt64(math.MaxInt64), "9223372036854775807", false)},
		{"t2", args{&t2}, NewNumVal(constant.MakeInt64(math.MinInt64), "-9223372036854775808", false)},
		{"t3", args{&t3}, NewNumVal(constant.MakeUnknown(), "NULL", false)},
		{"t4", args{&t4}, NewNumVal(constant.MakeUint64(math.MaxUint64/2), "9223372036854775807", false)},
		{"t5", args{&t5}, NewNumVal(constant.MakeUint64(0), "0", false)},
		{"t6", args{&t6}, NewNumVal(constant.MakeFloat64(math.MaxFloat32), "340282346638528860000000000000000000000", false)},
		{"t7", args{&t7}, NewNumVal(constant.MakeFloat64(-math.MaxFloat32), "-340282346638528860000000000000000000000", false)},
		{"t8", args{&t8}, NewNumVal(constant.MakeFloat64(math.MaxFloat64), "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", false)},
		{"t9", args{&t9}, NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", false)},
		{"t10", args{&t10}, NewNumVal(constant.MakeString(s), s, false)},
		{"t11", args{&t11}, NewNumVal(constant.MakeInt64(1), "1", false)},
		{"t12", args{&t12}, NewNumVal(constant.MakeInt64(0), "0", false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDatumToNumVal(tt.args.datum);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDatumToNumVal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformExprNodeToExpr(t *testing.T) {
	type args struct {
		node ast.ExprNode
	}

	t1 := ast.NewValueExpr(math.MaxInt64, "", "")
	t2 := ast.NewValueExpr(math.MinInt64, "", "")
	t3 := ast.NewValueExpr(nil, "", "")
	//this case is unwanted. Datum.SetUint64 is wrong.
	t4 := ast.NewValueExpr(math.MaxUint64/2, "", "")
	t5 := ast.NewValueExpr(0, "", "")
	t6 := ast.NewValueExpr(math.MaxFloat32, "", "")
	t7 := ast.NewValueExpr(-math.MaxFloat32, "", "")
	t8 := ast.NewValueExpr(math.MaxFloat64, "", "")
	t9 := ast.NewValueExpr(-math.MaxFloat64, "", "")
	s := "a string"
	t10 := ast.NewValueExpr(s, "", "")

	e1 := ast.NewValueExpr(1, "", "")
	e2 := ast.NewValueExpr(2, "", "")
	e3 := ast.NewValueExpr(3, "", "")
	e4 := ast.NewValueExpr(4, "", "")
	e5 := ast.NewValueExpr(5, "", "")
	e6 := ast.NewValueExpr(6, "", "")
	eTrue := ast.NewValueExpr(true, "", "")
	eFalse := ast.NewValueExpr(false, "", "")

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f3 := NewNumVal(constant.MakeInt64(3), "3", false)
	f4 := NewNumVal(constant.MakeInt64(4), "4", false)
	f5 := NewNumVal(constant.MakeInt64(5), "5", false)
	f6 := NewNumVal(constant.MakeInt64(6), "6", false)
	fTrue := NewNumVal(constant.MakeInt64(1), "1", false)
	fFalse := NewNumVal(constant.MakeInt64(0), "0", false)

	//2 * 3
	t11 := &ast.BinaryOperationExpr{
		Op: opcode.Mul,
		L:  e2,
		R:  e3,
	}

	t11Want := NewBinaryExpr(MULTI,
		f2,
		f3)

	//1 + 2 * 3
	t12 := &ast.BinaryOperationExpr{
		Op: opcode.Plus,
		L:  e1,
		R:  t11,
	}

	t12Want := NewBinaryExpr(PLUS,
		f1,
		t11Want)

	//1 + 2 * 3 + 4
	t13 := &ast.BinaryOperationExpr{
		Op: opcode.Plus,
		L:  t12,
		R:  e4,
	}

	t13Want := NewBinaryExpr(PLUS,
		t12Want,
		f4,
	)

	//1 + 2 * 3 + 4 - 5
	t14 := &ast.BinaryOperationExpr{
		Op: opcode.Minus,
		L:  t13,
		R:  e5,
	}

	t14Want := NewBinaryExpr(MINUS,
		t13Want,
		f5,
	)

	//-1 * 2
	t15_1 := &ast.UnaryOperationExpr{
		Op: opcode.Minus,
		V:  e1,
	}

	t15 := &ast.BinaryOperationExpr{
		Op: opcode.Mul,
		L:  t15_1,
		R:  e2,
	}

	t15Want := NewBinaryExpr(MULTI,
		NewUnaryExpr(UNARY_MINUS, f1),
		f2,
	)

	//+1 * 2
	t16_1 := &ast.UnaryOperationExpr{
		Op: opcode.Plus,
		V:  e1,
	}

	t16 := &ast.BinaryOperationExpr{
		Op: opcode.Mul,
		L:  t16_1,
		R:  e2,
	}

	t16Want := NewBinaryExpr(MULTI,
		NewUnaryExpr(UNARY_PLUS, f1),
		f2,
	)

	//~1 * 2
	t17_1 := &ast.UnaryOperationExpr{
		Op: opcode.BitNeg,
		V:  e1,
	}

	t17 := &ast.BinaryOperationExpr{
		Op: opcode.Mul,
		L:  t17_1,
		R:  e2,
	}

	t17Want := NewBinaryExpr(MULTI,
		NewUnaryExpr(UNARY_TILDE, f1),
		f2,
	)

	//!1
	t18 := &ast.UnaryOperationExpr{
		Op: opcode.Not2,
		V:  e1,
	}

	t18Want := NewNotExpr(f1)

	//1 | 2
	t21 := &ast.BinaryOperationExpr{
		Op: opcode.Or,
		L:  e1,
		R:  e2,
	}

	t21Want := NewBinaryExpr(BIT_OR,
		f1,
		f2)

	//1 & 2
	t22 := &ast.BinaryOperationExpr{
		Op: opcode.And,
		L:  e1,
		R:  e2,
	}

	t22Want := NewBinaryExpr(BIT_AND,
		f1,
		f2)

	//1 ^ 2
	t23 := &ast.BinaryOperationExpr{
		Op: opcode.Xor,
		L:  e1,
		R:  e2,
	}

	t23Want := NewBinaryExpr(BIT_XOR,
		f1,
		f2)

	//1 << 2
	t24 := &ast.BinaryOperationExpr{
		Op: opcode.LeftShift,
		L:  e1,
		R:  e2,
	}

	t24Want := NewBinaryExpr(LEFT_SHIFT,
		f1,
		f2)

	//1 >> 2
	t25 := &ast.BinaryOperationExpr{
		Op: opcode.RightShift,
		L:  e1,
		R:  e2,
	}

	t25Want := NewBinaryExpr(RIGHT_SHIFT,
		f1,
		f2)

	//1 + 2 * 3 + 4 - 5 / 6
	t26_1 := &ast.BinaryOperationExpr{
		Op: opcode.Div,
		L:  e5,
		R:  e6,
	}

	t26 := &ast.BinaryOperationExpr{
		Op: opcode.Minus,
		L:  t13,
		R:  t26_1,
	}

	t26want_1 := NewBinaryExpr(DIV, f5, f6)
	t26Want := NewBinaryExpr(MINUS,
		t13Want,
		t26want_1,
	)

	//1 % 2
	t27 := &ast.BinaryOperationExpr{
		Op: opcode.Mod,
		L:  e1,
		R:  e2,
	}

	t27Want := NewBinaryExpr(MOD,
		f1,
		f2)

	//1 div 2
	t28 := &ast.BinaryOperationExpr{
		Op: opcode.IntDiv,
		L:  e1,
		R:  e2,
	}

	t28Want := NewBinaryExpr(INTEGER_DIV,
		f1,
		f2)

	//1 = 2
	t29 := &ast.BinaryOperationExpr{
		Op: opcode.EQ,
		L:  e1,
		R:  e2,
	}

	t29Want := NewComparisonExpr(EQUAL,
		f1,
		f2)

	//1 < 2
	t30 := &ast.BinaryOperationExpr{
		Op: opcode.LT,
		L:  e1,
		R:  e2,
	}

	t30Want := NewComparisonExpr(LESS_THAN,
		f1,
		f2)

	//1 <= 2
	t31 := &ast.BinaryOperationExpr{
		Op: opcode.LE,
		L:  e1,
		R:  e2,
	}

	t31Want := NewComparisonExpr(LESS_THAN_EQUAL,
		f1,
		f2)

	//1 > 2
	t32 := &ast.BinaryOperationExpr{
		Op: opcode.GT,
		L:  e1,
		R:  e2,
	}

	t32Want := NewComparisonExpr(GREAT_THAN,
		f1,
		f2)

	//1 >= 2
	t33 := &ast.BinaryOperationExpr{
		Op: opcode.GE,
		L:  e1,
		R:  e2,
	}

	t33Want := NewComparisonExpr(GREAT_THAN_EQUAL,
		f1,
		f2)

	//1 <>,!= 2
	t34 := &ast.BinaryOperationExpr{
		Op: opcode.NE,
		L:  e1,
		R:  e2,
	}

	t34Want := NewComparisonExpr(NOT_EQUAL,
		f1,
		f2)

	//1 and 0
	t35 := &ast.BinaryOperationExpr{
		Op: opcode.LogicAnd,
		L:  e1,
		R:  e2,
	}

	t35Want := NewAndExpr(f1, f2)

	//1 or 0
	t36 := &ast.BinaryOperationExpr{
		Op: opcode.LogicOr,
		L:  e1,
		R:  e2,
	}

	t36Want := NewOrExpr(f1, f2)

	//not 1
	t37 := &ast.UnaryOperationExpr{
		Op: opcode.Not,
		V:  e1,
	}

	t37Want := NewNotExpr(f1)

	//1 xor 0
	t38 := &ast.BinaryOperationExpr{
		Op: opcode.LogicXor,
		L:  e1,
		R:  e2,
	}

	t38Want := NewXorExpr(f1, f2)

	// is null
	t39 := &ast.IsNullExpr{
		Expr: e1,
		Not:  false,
	}

	t39Want := NewIsNullExpr(f1)

	// is not null
	t40 := &ast.IsNullExpr{
		Expr: e1,
		Not:  true,
	}

	t40Want := NewIsNotNullExpr(f1)

	//2 IN (1,2,3,4);
	t41 := &ast.PatternInExpr{
		Expr: e2,
		List: []ast.ExprNode{e1, e2, e3, e4},
		Not:  false,
		Sel:  nil,
	}

	t41Want := NewComparisonExpr(IN, f2, &ExprList{
		Exprs: []Expr{f1, f2, f3, f4},
	})

	//2 NOT IN (1,2,3,4);
	t42 := &ast.PatternInExpr{
		Expr: e2,
		List: []ast.ExprNode{e1, e2, e3, e4},
		Not:  true,
		Sel:  nil,
	}

	t42Want := NewComparisonExpr(NOT_IN, f2, &ExprList{
		Exprs: []Expr{f1, f2, f3, f4},
	})

	//2 LIKE 'xxx';
	t43 := &ast.PatternLikeExpr{
		Expr:     e1,
		Pattern:  e2,
		Not:      false,
		Escape:   0,
		PatChars: nil,
		PatTypes: nil,
	}

	t43Want := NewComparisonExpr(LIKE, f1, f2)

	//2 NOT LIKE 'xxx';
	t44 := &ast.PatternLikeExpr{
		Expr:     e1,
		Pattern:  e2,
		Not:      true,
		Escape:   0,
		PatChars: nil,
		PatTypes: nil,
	}

	t44Want := NewComparisonExpr(NOT_LIKE, f1, f2)

	//2 REGEXP 'xxx';
	t45 := &ast.PatternRegexpExpr{
		Expr:    e1,
		Pattern: e2,
		Not:     false,
		Re:      nil,
		Sexpr:   nil,
	}

	t45Want := NewComparisonExpr(REG_MATCH, f1, f2)

	//2 NOT REGEXP 'xxx';
	t46 := &ast.PatternRegexpExpr{
		Expr:    e1,
		Pattern: e2,
		Not:     true,
		Re:      nil,
		Sexpr:   nil,
	}

	t46Want := NewComparisonExpr(NOT_REG_MATCH, f1, f2)

	t47subq, t47want_subq := gen_transform_t1()

	//SubqueryExpr SELECT t.a FROM sa.t,u
	t47 := &ast.SubqueryExpr{
		Query:      t47subq,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	t47Want := NewSubquery(t47want_subq.Select, false)

	//ExistsSubqueryExpr
	t48 := &ast.ExistsSubqueryExpr{
		Sel: e1,
		Not: true,
	}

	//just for passing the case
	var _ SelectStatement = f1
	t48Want := NewSubquery(f1, true)

	//CompareSubqueryExpr
	t49 := &ast.CompareSubqueryExpr{
		L:   e1,
		Op:  opcode.GT,
		R:   e2,
		All: true,
	}

	//just for passing the case
	var _ SelectStatement = f2
	t49Want := NewComparisonExprWithSubop(GREAT_THAN, ALL, f1, f2)

	// '(' e1 ')'
	t50 := &ast.ParenthesesExpr{Expr: e1}

	t50Want := NewParenExpr(f1)

	//ColumnNameExpr
	t51 := &ast.ColumnNameExpr{
		Name:  &ast.ColumnName{
			Schema: model.CIStr{O: "sch", L: "sch"},
			Table: model.CIStr{O: "t1", L: "sch"},
			Name: model.CIStr{O: "a", L: "sch"}},
		Refer: nil,
	}

	t51Want,_ := NewUnresolvedName("sch","t1","a")

	//FuncCallExpr
	t52 := &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		Schema: model.CIStr{O: "t1", L: "t1"},
		FnName: model.CIStr{O: "abs", L: "abs"},
		Args:   []ast.ExprNode{e1,e2,e3},
	}

	t52Fname,_ := NewUnresolvedName("t1","abs")
	t52Want := NewFuncExpr(0,t52Fname,[]Expr{f1,f2,f3},nil)

	//AggregateFuncExpr avg
	t53 := &ast.AggregateFuncExpr{
		F:        "avg",
		Args:     []ast.ExprNode{e1,e2,e3},
		Distinct: true,
		Order:    nil,
	}

	t53Fname,_ := NewUnresolvedName("avg")
	t53Want := NewFuncExpr(FUNC_TYPE_DISTINCT,t53Fname,[]Expr{f1,f2,f3},nil)

	//FuncCastExpr cast
	t54 := &ast.FuncCastExpr{
		Expr:            e1,
		Tp:              types.NewFieldType(mysql.TypeFloat),
		FunctionType:    0,
		ExplicitCharSet: false,
	}

	t54Want := NewCastExpr(f1,TYPE_FLOAT)

	//RowExpr (1,2,3,4)
	t55 := &ast.RowExpr{Values: []ast.ExprNode{e1,e2,e3,e4,&ast.DefaultExpr{}}}
	t55Want := NewTuple([]Expr{f1,f2,f3,f4,NewDefaultVal()})

	//BetweenExpr
	t56 := &ast.BetweenExpr{
		Expr:  e1,
		Left:  e2,
		Right: e3,
		Not:   true,
	}

	t56Want := NewRangeCond(true,f1,f2,f3)

	//CaseExpr
	t57When := []*ast.WhenClause{
		&ast.WhenClause{
			Expr:   e2,
			Result: e3,
		},
		&ast.WhenClause{
			Expr:   e4,
			Result: e5,
		},
	}
	t57 := &ast.CaseExpr{
		Value:       e1,
		WhenClauses: t57When,
		ElseClause:  e6,
	}

	t57Want_when :=[]*When{
		&When{Cond: f2,Val:f3},
		&When{Cond: f4,Val:f5},
	}
	t57Want := NewCaseExpr(f1,t57Want_when,f6)

	//TimeUnitExpr
	t58 := &ast.TimeUnitExpr{Unit: ast.TimeUnitSecond}
	t58Want := NewIntervalExpr(INTERVAL_TYPE_SECOND)

	//IsTruthExpr
	t59 := &ast.IsTruthExpr{
		Expr: e1,
		Not:  true,
		True: 1,
	}

	t59Want := NewComparisonExpr(IS_DISTINCT_FROM,f1,f1)

	//DefaultExpr
	t60 := &ast.DefaultExpr{}
	t60Want := NewDefaultVal()

	tests := []struct {
		name string
		args args
		want Expr
	}{
		{"t1", args{t1}, NewNumVal(constant.MakeInt64(math.MaxInt64), "9223372036854775807", false)},
		{"t2", args{t2}, NewNumVal(constant.MakeInt64(math.MinInt64), "-9223372036854775808", false)},
		{"t3", args{t3}, NewNumVal(constant.MakeUnknown(), "NULL", false)},
		{"t4", args{t4}, NewNumVal(constant.MakeUint64(math.MaxUint64/2), "9223372036854775807", false)},
		{"t5", args{t5}, NewNumVal(constant.MakeUint64(0), "0", false)},
		{"t6", args{t6}, NewNumVal(constant.MakeFloat64(math.MaxFloat32), "340282346638528860000000000000000000000", false)},
		{"t7", args{t7}, NewNumVal(constant.MakeFloat64(-math.MaxFloat32), "-340282346638528860000000000000000000000", false)},
		{"t8", args{t8}, NewNumVal(constant.MakeFloat64(math.MaxFloat64), "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", false)},
		{"t9", args{t9}, NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", false)},
		{"t10", args{t10}, NewNumVal(constant.MakeString(s), s, false)},
		{"t11", args{t11}, t11Want},
		{"t12", args{t12}, t12Want},
		{"t13", args{t13}, t13Want},
		{"t14", args{t14}, t14Want},
		{"t15", args{t15}, t15Want},
		{"t16", args{t16}, t16Want},
		{"t17", args{t17}, t17Want},
		{"t18", args{t18}, t18Want},
		{"t19", args{eTrue}, fTrue},
		{"t20", args{eFalse}, fFalse},
		{"t21", args{t21}, t21Want},
		{"t22", args{t22}, t22Want},
		{"t23", args{t23}, t23Want},
		{"t24", args{t24}, t24Want},
		{"t25", args{t25}, t25Want},
		{"t26", args{t26}, t26Want},
		{"t27", args{t27}, t27Want},
		{"t28", args{t28}, t28Want},
		{"t29", args{t29}, t29Want},
		{"t30", args{t30}, t30Want},
		{"t31", args{t31}, t31Want},
		{"t32", args{t32}, t32Want},
		{"t33", args{t33}, t33Want},
		{"t34", args{t34}, t34Want},
		{"t35", args{t35}, t35Want},
		{"t36", args{t36}, t36Want},
		{"t37", args{t37}, t37Want},
		{"t38", args{t38}, t38Want},
		{"t39", args{t39}, t39Want},
		{"t40", args{t40}, t40Want},
		{"t41", args{t41}, t41Want},
		{"t42", args{t42}, t42Want},
		{"t43", args{t43}, t43Want},
		{"t44", args{t44}, t44Want},
		{"t45", args{t45}, t45Want},
		{"t46", args{t46}, t46Want},
		{"t47", args{t47}, t47Want},
		{"t48", args{t48}, t48Want},
		{"t49", args{t49}, t49Want},
		{"t50", args{t50}, t50Want},
		{"t51", args{t51}, t51Want},
		{"t52", args{t52}, t52Want},
		{"t53", args{t53}, t53Want},
		{"t54", args{t54}, t54Want},
		{"t55", args{t55}, t55Want},
		{"t56", args{t56}, t56Want},
		{"t57", args{t57}, t57Want},
		{"t58", args{t58}, t58Want},
		{"t59", args{t59}, t59Want},
		{"t60", args{t60}, t60Want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformExprNodeToExpr(tt.args.node);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformExprNodeToExpr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformColumnNameListToNameList(t *testing.T) {
	type args struct {
		cn []*ast.ColumnName
	}
	l1 := []*ast.ColumnName{
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{O: "A", L: "a"},
		},
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{O: "B", L: "b"},
		},
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{O: "C", L: "c"},
		},
	}

	l1Want := IdentifierList{
		"A",
		"B",
		"C",
	}
	tests := []struct {
		name string
		args args
		want IdentifierList
	}{
		{"t1", args{l1}, l1Want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformColumnNameListToNameList(tt.args.cn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformColumnNameListToNameList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func gen_transform_t1() (*ast.SelectStmt, *Select) {
	//SELECT t.a FROM sa.t ;
	//SELECT t.a FROM sa.t,u ;
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{O: "sa", L: "sa"},
		Name:           model.CIStr{O: "t", L: "t"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource := &ast.TableSource{
		Source: t1TableName,
		AsName: model.CIStr{},
	}

	t1TableName2 := &ast.TableName{
		Schema:         model.CIStr{},
		Name:           model.CIStr{O: "u", L: "u"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource2 := &ast.TableSource{
		Source: t1TableName2,
		AsName: model.CIStr{},
	}

	t1Join := &ast.Join{
		Left:           t1TableSource,
		Right:          t1TableSource2,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}
	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1ColumnName := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "t", L: "t"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1ColumnNameExpr := &ast.ColumnNameExpr{
		Name:  t1ColumnName,
		Refer: nil,
	}
	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1ColumnNameExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	t1wantTableName := &TableName{
		objName: objName{
			ObjectName: "t",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr: t1wantTableName,
		As:   AliasClause{},
	}

	t1wantTableName2 := &TableName{
		objName: objName{
			ObjectName: "u",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr: t1wantTableName2,
		As:   AliasClause{},
	}

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     t1wantAliasedTableExpr,
			Right:    t1wantAliasedTableExpr2,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantFied, _ := NewUnresolvedName("", "t", "a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: t1wantFied,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t2() (*ast.SelectStmt, *Select) {
	//SELECT t.a FROM sa.t,u,v
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{O: "sa", L: "sa"},
		Name:           model.CIStr{O: "t", L: "t"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource := &ast.TableSource{
		Source: t1TableName,
		AsName: model.CIStr{},
	}

	t1TableName2 := &ast.TableName{
		Schema:         model.CIStr{},
		Name:           model.CIStr{O: "u", L: "u"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource2 := &ast.TableSource{
		Source: t1TableName2,
		AsName: model.CIStr{},
	}

	t1TableName3 := &ast.TableName{
		Schema:         model.CIStr{},
		Name:           model.CIStr{O: "v", L: "v"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource3 := &ast.TableSource{
		Source: t1TableName3,
		AsName: model.CIStr{},
	}

	t1Join := &ast.Join{
		Left:           t1TableSource,
		Right:          t1TableSource2,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1Join2 := &ast.Join{
		Left:           t1Join,
		Right:          t1TableSource3,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join2}

	t1ColumnName := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "t", L: "t"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1ColumnNameExpr := &ast.ColumnNameExpr{
		Name:  t1ColumnName,
		Refer: nil,
	}
	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1ColumnNameExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	t1wantTableName := &TableName{
		objName: objName{
			ObjectName: "t",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr: t1wantTableName,
		As:   AliasClause{},
	}

	t1wantTableName2 := &TableName{
		objName: objName{
			ObjectName: "u",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr: t1wantTableName2,
		As:   AliasClause{},
	}

	t1wantTableName3 := &TableName{
		objName: objName{
			ObjectName: "v",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr3 := &AliasedTableExpr{
		Expr: t1wantTableName3,
		As:   AliasClause{},
	}

	t1wantJoin_1_2 := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     t1wantAliasedTableExpr,
		Right:    t1wantAliasedTableExpr2,
		Cond:     nil,
	}

	t1wantJoin_1_2_Join_3 := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     t1wantJoin_1_2,
		Right:    t1wantAliasedTableExpr3,
		Cond:     nil,
	}

	t1wantTableExprArray := []TableExpr{
		t1wantJoin_1_2_Join_3,
	}

	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantFied, _ := NewUnresolvedName("", "t", "a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: t1wantFied,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t3() (*ast.SelectStmt, *Select) {
	//SELECT t.a,u.a FROM sa.t,u where t.a = u.a
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{O: "sa", L: "sa"},
		Name:           model.CIStr{O: "t", L: "t"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource := &ast.TableSource{
		Source: t1TableName,
		AsName: model.CIStr{},
	}

	t1TableName2 := &ast.TableName{
		Schema:         model.CIStr{},
		Name:           model.CIStr{O: "u", L: "u"},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	t1TableSource2 := &ast.TableSource{
		Source: t1TableName2,
		AsName: model.CIStr{},
	}

	t1Join := &ast.Join{
		Left:           t1TableSource,
		Right:          t1TableSource2,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}
	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1ColumnName := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "t", L: "t"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1ColumnNameExpr := &ast.ColumnNameExpr{
		Name:  t1ColumnName,
		Refer: nil,
	}

	t1ColumnName2 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "u", L: "u"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1ColumnNameExpr2 := &ast.ColumnNameExpr{
		Name:  t1ColumnName2,
		Refer: nil,
	}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1ColumnNameExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1ColumnNameExpr2,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1ColumnName3 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "t", L: "t"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1Where1 := &ast.ColumnNameExpr{
		Name:  t1ColumnName3,
		Refer: nil,
	}

	t1ColumnName4 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{O: "u", L: "u"},
		Name:   model.CIStr{O: "a", L: "a"},
	}
	t1Where2 := &ast.ColumnNameExpr{
		Name:  t1ColumnName4,
		Refer: nil,
	}

	t1Where := &ast.BinaryOperationExpr{
		Op: opcode.EQ,
		L:  t1Where1,
		R:  t1Where2,
	}
	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t1Where,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	t1wantTableName := &TableName{
		objName: objName{
			ObjectName: "t",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr: t1wantTableName,
		As:   AliasClause{},
	}

	t1wantTableName2 := &TableName{
		objName: objName{
			ObjectName: "u",
			ObjectNamePrefix: ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr: t1wantTableName2,
		As:   AliasClause{},
	}

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     t1wantAliasedTableExpr,
			Right:    t1wantAliasedTableExpr2,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantField1, _ := NewUnresolvedName("", "t", "a")
	t1wantField2, _ := NewUnresolvedName("", "u", "a")

	t1wantWhere1, _ := NewUnresolvedName("", "t", "a")
	t1wantWhere2, _ := NewUnresolvedName("", "u", "a")

	t1wantWhereExpr := &ComparisonExpr{
		Op:    EQUAL,
		SubOp: 0,
		Left:  t1wantWhere1,
		Right: t1wantWhere2,
	}

	t1wantFieldList := []SelectExpr{
		{
			Expr: t1wantField1,
			As:   "",
		},
		{
			Expr: t1wantField2,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(t1wantWhereExpr),
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_var_ref(schema, table, name string) *ast.ColumnNameExpr {
	var_name := &ast.ColumnName{
		Schema: model.CIStr{O: schema, L: schema},
		Table:  model.CIStr{O: table, L: table},
		Name:   model.CIStr{O: name, L: name},
	}
	var_ := &ast.ColumnNameExpr{
		Name:  var_name,
		Refer: nil,
	}
	return var_
}

func gen_binary_expr(op opcode.Op, l, r ast.ExprNode) *ast.BinaryOperationExpr {
	return &ast.BinaryOperationExpr{
		Op: op,
		L:  l,
		R:  r,
	}
}

func gen_table(sch, name string) *ast.TableSource {
	sa_t_name := &ast.TableName{
		Schema:         model.CIStr{O: sch, L: sch},
		Name:           model.CIStr{O: name, L: name},
		DBInfo:         nil,
		TableInfo:      nil,
		IndexHints:     nil,
		PartitionNames: nil,
		TableSample:    nil,
	}

	sa_t := &ast.TableSource{
		Source: sa_t_name,
		AsName: model.CIStr{},
	}
	return sa_t
}

func gen_want_table(sch, name string) *AliasedTableExpr {
	want_sa_t_name := NewTableName(Identifier(name), ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      Identifier(sch),
		ExplicitCatalog: false,
		ExplicitSchema:  len(sch) != 0,
	})

	want_sa_t := &AliasedTableExpr{
		Expr: want_sa_t_name,
		As:   AliasClause{},
	}
	return want_sa_t
}

func gen_transform_t4() (*ast.SelectStmt, *Select) {
	//SELECT t.a,u.a,t.b * u.b FROM sa.t,u where t.a = u.a and t.b > u.b
	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}
	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u}

	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_table_refs := []TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     want_sa_t,
			Right:    want_u,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: want_table_refs}

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t5() (*ast.SelectStmt, *Select) {
	//SELECT t.a,u.a,t.b * u.b FROM sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b
	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")
	t_c := gen_var_ref("", "t", "c")
	t_d := gen_var_ref("", "t", "d")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")
	u_c := gen_var_ref("", "u", "c")
	u_d := gen_var_ref("", "u", "d")

	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ, t_c, u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE, t_d, u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr, t_c_eq_u_c, t_d_ne_u_d)

	onCond := &ast.OnCondition{Expr: t_c_eq_u_c_or_t_d_ne_u_d}

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}
	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u}

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//=========

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")
	want_t_c, _ := NewUnresolvedName("", "t", "c")
	want_t_d, _ := NewUnresolvedName("", "t", "d")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")
	want_u_d, _ := NewUnresolvedName("", "u", "d")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL, want_t_c, want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL, want_t_d, want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c, want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_table_refs := []TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     want_sa_t,
			Right:    want_u,
			Cond:     want_join_on,
		},
	}
	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t6() (*ast.SelectStmt, *Select) {
	/*
		SELECT t.a,u.a,t.b * u.b
					FROM sa.t join u on t.c = u.c or t.d != u.d
							  join v on u.a != v.a
					where t.a = u.a and t.b > u.b
	*/
	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")
	t_c := gen_var_ref("", "t", "c")
	t_d := gen_var_ref("", "t", "d")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")
	u_c := gen_var_ref("", "u", "c")
	u_d := gen_var_ref("", "u", "d")

	v_a := gen_var_ref("", "v", "a")
	//v_b := gen_var_ref("","v","b")
	//v_c := gen_var_ref("","v","c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	v := gen_table("", "v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ, t_c, u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE, t_d, u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr, t_c_eq_u_c, t_d_ne_u_d)

	t_u_onCond := &ast.OnCondition{Expr: t_c_eq_u_c_or_t_d_ne_u_d}

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             t_u_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	//u.a != v.a
	u_a_ne_v_a := gen_binary_expr(opcode.NE, u_a, v_a)

	u_v_onCond := &ast.OnCondition{Expr: u_a_ne_v_a}

	sa_t_cross_u_cross_v := &ast.Join{
		Left:           sa_t_cross_u,
		Right:          v,
		Tp:             ast.CrossJoin,
		On:             u_v_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u_cross_v}

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//=========

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")
	want_t_c, _ := NewUnresolvedName("", "t", "c")
	want_t_d, _ := NewUnresolvedName("", "t", "d")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")
	want_u_d, _ := NewUnresolvedName("", "u", "d")

	want_v_a, _ := NewUnresolvedName("", "v", "a")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL, want_t_c, want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL, want_t_d, want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c, want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL, want_u_a, want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_v := gen_want_table("", "v")

	want_t_u_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_t_u_join,
		Right:    want_v,
		Cond:     want_u_v_join_on,
	}

	want_table_refs := []TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t7() (*ast.SelectStmt, *Select) {
	/*
		SELECT t.a,u.a,t.b * u.b
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
	*/
	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")
	t_c := gen_var_ref("", "t", "c")
	t_d := gen_var_ref("", "t", "d")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")
	u_c := gen_var_ref("", "u", "c")
	u_d := gen_var_ref("", "u", "d")

	v_a := gen_var_ref("", "v", "a")
	v_b := gen_var_ref("", "v", "b")
	//v_c := gen_var_ref("","v","c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	v := gen_table("", "v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ, t_c, u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE, t_d, u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr, t_c_eq_u_c, t_d_ne_u_d)

	t_u_onCond := &ast.OnCondition{Expr: t_c_eq_u_c_or_t_d_ne_u_d}

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             t_u_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	//u.a != v.a
	u_a_ne_v_a := gen_binary_expr(opcode.NE, u_a, v_a)

	u_v_onCond := &ast.OnCondition{Expr: u_a_ne_v_a}

	sa_t_cross_u_cross_v := &ast.Join{
		Left:           sa_t_cross_u,
		Right:          v,
		Tp:             ast.CrossJoin,
		On:             u_v_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u_cross_v}

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	//group by t.a,u.a,(t.b+u.b+v.b)

	byitem_t_a := &ast.ByItem{
		Expr:      t_a,
		Desc:      false,
		NullOrder: true,
	}

	byitem_u_a := &ast.ByItem{
		Expr:      u_a,
		Desc:      false,
		NullOrder: true,
	}

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus, gen_binary_expr(opcode.Plus, t_b, u_b), v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item := []*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby := &ast.GroupByClause{Items: t_groupby_item}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          t_groupby,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//=========

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")
	want_t_c, _ := NewUnresolvedName("", "t", "c")
	want_t_d, _ := NewUnresolvedName("", "t", "d")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")
	want_u_d, _ := NewUnresolvedName("", "u", "d")

	want_v_a, _ := NewUnresolvedName("", "v", "a")
	want_v_b, _ := NewUnresolvedName("", "v", "b")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL, want_t_c, want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL, want_t_d, want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c, want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL, want_u_a, want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_v := gen_want_table("", "v")

	want_t_u_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_t_u_join,
		Right:    want_v,
		Cond:     want_u_v_join_on,
	}

	want_table_refs := []TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS, NewBinaryExpr(PLUS, want_t_b, want_u_b), want_v_b)

	want_paren_t_b_plus_u_b_plus_v_b := NewParenExpr(want_t_b_plus_u_b_plus_v_b)

	//group by t.a,u.a,(t.b+u.b+v.b)
	want_groupby := []Expr{
		want_t_a,
		want_u_a,
		want_paren_t_b_plus_u_b_plus_v_b,
	}

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  want_groupby,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t8() (*ast.SelectStmt, *Select) {
	/*
		SELECT t.a,u.a,t.b * u.b
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
		having t.a = 'jj' and v.c > 1000
	*/
	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")
	t_c := gen_var_ref("", "t", "c")
	t_d := gen_var_ref("", "t", "d")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")
	u_c := gen_var_ref("", "u", "c")
	u_d := gen_var_ref("", "u", "d")

	v_a := gen_var_ref("", "v", "a")
	v_b := gen_var_ref("", "v", "b")
	v_c := gen_var_ref("", "v", "c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	v := gen_table("", "v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ, t_c, u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE, t_d, u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr, t_c_eq_u_c, t_d_ne_u_d)

	t_u_onCond := &ast.OnCondition{Expr: t_c_eq_u_c_or_t_d_ne_u_d}

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             t_u_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	//u.a != v.a
	u_a_ne_v_a := gen_binary_expr(opcode.NE, u_a, v_a)

	u_v_onCond := &ast.OnCondition{Expr: u_a_ne_v_a}

	sa_t_cross_u_cross_v := &ast.Join{
		Left:           sa_t_cross_u,
		Right:          v,
		Tp:             ast.CrossJoin,
		On:             u_v_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u_cross_v}

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	//group by t.a,u.a,(t.b+u.b+v.b)

	byitem_t_a := &ast.ByItem{
		Expr:      t_a,
		Desc:      false,
		NullOrder: true,
	}

	byitem_u_a := &ast.ByItem{
		Expr:      u_a,
		Desc:      false,
		NullOrder: true,
	}

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus, gen_binary_expr(opcode.Plus, t_b, u_b), v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item := []*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby := &ast.GroupByClause{Items: t_groupby_item}

	//having t.a = 'jj' and v.c > 1000

	t_jj := ast.NewValueExpr("jj", "", "")
	t_a_eq_tjj := gen_binary_expr(opcode.EQ, t_a, t_jj)
	v_c_gt_1000 := gen_binary_expr(opcode.GT, v_c, ast.NewValueExpr(1000, "", ""))
	t_a_eq_tjj_and_v_c_gt_1000 := gen_binary_expr(opcode.LogicAnd, t_a_eq_tjj, v_c_gt_1000)

	t_having := &ast.HavingClause{Expr: t_a_eq_tjj_and_v_c_gt_1000}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          t_groupby,
		Having:           t_having,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//=========

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")
	want_t_c, _ := NewUnresolvedName("", "t", "c")
	want_t_d, _ := NewUnresolvedName("", "t", "d")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")
	want_u_d, _ := NewUnresolvedName("", "u", "d")

	want_v_a, _ := NewUnresolvedName("", "v", "a")
	want_v_b, _ := NewUnresolvedName("", "v", "b")
	want_v_c, _ := NewUnresolvedName("", "v", "c")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL, want_t_c, want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL, want_t_d, want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c, want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL, want_u_a, want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_v := gen_want_table("", "v")

	want_t_u_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_t_u_join,
		Right:    want_v,
		Cond:     want_u_v_join_on,
	}

	want_table_refs := []TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS, NewBinaryExpr(PLUS, want_t_b, want_u_b), want_v_b)

	want_paren_t_b_plus_u_b_plus_v_b := NewParenExpr(want_t_b_plus_u_b_plus_v_b)

	//group by t.a,u.a,(t.b+u.b+v.b)
	want_groupby := []Expr{
		want_t_a,
		want_u_a,
		want_paren_t_b_plus_u_b_plus_v_b,
	}

	//having t.a = 'jj' and v.c > 1000
	want_tjj := NewNumVal(constant.MakeString("jj"), "jj", false)
	want_t_a_eq_tjj := NewComparisonExpr(EQUAL, want_t_a, want_tjj)
	want_v_c_gt_1000 := NewComparisonExpr(GREAT_THAN, want_v_c, NewNumVal(constant.MakeInt64(1000), "1000", false))
	want_t_a_eq_tjj_and_v_c_gt_1000 := NewAndExpr(want_t_a_eq_tjj, want_v_c_gt_1000)
	want_having := NewWhere(want_t_a_eq_tjj_and_v_c_gt_1000)
	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  want_groupby,
		Having:   want_having,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t9() (*ast.SelectStmt, *Select) {
	/*
		SELECT t.a,u.a,t.b * u.b tubb
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
		having t.a = 'jj' and v.c > 1000
		order by t.a asc,u.a desc,v.d asc,tubb
		limit 100,2000
	*/
	t_a := gen_var_ref("", "t", "a")
	t_b := gen_var_ref("", "t", "b")
	t_c := gen_var_ref("", "t", "c")
	t_d := gen_var_ref("", "t", "d")

	u_a := gen_var_ref("", "u", "a")
	u_b := gen_var_ref("", "u", "b")
	u_c := gen_var_ref("", "u", "c")
	u_d := gen_var_ref("", "u", "d")

	v_a := gen_var_ref("", "v", "a")
	v_b := gen_var_ref("", "v", "b")
	v_c := gen_var_ref("", "v", "c")
	v_d := gen_var_ref("", "v", "d")
	tubb := gen_var_ref("", "", "tubb")

	sa_t := gen_table("sa", "t")

	u := gen_table("", "u")

	v := gen_table("", "v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ, t_c, u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE, t_d, u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr, t_c_eq_u_c, t_d_ne_u_d)

	t_u_onCond := &ast.OnCondition{Expr: t_c_eq_u_c_or_t_d_ne_u_d}

	sa_t_cross_u := &ast.Join{
		Left:           sa_t,
		Right:          u,
		Tp:             ast.CrossJoin,
		On:             t_u_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	//u.a != v.a
	u_a_ne_v_a := gen_binary_expr(opcode.NE, u_a, v_a)

	u_v_onCond := &ast.OnCondition{Expr: u_a_ne_v_a}

	sa_t_cross_u_cross_v := &ast.Join{
		Left:           sa_t_cross_u,
		Right:          v,
		Tp:             ast.CrossJoin,
		On:             u_v_onCond,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: sa_t_cross_u_cross_v}

	t_b_multi_u_b := gen_binary_expr(opcode.Mul, t_b, u_b)
	select_fields := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      u_a,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t_b_multi_u_b,
			AsName:    model.CIStr{O: "tubb", L: "tubb"},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ, t_a, u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT, t_b, u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd, t_a_eq_u_a, t_b_gt_u_b)

	//group by t.a,u.a,(t.b+u.b+v.b)

	byitem_t_a := &ast.ByItem{
		Expr:      t_a,
		Desc:      false,
		NullOrder: true,
	}

	byitem_u_a := &ast.ByItem{
		Expr:      u_a,
		Desc:      false,
		NullOrder: true,
	}

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus, gen_binary_expr(opcode.Plus, t_b, u_b), v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item := []*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby := &ast.GroupByClause{Items: t_groupby_item}

	//having t.a = 'jj' and v.c > 1000

	t_jj := ast.NewValueExpr("jj", "", "")
	t_a_eq_tjj := gen_binary_expr(opcode.EQ, t_a, t_jj)
	v_c_gt_1000 := gen_binary_expr(opcode.GT, v_c, ast.NewValueExpr(1000, "", ""))
	t_a_eq_tjj_and_v_c_gt_1000 := gen_binary_expr(opcode.LogicAnd, t_a_eq_tjj, v_c_gt_1000)

	t_having := &ast.HavingClause{Expr: t_a_eq_tjj_and_v_c_gt_1000}

	//order by t.a asc,u.a desc,v.d asc,tubb
	byitem_t_a_asc := &ast.ByItem{
		Expr:      t_a,
		Desc:      false,
		NullOrder: false,
	}

	byitem_u_a_desc := &ast.ByItem{
		Expr:      u_a,
		Desc:      true,
		NullOrder: false,
	}

	byitem_v_d_asc := &ast.ByItem{
		Expr:      v_d,
		Desc:      false,
		NullOrder: false,
	}

	byitem_tubb := &ast.ByItem{
		Expr:      tubb,
		Desc:      false,
		NullOrder: false,
	}

	t_orderby_items := []*ast.ByItem{
		byitem_t_a_asc,
		byitem_u_a_desc,
		byitem_v_d_asc,
		byitem_tubb,
	}

	t_orderby := &ast.OrderByClause{
		Items:    t_orderby_items,
		ForUnion: false,
	}

	//limit 100,2000
	t_limit := &ast.Limit{
		Count:  ast.NewValueExpr(2000, "", ""),
		Offset: ast.NewValueExpr(100, "", ""),
	}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t_a_eq_u_a_and_t_b_gt_u_b,
		Fields:           t1FieldList,
		GroupBy:          t_groupby,
		Having:           t_having,
		WindowSpecs:      nil,
		OrderBy:          t_orderby,
		Limit:            t_limit,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//=========

	want_t_a, _ := NewUnresolvedName("", "t", "a")
	want_t_b, _ := NewUnresolvedName("", "t", "b")
	want_t_c, _ := NewUnresolvedName("", "t", "c")
	want_t_d, _ := NewUnresolvedName("", "t", "d")

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")
	want_u_d, _ := NewUnresolvedName("", "u", "d")

	want_v_a, _ := NewUnresolvedName("", "v", "a")
	want_v_b, _ := NewUnresolvedName("", "v", "b")
	want_v_c, _ := NewUnresolvedName("", "v", "c")
	want_v_d, _ := NewUnresolvedName("", "v", "d")

	want_tubb, _ := NewUnresolvedName("", "", "tubb")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL, want_t_c, want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL, want_t_d, want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c, want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL, want_u_a, want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t := gen_want_table("sa", "t")

	want_u := gen_want_table("", "u")

	want_v := gen_want_table("", "v")

	want_t_u_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_t_u_join,
		Right:    want_v,
		Cond:     want_u_v_join_on,
	}

	want_table_refs := []TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI, want_t_b, want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL, want_t_a, want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN, want_t_b, want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a, want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS, NewBinaryExpr(PLUS, want_t_b, want_u_b), want_v_b)

	want_paren_t_b_plus_u_b_plus_v_b := NewParenExpr(want_t_b_plus_u_b_plus_v_b)

	//group by t.a,u.a,(t.b+u.b+v.b)
	want_groupby := []Expr{
		want_t_a,
		want_u_a,
		want_paren_t_b_plus_u_b_plus_v_b,
	}

	//having t.a = 'jj' and v.c > 1000
	want_tjj := NewNumVal(constant.MakeString("jj"), "jj", false)
	want_t_a_eq_tjj := NewComparisonExpr(EQUAL, want_t_a, want_tjj)
	num1000 := NewNumVal(constant.MakeInt64(1000), "1000", false)
	want_v_c_gt_1000 := NewComparisonExpr(GREAT_THAN, want_v_c, num1000)
	want_t_a_eq_tjj_and_v_c_gt_1000 := NewAndExpr(want_t_a_eq_tjj, want_v_c_gt_1000)
	want_having := NewWhere(want_t_a_eq_tjj_and_v_c_gt_1000)

	////order by t.a asc,u.a desc,v.d asc,tubb
	want_orderby := []*Order{
		NewOrder(want_t_a, Ascending, false),
		NewOrder(want_u_a, Descending, false),
		NewOrder(want_v_d, Ascending, false),
		NewOrder(want_tubb, Ascending, false),
	}

	//limit 100,2000
	num100 := NewNumVal(constant.MakeInt64(100), "100", false)
	num2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)

	want_limit := NewLimit(num100, num2000)

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_t_a,
			As:   "",
		},
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: want_t_b_multi_u_b,
			As:   "tubb",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:    t1wantFieldList,
		GroupBy:  want_groupby,
		Having:   want_having,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: want_orderby,
		Limit:   want_limit,
	}
	return t1, t1want
}

func gen_transform_t10() (*ast.SelectStmt, *Select) {
	//SELECT *	FROM u;

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1_wildcard := &ast.WildCardField{}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  t1_wildcard,
			Expr:      nil,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_u := gen_want_table("", "u")

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: "",
			Left:     want_u,
			Right:    nil,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_star := StarExpr()

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_star,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t11() (*ast.SelectStmt, *Select) {
	//SELECT u.*	FROM u;

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1_wildcard := &ast.WildCardField{Table: model.NewCIStr("u")}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  t1_wildcard,
			Expr:      nil,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_u := gen_want_table("", "u")

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: "",
			Left:     want_u,
			Right:    nil,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_star, _ := NewUnresolvedNameWithStar("u")

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_star,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t12() (*ast.SelectStmt, *Select) {
	/*
		SELECT abs(u.a),count(u.b),cast(u.c as char)
		from u
	*/

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1_func_call_agg := []ast.ExprNode{
		gen_var_ref("", "u", "a"),
	}

	t1_func_call := &ast.FuncCallExpr{
		Tp:     0,
		Schema: model.CIStr{},
		FnName: model.CIStr{O: "abs", L: "abs"},
		Args:   t1_func_call_agg,
	}

	t1_agg_func_agg := []ast.ExprNode{
		gen_var_ref("", "u", "b"),
	}

	t1_agg_func := &ast.AggregateFuncExpr{
		F:        "count",
		Args:     t1_agg_func_agg,
		Distinct: false,
		Order:    nil,
	}

	t1_func_cast := &ast.FuncCastExpr{
		Expr:            gen_var_ref("", "u", "c"),
		Tp:              types.NewFieldType(253),
		FunctionType:    0,
		ExplicitCharSet: false,
	}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1_func_call,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1_agg_func,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      t1_func_cast,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_u := gen_want_table("", "u")

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: "",
			Left:     want_u,
			Right:    nil,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_u_a, _ := NewUnresolvedName("", "u", "a")
	want_u_b, _ := NewUnresolvedName("", "u", "b")
	want_u_c, _ := NewUnresolvedName("", "u", "c")

	want_abs, _ := NewUnresolvedName("", "abs")
	want_call_abs := NewFuncExpr(0, want_abs, []Expr{want_u_a}, nil)

	want_count, _ := NewUnresolvedName("count")
	want_call_count := NewFuncExpr(FUNC_TYPE_ALL, want_count, []Expr{want_u_b}, nil)

	want_call_cast := NewCastExpr(want_u_c, TYPE_VARSTRING)

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_call_abs,
			As:   "",
		},
		{
			Expr: want_call_count,
			As:   "",
		},
		{
			Expr: want_call_cast,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t13() (*ast.SelectStmt, *Select) {
	/*
		SELECT u.a,(SELECT t.a FROM sa.t,u)
		from u
	*/

	sub, want_sub := gen_transform_t1()
	subExpr := &ast.SubqueryExpr{
		Query:      sub,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      gen_var_ref("", "u", "a"),
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      subExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_u := gen_want_table("", "u")

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: "",
			Left:     want_u,
			Right:    nil,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_u_a, _ := NewUnresolvedName("", "u", "a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: NewSubquery(want_sub.Select, false),
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t14() (*ast.SelectStmt, *Select) {
	/*
		SELECT u.a,(SELECT t.a FROM sa.t,u)
		from u,(SELECT t.a,u.a FROM sa.t,u where t.a = u.a)
	*/

	sub, want_sub := gen_transform_t1()
	subExpr := &ast.SubqueryExpr{
		Query:      sub,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	sub1, want_sub1 := gen_transform_t3()
	subExpr1 := &ast.SubqueryExpr{
		Query:      sub1,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	from_sub := &ast.TableSource{
		Source: subExpr1,
		AsName: model.CIStr{},
	}

	t1Join2 := &ast.Join{
		Left:           t1Join,
		Right:          from_sub,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join2}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      gen_var_ref("", "u", "a"),
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      subExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            nil,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	want_u := gen_want_table("", "u")

	from2 := NewAliasedTableExpr(NewSubquery(want_sub1.Select, false), AliasClause{
		Alias: "",
	})

	wantjoin1 := NewJoinTableExpr("", want_u, nil, nil)

	t1wantTableExprArray := []TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     wantjoin1,
			Right:    from2,
			Cond:     nil,
		},
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_u_a, _ := NewUnresolvedName("", "u", "a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: NewSubquery(want_sub.Select, false),
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    nil,
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func gen_transform_t15() (*ast.SelectStmt, *Select) {
	/*
		SELECT u.a,(SELECT t.a FROM sa.t,u)
		from u,(SELECT t.a,u.a FROM sa.t,u where t.a = u.a)
		where (u.a,u.b,u.c) in (SELECT t.a,u.a,t.b * u.b tubb
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
		having t.a = 'jj' and v.c > 1000
		order by t.a asc,u.a desc,v.d asc,tubb
		limit 100,2000)
	*/

	sub, want_sub := gen_transform_t1()
	subExpr := &ast.SubqueryExpr{
		Query:      sub,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	sub1, want_sub1 := gen_transform_t3()
	subExpr1 := &ast.SubqueryExpr{
		Query:      sub1,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	sub2, want_sub2 := gen_transform_t9()
	subExpr2 := &ast.SubqueryExpr{
		Query:      sub2,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	t1row := []ast.ExprNode{
		gen_var_ref("", "u", "a"),
		gen_var_ref("", "u", "b"),
		gen_var_ref("", "u", "c"),
	}

	t1rowExpr := &ast.RowExpr{Values: t1row}

	t1_row_in_expr := &ast.PatternInExpr{
		Expr: t1rowExpr,
		List: nil,
		Not:  false,
		Sel:  subExpr2,
	}

	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	from_sub := &ast.TableSource{
		Source: subExpr1,
		AsName: model.CIStr{},
	}

	t1Join2 := &ast.Join{
		Left:           t1Join,
		Right:          from_sub,
		Tp:             ast.CrossJoin,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join2}

	t1SelectField := []*ast.SelectField{
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      gen_var_ref("", "u", "a"),
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
		&ast.SelectField{
			Offset:    0,
			WildCard:  nil,
			Expr:      subExpr,
			AsName:    model.CIStr{},
			Auxiliary: false,
		},
	}

	t1FieldList := &ast.FieldList{Fields: t1SelectField}

	t1 := &ast.SelectStmt{
		SelectStmtOpts:   nil,
		Distinct:         false,
		From:             t1TableRef,
		Where:            t1_row_in_expr,
		Fields:           t1FieldList,
		GroupBy:          nil,
		Having:           nil,
		WindowSpecs:      nil,
		OrderBy:          nil,
		Limit:            nil,
		LockInfo:         nil,
		TableHints:       nil,
		IsInBraces:       false,
		QueryBlockOffset: 0,
		SelectIntoOpt:    nil,
		AfterSetOperator: nil,
		Kind:             0,
		Lists:            nil,
	}

	//
	tp_want_u_a, _ := NewUnresolvedName("", "u", "a")
	tp_want_u_b, _ := NewUnresolvedName("", "u", "b")
	tp_want_u_c, _ := NewUnresolvedName("", "u", "c")

	want_tuple := []Expr{
		tp_want_u_a,
		tp_want_u_b,
		tp_want_u_c,
	}

	tupleExpr := NewTuple(want_tuple)

	want_in_expr := NewComparisonExpr(IN, tupleExpr, NewSubquery(want_sub2.Select, false))

	want_joinu := gen_want_table("", "u")

	from2 := NewAliasedTableExpr(NewSubquery(want_sub1.Select, false), AliasClause{
		Alias: "",
	})

	wantjoin1 := NewJoinTableExpr(JOIN_TYPE_CROSS, NewJoinTableExpr("", want_joinu, nil, nil), from2, nil)

	t1wantTableExprArray := []TableExpr{
		//&JoinTableExpr{
		//	JoinType:  "",
		//	Left:      wantjoin1,
		//	Right:     nil,
		//	Cond:      nil,
		//},
		wantjoin1,
	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_u_a, _ := NewUnresolvedName("", "u", "a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_u_a,
			As:   "",
		},
		{
			Expr: NewSubquery(want_sub.Select, false),
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:     t1wantFrom,
		Distinct: false,
		Where:    NewWhere(want_in_expr),
		Exprs:    t1wantFieldList,
		GroupBy:  nil,
		Having:   nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1, t1want
}

func Test_transformSelectStmtToSelect(t *testing.T) {
	type args struct {
		ss *ast.SelectStmt
	}

	t1, t1want := gen_transform_t1()
	t2, t2want := gen_transform_t2()
	t3, t3want := gen_transform_t3()
	t4, t4want := gen_transform_t4()
	t5, t5want := gen_transform_t5()
	t6, t6want := gen_transform_t6()
	t7, t7want := gen_transform_t7()
	t8, t8want := gen_transform_t8()
	t9, t9want := gen_transform_t9()
	t10, t10want := gen_transform_t10()
	t11, t11want := gen_transform_t11()
	t12, t12want := gen_transform_t12()
	t13, t13want := gen_transform_t13()
	t14, t14want := gen_transform_t14()
	t15, t15want := gen_transform_t15()

	tests := []struct {
		name string
		args args
		want *Select
	}{
		{"t1", args{t1}, t1want},
		{"t2", args{t2}, t2want},
		{"t3", args{t3}, t3want},
		{"t4", args{t4}, t4want},
		{"t5", args{t5}, t5want},
		{"t6", args{t6}, t6want},
		{"t7", args{t7}, t7want},
		{"t8", args{t8}, t8want},
		{"t9", args{t9}, t9want},
		{"t10", args{t10}, t10want},
		{"t11", args{t11}, t11want},
		{"t12", args{t12}, t12want},
		{"t13", args{t13}, t13want},
		{"t14", args{t14}, t14want},
		{"t15", args{t15}, t15want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSelectStmtToSelect(tt.args.ss);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformSelectStmtToSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformSetOprStmtToSelectStatement(t *testing.T) {

}

func gen_insert_t1()(*ast.InsertStmt,*Insert){
	e1 := ast.NewValueExpr(1, "", "")
	e2 := ast.NewValueExpr(2, "", "")
	e3 := ast.NewValueExpr(3, "", "")
	e4 := ast.NewValueExpr(4, "", "")
	e5 := ast.NewValueExpr(5, "", "")
	e6 := ast.NewValueExpr(6, "", "")
	eTrue := ast.NewValueExpr(true, "", "")
	eFalse := ast.NewValueExpr(false, "", "")

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f3 := NewNumVal(constant.MakeInt64(3), "3", false)
	f4 := NewNumVal(constant.MakeInt64(4), "4", false)
	f5 := NewNumVal(constant.MakeInt64(5), "5", false)
	f6 := NewNumVal(constant.MakeInt64(6), "6", false)
	fTrue := NewNumVal(constant.MakeInt64(1), "1", false)
	fFalse := NewNumVal(constant.MakeInt64(0), "0", false)

	//============================
	u := gen_table("", "u")

	t1Join := &ast.Join{
		Left:           u,
		Right:          nil,
		Tp:             0,
		On:             nil,
		Using:          nil,
		NaturalJoin:    false,
		StraightJoin:   false,
		ExplicitParens: false,
	}

	t1TableRef := &ast.TableRefsClause{TableRefs: t1Join}

	colNames := []*ast.ColumnName{
		{Name: model.CIStr{O: "a", L: "a"}},
		{Name: model.CIStr{O: "b", L: "b"}},
		{Name: model.CIStr{O: "c", L: "c"}},
		{Name: model.CIStr{O: "d", L: "d"}},
	}

	lists := [][]ast.ExprNode{
		{
			e1,e2,e3,e4,
		},
		{
			e5,e6,eTrue,eFalse,
		},
	}

	partitionNames :=[]model.CIStr{
		{O: "p1", L: "p1"},
		{O: "p2", L: "p2"},
	}

	t1_insert :=&ast.InsertStmt{
		IsReplace:      false,
		IgnoreErr:      false,
		Table:          t1TableRef,
		Columns:        colNames,
		Lists:          lists,
		Setlist:        nil,
		Priority:       0,
		OnDuplicate:    nil,
		Select:         nil,
		TableHints:     nil,
		PartitionNames: partitionNames,
	}

	//================================
	want_u := gen_want_table("", "u")

	want_table_ref := &JoinTableExpr{
		JoinType: "",
		Left:     want_u,
		Right:    nil,
		Cond:     nil,
	}

	want_col_names := IdentifierList{
		Identifier("a"),
		Identifier("b"),
		Identifier("c"),
		Identifier("d"),
	}

	var rows []Exprs = []Exprs{
		[]Expr{
			f1,f2,f3,f4,
		},
		[]Expr{
			f5,f6,fTrue,fFalse,
		},
	}
	vc := NewValuesClause(rows)
	sel := NewSelect(vc,nil,nil)

	pnames := IdentifierList{
		"p1","p2",
	}

	t1_want_insert :=&Insert{
		Table:          want_table_ref,
		Columns:        want_col_names,
		Rows:           sel,
		PartitionNames: pnames,
	}


	return t1_insert,t1_want_insert
}

func gen_insert_t2()(*ast.InsertStmt,*Insert){
	t1,t1_want := gen_insert_t1()

	t1.Lists = nil

	t1_sel,t1_want_sel := gen_transform_t1()

	t1.Select = t1_sel

	t1_want.Rows = NewSelect(t1_want_sel,nil,nil)
	return t1,t1_want
}

func gen_insert_t3()(*ast.InsertStmt,*Insert){
	sql := `
insert into u partition(p1,p2) (a,b,c,d) values (1,2,3,4),(5,6,1,0)
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.InsertStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	_,want := gen_insert_t1()

	return n,want
}

func Test_transformInsertStmtToInsert(t *testing.T) {
	type args struct {
		is *ast.InsertStmt
	}

	t1,t1_want := gen_insert_t1()
	t2,t2_want := gen_insert_t2()
	t3,t3_want := gen_insert_t3()

	tests := []struct {
		name string
		args args
		want *Insert
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformInsertStmtToInsert(tt.args.is); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformInsertStmtToInsert() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_create_table_t1()(*ast.CreateTableStmt,*CreateTable){
	sql := `create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY RANGE( YEAR(purchased) )
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0 VALUES LESS THAN (1990) 
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 VALUES LESS THAN (2000) (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 VALUES LESS THAN MAXVALUE (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
);
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
			),
			NewAttributeCheck(
				NewBinaryExpr(PLUS,bname,cname),
				true,
				"cx",
				),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
			),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			NewValuesLessThan([]Expr{f1990}),
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
					),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
					),
			},
			),
			NewPartition(Identifier("p1"),
				NewValuesLessThan([]Expr{f2000}),
				[]TableOption{},
				[]*SubPartition{
					NewSubPartition(
						Identifier("s2"),
						[]TableOption{},
					),
					NewSubPartition(
						Identifier("s3"),
						[]TableOption{},
						),
				},
				),
		NewPartition(Identifier("p2"),
			NewValuesLessThan([]Expr{NewMaxValue()}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewRangeType(NewFuncExpr(0,year_name,[]Expr{purchased_name},nil),nil),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func generate_create_table_t2()(*ast.CreateTableStmt,*CreateTable){
	sql := `create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY LIST( YEAR(purchased) )
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0 VALUES IN (1990,1991,1992,1993) 
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 VALUES IN (2000,2001,2002,2003) (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 VALUES IN (NULL,NULL,NULL,NULL) (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
);
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
		),
		NewAttributeCheck(
			NewBinaryExpr(PLUS,bname,cname),
			true,
			"cx",
		),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	f1991 := NewNumVal(constant.MakeInt64(1991), "1991", false)
	f1992 := NewNumVal(constant.MakeInt64(1992), "1992", false)
	f1993 := NewNumVal(constant.MakeInt64(1993), "1993", false)

	f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)
	f2001 := NewNumVal(constant.MakeInt64(2001), "2001", false)
	f2002 := NewNumVal(constant.MakeInt64(2002), "2002", false)
	f2003 := NewNumVal(constant.MakeInt64(2003), "2003", false)

	null_val := NewNumVal(constant.MakeUnknown(), "NULL", false)

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
		),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			NewValuesIn([]Exprs{[]Expr{f1990},[]Expr{f1991},[]Expr{f1992},[]Expr{f1993}}),
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
				),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p1"),
			NewValuesIn([]Exprs{[]Expr{f2000},[]Expr{f2001},[]Expr{f2002},[]Expr{f2003}}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s2"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s3"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p2"),
			NewValuesIn([]Exprs{[]Expr{null_val},[]Expr{null_val},[]Expr{null_val},[]Expr{null_val}}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewListType(NewFuncExpr(0,year_name,[]Expr{purchased_name},nil),nil),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func generate_create_table_t3()(*ast.CreateTableStmt,*CreateTable){
	sql := `create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY LIST COLUMNS(a,b)
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0 VALUES IN ((1990,1991),(1992,1993)) 
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 VALUES IN ((2000,2001),(2002,2003)) (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 VALUES IN ((NULL,NULL),(NULL,NULL)) (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
);
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	//year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
		),
		NewAttributeCheck(
			NewBinaryExpr(PLUS,bname,cname),
			true,
			"cx",
		),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	f1991 := NewNumVal(constant.MakeInt64(1991), "1991", false)
	f1992 := NewNumVal(constant.MakeInt64(1992), "1992", false)
	f1993 := NewNumVal(constant.MakeInt64(1993), "1993", false)

	f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)
	f2001 := NewNumVal(constant.MakeInt64(2001), "2001", false)
	f2002 := NewNumVal(constant.MakeInt64(2002), "2002", false)
	f2003 := NewNumVal(constant.MakeInt64(2003), "2003", false)

	null_val := NewNumVal(constant.MakeUnknown(), "NULL", false)

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
		),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			NewValuesIn([]Exprs{[]Expr{f1990,f1991},[]Expr{f1992,f1993}}),
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
				),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p1"),
			NewValuesIn([]Exprs{[]Expr{f2000,f2001},[]Expr{f2002,f2003}}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s2"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s3"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p2"),
			NewValuesIn([]Exprs{[]Expr{null_val,null_val},[]Expr{null_val,null_val}}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewListType(nil,[]*UnresolvedName{aname,bname}),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func generate_create_table_t4()(*ast.CreateTableStmt,*CreateTable){
	sql := `create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY RANGE COLUMNS(a,b)
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0 VALUES LESS THAN (1990,1991) 
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 VALUES LESS THAN (2000,2001) (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 VALUES LESS THAN (MAXVALUE,MAXVALUE) (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
)
;
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	//year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
		),
		NewAttributeCheck(
			NewBinaryExpr(PLUS,bname,cname),
			true,
			"cx",
		),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	f1991 := NewNumVal(constant.MakeInt64(1991), "1991", false)
	//f1992 := NewNumVal(constant.MakeInt64(1992), "1992", false)
	//f1993 := NewNumVal(constant.MakeInt64(1993), "1992", false)

	f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)
	f2001 := NewNumVal(constant.MakeInt64(2001), "2001", false)
	//f2002 := NewNumVal(constant.MakeInt64(2002), "2002", false)
	//f2003 := NewNumVal(constant.MakeInt64(2003), "2003", false)

	//null_val := NewNumVal(constant.MakeUnknown(), "NULL", false)
	max_value := NewMaxValue()

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
		),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			NewValuesLessThan([]Expr{f1990,f1991}),
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
				),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p1"),
			NewValuesLessThan([]Expr{f2000,f2001}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s2"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s3"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p2"),
			NewValuesLessThan([]Expr{max_value,max_value}),
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewRangeType(nil,[]*UnresolvedName{aname,bname}),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func generate_create_table_t5()(*ast.CreateTableStmt,*CreateTable){
	sql := `
create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY HASH(YEAR(purchased))
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0  
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
)
;
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
		),
		NewAttributeCheck(
			NewBinaryExpr(PLUS,bname,cname),
			true,
			"cx",
		),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	//f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	//f1991 := NewNumVal(constant.MakeInt64(1991), "1991", false)
	//f1992 := NewNumVal(constant.MakeInt64(1992), "1992", false)
	//f1993 := NewNumVal(constant.MakeInt64(1993), "1993", false)

	//f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)
	//f2001 := NewNumVal(constant.MakeInt64(2001), "2001", false)
	//f2002 := NewNumVal(constant.MakeInt64(2002), "2002", false)
	//f2003 := NewNumVal(constant.MakeInt64(2003), "2003", false)

	//null_val := NewNumVal(constant.MakeUnknown(), "NULL", false)
	//max_value := NewMaxValue()

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
		),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			nil,
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
				),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p1"),
			nil,
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s2"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s3"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p2"),
			nil,
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewHashType(false,NewFuncExpr(0,year_name,[]Expr{purchased_name},nil)),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func generate_create_table_t6()(*ast.CreateTableStmt,*CreateTable){
	sql := `
create table if not exists A
(
	a int not null default 1 auto_increment unique primary key
		COMMENT "a" 
		collate utf8_bin 
		column_format dynamic
		STORAGE disk
		REFERENCES B(b asc,c desc) match full on delete cascade on update restrict
		CONSTRAINT cx check (b+c) enforced,
	b int GENERATED ALWAYS AS (1+2) virtual,
	index a_b_idx using btree (a,b) key_block_size 64 comment "a_b_idx"
)engine = "innodb" ROW_FORMAT = dynamic comment = "table A" COMPRESSION = "lz4"
PARTITION BY LINEAR KEY(a,b)
PARTITIONS 3

SUBPARTITION BY HASH( TO_DAYS(purchased) ) 
SUBPARTITIONS 2

(
	PARTITION p0  
	engine = "innodb"
	comment = "p0"
	DATA DIRECTORY = "/data"
	INDEX DIRECTORY = "/index"
	max_rows = 1000
	min_rows = 100
	TABLESPACE = tspace
	(
		SUBPARTITION s0
			engine = "innodb"
			comment = "sub_s0"
			DATA DIRECTORY = "/data/s0"
			INDEX DIRECTORY = "/index/s0"
			max_rows = 1000
			min_rows = 100
			TABLESPACE = tspace_s0
		,
		SUBPARTITION s1
	),
	PARTITION p1 (
		SUBPARTITION s2,
		SUBPARTITION s3
	),
	PARTITION p2 (
		SUBPARTITION s4,
		SUBPARTITION s5
	)
)
;
`
	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	//year_name,_ := NewUnresolvedName("","YEAR")
	purchased_name,_ := NewUnresolvedName("","","purchased")
	to_days_name,_ := NewUnresolvedName("","TO_DAYS")

	col1_attr_arr := []ColumnAttribute{
		NewAttributeNull(false),
		NewAttributeDefault(NewNumVal(constant.MakeInt64(1),"1",false)),
		NewAttributeAutoIncrement(),
		NewAttributeUniqueKey(),
		NewAttributePrimaryKey(),
		NewAttributeComment(NewNumVal(constant.MakeString("a"),"a",false)),
		NewAttributeCollate("utf8_bin"),
		NewAttributeColumnFormat("DYNAMIC"),
		NewAttributeStorage("disk"),
		NewAttributeReference(
			NewTableName("B",ObjectNamePrefix{}),
			[]*KeyPart{NewKeyPart(bname,-1,nil),NewKeyPart(cname,-1,nil)},
			MATCH_FULL,
			REFERENCE_OPTION_CASCADE,
			REFERENCE_OPTION_RESTRICT,
		),
		NewAttributeCheck(
			NewBinaryExpr(PLUS,bname,cname),
			true,
			"cx",
		),
	}

	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f2 := NewNumVal(constant.MakeInt64(2), "2", false)
	//f1990 := NewNumVal(constant.MakeInt64(1990), "1990", false)
	//f1991 := NewNumVal(constant.MakeInt64(1991), "1991", false)
	//f1992 := NewNumVal(constant.MakeInt64(1992), "1992", false)
	//f1993 := NewNumVal(constant.MakeInt64(1993), "1993", false)

	//f2000 := NewNumVal(constant.MakeInt64(2000), "2000", false)
	//f2001 := NewNumVal(constant.MakeInt64(2001), "2001", false)
	//f2002 := NewNumVal(constant.MakeInt64(2002), "2002", false)
	//f2003 := NewNumVal(constant.MakeInt64(2003), "2003", false)

	//null_val := NewNumVal(constant.MakeUnknown(), "NULL", false)
	//max_value := NewMaxValue()

	col2_attr_arr := []ColumnAttribute{
		NewAttributeGeneratedAlways(NewBinaryExpr(PLUS,f1,f2),false),
	}

	want_defs :=TableDefs{
		NewColumnTableDef(aname,TYPE_LONG, col1_attr_arr),
		NewColumnTableDef(bname,TYPE_LONG, col2_attr_arr),
		NewIndex([]*KeyPart{NewKeyPart(aname,-1,nil),NewKeyPart(bname,-1,nil)},
			"a_b_idx",
			false,
			NewIndexOption(64,INDEX_TYPE_BTREE,"","a_b_idx",VISIBLE_TYPE_INVALID,"",""),
		),
	}

	table_options :=[]TableOption{
		NewTableOptionEngine("innodb"),
		NewTableOptionRowFormat(ROW_FORMAT_DYNAMIC),
		NewTableOptionComment("table A"),
		NewTableOptionCompression("lz4"),
	}

	partitions := []*Partition{
		NewPartition(Identifier("p0"),
			nil,
			[]TableOption{
				NewTableOptionEngine("innodb"),
				NewTableOptionComment("p0"),
				NewTableOptionDataDirectory("/data"),
				NewTableOptionIndexDirectory("/index"),
				NewTableOptionMaxRows(1000),
				NewTableOptionMinRows(100),
				NewTableOptionTablespace("tspace"),
			},
			[]*SubPartition{
				NewSubPartition(Identifier("s0"),
					[]TableOption{
						NewTableOptionEngine("innodb"),
						NewTableOptionComment("sub_s0"),
						NewTableOptionDataDirectory("/data/s0"),
						NewTableOptionIndexDirectory("/index/s0"),
						NewTableOptionMaxRows(1000),
						NewTableOptionMinRows(100),
						NewTableOptionTablespace("tspace_s0"),
					},
				),
				NewSubPartition(Identifier("s1"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p1"),
			nil,
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s2"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s3"),
					[]TableOption{},
				),
			},
		),
		NewPartition(Identifier("p2"),
			nil,
			[]TableOption{},
			[]*SubPartition{
				NewSubPartition(
					Identifier("s4"),
					[]TableOption{},
				),
				NewSubPartition(
					Identifier("s5"),
					[]TableOption{},
				),
			},
		),
	}

	partition_option := &PartitionOption{
		PartBy:     PartitionBy{
			NewKeyType(true,[]*UnresolvedName{aname,bname}),
			3,
		},
		SubPartBy:  NewPartitionBy(NewHashType(false,NewFuncExpr(0,to_days_name,[]Expr{purchased_name},nil)),2),
		Partitions: partitions,
	}

	want:= &CreateTable{
		IfNotExists:     true,
		Table:           TableName{objName:objName{
			ObjectNamePrefix: ObjectNamePrefix{},
			ObjectName:       "A",
		}},
		Defs:            want_defs,
		Options:         table_options,
		PartitionOption: partition_option,
	}

	return n,want
}

func Test_transformCreateTableStmtToCreateTable(t *testing.T) {
	type args struct {
		cts *ast.CreateTableStmt
	}

	t1,t1want := generate_create_table_t1()
	t2,t2want := generate_create_table_t2()
	t3,t3want := generate_create_table_t3()
	t4,t4want := generate_create_table_t4()
	t5,t5want := generate_create_table_t5()
	t6,t6want := generate_create_table_t6()

	tests := []struct {
		name string
		args args
		want *CreateTable
	}{
		{"t1",args{cts: t1},t1want},
		{"t2",args{cts: t2},t2want},
		{"t3",args{cts: t3},t3want},
		{"t4",args{cts: t4},t4want},
		{"t5",args{cts: t5},t5want},
		{"t6",args{cts: t6},t6want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCreateTableStmtToCreateTable(tt.args.cts);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCreateTableStmtToCreateTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_create_database()(*ast.CreateDatabaseStmt,*CreateDatabase) {
	sql := `
create database if not exists A
character set = 'utf8mb4'
collate = 'utf8_bin'
ENCRYPTION = 'Y';
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.CreateDatabaseStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	createOpts := []CreateOption{
		NewCreateOptionCharset("utf8mb4"),
		NewCreateOptionCollate("utf8_bin"),
		NewCreateOptionEncryption("Y"),
	}

	t1_want := &CreateDatabase{
		IfNotExists:   true,
		Name:          "A",
		CreateOptions: createOpts,
	}

	return n,t1_want
}

func Test_transformCreateDatabaseStmtToCreateDatabase(t *testing.T) {
	type args struct {
		cds *ast.CreateDatabaseStmt
	}

	t1,t1_want := generate_create_database()

	tests := []struct {
		name string
		args args
		want *CreateDatabase
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCreateDatabaseStmtToCreateDatabase(tt.args.cds);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCreateDatabaseStmtToCreateDatabase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_drop_database()(*ast.DropDatabaseStmt,*DropDatabase){
	sql := `
drop database if exists A
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.DropDatabaseStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	want := &DropDatabase{
		Name:          "A",
		IfExists:      true,
	}

	return n,want
}

func Test_transformDropDatabaseStmtToDropDatabase(t *testing.T) {
	type args struct {
		dds *ast.DropDatabaseStmt
	}

	t1,t1_want := generate_drop_database()

	tests := []struct {
		name string
		args args
		want *DropDatabase
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDropDatabaseStmtToDropDatabase(tt.args.dds); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDropDatabaseStmtToDropDatabase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_drop_table()(*ast.DropTableStmt,*DropTable) {
	sql := `
drop table if exists A,B,C
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.DropTableStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	names := []*TableName{
		NewTableName("A",ObjectNamePrefix{
			CatalogName:     "",
			SchemaName:      "",
			ExplicitCatalog: false,
			ExplicitSchema:  false,
		}),
		NewTableName("B",ObjectNamePrefix{
			CatalogName:     "",
			SchemaName:      "",
			ExplicitCatalog: false,
			ExplicitSchema:  false,
		}),
		NewTableName("C",ObjectNamePrefix{
			CatalogName:     "",
			SchemaName:      "",
			ExplicitCatalog: false,
			ExplicitSchema:  false,
		}),
	}

	want := &DropTable{
		IfExists:      true,
		Names:         names,
	}
	return n, want
}

func Test_transformDropTableStmtToDropTable(t *testing.T) {
	type args struct {
		dts *ast.DropTableStmt
	}

	t1,t1_want := generate_drop_table()

	tests := []struct {
		name string
		args args
		want *DropTable
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDropTableStmtToDropTable(tt.args.dts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDropTableStmtToDropTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_delete_table()(*ast.DeleteStmt,*Delete){
	sql := `
delete from A as AA
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.DeleteStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	tn := NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	})

	ac := AliasClause{
		Alias:       "AA",
	}

	want := &Delete{
		Table:         NewJoinTableExpr("",NewAliasedTableExpr(tn,ac),nil,nil),
		Where:         NewWhere(nil),
		OrderBy:       nil,
		Limit:         nil,
	}

	return n, want
}

func generate_delete_table2()(*ast.DeleteStmt,*Delete){
	sql := `
delete from A as AA
where a != 0 
order by b
limit 1
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}
	n,ok := stmt[0].(*ast.DeleteStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	tn := NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	})

	ac := AliasClause{
		Alias:       "AA",
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	o := OrderBy{
		NewOrder(bname,Ascending,true),
	}

	l := NewLimit(nil,f1)

	want := &Delete{
		Table:         NewJoinTableExpr("",NewAliasedTableExpr(tn,ac),nil,nil),
		Where:         NewWhere(NewComparisonExpr(NOT_EQUAL,aname,f0)),
		OrderBy:       o,
		Limit:         l,
	}

	return n, want
}


func Test_transformDeleteStmtToDelete(t *testing.T) {
	type args struct {
		ds *ast.DeleteStmt
	}

	t1,t1_want := generate_delete_table()
	t2,t2_want := generate_delete_table2()

	tests := []struct {
		name string
		args args
		want *Delete
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDeleteStmtToDelete(tt.args.ds);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDeleteStmtToDelete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_update_stmt_1() (*ast.UpdateStmt, *Update) {
	sql := `
update A as AA
set a = 3, b = 4
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}

	n,ok := stmt[0].(*ast.UpdateStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	tn := NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	})

	ac := AliasClause{
		Alias:       "AA",
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")

	f3 := NewNumVal(constant.MakeInt64(3), "3", false)
	f4 := NewNumVal(constant.MakeInt64(4), "4", false)

	ue := UpdateExprs{
		NewUpdateExpr(false,[]*UnresolvedName{aname},f3),
		NewUpdateExpr(false,[]*UnresolvedName{bname},f4),
	}

	want := &Update{
		Table:         NewJoinTableExpr("",NewAliasedTableExpr(tn,ac),nil,nil),
		Exprs:         ue,
		From:          nil,
		Where:         NewWhere(nil),
		OrderBy:       nil,
		Limit:         nil,
	}

	return n, want
}

func generate_update_stmt_2() (*ast.UpdateStmt, *Update) {
	sql := `
update A as AA
set a = 3, b = 4
where a != 0
order by b
limit 1
;
`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}

	n,ok := stmt[0].(*ast.UpdateStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	tn := NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	})

	ac := AliasClause{
		Alias:       "AA",
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)
	f3 := NewNumVal(constant.MakeInt64(3), "3", false)
	f4 := NewNumVal(constant.MakeInt64(4), "4", false)

	ue := UpdateExprs{
		NewUpdateExpr(false,[]*UnresolvedName{aname},f3),
		NewUpdateExpr(false,[]*UnresolvedName{bname},f4),
	}

	o := OrderBy{
		NewOrder(bname,Ascending,true),
	}

	l := NewLimit(nil,f1)

	want := &Update{
		Table:         NewJoinTableExpr("",NewAliasedTableExpr(tn,ac),nil,nil),
		Exprs:         ue,
		From:          nil,
		Where:         NewWhere(NewComparisonExpr(NOT_EQUAL,aname,f0)),
		OrderBy:       o,
		Limit:         l,
	}

	return n, want
}

func Test_transformUpdateStmtToUpdate(t *testing.T) {
	type args struct {
		us *ast.UpdateStmt
	}

	t1,t1_want := generate_update_stmt_1()
	t2,t2_want := generate_update_stmt_2()

	tests := []struct {
		name string
		args args
		want *Update
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformUpdateStmtToUpdate(tt.args.us);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformUpdateStmtToUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_load_data_1()(*ast.LoadDataStmt,*Load) {
	sql := `
load data infile 'data.txt' INTO TABLE db.A;
;`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}

	n,ok := stmt[0].(*ast.LoadDataStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	want := &Load{
		Local:             false,
		File:              "data.txt",
		DuplicateHandling: NewDuplicateKeyError(),
		Table:             NewTableName("A",ObjectNamePrefix{
			CatalogName:     "",
			SchemaName:      "db",
			ExplicitCatalog: false,
			ExplicitSchema:  true,
		}),
		Fields:            NewFields("\t",false,0,'\\'),
		Lines:             NewLines("","\n"),
		IgnoredLines:      0,
		ColumnList:        make([]LoadColumn,0),
		Assignments:       nil,
	}

	return n, want
}

func generate_load_data_2()(*ast.LoadDataStmt,*Load) {
	sql := `
load data 
local
infile 'data.txt'
replace
INTO TABLE db.A
FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '\n' ESCAPED BY '\\'
LINES STARTING BY '#' TERMINATED BY '\n'
IGNORE 2 LINES
(a,b,@vc,@vd)
set c = @vc != 0, d = @vd != 1 
;`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}

	n,ok := stmt[0].(*ast.LoadDataStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")
	dname,_ := NewUnresolvedName("","","d")

	colList :=[]LoadColumn{
		aname,
		bname,
		NewVarExpr("vc",false,false,nil),
		NewVarExpr("vd",false,false,nil),
	}

	vc := NewVarExpr("vc",false,false,nil)
	vd := NewVarExpr("vd",false,false,nil)

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	assigns := UpdateExprs{
		NewUpdateExpr(false,[]*UnresolvedName{cname},NewComparisonExpr(NOT_EQUAL,vc,f0)),
		NewUpdateExpr(false,[]*UnresolvedName{dname},NewComparisonExpr(NOT_EQUAL,vd,f1)),
	}
	want := &Load{
		Local:             true,
		File:              "data.txt",
		DuplicateHandling: NewDuplicateKeyReplace(),
		Table:             NewTableName("A",ObjectNamePrefix{
			CatalogName:     "",
			SchemaName:      "db",
			ExplicitCatalog: false,
			ExplicitSchema:  true,
		}),
		Fields:            NewFields("\t",true,'\n','\\'),
		Lines:             NewLines("#","\n"),
		IgnoredLines:      2,
		ColumnList:        colList,
		Assignments:       assigns,
	}

	return n, want
}

func Test_transformLoadDataStmtToLoad(t *testing.T) {
	type args struct {
		lds *ast.LoadDataStmt
	}

	t1,t1_want := generate_load_data_1()
	t2,t2_want := generate_load_data_2()

	tests := []struct {
		name string
		args args
		want *Load
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformLoadDataStmtToLoad(tt.args.lds);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformLoadDataStmtToLoad() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformBeginStmtToBeginTransaction(t *testing.T) {
	type args struct {
		bs *ast.BeginStmt
	}

	t1 := &ast.BeginStmt{ReadOnly: true}
	t1_want := &BeginTransaction{Modes: MakeTransactionModes(READ_WRITE_MODE_READ_ONLY)}

	t2 := &ast.BeginStmt{ReadOnly: false}
	t2_want := &BeginTransaction{Modes: MakeTransactionModes(READ_WRITE_MODE_READ_WRITE)}

	tests := []struct {
		name string
		args args
		want *BeginTransaction
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformBeginStmtToBeginTransaction(tt.args.bs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformBeginStmtToBeginTransaction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformCommitTransaction(t *testing.T) {
	type args struct {
		cs *ast.CommitStmt
	}

	t1 := &ast.CommitStmt{CompletionType: ast.CompletionTypeDefault}
	t1_want := &CommitTransaction{Type: COMPLETION_TYPE_NO_CHAIN}

	t2 := &ast.CommitStmt{CompletionType: ast.CompletionTypeChain}
	t2_want := &CommitTransaction{Type: COMPLETION_TYPE_CHAIN}

	t3 := &ast.CommitStmt{CompletionType: ast.CompletionTypeRelease}
	t3_want := &CommitTransaction{Type: COMPLETION_TYPE_RELEASE}

	tests := []struct {
		name string
		args args
		want *CommitTransaction
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCommitStmtToCommitTransaction(tt.args.cs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCommitTransaction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformRollbackStmtToRollbackTransaction(t *testing.T) {
	type args struct {
		rs *ast.RollbackStmt
	}

	t1 := &ast.RollbackStmt{CompletionType: ast.CompletionTypeDefault}
	t1_want := &RollbackTransaction{Type: COMPLETION_TYPE_NO_CHAIN}

	t2 := &ast.RollbackStmt{CompletionType: ast.CompletionTypeChain}
	t2_want := &RollbackTransaction{Type: COMPLETION_TYPE_CHAIN}

	t3 := &ast.RollbackStmt{CompletionType: ast.CompletionTypeRelease}
	t3_want := &RollbackTransaction{Type: COMPLETION_TYPE_RELEASE}

	tests := []struct {
		name string
		args args
		want *RollbackTransaction
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformRollbackStmtToRollbackTransaction(tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformRollbackStmtToRollbackTransaction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_show_create_table() (*ast.ShowStmt, *ShowCreate) {
	sql := `
show create table db.A
;`

	p := parser.New()
	stmt,_, err := p.Parse(sql,"","")
	if err != nil{
		panic(fmt.Errorf("%v",err))
	}

	n,ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v",ok))
	}

	u,_ := NewUnresolvedObjectName(2,[3]string{"db","A"})

	want := &ShowCreate{Name: u}

	return n,want
}

func generate_show_create_database() (*ast.ShowStmt, *ShowCreateDatabase) {
	sql := `
show create database if not exists D
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &ShowCreateDatabase{IfNotExists: true,Name:"D"}
	return n,want
}

func generate_show_columns_1() (*ast.ShowStmt, *ShowColumns) {
	sql := `
show columns from A
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	want := &ShowColumns{Table:t}
	return n,want
}

func generate_show_columns_2() (*ast.ShowStmt, *ShowColumns) {
	sql := `
show EXTENDED FULL columns 
from A
from db
like 'a%'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	ls := NewNumVal(constant.MakeString("a%"),"a%",false)
	l := NewComparisonExpr(LIKE,nil,ls)

	want := &ShowColumns{Ext:true,Full:true,Table:t,DBName: "db",Like: l}
	return n,want
}

func generate_show_columns_3() (*ast.ShowStmt, *ShowColumns) {
	sql := `
show EXTENDED FULL columns 
from A
from db
where a != 0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	want := &ShowColumns{Ext:true,Full:true,Table:t,DBName: "db",Where: where}
	return n,want
}

func generate_show_databases_1() (*ast.ShowStmt, *ShowDatabases) {
	sql := `
show databases 
like 'a%'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	ls := NewNumVal(constant.MakeString("a%"),"a%",false)
	l := NewComparisonExpr(LIKE,nil,ls)
	want := NewShowDatabases(l,nil)

	return n,want
}

func generate_show_databases_2() (*ast.ShowStmt, *ShowDatabases) {
	sql := `
show databases 
where a != 0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	want := NewShowDatabases(nil,where)

	return n,want
}

func generate_show_tables_1() (*ast.ShowStmt, *ShowTables) {
	sql := `
show tables from db
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := NewShowTables(false,false,"db",nil,nil)
	return n,want
}

func generate_show_tables_2() (*ast.ShowStmt, *ShowTables) {
	sql := `
show FULL tables 
from db
like 'a%'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	ls := NewNumVal(constant.MakeString("a%"),"a%",false)
	l := NewComparisonExpr(LIKE,nil,ls)

	want := NewShowTables(false,true,"db",l,nil)
	return n,want
}

func generate_show_tables_3() (*ast.ShowStmt, *ShowTables) {
	sql := `
show FULL tables 
from db
where a != 0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	want := NewShowTables(false,true,"db",nil,where)
	return n,want
}

func generate_show_processlist()(*ast.ShowStmt,*ShowProcessList) {
	sql := `
show FULL PROCESSLIST
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := NewShowProcessList(true)
	return n,want
}

func generate_show_errors() (*ast.ShowStmt, *ShowErrors) {
	sql := `
show errors
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}
	return n,NewShowErrors()
}

func generate_show_warnings() (*ast.ShowStmt, *ShowWarnings) {
	sql := `
show warnings
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}
	return n,NewShowWarnings()
}

func generate_show_variables_1()(*ast.ShowStmt,*ShowVariables){
	sql := `
show VARIABLES
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	return n,NewShowVariables(false,nil,nil)
}

func generate_show_variables_2()(*ast.ShowStmt,*ShowVariables){
	sql := `
show VARIABLES
like 'a%'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	ls := NewNumVal(constant.MakeString("a%"),"a%",false)
	l := NewComparisonExpr(LIKE,nil,ls)

	return n,NewShowVariables(false,l,nil)
}

func generate_show_variables_3()(*ast.ShowStmt,*ShowVariables){
	sql := `
show global VARIABLES
where a!=0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	return n,NewShowVariables(true,nil,where)
}

func generate_show_index_1()(*ast.ShowStmt,*ShowIndex) {
	sql := `
show index from A from db where a != 0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	want := &ShowIndex{
		TableName: TableName{
			objName:   objName{
				ObjectNamePrefix: ObjectNamePrefix{
					CatalogName: "",
					SchemaName: "db",
					ExplicitCatalog: false,
					ExplicitSchema: true,
				},
				ObjectName:       "A",
			},
		},
		Where:     where,
	}
	return n,want
}

func generate_show_index_2()(*ast.ShowStmt,*ShowIndex) {
	sql := `
show index from A where a != 0
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ShowStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	f0 := NewNumVal(constant.MakeInt64(0), "0", false)

	w := NewComparisonExpr(NOT_EQUAL,aname,f0)
	where := NewWhere(w)

	want := &ShowIndex{
		TableName: TableName{
			objName:   objName{
				ObjectNamePrefix: ObjectNamePrefix{
					CatalogName: "",
					SchemaName: "",
					ExplicitCatalog: false,
					ExplicitSchema: false,
				},
				ObjectName:       "A",
			},
		},
		Where:     where,
	}
	return n,want
}

func Test_transformShowStmtToShow(t *testing.T) {
	type args struct {
		ss *ast.ShowStmt
	}

	t1,t1_want := generate_show_create_table()
	t2,t2_want := generate_show_create_database()
	t3,t3_want := generate_show_columns_1()
	t4,t4_want := generate_show_columns_2()
	t5,t5_want := generate_show_columns_3()
	t6,t6_want := generate_show_databases_1()
	t7,t7_want := generate_show_databases_2()
	t8,t8_want := generate_show_tables_1()
	t9,t9_want := generate_show_tables_2()
	t10,t10_want := generate_show_tables_3()
	t11,t11_want := generate_show_processlist()
	t12,t12_want := generate_show_errors()
	t13,t13_want := generate_show_warnings()
	t14,t14_want := generate_show_variables_1()
	t15,t15_want := generate_show_variables_2()
	t16,t16_want := generate_show_variables_3()
	t17,t17_want := generate_show_index_1()
	t18,t18_want := generate_show_index_2()

	tests := []struct {
		name string
		args args
		want Show
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
		{"t4",args{t4},t4_want},
		{"t5",args{t5},t5_want},
		{"t6",args{t6},t6_want},
		{"t7",args{t7},t7_want},
		{"t8",args{t8},t8_want},
		{"t9",args{t9},t9_want},
		{"t10",args{t10},t10_want},
		{"t11",args{t11},t11_want},
		{"t12",args{t12},t12_want},
		{"t13",args{t13},t13_want},
		{"t14",args{t14},t14_want},
		{"t15",args{t15},t15_want},
		{"t16",args{t16},t16_want},
		{"t17",args{t17},t17_want},
		{"t18",args{t18},t18_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformShowStmtToShow(tt.args.ss);
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformShowStmtToShow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_explain_tablename() (*ast.ExplainStmt, Explain) {
	sql := `
explain A a
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	aname,_ := NewUnresolvedName("","","a")

	st := &ShowColumns{
		Ext:      false,
		Full:     false,
		Table:    t,
		DBName:   "",
		Like:     nil,
		Where:    nil,
		ColName: aname,
	}
	want := NewExplainStmt(st,"")

	return n,want
}

func generate_explain_select() (*ast.ExplainStmt, Explain) {
	sql := `
explain format = "tree" select a from A
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	//t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	aname,_ := NewUnresolvedName("","","a")

	jt := NewJoinTableExpr("",NewAliasedTableExpr(NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	}),AliasClause{}),nil,nil)

	sc := &SelectClause{
		From:            &From{[]TableExpr{jt}},
		Distinct:        false,
		Where:           nil,
		Exprs:           []SelectExpr{SelectExpr{Expr: aname}},
		GroupBy:         nil,
		Having:          nil,
	}

	st := NewSelect(sc,nil,nil)
	want := NewExplainStmt(st,"tree")

	return n,want
}

func generate_explain_set_select() (*ast.ExplainStmt, Explain) {
	sql := `
explain format = "tree" select a from A union select b from B
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	//t,_ := NewUnresolvedObjectName(2,[3]string{"","A"})

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")

	jt := NewJoinTableExpr("",NewAliasedTableExpr(NewTableName(Identifier("A"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	}),AliasClause{}),nil,nil)

	left := &SelectClause{
		From:            &From{[]TableExpr{jt}},
		Distinct:        false,
		Where:           nil,
		Exprs:           []SelectExpr{SelectExpr{Expr: aname}},
		GroupBy:         nil,
		Having:          nil,
	}

	r_jt := NewJoinTableExpr("",NewAliasedTableExpr(NewTableName(Identifier("B"),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	}),AliasClause{}),nil,nil)

	right := &SelectClause{
		From:            &From{[]TableExpr{r_jt}},
		Distinct:        false,
		Where:           nil,
		Exprs:           []SelectExpr{SelectExpr{Expr: bname}},
		GroupBy:         nil,
		Having:          nil,
	}

	st := NewUnionClause(UNION,
		NewSelect(left,nil,nil),
		NewSelect(right,nil,nil),
		false)
	want := NewExplainStmt(NewSelect(st,nil,nil),"tree")

	return n,want
}

func generate_explain_delete() (*ast.ExplainStmt, Explain) {
	sql := `
explain  format = "tree" 
delete from A as AA
where a != 0 
order by b
limit 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	_,del := generate_delete_table2()
	want := NewExplainStmt(del,"tree")
	return n,want
}

func generate_explain_update() (*ast.ExplainStmt, Explain) {
	sql := `
explain  format = "tree" 
update A as AA
set a = 3, b = 4
where a != 0
order by b
limit 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	_,up := generate_update_stmt_2()
	want := NewExplainStmt(up,"tree")
	return n,want
}

func generate_explain_insert() (*ast.ExplainStmt, Explain) {
	sql := `
explain  format = "tree" 
insert into u partition(p1,p2) (a,b,c,d) values (1,2,3,4),(5,6,1,0)
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	_,in := gen_insert_t3()
	want := NewExplainStmt(in,"tree")
	return n,want
}

func generate_explain_analyze_insert() (*ast.ExplainStmt, Explain) {
	sql := `
explain analyze 
insert into u partition(p1,p2) (a,b,c,d) values (1,2,3,4),(5,6,1,0)
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	_,in := gen_insert_t3()
	want := NewExplainAnalyze(in,"row")
	return n,want
}

func Test_transformExplainStmtToExplain(t *testing.T) {
	type args struct {
		es *ast.ExplainStmt
	}

	t1,t1_want := generate_explain_tablename()
	t2,t2_want := generate_explain_select()
	t3,t3_want := generate_explain_set_select()
	t4,t4_want := generate_explain_delete()
	t5,t5_want := generate_explain_update()
	t6,t6_want := generate_explain_insert()
	t7,t7_want := generate_explain_analyze_insert()

	tests := []struct {
		name string
		args args
		want Explain
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
		{"t4",args{t4},t4_want},
		{"t5",args{t5},t5_want},
		{"t6",args{t6},t6_want},
		{"t7",args{t7},t7_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformExplainStmtToExplain(tt.args.es);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformExplainStmtToExplain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_explain_for() (*ast.ExplainForStmt, Explain) {
	sql := `
explain
format = "tree"
for connection 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.ExplainForStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := NewExplainFor("tree",1)
	return n,want
}

func Test_transformExplainForStmtToExplain(t *testing.T) {
	type args struct {
		efs *ast.ExplainForStmt
	}

	t1,t1_want := generate_explain_for()

	tests := []struct {
		name string
		args args
		want Explain
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformExplainForStmtToExplain(tt.args.efs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformExplainForStmtToExplain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_set_var_1()(*ast.SetStmt,*SetVar){
	sql := `
set a = 0, b = 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	a := []*VarAssignmentExpr{
		NewVarAssignmentExpr(true,false,"a",f0,nil),
		NewVarAssignmentExpr(true,false,"b",f1,nil),
	}
	want := NewSetVar(a)

	return n, want
}

func generate_set_user_defined_var()(*ast.SetStmt,*SetVar){
	sql := `
set @a = 0, @b = 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	a := []*VarAssignmentExpr{
		NewVarAssignmentExpr(false,false,"a",f0,nil),
		NewVarAssignmentExpr(false,false,"b",f1,nil),
	}
	want := NewSetVar(a)

	return n, want
}

func generate_set_var_2()(*ast.SetStmt,*SetVar){
	sql := `
set a = 0,session b = 1, @@session.c = 1, global d = 1, @@global.e = 1
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	a := []*VarAssignmentExpr{
		NewVarAssignmentExpr(true,false,"a",f0,nil),
		NewVarAssignmentExpr(true,false,"b",f1,nil),
		NewVarAssignmentExpr(true,false,"c",f1,nil),
		NewVarAssignmentExpr(true,true,"d",f1,nil),
		NewVarAssignmentExpr(true,true,"e",f1,nil),
	}
	want := NewSetVar(a)

	return n, want
}

func Test_transformSetStmtToSetVar(t *testing.T) {
	type args struct {
		ss *ast.SetStmt
	}

	t1,t1_want := generate_set_var_1()
	t2,t2_want := generate_set_user_defined_var()
	t3,t3_want := generate_set_var_2()

	tests := []struct {
		name string
		args args
		want *SetVar
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSetStmtToSetVar(tt.args.ss);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformSetStmtToSetVar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_create_index_1()(*ast.CreateIndexStmt,*CreateIndex) {
	sql := `
create  unique index if not exists idx1
using btree
on A (a,b(10),(a+b),(a-b))
KEY_BLOCK_SIZE 10
with parser x
comment 'x'
visible
lock = default
algorithm = default
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateIndexStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	//cname,_ := NewUnresolvedName("","","c")

	kps := []*KeyPart {
		NewKeyPart(aname,-1,nil),
		NewKeyPart(bname,10,nil),
		NewKeyPart(nil,0,NewBinaryExpr(PLUS,aname,bname)),
		NewKeyPart(nil,0,NewBinaryExpr(MINUS,aname,bname)),
	}

	io := &IndexOption{
		KeyBlockSize:             10,
		iType:                    INDEX_TYPE_BTREE,
		ParserName:               "x",
		Comment:                  "x",
		Visible:                  VISIBLE_TYPE_VISIBLE,
		EngineAttribute:          "",
		SecondaryEngineAttribute: "",
	}

	want := &CreateIndex{
		Name:          "idx1",
		Table:         TableName{
			objName:   objName{
				ObjectNamePrefix: ObjectNamePrefix{},
				ObjectName:       "A",
			},
		},
		IndexCat:      INDEX_CATEGORY_UNIQUE,
		IfNotExists:   true,
		KeyParts:      kps,
		IndexOption:   io,
		MiscOption:    nil,
	}

	return n, want
}

func generate_create_index_2()(*ast.CreateIndexStmt,*CreateIndex) {
	sql := `
create index idx1
on A (a,b(10),(a+b),(a-b))
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateIndexStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	//cname,_ := NewUnresolvedName("","","c")

	kps := []*KeyPart {
		NewKeyPart(aname,-1,nil),
		NewKeyPart(bname,10,nil),
		NewKeyPart(nil,0,NewBinaryExpr(PLUS,aname,bname)),
		NewKeyPart(nil,0,NewBinaryExpr(MINUS,aname,bname)),
	}

	io := &IndexOption{
		KeyBlockSize:             0,
		iType:                    INDEX_TYPE_INVALID,
		ParserName:               "",
		Comment:                  "",
		Visible:                  VISIBLE_TYPE_INVALID,
		EngineAttribute:          "",
		SecondaryEngineAttribute: "",
	}

	want := &CreateIndex{
		Name:          "idx1",
		Table:         TableName{
			objName:   objName{
				ObjectNamePrefix: ObjectNamePrefix{},
				ObjectName:       "A",
			},
		},
		IndexCat:      INDEX_CATEGORY_NONE,
		IfNotExists:   false,
		KeyParts:      kps,
		IndexOption:   io,
		MiscOption:    nil,
	}

	return n, want
}

func Test_transformCreateIndexStmtToCreateIndex(t *testing.T) {
	type args struct {
		cis *ast.CreateIndexStmt
	}

	t1,t1_want := generate_create_index_1()
	t2,t2_want := generate_create_index_2()

	tests := []struct {
		name string
		args args
		want *CreateIndex
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCreateIndexStmtToCreateIndex(tt.args.cis);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCreateIndexStmtToCreateIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_drop_index1()(*ast.DropIndexStmt,*DropIndex) {
	sql := `
drop index if exists idx1 on A
algorithm default
lock default
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.DropIndexStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &DropIndex{
		Name:          "idx1",
		TableName:     TableName{
			objName:   objName{
				ObjectName:       "A",
			},
		},
		IfExists:      true,
		MiscOption:    nil,
	}

	return n, want
}

func generate_drop_index2()(*ast.DropIndexStmt,*DropIndex) {
	sql := `
drop index idx1 on A
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.DropIndexStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &DropIndex{
		Name:          "idx1",
		TableName:     TableName{
			objName:   objName{
				ObjectName:       "A",
			},
		},
		IfExists:      false,
		MiscOption:    nil,
	}

	return n, want
}

func Test_transformDropIndexStmt(t *testing.T) {
	type args struct {
		dis *ast.DropIndexStmt
	}

	t1,t1_want := generate_drop_index1()
	t2,t2_want := generate_drop_index2()

	tests := []struct {
		name string
		args args
		want *DropIndex
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDropIndexStmtToDropIndex(tt.args.dis); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDropIndexStmt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_create_role_1()(*ast.CreateUserStmt,*CreateRole){
	sql := `
create role if not exists 'a','b','a'@'b'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	r := []*Role{
		NewRole("a","%"),
		NewRole("b","%"),
		NewRole("a","b"),
	}

	want := &CreateRole{
		IfNotExists:   true,
		Roles:         r,
	}

	return n, want
}

func generate_create_role_2()(*ast.CreateUserStmt,*CreateRole){
	sql := `
create role 'a'@'b'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	r := []*Role{
		NewRole("a","b"),
	}

	want := &CreateRole{
		IfNotExists:   false,
		Roles:         r,
	}

	return n, want
}

func Test_transformCreateUserStmtToCreateRole(t *testing.T) {
	type args struct {
		cus *ast.CreateUserStmt
	}

	t1,t1_want := generate_create_role_1()
	t2,t2_want := generate_create_role_2()

	tests := []struct {
		name string
		args args
		want *CreateRole
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCreateUserStmtToCreateRole(tt.args.cus); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCreateUserStmtToCreateUser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_drop_role_1()(*ast.DropUserStmt,*DropRole){
	sql := `
drop role if exists 'a','b','a'@'b'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.DropUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	r := []*Role {
		NewRole("a","%"),
		NewRole("b","%"),
		NewRole("a","b"),
	}

	want := &DropRole{
		IfExists:      true,
		Roles:         r,
	}

	return n, want
}

func Test_transformDropUserStmtToDropRole(t *testing.T) {
	type args struct {
		dus *ast.DropUserStmt
	}

	t1,t1_want := generate_drop_role_1()

	tests := []struct {
		name string
		args args
		want *DropRole
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDropUserStmtToDropRole(tt.args.dus); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDropUserStmtToDropRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_create_user_1()(*ast.CreateUserStmt,*CreateUser) {
	sql := `
create user if not exists
u1 identified by 'u1', u2 identified with u2row as 'u2'
require cipher 'xxx'  subject 'yyy'
with MAX_QUERIES_PER_HOUR 0 MAX_UPDATES_PER_HOUR 1
PASSWORD EXPIRE INTERVAL 1 DAY PASSWORD EXPIRE DEFAULT ACCOUNT LOCK ACCOUNT UNLOCK
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	users := []*User{
		NewUser("u1","%","","u1"),
		NewUser("u2","%","","u2"),//u2row can't be recoginzed
	}

	tls := []TlsOption{
		&TlsOptionCipher{Cipher: "xxx"},
		&TlsOptionSubject{Subject: "yyy"},
	}

	//f0 := NewNumVal(constant.MakeInt64(0), "0", false)
	//f1 := NewNumVal(constant.MakeInt64(1), "1", false)

	res := []ResourceOption{
		&ResourceOptionMaxQueriesPerHour{Count: 0},
		&ResourceOptionMaxUpdatesPerHour{Count: 1},
	}

	miscs := []UserMiscOption{
		&UserMiscOptionPasswordExpireInterval{Value: 1},
		&UserMiscOptionPasswordExpireDefault{},
		&UserMiscOptionAccountLock{},
		&UserMiscOptionAccountUnlock{},
	}

	want := &CreateUser{
		IfNotExists:   true,
		Users:         users,
		Roles:         nil,
		TlsOpts:       tls,
		ResOpts:       res,
		MiscOpts:      miscs,
	}

	return n, want
}

func generate_create_user_2()(*ast.CreateUserStmt,*CreateUser) {
	sql := `
create user
u1 , u2
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.CreateUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	users := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","%","",""),//u2row can't be recoginzed
	}

	want := &CreateUser{
		IfNotExists:   false,
		Users:         users,
		Roles:         nil,
		TlsOpts:       make([]TlsOption,0),
		ResOpts:       make([]ResourceOption,0),
		MiscOpts:      make([]UserMiscOption,0),
	}

	return n, want
}

func Test_transformCreatUserStmtToCreateUser(t *testing.T) {
	type args struct {
		cus *ast.CreateUserStmt
	}

	t1,t1_want := generate_create_user_1()
	t2,t2_want := generate_create_user_2()

	tests := []struct {
		name string
		args args
		want *CreateUser
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformCreatUserStmtToCreateUser(tt.args.cus);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformCreatUserStmtToCreateUser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_drop_user_1()(*ast.DropUserStmt,*DropUser){
	sql := `
drop user if exists
u1 , u2
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.DropUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	users := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","%","",""),
	}

	want := &DropUser{
		IfExists:      true,
		Users:         users,
	}

	return n, want
}

func Test_transformDropUserStmtToDropUser(t *testing.T) {
	type args struct {
		dus *ast.DropUserStmt
	}

	t1,t1_want := generate_drop_user_1()

	tests := []struct {
		name string
		args args
		want *DropUser
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDropUserStmtToDropUser(tt.args.dus); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDropUserStmtToDropUser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_alter_user_1() (*ast.AlterUserStmt,*AlterUser) {
	sql := `
alter user if exists
u1 identified by 'u1', u2 identified with u2row as 'u2'
require cipher 'xxx'  subject 'yyy'
with MAX_QUERIES_PER_HOUR 0 MAX_UPDATES_PER_HOUR 1
PASSWORD EXPIRE INTERVAL 1 DAY PASSWORD EXPIRE DEFAULT ACCOUNT LOCK ACCOUNT UNLOCK
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.AlterUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	users := []*User{
		NewUser("u1","%","","u1"),
		NewUser("u2","%","","u2"),//u2row can't be recoginzed
	}

	tls := []TlsOption{
		&TlsOptionCipher{Cipher: "xxx"},
		&TlsOptionSubject{Subject: "yyy"},
	}

	res := []ResourceOption{
		&ResourceOptionMaxQueriesPerHour{Count: 0},
		&ResourceOptionMaxUpdatesPerHour{Count: 1},
	}

	miscs := []UserMiscOption{
		&UserMiscOptionPasswordExpireInterval{Value: 1},
		&UserMiscOptionPasswordExpireDefault{},
		&UserMiscOptionAccountLock{},
		&UserMiscOptionAccountUnlock{},
	}

	want := &AlterUser{
		IfExists:   true,
		Users:         users,
		Roles:         nil,
		TlsOpts:       tls,
		ResOpts:       res,
		MiscOpts:      miscs,
	}

	return n, want
}

func generate_alter_user_2() (*ast.AlterUserStmt,*AlterUser) {
	sql := `
alter user
u1, u2
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.AlterUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	users := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","%","",""),//u2row can't be recoginzed
	}

	want := &AlterUser{
		IfExists:   false,
		Users:         users,
		Roles:         nil,
		TlsOpts:       make([]TlsOption,0),
		ResOpts:       make([]ResourceOption,0),
		MiscOpts:      make([]UserMiscOption,0),
	}
	return n,want
}

func generate_alter_user_3() (*ast.AlterUserStmt,*AlterUser) {
	sql := `
alter user
user() IDENTIFIED BY 'xxx'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.AlterUserStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &AlterUser{
		IfExists:   false,
		IsUserFunc: true,
		UserFunc: NewUser("","","","xxx"),
		Users:         nil,
		Roles:         nil,
		TlsOpts:       nil,
		ResOpts:       nil,
		MiscOpts:      nil,
	}
	return n,want
}

func Test_transformAlterUserStmtToAlterUser(t *testing.T) {
	type args struct {
		aus *ast.AlterUserStmt
	}

	t1,t1_want := generate_alter_user_1()
	t2,t2_want := generate_alter_user_2()
	t3,t3_want := generate_alter_user_3()

	tests := []struct {
		name string
		args args
		want *AlterUser
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformAlterUserStmtToAlterUser(tt.args.aus);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformAlterUserStmtToAlterUser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_roke_1()(*ast.RevokeStmt,*Revoke) {
	sql := `
revoke all,all(a,b),create(a,b),select(a,b),super(a,b,c)
on table db.A
from u1,'u2'@'h2',''@'h3'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.RevokeStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")

	privs := []*Privilege{
		NewPrivilege(PRIVILEGE_TYPE_STATIC_ALL,nil),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_ALL,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_CREATE,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SELECT,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SUPER,[]*UnresolvedName{aname,bname,cname}),
	}

	u := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","h2","",""),
		NewUser("","h3","",""),
	}

	want := &Revoke{
		Privileges:    privs,
		ObjType:       OBJECT_TYPE_TABLE,
		Level:         NewPrivilegeLevel(PRIVILEGE_LEVEL_TYPE_TABLE,"db","A",""),
		Users:         u,
		Roles:         nil,
	}

	return n, want
}

func generate_roke_2()(*ast.RevokeStmt,*Revoke) {
	sql := `
revoke super(a,b,c)
on procedure db.func
from '@''h3'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.RevokeStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")

	privs := []*Privilege{
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SUPER,[]*UnresolvedName{aname,bname,cname}),
	}

	u := []*User{
		NewUser("@'h3","%","",""),
	}

	want := &Revoke{
		Privileges:    privs,
		ObjType:       OBJECT_TYPE_PROCEDURE,
		Level:         NewPrivilegeLevel(PRIVILEGE_LEVEL_TYPE_TABLE,"db","func",""),
		Users:         u,
		Roles:         nil,
	}

	return n, want
}

func Test_transformRevokeStmtToRevoke(t *testing.T) {
	type args struct {
		rs *ast.RevokeStmt
	}

	t1,t1_want := generate_roke_1()
	t2,t2_want := generate_roke_2()

	tests := []struct {
		name string
		args args
		want *Revoke
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformRevokeStmtToRevoke(tt.args.rs);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformRevokeStmtToRevoke() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_roke_3()(*ast.RevokeRoleStmt,*Revoke) {
	sql := `
revoke r1,r2,r3 
from u1,u2,u3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.RevokeRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	rirr :=[]*Role{
		NewRole("r1","%"),
		NewRole("r2","%"),
		NewRole("r3","%"),
	}

	u := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","%","",""),
		NewUser("u3","%","",""),
	}

	want := &Revoke{
		IsRevokeRole: true,
		RolesInRevokeRole: rirr,
		Privileges:    nil,
		ObjType:       0,
		Level:         nil,
		Users:         u,
		Roles:         nil,
	}

	return n, want
}

func Test_transformRevokeRoleStmtToRevoke(t *testing.T) {
	type args struct {
		rrs *ast.RevokeRoleStmt
	}

	t1,t1_want := generate_roke_3()

	tests := []struct {
		name string
		args args
		want *Revoke
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformRevokeRoleStmtToRevoke(tt.args.rrs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformRevokeRoleStmtToRevoke() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_grant_1()(*ast.GrantStmt,*Grant){
	sql := `
grant all,all(a,b),create(a,b),select(a,b),super(a,b,c)
on table db.A
to u1,'u2'@'h2',''@'h3'
with grant option
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.GrantStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")

	privs := []*Privilege{
		NewPrivilege(PRIVILEGE_TYPE_STATIC_ALL,nil),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_ALL,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_CREATE,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SELECT,[]*UnresolvedName{aname,bname}),
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SUPER,[]*UnresolvedName{aname,bname,cname}),
	}

	u := []*User{
		NewUser("u1","%","",""),
		NewUser("u2","h2","",""),
		NewUser("","h3","",""),
	}

	want := &Grant{
		Privileges:    privs,
		ObjType:       OBJECT_TYPE_TABLE,
		Level:         NewPrivilegeLevel(PRIVILEGE_LEVEL_TYPE_TABLE,"db","A",""),
		Users:         u,
		Roles:         nil,
		GrantOption: true,
	}

	return n, want
}

func generate_grant_2()(*ast.GrantStmt,*Grant) {
	sql := `
grant super(a,b,c)
on procedure db.func
to '@''h3'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.GrantStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	aname,_ := NewUnresolvedName("","","a")
	bname,_ := NewUnresolvedName("","","b")
	cname,_ := NewUnresolvedName("","","c")

	privs := []*Privilege{
		NewPrivilege(PRIVILEGE_TYPE_STATIC_SUPER,[]*UnresolvedName{aname,bname,cname}),
	}

	u := []*User{
		NewUser("@'h3","%","",""),
	}

	want := &Grant{
		Privileges:    privs,
		ObjType:       OBJECT_TYPE_PROCEDURE,
		Level:         NewPrivilegeLevel(PRIVILEGE_LEVEL_TYPE_TABLE,"db","func",""),
		Users:         u,
		Roles:         nil,
	}

	return n, want
}

func Test_transformGrantStmtToGrant(t *testing.T) {
	type args struct {
		gs *ast.GrantStmt
	}

	t1,t1_want := generate_grant_1()
	t2,t2_want := generate_grant_2()

	tests := []struct {
		name string
		args args
		want *Grant
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformGrantStmtToGrant(tt.args.gs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformGrantStmtToGrant() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_grant_3()(*ast.GrantRoleStmt,*Grant){
	sql := `
grant r1,r2,r3
to u2,u3,u4
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.GrantRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &Grant{
		IsGrantRole:      true,
		RolesInGrantRole: []*Role{NewRole("r1","%"),NewRole("r2","%"),NewRole("r3","%")},
		Privileges:       nil,
		ObjType:          0,
		Level:            nil,
		Users:            []*User{NewUser("u2","%","",""),NewUser("u3","%","",""),NewUser("u4","%","","")},
		Roles:            nil,
		GrantOption:      false,
	}

	return n, want
}

func Test_transformGrantRoleStmtToGrant(t *testing.T) {
	type args struct {
		grs *ast.GrantRoleStmt
	}

	t1,t1_want := generate_grant_3()

	tests := []struct {
		name string
		args args
		want *Grant
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformGrantRoleStmtToGrant(tt.args.grs);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformGrantRoleStmtToGrant() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_grant_4()(*ast.GrantProxyStmt,*Grant){
	sql := `
grant proxy on u1
to u2,u3,u4
WITH GRANT OPTION
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.GrantProxyStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &Grant{
		IsGrantRole:      false,
		IsProxy: true,
		RolesInGrantRole: nil,
		Privileges:       nil,
		ObjType:          0,
		Level:            nil,
		ProxyUser: NewUser("u1","%","",""),
		Users:            []*User{NewUser("u2","%","",""),NewUser("u3","%","",""),NewUser("u4","%","","")},
		Roles:            nil,
		GrantOption:      true,
	}

	return n, want
}

func Test_transformGrantProxyStmtToGrant(t *testing.T) {
	type args struct {
		gps *ast.GrantProxyStmt
	}

	t1,t1_want := generate_grant_4()

	tests := []struct {
		name string
		args args
		want *Grant
	}{
		{"t1",args{t1},t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformGrantProxyStmtToGrant(tt.args.gps);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformGrantProxyStmtToGrant() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_setDefaultRole_1()(*ast.SetDefaultRoleStmt,*SetDefaultRole) {
	sql := `
set default role
none
to u1,u2,u3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetDefaultRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetDefaultRole{
		Type:          SET_DEFAULT_ROLE_TYPE_NONE,
		Roles:         nil,
		Users:         []*User{NewUser("u1","%","",""),NewUser("u2","%","",""),NewUser("u3","%","","")},
	}

	return n, want
}

func generate_setDefaultRole_2()(*ast.SetDefaultRoleStmt,*SetDefaultRole) {
	sql := `
set default role
all
to u1,u2,u3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetDefaultRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetDefaultRole{
		Type:          SET_DEFAULT_ROLE_TYPE_ALL,
		Roles:         nil,
		Users:         []*User{NewUser("u1","%","",""),NewUser("u2","%","",""),NewUser("u3","%","","")},
	}

	return n, want
}

func generate_setDefaultRole_3()(*ast.SetDefaultRoleStmt,*SetDefaultRole) {
	sql := `
set default role
r1,r2,r3
to u1,u2,u3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetDefaultRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetDefaultRole{
		Type:          SET_DEFAULT_ROLE_TYPE_NORMAL,
		Roles:         []*Role{NewRole("r1","%"),NewRole("r2","%"),NewRole("r3","%")},
		Users:         []*User{NewUser("u1","%","",""),NewUser("u2","%","",""),NewUser("u3","%","","")},
	}

	return n, want
}

func Test_transformSetDefaultRoleStmtToSetDefaultRole(t *testing.T) {
	type args struct {
		sdrs *ast.SetDefaultRoleStmt
	}

	t1,t1_want := generate_setDefaultRole_1()
	t2,t2_want := generate_setDefaultRole_2()
	t3,t3_want := generate_setDefaultRole_3()

	tests := []struct {
		name string
		args args
		want *SetDefaultRole
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSetDefaultRoleStmtToSetDefaultRole(tt.args.sdrs);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformSetDefaultRoleStmtToSetDefaultRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_set_role_1()(*ast.SetRoleStmt,*SetRole) {
	sql := `
set role
default
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetRole{
		Type:          SET_ROLE_TYPE_DEFAULT,
		Roles:         nil,
	}

	return n, want
}

func generate_set_role_2()(*ast.SetRoleStmt,*SetRole) {
	sql := `
set role
all except r1,r2,r3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetRole{
		Type:          SET_ROLE_TYPE_ALL_EXCEPT,
		Roles:         []*Role{NewRole("r1","%"),NewRole("r2","%"),NewRole("r3","%")},
	}

	return n, want
}

func generate_set_role_3()(*ast.SetRoleStmt,*SetRole) {
	sql := `
set role
r1,r2,r3
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetRoleStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetRole{
		Type:          SET_ROLE_TYPE_NORMAL,
		Roles:         []*Role{NewRole("r1","%"),NewRole("r2","%"),NewRole("r3","%")},
	}

	return n, want
}

func Test_transformSetRoleStmtToSetRole(t *testing.T) {
	type args struct {
		srs *ast.SetRoleStmt
	}

	t1,t1_want := generate_set_role_1()
	t2,t2_want := generate_set_role_2()
	t3,t3_want := generate_set_role_3()

	tests := []struct {
		name string
		args args
		want *SetRole
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
		{"t3",args{t3},t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSetRoleStmtToSetRole(tt.args.srs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformSetRoleStmtToSetRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func generate_set_password_1()(*ast.SetPwdStmt,*SetPassword) {
	sql := `
set password for u1@h1
= 'ppp'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetPwdStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetPassword{
		User:          NewUser("u1","h1","",""),
		Password:      "ppp",
	}

	return n,want
}

func generate_set_password_2()(*ast.SetPwdStmt,*SetPassword) {
	sql := `
set password
= 'ppp'
;`

	p := parser.New()
	stmt, _, err := p.Parse(sql, "", "")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}

	n, ok := stmt[0].(*ast.SetPwdStmt)
	if !ok {
		panic(fmt.Errorf("%v", ok))
	}

	want := &SetPassword{
		User:          nil,
		Password:      "ppp",
	}

	return n,want
}

func Test_transformSetPwdStmtToSetPassword(t *testing.T) {
	type args struct {
		sps *ast.SetPwdStmt
	}

	t1,t1_want := generate_set_password_1()
	t2,t2_want := generate_set_password_2()

	tests := []struct {
		name string
		args args
		want *SetPassword
	}{
		{"t1",args{t1},t1_want},
		{"t2",args{t2},t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSetPwdStmtToSetPassword(tt.args.sps); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformSetPwdStmtToSetPassword() = %v, want %v", got, tt.want)
			}
		})
	}
}