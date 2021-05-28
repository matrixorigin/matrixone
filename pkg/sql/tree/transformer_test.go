package tree

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
	_ "github.com/pingcap/parser/test_driver"
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

	sql :=`SELECT abs(u.a) + sqrt(u.b)
			FROM u

;`

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Errorf("parser parse failed.error:%v",err)
		return
	}

	for _, _ = range stmtNodes {

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
	t4 := test_driver.NewDatum(math.MaxUint64/2)
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
		{"t1",args{&t1},NewNumVal(constant.MakeInt64(math.MaxInt64),"",false)},
		{"t2",args{&t2},NewNumVal(constant.MakeInt64(math.MinInt64),"",false)},
		{"t3",args{&t3},NewNumVal(constant.MakeUnknown(),"",false)},
		{"t4",args{&t4},NewNumVal(constant.MakeUint64(math.MaxUint64/2),"",false)},
		{"t5",args{&t5},NewNumVal(constant.MakeUint64(0),"",false)},
		{"t6",args{&t6},NewNumVal(constant.MakeFloat64(math.MaxFloat32),"",false)},
		{"t7",args{&t7},NewNumVal(constant.MakeFloat64(-math.MaxFloat32),"",false)},
		{"t8",args{&t8},NewNumVal(constant.MakeFloat64(math.MaxFloat64),"",false)},
		{"t9",args{&t9},NewNumVal(constant.MakeFloat64(-math.MaxFloat64),"",false)},
		{"t10",args{&t10},NewNumVal(constant.MakeString(s),"",false)},
		{"t11",args{&t11},NewNumVal(constant.MakeInt64(1),"",false)},
		{"t12",args{&t12},NewNumVal(constant.MakeInt64(0),"",false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformDatumToNumVal(tt.args.datum); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformDatumToNumVal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformExprNodeToExpr(t *testing.T) {
	type args struct {
		node ast.ExprNode
	}

	t1 := ast.NewValueExpr(math.MaxInt64,"","")
	t2 := ast.NewValueExpr(math.MinInt64,"","")
	t3 := ast.NewValueExpr(nil,"","")
	//this case is unwanted. Datum.SetUint64 is wrong.
	t4 := ast.NewValueExpr(math.MaxUint64/2,"","")
	t5 := ast.NewValueExpr(0,"","")
	t6 := ast.NewValueExpr(math.MaxFloat32,"","")
	t7 := ast.NewValueExpr(-math.MaxFloat32,"","")
	t8 := ast.NewValueExpr(math.MaxFloat64,"","")
	t9 := ast.NewValueExpr(-math.MaxFloat64,"","")
	s := "a string"
	t10 := ast.NewValueExpr(s,"","")

	e1 := ast.NewValueExpr(1, "","")
	e2 := ast.NewValueExpr(2, "","")
	e3 := ast.NewValueExpr(3, "","")
	e4 := ast.NewValueExpr(4, "","")
	e5 := ast.NewValueExpr(5, "","")
	e6 := ast.NewValueExpr(6, "","")
	eTrue := ast.NewValueExpr(true, "","")
	eFalse := ast.NewValueExpr(false, "","")

	f1 := NewNumVal(constant.MakeInt64(1),"",false)
	f2 := NewNumVal(constant.MakeInt64(2),"",false)
	f3 := NewNumVal(constant.MakeInt64(3),"",false)
	f4 := NewNumVal(constant.MakeInt64(4),"",false)
	f5 := NewNumVal(constant.MakeInt64(5),"",false)
	f6 := NewNumVal(constant.MakeInt64(6),"",false)
	fTrue := NewNumVal(constant.MakeInt64(1),"",false)
	fFalse := NewNumVal(constant.MakeInt64(0),"",false)

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
		NewUnaryExpr(UNARY_MINUS,f1),
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
		NewUnaryExpr(UNARY_PLUS,f1),
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
		NewUnaryExpr(UNARY_TILDE,f1),
		f2,
	)

	//!1
	t18 := &ast.UnaryOperationExpr{
		Op: opcode.Not2,
		V:  e1,
	}

	t18Want := NewUnaryExpr(UNARY_MARK,f1)

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

	t26want_1 := NewBinaryExpr(DIV,f5,f6)
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

	t35Want := NewAndExpr(f1,f2)

	//1 or 0
	t36 := &ast.BinaryOperationExpr{
		Op: opcode.LogicOr,
		L:  e1,
		R:  e2,
	}

	t36Want := NewOrExpr(f1,f2)

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

	t38Want := NewXorExpr(f1,f2)

	// is null
	t39 := &ast.IsNullExpr{
		Expr:e1,
		Not:false,
	}

	t39Want := NewIsNullExpr(f1)

	// is not null
	t40 := &ast.IsNullExpr{
		Expr:e1,
		Not:true,
	}

	t40Want := NewIsNotNullExpr(f1)

	//2 IN (1,2,3,4);
	t41 := &ast.PatternInExpr{
		Expr: e2,
		List: []ast.ExprNode{e1, e2, e3, e4},
		Not:  false,
		Sel:  nil,
	}

	t41Want := NewComparisonExpr(IN,f2,&ExprList{
		Exprs:   []Expr{f1,f2,f3,f4},
	})

	//2 NOT IN (1,2,3,4);
	t42 := &ast.PatternInExpr{
		Expr: e2,
		List: []ast.ExprNode{e1, e2, e3, e4},
		Not:  true,
		Sel:  nil,
	}

	t42Want := NewComparisonExpr(NOT_IN,f2,&ExprList{
		Exprs:   []Expr{f1,f2,f3,f4},
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

	t43Want := NewComparisonExpr(LIKE,f1,f2)

	//2 NOT LIKE 'xxx';
	t44 := &ast.PatternLikeExpr{
		Expr:     e1,
		Pattern:  e2,
		Not:      true,
		Escape:   0,
		PatChars: nil,
		PatTypes: nil,
	}

	t44Want := NewComparisonExpr(NOT_LIKE,f1,f2)

	//2 REGEXP 'xxx';
	t45 := &ast.PatternRegexpExpr{
		Expr:    e1,
		Pattern: e2,
		Not:     false,
		Re:      nil,
		Sexpr:   nil,
	}

	t45Want := NewComparisonExpr(REG_MATCH,f1,f2)

	//2 NOT REGEXP 'xxx';
	t46 := &ast.PatternRegexpExpr{
		Expr:    e1,
		Pattern: e2,
		Not:     true,
		Re:      nil,
		Sexpr:   nil,
	}

	t46Want := NewComparisonExpr(NOT_REG_MATCH,f1,f2)

	//SubqueryExpr nil
	t47:=&ast.SubqueryExpr{
		Query:      nil,
		Evaluated:  false,
		Correlated: false,
		MultiRows:  false,
		Exists:     false,
	}

	t47Want := NewSubquery(nil,false)

	//ExistsSubqueryExpr
	t48:=&ast.ExistsSubqueryExpr{
		Sel: e1,
		Not: true,
	}

	//just for passing the case
	var _ SelectStatement = f1
	t48Want := NewSubquery(f1,true)

	//CompareSubqueryExpr
	t49:=&ast.CompareSubqueryExpr{
		L:   e1,
		Op:  opcode.GT,
		R:   e2,
		All: true,
	}

	//just for passing the case
	var _ SelectStatement = f2
	t49Want := NewComparisonExprWithSubop(GREAT_THAN,ALL,f1,f2)

	// '(' e1 ')'
	t50 :=&ast.ParenthesesExpr{Expr: e1}

	t50Want := NewParenExpr(f1)

	tests := []struct {
		name string
		args args
		want Expr
	}{
		{"t1",args{t1},NewNumVal(constant.MakeInt64(math.MaxInt64),"",false)},
		{"t2",args{t2},NewNumVal(constant.MakeInt64(math.MinInt64),"",false)},
		{"t3",args{t3},NewNumVal(constant.MakeUnknown(),"",false)},
		{"t4",args{t4},NewNumVal(constant.MakeUint64(math.MaxUint64/2),"",false)},
		{"t5",args{t5},NewNumVal(constant.MakeUint64(0),"",false)},
		{"t6",args{t6},NewNumVal(constant.MakeFloat64(math.MaxFloat32),"",false)},
		{"t7",args{t7},NewNumVal(constant.MakeFloat64(-math.MaxFloat32),"",false)},
		{"t8",args{t8},NewNumVal(constant.MakeFloat64(math.MaxFloat64),"",false)},
		{"t9",args{t9},NewNumVal(constant.MakeFloat64(-math.MaxFloat64),"",false)},
		{"t10",args{t10},NewNumVal(constant.MakeString(s),"",false)},
		{"t11",args{t11}, t11Want},
		{"t12",args{t12}, t12Want},
		{"t13",args{t13}, t13Want},
		{"t14",args{t14}, t14Want},
		{"t15",args{t15}, t15Want},
		{"t16",args{t16}, t16Want},
		{"t17",args{t17}, t17Want},
		{"t18",args{t18}, t18Want},
		{"t19",args{eTrue},fTrue},
		{"t20",args{eFalse},fFalse},
		{"t21",args{t21}, t21Want},
		{"t22",args{t22}, t22Want},
		{"t23",args{t23}, t23Want},
		{"t24",args{t24}, t24Want},
		{"t25",args{t25}, t25Want},
		{"t26",args{t26}, t26Want},
		{"t27",args{t27}, t27Want},
		{"t28",args{t28}, t28Want},
		{"t29",args{t29}, t29Want},
		{"t30",args{t30}, t30Want},
		{"t31",args{t31}, t31Want},
		{"t32",args{t32}, t32Want},
		{"t33",args{t33}, t33Want},
		{"t34",args{t34}, t34Want},
		{"t35",args{t35}, t35Want},
		{"t36",args{t36}, t36Want},
		{"t37",args{t37}, t37Want},
		{"t38",args{t38}, t38Want},
		{"t39",args{t39}, t39Want},
		{"t40",args{t40}, t40Want},
		{"t41",args{t41}, t41Want},
		{"t42",args{t42}, t42Want},
		{"t43",args{t43}, t43Want},
		{"t44",args{t44}, t44Want},
		{"t45",args{t45}, t45Want},
		{"t46",args{t46}, t46Want},
		{"t47",args{t47}, t47Want},
		{"t48",args{t48}, t48Want},
		{"t49",args{t49}, t49Want},
		{"t50",args{t50}, t50Want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformExprNodeToExpr(tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformExprNodeToExpr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transformColumnNameListToNameList(t *testing.T) {
	type args struct {
		cn []*ast.ColumnName
	}
	l1:=[]*ast.ColumnName{
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{"A","a"},
		},
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{"B","b"},
		},
		&ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.CIStr{},
			Name:   model.CIStr{"C","c"},
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
		{"t1",args{l1},l1Want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformColumnNameListToNameList(tt.args.cn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transformColumnNameListToNameList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func gen_transform_t1()(*ast.SelectStmt,*Select){
	//SELECT t.a FROM sa.t ;
	//SELECT t.a FROM sa.t,u ;
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{"sa","sa"},
		Name:           model.CIStr{"t","t"},
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
		Name:           model.CIStr{"u","u"},
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
		Table:  model.CIStr{"t","t"},
		Name:   model.CIStr{"a","a"},
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

	t1FieldList :=&ast.FieldList{Fields: t1SelectField}

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

	t1wantTableName :=&TableName{
		objName:   objName{
			ObjectName: "t",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr:      t1wantTableName,
		As:        AliasClause{},
	}

	t1wantTableName2 :=&TableName{
		objName:   objName{
			ObjectName: "u",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr:      t1wantTableName2,
		As:        AliasClause{},
	}

	t1wantTableExprArray :=[]TableExpr{
		&JoinTableExpr{
			JoinType:  JOIN_TYPE_CROSS,
			Left:      t1wantAliasedTableExpr,
			Right:     t1wantAliasedTableExpr2,
			Cond:      nil,
		},

	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantFied,_ := NewUnresolvedName("","t","a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: t1wantFied,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:            t1wantFrom,
		Distinct:        false,
		Where:           nil,
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t2()(*ast.SelectStmt,*Select){
	//SELECT t.a FROM sa.t,u,v ;
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{"sa","sa"},
		Name:           model.CIStr{"t","t"},
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
		Name:           model.CIStr{"u","u"},
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
		Name:           model.CIStr{"v","v"},
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
		Table:  model.CIStr{"t","t"},
		Name:   model.CIStr{"a","a"},
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

	t1FieldList :=&ast.FieldList{Fields: t1SelectField}

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

	t1wantTableName :=&TableName{
		objName:   objName{
			ObjectName: "t",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr:      t1wantTableName,
		As:        AliasClause{},
	}

	t1wantTableName2 :=&TableName{
		objName:   objName{
			ObjectName: "u",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr:      t1wantTableName2,
		As:        AliasClause{},
	}

	t1wantTableName3 :=&TableName{
		objName:   objName{
			ObjectName: "v",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr3 := &AliasedTableExpr{
		Expr:      t1wantTableName3,
		As:        AliasClause{},
	}

	t1wantJoin_1_2 :=&JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      t1wantAliasedTableExpr,
		Right:     t1wantAliasedTableExpr2,
		Cond:      nil,
	}

	t1wantJoin_1_2_Join_3 :=&JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      t1wantJoin_1_2,
		Right:     t1wantAliasedTableExpr3,
		Cond:      nil,
	}

	t1wantTableExprArray :=[]TableExpr{
		t1wantJoin_1_2_Join_3,
	}

	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantFied,_ := NewUnresolvedName("","t","a")

	t1wantFieldList := []SelectExpr{
		{
			Expr: t1wantFied,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:            t1wantFrom,
		Distinct:        false,
		Where:           nil,
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t3()(*ast.SelectStmt,*Select){
	//SELECT t.a,u.a FROM sa.t,u where t.a = u.a;
	t1TableName := &ast.TableName{
		Schema:         model.CIStr{"sa","sa"},
		Name:           model.CIStr{"t","t"},
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
		Name:           model.CIStr{"u","u"},
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
		Table:  model.CIStr{"t","t"},
		Name:   model.CIStr{"a","a"},
	}
	t1ColumnNameExpr := &ast.ColumnNameExpr{
		Name:  t1ColumnName,
		Refer: nil,
	}

	t1ColumnName2 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{"u","u"},
		Name:   model.CIStr{"a","a"},
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

	t1FieldList :=&ast.FieldList{Fields: t1SelectField}

	t1ColumnName3 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{"t","t"},
		Name:   model.CIStr{"a","a"},
	}
	t1Where1 := &ast.ColumnNameExpr{
		Name:  t1ColumnName3,
		Refer: nil,
	}

	t1ColumnName4 := &ast.ColumnName{
		Schema: model.CIStr{},
		Table:  model.CIStr{"u","u"},
		Name:   model.CIStr{"a","a"},
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

	t1wantTableName :=&TableName{
		objName:   objName{
			ObjectName: "t",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "sa",
				ExplicitCatalog: false,
				ExplicitSchema:  true,
			},
		},
	}

	t1wantAliasedTableExpr := &AliasedTableExpr{
		Expr:      t1wantTableName,
		As:        AliasClause{},
	}

	t1wantTableName2 :=&TableName{
		objName:   objName{
			ObjectName: "u",
			ObjectNamePrefix:ObjectNamePrefix{
				CatalogName:     "",
				SchemaName:      "",
				ExplicitCatalog: false,
				ExplicitSchema:  false,
			},
		},
	}

	t1wantAliasedTableExpr2 := &AliasedTableExpr{
		Expr:      t1wantTableName2,
		As:        AliasClause{},
	}

	t1wantTableExprArray :=[]TableExpr{
		&JoinTableExpr{
			JoinType:  JOIN_TYPE_CROSS,
			Left:      t1wantAliasedTableExpr,
			Right:     t1wantAliasedTableExpr2,
			Cond:      nil,
		},

	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	t1wantField1,_ := NewUnresolvedName("","t","a")
	t1wantField2,_ := NewUnresolvedName("","u","a")

	t1wantWhere1,_ := NewUnresolvedName("","t","a")
	t1wantWhere2,_ := NewUnresolvedName("","u","a")

	t1wantWhereExpr := &ComparisonExpr{
		Op:       EQUAL,
		SubOp:    0,
		Left:     t1wantWhere1,
		Right:    t1wantWhere2,
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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(t1wantWhereExpr),
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_var_ref(schema,table,name string)*ast.ColumnNameExpr{
	var_name := &ast.ColumnName{
		Schema: model.CIStr{schema,schema},
		Table:  model.CIStr{table,table},
		Name:   model.CIStr{name,name},
	}
	var_ := &ast.ColumnNameExpr{
		Name:  var_name,
		Refer: nil,
	}
	return var_
}

func gen_binary_expr(op opcode.Op,l,r ast.ExprNode)*ast.BinaryOperationExpr{
	return &ast.BinaryOperationExpr{
		Op: op,
		L:  l,
		R:  r,
	}
}

func gen_table(sch,name string)*ast.TableSource{
	sa_t_name := &ast.TableName{
		Schema:         model.CIStr{sch,sch},
		Name:           model.CIStr{name,name},
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

func gen_want_table(sch,name string)*AliasedTableExpr{
	want_sa_t_name := NewTableName(Identifier(name),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      Identifier(sch),
		ExplicitCatalog: false,
		ExplicitSchema:  len(sch) != 0,
	})

	want_sa_t :=  &AliasedTableExpr{
		Expr: want_sa_t_name,
		As:   AliasClause{},
	}
	return want_sa_t
}

func gen_transform_t4()(*ast.SelectStmt,*Select){
	//SELECT t.a,u.a,t.b * u.b FROM sa.t,u where t.a = u.a and t.b > u.b;
	sa_t := gen_table("sa","t")

	u := gen_table("","u")

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

	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_table_refs :=[]TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     want_sa_t,
			Right:    want_u,
			Cond:     nil,
		},

	}
	t1wantFrom := &From{Tables: want_table_refs}

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t5()(*ast.SelectStmt,*Select){
	//SELECT t.a,u.a,t.b * u.b FROM sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b;
	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")
	t_c := gen_var_ref("","t","c")
	t_d := gen_var_ref("","t","d")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")
	u_c := gen_var_ref("","u","c")
	u_d := gen_var_ref("","u","d")

	sa_t := gen_table("sa","t")

	u := gen_table("","u")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ,t_c,u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE,t_d,u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr,t_c_eq_u_c,t_d_ne_u_d)

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

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")
	want_t_c,_ := NewUnresolvedName("","t","c")
	want_t_d,_ := NewUnresolvedName("","t","d")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")
	want_u_c,_ := NewUnresolvedName("","u","c")
	want_u_d,_ := NewUnresolvedName("","u","d")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL,want_t_c,want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL,want_t_d,want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c,want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_table_refs :=[]TableExpr{
		&JoinTableExpr{
			JoinType: JOIN_TYPE_CROSS,
			Left:     want_sa_t,
			Right:    want_u,
			Cond:     want_join_on,
		},

	}
	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t6()(*ast.SelectStmt,*Select){
	/*
	SELECT t.a,u.a,t.b * u.b
				FROM sa.t join u on t.c = u.c or t.d != u.d
						  join v on u.a != v.a
				where t.a = u.a and t.b > u.b;
	 */
	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")
	t_c := gen_var_ref("","t","c")
	t_d := gen_var_ref("","t","d")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")
	u_c := gen_var_ref("","u","c")
	u_d := gen_var_ref("","u","d")

	v_a := gen_var_ref("","v","a")
	//v_b := gen_var_ref("","v","b")
	//v_c := gen_var_ref("","v","c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa","t")

	u := gen_table("","u")

	v := gen_table("","v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ,t_c,u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE,t_d,u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr,t_c_eq_u_c,t_d_ne_u_d)

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
	u_a_ne_v_a := gen_binary_expr(opcode.NE,u_a,v_a)

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

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")
	want_t_c,_ := NewUnresolvedName("","t","c")
	want_t_d,_ := NewUnresolvedName("","t","d")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")
	want_u_c,_ := NewUnresolvedName("","u","c")
	want_u_d,_ := NewUnresolvedName("","u","d")

	want_v_a,_ := NewUnresolvedName("","v","a")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL,want_t_c,want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL,want_t_d,want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c,want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL,want_u_a,want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_v := gen_want_table("","v")

	want_t_u_join :=&JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      want_t_u_join,
		Right:     want_v,
		Cond:      want_u_v_join_on,
	}

	want_table_refs :=[]TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t7()(*ast.SelectStmt,*Select){
	/*
		SELECT t.a,u.a,t.b * u.b
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b;
		group by t.a,u.a,(t.b+u.b+v.b)
	*/
	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")
	t_c := gen_var_ref("","t","c")
	t_d := gen_var_ref("","t","d")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")
	u_c := gen_var_ref("","u","c")
	u_d := gen_var_ref("","u","d")

	v_a := gen_var_ref("","v","a")
	v_b := gen_var_ref("","v","b")
	//v_c := gen_var_ref("","v","c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa","t")

	u := gen_table("","u")

	v := gen_table("","v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ,t_c,u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE,t_d,u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr,t_c_eq_u_c,t_d_ne_u_d)

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
	u_a_ne_v_a := gen_binary_expr(opcode.NE,u_a,v_a)

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

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus,gen_binary_expr(opcode.Plus,t_b,u_b),v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item :=[]*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby :=&ast.GroupByClause{Items: t_groupby_item}

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

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")
	want_t_c,_ := NewUnresolvedName("","t","c")
	want_t_d,_ := NewUnresolvedName("","t","d")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")
	want_u_c,_ := NewUnresolvedName("","u","c")
	want_u_d,_ := NewUnresolvedName("","u","d")

	want_v_a,_ := NewUnresolvedName("","v","a")
	want_v_b,_ := NewUnresolvedName("","v","b")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL,want_t_c,want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL,want_t_d,want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c,want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL,want_u_a,want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_v := gen_want_table("","v")

	want_t_u_join :=&JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      want_t_u_join,
		Right:     want_v,
		Cond:      want_u_v_join_on,
	}

	want_table_refs :=[]TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS,NewBinaryExpr(PLUS,want_t_b,want_u_b),want_v_b)

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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         want_groupby,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t8()(*ast.SelectStmt,*Select){
	/*
	SELECT t.a,u.a,t.b * u.b
	FROM sa.t join u on t.c = u.c or t.d != u.d
			  join v on u.a != v.a
	where t.a = u.a and t.b > u.b;
	group by t.a,u.a,(t.b+u.b+v.b)
	having t.a = 'jj' and v.c > 1000
	*/
	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")
	t_c := gen_var_ref("","t","c")
	t_d := gen_var_ref("","t","d")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")
	u_c := gen_var_ref("","u","c")
	u_d := gen_var_ref("","u","d")

	v_a := gen_var_ref("","v","a")
	v_b := gen_var_ref("","v","b")
	v_c := gen_var_ref("","v","c")
	//v_d := gen_var_ref("","v","d")

	sa_t := gen_table("sa","t")

	u := gen_table("","u")

	v := gen_table("","v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ,t_c,u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE,t_d,u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr,t_c_eq_u_c,t_d_ne_u_d)

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
	u_a_ne_v_a := gen_binary_expr(opcode.NE,u_a,v_a)

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

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus,gen_binary_expr(opcode.Plus,t_b,u_b),v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item :=[]*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby :=&ast.GroupByClause{Items: t_groupby_item}

	//having t.a = 'jj' and v.c > 1000

	t_jj := ast.NewValueExpr("jj","","")
	t_a_eq_tjj := gen_binary_expr(opcode.EQ,t_a,t_jj)
	v_c_gt_1000 := gen_binary_expr(opcode.GT,v_c,ast.NewValueExpr(1000,"",""))
	t_a_eq_tjj_and_v_c_gt_1000 := gen_binary_expr(opcode.LogicAnd,t_a_eq_tjj,v_c_gt_1000)

	t_having :=&ast.HavingClause{Expr: t_a_eq_tjj_and_v_c_gt_1000}

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

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")
	want_t_c,_ := NewUnresolvedName("","t","c")
	want_t_d,_ := NewUnresolvedName("","t","d")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")
	want_u_c,_ := NewUnresolvedName("","u","c")
	want_u_d,_ := NewUnresolvedName("","u","d")

	want_v_a,_ := NewUnresolvedName("","v","a")
	want_v_b,_ := NewUnresolvedName("","v","b")
	want_v_c,_ := NewUnresolvedName("","v","c")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL,want_t_c,want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL,want_t_d,want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c,want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL,want_u_a,want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_v := gen_want_table("","v")

	want_t_u_join :=&JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      want_t_u_join,
		Right:     want_v,
		Cond:      want_u_v_join_on,
	}

	want_table_refs :=[]TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS,NewBinaryExpr(PLUS,want_t_b,want_u_b),want_v_b)

	want_paren_t_b_plus_u_b_plus_v_b := NewParenExpr(want_t_b_plus_u_b_plus_v_b)

	//group by t.a,u.a,(t.b+u.b+v.b)
	want_groupby := []Expr{
		want_t_a,
		want_u_a,
		want_paren_t_b_plus_u_b_plus_v_b,
	}

	//having t.a = 'jj' and v.c > 1000
	want_tjj :=NewNumVal(constant.MakeString("jj"),"",false)
	want_t_a_eq_tjj := NewComparisonExpr(EQUAL,want_t_a,want_tjj)
	want_v_c_gt_1000 := NewComparisonExpr(GREAT_THAN,want_v_c,NewNumVal(constant.MakeInt64(1000),"",false))
	want_t_a_eq_tjj_and_v_c_gt_1000 := NewAndExpr(want_t_a_eq_tjj,want_v_c_gt_1000)
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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         want_groupby,
		Having:          want_having,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t9()(*ast.SelectStmt,*Select){
	/*
	SELECT t.a,u.a,t.b * u.b tubb
	FROM sa.t join u on t.c = u.c or t.d != u.d
			  join v on u.a != v.a
	where t.a = u.a and t.b > u.b;
	group by t.a,u.a,(t.b+u.b+v.b)
	having t.a = 'jj' and v.c > 1000
	order by t.a asc,u.a desc,v.d asc,tubb
	limit 100,2000
	*/
	t_a := gen_var_ref("","t","a")
	t_b := gen_var_ref("","t","b")
	t_c := gen_var_ref("","t","c")
	t_d := gen_var_ref("","t","d")

	u_a := gen_var_ref("","u","a")
	u_b := gen_var_ref("","u","b")
	u_c := gen_var_ref("","u","c")
	u_d := gen_var_ref("","u","d")

	v_a := gen_var_ref("","v","a")
	v_b := gen_var_ref("","v","b")
	v_c := gen_var_ref("","v","c")
	v_d := gen_var_ref("","v","d")
	tubb := gen_var_ref("","","tubb")

	sa_t := gen_table("sa","t")

	u := gen_table("","u")

	v := gen_table("","v")

	t_c_eq_u_c := gen_binary_expr(opcode.EQ,t_c,u_c)
	t_d_ne_u_d := gen_binary_expr(opcode.NE,t_d,u_d)
	t_c_eq_u_c_or_t_d_ne_u_d := gen_binary_expr(opcode.LogicOr,t_c_eq_u_c,t_d_ne_u_d)

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
	u_a_ne_v_a := gen_binary_expr(opcode.NE,u_a,v_a)

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

	t_b_multi_u_b := gen_binary_expr(opcode.Mul,t_b,u_b)
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
			AsName:    model.CIStr{"tubb","tubb"},
			Auxiliary: false,
		},
	}

	t1FieldList :=&ast.FieldList{Fields: select_fields}

	//t.a = u.a
	t_a_eq_u_a := gen_binary_expr(opcode.EQ,t_a,u_a)

	//t.b > u.b
	t_b_gt_u_b := gen_binary_expr(opcode.GT,t_b,u_b)

	//t.a = u.a and t.b > u.b
	t_a_eq_u_a_and_t_b_gt_u_b := gen_binary_expr(opcode.LogicAnd,t_a_eq_u_a,t_b_gt_u_b)

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

	t_b_plus_u_b_plus_v_b := gen_binary_expr(opcode.Plus,gen_binary_expr(opcode.Plus,t_b,u_b),v_b)

	paren_t_b_plus_u_b_plus_v_b := &ast.ParenthesesExpr{Expr: t_b_plus_u_b_plus_v_b}

	byitem_t_b_plus_u_b_plus_v_b := &ast.ByItem{
		Expr:      paren_t_b_plus_u_b_plus_v_b,
		Desc:      false,
		NullOrder: true,
	}

	t_groupby_item :=[]*ast.ByItem{
		byitem_t_a,
		byitem_u_a,
		byitem_t_b_plus_u_b_plus_v_b,
	}

	t_groupby :=&ast.GroupByClause{Items: t_groupby_item}

	//having t.a = 'jj' and v.c > 1000

	t_jj := ast.NewValueExpr("jj","","")
	t_a_eq_tjj := gen_binary_expr(opcode.EQ,t_a,t_jj)
	v_c_gt_1000 := gen_binary_expr(opcode.GT,v_c,ast.NewValueExpr(1000,"",""))
	t_a_eq_tjj_and_v_c_gt_1000 := gen_binary_expr(opcode.LogicAnd,t_a_eq_tjj,v_c_gt_1000)

	t_having :=&ast.HavingClause{Expr: t_a_eq_tjj_and_v_c_gt_1000}

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

	byitem_v_d_asc :=&ast.ByItem{
		Expr:      v_d,
		Desc:      false,
		NullOrder: false,
	}

	byitem_tubb :=&ast.ByItem{
		Expr:      tubb,
		Desc:      false,
		NullOrder: false,
	}

	t_orderby_items :=[]*ast.ByItem{
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
		Count:  ast.NewValueExpr(2000,"",""),
		Offset: ast.NewValueExpr(100,"",""),
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

	want_t_a,_ := NewUnresolvedName("","t","a")
	want_t_b,_ := NewUnresolvedName("","t","b")
	want_t_c,_ := NewUnresolvedName("","t","c")
	want_t_d,_ := NewUnresolvedName("","t","d")

	want_u_a,_ := NewUnresolvedName("","u","a")
	want_u_b,_ := NewUnresolvedName("","u","b")
	want_u_c,_ := NewUnresolvedName("","u","c")
	want_u_d,_ := NewUnresolvedName("","u","d")

	want_v_a,_ := NewUnresolvedName("","v","a")
	want_v_b,_ := NewUnresolvedName("","v","b")
	want_v_c,_ := NewUnresolvedName("","v","c")
	want_v_d,_ := NewUnresolvedName("","v","d")

	want_tubb,_ := NewUnresolvedName("","","tubb")

	//t.c = u.c or t.d != u.d
	want_t_c_eq_u_c := NewComparisonExpr(EQUAL,want_t_c,want_u_c)
	want_t_d_ne_u_d := NewComparisonExpr(NOT_EQUAL,want_t_d,want_u_d)
	want_t_c_eq_u_c_or_t_d_ne_u_d := NewOrExpr(want_t_c_eq_u_c,want_t_d_ne_u_d)

	want_join_on := NewOnJoinCond(want_t_c_eq_u_c_or_t_d_ne_u_d)

	//u.a != v.a
	want_u_a_ne_v_a := NewComparisonExpr(NOT_EQUAL,want_u_a,want_v_a)
	want_u_v_join_on := NewOnJoinCond(want_u_a_ne_v_a)

	want_sa_t :=gen_want_table("sa","t")

	want_u := gen_want_table("","u")

	want_v := gen_want_table("","v")

	want_t_u_join :=&JoinTableExpr{
		JoinType: JOIN_TYPE_CROSS,
		Left:     want_sa_t,
		Right:    want_u,
		Cond:     want_join_on,
	}

	want_u_v_join := &JoinTableExpr{
		JoinType:  JOIN_TYPE_CROSS,
		Left:      want_t_u_join,
		Right:     want_v,
		Cond:      want_u_v_join_on,
	}

	want_table_refs :=[]TableExpr{
		want_u_v_join,
	}

	t1wantFrom := &From{Tables: want_table_refs}

	//t.b * u.b
	want_t_b_multi_u_b := NewBinaryExpr(MULTI,want_t_b,want_u_b)

	//t.a = u.a
	want_t_a_eq_u_a := NewComparisonExpr(EQUAL,want_t_a,want_u_a)

	//t.b > u.b
	want_t_b_gt_u_b := NewComparisonExpr(GREAT_THAN,want_t_b,want_u_b)

	//t.a = u.a and t.b > u.b
	want_t_a_eq_u_a_logicand_t_b_gt_u_b := NewAndExpr(want_t_a_eq_u_a,want_t_b_gt_u_b)

	want_t_b_plus_u_b_plus_v_b := NewBinaryExpr(PLUS,NewBinaryExpr(PLUS,want_t_b,want_u_b),want_v_b)

	want_paren_t_b_plus_u_b_plus_v_b := NewParenExpr(want_t_b_plus_u_b_plus_v_b)

	//group by t.a,u.a,(t.b+u.b+v.b)
	want_groupby := []Expr{
		want_t_a,
		want_u_a,
		want_paren_t_b_plus_u_b_plus_v_b,
	}

	//having t.a = 'jj' and v.c > 1000
	want_tjj :=NewNumVal(constant.MakeString("jj"),"",false)
	want_t_a_eq_tjj := NewComparisonExpr(EQUAL,want_t_a,want_tjj)
	num1000 := NewNumVal(constant.MakeInt64(1000),"",false)
	want_v_c_gt_1000 := NewComparisonExpr(GREAT_THAN,want_v_c,num1000)
	want_t_a_eq_tjj_and_v_c_gt_1000 := NewAndExpr(want_t_a_eq_tjj,want_v_c_gt_1000)
	want_having := NewWhere(want_t_a_eq_tjj_and_v_c_gt_1000)

	////order by t.a asc,u.a desc,v.d asc,tubb
	want_orderby := []*Order{
		NewOrder(want_t_a,Ascending,false),
		NewOrder(want_u_a,Descending,false),
		NewOrder(want_v_d,Ascending,false),
		NewOrder(want_tubb,Ascending,false),
	}

	//limit 100,2000
	num100 := NewNumVal(constant.MakeInt64(100),"",false)
	num2000 := NewNumVal(constant.MakeInt64(2000),"",false)

	want_limit := NewLimit(num100,num2000)

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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           NewWhere(want_t_a_eq_u_a_logicand_t_b_gt_u_b),
		Exprs:           t1wantFieldList,
		GroupBy:         want_groupby,
		Having:          want_having,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: want_orderby,
		Limit:   want_limit,
	}
	return t1,t1want
}

func gen_transform_t10()(*ast.SelectStmt,*Select){
	//SELECT *	FROM u;

	u := gen_table("","u")

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

	t1FieldList :=&ast.FieldList{Fields: t1SelectField}

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

	want_u := gen_want_table("","u")

	t1wantTableExprArray :=[]TableExpr{
		&JoinTableExpr{
			JoinType:  "",
			Left:      want_u,
			Right:     nil,
			Cond:      nil,
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
		From:            t1wantFrom,
		Distinct:        false,
		Where:           nil,
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func gen_transform_t11()(*ast.SelectStmt,*Select){
	//SELECT u.*	FROM u;

	u := gen_table("","u")

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

	t1FieldList :=&ast.FieldList{Fields: t1SelectField}

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

	want_u := gen_want_table("","u")

	t1wantTableExprArray :=[]TableExpr{
		&JoinTableExpr{
			JoinType:  "",
			Left:      want_u,
			Right:     nil,
			Cond:      nil,
		},

	}
	t1wantFrom := &From{Tables: t1wantTableExprArray}

	want_star,_ := NewUnresolvedNameWithStar("u")

	t1wantFieldList := []SelectExpr{
		{
			Expr: want_star,
			As:   "",
		},
	}

	t1wantSelectClause := &SelectClause{
		From:            t1wantFrom,
		Distinct:        false,
		Where:           nil,
		Exprs:           t1wantFieldList,
		GroupBy:         nil,
		Having:          nil,
	}

	t1want := &Select{
		Select:  t1wantSelectClause,
		OrderBy: nil,
		Limit:   nil,
	}
	return t1,t1want
}

func Test_transformSelectStmtToSelect(t *testing.T) {
	type args struct {
		ss *ast.SelectStmt
	}

	t1,t1want :=gen_transform_t1()
	t2,t2want :=gen_transform_t2()
	t3,t3want :=gen_transform_t3()
	t4,t4want :=gen_transform_t4()
	t5,t5want :=gen_transform_t5()
	t6,t6want :=gen_transform_t6()
	t7,t7want :=gen_transform_t7()
	t8,t8want :=gen_transform_t8()
	t9,t9want :=gen_transform_t9()
	t10,t10want :=gen_transform_t10()
	t11,t11want :=gen_transform_t11()

	tests := []struct {
		name string
		args args
		want *Select
	}{
		{"t1",args{t1},t1want},
		{"t2",args{t2},t2want},
		{"t3",args{t3},t3want},
		{"t4",args{t4},t4want},
		{"t5",args{t5},t5want},
		{"t6",args{t6},t6want},
		{"t7",args{t7},t7want},
		{"t8",args{t8},t8want},
		{"t9",args{t9},t9want},
		{"t10",args{t10},t10want},
		{"t11",args{t11},t11want},
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