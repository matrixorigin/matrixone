package tree

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
	"github.com/pingcap/parser/types"
	"go/constant"
	"matrixone/pkg/defines"
	"strconv"
)

//transform test_driver.ValueExpr::Datum to tree.NumVal
//decimal -> ?
//null -> unknown
func transformDatumToNumVal(datum *test_driver.Datum) *NumVal {
	switch datum.Kind() {
	case test_driver.KindNull: //go Unknown Value expresses the null value.
		return NewNumVal(constant.MakeUnknown(), "NULL", false)
	case test_driver.KindInt64: //include mysql true,false
		orgStr := strconv.FormatInt(datum.GetInt64(), 10)
		return NewNumVal(constant.MakeInt64(datum.GetInt64()), orgStr, false)
	case test_driver.KindUint64:
		orgStr := strconv.FormatUint(datum.GetUint64(), 10)
		return NewNumVal(constant.MakeUint64(datum.GetUint64()), orgStr, false)
	case test_driver.KindFloat32:
		orgStr := strconv.FormatFloat(datum.GetFloat64(), 'f', -1, 64)
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()), orgStr, false)
	case test_driver.KindFloat64: //mysql 1.2E3, 1.2E-3, -1.2E3, -1.2E-3;
		orgStr := strconv.FormatFloat(datum.GetFloat64(), 'f', -1, 64)
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()), orgStr, false)
	case test_driver.KindString:
		return NewNumVal(constant.MakeString(datum.GetString()), datum.GetString(), false)
	case test_driver.KindMysqlDecimal: //mysql .2, 3.4, -6.78, +9.10
		deci := datum.GetMysqlDecimal().ToString()
		f64, err := strconv.ParseFloat(string(deci), 64)
		if err != nil {
			panic(fmt.Errorf("convert decimal string to float64 failed. %v\n", err))
		}
		return NewNumVal(constant.MakeFloat64(f64), string(deci), false)
	case test_driver.KindBytes:
		fallthrough
	case test_driver.KindBinaryLiteral:
		fallthrough
	case test_driver.KindMysqlDuration:
		fallthrough
	case test_driver.KindMysqlEnum:
		fallthrough
	case test_driver.KindMysqlBit:
		fallthrough
	case test_driver.KindMysqlSet:
		fallthrough
	case test_driver.KindMysqlTime:
		fallthrough
	case test_driver.KindInterface:
		fallthrough
	case test_driver.KindMinNotNull:
		fallthrough
	case test_driver.KindMaxValue:
		fallthrough
	case test_driver.KindRaw:
		fallthrough
	case test_driver.KindMysqlJSON:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported datum type %v\n", datum.Kind()))
	}
}

//transform ast.UnaryOperationExpr to tree.UnaryExpr
func transformUnaryOperatorExprToUnaryExpr(uoe *ast.UnaryOperationExpr) *UnaryExpr {
	switch uoe.Op {
	case opcode.Minus:
		e := transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_MINUS, e)
	case opcode.Plus:
		e := transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_PLUS, e)
	case opcode.BitNeg: //~
		e := transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_TILDE, e)
	case opcode.Not2: //!
		e := transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_MARK, e)

	}
	panic(fmt.Errorf("unsupported unary expr. op:%s \n", uoe.Op.String()))
	return nil
}

//transform ast.UnaryOperationExpr to tree.NotExpr
func transformUnaryOperatorExprToNotExpr(uoe *ast.UnaryOperationExpr) *NotExpr {
	switch uoe.Op {
	case opcode.Not: //not,!
		e := transformExprNodeToExpr(uoe.V)
		return NewNotExpr(e)
	}
	panic(fmt.Errorf("unsupported not expr. op:%s \n", uoe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.BinaryExpr
func transformBinaryOperationExprToBinaryExpr(boe *ast.BinaryOperationExpr) *BinaryExpr {
	switch boe.Op {
	//math operation
	case opcode.Plus:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(PLUS, l, r)
	case opcode.Minus:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MINUS, l, r)
	case opcode.Mul:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MULTI, l, r)
	case opcode.Div: // /
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(DIV, l, r)
	case opcode.Mod: //%
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MOD, l, r)
	case opcode.IntDiv: // integer division
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(INTEGER_DIV, l, r)
	//bit wise operation
	case opcode.Or: //bit or |
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_OR, l, r)
	case opcode.And: //bit and &
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_AND, l, r)
	case opcode.Xor: //bit xor ^
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_XOR, l, r)
	case opcode.LeftShift: //<<
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(LEFT_SHIFT, l, r)
	case opcode.RightShift: //>>
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(RIGHT_SHIFT, l, r)
		//logic operation
	}
	panic(fmt.Errorf("unsupported binary expr. op:%s \n", boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.ComparisonExpr
func transformBinaryOperationExprToComparisonExpr(boe *ast.BinaryOperationExpr) *ComparisonExpr {
	switch boe.Op {
	//comparison operation
	case opcode.EQ: // =
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(EQUAL, l, r)
	case opcode.LT: // <
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(LESS_THAN, l, r)
	case opcode.LE: // <=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(LESS_THAN_EQUAL, l, r)
	case opcode.GT: // >
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(GREAT_THAN, l, r)
	case opcode.GE: // >=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(GREAT_THAN_EQUAL, l, r)
	case opcode.NE: // <>,!=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(NOT_EQUAL, l, r)
	}
	panic(fmt.Errorf("unsupported comparison expr. op:%s \n", boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.AndExpr
func transformBinaryOperationExprToAndExpr(boe *ast.BinaryOperationExpr) *AndExpr {
	switch boe.Op {
	//logic operation
	case opcode.LogicAnd: // and,&&
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewAndExpr(l, r)
	}
	panic(fmt.Errorf("unsupported and expr. op:%s \n", boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.OrExpr
func transformBinaryOperationExprToOrExpr(boe *ast.BinaryOperationExpr) *OrExpr {
	switch boe.Op {
	//logic operation
	case opcode.LogicOr: // or,||
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewOrExpr(l, r)
	}
	panic(fmt.Errorf("unsupported or expr. op:%s \n", boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.XorExpr
func transformBinaryOperationExprToXorExpr(boe *ast.BinaryOperationExpr) *XorExpr {
	switch boe.Op {
	//logic operation
	case opcode.LogicXor: // xor
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewXorExpr(l, r)
	}
	panic(fmt.Errorf("unsupported xor expr. op:%s \n", boe.Op.String()))
	return nil
}

//transform ast.IsNullExpr to tree.IsNullExpr
func transformIsNullExprToIsNullExpr(ine *ast.IsNullExpr) *IsNullExpr {
	if !ine.Not {
		e := transformExprNodeToExpr(ine.Expr)
		return NewIsNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is null expr. %v \n", ine))
	return nil
}

//transform ast.IsNotNullExpr to tree.IsNotNullExpr
func transformIsNullExprToIsNotNullExpr(ine *ast.IsNullExpr) *IsNotNullExpr {
	if ine.Not {
		e := transformExprNodeToExpr(ine.Expr)
		return NewIsNotNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is not null expr. %v \n", ine))
	return nil
}

//transform ast.PatternInExpr (in expression) to tree.ComparisonExpr.In
func transformPatternInExprToComparisonExprIn(pie *ast.PatternInExpr) *ComparisonExpr {
	e1 := transformExprNodeToExpr(pie.Expr)
	var e2 Expr
	var op ComparisonOp
	if len(pie.List) != 0 {
		// => ExprList
		l := &ExprList{
			Exprs: make([]Expr, len(pie.List)),
		}
		for i, x := range pie.List {
			l.Exprs[i] = transformExprNodeToExpr(x)
		}
		e2 = l
	} else if pie.Sel != nil {
		e2 = transformExprNodeToExpr(pie.Sel)
	}

	if pie.Not {
		op = NOT_IN
	} else {
		op = IN
	}

	return NewComparisonExpr(op, e1, e2)
}

//transform ast.PatternLikeExpr (in expression) to tree.ComparisonExpr.LIKE
func transformPatternLikeExprToComparisonExprIn(ple *ast.PatternLikeExpr) *ComparisonExpr {
	//may have Expr
	var e1 Expr = nil
	if ple.Expr != nil {
		e1 = transformExprNodeToExpr(ple.Expr)
	}

	//Must have Pattern
	e2 := transformExprNodeToExpr(ple.Pattern)
	//TODO:escape

	var op ComparisonOp

	if ple.Not {
		op = NOT_LIKE
	} else {
		op = LIKE
	}

	return NewComparisonExpr(op, e1, e2)
}

//transform ast.PatternRegexpExpr (in expression) to tree.ComparisonExpr.REG_MATCH
func transformPatternRegexpExprToComparisonExprIn(pre *ast.PatternRegexpExpr) *ComparisonExpr {
	e1 := transformExprNodeToExpr(pre.Expr)
	e2 := transformExprNodeToExpr(pre.Pattern)

	var op ComparisonOp

	if pre.Not {
		op = NOT_REG_MATCH
	} else {
		op = REG_MATCH
	}

	return NewComparisonExpr(op, e1, e2)
}

//transform ast.ResultSetNode to tree.SelectStatement
func transformResultSetNodeToSelectStatement(rsn ast.ResultSetNode) SelectStatement {
	switch n := rsn.(type) {
	case *ast.SubqueryExpr:
		return transformSubqueryExprToSubquery(n)
	case *ast.SelectStmt:
		return transformSelectStmtToSelectStatement(n)
	case *ast.SetOprStmt:
		return transformSetOprStmtToSelectStatement(n)
	}
	panic(fmt.Errorf("unsupported resultSetNode\n"))
	return nil
}

//transform ast.SubqueryExpr to tree.Subquery
func transformSubqueryExprToSubquery(se *ast.SubqueryExpr) *Subquery {
	e := transformResultSetNodeToSelectStatement(se.Query)
	return NewSubquery(e, se.Exists)
}

//transform ast.ExistsSubqueryExpr to tree.Subquery
func transformExistsSubqueryExprToSubquery(ese *ast.ExistsSubqueryExpr) *Subquery {
	e := transformExprNodeToExpr(ese.Sel)
	return NewSubquery(e, ese.Not)
}

//transform ast.CompareSubqueryExpr to tree.ComparisonExpr.SubOp
func transformCompareSubqueryExprToSubquery(cse *ast.CompareSubqueryExpr) *ComparisonExpr {
	l := transformExprNodeToExpr(cse.L)
	r := transformExprNodeToExpr(cse.R)
	var subop ComparisonOp

	if cse.All {
		subop = ALL
	} else {
		subop = ANY
	}

	switch cse.Op {
	//comparison operation
	case opcode.EQ: // =
		return NewComparisonExprWithSubop(EQUAL, subop, l, r)
	case opcode.LT: // <
		return NewComparisonExprWithSubop(LESS_THAN, subop, l, r)
	case opcode.LE: // <=
		return NewComparisonExprWithSubop(LESS_THAN_EQUAL, subop, l, r)
	case opcode.GT: // >
		return NewComparisonExprWithSubop(GREAT_THAN, subop, l, r)
	case opcode.GE: // >=
		return NewComparisonExprWithSubop(GREAT_THAN_EQUAL, subop, l, r)
	case opcode.NE: // <>,!=
		return NewComparisonExprWithSubop(NOT_EQUAL, subop, l, r)
	}
	panic(fmt.Errorf("unsupported CompareSubqueryExpr expr. op:%s \n", cse.Op.String()))
	return nil
}

//transform ast.ParenthesesExpr to tree.ParenExpr
func transformParenthesesExprToParenExpr(pe *ast.ParenthesesExpr) *ParenExpr {
	e := transformExprNodeToExpr(pe.Expr)
	return NewParenExpr(e)
}

//transform ast.TableName to tree.TableName
func transformTableNameToTableName(tn *ast.TableName) *TableName {
	return NewTableName(Identifier(tn.Name.O), ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      Identifier(tn.Schema.O),
		ExplicitCatalog: false,
		ExplicitSchema:  len(tn.Schema.O) != 0,
	})
}

//transform ast.TableName to tree.UnresolvedObjectName
func transformTableNameToUnresolvedObjectName(tn *ast.TableName)*UnresolvedObjectName{
	u,err := NewUnresolvedObjectName(2,[3]string{tn.Schema.O,tn.Name.O})
	if err != nil {
		panic(fmt.Errorf("TableName to UnresolvedObjectName failed. error:%v\n",err))
	}
	return u
}

//transform ast.TableSource to tree.AliasedTableExpr
func transformTableSourceToAliasedTableExpr(ts *ast.TableSource) *AliasedTableExpr {
	te := transformResultSetNodeToTableExpr(ts.Source)
	return NewAliasedTableExpr(te, AliasClause{
		Alias: Identifier(ts.AsName.O),
	})
}

//transform ast.SelectStmt to tree.StatementSource
func transformSelectStmtToStatementSource(ss *ast.SelectStmt) *StatementSource {
	sts := transformSelectStmtToSelectStatement(ss)
	return NewStatementSource(sts)
}

//transform ast.SelectStmt to tree.Subquery
func transformSelectStmtToSubquery(ss *ast.SelectStmt) *Subquery {
	sts := transformSelectStmtToSelectStatement(ss)
	return NewSubquery(sts, false)
}

//transform ast.ResultSetNode to tree.TableExpr
func transformResultSetNodeToTableExpr(rsn ast.ResultSetNode) TableExpr {
	switch n := rsn.(type) {
	case *ast.SubqueryExpr:
		return transformSubqueryExprToSubquery(n)
	case *ast.Join:
		return transformJoinToTableExpr(n)
	case *ast.TableName:
		return transformTableNameToTableName(n)
	case *ast.TableSource:
		return transformTableSourceToAliasedTableExpr(n)
	case *ast.SelectStmt:
		return transformSelectStmtToSubquery(n)
	case *ast.SetOprStmt:
		return transformSetOprStmtToSelectStatement(n)
	}
	panic(fmt.Errorf("unsupported ResultSetNode type:%v \n", rsn))
	return nil
}

//transform []*ast.ColumnName to tree.IdentifierList
func transformColumnNameListToNameList(cn []*ast.ColumnName) IdentifierList {
	var l IdentifierList
	for _, x := range cn {
		l = append(l, Identifier(x.Name.O))
	}
	return l
}

//transform []*ast.ColumnName to []*tree.UnresolvedName
func transformColumnNameListToUnresolvedNameList(cn []*ast.ColumnName) []*UnresolvedName {
	var l []*UnresolvedName
	for _, x := range cn {
		un := transformColumnNameToUnresolvedName(x)
		l = append(l, un)
	}
	return l
}

//transform []model.CIStr to tree.IdentifirList
func transformCIStrToIdentifierList(ci []model.CIStr) IdentifierList {
	var l IdentifierList
	for _, x := range ci {
		l = append(l, Identifier(x.O))
	}
	return l
}

/*
transform ast.Join to tree.JoinTableExpr
This is core of transformation from ast.TableRefsClause to tree.From

FROM: https://dev.mysql.com/doc/refman/8.0/en/join.html
In MySQL, JOIN, CROSS JOIN, and INNER JOIN are syntactic equivalents (they can replace each other).
In standard SQL, they are not equivalent. INNER JOIN is used with an ON clause, CROSS JOIN is used otherwise.

INNER JOIN is used with the ON condition.
NATURAL JOIN has implicit ON condition -  columns with same names in both tables.
STRAIGHT JOIN defines the order among the tables from the left to the right.
*/
func transformJoinToJoinTableExpr(j *ast.Join) *JoinTableExpr {
	var t string
	var joinCon JoinCond

	switch j.Tp {
	case ast.CrossJoin:

		t = JOIN_TYPE_CROSS
	case ast.LeftJoin:
		t = JOIN_TYPE_LEFT
	case ast.RightJoin:
		t = JOIN_TYPE_RIGHT
	}

	if j.NaturalJoin {
		joinCon = NewNaturalJoinCond()
	} else if j.StraightJoin {
		//TODO:
	}

	if j.ExplicitParens {
		//TODO:
	}

	l := transformResultSetNodeToTableExpr(j.Left)

	if j.Right == nil {
		return NewJoinTableExpr(t, l, nil, joinCon)
	}
	r := transformResultSetNodeToTableExpr(j.Right)

	if j.On != nil {
		onE := transformExprNodeToExpr(j.On.Expr)
		joinCon = NewOnJoinCond(onE)
	} else if j.Using != nil {
		iList := transformColumnNameListToNameList(j.Using)
		joinCon = NewUsingJoinCond(iList)
	}

	return NewJoinTableExpr(t, l, r, joinCon)
}

//transform ast.Join to tree.ParenTableExpr
func transformJoinToParenTableExpr(j *ast.Join) *ParenTableExpr {
	if j.ExplicitParens {
		//j.ExplicitParens = false
		jt := transformJoinToJoinTableExpr(j)
		return NewParenTableExpr(jt)
	}
	panic(fmt.Errorf("Need ExplicitParens :%v \n", j))
	return nil
}

//transform ast.Join to tree.TableExpr
func transformJoinToTableExpr(j *ast.Join) TableExpr {
	jt := transformJoinToJoinTableExpr(j)
	if j.ExplicitParens {
		return NewParenTableExpr(jt)
	}
	return jt
}

//transform ast.TableRefsClause to tree.From
func transformTableRefsClauseToFrom(trc *ast.TableRefsClause) *From {
	var te []TableExpr = make([]TableExpr, 1)
	t := transformJoinToTableExpr(trc.TableRefs)
	te[0] = t
	return NewFrom(te)
}

//transform ast.ColumnNameExpr to tree.UnresolvedName
func transformColumnNameExprToUnresolvedName(cne *ast.ColumnNameExpr) *UnresolvedName {
	if cne.Name == nil {
		panic(fmt.Errorf("need column name\n"))
	}
	cn := cne.Name
	ud, _ := NewUnresolvedName(cn.Schema.O, cn.Table.O, cn.Name.O)
	return ud
}

//transform ast.ColumnName to tree.UnresolvedName
func transformColumnNameToUnresolvedName(cn *ast.ColumnName) *UnresolvedName {
	ud, _ := NewUnresolvedName(cn.Schema.O, cn.Table.O, cn.Name.O)
	return ud
}

//transform ast.FuncCallExpr to tree.FuncExpr
func transformFuncCallExprToFuncExpr(fce *ast.FuncCallExpr) *FuncExpr {
	fname, _ := NewUnresolvedName(fce.Schema.O, fce.FnName.O)
	var es Exprs = make([]Expr, len(fce.Args))
	for i, arg := range fce.Args {
		e := transformExprNodeToExpr(arg)
		es[i] = e
	}

	return NewFuncExpr(0, fname, es, nil)
}

//transform ast.AggregateFuncExpr to tree.FuncExpr
func transformAggregateFuncExprToFuncExpr(afe *ast.AggregateFuncExpr) *FuncExpr {
	fname, _ := NewUnresolvedName(afe.F)
	var es Exprs = make([]Expr, len(afe.Args))
	for i, arg := range afe.Args {
		e := transformExprNodeToExpr(arg)
		es[i] = e
	}

	var ft funcType = 0
	if afe.Distinct {
		ft = FUNC_TYPE_DISTINCT
	} else {
		ft = FUNC_TYPE_ALL
	}

	var ob OrderBy
	if afe.Order != nil {
		ob = transformOrderByClauseToOrderBy(afe.Order)
	}
	return NewFuncExpr(ft, fname, es, ob)
}

//transform types.FieldType to ResolvableTypeReference
func transformFieldTypeToResolvableTypeReference(ft *types.FieldType) ResolvableTypeReference {
	var t *T
	var timeType bool = false
	switch ft.Tp {
	case mysql.TypeUnspecified:
		panic(fmt.Errorf("unsupported type\n"))
	case mysql.TypeTiny:
		t = TYPE_TINY
	case mysql.TypeShort:
		t = TYPE_SHORT
	case mysql.TypeLong:
		t = TYPE_LONG
	case mysql.TypeFloat:
		t = TYPE_FLOAT
	case mysql.TypeDouble:
		t = TYPE_DOUBLE
	case mysql.TypeNull:
		t = TYPE_NULL
	case mysql.TypeTimestamp:
		t = TYPE_TIMESTAMP
		timeType = true
	case mysql.TypeLonglong:
		t = TYPE_LONGLONG
	case mysql.TypeInt24:
		t = TYPE_INT24
	case mysql.TypeDate:
		t = TYPE_DATE
		timeType = true
	case mysql.TypeDuration:
		t = TYPE_DURATION
		timeType = true
	case mysql.TypeDatetime:
		t = TYPE_DATETIME
		timeType = true
	case mysql.TypeYear:
		t = TYPE_YEAR
		timeType = true
	case mysql.TypeNewDate:
		t = TYPE_NEWDATE
		timeType = true
	case mysql.TypeVarchar:
		t = TYPE_VARCHAR
	case mysql.TypeBit:
		t = TYPE_BIT
	case mysql.TypeJSON:
		t = TYPE_JSON
	case mysql.TypeNewDecimal:
		t = TYPE_NEWDATE
		timeType = true
	case mysql.TypeEnum:
		t = TYPE_ENUM
	case mysql.TypeSet:
		t = TYPE_SET
	case mysql.TypeTinyBlob:
		t = TYPE_TINY_BLOB
	case mysql.TypeMediumBlob:
		t = TYPE_MEDIUM_BLOB
	case mysql.TypeLongBlob:
		t = TYPE_LONG_BLOB
	case mysql.TypeBlob:
		t = TYPE_BLOB
	case mysql.TypeVarString:
		t = TYPE_VARSTRING
	case mysql.TypeString:
		t = TYPE_STRING
	case mysql.TypeGeometry:
		t = TYPE_GEOMETRY
	default:
		panic("unsupported cast type")
	}

	tt := &T{InternalType: t.InternalType}

	displayWith := ft.Flen
	if displayWith == -1 {
		displayWith = 0
	}

	tt.InternalType.DisplayWith = int32(displayWith)

	precision := ft.Decimal
	if precision == -1 {
		precision = 0
		if timeType {
			tt.InternalType.TimePrecisionIsSet = false
		}
	} else if precision == 0 {
		if timeType {
			tt.InternalType.TimePrecisionIsSet = true
		}
	}

	tt.InternalType.Precision = int32(precision)

	//unsigned
	tt.InternalType.Unsigned = (uint32(ft.Flag) & defines.UNSIGNED_FLAG) > 0

	//binary
	tt.InternalType.Binary = (uint32(ft.Flag) & defines.BINARY_FLAG) > 0

	return tt
}

//transform ast.FuncCastExpr to tree.CastExpr
func transformFuncCastExprToCastExpr(fce *ast.FuncCastExpr) *CastExpr {
	e := transformExprNodeToExpr(fce.Expr)
	var t ResolvableTypeReference = transformFieldTypeToResolvableTypeReference(fce.Tp)
	return NewCastExpr(e, t)
}

//transform ast.RowExpr to tree.Tuple
func transformRowExprToTuple(re *ast.RowExpr) *Tuple {
	var ar []Expr = make([]Expr, len(re.Values))
	for i, en := range re.Values {
		ar[i] = transformExprNodeToExpr(en)
	}
	return NewTuple(ar)
}

//transform ast.BetweenExpr to tree.RangeCond
func transformBetweenExprToRangeCond(be *ast.BetweenExpr) *RangeCond {
	e := transformExprNodeToExpr(be.Expr)
	l := transformExprNodeToExpr(be.Left)
	r := transformExprNodeToExpr(be.Right)
	return NewRangeCond(be.Not, e, l, r)
}

//transform ast.WhenClause to tree.When
func transformWhenClauseToWhen(wc *ast.WhenClause) *When {
	c := transformExprNodeToExpr(wc.Expr)
	v := transformExprNodeToExpr(wc.Result)
	return NewWhen(c, v)
}

//transform ast.CaseExpr to tree.CaseExpr
func transformCaseExprToCaseExpr(ce *ast.CaseExpr) *CaseExpr {
	var e Expr = nil
	var el Expr = nil
	if ce.Value != nil {
		e = transformExprNodeToExpr(ce.Value)
	}
	var whens []*When = make([]*When, len(ce.WhenClauses))
	for i, w := range ce.WhenClauses {
		whens[i] = transformWhenClauseToWhen(w)
	}
	if ce.ElseClause != nil {
		el = transformExprNodeToExpr(ce.ElseClause)
	}
	return NewCaseExpr(e, whens, el)
}

//transform ast.TimeUnitExpr to tree.IntervalExpr
func transformTimeUnitExprToIntervalExpr(tue *ast.TimeUnitExpr) *IntervalExpr {
	switch tue.Unit {
	case ast.TimeUnitInvalid:
		return NewIntervalExpr(INTERVAL_TYPE_INVALID)
	case ast.TimeUnitMicrosecond:
		return NewIntervalExpr(INTERVAL_TYPE_MICROSECOND)
	case ast.TimeUnitSecond:
		return NewIntervalExpr(INTERVAL_TYPE_SECOND)
	case ast.TimeUnitMinute:
		return NewIntervalExpr(INTERVAL_TYPE_MINUTE)
	case ast.TimeUnitHour:
		return NewIntervalExpr(INTERVAL_TYPE_HOUR)
	case ast.TimeUnitDay:
		return NewIntervalExpr(INTERVAL_TYPE_DAY)
	case ast.TimeUnitWeek:
		return NewIntervalExpr(INTERVAL_TYPE_WEEK)
	case ast.TimeUnitMonth:
		return NewIntervalExpr(INTERVAL_TYPE_MONTH)
	case ast.TimeUnitQuarter:
		return NewIntervalExpr(INTERVAL_TYPE_QUARTER)
	case ast.TimeUnitYear:
		return NewIntervalExpr(INTERVAL_TYPE_YEAR)
	case ast.TimeUnitSecondMicrosecond:
		return NewIntervalExpr(INTERVAL_TYPE_SECOND_MICROSECOND)
	case ast.TimeUnitMinuteMicrosecond:
		return NewIntervalExpr(INTERVAL_TYPE_MINUTE_MICROSECOND)
	case ast.TimeUnitMinuteSecond:
		return NewIntervalExpr(INTERVAL_TYPE_MINUTE_SECOND)
	case ast.TimeUnitHourMicrosecond:
		return NewIntervalExpr(INTERVAL_TYPE_HOUR_MICROSECOND)
	case ast.TimeUnitHourSecond:
		return NewIntervalExpr(INTERVAL_TYPE_HOUR_SECOND)
	case ast.TimeUnitHourMinute:
		return NewIntervalExpr(INTERVAL_TYPE_HOUR_MINUTE)
	case ast.TimeUnitDayMicrosecond:
		return NewIntervalExpr(INTERVAL_TYPE_DAY_MICROSECOND)
	case ast.TimeUnitDaySecond:
		return NewIntervalExpr(INTERVAL_TYPE_DAY_SECOND)
	case ast.TimeUnitDayMinute:
		return NewIntervalExpr(INTERVAL_TYPE_DAYMINUTE)
	case ast.TimeUnitDayHour:
		return NewIntervalExpr(INTERVAL_TYPE_DAYHOUR)
	case ast.TimeUnitYearMonth:
		return NewIntervalExpr(INTERVAL_TYPE_YEARMONTH)
	}
	panic(fmt.Errorf("unsupported time unit type %v \n", tue.Unit))
	return nil
}

//transform ast.IsTruthExpr to tree.ComparisonExpr
func transformIsTruthExprToComparisonExpr(ite *ast.IsTruthExpr) *ComparisonExpr {
	e := transformExprNodeToExpr(ite.Expr)
	var op ComparisonOp
	if ite.Not {
		op = IS_DISTINCT_FROM
	} else {
		op = IS_NOT_DISTINCT_FROM
	}
	var r *NumVal
	if ite.True == 1 {
		r = NewNumVal(constant.MakeInt64(1), "1", false)
	} else {
		r = NewNumVal(constant.MakeInt64(0), "0", false)
	}

	return NewComparisonExpr(op, e, r)
}

//transform ast.DefaultExpr to tree.DefaultVal
func transformDefaultExprToDefaultVal(expr *ast.DefaultExpr) *DefaultVal {
	return NewDefaultVal()
}

//transform ast.MaxValueExpr to tree.MaxValue
func transformMaxValueExprToMaxValue(expr *ast.MaxValueExpr) *MaxValue {
	return NewMaxValue()
}

//transform ast.VariableExpr to tree.VarExpr
func transformVariableExprToVarExpr(ve *ast.VariableExpr) *VarExpr {
	var e Expr = nil
	if ve.Value != nil {
		e = transformExprNodeToExpr(ve.Value)
	}

	return NewVarExpr(ve.Name,ve.IsSystem,ve.IsGlobal,e)
}

//transform ast.ExprNode to tree.Expr
func transformExprNodeToExpr(node ast.ExprNode) Expr {
	switch n := node.(type) {
	case ast.ValueExpr:
		if ve, ok := n.(*test_driver.ValueExpr); !ok {
			panic("convert to test_driver.ValueExpr failed.")
		} else {
			return transformDatumToNumVal(&ve.Datum)
		}
	case *ast.BinaryOperationExpr:
		switch n.Op {
		case opcode.Plus,
			opcode.Minus,
			opcode.Mul,
			opcode.Div,
			opcode.Mod,
			opcode.IntDiv,
			opcode.Or,
			opcode.And,
			opcode.Xor,
			opcode.LeftShift,
			opcode.RightShift:
			return transformBinaryOperationExprToBinaryExpr(n)
		case opcode.EQ,
			opcode.LT,
			opcode.LE,
			opcode.GT,
			opcode.GE,
			opcode.NE:
			return transformBinaryOperationExprToComparisonExpr(n)
		case opcode.LogicAnd:
			return transformBinaryOperationExprToAndExpr(n)
		case opcode.LogicOr:
			return transformBinaryOperationExprToOrExpr(n)
		case opcode.LogicXor:
			return transformBinaryOperationExprToXorExpr(n)
		}

	case *ast.UnaryOperationExpr:
		switch n.Op {
		case opcode.Not:
			return transformUnaryOperatorExprToNotExpr(n)
		}
		return transformUnaryOperatorExprToUnaryExpr(n)
	case *ast.IsNullExpr:
		if n.Not {
			return transformIsNullExprToIsNotNullExpr(n)
		} else {
			return transformIsNullExprToIsNullExpr(n)
		}
	case *ast.PatternInExpr:
		return transformPatternInExprToComparisonExprIn(n)
	case *ast.PatternLikeExpr:
		return transformPatternLikeExprToComparisonExprIn(n)
	case *ast.PatternRegexpExpr:
		return transformPatternRegexpExprToComparisonExprIn(n)
	case *ast.SubqueryExpr:
		return transformSubqueryExprToSubquery(n)
	case *ast.ExistsSubqueryExpr:
		return transformExistsSubqueryExprToSubquery(n)
	case *ast.CompareSubqueryExpr:
		return transformCompareSubqueryExprToSubquery(n)
	case *ast.ParenthesesExpr:
		return transformParenthesesExprToParenExpr(n)
	case *ast.ColumnNameExpr:
		return transformColumnNameExprToUnresolvedName(n)
	case *ast.FuncCallExpr:
		return transformFuncCallExprToFuncExpr(n)
	case *ast.AggregateFuncExpr:
		return transformAggregateFuncExprToFuncExpr(n)
	case *ast.FuncCastExpr:
		return transformFuncCastExprToCastExpr(n)
	case *ast.RowExpr:
		return transformRowExprToTuple(n)
	case *ast.BetweenExpr:
		return transformBetweenExprToRangeCond(n)
	case *ast.CaseExpr:
		return transformCaseExprToCaseExpr(n)
	case *ast.TimeUnitExpr:
		return transformTimeUnitExprToIntervalExpr(n)
	case *ast.IsTruthExpr:
		return transformIsTruthExprToComparisonExpr(n)
	case *ast.DefaultExpr:
		return transformDefaultExprToDefaultVal(n)
	case *ast.MaxValueExpr:
		return transformMaxValueExprToMaxValue(n)
	case *ast.VariableExpr:
		return transformVariableExprToVarExpr(n)
	}
	panic(fmt.Errorf("unsupported node %v \n", node))
	return nil
}

//transform ast.WildCardField to
func transformWildCardFieldToVarName(wcf *ast.WildCardField) VarName {
	sch := len(wcf.Schema.O) != 0
	tbl := len(wcf.Table.O) != 0
	if sch && tbl {
		//UnresolvedName
		u, _ := NewUnresolvedNameWithStar(wcf.Schema.O, wcf.Table.O)
		return u
	} else if tbl {
		//UnresolvedName
		u, _ := NewUnresolvedNameWithStar(wcf.Table.O)
		return u
	} else {
		//*
		return StarExpr()
	}
}

//transform ast.FieldList to tree.SelectExprs
func transformFieldListToSelectExprs(fl *ast.FieldList) SelectExprs {
	var sea []SelectExpr = make([]SelectExpr, len(fl.Fields))
	for i, se := range fl.Fields {
		var e Expr
		if se.Expr != nil {
			e = transformExprNodeToExpr(se.Expr)
		} else {
			e = transformWildCardFieldToVarName(se.WildCard)
		}

		sea[i].Expr = e
		sea[i].As = UnrestrictedIdentifier(se.AsName.O)
	}
	return sea
}

//transform ast.GroupByClause to tree.GroupBy
func transformGroupByClauseToGroupBy(gbc *ast.GroupByClause) GroupBy {
	var gb []Expr = make([]Expr, len(gbc.Items))
	for i, bi := range gbc.Items {
		gb[i] = transformExprNodeToExpr(bi.Expr)
	}
	return gb
}

//transform ast.ByItem to tree.Order
func transformByItemToOrder(bi *ast.ByItem) *Order {
	e := transformExprNodeToExpr(bi.Expr)
	var a Direction
	if bi.Desc {
		a = Descending
	} else {
		a = Ascending
	}
	return NewOrder(e, a, bi.NullOrder)
}

//transform ast.OrderByClause to tree.OrderBy
func transformOrderByClauseToOrderBy(obc *ast.OrderByClause) OrderBy {
	var ob []*Order = make([]*Order, len(obc.Items))
	for i, obi := range obc.Items {
		ob[i] = transformByItemToOrder(obi)
	}
	return ob
}

//transform ast.HavingClause to tree.Where
func transformHavingClauseToWhere(hc *ast.HavingClause) *Where {
	e := transformExprNodeToExpr(hc.Expr)
	return NewWhere(e)
}

//transform ast.Limit to tree.Limit
func transformLimitToLimit(l *ast.Limit) *Limit {
	var o Expr = nil
	if l.Offset != nil {
		o = transformExprNodeToExpr(l.Offset)
	}
	c := transformExprNodeToExpr(l.Count)
	return NewLimit(o, c)
}

//transform ast.SelectStmt to tree.SelectClause
func transformSelectStmtToSelectClause(ss *ast.SelectStmt) *SelectClause {
	var from *From = nil
	if ss.From != nil {
		from = transformTableRefsClauseToFrom(ss.From)
	}

	var where *Where = nil
	if ss.Where != nil {
		where = NewWhere(transformExprNodeToExpr(ss.Where))
	}

	sea := transformFieldListToSelectExprs(ss.Fields)

	var gb []Expr = nil
	if ss.GroupBy != nil {
		gb = transformGroupByClauseToGroupBy(ss.GroupBy)
	}

	var having *Where = nil
	if ss.Having != nil {
		having = transformHavingClauseToWhere(ss.Having)
	}

	return &SelectClause{
		From:     from,
		Distinct: ss.Distinct,
		Where:    where,
		Exprs:    sea,
		GroupBy:  gb,
		Having:   having,
	}
}

//transform ast.SelectStmt to tree.Select
func transformSelectStmtToSelect(ss *ast.SelectStmt) *Select {
	sc := transformSelectStmtToSelectClause(ss)

	var ob []*Order = nil
	if ss.OrderBy != nil {
		ob = transformOrderByClauseToOrderBy(ss.OrderBy)
	}

	var lmt *Limit
	if ss.Limit != nil {
		lmt = transformLimitToLimit(ss.Limit)
	}

	return &Select{
		Select:  sc,
		OrderBy: ob,
		Limit:   lmt,
	}
}

//transform ast.SelectStmt(IsInBraces is true) to tree.ParenSelect
func transformSelectStmtToParenSelect(ss *ast.SelectStmt) *ParenSelect {
	if !ss.IsInBraces {
		panic(fmt.Errorf("only in brace\n"))
	}
	ss.IsInBraces = false
	s := transformSelectStmtToSelect(ss)
	return &ParenSelect{
		Select: s,
	}
}

//transform ast.SelectStmt to tree.SelectStatement
func transformSelectStmtToSelectStatement(ss *ast.SelectStmt) SelectStatement {
	//if ss.IsInBraces {
	//	return transformSelectStmtToParenSelect(ss)
	//} else {
	return transformSelectStmtToSelectClause(ss)
	//}
}

//transform ast.Node to tree.(UnionType,bool)
func transformSetOprTypeToUnionType(n ast.Node) (UnionType, bool) {
	var oprType ast.SetOprType
	switch sel := n.(type) {
	case *ast.SelectStmt:
		if sel.AfterSetOperator == nil {
			panic(fmt.Errorf("need set operator\n"))
		}
		oprType = *sel.AfterSetOperator
	case *ast.SetOprSelectList:
		if sel.AfterSetOperator == nil {
			panic(fmt.Errorf("need set operator\n"))
		}
		oprType = *sel.AfterSetOperator
	default:
		panic(fmt.Errorf("unsupported single node %v\n", n))
	}
	var all bool
	var t UnionType
	switch oprType {
	case ast.Union:
		t = UNION
		all = false
	case ast.UnionAll:
		t = UNION
		all = true
	case ast.Except:
		t = EXCEPT
		all = false
	case ast.ExceptAll:
		t = EXCEPT
		all = true
	case ast.Intersect:
		t = INTERSECT
		all = false
	case ast.IntersectAll:
		t = INTERSECT
		all = true
	}
	return t, all
}

//transform ast.Node to tree.SelectStatement
func transformSingleNodeToSelectStatement(n ast.Node) SelectStatement {
	switch sel := n.(type) {
	case *ast.SelectStmt:
		return transformSelectStmtToSelect(sel)
	case *ast.SetOprSelectList:
		return transformSetOprSelectListToSelectStatement(sel)
	default:
		panic(fmt.Errorf("unsupported single node %v\n", n))
	}
}

/*
transform []ast.Node to tree.SelectgStatement
Set operations: UNION,INTERSECT,EXCEPT
Precedence:
INTERSECT > UNION
INTERSECT > EXCEPT
UNION = EXCEPT

Left Associativity: UNION,INTERSECT,EXCEPT
*/
func transformSelectArrayToSelectStatement(selects []ast.Node) SelectStatement {
	if len(selects) == 0 {
		panic(fmt.Errorf("need Selects\n"))
	} else if len(selects) == 1 {
		return transformSingleNodeToSelectStatement(selects[0])
	} else if len(selects) == 2 {
		//just two
		l := transformSingleNodeToSelectStatement(selects[0])
		r := transformSingleNodeToSelectStatement(selects[1])
		t, all := transformSetOprTypeToUnionType(selects[1])
		uc := NewUnionClause(t, l, r, all)
		return uc
	}

	//find the last EXCEPT or UNION
	var i int
	for i = len(selects) - 1; i > 0; i-- { //exclude the first one
		var find bool = false
		switch sel := selects[i].(type) {
		case *ast.SelectStmt:
			if sel.AfterSetOperator != nil && (*sel.AfterSetOperator != ast.Intersect && *sel.AfterSetOperator != ast.IntersectAll) {
				find = true
				break
			}
		case *ast.SetOprSelectList:
			if sel.AfterSetOperator != nil && (*sel.AfterSetOperator != ast.Intersect && *sel.AfterSetOperator != ast.IntersectAll) {
				find = true
				break
			}
		default:
			panic(fmt.Errorf("unsupported union statement %v\n", selects[i]))
		}
		if find {
			break
		}
	}

	if i > 0 { //Got the last EXCEPT or UNION
		//split the list into two parts
		//recursively transform them

		//left part
		l := transformSelectArrayToSelectStatement(selects[:i])

		//right part
		r := transformSelectArrayToSelectStatement(selects[i:])

		//union type
		t, all := transformSetOprTypeToUnionType(selects[i])
		uc := NewUnionClause(t, l, r, all)
		return uc
	} else {
		//Got single / multiple INTERSECT
		var left SelectStatement
		for j, n := range selects {
			stmt := transformSingleNodeToSelectStatement(n)
			if j == 0 { //first
				left = stmt
			} else {
				t, all := transformSetOprTypeToUnionType(n)
				left = NewUnionClause(t, left, stmt, all)
			}
		}
		//return NewSelect(uc,nil,nil)
		return left
	}
	panic(fmt.Errorf("missing something\n"))
	return nil
}

//transform ast.SetOprSelectList to tree.SelectStatement
func transformSetOprSelectListToSelectStatement(sosl *ast.SetOprSelectList) SelectStatement {
	return transformSelectArrayToSelectStatement(sosl.Selects)
	panic(fmt.Errorf("missing something\n"))
	return nil
}

//transform ast.SetOprStmt to tree.SelectStatement
func transformSetOprStmtToSelectStatement(sos *ast.SetOprStmt) SelectStatement {
	var ordy OrderBy = nil
	var lm *Limit = nil
	if sos.OrderBy != nil {
		ordy = transformOrderByClauseToOrderBy(sos.OrderBy)
	}
	if sos.Limit != nil {
		lm = transformLimitToLimit(sos.Limit)
	}
	ss := transformSetOprSelectListToSelectStatement(sos.SelectList)
	return NewSelect(ss, ordy, lm)
}

//transform ast.InsertStmt to tree.Insert
func transformInsertStmtToInsert(is *ast.InsertStmt) *Insert {
	var table TableExpr = nil
	if is.Table != nil {
		table = transformJoinToTableExpr(is.Table.TableRefs)
	}
	var colums IdentifierList = nil
	colums = transformColumnNameListToNameList(is.Columns)

	var rows []Exprs = nil
	if is.Lists != nil {
		for _, row := range is.Lists {
			var arr Exprs = nil
			for _, col := range row {
				e := transformExprNodeToExpr(col)
				arr = append(arr, e)
			}
			rows = append(rows, arr)
		}
	} else if is.Select != nil {
		if ss, ok := is.Select.(*ast.SelectStmt); !ok {
			panic(fmt.Errorf("needs selectstmt\n"))
		} else {
			for _, row := range ss.Lists {
				e := transformExprNodeToExpr(row)
				rows = append(rows, []Expr{e})
			}
		}
	} else {
		panic(fmt.Errorf("empty insertstmt\n"))
	}

	partition := transformCIStrToIdentifierList(is.PartitionNames)

	vc := NewValuesClause(rows)
	sel := NewSelect(vc, nil, nil)
	return NewInsert(table, colums, sel, partition)
}

//transform ast.IndexPartSpecification to tree.KeyPart
func transformIndexPartSpecificationToKeyPart(ips *ast.IndexPartSpecification) *KeyPart {
	var cname *UnresolvedName = nil
	if ips.Column != nil {
		cname = transformColumnNameToUnresolvedName(ips.Column)
	}

	var e Expr = nil
	if ips.Expr != nil {
		e = transformExprNodeToExpr(ips.Expr)
	}

	return NewKeyPart(cname, ips.Length, e)
}

//transform []*ast.IndexPartSpecification to []*KeyPart
func transformIndexPartSpecificationArrayToKeyPartArray(ipsArr []*ast.IndexPartSpecification) []*KeyPart {
	var kparts []*KeyPart = make([]*KeyPart, len(ipsArr))
	for i, p := range ipsArr {
		kparts[i] = transformIndexPartSpecificationToKeyPart(p)
	}
	return kparts
}

//transform ast.ReferOptionType to tree.ReferenceOptionType
func transformReferOptionTypeToReferenceOptionType(rot ast.ReferOptionType) ReferenceOptionType {
	switch rot {
	case ast.ReferOptionNoOption:
		return REFERENCE_OPTION_INVALID
	case ast.ReferOptionRestrict:
		return REFERENCE_OPTION_RESTRICT
	case ast.ReferOptionCascade:
		return REFERENCE_OPTION_CASCADE
	case ast.ReferOptionSetNull:
		return REFERENCE_OPTION_SET_NULL
	case ast.ReferOptionNoAction:
		return REFERENCE_OPTION_NO_ACTION
	case ast.ReferOptionSetDefault:
		return REFERENCE_OPTION_SET_DEFAULT
	}
	panic(fmt.Errorf("invalid reference option %v\n", rot))
	return REFERENCE_OPTION_INVALID
}

//transform ast.ReferenceDef to tree.AttributeReference
func transformReferenceDefToAttributeReference(rd *ast.ReferenceDef) *AttributeReference {
	tname := transformTableNameToTableName(rd.Table)
	var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(rd.IndexPartSpecifications)
	var ondelete ReferenceOptionType = REFERENCE_OPTION_INVALID
	if rd.OnDelete != nil {
		ondelete = transformReferOptionTypeToReferenceOptionType(rd.OnDelete.ReferOpt)
	}
	var onupdate ReferenceOptionType = REFERENCE_OPTION_INVALID
	if rd.OnUpdate != nil {
		onupdate = transformReferOptionTypeToReferenceOptionType(rd.OnUpdate.ReferOpt)
	}
	var match MatchType = MATCH_INVALID
	switch rd.Match {
	case ast.MatchNone:
		match = MATCH_INVALID
	case ast.MatchFull:
		match = MATCH_FULL
	case ast.MatchPartial:
		match = MATCH_PARTIAL
	case ast.MatchSimple:
		match = MATCH_SIMPLE
	}
	return NewAttributeReference(tname, kparts, match, ondelete, onupdate)
}

//transform ast.ColumnOption to tree.ColumnAttribute
func transformColumnOptionToColumnAttribute(co *ast.ColumnOption) ColumnAttribute {
	switch co.Tp {
	case ast.ColumnOptionPrimaryKey:
		return NewAttributePrimaryKey()
	case ast.ColumnOptionNotNull:
		return NewAttributeNull(false)
	case ast.ColumnOptionAutoIncrement:
		return NewAttributeAutoIncrement()
	case ast.ColumnOptionDefaultValue:
		e := transformExprNodeToExpr(co.Expr)
		return NewAttributeDefault(e)
	case ast.ColumnOptionUniqKey:
		return NewAttributeUniqueKey()
	case ast.ColumnOptionNull:
		return NewAttributeNull(true)
	case ast.ColumnOptionComment:
		e := transformExprNodeToExpr(co.Expr)
		return NewAttributeComment(e)
	case ast.ColumnOptionGenerated:
		e := transformExprNodeToExpr(co.Expr)
		return NewAttributeGeneratedAlways(e, co.Stored)
	case ast.ColumnOptionReference:
		return transformReferenceDefToAttributeReference(co.Refer)
	case ast.ColumnOptionCollate:
		return NewAttributeCollate(co.StrValue)
	case ast.ColumnOptionCheck:
		e := transformExprNodeToExpr(co.Expr)
		return NewAttributeCheck(e, co.Enforced, co.ConstraintName)
	case ast.ColumnOptionColumnFormat:
		return NewAttributeColumnFormat(co.StrValue)
	case ast.ColumnOptionStorage:
		return NewAttributeStorage(co.StrValue)
	case ast.ColumnOptionAutoRandom:
		return NewAttributeAutoRandom(co.AutoRandomBitLength)
	case ast.ColumnOptionOnUpdate:
		e := transformExprNodeToExpr(co.Expr)
		return NewAttributeOnUpdate(e)
	case ast.ColumnOptionFulltext:
		fallthrough
	case ast.ColumnOptionNoOption:
		fallthrough
	default:
		panic(fmt.Errorf("invalid column option\n"))
	}
	panic(fmt.Errorf("invalid column option\n"))
	return nil
}

//transform ast.ColumnDef to tree.ColumnTableDef
func transformColumnDefToTableDef(cd *ast.ColumnDef) *ColumnTableDef {
	name := transformColumnNameToUnresolvedName(cd.Name)
	t := transformFieldTypeToResolvableTypeReference(cd.Tp)
	var attr_arr []ColumnAttribute = make([]ColumnAttribute, len(cd.Options))
	for i, op := range cd.Options {
		attr_arr[i] = transformColumnOptionToColumnAttribute(op)
	}

	return NewColumnTableDef(name, t, attr_arr)
}

//transform model.IndexType to tree.IndexType
func transformIndexTypeToIndexType(it model.IndexType) IndexType {
	switch it {
	case model.IndexTypeInvalid:
		return INDEX_TYPE_INVALID
	case model.IndexTypeBtree:
		return INDEX_TYPE_BTREE
	case model.IndexTypeHash:
		return INDEX_TYPE_HASH
	case model.IndexTypeRtree:
		return INDEX_TYPE_RTREE
	}
	return INDEX_TYPE_INVALID
}

//transform ast.IndexKeyType to tree.IndexType
func transformIndexKeyTypeToIndexCategory(it ast.IndexKeyType) IndexCategory {
	switch it {
	case ast.IndexKeyTypeNone:
		return INDEX_CATEGORY_NONE
	case ast.IndexKeyTypeUnique:
		return INDEX_CATEGORY_UNIQUE
	case ast.IndexKeyTypeSpatial:
		return INDEX_CATEGORY_SPATIAL
	case ast.IndexKeyTypeFullText:
		return INDEX_CATEGORY_FULLTEXT
	}
	return INDEX_CATEGORY_NONE
}

//transform ast.IndexVisibility to tree.VisibleType
func transformIndexVisibilityToVisibleType(iv ast.IndexVisibility) VisibleType {
	switch iv {
	case ast.IndexVisibilityDefault:
		return VISIBLE_TYPE_INVALID
	case ast.IndexVisibilityVisible:
		return VISIBLE_TYPE_VISIBLE
	case ast.IndexVisibilityInvisible:
		return VISIBLE_TYPE_INVISIBLE
	}
	return VISIBLE_TYPE_INVALID
}

//transform ast.IndexOption to tree.IndexOption
func transformIndexOptionToIndexOption(io *ast.IndexOption) *IndexOption {
	it := transformIndexTypeToIndexType(io.Tp)
	vt := transformIndexVisibilityToVisibleType(io.Visibility)
	return NewIndexOption(io.KeyBlockSize, it, io.ParserName.O, io.Comment, vt, "", "")
}

//transform ast.Constraint to IndexTableDef
func transformConstraintToIndexTableDef(c *ast.Constraint) IndexTableDef {
	switch c.Tp {
	case ast.ConstraintPrimaryKey:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil {
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewPrimaryKeyIndex(kparts, c.Name, c.IsEmptyIndex, io)
	case ast.ConstraintIndex:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil {
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewIndex(kparts, c.Name, c.IsEmptyIndex, io)
	case ast.ConstraintUniq:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil {
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewUniqueIndex(kparts, c.Name, c.IsEmptyIndex, io)
	case ast.ConstraintForeignKey:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var refer *AttributeReference = nil
		if c.Refer != nil {
			refer = transformReferenceDefToAttributeReference(c.Refer)
		}
		return NewForeignKey(c.IfNotExists, kparts, c.Name, refer, c.IsEmptyIndex)
	case ast.ConstraintFulltext:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil {
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewFullTextIndex(kparts, c.Name, c.IsEmptyIndex, io)
	case ast.ConstraintCheck:
		e := transformExprNodeToExpr(c.Expr)
		return NewCheckIndex(e, c.Enforced)
	case ast.ConstraintNoConstraint:
		fallthrough
	case ast.ConstraintKey:
		fallthrough
	case ast.ConstraintUniqKey:
		fallthrough
	case ast.ConstraintUniqIndex:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported constraint %v\n", c.Tp))
	}
	return nil
}

//transform ast.RowFormat to tree.RowFormatType
func transformRowFormatToRowFormatType(rf uint64) RowFormatType {
	switch rf {
	case ast.RowFormatDefault:
		return ROW_FORMAT_DEFAULT
	case ast.RowFormatDynamic:
		return ROW_FORMAT_DYNAMIC
	case ast.RowFormatFixed:
		return ROW_FORMAT_FIXED
	case ast.RowFormatCompressed:
		return ROW_FORMAT_COMPRESSED
	case ast.RowFormatRedundant:
		return ROW_FORMAT_REDUNDANT
	case ast.RowFormatCompact:
		return ROW_FORMAT_COMPACT
	case ast.TokuDBRowFormatDefault:
		fallthrough
	case ast.TokuDBRowFormatFast:
		fallthrough
	case ast.TokuDBRowFormatSmall:
		fallthrough
	case ast.TokuDBRowFormatZlib:
		fallthrough
	case ast.TokuDBRowFormatQuickLZ:
		fallthrough
	case ast.TokuDBRowFormatLzma:
		fallthrough
	case ast.TokuDBRowFormatSnappy:
		fallthrough
	case ast.TokuDBRowFormatUncompressed:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported row format %v\n", rf))
	}
	return ROW_FORMAT_DEFAULT
}

//transform ast.TableOption to tree.TableOption
func transformTableOptionToTableOption(to *ast.TableOption) TableOption {
	switch to.Tp {
	case ast.TableOptionEngine:
		return NewTableOptionEngine(to.StrValue)
	case ast.TableOptionCharset:
		return NewTableOptionCharset(to.StrValue)
	case ast.TableOptionCollate:
		return NewTableOptionCollate(to.StrValue)
	case ast.TableOptionAutoIncrement:
		return NewTableOptionAutoIncrement(to.UintValue)
	case ast.TableOptionComment:
		return NewTableOptionComment(to.StrValue)
	case ast.TableOptionAvgRowLength:
		return NewTableOptionAvgRowLength(to.UintValue)
	case ast.TableOptionCheckSum:
		return NewTableOptionChecksum(to.UintValue)
	case ast.TableOptionCompression:
		return NewTableOptionCompression(to.StrValue)
	case ast.TableOptionConnection:
		return NewTableOptionConnection(to.StrValue)
	case ast.TableOptionPassword:
		return NewTableOptionPassword(to.StrValue)
	case ast.TableOptionKeyBlockSize:
		return NewTableOptionKeyBlockSize(to.UintValue)
	case ast.TableOptionMaxRows:
		return NewTableOptionMaxRows(to.UintValue)
	case ast.TableOptionMinRows:
		return NewTableOptionMinRows(to.UintValue)
	case ast.TableOptionDelayKeyWrite:
		return NewTableOptionDelayKeyWrite(to.UintValue)
	case ast.TableOptionRowFormat:
		ft := transformRowFormatToRowFormatType(to.UintValue)
		return NewTableOptionRowFormat(ft)
	case ast.TableOptionStatsPersistent:
		return NewTableOptionStatsPersistent()
	case ast.TableOptionStatsAutoRecalc:
		return NewTableOptionStatsAutoRecalc(to.UintValue, to.Default)
	case ast.TableOptionPackKeys:
		return NewTableOptionPackKeys()
	case ast.TableOptionTablespace:
		return NewTableOptionTablespace(to.StrValue)
	case ast.TableOptionDataDirectory:
		return NewTableOptionDataDirectory(to.StrValue)
	case ast.TableOptionIndexDirectory:
		return NewTableOptionIndexDirectory(to.StrValue)
	case ast.TableOptionStorageMedia:
		return NewTableOptionStorageMedia(to.StrValue)
	case ast.TableOptionStatsSamplePages:
		return NewTableOptionStatsSamplePages(to.UintValue, to.Default)
	case ast.TableOptionSecondaryEngine:
		return NewTableOptionSecondaryEngine(to.StrValue)
	case ast.TableOptionSecondaryEngineNull:
		return NewTableOptionSecondaryEngineNull()
	case ast.TableOptionUnion:
		var name []*TableName = make([]*TableName, len(to.TableNames))
		for i, tn := range to.TableNames {
			name[i] = transformTableNameToTableName(tn)
		}
		return NewTableOptionUnion(name)
	case ast.TableOptionEncryption:
		return NewTableOptionEncryption(to.StrValue)
	case ast.TableOptionNone: //mysql 8.0 does not have it
	case ast.TableOptionAutoIdCache: //mysql 8.0 does not have it
	case ast.TableOptionAutoRandomBase: //mysql 8.0 does not have it
	case ast.TableOptionShardRowID: //mysql 8.0 does not have it
	case ast.TableOptionPreSplitRegion: //mysql 8.0 does not have it
	case ast.TableOptionNodegroup: //mysql 8.0 does not have it
	case ast.TableOptionInsertMethod: //mysql 8.0 does not have it
	case ast.TableOptionTableCheckSum: //mysql 8.0 does not have it
	default:
		panic(fmt.Errorf("unsupported table option\n"))
	}
	return nil
}

//transform ast.PartitionMethod to tree.PartitionBy
func transformPartitionMethodToPartitionBy(pm *ast.PartitionMethod) *PartitionBy {
	var partType PartitionType
	switch pm.Tp {
	case model.PartitionTypeRange:
		var e Expr = nil
		var c []*UnresolvedName = nil
		if pm.Expr != nil {
			e = transformExprNodeToExpr(pm.Expr)
		}
		if pm.ColumnNames != nil {
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewRangeType(e, c)
	case model.PartitionTypeHash:
		var e Expr = nil
		if pm.Expr != nil {
			e = transformExprNodeToExpr(pm.Expr)
		}
		partType = NewHashType(pm.Linear, e)
	case model.PartitionTypeList:
		var e Expr = nil
		var c []*UnresolvedName = nil
		if pm.Expr != nil {
			e = transformExprNodeToExpr(pm.Expr)
		}
		if pm.ColumnNames != nil {
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewListType(e, c)
	case model.PartitionTypeKey:
		var c []*UnresolvedName = nil
		if pm.ColumnNames != nil {
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewKeyType(pm.Linear, c)
	default:
		panic(fmt.Errorf("unsupported partition type %v\n", pm.Tp))
	}
	return NewPartitionBy(partType, pm.Num)
}

//transform ast.PartitionDefinitionClause to tree.Values
func transformPartitionDefinitionClauseToValues(pdc ast.PartitionDefinitionClause) Values {
	switch n := pdc.(type) {
	case *ast.PartitionDefinitionClauseLessThan:
		var el Exprs = make([]Expr, len(n.Exprs))
		for i, e := range n.Exprs {
			el[i] = transformExprNodeToExpr(e)
		}
		return NewValuesLessThan(el)
	case *ast.PartitionDefinitionClauseIn:
		var el []Exprs = make([]Exprs, len(n.Values))
		for i, v := range n.Values {
			el[i] = make([]Expr, len(v))
			for j, vv := range v {
				el[i][j] = transformExprNodeToExpr(vv)
			}
		}
		return NewValuesIn(el)
	case *ast.PartitionDefinitionClauseNone:
	case *ast.PartitionDefinitionClauseHistory:
	default:
		panic(fmt.Errorf("unsupported PartitionDefinitionClause %v \n", n))
	}
	return nil
}

//transform ast.SubPartitionDefinition to tree.SubPartition
func transformSubPartitionDefinitionToSubPartition(spd *ast.SubPartitionDefinition) *SubPartition {
	var options []TableOption = nil
	if spd.Options != nil {
		options = make([]TableOption, len(spd.Options))
		for i, o := range spd.Options {
			options[i] = transformTableOptionToTableOption(o)
		}
	}
	return NewSubPartition(Identifier(spd.Name.O), options)
}

//transform ast.PartitionDefinition to tree.Partition
func transformPartitionDefinitionToPartition(pd *ast.PartitionDefinition) *Partition {
	name := Identifier(pd.Name.O)
	var values Values = nil
	if pd.Clause != nil {
		values = transformPartitionDefinitionClauseToValues(pd.Clause)
	}

	var options []TableOption = nil
	if pd.Options != nil {
		options = make([]TableOption, len(pd.Options))
		for i, o := range pd.Options {
			options[i] = transformTableOptionToTableOption(o)
		}
	}

	var subs []*SubPartition = nil
	if pd.Sub != nil {
		subs = make([]*SubPartition, len(pd.Sub))
		for i, s := range pd.Sub {
			subs[i] = transformSubPartitionDefinitionToSubPartition(s)
		}
	}

	return NewPartition(name, values, options, subs)
}

//transform ast.PartitionOptions to tree.PartitionOption
func transformPartitionOptionsToPartitionOption(po *ast.PartitionOptions) *PartitionOption {
	var partBy *PartitionBy = transformPartitionMethodToPartitionBy(&po.PartitionMethod)
	var subPartBy *PartitionBy = nil
	if po.Sub != nil {
		subPartBy = transformPartitionMethodToPartitionBy(po.Sub)
	}

	var parts []*Partition = nil
	if po.Definitions != nil {
		parts = make([]*Partition, len(po.Definitions))
		for i, d := range po.Definitions {
			parts[i] = transformPartitionDefinitionToPartition(d)
		}
	}

	return NewPartitionOption(partBy, subPartBy, parts)
}

//transform ast.CreateTableStmt to tree.CreateTable
func transformCreateTableStmtToCreateTable(cts *ast.CreateTableStmt) *CreateTable {
	table := transformTableNameToTableName(cts.Table)
	var defs TableDefs = make([]TableDef, len(cts.Cols)+len(cts.Constraints))
	for i, col := range cts.Cols {
		defs[i] = transformColumnDefToTableDef(col)
	}

	for i, ct := range cts.Constraints {
		defs[i+len(cts.Cols)] = transformConstraintToIndexTableDef(ct)
	}

	var options []TableOption = nil
	if cts.Options != nil {
		options = make([]TableOption, len(cts.Options))
		for i, to := range cts.Options {
			options[i] = transformTableOptionToTableOption(to)
		}
	}

	var partition *PartitionOption = nil
	if cts.Partition != nil {
		partition = transformPartitionOptionsToPartitionOption(cts.Partition)
	}

	return &CreateTable{
		IfNotExists:     cts.IfNotExists,
		Table:           *table,
		Defs:            defs,
		Options:         options,
		PartitionOption: partition,
	}
}

//transform ast.DatabaseOption to tree.CreateOption
func transformDatabaseOptionToCreateOption(do *ast.DatabaseOption)CreateOption{
	switch do.Tp {
	case ast.DatabaseOptionNone:
		panic("no such thing")
	case ast.DatabaseOptionCharset:
		return NewCreateOptionCharset(do.Value)
	case ast.DatabaseOptionCollate:
		return NewCreateOptionCollate(do.Value)
	case ast.DatabaseOptionEncryption:
		return NewCreateOptionEncryption(do.Value)
	}
	panic(fmt.Errorf("unsupported database option %v\n",do.Tp))
	return nil
}

//transform ast.CreateDatabaseStmt to tree.CreateDatabase
func transformCreateDatabaseStmtToCreateDatabase(cds *ast.CreateDatabaseStmt)*CreateDatabase{
	var opts []CreateOption = nil
	if cds.Options != nil{
		opts = make([]CreateOption,len(cds.Options))
		for i,co := range cds.Options {
			opts[i] = transformDatabaseOptionToCreateOption(co)
		}
	}
	return NewCreateDatabase(cds.IfNotExists,Identifier(cds.Name),opts)
}

//transform ast.DropDatabaseStmt to tree.DropDatabase
func transformDropDatabaseStmtToDropDatabase(dds *ast.DropDatabaseStmt)*DropDatabase{
	return NewDropDatabase(Identifier(dds.Name),dds.IfExists)
}

//transform ast.DropTableStmt to tree.DropTable
func transformDropTableStmtToDropTable(dts *ast.DropTableStmt)*DropTable{
	var names TableNames = nil
	if dts.Tables != nil {
		names = make([]*TableName,len(dts.Tables))
		for i,t := range dts.Tables {
			names[i] = transformTableNameToTableName(t)
		}
	}
	return NewDropTable(dts.IfExists,names)
}

//transform ast.DeleteStmt to tree.Delete
func transformDeleteStmtToDelete(ds *ast.DeleteStmt)*Delete{
	if ds.Tables != nil && ds.TableRefs != nil {
		panic("unsupported multiple-Table syntax for Delete statement ")
	}
	tabExpr := transformJoinToTableExpr(ds.TableRefs.TableRefs)
	var w Expr = nil
	if ds.Where != nil {
		w = transformExprNodeToExpr(ds.Where)
	}

	var o OrderBy = nil
	if ds.Order != nil {
		o = transformOrderByClauseToOrderBy(ds.Order)
	}

	var l *Limit = nil
	if ds.Limit != nil {
		l = transformLimitToLimit(ds.Limit)
	}

	return NewDelete(tabExpr,NewWhere(w),o,l);
}

//transform ast.Assignment to tree.UpdateExpr
func transformAssignmentToUpdateExpr(a *ast.Assignment) *UpdateExpr {
	un := transformColumnNameToUnresolvedName(a.Column)
	e := transformExprNodeToExpr(a.Expr)
	return NewUpdateExpr(false, []*UnresolvedName{un}, e)
}

//transform ast.UpdateStmt to tree.Update
func transformUpdateStmtToUpdate(us *ast.UpdateStmt) *Update {
	te := transformJoinToTableExpr(us.TableRefs.TableRefs)

	var ues UpdateExprs = make([]*UpdateExpr,len(us.List))
	for i,as := range us.List {
		ues[i] = transformAssignmentToUpdateExpr(as)
	}

	var w Expr = nil
	if us.Where != nil {
		w = transformExprNodeToExpr(us.Where)
	}

	var o OrderBy = nil
	if us.Order != nil {
		o = transformOrderByClauseToOrderBy(us.Order)
	}

	var l *Limit = nil
	if us.Limit != nil {
		l = transformLimitToLimit(us.Limit)
	}

	return NewUpdate(te,ues,nil,NewWhere(w),o,l)
}

//transform ast.ColumnNameOrUserVar to tree.LoadColumn
func transformColumnNameOrUserVarToLoadColumn(cnouv *ast.ColumnNameOrUserVar) LoadColumn {
	if cnouv.ColumnName != nil {
		return transformColumnNameToUnresolvedName(cnouv.ColumnName)
	}
	if cnouv.UserVar != nil {
		return transformVariableExprToVarExpr(cnouv.UserVar)
	}
	panic("Both of ColumnName and UserVar are nil")
	return nil
}

//transform ast.LoadDataStmt to tree.Load
func transformLoadDataStmtToLoad(lds *ast.LoadDataStmt) *Load {
	var dk DuplicateKey = nil
	switch lds.OnDuplicate {
	case ast.OnDuplicateKeyHandlingError:
		dk = NewDuplicateKeyError()
	case ast.OnDuplicateKeyHandlingReplace:
		dk = NewDuplicateKeyReplace()
	case ast.OnDuplicateKeyHandlingIgnore:
		dk = NewDuplicateKeyIgnore()
	}

	tn := transformTableNameToTableName(lds.Table)

	var fie *Fields = nil
	if lds.FieldsInfo != nil {
		fc := lds.FieldsInfo
		fie = NewFields(fc.Terminated,fc.OptEnclosed,fc.Enclosed,fc.Escaped)
	}

	var li *Lines = nil
	if lds.LinesInfo != nil {
		lc := lds.LinesInfo
		li = NewLines(lc.Starting,lc.Terminated)
	}

	var lcs []LoadColumn = nil
	if lds.ColumnsAndUserVars != nil {
		lcs = make([]LoadColumn,len(lds.ColumnsAndUserVars))
		for i,c := range lds.ColumnsAndUserVars {
			lcs[i] = transformColumnNameOrUserVarToLoadColumn(c)
		}
	}

	var ues UpdateExprs = nil
	if lds.ColumnAssignments != nil {
		ues = make([]*UpdateExpr,len(lds.ColumnAssignments))
		for i,a := range lds.ColumnAssignments {
			ues[i] = transformAssignmentToUpdateExpr(a)
		}
	}

	return NewLoad(lds.IsLocal,lds.Path,dk,tn,fie,li,lds.IgnoreLines,lcs,ues)
}

//transform ast.BeginStmt to tree.BeginTransaction
func transformBeginStmtToBeginTransaction(bs *ast.BeginStmt)*BeginTransaction{
	var rwm ReadWriteMode = READ_WRITE_MODE_READ_WRITE
	if bs.ReadOnly {
		rwm = READ_WRITE_MODE_READ_ONLY
	}
	return NewBeginTransaction(MakeTransactionModes(rwm))
}

//transform ast.CompletionType to tree.CompletionType
func transformCompletionTypeToCompletionType(ct ast.CompletionType) CompletionType {
	var t CompletionType = COMPLETION_TYPE_NO_CHAIN
	switch ct {
	case ast.CompletionTypeDefault:
		t = COMPLETION_TYPE_NO_CHAIN
	case ast.CompletionTypeChain:
		t = COMPLETION_TYPE_CHAIN
	case ast.CompletionTypeRelease:
		t = COMPLETION_TYPE_RELEASE
	}
	return t
}

//transform ast.CommitStmt to tree.CommitTransaction
func transformCommitStmtToCommitTransaction(cs *ast.CommitStmt) *CommitTransaction {
	t := transformCompletionTypeToCompletionType(cs.CompletionType)
	return NewCommitTransaction(t)
}

//transform ast.RollbackStmt to tree.RollbackTransaction
func transformRollbackStmtToRollbackTransaction(rs *ast.RollbackStmt) *RollbackTransaction {
	t := transformCompletionTypeToCompletionType(rs.CompletionType)
	return NewRollbackTransaction(t)
}

//transform ast.UseStmt to tree.Use
func transformUseStmtToUse(us *ast.UseStmt) *Use {
	return NewUse(us.DBName)
}

//transform ast.ShowStmt to tree.Show
func transformShowStmtToShow(ss *ast.ShowStmt)Show{
	switch ss.Tp {
	case ast.ShowCreateTable:
		u := transformTableNameToUnresolvedObjectName(ss.Table)
		return NewShowCreate(u)
	case ast.ShowCreateDatabase:
		return NewShowCreateDatabase(ss.IfNotExists,ss.DBName)
	case ast.ShowColumns:
		u := transformTableNameToUnresolvedObjectName(ss.Table)

		var cname *UnresolvedName = nil
		if ss.Column != nil {
			cname = transformColumnNameToUnresolvedName(ss.Column)
		}

		var l *ComparisonExpr = nil
		if ss.Pattern != nil {
			l = transformPatternLikeExprToComparisonExprIn(ss.Pattern)
		}

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}

		return NewShowColumns(ss.Extended, ss.Full, u, ss.DBName, l, w, cname)
	case ast.ShowDatabases:
		var l *ComparisonExpr = nil
		if ss.Pattern != nil {
			l = transformPatternLikeExprToComparisonExprIn(ss.Pattern)
		}

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}
		return NewShowDatabases(l,w)
	case ast.ShowTables:
		var l *ComparisonExpr = nil
		if ss.Pattern != nil {
			l = transformPatternLikeExprToComparisonExprIn(ss.Pattern)
		}

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}
		return NewShowTables(ss.Extended,ss.Full,ss.DBName,l,w)
	case ast.ShowProcessList:
		return NewShowProcessList(ss.Full)
	case ast.ShowErrors:
		return NewShowErrors()
	case ast.ShowWarnings:
		return NewShowWarnings()
	case ast.ShowVariables:
		var l *ComparisonExpr = nil
		if ss.Pattern != nil {
			l = transformPatternLikeExprToComparisonExprIn(ss.Pattern)
		}

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}
		return NewShowVariables(ss.GlobalScope,l,w)
	case ast.ShowStatus:
		var l *ComparisonExpr = nil
		if ss.Pattern != nil {
			l = transformPatternLikeExprToComparisonExprIn(ss.Pattern)
		}

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}
		return NewShowStatus(ss.GlobalScope,l,w)
	case ast.ShowIndex:
		t := transformTableNameToTableName(ss.Table)

		var w *Where = nil
		if ss.Where != nil {
			e := transformExprNodeToExpr(ss.Where)
			w = NewWhere(e)
		}
		return NewShowIndex(*t,w)
	}
	panic(fmt.Errorf("unsupported show %v\n",ss.Tp))
	return nil
}

//transform ast.ExplainStmt to tree.Explain
func transformExplainStmtToExplain(es *ast.ExplainStmt) Explain {
	var stmt Statement = nil
	switch st := es.Stmt.(type) {
	case *ast.ShowStmt:
		stmt = transformShowStmtToShow(st)
	case *ast.SetOprStmt:
		stmt = transformSetOprStmtToSelectStatement(st)
	case *ast.SelectStmt:
		stmt = transformSelectStmtToSelect(st)
	case *ast.DeleteStmt:
		stmt = transformDeleteStmtToDelete(st)
	case *ast.InsertStmt:
		stmt = transformInsertStmtToInsert(st)
	case *ast.UpdateStmt:
		stmt = transformUpdateStmtToUpdate(st)
	default:
		panic(fmt.Errorf("unsupported Statment %v \n",st))
	}
	if es.Analyze {
		return NewExplainAnalyze(stmt,es.Format)
	}else{
		return NewExplainStmt(stmt,es.Format)
	}
	return nil
}

//transform ast.ExplainForStmt to tree.Explain
func transformExplainForStmtToExplain(efs *ast.ExplainForStmt) Explain {
	return NewExplainFor(efs.Format,efs.ConnectionID)
}

//transform ast.VariableAssignment to tree.VarAssignmentExpr
func transformVariableAssignment(va *ast.VariableAssignment)*VarAssignmentExpr {
	var v Expr = nil
	if va.Value != nil {
		v = transformExprNodeToExpr(va.Value)
	}

	var r Expr = nil
	if va.ExtendValue != nil {
		r = transformExprNodeToExpr(va.ExtendValue)
	}
	return NewVarAssignmentExpr(va.IsSystem,va.IsGlobal,va.Name,v,r)
}

//transform ast.SetStmt to tree.SetVar
func transformSetStmtToSetVar(ss *ast.SetStmt) *SetVar{
	var a []*VarAssignmentExpr = nil
	if ss.Variables != nil {
		a = make([]*VarAssignmentExpr,len(ss.Variables))
		for i,v := range ss.Variables {
			a[i] = transformVariableAssignment(v)
		}
	}
	return NewSetVar(a)
}

//transform ast.IndexLockAndAlgorithm to []tree.MiscOption
func transformIndexLockAndAlgorithmToMiscOption(ilaa *ast.IndexLockAndAlgorithm)[]MiscOption {
	var misc []MiscOption
	switch ilaa.LockTp {
	case ast.LockTypeNone:
		misc = append(misc,&LockNone{})
	case ast.LockTypeDefault:
		misc = append(misc,&LockDefault{})
	case ast.LockTypeShared:
		misc = append(misc,&LockShared{})
	case ast.LockTypeExclusive:
		misc = append(misc,&LockExclusive{})
	}

	switch ilaa.AlgorithmTp {
	case ast.AlgorithmTypeDefault:
		misc = append(misc,&AlgorithmDefault{})
	case ast.AlgorithmTypeCopy:
		misc = append(misc,&AlgorithmCopy{})
	case ast.AlgorithmTypeInplace:
		misc = append(misc,&AlgorithmInplace{})
	default:
		misc = append(misc,&AlgorithmDefault{})
	}

	return misc
}

//transform ast.CreateIndexStmt to tree.CreateIndex
func transformCreateIndexStmtToCreateIndex(cis *ast.CreateIndexStmt) *CreateIndex{
	tab := transformTableNameToTableName(cis.Table)

	it := transformIndexKeyTypeToIndexCategory(cis.KeyType)

	kps := transformIndexPartSpecificationArrayToKeyPartArray(cis.IndexPartSpecifications)
	var indexOpt *IndexOption = nil
	if cis.IndexOption != nil {
		indexOpt = transformIndexOptionToIndexOption(cis.IndexOption)
	}

	var misc []MiscOption = nil
	if cis.LockAlg != nil {
		misc = transformIndexLockAndAlgorithmToMiscOption(cis.LockAlg)
	}

	return NewCreateIndex(Identifier(cis.IndexName), *tab, cis.IfNotExists, it, kps, indexOpt, misc)
}

//transform ast.DropIndexStmt to tree.DropIndex
func transformDropIndexStmtToDropIndex(dis *ast.DropIndexStmt) *DropIndex {
	tab := transformTableNameToTableName(dis.Table)

	var misc []MiscOption = nil
	if dis.LockAlg != nil {
		misc = transformIndexLockAndAlgorithmToMiscOption(dis.LockAlg)
	}

	return NewDropIndex(Identifier(dis.IndexName),*tab,dis.IfExists,misc)
}

//transform ast.UserSpec to tree.Role
func transformUserSpecToRole(us *ast.UserSpec) *Role {
	if !us.IsRole {
		panic("it is not role")
	}else if us.User == nil {
		panic("role is null")
	}

	return NewRole(us.User.Username,us.User.Hostname)
}

//transform ast.CreateUserStmt to tree.CreateRole
func transformCreateUserStmtToCreateRole(cus *ast.CreateUserStmt)*CreateRole {
	if !cus.IsCreateRole {
		panic("it is not create role statement")
	}

	var r []*Role = nil
	if cus.Specs != nil {
		r = make([]*Role,len(cus.Specs))
		for i,us := range cus.Specs {
			r[i] = transformUserSpecToRole(us)
		}
	}

	return NewCreateRole(cus.IfNotExists,r)
}

//transform ast.DropUserStmt to tree.DropRole
func transformDropUserStmtToDropRole(dus *ast.DropUserStmt) *DropRole {
	if !dus.IsDropRole {
		panic("it is not drop user statement")
	}

	var r []*Role = nil
	if dus.UserList != nil {
		r = make([]*Role,len(dus.UserList))
		for i,us := range dus.UserList {
			r[i] = NewRole(us.Username,us.Hostname)
		}
	}
	return NewDropRole(dus.IfExists,r)
}

//transform ast.AuthOption to tree.User
func transformAuthOptionToUser(ao *ast.AuthOption) *User{
	ap := ""
	as := ""

	if ao != nil {
		if ao.ByAuthString {
			as = ao.AuthString
		} else {
			as = ao.HashString
		}
	}

	return NewUser("","",ap,as)
}

//transform ast.UserSpec to tree.Role
func transformUserSpecToUser(us *ast.UserSpec) *User {
	if us.IsRole {
		panic("it is not user")
	}else if us.User == nil {
		panic("user is null")
	}

	u := transformAuthOptionToUser(us.AuthOpt)
	u.Username = us.User.Username
	u.Hostname = us.User.Hostname
	return u
}

func transformTLSOptionToTlsOption(t *ast.TLSOption) TlsOption {
	switch t.Type {
	case ast.TslNone:
		return &TlsOptionNone{}
	case ast.Ssl:
		return &TlsOptionSSL{}
	case ast.X509:
		return &TlsOptionX509{}
	case ast.Cipher:
		return &TlsOptionCipher{
			Cipher:        t.Value,
		}
	case ast.Issuer:
		return &TlsOptionIssuer{
			Issuer:        t.Value,
		}
	case ast.Subject:
		return &TlsOptionSubject{
			Subject:       t.Value,
		}
	default:
		panic("unsupported tlsoption")
	}
	return nil
}

//transform ast.ResourceOption to tree.ResourceOption
func transformResourceOptionToResourceOption(ro *ast.ResourceOption)ResourceOption{
	switch ro.Type {
	case ast.MaxQueriesPerHour:
		return &ResourceOptionMaxQueriesPerHour{Count:ro.Count}
	case ast.MaxUpdatesPerHour:
		return &ResourceOptionMaxUpdatesPerHour{Count:ro.Count}
	case ast.MaxConnectionsPerHour:
		return &ResourceOptionMaxConnectionPerHour{Count:ro.Count}
	case ast.MaxUserConnections:
		return &ResourceOptionMaxUserConnections{Count :ro.Count}
	}
	return nil
}

//transform ast.PasswordOrLockOption to tree.UserMiscOption
func transformPasswordOrLockOptionToUserMiscOption(polo *ast.PasswordOrLockOption) UserMiscOption {
	switch polo.Type {
	case ast.PasswordExpire:
		return &UserMiscOptionPasswordExpireNone{}
	case ast.PasswordExpireDefault:
		return &UserMiscOptionPasswordExpireDefault{}
	case ast.PasswordExpireNever:
		return &UserMiscOptionPasswordExpireNever{}
	case ast.PasswordExpireInterval:
		return &UserMiscOptionPasswordExpireInterval{Value: polo.Count}
	case ast.Lock:
		return &UserMiscOptionAccountLock{}
	case ast.Unlock:
		return &UserMiscOptionAccountUnlock{}
	default:
		panic("unsupported password or lock")
	}
	return nil
}

//transform ast.CreateUserStmt to tree.CreateUser
func transformCreatUserStmtToCreateUser(cus *ast.CreateUserStmt)*CreateUser{
	if cus.IsCreateRole {
		panic("it is not the create user statement")
	}

	var u []*User = nil
	var r []*Role = nil
	if cus.Specs != nil {
		for _,us := range cus.Specs {
			if us.IsRole {
				ret := transformUserSpecToRole(us)
				r = append(r,ret)
			}else{
				ret := transformUserSpecToUser(us)
				u = append(u,ret)
			}
		}
	}

	var tls []TlsOption = nil
	if cus.TLSOptions != nil {
		tls = make([]TlsOption,len(cus.TLSOptions))
		for i,t := range cus.TLSOptions {
			tls[i] = transformTLSOptionToTlsOption(t)
		}
	}

	var res []ResourceOption = nil
	if cus.ResourceOptions != nil {
		res = make([]ResourceOption,len(cus.ResourceOptions))
		for i,re := range cus.ResourceOptions {
			res[i] = transformResourceOptionToResourceOption(re)
		}
	}

	var umo []UserMiscOption = nil
	if cus.PasswordOrLockOptions != nil {
		umo = make([]UserMiscOption,len(cus.PasswordOrLockOptions))
		for i,p := range cus.PasswordOrLockOptions {
			umo[i] = transformPasswordOrLockOptionToUserMiscOption(p)
		}
	}

	return NewCreateUser(cus.IfNotExists,u,r,tls,res,umo)
}

//transform ast.DropUserStmt to tree.DropUser
func transformDropUserStmtToDropUser(dus *ast.DropUserStmt)*DropUser{
	if dus.IsDropRole {
		panic("it is not the drop user statement")
	}

	var u []*User = nil
	if dus.UserList != nil {
		u = make([]*User,len(dus.UserList))
		for i,ul := range dus.UserList {
			u[i] = NewUser(ul.Username,ul.Hostname,"","")
		}
	}
	return NewDropUser(dus.IfExists,u)
}

//transform ast.AlterUserStmt to tree.AlterUser
func transformAlterUserStmtToAlterUser(aus *ast.AlterUserStmt) *AlterUser{
	if aus.CurrentAuth != nil {
		userfunc := transformAuthOptionToUser(aus.CurrentAuth)
		return NewAlterUser(aus.IfExists,true,userfunc,nil,nil,nil,nil,nil)
	}

	var u []*User = nil
	var r []*Role = nil
	if aus.Specs != nil {
		for _,us := range aus.Specs {
			if us.IsRole {
				ret := transformUserSpecToRole(us)
				r = append(r,ret)
			}else{
				ret := transformUserSpecToUser(us)
				u = append(u,ret)
			}
		}
	}

	var tls []TlsOption = nil
	if aus.TLSOptions != nil {
		tls = make([]TlsOption,len(aus.TLSOptions))
		for i,t := range aus.TLSOptions {
			tls[i] = transformTLSOptionToTlsOption(t)
		}
	}

	var res []ResourceOption = nil
	if aus.ResourceOptions != nil {
		res = make([]ResourceOption,len(aus.ResourceOptions))
		for i,re := range aus.ResourceOptions {
			res[i] = transformResourceOptionToResourceOption(re)
		}
	}

	var umo []UserMiscOption = nil
	if aus.PasswordOrLockOptions != nil {
		umo = make([]UserMiscOption,len(aus.PasswordOrLockOptions))
		for i,p := range aus.PasswordOrLockOptions {
			umo[i] = transformPasswordOrLockOptionToUserMiscOption(p)
		}
	}

	return NewAlterUser(aus.IfExists,false,nil,u,r,tls,res,umo)
}

//transform mysql.PrivilegeType to tree.PrivilegeType
func transformPrivilegeTypeToPrivilegeType(pt mysql.PrivilegeType)PrivilegeType{
	switch pt {
	case mysql.UsagePriv:
		return PRIVILEGE_TYPE_STATIC_USAGE
	case mysql.CreatePriv:
		return PRIVILEGE_TYPE_STATIC_CREATE
	case mysql.SelectPriv:
		return PRIVILEGE_TYPE_STATIC_SELECT
	case mysql.InsertPriv:
		return PRIVILEGE_TYPE_STATIC_INSERT
	case mysql.UpdatePriv:
		return PRIVILEGE_TYPE_STATIC_UPDATE
	case mysql.DeletePriv:
		return PRIVILEGE_TYPE_STATIC_DELETE
	case mysql.ShowDBPriv:
		return PRIVILEGE_TYPE_STATIC_SHOW_DATABASES
	case mysql.SuperPriv:
		return PRIVILEGE_TYPE_STATIC_SUPER
	case mysql.CreateUserPriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_USER
	case mysql.TriggerPriv:
		return PRIVILEGE_TYPE_STATIC_TRIGGER
	case mysql.DropPriv:
		return PRIVILEGE_TYPE_STATIC_DROP
	case mysql.ProcessPriv:
		return PRIVILEGE_TYPE_STATIC_PROCESS
	case mysql.GrantPriv:
		return PRIVILEGE_TYPE_STATIC_GRANT_OPTION
	case mysql.ReferencesPriv:
		return PRIVILEGE_TYPE_STATIC_REFERENCES
	case mysql.AlterPriv:
		return PRIVILEGE_TYPE_STATIC_ALTER
	case mysql.ExecutePriv:
		return PRIVILEGE_TYPE_STATIC_EXECUTE
	case mysql.IndexPriv:
		return PRIVILEGE_TYPE_STATIC_INDEX
	case mysql.CreateViewPriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_VIEW
	case mysql.ShowViewPriv:
		return PRIVILEGE_TYPE_STATIC_SHOW_VIEW
	case mysql.CreateRolePriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_ROLE
	case mysql.DropRolePriv:
		return PRIVILEGE_TYPE_STATIC_DROP_ROLE
	case mysql.CreateTMPTablePriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES
	case mysql.LockTablesPriv:
		return PRIVILEGE_TYPE_STATIC_LOCK_TABLES
	case mysql.CreateRoutinePriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE
	case mysql.AlterRoutinePriv:
		return PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE
	case mysql.EventPriv:
		return PRIVILEGE_TYPE_STATIC_EVENT
	case mysql.ShutdownPriv:
		return PRIVILEGE_TYPE_STATIC_SHUTDOWN
	case mysql.ReloadPriv:
		return PRIVILEGE_TYPE_STATIC_RELOAD
	case mysql.FilePriv:
		return PRIVILEGE_TYPE_STATIC_FILE
	case mysql.CreateTablespacePriv:
		return PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE
	case mysql.ReplicationClientPriv:
		return PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT
	case mysql.ReplicationSlavePriv:
		return PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE
	case mysql.AllPriv:
		return PRIVILEGE_TYPE_STATIC_ALL
	case mysql.ConfigPriv:
		fallthrough
	case mysql.ExtendedPriv:
		fallthrough
	default:
		panic("unsupported privilege type")
	}
}

//transform ast.PrivElem to tree.Privilege
func transformPrivElemToPrivilege(pe *ast.PrivElem)*Privilege {
	pt := transformPrivilegeTypeToPrivilegeType(pe.Priv)
	var cols []*UnresolvedName = nil
	if pe.Cols != nil {
		cols = make([]*UnresolvedName,len(pe.Cols))
		for i,c := range pe.Cols {
			cols[i] = transformColumnNameToUnresolvedName(c)
		}
	}
	return NewPrivilege(pt,cols)
}

// transform ast.ObjectTypeType To tree.ObjectType
func transformObjectTypeTypeToObjectType(ott ast.ObjectTypeType)ObjectType{
	switch ott {
	case ast.ObjectTypeNone:
		return OBJECT_TYPE_NONE
	case ast.ObjectTypeTable:
		return OBJECT_TYPE_TABLE
	case ast.ObjectTypeFunction:
		return OBJECT_TYPE_FUNCTION
	case ast.ObjectTypeProcedure:
		return OBJECT_TYPE_PROCEDURE
	}
	return OBJECT_TYPE_NONE
}

//transform ast.GrantLevelType to tree.PrivilegeLevelType
func transformGrantLevelTypeToPrivilegeLevelType(glt ast.GrantLevelType)PrivilegeLevelType {
	switch glt {
	case ast.GrantLevelNone:
		return PRIVILEGE_LEVEL_TYPE_GLOBAL
	case ast.GrantLevelGlobal:
		return PRIVILEGE_LEVEL_TYPE_GLOBAL
	case ast.GrantLevelDB:
		return PRIVILEGE_LEVEL_TYPE_DATABASE
	case ast.GrantLevelTable:
		return PRIVILEGE_LEVEL_TYPE_TABLE
	}
	return PRIVILEGE_LEVEL_TYPE_GLOBAL
}

//transform ast.GrantLevel to tree.PrivilegeLevel
func transformGrantLevelToPrivilegeLevel(gl *ast.GrantLevel) *PrivilegeLevel {
	plt := transformGrantLevelTypeToPrivilegeLevelType(gl.Level)
	return NewPrivilegeLevel(plt,gl.DBName,gl.TableName,"")
}

//transform ast.RevokeStmt to tree.Revoke
func transformRevokeStmtToRevoke(rs *ast.RevokeStmt)*Revoke{
	var p[]*Privilege = nil
	if rs.Privs != nil {
		p = make([]*Privilege,len(rs.Privs))
		for i,pr := range rs.Privs {
			p[i] = transformPrivElemToPrivilege(pr)
		}
	}

	objType := transformObjectTypeTypeToObjectType(rs.ObjectType)
	pl := transformGrantLevelToPrivilegeLevel(rs.Level)

	var u []*User = nil
	var r []*Role = nil
	if rs.Users != nil {
		for _,us := range rs.Users {
			if us.IsRole {
				ret := transformUserSpecToRole(us)
				r = append(r,ret)
			}else{
				ret := transformUserSpecToUser(us)
				u = append(u,ret)
			}
		}
	}

	return NewRevoke(false, nil, p, objType, pl, u, r)
}

//transform ast.RevokeRoleStmt to tree.Revoke
func transformRevokeRoleStmtToRevoke(rrs *ast.RevokeRoleStmt) *Revoke {
	var r []*Role = nil
	if rrs.Roles != nil {
		r = make([]*Role,len(rrs.Roles))
		for i,rl := range rrs.Roles {
			r[i] = NewRole(rl.Username,rl.Hostname)
		}
	}


	var u []*User = nil
	if rrs.Users != nil {
		u = make([]*User,len(rrs.Users))
		for i,us := range rrs.Users{
			u[i] = NewUser(us.Username,us.Hostname,"","")
		}
	}
	return NewRevoke(true,r,nil,0,nil,u,nil)
}

//transform ast.GrantStmt to tree.Grant
func transformGrantStmtToGrant(gs *ast.GrantStmt)*Grant{
	var p[]*Privilege = nil
	if gs.Privs != nil {
		p = make([]*Privilege,len(gs.Privs))
		for i,pr := range gs.Privs {
			p[i] = transformPrivElemToPrivilege(pr)
		}
	}

	objType := transformObjectTypeTypeToObjectType(gs.ObjectType)
	pl := transformGrantLevelToPrivilegeLevel(gs.Level)

	var u []*User = nil
	var r []*Role = nil
	if gs.Users != nil {
		for _,us := range gs.Users {
			if us.IsRole {
				ret := transformUserSpecToRole(us)
				r = append(r,ret)
			}else{
				ret := transformUserSpecToUser(us)
				u = append(u,ret)
			}
		}
	}

	return NewGrant(false, false, nil, p, objType, pl, nil, u, r, gs.WithGrant)
}

//transform ast.GrantRoleStmt to tree.Grant
func transformGrantRoleStmtToGrant(grs *ast.GrantRoleStmt)*Grant{
	var r []*Role = nil
	if grs.Roles != nil {
		r = make([]*Role,len(grs.Roles))
		for i,rl := range grs.Roles {
			r[i] = NewRole(rl.Username,rl.Hostname)
		}
	}

	var u []*User = nil
	if grs.Users != nil {
		u = make([]*User,len(grs.Users))
		for i,us := range grs.Users{
			u[i] = NewUser(us.Username,us.Hostname,"","")
		}
	}
	return NewGrant(true, false, r, nil, OBJECT_TYPE_NONE, nil, nil, u, nil, false)
}

//transform ast.GrantProxyStmt to tree.Grant
func transformGrantProxyStmtToGrant(gps *ast.GrantProxyStmt)*Grant {
	pu := NewUser(gps.LocalUser.Username,gps.LocalUser.Hostname,"","")

	var u []*User = nil
	if gps.ExternalUsers != nil {
		u = make([]*User,len(gps.ExternalUsers))
		for i,us := range gps.ExternalUsers{
			u[i] = NewUser(us.Username,us.Hostname,"","")
		}
	}

	return NewGrant(false, true, nil, nil, OBJECT_TYPE_NONE, nil, pu, u, nil, gps.WithGrant)
}

//transform ast.SetDefaultRoleStmt to tree.SetDefaultRole
func transformSetDefaultRoleStmtToSetDefaultRole(sdrs *ast.SetDefaultRoleStmt)*SetDefaultRole {
	var t SetDefaultRoleType = SET_DEFAULT_ROLE_TYPE_NORMAL
	switch sdrs.SetRoleOpt {
	case ast.SetRoleNone:
		t = SET_DEFAULT_ROLE_TYPE_NONE
	case ast.SetRoleAll:
		t = SET_DEFAULT_ROLE_TYPE_ALL
	case ast.SetRoleRegular:
		t = SET_DEFAULT_ROLE_TYPE_NORMAL
	default :
		panic("unsupported default role type")
	}

	var r []*Role = nil
	if sdrs.RoleList != nil {
		r = make([]*Role,len(sdrs.RoleList))
		for i,rl := range sdrs.RoleList {
			r[i] = NewRole(rl.Username,rl.Hostname)
		}
	}

	var u []*User = nil
	if sdrs.UserList != nil {
		u = make([]*User,len(sdrs.UserList))
		for i,us := range sdrs.UserList{
			u[i] = NewUser(us.Username,us.Hostname,"","")
		}
	}

	return NewSetDefaultRole(t,r,u)
}

//transform ast.SetRoleStmt to tree.SetRole
func transformSetRoleStmtToSetRole(srs *ast.SetRoleStmt) *SetRole {
	var t SetRoleType = SET_ROLE_TYPE_NORMAL
	switch srs.SetRoleOpt {
	case ast.SetRoleDefault:
		t = SET_ROLE_TYPE_DEFAULT
	case ast.SetRoleNone:
		t = SET_ROLE_TYPE_NONE
	case ast.SetRoleAll:
		t = SET_ROLE_TYPE_ALL
	case ast.SetRoleAllExcept:
		t = SET_ROLE_TYPE_ALL_EXCEPT
	case ast.SetRoleRegular:
		t = SET_ROLE_TYPE_NORMAL
	}

	var r []*Role = nil
	if srs.RoleList != nil {
		r = make([]*Role,len(srs.RoleList))
		for i,rl := range srs.RoleList {
			r[i] = NewRole(rl.Username,rl.Hostname)
		}
	}

	return NewSetRole(t,r)
}

//transform ast.SetPwdStmt to tree.SetPassword
func transformSetPwdStmtToSetPassword(sps *ast.SetPwdStmt) *SetPassword {
	var u *User = nil
	if sps.User != nil {
		u = NewUser(sps.User.Username,sps.User.Hostname,"","")
	}
	return NewSetPassword(u,sps.Password)
}