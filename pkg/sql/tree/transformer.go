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
		orgStr := strconv.FormatInt(datum.GetInt64(),10)
		return NewNumVal(constant.MakeInt64(datum.GetInt64()), orgStr, false)
	case test_driver.KindUint64:
		orgStr := strconv.FormatUint(datum.GetUint64(),10)
		return NewNumVal(constant.MakeUint64(datum.GetUint64()), orgStr, false)
	case test_driver.KindFloat32:
		orgStr := strconv.FormatFloat(datum.GetFloat64(),'f',-1,64)
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()), orgStr, false)
	case test_driver.KindFloat64: //mysql 1.2E3, 1.2E-3, -1.2E3, -1.2E-3;
		orgStr := strconv.FormatFloat(datum.GetFloat64(),'f',-1,64)
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()), orgStr, false)
	case test_driver.KindString:
		return NewNumVal(constant.MakeString(datum.GetString()), datum.GetString(), false)
	case test_driver.KindMysqlDecimal: //mysql .2, 3.4, -6.78, +9.10
		deci := datum.GetMysqlDecimal().ToString()
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()),string(deci),false)
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
		panic(fmt.Errorf("unsupported datum type %v",datum.Kind()))
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
	panic(fmt.Errorf("unsupported unary expr. op:%s ", uoe.Op.String()))
	return nil
}

//transform ast.UnaryOperationExpr to tree.NotExpr
func transformUnaryOperatorExprToNotExpr(uoe *ast.UnaryOperationExpr) *NotExpr {
	switch uoe.Op {
	case opcode.Not: //not,!
		e := transformExprNodeToExpr(uoe.V)
		return NewNotExpr(e)
	}
	panic(fmt.Errorf("unsupported not expr. op:%s ", uoe.Op.String()))
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
	panic(fmt.Errorf("unsupported binary expr. op:%s ", boe.Op.String()))
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
	panic(fmt.Errorf("unsupported comparison expr. op:%s ", boe.Op.String()))
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
	panic(fmt.Errorf("unsupported and expr. op:%s ", boe.Op.String()))
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
	panic(fmt.Errorf("unsupported or expr. op:%s ", boe.Op.String()))
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
	panic(fmt.Errorf("unsupported xor expr. op:%s ", boe.Op.String()))
	return nil
}

//transform ast.IsNullExpr to tree.IsNullExpr
func transformIsNullExprToIsNullExpr(ine *ast.IsNullExpr) *IsNullExpr {
	if !ine.Not {
		e := transformExprNodeToExpr(ine.Expr)
		return NewIsNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is null expr. %v ", ine))
	return nil
}

//transform ast.IsNotNullExpr to tree.IsNotNullExpr
func transformIsNullExprToIsNotNullExpr(ine *ast.IsNullExpr) *IsNotNullExpr {
	if ine.Not {
		e := transformExprNodeToExpr(ine.Expr)
		return NewIsNotNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is not null expr. %v ", ine))
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
	e1 := transformExprNodeToExpr(ple.Expr)
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
	panic(fmt.Errorf("unsupported resultSetNode"))
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
	panic(fmt.Errorf("unsupported CompareSubqueryExpr expr. op:%s ", cse.Op.String()))
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
	panic(fmt.Errorf("unsupported ResultSetNode type:%v ", rsn))
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
func transformCIStrToIdentifierList(ci []model.CIStr)IdentifierList{
	var l IdentifierList
	for _,x := range ci{
		l = append(l,Identifier(x.O))
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
	panic(fmt.Errorf("Need ExplicitParens :%v ", j))
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
		panic(fmt.Errorf("need column name"))
	}
	cn := cne.Name
	ud, _ := NewUnresolvedName(cn.Schema.O, cn.Table.O, cn.Name.O)
	return ud
}

//transform ast.ColumnName to tree.UnresolvedName
func transformColumnNameToUnresolvedName(cn *ast.ColumnName)*UnresolvedName{
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
func transformFieldTypeToResolvableTypeReference(ft *types.FieldType)ResolvableTypeReference{
	var t ResolvableTypeReference
	switch ft.Tp {
	case mysql.TypeUnspecified:
		panic(fmt.Errorf("unsupported type"))
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
	case mysql.TypeLonglong:
		t = TYPE_LONGLONG
	case mysql.TypeInt24:
		t = TYPE_INT24
	case mysql.TypeDate:
		t = TYPE_DATE
	case mysql.TypeDuration:
		t = TYPE_DURATION
	case mysql.TypeDatetime:
		t = TYPE_DATETIME
	case mysql.TypeYear:
		t = TYPE_YEAR
	case mysql.TypeNewDate:
		t = TYPE_NEWDATE
	case mysql.TypeVarchar:
		t = TYPE_VARCHAR
	case mysql.TypeBit:
		t = TYPE_BIT
	case mysql.TypeJSON:
		t = TYPE_JSON
	case mysql.TypeNewDecimal:
		t = TYPE_NEWDATE
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
	return t
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
	panic(fmt.Errorf("unsupported time unit type %v ", tue.Unit))
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
		r = NewNumVal(constant.MakeInt64(1),"",false)
	}else{
		r = NewNumVal(constant.MakeInt64(0),"",false)
	}

	return NewComparisonExpr(op,e,r)
}

//transform ast.DefaultExpr to tree.DefaultVal
func transformDefaultExprToDefaultVal(expr *ast.DefaultExpr) *DefaultVal {
	return NewDefaultVal()
}

//transform ast.MaxValueExpr to tree.MaxValue
func transformMaxValueExprToMaxValue(expr *ast.MaxValueExpr) *MaxValue {
	return NewMaxValue()
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
	}
	panic(fmt.Errorf("unsupported node %v ", node))
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
		panic(fmt.Errorf("only in brace"))
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
			panic(fmt.Errorf("need set operator"))
		}
		oprType = *sel.AfterSetOperator
	case *ast.SetOprSelectList:
		if sel.AfterSetOperator == nil {
			panic(fmt.Errorf("need set operator"))
		}
		oprType = *sel.AfterSetOperator
	default:
		panic(fmt.Errorf("unsupported single node %v", n))
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
		panic(fmt.Errorf("unsupported single node %v", n))
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
		panic(fmt.Errorf("need Selects"))
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
			panic(fmt.Errorf("unsupported union statement %v", selects[i]))
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
	panic(fmt.Errorf("missing something"))
	return nil
}

//transform ast.SetOprSelectList to tree.SelectStatement
func transformSetOprSelectListToSelectStatement(sosl *ast.SetOprSelectList) SelectStatement {
	return transformSelectArrayToSelectStatement(sosl.Selects)
	panic(fmt.Errorf("missing something"))
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
		for _,row := range is.Lists{
			var arr Exprs = nil
			for _,col := range row{
				e := transformExprNodeToExpr(col)
				arr = append(arr,e)
			}
			rows = append(rows,arr)
		}
	}else if is.Select != nil{
		if ss,ok := is.Select.(*ast.SelectStmt); !ok{
			panic(fmt.Errorf("needs selectstmt"))
		}else{
			for _,row := range ss.Lists{
				e := transformExprNodeToExpr(row)
				rows = append(rows,[]Expr{e})
			}
		}
	}else{
		panic(fmt.Errorf("empty insertstmt"))
	}

	partition := transformCIStrToIdentifierList(is.PartitionNames)

	vc := NewValuesClause(rows)
	sel := NewSelect(vc,nil,nil)
	return NewInsert(table, colums, sel, partition)
}

//transform ast.IndexPartSpecification to tree.KeyPart
func transformIndexPartSpecificationToKeyPart(ips *ast.IndexPartSpecification)*KeyPart{
	cname := transformColumnNameToUnresolvedName(ips.Column)
	var e Expr = nil
	if ips.Expr != nil{
		e = transformExprNodeToExpr(ips.Expr)
	}

	return NewKeyPart(cname,ips.Length,e)
}

//transform []*ast.IndexPartSpecification to []*KeyPart
func transformIndexPartSpecificationArrayToKeyPartArray(ipsArr []*ast.IndexPartSpecification)[]*KeyPart{
	var kparts []*KeyPart = make([]*KeyPart,len(ipsArr))
	for i,p := range ipsArr{
		kparts[i] = transformIndexPartSpecificationToKeyPart(p)
	}
	return kparts
}

//transform ast.ReferOptionType to tree.ReferenceOptionType
func transformReferOptionTypeToReferenceOptionType(rot ast.ReferOptionType)ReferenceOptionType{
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
	panic(fmt.Errorf("invalid reference option %v",rot))
	return REFERENCE_OPTION_INVALID
}

//transform ast.ReferenceDef to tree.AttributeReference
func transformReferenceDefToAttributeReference(rd *ast.ReferenceDef)*AttributeReference{
	tname := transformTableNameToTableName(rd.Table)
	var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(rd.IndexPartSpecifications)
	var ondelete ReferenceOptionType = REFERENCE_OPTION_INVALID
	if rd.OnDelete != nil{
		ondelete = transformReferOptionTypeToReferenceOptionType(rd.OnDelete.ReferOpt)
	}
	var onupdate ReferenceOptionType = REFERENCE_OPTION_INVALID
	if rd.OnUpdate != nil{
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
	return NewAttributeReference(tname,kparts,match,ondelete,onupdate)
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
		return NewAttributeGeneratedAlways(e,co.Stored)
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
		panic(fmt.Errorf("invalid column option"))
	}
	panic(fmt.Errorf("invalid column option"))
	return nil
}

//transform ast.ColumnDef to tree.ColumnTableDef
func transformColumnDefToTableDef(cd *ast.ColumnDef)*ColumnTableDef{
	name := transformColumnNameToUnresolvedName(cd.Name)
	t := transformFieldTypeToResolvableTypeReference(cd.Tp)
	var attr_arr []ColumnAttribute = make([]ColumnAttribute,len(cd.Options))
	for i,op := range cd.Options{
		attr_arr[i] = transformColumnOptionToColumnAttribute(op)
	}

	return NewColumnTableDef(name,t,attr_arr)
}

//transform model.IndexType to tree.IndexType
func transformIndexTypeToIndexType(it model.IndexType)IndexType {
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
func transformIndexOptionToIndexOption(io *ast.IndexOption)*IndexOption{
	it := transformIndexTypeToIndexType(io.Tp)
	vt := transformIndexVisibilityToVisibleType(io.Visibility)
	return NewIndexOption(io.KeyBlockSize,it,io.ParserName.O,io.Comment,vt,"","")
}

//transform ast.Constraint to IndexTableDef
func transformConstraintToIndexTableDef(c *ast.Constraint)IndexTableDef{
	switch c.Tp {
	case ast.ConstraintPrimaryKey:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil{
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewPrimaryKeyIndex(kparts,c.Name,c.IsEmptyIndex,io)
	case ast.ConstraintIndex:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil{
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewIndex(kparts,c.Name,c.IsEmptyIndex,io)
	case ast.ConstraintUniq:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil{
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewUniqueIndex(kparts,c.Name,c.IsEmptyIndex,io)
	case ast.ConstraintForeignKey:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var refer *AttributeReference = nil
		if c.Refer != nil{
			refer = transformReferenceDefToAttributeReference(c.Refer)
		}
		return NewForeignKey(c.IfNotExists,kparts,c.Name,refer,c.IsEmptyIndex)
	case ast.ConstraintFulltext:
		var kparts []*KeyPart = transformIndexPartSpecificationArrayToKeyPartArray(c.Keys)
		var io *IndexOption = nil
		if c.Option != nil{
			io = transformIndexOptionToIndexOption(c.Option)
		}
		return NewFullTextIndex(kparts,c.Name,c.IsEmptyIndex,io)
	case ast.ConstraintCheck:
		e := transformExprNodeToExpr(c.Expr)
		return NewCheckIndex(e,c.Enforced)
	case ast.ConstraintNoConstraint:
		fallthrough
	case ast.ConstraintKey:
		fallthrough
	case ast.ConstraintUniqKey:
		fallthrough
	case ast.ConstraintUniqIndex:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported constraint %v",c.Tp))
	}
	return nil
}

//transform ast.RowFormat to tree.RowFormatType
func transformRowFormatToRowFormatType(rf uint64)RowFormatType{
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
		panic(fmt.Errorf("unsupported row format %v",rf))
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
		return NewTableOptionStatsAutoRecalc(to.UintValue,to.Default)
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
		return NewTableOptionStatsSamplePages(to.UintValue,to.Default)
	case ast.TableOptionSecondaryEngine:
		return NewTableOptionSecondaryEngine(to.StrValue)
	case ast.TableOptionSecondaryEngineNull:
		return NewTableOptionSecondaryEngineNull()
	case ast.TableOptionUnion:
		var name []*TableName = make([]*TableName,len(to.TableNames))
		for i,tn := range to.TableNames{
			name[i] = transformTableNameToTableName(tn)
		}
		return NewTableOptionUnion(name)
	case ast.TableOptionEncryption:
		return NewTableOptionEncryption(to.StrValue)
	case ast.TableOptionNone://mysql 8.0 does not have it
	case ast.TableOptionAutoIdCache://mysql 8.0 does not have it
	case ast.TableOptionAutoRandomBase://mysql 8.0 does not have it
	case ast.TableOptionShardRowID://mysql 8.0 does not have it
	case ast.TableOptionPreSplitRegion://mysql 8.0 does not have it
	case ast.TableOptionNodegroup://mysql 8.0 does not have it
	case ast.TableOptionInsertMethod://mysql 8.0 does not have it
	case ast.TableOptionTableCheckSum://mysql 8.0 does not have it
	default:
		panic(fmt.Errorf("unsupported table option"))
	}
	return nil
}

//transform ast.PartitionMethod to tree.PartitionBy
func transformPartitionMethodToPartitionBy(pm *ast.PartitionMethod)*PartitionBy{
	var partType PartitionType
	switch pm.Tp {
	case model.PartitionTypeRange:
		var e Expr = nil
		var c []*UnresolvedName = nil
		if pm.Expr != nil{
			e = transformExprNodeToExpr(pm.Expr)
		}
		if pm.ColumnNames != nil{
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewRangeType(e,c)
	case model.PartitionTypeHash:
		var e Expr = nil
		if pm.Expr != nil{
			e = transformExprNodeToExpr(pm.Expr)
		}
		partType = NewHashType(pm.Linear,e)
	case model.PartitionTypeList:
		var e Expr = nil
		var c []*UnresolvedName = nil
		if pm.Expr != nil{
			e = transformExprNodeToExpr(pm.Expr)
		}
		if pm.ColumnNames != nil{
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewListType(e,c)
	case model.PartitionTypeKey:
		var c []*UnresolvedName = nil
		if pm.ColumnNames != nil{
			c = transformColumnNameListToUnresolvedNameList(pm.ColumnNames)
		}
		partType = NewKeyType(pm.Linear,c)
	default:
		panic(fmt.Errorf("unsupported partition type %v",pm.Tp))
	}
	return NewPartitionBy(partType,pm.Num)
}

//transform ast.PartitionDefinitionClause to tree.Values
func transformPartitionDefinitionClauseToValues(pdc ast.PartitionDefinitionClause) Values {
	switch n := pdc.(type) {
	case *ast.PartitionDefinitionClauseLessThan:
		var el Exprs = make([]Expr,len(n.Exprs))
		for i,e := range n.Exprs{
			el[i] = transformExprNodeToExpr(e)
		}
		return NewValuesLessThan(el)
	case *ast.PartitionDefinitionClauseIn:
		var el []Exprs = make([]Exprs,len(n.Values))
		for i,v := range n.Values{
			el[i] = make([]Expr,len(v))
			for j,vv := range v{
				el[i][j] = transformExprNodeToExpr(vv)
			}
		}
		return NewValuesIn(el)
	case *ast.PartitionDefinitionClauseNone:
	case *ast.PartitionDefinitionClauseHistory:
	default:
		panic(fmt.Errorf("unsupported PartitionDefinitionClause %v ",n))
	}
	return nil
}

//transform ast.SubPartitionDefinition to tree.SubPartition
func transformSubPartitionDefinitionToSubPartition(spd *ast.SubPartitionDefinition)*SubPartition  {
	var options []TableOption = nil
	if spd.Options != nil{
		options = make([]TableOption,len(spd.Options))
		for i,o := range spd.Options{
			options[i] = transformTableOptionToTableOption(o)
		}
	}
	return NewSubPartition(Identifier(spd.Name.O),options)
}

//transform ast.PartitionDefinition to tree.Partition
func transformPartitionDefinitionToPartition(pd *ast.PartitionDefinition) *Partition {
	name := Identifier(pd.Name.O)
	var values Values = nil
	if pd.Clause != nil {
		values = transformPartitionDefinitionClauseToValues(pd.Clause)
	}

	var options []TableOption = nil
	if pd.Options != nil{
		options = make([]TableOption,len(pd.Options))
		for i,o := range pd.Options{
			options[i] = transformTableOptionToTableOption(o)
		}
	}

	var subs []*SubPartition = nil
	if pd.Sub != nil {
		subs = make([]*SubPartition,len(pd.Sub))
		for i,s := range pd.Sub{
			subs[i] = transformSubPartitionDefinitionToSubPartition(s)
		}
	}

	return NewPartition(name,values,options,subs)
}

//transform ast.PartitionOptions to tree.PartitionOption
func transformPartitionOptionsToPartitionOption(po *ast.PartitionOptions)*PartitionOption{
	var partBy *PartitionBy = transformPartitionMethodToPartitionBy(&po.PartitionMethod)
	var subPartBy *PartitionBy = nil
	if po.Sub != nil{
		subPartBy = transformPartitionMethodToPartitionBy(po.Sub)
	}

	var parts []*Partition = nil
	if po.Definitions != nil{
		parts = make([]*Partition,len(po.Definitions))
		for i,d := range po.Definitions{
			parts[i] = transformPartitionDefinitionToPartition(d)
		}
	}

	return NewPartitionOption(partBy,subPartBy,parts)
}

//transform ast.CreateTableStmt to tree.CreateTable
func transformCreateTableStmtToCreateTable(cts *ast.CreateTableStmt)*CreateTable{
	table := transformTableNameToTableName(cts.Table)
	var defs TableDefs = make([]TableDef,len(cts.Cols) + len(cts.Constraints))
	for i,col := range cts.Cols{
		defs[i] = transformColumnDefToTableDef(col)
	}

	for i,ct := range cts.Constraints{
		defs[i + len(cts.Cols)] = transformConstraintToIndexTableDef(ct)
	}

	var options []TableOption = nil
	if cts.Options != nil{
		options = make([]TableOption,len(cts.Options))
		for i,to := range cts.Options{
			options[i] = transformTableOptionToTableOption(to)
		}
	}

	var partition *PartitionOption = nil
	if cts.Partition != nil{
		partition = transformPartitionOptionsToPartitionOption(cts.Partition)
	}

	return &CreateTable{
		IfNotExists: cts.IfNotExists,
		Table:       *table,
		Defs:        defs,
		Options: options,
		PartitionOption: partition,
	}
}