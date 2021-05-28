package tree

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
	"go/constant"
)

//transform test_driver.ValueExpr::Datum to tree.NumVal
//decimal -> ?
//null -> unknown
func transformDatumToNumVal(datum *test_driver.Datum) *NumVal{
	switch datum.Kind() {
	case	test_driver.KindNull          ://go Unknown Value expresses the null value.
		return NewNumVal(constant.MakeUnknown(),"",false)
	case	test_driver.KindInt64         ://include mysql true,false
		return NewNumVal(constant.MakeInt64(datum.GetInt64()),"",false)
	case	test_driver.KindUint64        :
		return NewNumVal(constant.MakeUint64(datum.GetUint64()),"",false)
	case	test_driver.KindFloat32       :
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()),"",false)
	case	test_driver.KindFloat64       ://mysql 1.2E3, 1.2E-3, -1.2E3, -1.2E-3;
		return NewNumVal(constant.MakeFloat64(datum.GetFloat64()),"",false)
	case	test_driver.KindString        :
		return NewNumVal(constant.MakeString(datum.GetString()),"",false)
	case	test_driver.KindBytes         :
		fallthrough
	case	test_driver.KindBinaryLiteral :
		fallthrough
	case	test_driver.KindMysqlDecimal  ://mysql .2, 3.4, -6.78, +9.10
		fallthrough
	case	test_driver.KindMysqlDuration :
		fallthrough
	case	test_driver.KindMysqlEnum     :
		fallthrough
	case	test_driver.KindMysqlBit      :
		fallthrough
	case	test_driver.KindMysqlSet      :
		fallthrough
	case	test_driver.KindMysqlTime     :
		fallthrough
	case	test_driver.KindInterface     :
		fallthrough
	case	test_driver.KindMinNotNull    :
		fallthrough
	case	test_driver.KindMaxValue      :
		fallthrough
	case	test_driver.KindRaw           :
		fallthrough
	case	test_driver.KindMysqlJSON     :
		fallthrough
	default:
		panic("unsupported datum type")
	}
}

//transform ast.UnaryOperationExpr to tree.UnaryExpr
func transformUnaryOperatorExprToUnaryExpr(uoe *ast.UnaryOperationExpr)*UnaryExpr{
	switch uoe.Op {
	case opcode.Minus:
		e:= transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_MINUS,e)
	case opcode.Plus:
		e:= transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_PLUS,e)
	case opcode.BitNeg://~
		e:= transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_TILDE,e)
	case opcode.Not2://!
		e:= transformExprNodeToExpr(uoe.V)
		return NewUnaryExpr(UNARY_MARK,e)

	}
	panic(fmt.Errorf("unsupported unary expr. op:%s ",uoe.Op.String()))
	return nil
}

//transform ast.UnaryOperationExpr to tree.NotExpr
func transformUnaryOperatorExprToNotExpr(uoe *ast.UnaryOperationExpr)*NotExpr{
	switch uoe.Op {
	case opcode.Not://not,!
		e:= transformExprNodeToExpr(uoe.V)
		return NewNotExpr(e)
	}
	panic(fmt.Errorf("unsupported not expr. op:%s ",uoe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.BinaryExpr
func transformBinaryOperationExprToBinaryExpr(boe *ast.BinaryOperationExpr)*BinaryExpr{
	switch boe.Op {
	//math operation
	case opcode.Plus:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(PLUS,l,r)
	case opcode.Minus:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MINUS,l,r)
	case opcode.Mul:
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MULTI,l,r)
	case opcode.Div:// /
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(DIV,l,r)
	case opcode.Mod://%
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(MOD,l,r)
	case opcode.IntDiv:// integer division
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(INTEGER_DIV,l,r)
	//bit wise operation
	case opcode.Or://bit or |
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_OR,l,r)
	case opcode.And://bit and &
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_AND,l,r)
	case opcode.Xor://bit xor ^
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(BIT_XOR,l,r)
	case opcode.LeftShift://<<
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(LEFT_SHIFT,l,r)
	case opcode.RightShift://>>
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewBinaryExpr(RIGHT_SHIFT,l,r)
	//logic operation
	}
	panic(fmt.Errorf("unsupported binary expr. op:%s ",boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.ComparisonExpr
func transformBinaryOperationExprToComparisonExpr(boe *ast.BinaryOperationExpr)*ComparisonExpr{
	switch boe.Op {
	//comparison operation
	case opcode.EQ:// =
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(EQUAL,l,r)
	case opcode.LT:// <
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(LESS_THAN,l,r)
	case opcode.LE:// <=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(LESS_THAN_EQUAL,l,r)
	case opcode.GT:// >
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(GREAT_THAN,l,r)
	case opcode.GE:// >=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(GREAT_THAN_EQUAL,l,r)
	case opcode.NE:// <>,!=
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewComparisonExpr(NOT_EQUAL,l,r)
	}
	panic(fmt.Errorf("unsupported comparison expr. op:%s ",boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.AndExpr
func transformBinaryOperationExprToAndExpr(boe *ast.BinaryOperationExpr)*AndExpr{
	switch boe.Op {
	//logic operation
	case opcode.LogicAnd:// and,&&
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewAndExpr(l,r)
	}
	panic(fmt.Errorf("unsupported and expr. op:%s ",boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.OrExpr
func transformBinaryOperationExprToOrExpr(boe *ast.BinaryOperationExpr)*OrExpr{
	switch boe.Op {
	//logic operation
	case opcode.LogicOr:// or,||
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewOrExpr(l,r)
	}
	panic(fmt.Errorf("unsupported or expr. op:%s ",boe.Op.String()))
	return nil
}

//transform ast.BinaryOperationExpr to tree.XorExpr
func transformBinaryOperationExprToXorExpr(boe *ast.BinaryOperationExpr)*XorExpr{
	switch boe.Op {
	//logic operation
	case opcode.LogicXor:// xor
		l := transformExprNodeToExpr(boe.L)
		r := transformExprNodeToExpr(boe.R)
		return NewXorExpr(l,r)
	}
	panic(fmt.Errorf("unsupported xor expr. op:%s ",boe.Op.String()))
	return nil
}

//transform ast.IsNullExpr to tree.IsNullExpr
func transformIsNullExprToIsNullExpr(ine *ast.IsNullExpr)*IsNullExpr{
	if !ine.Not{
		e:= transformExprNodeToExpr(ine.Expr)
		return NewIsNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is null expr. %v ",ine))
	return nil
}

//transform ast.IsNotNullExpr to tree.IsNotNullExpr
func transformIsNullExprToIsNotNullExpr(ine *ast.IsNullExpr)*IsNotNullExpr{
	if ine.Not{
		e:= transformExprNodeToExpr(ine.Expr)
		return NewIsNotNullExpr(e)
	}
	panic(fmt.Errorf("unsupported is not null expr. %v ",ine))
	return nil
}

//transform ast.PatternInExpr (in expression) to tree.ComparisonExpr.In
func transformPatternInExprToComparisonExprIn(pie *ast.PatternInExpr)*ComparisonExpr{
	e1 := transformExprNodeToExpr(pie.Expr)
	var e2 Expr
	var op ComparisonOp
	if len(pie.List) != 0{
		// => ExprList
		l := &ExprList{
			Exprs: make([]Expr,len(pie.List)),
		}
		for i,x := range pie.List{
			l.Exprs[i] = transformExprNodeToExpr(x)
		}
		e2 = l
	}else if pie.Sel != nil{
		e2 = transformExprNodeToExpr(pie.Sel)
	}

	if pie.Not{
		op = NOT_IN
	}else{
		op = IN
	}

	return NewComparisonExpr(op,e1,e2)
}

//transform ast.PatternLikeExpr (in expression) to tree.ComparisonExpr.LIKE
func transformPatternLikeExprToComparisonExprIn(ple *ast.PatternLikeExpr)*ComparisonExpr{
	e1 := transformExprNodeToExpr(ple.Expr)
	e2 := transformExprNodeToExpr(ple.Pattern)
	//TODO:escape

	var op ComparisonOp

	if ple.Not{
		op = NOT_LIKE
	}else{
		op = LIKE
	}

	return NewComparisonExpr(op,e1,e2)
}

//transform ast.PatternRegexpExpr (in expression) to tree.ComparisonExpr.REG_MATCH
func transformPatternRegexpExprToComparisonExprIn(pre *ast.PatternRegexpExpr)*ComparisonExpr{
	e1 := transformExprNodeToExpr(pre.Expr)
	e2 := transformExprNodeToExpr(pre.Pattern)

	var op ComparisonOp

	if pre.Not{
		op = NOT_REG_MATCH
	}else{
		op = REG_MATCH
	}

	return NewComparisonExpr(op,e1,e2)
}

//transform ast.ResultSetNode to tree.SelectStatement
func transformResultSetNodeToSelectStatement(rsn ast.ResultSetNode)SelectStatement{
	switch n := rsn.(type) {
	case *ast.SelectStmt:
		return transformSelectStmtToSelectStatement(n)
	}
	panic(fmt.Errorf("unsupported resultSetNode"))
	return nil
}

//transform ast.SubqueryExpr to tree.Subquery
func transformSubqueryExprToSubquery(se *ast.SubqueryExpr)*Subquery{
	e:= transformResultSetNodeToSelectStatement(se.Query)
	return NewSubquery(e,se.Exists)
}

//transform ast.ExistsSubqueryExpr to tree.Subquery
func transformExistsSubqueryExprToSubquery(ese *ast.ExistsSubqueryExpr)*Subquery{
	e:= transformExprNodeToExpr(ese.Sel)
	return NewSubquery(e,ese.Not)
}

//transform ast.CompareSubqueryExpr to tree.ComparisonExpr.SubOp
func transformCompareSubqueryExprToSubquery(cse *ast.CompareSubqueryExpr)*ComparisonExpr{
	l := transformExprNodeToExpr(cse.L)
	r := transformExprNodeToExpr(cse.R)
	var subop ComparisonOp

	if cse.All{
		subop = ALL
	}else{
		subop = ANY
	}

	switch cse.Op {
	//comparison operation
	case opcode.EQ:// =
		return NewComparisonExprWithSubop(EQUAL,subop,l,r)
	case opcode.LT:// <
		return NewComparisonExprWithSubop(LESS_THAN,subop,l,r)
	case opcode.LE:// <=
		return NewComparisonExprWithSubop(LESS_THAN_EQUAL,subop,l,r)
	case opcode.GT:// >
		return NewComparisonExprWithSubop(GREAT_THAN,subop,l,r)
	case opcode.GE:// >=
		return NewComparisonExprWithSubop(GREAT_THAN_EQUAL,subop,l,r)
	case opcode.NE:// <>,!=
		return NewComparisonExprWithSubop(NOT_EQUAL,subop,l,r)
	}
	panic(fmt.Errorf("unsupported CompareSubqueryExpr expr. op:%s ",cse.Op.String()))
	return nil
}

//transform ast.ParenthesesExpr to tree.ParenExpr
func transformParenthesesExprToParenExpr(pe *ast.ParenthesesExpr)*ParenExpr{
	e := transformExprNodeToExpr(pe.Expr)
	return NewParenExpr(e)
}

//transform ast.TableName to tree.TableName
func transformTableNameToTableName(tn *ast.TableName)*TableName{
	return NewTableName(Identifier(tn.Name.O),ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      Identifier(tn.Schema.O),
		ExplicitCatalog: false,
		ExplicitSchema:  len(tn.Schema.O) != 0,
	})
}

//transform ast.TableSource to tree.AliasedTableExpr
func transformTableSourceToAliasedTableExpr(ts *ast.TableSource)*AliasedTableExpr{
	te := transformResultSetNodeToTableExpr(ts.Source)
	return NewAliasedTableExpr(te,AliasClause{
		Alias:       Identifier(ts.AsName.O),
	})
}

//transform ast.SelectStmt to tree.StatementSource
func transformSelectStmtToStatementSource(ss *ast.SelectStmt)*StatementSource{
	sts:=transformSelectStmtToSelectStatement(ss)
	return NewStatementSource(sts)
}

//transform ast.ResultSetNode to tree.TableExpr
func transformResultSetNodeToTableExpr(rsn ast.ResultSetNode)TableExpr{
	switch n := rsn.(type) {
	case *ast.SubqueryExpr:
		return transformSubqueryExprToSubquery(n)
	case *ast.Join:
		if n.ExplicitParens{
			return transformJoinToParenTableExpr(n)
		}
		return transformJoinToJoinTableExpr(n)
	case *ast.TableName:
		return transformTableNameToTableName(n)
	case *ast.TableSource:
		return transformTableSourceToAliasedTableExpr(n)
	case *ast.SelectStmt:
		return transformSelectStmtToStatementSource(n)
	}
	panic(fmt.Errorf("unsupported ResultSetNode type:%v ",rsn))
	return nil
}

//transform []*ast.ColumnName to tree.IdentifierList
func transformColumnNameListToNameList(cn []*ast.ColumnName)IdentifierList{
	var l IdentifierList
	for _,x := range cn{
		l=append(l,Identifier(x.Name.O))
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
func transformJoinToJoinTableExpr(j *ast.Join)*JoinTableExpr{
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

	if j.NaturalJoin{
		joinCon = NewNaturalJoinCond()
	}else if j.StraightJoin{
		//TODO:
	}

	if j.ExplicitParens{
		//TODO:
	}

	l := transformResultSetNodeToTableExpr(j.Left)

	if j.Right == nil{
		return NewJoinTableExpr(t,l,nil,joinCon)
	}
	r := transformResultSetNodeToTableExpr(j.Right)


	if j.On != nil{
		onE := transformExprNodeToExpr(j.On.Expr)
		joinCon = NewOnJoinCond(onE)
	}else if j.Using != nil{
		iList := transformColumnNameListToNameList(j.Using)
		joinCon = NewUsingJoinCond(iList)
	}

	return NewJoinTableExpr(t,l,r,joinCon)
}

//transform ast.Join to tree.ParenTableExpr
func transformJoinToParenTableExpr(j *ast.Join)*ParenTableExpr{
	if j.ExplicitParens{
		j.ExplicitParens = false
		jt := transformJoinToJoinTableExpr(j)
		return NewParenTableExpr(jt)
	}
	panic(fmt.Errorf("Need ExplicitParens :%v ",j))
	return nil
}

//transform ast.TableRefsClause to tree.From
func transformTableRefsClauseToFrom(trc *ast.TableRefsClause)*From{
	var te []TableExpr=make([]TableExpr,1)
	t := transformJoinToJoinTableExpr(trc.TableRefs)
	te[0] = t
	return NewFrom(te)
}

//transform ast.ColumnNameExpr to tree.UnresolvedName
func transformColumnNameExprToUnresolvedName(cne *ast.ColumnNameExpr)*UnresolvedName{
	cn:=cne.Name
	ud,_:= NewUnresolvedName(cn.Schema.O,cn.Table.O,cn.Name.O)
	return ud
}

//transform ast.ExprNode to tree.Expr
func transformExprNodeToExpr(node ast.ExprNode)Expr{
	switch n :=node.(type){
	case ast.ValueExpr:
		if ve,ok := n.(*test_driver.ValueExpr); !ok{
			panic("convert to test_driver.ValueExpr failed.")
		}else{
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
			opcode.NE			:
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
		if n.Not{
			return transformIsNullExprToIsNotNullExpr(n)
		}else{
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
	}
	panic(fmt.Errorf("unsupported node %v ",node))
	return nil
}

//transform ast.WildCardField to
func transformWildCardFieldToVarName(wcf *ast.WildCardField)VarName{
	sch := len(wcf.Schema.O) != 0
	tbl := len(wcf.Table.O) != 0
	if sch && tbl{
		//UnresolvedName
		u,_ := NewUnresolvedNameWithStar(wcf.Schema.O,wcf.Table.O)
		return u
	}else if tbl{
		//UnresolvedName
		u,_ := NewUnresolvedNameWithStar(wcf.Table.O)
		return u
	}else{
		//*
		return StarExpr()
	}
}

//transform ast.FieldList to tree.SelectExprs
func transformFieldListToSelectExprs(fl *ast.FieldList)SelectExprs{
	var sea []SelectExpr=make([]SelectExpr,len(fl.Fields))
	for i,se := range fl.Fields{
		var e Expr
		if se.Expr != nil{
			e = transformExprNodeToExpr(se.Expr)
		}else{
			e = transformWildCardFieldToVarName(se.WildCard)
		}

		sea[i].Expr = e
		sea[i].As = UnrestrictedIdentifier(se.AsName.O)
	}
	return sea
}

//transform ast.GroupByClause to tree.GroupBy
func transformGroupByClauseToGroupBy(gbc *ast.GroupByClause)GroupBy{
	var gb []Expr = make([]Expr,len(gbc.Items))
	for i,bi := range gbc.Items{
		gb[i] = transformExprNodeToExpr(bi.Expr)
	}
	return gb
}

//transform ast.ByItem to tree.Order
func transformByItemToOrder(bi *ast.ByItem)*Order{
	e:= transformExprNodeToExpr(bi.Expr)
	var a Direction
	if bi.Desc{
		a = Descending
	}else{
		a = Ascending
	}
	return NewOrder(e,a,bi.NullOrder)
}

//transform ast.OrderByClause to tree.OrderBy
func transformOrderByClauseToOrderBy(obc *ast.OrderByClause)OrderBy{
	var ob []*Order = make([]*Order,len(obc.Items))
	for i,obi := range obc.Items{
		ob[i] = transformByItemToOrder(obi)
	}
	return ob
}

//transform ast.HavingClause to tree.Where
func transformHavingClauseToWhere(hc *ast.HavingClause)*Where{
	e := transformExprNodeToExpr(hc.Expr)
	return NewWhere(e)
}

//transform ast.Limit to tree.Limit
func transformLimitToLimit(l *ast.Limit) *Limit {
	o := transformExprNodeToExpr(l.Offset)
	c := transformExprNodeToExpr(l.Count)
	return NewLimit(o,c)
}

//transform ast.SelectStmt to tree.SelectClause
func transformSelectStmtToSelectClause(ss *ast.SelectStmt)*SelectClause{
	var from *From = nil
	if ss.From != nil{
		from = transformTableRefsClauseToFrom(ss.From)
	}

	var where *Where = nil
	if ss.Where != nil{
		where = NewWhere(transformExprNodeToExpr(ss.Where))
	}

	sea := transformFieldListToSelectExprs(ss.Fields)

	var gb []Expr = nil
	if ss.GroupBy != nil{
		gb = transformGroupByClauseToGroupBy(ss.GroupBy)
	}

	var having *Where = nil
	if ss.Having != nil{
		having = transformHavingClauseToWhere(ss.Having)
	}

	return &SelectClause{
		From:            from,
		Distinct: ss.Distinct,
		Where: where,
		Exprs: sea,
		GroupBy: gb,
		Having: having,
	}
}

//transform ast.SelectStmt to tree.Select
func transformSelectStmtToSelect(ss *ast.SelectStmt)*Select{
	sc := transformSelectStmtToSelectClause(ss)

	var ob []*Order = nil
	if ss.OrderBy != nil{
		ob = transformOrderByClauseToOrderBy(ss.OrderBy)
	}

	var lmt *Limit
	if ss.Limit != nil{
		lmt = transformLimitToLimit(ss.Limit)
	}

	return &Select{
		Select:  sc,
		OrderBy: ob,
		Limit: lmt,
	}
}

//transform ast.SelectStmt(IsInBraces is true) to tree.ParenSelect
func transformSelectStmtToParenSelect(ss *ast.SelectStmt)*ParenSelect{
	if !ss.IsInBraces{
		panic(fmt.Errorf("only in brace"))
	}
	ss.IsInBraces = false
	s := transformSelectStmtToSelect(ss)
	return &ParenSelect{
		Select: s,
	}
}

//transform ast.SelectStmt to tree.SelectStatement
func transformSelectStmtToSelectStatement(ss *ast.SelectStmt)SelectStatement{
	if ss.IsInBraces{
		return transformSelectStmtToParenSelect(ss)
	}else{
		return transformSelectStmtToSelectClause(ss)
	}
}