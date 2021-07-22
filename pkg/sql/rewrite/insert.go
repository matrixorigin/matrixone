package rewrite

import "matrixone/pkg/sql/tree"

func rewriteInsert(stmt *tree.Insert) *tree.Insert {
	jtbl, ok := stmt.Table.(*tree.JoinTableExpr)
	if !ok {
		return stmt
	}
	atbl, ok := jtbl.Left.(*tree.AliasedTableExpr)
	if !ok {
		return stmt
	}
	tbl, ok := atbl.Expr.(*tree.TableName)
	if !ok {
		return stmt
	}
	stmt.Table = tbl
	return stmt
}
