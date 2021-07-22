package rewrite

import (
	"matrixone/pkg/sql/tree"
)

func rewriteSelect(stmt *tree.Select) *tree.Select {
	if sel, ok := stmt.Select.(*tree.SelectClause); ok {
		stmt.Select = rewriteSelectCluase(sel)
	}
	return stmt
}

func rewriteSelectCluase(stmt *tree.SelectClause) *tree.SelectClause {
	stmt.Exprs = rewriteProjection(stmt.Exprs)
	stmt.From.Tables = rewriteFrom(stmt.From.Tables)
	return stmt
}

func rewriteFrom(stmts tree.TableExprs) tree.TableExprs {
	for i, stmt := range stmts {
		stmts[i] = rewriteTable(stmt)
	}
	return stmts
}

func rewriteTable(stmt tree.TableExpr) tree.TableExpr {
	switch stmt := stmt.(type) {
	case *tree.TableName:
		return stmt
	case *tree.JoinTableExpr:
		if stmt.Right == nil {
			return rewriteTable(stmt.Left)
		}
		stmt.Left = rewriteTable(stmt.Left)
		stmt.Right = rewriteTable(stmt.Right)
		return stmt
	case *tree.ParenTableExpr:
		stmt.Expr = rewriteTable(stmt.Expr)
		return stmt
	case *tree.AliasedTableExpr:
		if len(stmt.As.Alias) == 0 {
			return rewriteTable(stmt.Expr)
		}
		stmt.Expr = rewriteTable(stmt.Expr)
		return stmt
	}
	return stmt
}
