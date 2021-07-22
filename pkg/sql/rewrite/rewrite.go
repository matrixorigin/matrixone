package rewrite

import (
	"matrixone/pkg/sql/tree"
)

func Rewrite(stmt tree.Statement) tree.Statement {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return rewriteSelect(stmt)
	case *tree.ParenSelect:
		stmt.Select = rewriteSelect(stmt.Select)
		return stmt
	case *tree.Insert:
		return rewriteInsert(stmt)
	}
	return stmt
}
