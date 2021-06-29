package build

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildFrom(stmts tree.TableExprs) (op.OP, error) {
	r, err := b.buildFromTable(stmts[0])
	if err != nil {
		return nil, err
	}
	if stmts = stmts[1:]; len(stmts) == 0 {
		return r, nil
	}
	s, err := b.buildFrom(stmts)
	if err != nil {
		return nil, err
	}
	if err := b.checkProduct(r, s); err != nil {
		return nil, err
	}
	return product.New(r, s), nil
}

func (b *build) buildFromTable(stmt tree.TableExpr) (op.OP, error) {
	switch stmt := stmt.(type) {
	case *tree.AliasedTableExpr:
		o, err := b.buildFromTable(stmt.Expr)
		if err != nil {
			return nil, err
		}
		if len(stmt.As.Alias) > 0 {
			o.Rename(string(stmt.As.Alias))
		}
		return o, nil
	case *tree.JoinTableExpr:
		if stmt.Right == nil {
			return b.buildFromTable(stmt.Left)
		}
		return b.buildJoin(stmt)
	case *tree.TableName:
		return b.buildTable(stmt)
	case *tree.ParenTableExpr:
		return b.buildFromTable(stmt.Expr)
	case *tree.Subquery:
		return b.buildSelectStatement(stmt.Select)
	case *tree.StatementSource:
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown table expr: %T", stmt))
	}
	return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown table expr: %T", stmt))
}
