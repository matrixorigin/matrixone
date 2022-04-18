package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func getColumnIndex(tableDef *plan.TableDef, name string) int32 {
	for idx, col := range tableDef.Cols {
		if col.Name == name {
			return int32(idx)
		}
	}
	return -1
}

func buildPlan(ctx CompilerContext, stmt tree.Statement) (*Query, error) {
	query := &Query{}
	err := buildStatement(stmt, ctx, query)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func buildStatement(stmt tree.Statement, ctx CompilerContext, query *Query) error {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return buildSelect(stmt, ctx, query)
	case *tree.ParenSelect:
		return buildSelect(stmt.Select, ctx, query)
	case *tree.Insert:
		return buildInsert(stmt, ctx, query)
	case *tree.Update:
		return buildUpdate(stmt, ctx, query)
	case *tree.Delete:
		return buildDelete(stmt, ctx, query)
	}
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func buildUpdate(stmt *tree.Update, ctx CompilerContext, query *Query) error {
	return nil
}

func buildDelete(stmt *tree.Delete, ctx CompilerContext, query *Query) error {
	return nil
}
