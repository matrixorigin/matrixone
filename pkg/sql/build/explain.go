package build

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/explain"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildExplain(stmt tree.Explain) (op.OP, error) {
	switch n := stmt.(type) {
	case *tree.ExplainStmt:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	case *tree.ExplainAnalyze:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	case *tree.ExplainFor:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	}
	return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport explain statement: '%v'", stmt))
}
