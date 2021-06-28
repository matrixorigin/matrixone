package build

import (
	"fmt"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/explain"
	"matrixone/pkg/sql/tree"
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
	return nil, fmt.Errorf("unsupport explain statement: '%v'", stmt)
}
