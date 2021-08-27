package build

import (
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildTable(stmt *tree.TableName) (op.OP, error) {
	return b.getTable(string(stmt.ObjectName))
}

func (b *build) getTable(name string) (op.OP, error) {
	r, err := b.e.Relation(name)
	if err != nil {
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	return relation.New(name, r), nil
}
