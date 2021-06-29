package build

import (
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/showDatabases"
	"matrixone/pkg/sql/op/showTables"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildShowTables(stmt *tree.ShowTables) (op.OP, error) {
	if len(stmt.DBName) == 0 {
		stmt.DBName = b.db
	}
	db, err := b.e.Database(stmt.DBName)
	if err != nil {
		return nil, sqlerror.New(errno.InvalidSchemaName, err.Error())
	}
	return showTables.New(db), nil
}

func (b *build) buildShowDatabases(stmt *tree.ShowDatabases) (op.OP, error) {
	return showDatabases.New(b.e), nil
}
