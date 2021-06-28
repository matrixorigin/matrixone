package build

import (
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dropDatabase"
	"matrixone/pkg/sql/op/dropTable"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildDropTable(stmt *tree.DropTable) (op.OP, error) {
	dbs, ids := make([]string, len(stmt.Names)), make([]string, len(stmt.Names))
	for i, tbl := range stmt.Names {
		if len(tbl.SchemaName) == 0 {
			dbs[i] = b.db
		} else {
			dbs[i] = string(tbl.SchemaName)
		}
		ids[i] = string(tbl.ObjectName)
	}
	return dropTable.New(stmt.IfExists, dbs, ids, b.e), nil
}

func (b *build) buildDropDatabase(stmt *tree.DropDatabase) (op.OP, error) {
	return dropDatabase.New(stmt.IfExists, string(stmt.Name), b.e), nil
}
