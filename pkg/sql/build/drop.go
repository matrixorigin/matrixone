// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
