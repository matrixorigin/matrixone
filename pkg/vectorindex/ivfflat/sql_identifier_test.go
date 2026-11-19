// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestVersionSQLKeepsConfiguredIdentifiersAsData(t *testing.T) {
	cfg := vectorindex.IndexTableConfig{DbName: "db`name", MetadataTable: "meta` WHERE true; DELETE FROM victim -- "}
	stmts, err := mysql.Parse(context.Background(), versionSQL(cfg), 0)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	clause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	require.Len(t, clause.From.Tables, 1)
	var table tree.TableExpr = clause.From.Tables[0]
	for {
		switch node := table.(type) {
		case *tree.JoinTableExpr:
			table = node.Left
		case *tree.AliasedTableExpr:
			table = node.Expr
		default:
			name := table.(*tree.TableName)
			require.Equal(t, cfg.DbName, string(name.SchemaName))
			require.Equal(t, cfg.MetadataTable, string(name.ObjectName))
			return
		}
	}
}
