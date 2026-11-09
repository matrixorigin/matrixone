// Copyright 2026 Matrix Origin
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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestPrepareTableDescription(t *testing.T) {
	for _, keyword := range []string{"desc", "describe", "explain"} {
		for _, tc := range []struct {
			name, target, database, table, column string
		}{
			{name: "unqualified", target: "src", table: "src"},
			{name: "qualified", target: "app.v", database: "app", table: "v"},
			{name: "column", target: "app.src x", database: "app", table: "src", column: "x"},
			{name: "quoted", target: "`odd db`.`odd``table` `odd``column`", database: "odd db", table: "odd`table", column: "odd`column"},
		} {
			t.Run(keyword+"/"+tc.name, func(t *testing.T) {
				query := keyword + " " + tc.target
				direct, err := ParseOne(t.Context(), query, 1)
				require.NoError(t, err)
				defer direct.Free()
				require.IsType(t, &tree.ShowColumns{}, direct)

				stmt, err := ParseOne(t.Context(), "prepare description_stmt from "+query, 1)
				require.NoError(t, err)
				defer stmt.Free()
				require.IsType(t, &tree.PrepareStmt{}, stmt)
				prepared := stmt.(*tree.PrepareStmt)
				require.Equal(t, tree.Identifier("description_stmt"), prepared.Name)
				require.IsType(t, &tree.ShowColumns{}, prepared.Stmt)
				defer prepared.Stmt.Free()
				show := prepared.Stmt.(*tree.ShowColumns)
				require.Equal(t, tc.database, show.Table.GetDBName())
				require.Equal(t, tc.table, show.Table.GetTableName())
				if tc.column == "" {
					require.Nil(t, show.ColName)
				} else {
					require.NotNil(t, show.ColName)
					require.Equal(t, tc.column, show.ColName.ColNameOrigin())
				}
				require.Equal(t, direct, show, "native PREPARE must preserve the direct description AST")
			})
		}
	}
}

func TestPrepareDescriptionAcceptanceBoundary(t *testing.T) {
	for _, query := range []string{
		"select 1", "show columns from app.src", "show full columns from app.v", "explain select 1",
	} {
		t.Run(query, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), "prepare description_stmt from "+query, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.IsType(t, &tree.PrepareStmt{}, stmt)
			stmt.(*tree.PrepareStmt).Stmt.Free()
		})
	}
	for _, query := range []string{
		"", "desc", "describe app.", "desc app.src x y", "explain for connection 1", "explain format='json' for connection 1",
	} {
		t.Run("reject/"+query, func(t *testing.T) {
			_, err := ParseOne(t.Context(), "prepare description_stmt from "+query, 1)
			require.Error(t, err)
		})
	}
}
