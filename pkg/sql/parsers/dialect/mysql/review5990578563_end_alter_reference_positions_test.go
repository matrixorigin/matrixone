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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A column declared with the unquoted keyword END must stay referenceable by
// the same spelling in every unambiguous single-column ALTER position, with
// the quoted twin interchangeable and each statement binding the concrete
// clause that owns the position. The supported compound-statement boundary
// (a BEGIN ... END block) is positively covered by the existing
// TestParseCompoundStatementList in this package.
func TestEndColumnUsableAcrossAlterPositions(t *testing.T) {
	alter := func(t *testing.T, sql string) *tree.AlterTable {
		t.Helper()
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, "statement must parse: %s", sql)
		alterStmt, ok := stmt.(*tree.AlterTable)
		require.True(t, ok, "statement must bind an ALTER TABLE shape: %s", sql)
		require.Len(t, alterStmt.Options, 1, "one alter option per statement: %s", sql)
		t.Cleanup(func() {
			stmt.Free()
		})
		return alterStmt
	}
	option := func(t *testing.T, sql string) any {
		return alter(t, sql).Options[0]
	}
	// Every unambiguous DROP COLUMN reference carries the same normalized
	// column identity for the bare keyword and its quoted twin.
	dropName := func(t *testing.T, sql string) string {
		drop, ok := option(t, sql).(*tree.AlterOptionDrop)
		require.True(t, ok, "DROP COLUMN must bind the drop clause: %s", sql)
		require.Equal(t, tree.AlterTableDropColumn, drop.Typ, "drop type: %s", sql)
		return string(drop.Name)
	}
	require.Equal(t, dropName(t, "ALTER TABLE end_cycle DROP COLUMN end"),
		dropName(t, "ALTER TABLE end_cycle DROP COLUMN `end`"),
		"the bare END drop and its quoted twin are one column identity")
	require.Equal(t, "end", dropName(t, "ALTER TABLE end_cycle DROP COLUMN end"))
	require.Equal(t, "end", dropName(t, "ALTER TABLE end_cycle DROP `end`"))

	for _, tc := range []struct {
		sql string
		typ interface{}
	}{
		{"ALTER TABLE end_cycle RENAME COLUMN end TO finish", &tree.AlterTableRenameColumnClause{}},
		{"ALTER TABLE end_cycle RENAME COLUMN finish TO end", &tree.AlterTableRenameColumnClause{}},
		{"ALTER TABLE end_cycle CHANGE COLUMN end end2 INT AFTER id", &tree.AlterTableChangeColumnClause{}},
		{"ALTER TABLE end_cycle ALTER COLUMN end SET DEFAULT 7", &tree.AlterTableAlterColumnClause{}},
		{"ALTER TABLE end_cycle ALTER COLUMN `end` SET DEFAULT 7", &tree.AlterTableAlterColumnClause{}},
		{"ALTER TABLE end_cycle ALTER COLUMN end DROP DEFAULT", &tree.AlterTableAlterColumnClause{}},
		{"ALTER TABLE end_cycle ADD COLUMN tail INT AFTER end", &tree.AlterAddCol{}},
	} {
		t.Run("position_binds_concrete_clause", func(t *testing.T) {
			require.IsType(t, tc.typ, option(t, tc.sql), "statement must bind its owning clause: %s", tc.sql)
		})
	}
	rename, ok := option(t, "ALTER TABLE end_cycle RENAME COLUMN end TO finish").(*tree.AlterTableRenameColumnClause)
	require.True(t, ok)
	require.NotNil(t, rename.OldColumnName, "the old column name must be bound")
	require.NotNil(t, rename.NewColumnName, "the new column name must be bound")
	setDefault, ok := option(t, "ALTER TABLE end_cycle ALTER COLUMN end SET DEFAULT 7").(*tree.AlterTableAlterColumnClause)
	require.True(t, ok)
	require.Equal(t, tree.AlterColumnOptionSetDefault, setDefault.OptionType, "SET DEFAULT must bind its option type")
	addAfter, ok := option(t, "ALTER TABLE end_cycle ADD COLUMN tail INT AFTER end").(*tree.AlterAddCol)
	require.True(t, ok)
	require.NotNil(t, addAfter.Position, "AFTER end must bind the positional clause")
}
