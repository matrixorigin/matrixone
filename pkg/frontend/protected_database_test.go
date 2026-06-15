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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testTableName(dbName, tableName string) tree.TableName {
	return tree.NewUnresolvedObjectName(dbName, tableName).ToTableName()
}

func TestProtectedDatabaseWriteTargetsFromClone(t *testing.T) {
	cloneTable := tree.NewCloneTable()
	cloneTable.CreateTable.Table = testTableName("dst_db", "dst_tbl")
	cloneTable.SrcTable = testTableName("src_db", "src_tbl")
	require.Equal(t, []string{"dst_db"}, protectedDatabaseWriteTargetsFromStmt(cloneTable))

	cloneDatabase := tree.NewCloneDatabase()
	cloneDatabase.DstDatabase = tree.Identifier("dst_db")
	cloneDatabase.SrcDatabase = tree.Identifier("src_db")
	require.Equal(t, []string{"dst_db"}, protectedDatabaseWriteTargetsFromStmt(cloneDatabase))
}

func TestProtectedDatabaseWriteTargetsFromMultipleObjects(t *testing.T) {
	dropTable := tree.NewDropTable(false, tree.TableNames{
		ptrTo(testTableName("protected_db", "t1")),
		ptrTo(testTableName("normal_db", "t2")),
	})
	require.Equal(t, []string{"protected_db", "normal_db"}, protectedDatabaseWriteTargetsFromStmt(dropTable))

	renameTable := tree.NewRenameTable([]*tree.AlterTable{
		{
			Table: ptrTo(testTableName("src_db", "t")),
			Options: tree.AlterTableOptions{
				tree.NewAlterOptionTableName(tree.NewUnresolvedObjectName("dst_db", "t2")),
			},
		},
	})
	require.Equal(t, []string{"src_db", "dst_db"}, protectedDatabaseWriteTargetsFromStmt(renameTable))
}

func TestProtectedDatabaseWriteTargetsFromRestore(t *testing.T) {
	require.Empty(t, protectedDatabaseWriteTargetsFromStmt(&tree.RestoreSnapShot{
		Level: tree.RESTORELEVELACCOUNT,
	}))
	require.Equal(t, []string{"dst_db"}, protectedDatabaseWriteTargetsFromStmt(&tree.RestoreSnapShot{
		Level:        tree.RESTORELEVELDATABASE,
		DatabaseName: tree.Identifier("dst_db"),
	}))
}

func TestPrivilegeTipWritesDatabase(t *testing.T) {
	require.False(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeSelect}))
	require.False(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeValues}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeInsert}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeUpdate}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeDelete}))
}

func ptrTo[T any](v T) *T {
	return &v
}
