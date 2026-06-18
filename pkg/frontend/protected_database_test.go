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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testTableName(dbName, tableName string) tree.TableName {
	return tree.NewUnresolvedObjectName(dbName, tableName).ToTableName()
}

func protectedTargetsFromStatement(stmt tree.Statement) []string {
	return determinePrivilegeSetOfStatement(stmt).writeDatabaseTargets
}

func TestProtectedDatabaseSetFromString(t *testing.T) {
	require.Nil(t, protectedDatabaseSetFromString(""))
	require.Nil(t, protectedDatabaseSetFromString(" , , "))
	require.Equal(t, map[string]struct{}{"db1": {}, "CamelDB": {}}, protectedDatabaseSetFromString(" db1, CamelDB "))
}

func TestProtectedDatabaseWriteTargetsFromDataBranchKeepsPrivileges(t *testing.T) {
	tableStmt := &tree.DataBranchCreateTable{}
	tableStmt.CreateTable.Table = testTableName("dst_db", "dst_tbl")
	tablePriv := determinePrivilegeSetOfStatement(tableStmt)
	require.Equal(t, objectTypeTable, tablePriv.objType)
	require.NotEqual(t, privilegeKindNone, tablePriv.kind)
	require.True(t, tablePriv.writeDatabaseAndTableDirectly)
	require.Equal(t, []string{"dst_db"}, tablePriv.writeDatabaseTargets)

	databaseStmt := tree.NewDataBranchCreateDatabase()
	databaseStmt.DstDatabase = tree.Identifier("dst_db")
	databasePriv := determinePrivilegeSetOfStatement(databaseStmt)
	require.Equal(t, objectTypeDatabase, databasePriv.objType)
	require.NotEqual(t, privilegeKindNone, databasePriv.kind)
	require.True(t, databasePriv.writeDatabaseAndTableDirectly)
	require.Equal(t, []string{"dst_db"}, databasePriv.writeDatabaseTargets)
}

func TestProtectedDatabaseWriteTargetsFromClone(t *testing.T) {
	cloneTable := tree.NewCloneTable()
	cloneTable.CreateTable.Table = testTableName("dst_db", "dst_tbl")
	cloneTable.SrcTable = testTableName("src_db", "src_tbl")
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(cloneTable))

	cloneDatabase := tree.NewCloneDatabase()
	cloneDatabase.DstDatabase = tree.Identifier("dst_db")
	cloneDatabase.SrcDatabase = tree.Identifier("src_db")
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(cloneDatabase))
}

func TestProtectedDatabaseWriteTargetsFromMultipleObjects(t *testing.T) {
	dropTable := tree.NewDropTable(false, tree.TableNames{
		ptrTo(testTableName("protected_db", "t1")),
		ptrTo(testTableName("normal_db", "t2")),
	})
	require.Equal(t, []string{"protected_db", "normal_db"}, protectedTargetsFromStatement(dropTable))

	renameTable := tree.NewRenameTable([]*tree.AlterTable{
		{
			Table: ptrTo(testTableName("src_db", "t")),
			Options: tree.AlterTableOptions{
				tree.NewAlterOptionTableName(tree.NewUnresolvedObjectName("dst_db", "t2")),
			},
		},
	})
	require.Equal(t, []string{"src_db", "dst_db"}, protectedTargetsFromStatement(renameTable))
}

func TestProtectedDatabaseWriteTargetsFromRestore(t *testing.T) {
	require.Empty(t, protectedTargetsFromStatement(&tree.RestoreSnapShot{
		Level: tree.RESTORELEVELACCOUNT,
	}))
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(&tree.RestoreSnapShot{
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

func TestProtectedDatabaseNameKeepsOriginalCase(t *testing.T) {
	protectedDatabases := map[string]struct{}{"CamelDB": {}}

	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), nil, protectedDatabases, "CamelDB"))
	require.True(t, checkProtectedDatabaseWriteWithSet(context.Background(), nil, protectedDatabases, "camedb"))
}

func TestCheckProtectedDatabaseWriteWithSet(t *testing.T) {
	ctx := context.Background()
	protectedDatabases := map[string]struct{}{"protected_db": {}, "CamelDB": {}}

	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, nil, "protected_db"))
	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases))
	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases, "normal_db"))
	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases, "normal_db", "protected_db"))
	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases, " CamelDB "))
	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases, "cameldb"))
}

func TestCanWriteProtectedDatabase(t *testing.T) {
	require.False(t, canWriteProtectedDatabase(nil))
	require.False(t, canWriteProtectedDatabase(&Session{}))

	accountAdminSession := &Session{}
	accountAdminSession.SetTenantInfo(&TenantInfo{Tenant: "account1", DefaultRole: accountAdminRoleName})
	require.True(t, canWriteProtectedDatabase(accountAdminSession))

	moAdminSession := &Session{}
	moAdminSession.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName})
	require.True(t, canWriteProtectedDatabase(moAdminSession))

	normalSession := &Session{}
	normalSession.SetTenantInfo(&TenantInfo{Tenant: "account1", DefaultRole: "normal_role"})
	require.False(t, canWriteProtectedDatabase(normalSession))
}

func ptrTo[T any](v T) *T {
	return &v
}
