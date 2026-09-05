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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBranchRequirementToPrivilegeCopiesLightPrivilegeFields(t *testing.T) {
	req := branchWriteTableRequirement(
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		PrivilegeTypeDelete,
		clusterTableModify,
	)

	priv := branchRequirementToPrivilege(req)

	require.Equal(t, privilegeKindGeneral, priv.kind)
	require.Equal(t, objectTypeTable, priv.objType)
	require.True(t, priv.writeDatabaseAndTableDirectly)
	require.Equal(t, req.isClusterTable, priv.isClusterTable)
	require.Equal(t, clusterTableModify, priv.clusterTableOperation)
	require.Len(t, priv.entries, 3)

	require.Equal(t, []PrivilegeType{
		PrivilegeTypeDelete,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
	}, []PrivilegeType{
		priv.entries[0].privilegeId,
		priv.entries[1].privilegeId,
		priv.entries[2].privilegeId,
	})
	for _, entry := range priv.entries {
		require.Equal(t, objectTypeTable, entry.objType)
		require.Equal(t, catalog.MO_CATALOG, entry.databaseName)
		require.Equal(t, catalog.MO_TABLES, entry.tableName)
		require.Equal(t, privilegeEntryTypeGeneral, entry.privilegeEntryTyp)
		require.Nil(t, entry.compound)
	}
}

func TestLockDataBranchTargetAccount(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()

	require.NoError(t, lockDataBranchTargetAccount(context.Background(), bh, nil))
	require.Empty(t, bh.executedSQLs)

	toAccount := &tree.ToAccountOpt{AccountName: tree.Identifier("target_account")}
	expectedSQL, err := getSqlForLockMoAccountNameFormat(context.Background(), toAccount.AccountName.String())
	require.NoError(t, err)
	require.NoError(t, lockDataBranchTargetAccount(context.Background(), bh, toAccount))
	require.Equal(t, []string{expectedSQL}, bh.executedSQLs)
}

func TestBranchDeleteDatabaseTableIDsSQLReusesCloneObjectFilter(t *testing.T) {
	const (
		accountID uint32 = 42
		dbName           = "db1"
	)
	expectedWhere := buildTableInfoListWhereClause(dbName, "", accountID) +
		fmt.Sprintf(" and relkind != %s", quoteSQLStringLiteral(catalog.SystemSequenceRel)) +
		fmt.Sprintf(" and relkind != %s", quoteSQLStringLiteral(catalog.SystemViewRel))
	expected := fmt.Sprintf(
		"select rel_id, relname from %s.%s where %s",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		expectedWhere,
	)

	got := branchDeleteDatabaseTableIDsSQL(accountID, dbName)

	require.Equal(t, expected, got)
	require.Contains(t, got, "relkind not in (")
	require.Contains(t, got, "'i'")
	require.Contains(t, got, "'fulltext'")
	require.Contains(t, got, "'metadata'")
	require.Contains(t, got, "'hnsw_meta'")
	require.Contains(t, got, "relkind = 'temporary_table'")
	require.Contains(t, got, "mo_is_legacy_temporary_table(coalesce(relkind, ''), coalesce(relname, ''), coalesce(reldatabase, ''), coalesce(rel_createsql, ''), coalesce(extra_info, ''))")
	require.Contains(t, got, "coalesce(relkind, '') not in ('r', 'v', 'e', 'm', 's', 'cluster', 'partition', 'S') and regexp_like(relname, '^__mo_tmp_[0-9a-f]{32}_')")
	require.Contains(t, got, "relkind != 'partition'")
	require.Contains(t, got, "relkind != 'S'")
	require.Contains(t, got, "relkind != 'v'")
	require.NotContains(t, got, "relname != 'mo_increment_columns'")
	require.NotContains(t, got, "relname != '__mo_account_lock'")
	require.NotContains(t, got, "relname not like")
}

func TestValidateDataBranchDeleteDatabaseTargetUsesDatabaseIdentity(t *testing.T) {
	const (
		accountID uint32 = 42
		dbName           = "db1"
		tableID   uint64 = 101
	)
	ctx := defines.AttachAccountId(context.Background(), accountID)

	tests := []struct {
		name         string
		databaseType string
		tables       [][]interface{}
		activeIDs    [][]interface{}
		wantIDs      []uint64
		wantErr      string
		wantSQLCount int
	}{
		{
			name:         "marked empty database",
			databaseType: catalog.SystemDBTypeDataBranch,
			wantIDs:      []uint64{},
			wantSQLCount: 2,
		},
		{
			name:         "unmarked empty database",
			wantErr:      "not an active branch database",
			wantSQLCount: 2,
		},
		{
			name:         "subscription database",
			databaseType: catalog.SystemDBTypeSubscription,
			wantErr:      "not an active branch database",
			wantSQLCount: 1,
		},
		{
			name:         "unknown database type",
			databaseType: "unknown",
			wantErr:      "not an active branch database",
			wantSQLCount: 1,
		},
		{
			name:         "marked database with ordinary table",
			databaseType: catalog.SystemDBTypeDataBranch,
			tables:       [][]interface{}{{int64(tableID), "local_t"}},
			wantErr:      "not an active branch table",
			wantSQLCount: 3,
		},
		{
			name:         "marked database with branch table",
			databaseType: catalog.SystemDBTypeDataBranch,
			tables:       [][]interface{}{{int64(tableID), "branch_t"}},
			activeIDs:    [][]interface{}{{int64(tableID)}},
			wantIDs:      []uint64{tableID},
			wantSQLCount: 3,
		},
		{
			name:         "legacy database with branch table",
			tables:       [][]interface{}{{int64(tableID), "branch_t"}},
			activeIDs:    [][]interface{}{{int64(tableID)}},
			wantIDs:      []uint64{tableID},
			wantSQLCount: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ses := newValidateSession(t)
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[branchDatabaseTypeSQL(accountID, dbName)] = branchStringResult(
				"dat_type", [][]interface{}{{test.databaseType}},
			)
			bh.sql2result[branchDeleteDatabaseTableIDsSQL(accountID, dbName)] = branchTableResult(test.tables)
			activeSQL := fmt.Sprintf(
				"select table_id from %s.%s where table_deleted = false and level != 'alter' and table_id in (%d)",
				catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, tableID,
			)
			bh.sql2result[activeSQL] = branchUint64Result("table_id", test.activeIDs)

			ids, err := validateDataBranchDeleteDatabaseTarget(ctx, ses, bh, dbName)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Nil(t, ids)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantIDs, ids)
			}
			require.Len(t, bh.executedSQLs, test.wantSQLCount)
		})
	}
}

func TestLoadBranchDatabaseTypeRejectsMissingDatabase(t *testing.T) {
	const (
		accountID uint32 = 42
		dbName           = "missing_db"
	)
	ctx := defines.AttachAccountId(context.Background(), accountID)
	ses := newValidateSession(t)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[branchDatabaseTypeSQL(accountID, dbName)] = branchStringResult("dat_type", nil)

	_, err := loadBranchDatabaseType(ctx, ses, bh, accountID, dbName)
	require.ErrorContains(t, err, "Unknown database")
}

func branchStringResult(columnName string, rows [][]interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName(columnName)
	column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	result.AddColumn(column)
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func branchUint64Result(columnName string, rows [][]interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName(columnName)
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	column.SetSigned(false)
	result.AddColumn(column)
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func branchTableResult(rows [][]interface{}) *MysqlResultSet {
	result := branchUint64Result("rel_id", nil)
	nameColumn := &MysqlColumn{}
	nameColumn.SetName("relname")
	nameColumn.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	result.AddColumn(nameColumn)
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func TestBuildTableInfoListWhereClauseUsesRelationKindForInternalObjects(t *testing.T) {
	got := buildTableInfoListWhereClause("db1", "", 42)

	require.Contains(t, got, "relkind not in (")
	require.Contains(t, got, "'i'")
	require.Contains(t, got, "'fulltext'")
	require.Contains(t, got, "'metadata'")
	require.Contains(t, got, "'hnsw_meta'")
	require.Contains(t, got, "relkind = 'temporary_table'")
	require.Contains(t, got, "mo_is_legacy_temporary_table(coalesce(relkind, ''), coalesce(relname, ''), coalesce(reldatabase, ''), coalesce(rel_createsql, ''), coalesce(extra_info, ''))")
	require.Contains(t, got, "coalesce(relkind, '') not in ('r', 'v', 'e', 'm', 's', 'cluster', 'partition', 'S') and regexp_like(relname, '^__mo_tmp_[0-9a-f]{32}_')")
	require.NotContains(t, got, catalog.MOAutoIncrTable)
	require.NotContains(t, got, catalog.MO_ACCOUNT_LOCK)
	require.NotContains(t, got, "relname not like '__mo_tmp_%'")

	systemCatalog := buildTableInfoListWhereClause(catalog.MO_CATALOG, "", 0)
	require.Contains(t, systemCatalog, "relname != '"+catalog.MOAutoIncrTable+"'")
	require.Contains(t, systemCatalog, "relname != '"+catalog.MO_ACCOUNT_LOCK+"'")
	require.Contains(t, systemCatalog, `relname not like '\\_\\_mo\\_index\\_%' escape '\\'`)
}

func TestQuoteIdentifierForSQLEscapesBackticks(t *testing.T) {
	require.Equal(t, "`acc``branch`", quoteIdentifierForSQL("acc`branch"))
}

func TestValidateDataBranchDiffOutputAs(t *testing.T) {
	newStmt := func(atTsExpr *tree.AtTimeStamp) *tree.DataBranchDiff {
		return &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{
				As: *tree.NewTableName(
					tree.Identifier("diff_out"),
					tree.ObjectNamePrefix{},
					atTsExpr,
				),
			},
		}
	}

	t.Run("accepts ordinary destination", func(t *testing.T) {
		require.NoError(t, validate(context.Background(), nil, newStmt(nil)))
	})

	t.Run("rejects destination snapshot", func(t *testing.T) {
		err := validate(context.Background(), nil, newStmt(&tree.AtTimeStamp{SnapshotName: "sp"}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination snapshot option is not supported")
	})
}
