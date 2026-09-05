// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 34)
	require.Len(t, clusterUpgEntries, 11)
	require.Equal(t, retireKafkaSinkDaemonTasks.UpgSql, clusterUpgEntries[0].UpgSql)
	require.Equal(t, catalog.MO_VIEW_DEPENDENCIES, clusterUpgEntries[1].TableName)
	require.Equal(t, catalog.MO_VIEW_REFRESH, clusterUpgEntries[2].TableName)
	for _, entry := range clusterUpgEntries[1:3] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create cluster table mo_catalog.mo_view_")
	}
	for _, tc := range []struct {
		entry     versions.UpgradeEntry
		tableName string
		indexName string
		column    string
	}{
		{clusterUpgEntries[3], catalog.MOSQLTask, "idx_account_id", "account_id"},
		{clusterUpgEntries[4], catalog.MOSQLTaskRun, "idx_account_id", "account_id"},
		{clusterUpgEntries[5], catalog.MOSysAsyncTask, "idx_task_parent_id", "task_parent_id"},
	} {
		require.Equal(t, tc.tableName, tc.entry.TableName)
		require.Equal(t, versions.ADD_INDEX, tc.entry.UpgType)
		require.Equal(t,
			fmt.Sprintf("create index %s on %s.%s(%s)", tc.indexName, catalog.MOTaskDB, tc.tableName, tc.column),
			tc.entry.UpgSql)
	}
	require.Equal(t, cleanupLegacyOrphanSQLTaskChildren.UpgSql, clusterUpgEntries[6].UpgSql)
	require.Equal(t, versions.MODIFY_METADATA, clusterUpgEntries[6].UpgType)
	require.Equal(t, int64(defines.MORPCVersion42), clusterUpgEntries[6].RequiredProtocolVersion)
	require.Equal(t, catalog.MO_CDC_SNAPSHOT, clusterUpgEntries[7].TableName)
	require.Equal(t, versions.CREATE_NEW_TABLE, clusterUpgEntries[7].UpgType)
	require.Equal(t, frontend.MoCatalogMoCdcSnapshotDDL, clusterUpgEntries[7].UpgSql)
	require.Equal(t, catalog.MO_CDC_WATERMARK, clusterUpgEntries[8].TableName)
	require.Equal(t, versions.ADD_COLUMN, clusterUpgEntries[8].UpgType)
	require.Contains(t, clusterUpgEntries[8].UpgSql, "source_table_id bigint unsigned not null default 0")
	require.Equal(t, int64(defines.MORPCVersion48), clusterUpgEntries[8].RequiredProtocolVersion)
	require.Equal(t, catalog.MO_CDC_WATERMARK, clusterUpgEntries[9].TableName)
	require.Equal(t, versions.ADD_COLUMN, clusterUpgEntries[9].UpgType)
	require.Contains(t, clusterUpgEntries[9].UpgSql, "owner_generation bigint unsigned not null default 0")
	require.Equal(t, int64(defines.MORPCVersion48), clusterUpgEntries[9].RequiredProtocolVersion)
	require.Equal(t, upgradeDaemonClaimPrecision.UpgSql, clusterUpgEntries[10].UpgSql)
	require.Equal(t, int64(defines.MORPCVersion48), clusterUpgEntries[10].RequiredProtocolVersion)
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries[:2] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}
	characterSetsTable := tenantUpgEntries[7]
	require.Equal(t, sysview.InformationDBConst, characterSetsTable.Schema)
	require.Equal(t, "CHARACTER_SETS", characterSetsTable.TableName)
	require.Equal(t, versions.CREATE_NEW_TABLE, characterSetsTable.UpgType)
	require.Equal(t, sysview.InformationSchemaCharacterSetsDDL, characterSetsTable.UpgSql)
	collations := tenantUpgEntries[8]
	require.Equal(t, sysview.InformationDBConst, collations.Schema)
	require.Equal(t, "COLLATIONS", collations.TableName)
	require.Equal(t, versions.MODIFY_METADATA, collations.UpgType)
	require.Equal(t, sysview.InformationSchemaCollationsData, collations.UpgSql)
	require.Contains(t, strings.ToLower(collations.PreSql), "delete from information_schema.collations")
	characterSets := tenantUpgEntries[9]
	require.Equal(t, sysview.InformationDBConst, characterSets.Schema)
	require.Equal(t, "CHARACTER_SETS", characterSets.TableName)
	require.Equal(t, versions.MODIFY_METADATA, characterSets.UpgType)
	require.Equal(t, sysview.InformationSchemaCharacterSetsData, characterSets.UpgSql)
	require.Contains(t, strings.ToLower(characterSets.PreSql), "delete from information_schema.character_sets")
	columns := tenantUpgEntries[10]
	require.Equal(t, sysview.InformationDBConst, columns.Schema)
	require.Equal(t, "COLUMNS", columns.TableName)
	require.Equal(t, versions.MODIFY_VIEW, columns.UpgType)
	require.Equal(t, sysview.InformationSchemaColumnsDDL, columns.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion46), columns.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(columns.PreSql), "drop view if exists information_schema.columns")
	checkConstraints := tenantUpgEntries[11]
	require.Equal(t, sysview.InformationDBConst, checkConstraints.Schema)
	require.Equal(t, "CHECK_CONSTRAINTS", checkConstraints.TableName)
	require.Equal(t, versions.CREATE_VIEW, checkConstraints.UpgType)
	require.Equal(t, sysview.InformationSchemaCheckConstraintsDDL, checkConstraints.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion41), checkConstraints.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(checkConstraints.PreSql), "drop view if exists information_schema.check_constraints")
	tableConstraints := tenantUpgEntries[12]
	require.Equal(t, sysview.InformationDBConst, tableConstraints.Schema)
	require.Equal(t, "TABLE_CONSTRAINTS", tableConstraints.TableName)
	require.Equal(t, versions.MODIFY_VIEW, tableConstraints.UpgType)
	require.Equal(t, sysview.InformationSchemaTableConstraintsDDL, tableConstraints.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion41), tableConstraints.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(tableConstraints.PreSql), "drop view if exists information_schema.table_constraints")
	hideInternalColumns := tenantUpgEntries[13]
	require.Equal(t, sysview.InformationDBConst, hideInternalColumns.Schema)
	require.Equal(t, "COLUMNS", hideInternalColumns.TableName)
	require.Equal(t, versions.MODIFY_VIEW, hideInternalColumns.UpgType)
	require.Equal(t, sysview.InformationSchemaColumnsDDL, hideInternalColumns.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion46), hideInternalColumns.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(hideInternalColumns.PreSql), "drop view if exists information_schema.columns")
	userDefinedFunctions := tenantUpgEntries[14]
	require.Equal(t, versions.DROP_INDEX, userDefinedFunctions.UpgType)
	require.Equal(t, catalog.MO_CATALOG, userDefinedFunctions.Schema)
	require.Equal(t, "mo_user_defined_function", userDefinedFunctions.TableName)
	require.Contains(t, strings.ToLower(userDefinedFunctions.UpgSql), "drop index name")
	userDefinedFunctionArgumentTypes := tenantUpgEntries[15]
	require.Equal(t, versions.ADD_COLUMN, userDefinedFunctionArgumentTypes.UpgType)
	require.Equal(t, catalog.MO_CATALOG, userDefinedFunctionArgumentTypes.Schema)
	require.Equal(t, "mo_user_defined_function", userDefinedFunctionArgumentTypes.TableName)
	require.Contains(t, strings.ToLower(userDefinedFunctionArgumentTypes.UpgSql), "arg_types")
	require.Contains(t, userDefinedFunctionArgumentTypes.UpgSql, "varchar(65535)")
	userDefinedFunctionBackfill := tenantUpgEntries[16]
	require.Equal(t, versions.MODIFY_METADATA, userDefinedFunctionBackfill.UpgType)
	require.Equal(t, catalog.MO_CATALOG, userDefinedFunctionBackfill.Schema)
	require.Equal(t, "mo_user_defined_function", userDefinedFunctionBackfill.TableName)
	require.Equal(t,
		"update mo_catalog.mo_user_defined_function set arg_types = "+catalog.UserDefinedFunctionArgumentTypesSQL,
		userDefinedFunctionBackfill.UpgSql,
	)
	userDefinedFunctionSignatureIndex := tenantUpgEntries[17]
	require.Equal(t, versions.ADD_INDEX, userDefinedFunctionSignatureIndex.UpgType)
	require.Equal(t, catalog.MO_CATALOG, userDefinedFunctionSignatureIndex.Schema)
	require.Equal(t, "mo_user_defined_function", userDefinedFunctionSignatureIndex.TableName)
	require.Contains(t, strings.ToLower(userDefinedFunctionSignatureIndex.UpgSql), "unique index name_db_arg_types")
	collationApplicability := tenantUpgEntries[18]
	require.Equal(t, versions.CREATE_VIEW, collationApplicability.UpgType)
	require.Equal(t, sysview.InformationDBConst, collationApplicability.Schema)
	require.Equal(t, "COLLATION_CHARACTER_SET_APPLICABILITY", collationApplicability.TableName)
	require.Equal(t, sysview.InformationSchemaCollationCharacterSetApplicabilityDDL, collationApplicability.UpgSql)
	require.Contains(t, strings.ToLower(collationApplicability.PreSql),
		"drop view if exists information_schema.collation_character_set_applicability")

	unsignedColumns := tenantUpgEntries[19]
	require.Equal(t, versions.MODIFY_METADATA, unsignedColumns.UpgType)
	require.Equal(t, catalog.MO_CATALOG, unsignedColumns.Schema)
	require.Equal(t, catalog.MO_COLUMNS, unsignedColumns.TableName)
	require.Equal(t, int64(defines.MORPCVersion34), unsignedColumns.RequiredProtocolVersion)
	require.True(t, unsignedColumns.AllowMoColumnsUpdate)
	statistics := tenantUpgEntries[20]
	require.Equal(t, versions.MODIFY_VIEW, statistics.UpgType)
	require.Equal(t, sysview.InformationDBConst, statistics.Schema)
	require.Equal(t, "STATISTICS", statistics.TableName)
	require.Equal(t, sysview.InformationSchemaStatisticsDDL, statistics.UpgSql)
	require.Contains(t, strings.ToLower(statistics.PreSql),
		"drop view if exists information_schema.statistics")
	for _, entry := range tenantUpgEntries {
		ddl := entry.UpgSql + entry.PostSql
		if strings.Contains(ddl, "mo_subscription_tables()") ||
			strings.Contains(ddl, "mo_subscription_columns()") {
			require.Equal(t, int64(defines.MORPCVersion46), entry.RequiredProtocolVersion,
				"view upgrade %s must wait for subscription metadata functions", entry.TableName)
		} else if strings.Contains(ddl, "mo_current_roles()") {
			require.Equal(t, int64(defines.MORPCVersion41), entry.RequiredProtocolVersion,
				"view upgrade %s must wait for mo_current_roles", entry.TableName)
		}
	}
	roleGrantIndex := tenantUpgEntries[21]
	require.Equal(t, versions.ADD_INDEX, roleGrantIndex.UpgType)
	require.Equal(t, catalog.MO_CATALOG, roleGrantIndex.Schema)
	require.Equal(t, "mo_role_grant", roleGrantIndex.TableName)
	require.Equal(t, int64(defines.MORPCVersion41), roleGrantIndex.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(roleGrantIndex.UpgSql),
		"index idx_mo_role_grant_grantee_id on mo_catalog.mo_role_grant(grantee_id)")

	metadataViews := []struct {
		name string
		ddl  string
	}{
		{name: "TABLES", ddl: sysview.InformationSchemaTablesDDL},
		{name: "COLUMNS", ddl: sysview.InformationSchemaColumnsDDL},
		{name: "STATISTICS", ddl: sysview.InformationSchemaStatisticsDDL},
		{name: "TABLE_CONSTRAINTS", ddl: sysview.InformationSchemaTableConstraintsDDL},
		{name: "KEY_COLUMN_USAGE", ddl: sysview.InformationSchemaKeyColumnUsageDDL},
		{name: "REFERENTIAL_CONSTRAINTS", ddl: sysview.InformationSchemaReferentialConstraintsDDL},
		{name: "CHECK_CONSTRAINTS", ddl: sysview.InformationSchemaCheckConstraintsDDL},
		{name: "VIEWS", ddl: sysview.InformationSchemaViewsDDL},
		{name: "PARTITIONS", ddl: sysview.InformationSchemaPartitionsDDL},
		{name: "SCHEMATA", ddl: sysview.InformationSchemaSchemataDDL},
	}
	for i, view := range metadataViews {
		entry := tenantUpgEntries[22+i]
		require.Equal(t, sysview.InformationDBConst, entry.Schema)
		require.Equal(t, view.name, entry.TableName)
		require.Equal(t, versions.MODIFY_VIEW, entry.UpgType)
		require.Equal(t, view.ddl, entry.UpgSql)
		expectedProtocol := int64(defines.MORPCVersion41)
		if view.name == "TABLES" || view.name == "COLUMNS" {
			expectedProtocol = defines.MORPCVersion46
		}
		require.Equal(t, expectedProtocol, entry.RequiredProtocolVersion)
		require.Contains(t, strings.ToLower(entry.PreSql),
			"drop view if exists information_schema."+strings.ToLower(view.name))
	}

	tablePrivileges := tenantUpgEntries[22+len(metadataViews)]
	require.Equal(t, sysview.InformationDBConst, tablePrivileges.Schema)
	require.Equal(t, "TABLE_PRIVILEGES", tablePrivileges.TableName)
	require.Equal(t, versions.MODIFY_VIEW, tablePrivileges.UpgType)
	require.Equal(t, int64(defines.MORPCVersion41), tablePrivileges.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(tablePrivileges.PreSql),
		"drop table if exists information_schema.table_privileges")
	require.Contains(t, strings.ToLower(tablePrivileges.UpgSql),
		"drop view if exists information_schema.table_privileges")
	require.Equal(t, sysview.InformationSchemaTablePrivilegesDDL, tablePrivileges.PostSql)
}

func TestAddCdcWatermarkSourceTableIDWaitsForCompatibleWriters(t *testing.T) {
	entry := addCdcWatermarkSourceTableID
	entry.CheckFunc = func(executor.TxnExecutor, uint32) (bool, error) {
		return false, nil
	}

	tests := []struct {
		name            string
		protocols       string
		wantErr         bool
		wantAlterColumn bool
	}{
		{
			name:      "one old CN blocks positional-insert schema change",
			protocols: `{"method":"GETPROTOCOLVERSION","result":"cn-a:48,cn-b:47"}`,
			wantErr:   true,
		},
		{
			name:            "all CNs support explicit-column inserts",
			protocols:       `{"method":"GETPROTOCOLVERSION","result":"cn-a:48,cn-b:48"}`,
			wantAlterColumn: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			altered := false
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				switch sql {
				case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
					return newProtocolVersionResultValue(t, test.protocols), nil
				case entry.UpgSql:
					altered = true
				}
				return executor.Result{}, nil
			})

			err := entry.Upgrade(txn, catalog.System_Account)
			if test.wantErr {
				require.ErrorContains(t, err, "cn-b")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantAlterColumn, altered)
		})
	}
}

func TestUpgradeDaemonClaimPrecision(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		ready, upgraded      bool
		failCheck, failAlter bool
		wantErr              bool
	}{
		{name: "upgrade then idempotent", ready: true},
		{name: "already upgraded", upgraded: true},
		{name: "old CN blocks upgrade", wantErr: true},
		{name: "check error propagates", failCheck: true, wantErr: true},
		{name: "alter error propagates", ready: true, failAlter: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upgraded := tc.upgraded
			alters := 0
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				switch {
				case strings.HasPrefix(sql, "select atttyp from mo_catalog.mo_columns"):
					require.Contains(t, sql, "account_id = 0")
					require.Contains(t, sql, "attname = 'last_run'")
					if tc.failCheck {
						return executor.Result{}, errors.New("check failed")
					}
					if upgraded {
						mp := mpool.MustNewZeroNoFixed()
						t.Cleanup(func() { mpool.DeleteMPool(mp) })
						result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
						result.NewBatchWithRowCount(1)
						typ := types.New(types.T_timestamp, 0, 6)
						encoded, err := typ.Marshal()
						require.NoError(t, err)
						require.NoError(t, executor.AppendStringRows(result, 0, []string{string(encoded)}))
						return result.GetResult(), nil
					}
				case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
					protocol := 47
					if tc.ready {
						protocol = 48
					}
					return newProtocolVersionResultValue(t, fmt.Sprintf(`{"method":"GETPROTOCOLVERSION","result":"cn-a:%d"}`, protocol)), nil
				case sql == upgradeDaemonClaimPrecision.UpgSql:
					alters++
					if tc.failAlter {
						return executor.Result{}, errors.New("alter failed")
					}
					upgraded = true
				default:
					t.Fatalf("unexpected SQL: %s", sql)
				}
				return executor.Result{}, nil
			})
			err := upgradeDaemonClaimPrecision.Upgrade(txn, catalog.System_Account)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoError(t, upgradeDaemonClaimPrecision.Upgrade(txn, catalog.System_Account))
			}
			wantAlters := 0
			if tc.ready && !tc.upgraded && !tc.failCheck {
				wantAlters = 1
			}
			require.Equal(t, wantAlters, alters)
		})
	}
}

func TestTaskMetadataIndexUpgradeReadsRelationDefinition(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		want      bool
		wantErr   bool
	}{
		{name: "existing index", ddl: "create table t (a int, key IDX_ACCOUNT_ID(a))", want: true},
		{name: "missing index", ddl: "create table t (a int, key other(a))"},
		{name: "name in column comment is not index", ddl: "create table t (a int comment 'idx_account_id')"},
		{name: "empty definition", wantErr: true},
		{name: "wrong statement", ddl: "select 1", wantErr: true},
		{name: "invalid SQL", ddl: "not sql", wantErr: true},
		{name: "query failure", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				require.Equal(t, "SHOW CREATE TABLE `mo_task`.`sql_task`", sql)
				if tc.name == "query failure" {
					return executor.Result{}, errors.New("catalog unavailable")
				}
				return newShowCreateTableResult(t, "sql_task", tc.ddl), nil
			})
			found, err := addSQLTaskAccountIndex.CheckFunc(txn, 0)
			require.Equal(t, tc.want, found)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDaemonClaimPrecisionCheckUsesStoredType(t *testing.T) {
	for _, tc := range []struct {
		name          string
		typ           types.Type
		want, corrupt bool
	}{
		{name: "seconds", typ: types.New(types.T_timestamp, 0, 0)},
		{name: "microseconds", typ: types.New(types.T_timestamp, 0, 6), want: true},
		{name: "different type", typ: types.New(types.T_datetime, 0, 6)},
		{name: "corrupt encoding", corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.typ.Marshal()
			require.NoError(t, err)
			if tc.corrupt {
				encoded = []byte{1}
			}
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				require.Contains(t, sql, "select atttyp")
				mp := mpool.MustNewZeroNoFixed()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(result, 0, []string{string(encoded)}))
				return result.GetResult(), nil
			})
			found, err := upgradeDaemonClaimPrecision.CheckFunc(txn, 0)
			require.Equal(t, tc.want, found)
			if tc.corrupt {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInformationSchemaMetadataVisibilityUpgradeChecks(t *testing.T) {
	views := []struct {
		name string
		ddl  string
	}{
		{name: "TABLES", ddl: sysview.InformationSchemaTablesDDL},
		{name: "COLUMNS", ddl: sysview.InformationSchemaColumnsDDL},
		{name: "STATISTICS", ddl: sysview.InformationSchemaStatisticsDDL},
		{name: "TABLE_CONSTRAINTS", ddl: sysview.InformationSchemaTableConstraintsDDL},
		{name: "KEY_COLUMN_USAGE", ddl: sysview.InformationSchemaKeyColumnUsageDDL},
		{name: "REFERENTIAL_CONSTRAINTS", ddl: sysview.InformationSchemaReferentialConstraintsDDL},
		{name: "CHECK_CONSTRAINTS", ddl: sysview.InformationSchemaCheckConstraintsDDL},
		{name: "VIEWS", ddl: sysview.InformationSchemaViewsDDL},
		{name: "PARTITIONS", ddl: sysview.InformationSchemaPartitionsDDL},
		{name: "SCHEMATA", ddl: sysview.InformationSchemaSchemataDDL},
	}
	checkErr := errors.New("check metadata view definition failed")

	for _, view := range views {
		for _, state := range []struct {
			name       string
			exists     bool
			definition string
			checkErr   error
			want       bool
		}{
			{name: "current", exists: true, definition: view.ddl, want: true},
			{name: "old", exists: true, definition: "old view definition"},
			{name: "missing", definition: view.ddl},
			{name: "error", checkErr: checkErr},
		} {
			t.Run(view.name+"/"+state.name, func(t *testing.T) {
				oldCheck := versions.CheckViewDefinition
				versions.CheckViewDefinition = func(
					txn executor.TxnExecutor,
					accountID uint32,
					schema string,
					viewName string,
				) (bool, string, error) {
					require.Nil(t, txn)
					require.Equal(t, uint32(42), accountID)
					require.Equal(t, sysview.InformationDBConst, schema)
					require.Equal(t, view.name, viewName)
					return state.exists, state.definition, state.checkErr
				}
				defer func() { versions.CheckViewDefinition = oldCheck }()

				entry := upgradeInformationSchemaMetadataVisibilityView(view.name, view.ddl)
				ok, err := entry.CheckFunc(nil, 42)
				if state.checkErr != nil {
					require.ErrorIs(t, err, state.checkErr)
					require.False(t, ok)
					return
				}
				require.NoError(t, err)
				require.Equal(t, state.want, ok)
			})
		}
	}
	allocatorIndex := tenantUpgEntries[33]
	require.Equal(t, versions.ADD_INDEX, allocatorIndex.UpgType)
	require.Equal(t, catalog.MO_CATALOG, allocatorIndex.Schema)
	require.Equal(t, "mo_iceberg_catalogs", allocatorIndex.TableName)
	require.Contains(t, strings.ToLower(allocatorIndex.UpgSql), "create index catalog_id_allocator")
}

func TestMoColumnsUnsignedBackfillPredicate(t *testing.T) {
	entry := backfillMoColumnsAttIsUnsigned()
	require.Contains(t, entry.UpgSql, "account_id = current_account_id()")
	require.Contains(t, entry.UpgSql, "att_is_unsigned IS NULL OR att_is_unsigned = 0")
	for _, typ := range []string{
		"TINYINT UNSIGNED",
		"SMALLINT UNSIGNED",
		"INT UNSIGNED",
		"BIGINT UNSIGNED",
	} {
		require.Contains(t, entry.UpgSql, "'"+typ+"'")
	}
	require.NotContains(t, entry.UpgSql, "DECIMAL")
	require.NotContains(t, entry.UpgSql, "BIT")
}

func TestMoColumnsUnsignedBackfillWaitsForAllCNsAndIsIdempotent(t *testing.T) {
	entry := backfillMoColumnsAttIsUnsigned()
	checkSQL := "SELECT 1 FROM mo_catalog.mo_columns WHERE " + moColumnsUnsignedMismatchPredicate + " LIMIT 1"

	t.Run("older CN blocks the update", func(t *testing.T) {
		updated := false
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			switch sql {
			case checkSQL:
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newProtocolVersionResultValue(t, `{"method":"GETPROTOCOLVERSION","result":"cn-a:34,cn-b:33"}`), nil
			case entry.UpgSql:
				updated = true
			}
			return executor.Result{}, nil
		})

		require.Error(t, entry.Upgrade(txn, 42))
		require.False(t, updated)
	})

	t.Run("all CNs ready then second run is check only", func(t *testing.T) {
		hasMismatch := true
		var executed []string
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			executed = append(executed, sql)
			switch sql {
			case checkSQL:
				if hasMismatch {
					result := executor.NewMemResult(nil, nil)
					result.NewBatchWithRowCount(1)
					return result.GetResult(), nil
				}
			case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newProtocolVersionResultValue(t, `{"method":"GETPROTOCOLVERSION","result":"cn-a:34,cn-b:34"}`), nil
			case entry.UpgSql:
				hasMismatch = false
			}
			return executor.Result{}, nil
		})

		require.NoError(t, entry.Upgrade(txn, 42))
		require.Equal(t, []string{
			checkSQL,
			"SELECT mo_ctl('cn', 'GetProtocolVersion', '')",
			entry.UpgSql,
		}, executed)

		executed = nil
		require.NoError(t, entry.Upgrade(txn, 42))
		require.Equal(t, []string{checkSQL}, executed)
	})
}

func TestInformationSchemaCollationsUpgradeCheckIsExact(t *testing.T) {
	checkSQL := informationSchemaCollationsCheckSQL()
	require.Contains(t, checkSQL, "(SELECT COUNT(*) FROM information_schema.COLLATIONS) = ")
	for _, collation := range sysview.SupportedCollationDefinitions {
		require.Contains(t, checkSQL, "COLLATION_NAME = '"+collation.Name+"'")
		require.Contains(t, checkSQL, "CHARACTER_SET_NAME = '"+collation.Charset+"'")
	}
}

func TestInformationSchemaCharacterSetsUpgradeCheckUsesCanonicalDefaults(t *testing.T) {
	checkSQL := informationSchemaCharacterSetsCheckSQL()
	for _, charset := range []string{"binary", "utf8", "utf8mb4"} {
		require.Contains(t, checkSQL,
			"CHARACTER_SET_NAME = '"+charset+"' AND DEFAULT_COLLATE_NAME = '"+
				sysview.DefaultCollationForCharset(charset)+"'")
	}
}

func TestUserDefinedFunctionArgumentTypesBackfillRejectsOversizedSignature(t *testing.T) {
	entry := backfillUserDefinedFunctionArgumentTypes()
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		require.Contains(t, sql, "length(")
		require.Contains(t, sql, "> 65535")
		return newShowCreateTableResult(t, "function", "create table function (id int)"), nil
	})

	finished, err := entry.CheckFunc(txn, 1)
	require.False(t, finished)
	require.Error(t, err)
	require.Contains(t, err.Error(), "catalog limit")
}

func TestForeignKeyMetadataTenantUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 34)

	for i, column := range []string{"referenced_index_name", "on_delete_origin", "on_update_origin"} {
		entry := tenantUpgEntries[2+i]
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, catalog.MOForeignKeys, entry.TableName)
		require.Contains(t, entry.UpgSql, "add column "+column)
	}

	keyColumnUsage := tenantUpgEntries[5]
	require.Equal(t, versions.CREATE_VIEW, keyColumnUsage.UpgType)
	require.Equal(t, "KEY_COLUMN_USAGE", keyColumnUsage.TableName)
	require.Contains(t, strings.ToLower(keyColumnUsage.UpgSql), "drop view if exists information_schema.key_column_usage")
	require.Contains(t, strings.ToLower(keyColumnUsage.PreSql), "drop table if exists information_schema.key_column_usage")
	require.Equal(t, sysview.InformationSchemaKeyColumnUsageDDL, keyColumnUsage.PostSql)

	referentialConstraints := tenantUpgEntries[6]
	require.Equal(t, versions.MODIFY_VIEW, referentialConstraints.UpgType)
	require.Equal(t, "REFERENTIAL_CONSTRAINTS", referentialConstraints.TableName)
	require.Equal(t, sysview.InformationSchemaReferentialConstraintsDDL, referentialConstraints.UpgSql)
	require.Contains(t, strings.ToLower(referentialConstraints.PreSql), "drop view if exists information_schema.referential_constraints")
}

func TestUpgradeInformationSchemaColumnsCheck(t *testing.T) {
	entry := upgradeInformationSchemaColumns()
	checkErr := errors.New("check view definition failed")
	for _, test := range []struct {
		name       string
		exists     bool
		definition string
		checkErr   error
		want       bool
	}{
		{name: "current definition", exists: true, definition: sysview.InformationSchemaColumnsDDL, want: true},
		{name: "old definition", exists: true, definition: "old view definition"},
		{name: "missing view", definition: sysview.InformationSchemaColumnsDDL},
		{name: "check error", checkErr: checkErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldCheck := versions.CheckViewDefinition
			versions.CheckViewDefinition = func(txn executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
				require.Nil(t, txn)
				require.Equal(t, uint32(42), accountID)
				require.Equal(t, sysview.InformationDBConst, schema)
				require.Equal(t, "COLUMNS", viewName)
				return test.exists, test.definition, test.checkErr
			}
			defer func() { versions.CheckViewDefinition = oldCheck }()

			ok, err := entry.CheckFunc(nil, 42)
			if test.checkErr != nil {
				require.ErrorIs(t, err, test.checkErr)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, ok)
		})
	}
}

func TestLegacyForeignKeyMetadataCatalogQueriesUseForeignKeyCatalogOnly(t *testing.T) {
	for _, query := range []string{legacyForeignKeyTableDefinitionsSQL, legacyForeignKeyReferencedIndexDefinitionsSQL} {
		require.NotContains(t, strings.ToLower(query), "rel_createsql")
		require.NotContains(t, strings.ToLower(query), "mo_tables")
	}
	require.Contains(t, legacyForeignKeyReferencedIndexDefinitionsSQL, "referenced_index_name = ''")
}

func TestLegacyForeignKeyMetadataUpdatesPreserveOrderAndActions(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(legacyForeignKeyTableDefinition{
		database: "db'one",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_default", columnName: "a", referDBName: "db'one", referTableName: "parent", referColumnName: "a", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
			{constraintName: "fk_default", columnName: "b", referDBName: "db'one", referTableName: "parent", referColumnName: "b", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
			{constraintName: "fk_restrict", columnName: "a", referDBName: "db'one", referTableName: "parent", referColumnName: "a", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	}, "create table child (a int, b int, constraint fk_default foreign key (b, a) references parent (b, a), "+
		"constraint fk_restrict foreign key (a) references parent (a) on delete restrict on update restrict)",
		"create table child (a int, b int, constraint fk_default foreign key (b, a) references parent (b, a), "+
			"constraint fk_restrict foreign key (a) references parent (a) on delete restrict on update restrict)")
	require.NoError(t, err)
	require.Len(t, updates, 3)

	for _, expected := range []string{
		"constraint_id = 1, on_delete = 'NO_ACTION', on_update = 'NO_ACTION', on_delete_origin = 'ACTION_ORIGIN_DEFAULT', on_update_origin = 'ACTION_ORIGIN_DEFAULT'",
		"constraint_name = 'fk_default' AND column_name = 'b'",
		"constraint_id = 2, on_delete = 'NO_ACTION', on_update = 'NO_ACTION', on_delete_origin = 'ACTION_ORIGIN_DEFAULT', on_update_origin = 'ACTION_ORIGIN_DEFAULT'",
		"constraint_name = 'fk_default' AND column_name = 'a'",
		"constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT', on_delete_origin = 'ACTION_ORIGIN_EXPLICIT', on_update_origin = 'ACTION_ORIGIN_EXPLICIT'",
		"constraint_name = 'fk_restrict' AND column_name = 'a'",
		"db_name = 'db''one'",
	} {
		require.Contains(t, strings.Join(updates, "\n"), expected)
	}
}

func TestLegacyForeignKeyMetadataUpdatesIgnoreStaleHistoricalActions(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{{
			constraintName: "fk_recreated", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "CASCADE", onUpdate: "SET_NULL",
		}},
	},
		"create table child (parent_id int, constraint fk_recreated foreign key (parent_id) references parent (id) on delete cascade on update set null)",
		"create table child (parent_id int, constraint fk_recreated foreign key (parent_id) references parent (id) on delete restrict on update restrict)",
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL', " +
			"on_delete_origin = 'ACTION_ORIGIN_EXPLICIT', on_update_origin = 'ACTION_ORIGIN_EXPLICIT' " +
			"WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_recreated' AND column_name = 'parent_id'",
	}, updates)
}

func TestLegacyForeignKeyMetadataUpdatesKeepAlterRestrictAmbiguous(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{{
			constraintName: "fk_added_by_alter", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT",
		}},
	},
		"create table child (parent_id int, constraint fk_added_by_alter foreign key (parent_id) references parent (id) on delete restrict on update restrict)",
		"create table child (parent_id int)",
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT', " +
			"on_delete_origin = 'ACTION_ORIGIN_LEGACY_AMBIGUOUS', on_update_origin = 'ACTION_ORIGIN_LEGACY_AMBIGUOUS' " +
			"WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'parent_id'",
	}, updates)
}

func TestLegacyForeignKeyReferencedIndexNameSelectsExactPrimaryKey(t *testing.T) {
	var queries []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		queries = append(queries, sql)
		return newLegacyForeignKeyIndexResult(t, [][]string{
			{"PRIMARY", "PRIMARY", "1", "id"},
			{"uq_parent_id", "UNIQUE", "1", "id"},
			{"uq_parent_compound", "UNIQUE", "1", "id"},
			{"uq_parent_compound", "UNIQUE", "2", "code"},
		}), nil
	})
	name, err := getLegacyForeignKeyReferencedIndexName(9, txnExecutor, legacyForeignKeyReferencedKey{
		database: "db",
		table:    "parent",
		columns:  []string{"id"},
	})
	require.NoError(t, err)
	require.Equal(t, "PRIMARY", name)
	require.Len(t, queries, 1)
	require.Contains(t, queries[0], "tbl.reldatabase = 'db'")
	require.Contains(t, queries[0], "ORDER BY CASE WHEN idx.type = 'PRIMARY' THEN 0 ELSE 1 END")
}

func TestLegacyForeignKeyReferencedIndexNameUsesOrderedLeadingPrefix(t *testing.T) {
	tests := []struct {
		name        string
		indexRows   [][]string
		foreignCols []string
		want        string
	}{
		{
			name: "composite primary prefix",
			indexRows: [][]string{
				{"PRIMARY", "PRIMARY", "1", "id"},
				{"PRIMARY", "PRIMARY", "2", "code"},
			},
			foreignCols: []string{"id"},
			want:        "PRIMARY",
		},
		{
			name: "primary wins over exact unique",
			indexRows: [][]string{
				{"PRIMARY", "PRIMARY", "1", "id"},
				{"PRIMARY", "PRIMARY", "2", "code"},
				{"uq_id", "UNIQUE", "1", "id"},
			},
			foreignCols: []string{"id"},
			want:        "PRIMARY",
		},
		{
			name: "non prefix is rejected",
			indexRows: [][]string{
				{"uq_code_id", "UNIQUE", "1", "code"},
				{"uq_code_id", "UNIQUE", "2", "id"},
			},
			foreignCols: []string{"id"},
			want:        "",
		},
		{
			name: "unique tie is lexical",
			indexRows: [][]string{
				{"uq_z", "UNIQUE", "1", "id"},
				{"uq_a", "UNIQUE", "1", "id"},
			},
			foreignCols: []string{"id"},
			want:        "uq_a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			txnExecutor := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
				return newLegacyForeignKeyIndexResult(t, test.indexRows), nil
			})
			name, err := getLegacyForeignKeyReferencedIndexName(9, txnExecutor, legacyForeignKeyReferencedKey{
				database: "db",
				table:    "parent",
				columns:  test.foreignCols,
			})
			require.NoError(t, err)
			require.Equal(t, test.want, name)
		})
	}
}

func TestVersionHandleMetadata(t *testing.T) {
	meta := Handler.Metadata()
	require.Equal(t, "4.0.6", meta.Version)
	require.Equal(t, "4.0.5", meta.MinUpgradeVersion)
	require.Equal(t, versions.Yes, meta.UpgradeTenant)
	require.Equal(t, versions.Yes, meta.UpgradeCluster)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries))+removedIndexVisibilityUpgradeOffset, meta.VersionOffset)
	require.Equal(t, int64(defines.MORPCVersion34), meta.RequiredProtocolVersion)
}

func TestTenantViewDefinitionChecks(t *testing.T) {
	entries := []versions.UpgradeEntry{
		upgradeInformationSchemaKeyColumnUsage(),
		upgradeInformationSchemaReferentialConstraints(),
		upgradeInformationSchemaCheckConstraints(),
		upgradeInformationSchemaTableConstraints(),
		upgradeInformationSchemaCollationCharacterSetApplicability(),
		upgradeInformationSchemaTablePrivileges(),
	}

	for _, entry := range entries {
		t.Run(entry.TableName+"/match", func(t *testing.T) {
			targetDefinition := entry.UpgSql
			if entry.PostSql != "" {
				targetDefinition = entry.PostSql
			}
			expectedViewName := entry.TableName
			if entry.TableName == "TABLE_PRIVILEGES" {
				expectedViewName = "table_privileges"
			}
			stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
				if accountID != 42 || schema != sysview.InformationDBConst || viewName != expectedViewName {
					t.Fatalf("unexpected view check arguments: account=%d schema=%s view=%s", accountID, schema, viewName)
				}
				return true, targetDefinition, nil
			})
			defer stub.Reset()

			matched, err := entry.CheckFunc(nil, 42)
			if err != nil || !matched {
				t.Fatalf("expected matching view definition, matched=%v err=%v", matched, err)
			}
		})

		t.Run(entry.TableName+"/mismatch", func(t *testing.T) {
			stub := gostub.Stub(&versions.CheckViewDefinition, func(executor.TxnExecutor, uint32, string, string) (bool, string, error) {
				return true, "old definition", nil
			})
			defer stub.Reset()

			matched, err := entry.CheckFunc(nil, 42)
			if err != nil || matched {
				t.Fatalf("expected mismatching view definition, matched=%v err=%v", matched, err)
			}
		})
	}

	stub := gostub.Stub(&versions.CheckViewDefinition, func(executor.TxnExecutor, uint32, string, string) (bool, string, error) {
		return false, "", errors.New("check failed")
	})
	defer stub.Reset()
	matched, err := entries[len(entries)-1].CheckFunc(nil, 42)
	if err == nil || matched {
		t.Fatalf("expected check error, matched=%v err=%v", matched, err)
	}
}

func TestCheckConstraintViewsUpgradeMixedProtocolInitializedTenant(t *testing.T) {
	tests := []struct {
		name     string
		entry    versions.UpgradeEntry
		exists   bool
		viewName string
		viewDef  string
	}{
		{
			name:     "missing check constraints view",
			entry:    upgradeInformationSchemaCheckConstraints(),
			exists:   false,
			viewName: "CHECK_CONSTRAINTS",
		},
		{
			name:     "legacy table constraints view",
			entry:    upgradeInformationSchemaTableConstraints(),
			exists:   true,
			viewName: "TABLE_CONSTRAINTS",
			viewDef:  sysview.InformationSchemaTableConstraintsLegacyDDL,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
				require.Equal(t, uint32(42), accountID)
				require.Equal(t, sysview.InformationDBConst, schema)
				require.Equal(t, test.viewName, viewName)
				return test.exists, test.viewDef, nil
			})
			defer stub.Reset()

			matched, err := test.entry.CheckFunc(nil, 42)
			require.NoError(t, err)
			require.False(t, matched)
		})
	}
}

func TestKeyColumnUsageViewUpgradeIsOrderedAndIdempotent(t *testing.T) {
	entry := upgradeInformationSchemaKeyColumnUsage()
	upgraded := false
	stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
		require.Equal(t, uint32(42), accountID)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "KEY_COLUMN_USAGE", viewName)
		if upgraded {
			return true, sysview.InformationSchemaKeyColumnUsageDDL, nil
		}
		return false, "", nil
	})
	defer stub.Reset()

	var executed []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		if strings.Contains(strings.ToLower(sql), "getprotocolversion") {
			return newProtocolVersionResultValue(t,
				`{"method":"GETPROTOCOLVERSION","result":"cn-a:41,cn-b:41"}`), nil
		}
		executed = append(executed, sql)
		if sql == entry.PostSql {
			upgraded = true
		}
		return executor.Result{}, nil
	})

	require.NoError(t, entry.Upgrade(txnExecutor, 42))
	require.Equal(t, []string{entry.PreSql, entry.UpgSql, entry.PostSql}, executed)

	executed = nil
	require.NoError(t, entry.Upgrade(txnExecutor, 42))
	require.Empty(t, executed)
}

func TestTablePrivilegesViewUpgradeConvergesAndIsIdempotent(t *testing.T) {
	for _, test := range []struct {
		name       string
		exists     bool
		definition string
		wantDDL    bool
	}{
		{name: "missing object", wantDDL: true},
		{name: "legacy base table", wantDDL: true},
		{name: "stale view", exists: true, definition: "old view definition", wantDDL: true},
		{name: "canonical view", exists: true, definition: sysview.InformationSchemaTablePrivilegesDDL},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry := upgradeInformationSchemaTablePrivileges()
			upgraded := false
			stub := gostub.Stub(&versions.CheckViewDefinition, func(
				_ executor.TxnExecutor,
				accountID uint32,
				schema string,
				viewName string,
			) (bool, string, error) {
				require.Equal(t, uint32(42), accountID)
				require.Equal(t, sysview.InformationDBConst, schema)
				require.Equal(t, "table_privileges", viewName)
				if upgraded {
					return true, sysview.InformationSchemaTablePrivilegesDDL, nil
				}
				return test.exists, test.definition, nil
			})
			defer stub.Reset()

			var executed []string
			txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				if strings.Contains(strings.ToLower(sql), "getprotocolversion") {
					return newProtocolVersionResultValue(t,
						`{"method":"GETPROTOCOLVERSION","result":"cn-a:41,cn-b:41"}`), nil
				}
				executed = append(executed, sql)
				if sql == entry.PostSql {
					upgraded = true
				}
				return executor.Result{}, nil
			})

			require.NoError(t, entry.Upgrade(txnExecutor, 42))
			if test.wantDDL {
				require.Equal(t, []string{entry.PreSql, entry.UpgSql, entry.PostSql}, executed)
			} else {
				require.Empty(t, executed)
			}

			executed = nil
			require.NoError(t, entry.Upgrade(txnExecutor, 42))
			require.Empty(t, executed)
		})
	}
}

func TestVersionHandleLifecycleWithNoLegacyDefinitions(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		tableStub := gostub.Stub(&versions.CheckTableDefinition, func(executor.TxnExecutor, uint32, string, string) (bool, error) {
			return true, nil
		})
		defer tableStub.Reset()

		stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, _ uint32, _ string, viewName string) (bool, string, error) {
			switch viewName {
			case "KEY_COLUMN_USAGE":
				return true, sysview.InformationSchemaKeyColumnUsageDDL, nil
			case "REFERENTIAL_CONSTRAINTS":
				return true, sysview.InformationSchemaReferentialConstraintsDDL, nil
			case "CHECK_CONSTRAINTS":
				return true, sysview.InformationSchemaCheckConstraintsDDL, nil
			case "COLLATION_CHARACTER_SET_APPLICABILITY":
				return true, sysview.InformationSchemaCollationCharacterSetApplicabilityDDL, nil
			case "TABLE_CONSTRAINTS":
				return true, sysview.InformationSchemaTableConstraintsDDL, nil
			case "COLUMNS":
				return true, sysview.InformationSchemaColumnsDDL, nil
			case "TABLES":
				return true, sysview.InformationSchemaTablesDDL, nil
			case "STATISTICS":
				return true, sysview.InformationSchemaStatisticsDDL, nil
			case "VIEWS":
				return true, sysview.InformationSchemaViewsDDL, nil
			case "PARTITIONS":
				return true, sysview.InformationSchemaPartitionsDDL, nil
			case "SCHEMATA":
				return true, sysview.InformationSchemaSchemataDDL, nil
			case "table_privileges":
				return true, sysview.InformationSchemaTablePrivilegesDDL, nil
			default:
				return false, "", errors.New("unexpected view")
			}
		})
		defer stub.Reset()

		var executed []string
		txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			for table, ddl := range map[string]string{
				"sql_task":       frontend.MoTaskSQLTaskDDL,
				"sql_task_run":   frontend.MoTaskSQLTaskRunDDL,
				"sys_async_task": frontend.MoTaskSysAsyncTaskDDL,
			} {
				if sql == "SHOW CREATE TABLE `mo_task`.`"+table+"`" {
					return newShowCreateTableResult(t, table, ddl), nil
				}
			}
			if strings.Contains(strings.ToLower(sql), "getprotocolversion") {
				return newProtocolVersionResultValue(t,
					fmt.Sprintf(
						`{"method":"GETPROTOCOLVERSION","result":"cn-a:%d,cn-b:%d"}`,
						defines.MORPCLatestVersion,
						defines.MORPCLatestVersion,
					)), nil
			}
			executed = append(executed, sql)
			return executor.Result{}, nil
		})

		if err := Handler.Prepare(context.Background(), txnExecutor, true); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if err := Handler.HandleTenantUpgrade(context.Background(), 9, txnExecutor); err != nil {
			t.Fatalf("tenant upgrade: %v", err)
		}
		if len(executed) == 0 || executed[len(executed)-1] != legacyForeignKeyReferencedIndexDefinitionsSQL {
			t.Fatalf("unexpected SQL: %v", executed)
		}
		if err := Handler.HandleClusterUpgrade(context.Background(), txnExecutor); err != nil {
			t.Fatalf("cluster upgrade: %v", err)
		}
		if err := Handler.HandleCreateFrameworkDeps(txnExecutor); err == nil || !strings.Contains(err.Error(), "Only v1.2.0") {
			t.Fatalf("unexpected framework-dependency result: %v", err)
		}
	})
}

func TestVersionHandleTenantUpgradeReturnsLegacyQueryError(t *testing.T) {
	want := errors.New("legacy query failed")
	txnExecutor := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
		return executor.Result{}, want
	})
	if err := Handler.HandleTenantUpgrade(context.Background(), 9, txnExecutor); !errors.Is(err, want) {
		t.Fatalf("expected legacy query error, got %v", err)
	}
}

func TestLegacyForeignKeyMetadataUpgradeReadsAndUpdatesDefinitions(t *testing.T) {
	definitionResult := newLegacyForeignKeyDefinitionResult(t)
	var updates []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		if sql == legacyForeignKeyTableDefinitionsSQL {
			return definitionResult, nil
		}
		if sql == "SHOW CREATE TABLE `db`.`child`" {
			return newShowCreateTableResult(t, "child", "create table child (parent_id int, child_id int, constraint fk_child_parent foreign key (child_id, parent_id) references parent (child_id, parent_id) on delete cascade on update set null)"), nil
		}
		updates = append(updates, sql)
		return executor.Result{}, nil
	})

	if err := upgradeLegacyForeignKeyMetadata(context.Background(), 9, txnExecutor); err != nil {
		t.Fatalf("upgrade legacy foreign-key metadata: %v", err)
	}
	updates = legacyForeignKeyMigrationUpdatesForAssertion(updates)
	if len(updates) != 2 {
		t.Fatalf("expected two metadata updates, got %d: %v", len(updates), updates)
	}
	for _, update := range updates {
		if !strings.Contains(update, "constraint_id =") || !strings.Contains(update, "constraint_name = 'fk_child_parent'") {
			t.Fatalf("unexpected update: %s", update)
		}
	}
}

func TestLegacyForeignKeyMetadataUpgradeUsesHistoricalCreateActions(t *testing.T) {
	definition := legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_default", columnName: "default_parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
			{constraintName: "fk_restrict", columnName: "restrict_parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	}
	showCreateSQL := "create table child (default_parent_id int, restrict_parent_id int, " +
		"constraint fk_default foreign key (default_parent_id) references parent (id) on delete restrict on update restrict, " +
		"constraint fk_restrict foreign key (restrict_parent_id) references parent (id) on delete restrict on update restrict)"
	historicalCreateSQL := "create table child (default_parent_id int, restrict_parent_id int, " +
		"constraint fk_default foreign key (default_parent_id) references parent (id), " +
		"constraint fk_restrict foreign key (restrict_parent_id) references parent (id) on delete restrict on update restrict)"
	var updates []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		switch {
		case sql == legacyForeignKeyTableDefinitionsSQL:
			return newLegacyForeignKeyDefinitionResultForDefinitions(t, []legacyForeignKeyTableDefinition{definition}), nil
		case sql == legacyForeignKeyReferencedIndexDefinitionsSQL:
			return executor.Result{}, nil
		case sql == "SHOW CREATE TABLE `db`.`child`":
			return newShowCreateTableResult(t, "child", showCreateSQL), nil
		case strings.HasPrefix(sql, "SELECT rel_createsql FROM mo_catalog.mo_tables"):
			return newHistoricalCreateSQLResult(t, historicalCreateSQL), nil
		default:
			updates = append(updates, sql)
			return executor.Result{}, nil
		}
	})

	require.NoError(t, upgradeLegacyForeignKeyMetadata(context.Background(), 9, txnExecutor))
	require.Len(t, updates, 2)
	require.Contains(t, strings.Join(updates, "\n"),
		"constraint_name = 'fk_default' AND column_name = 'default_parent_id'")
	require.Contains(t, strings.Join(updates, "\n"),
		"on_delete = 'NO_ACTION', on_update = 'NO_ACTION', on_delete_origin = 'ACTION_ORIGIN_DEFAULT', on_update_origin = 'ACTION_ORIGIN_DEFAULT'")
	require.Contains(t, strings.Join(updates, "\n"),
		"constraint_name = 'fk_restrict' AND column_name = 'restrict_parent_id'")
	require.Contains(t, strings.Join(updates, "\n"),
		"on_delete = 'RESTRICT', on_update = 'RESTRICT', on_delete_origin = 'ACTION_ORIGIN_EXPLICIT', on_update_origin = 'ACTION_ORIGIN_EXPLICIT'")
}

func TestLegacyForeignKeyMetadataUpgradeBackfillsReferencedIndexForNumberedLegacyForeignKey(t *testing.T) {
	definition := legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{{
			constraintName: "fk_child_parent", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT",
		}},
	}
	var updates []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		switch sql {
		case legacyForeignKeyTableDefinitionsSQL:
			// constraint_id was assigned by an earlier upgrade; it must not be
			// required to backfill the referenced key.
			return executor.Result{}, nil
		case legacyForeignKeyReferencedIndexDefinitionsSQL:
			return newLegacyForeignKeyDefinitionResultForDefinitions(t, []legacyForeignKeyTableDefinition{definition}), nil
		case "SHOW CREATE TABLE `db`.`child`":
			return newShowCreateTableResult(t, "child", "create table child (parent_id int, constraint fk_child_parent foreign key (parent_id) references parent (id))"), nil
		}
		if strings.HasPrefix(sql, "SELECT idx.name, idx.type") {
			return newLegacyForeignKeyIndexResult(t, [][]string{{"PRIMARY", "PRIMARY", "1", "id"}}), nil
		}
		updates = append(updates, sql)
		return executor.Result{}, nil
	})

	require.NoError(t, upgradeLegacyForeignKeyMetadata(context.Background(), 9, txnExecutor))
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET referenced_index_name = 'PRIMARY' WHERE db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_child_parent'",
	}, updates)
}

func TestLegacyForeignKeyMetadataUpgradeBackfillsAlterAndUnnamedForeignKeys(t *testing.T) {
	definitionResult := newLegacyForeignKeyDefinitionResultForDefinitions(t, []legacyForeignKeyTableDefinition{
		{
			database: "db",
			table:    "alter_child",
			foreignKeys: []legacyForeignKeyCatalogRow{
				{constraintName: "fk_added_by_alter", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "SET_NULL"},
				{constraintName: "fk_added_by_alter", columnName: "b", referDBName: "db", referTableName: "parent", referColumnName: "b", onDelete: "CASCADE", onUpdate: "SET_NULL"},
			},
		},
		{
			database: "db",
			table:    "unnamed_child",
			foreignKeys: []legacyForeignKeyCatalogRow{
				{constraintName: "catalog_generated_name", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
			},
		},
	})
	var updates []string
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		if sql == legacyForeignKeyTableDefinitionsSQL {
			return definitionResult, nil
		}
		switch sql {
		case "SHOW CREATE TABLE `db`.`alter_child`":
			return newShowCreateTableResult(t, "alter_child", "create table alter_child (a int, b int, constraint fk_added_by_alter foreign key (b, a) references parent (b, a) on delete cascade on update set null)"), nil
		case "SHOW CREATE TABLE `db`.`unnamed_child`":
			return newShowCreateTableResult(t, "unnamed_child", "create table unnamed_child (parent_id int, constraint catalog_generated_name foreign key (parent_id) references parent (id) on delete restrict on update restrict)"), nil
		}
		updates = append(updates, sql)
		return executor.Result{}, nil
	})

	require.NoError(t, upgradeLegacyForeignKeyMetadata(context.Background(), 9, txnExecutor))
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'alter_child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'b'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 2, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'alter_child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'a'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'unnamed_child' AND constraint_name = 'catalog_generated_name' AND column_name = 'parent_id'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesRejectInvalidDefinitions(t *testing.T) {
	for _, createSQL := range []string{
		"select 1",
		"create table child (a int); create table another_child (a int)",
	} {
		t.Run(createSQL, func(t *testing.T) {
			_, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
				database: "db",
				table:    "child",
			}, createSQL)
			if err == nil {
				t.Fatalf("expected invalid persisted definition to fail: %s", createSQL)
			}
		})
	}
}

func TestLegacyForeignKeyMetadataUpdatesBackfillAlterTableForeignKey(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_added_by_alter", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "SET_NULL"},
			{constraintName: "fk_added_by_alter", columnName: "b", referDBName: "db", referTableName: "parent", referColumnName: "b", onDelete: "CASCADE", onUpdate: "SET_NULL"},
		},
	}, "create table child (b int, a int, constraint fk_added_by_alter foreign key (b, a) references parent (b, a) on delete cascade on update set null)")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'b'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 2, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'a'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesPreservesRestrictForAlterFallback(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_added_by_alter", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	}, "create table child (parent_id int)")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'parent_id'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesRejectInconsistentCatalogActions(t *testing.T) {
	_, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_child_parent", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "fk_child_parent", columnName: "b", referDBName: "db", referTableName: "parent", referColumnName: "b", onDelete: "SET_NULL", onUpdate: "CASCADE"},
		},
	}, "create table child (a int, b int, constraint fk_child_parent foreign key (a, b) references parent (a, b))")
	require.ErrorContains(t, err, "inconsistent catalog actions")
}

func TestLegacyForeignKeyMetadataUpdatesDoNotMatchReusedConstraintName(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_reused", columnName: "b", referDBName: "db", referTableName: "parent", referColumnName: "b", onDelete: "CASCADE", onUpdate: "SET_NULL"},
		},
	}, "create table child (a int, b int, constraint fk_reused foreign key (a) references parent (a))")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_reused' AND column_name = 'b'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesEscapeCatalogIdentifiers(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: `db\name`,
		table:    `child\name`,
		foreignKeys: []legacyForeignKeyCatalogRow{
			{
				constraintName:  `fk\' OR 1=1 -- `,
				columnName:      `child\column`,
				referDBName:     `db\name`,
				referTableName:  `parent\name`,
				referColumnName: `id\column`,
				onDelete:        "CASCADE",
				onUpdate:        "SET_NULL",
			},
		},
	}, "create table `child\\name` (`child\\column` int, constraint `fk\\' OR 1=1 -- ` foreign key (`child\\column`) references `parent\\name` (`id\\column`))")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' " +
			"WHERE constraint_id = 0 AND db_name = 'db\\\\name' AND table_name = 'child\\\\name' " +
			"AND constraint_name = 'fk\\\\'' OR 1=1 -- ' AND column_name = 'child\\\\column'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesEscapeTrailingBackslashIdentifiers(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db\\",
		table:    "child\\",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{
				constraintName:  "fk\\",
				columnName:      "child\\",
				referDBName:     "db\\",
				referTableName:  "parent\\",
				referColumnName: "id\\",
				onDelete:        "CASCADE",
				onUpdate:        "SET_NULL",
			},
		},
	}, "create table `child\\` (`child\\` int, constraint `fk\\` foreign key (`child\\`) references `parent\\` (`id\\`))")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' " +
			"WHERE constraint_id = 0 AND db_name = 'db\\\\' AND table_name = 'child\\\\' " +
			"AND constraint_name = 'fk\\\\' AND column_name = 'child\\\\'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesRejectUnmatchedCompositeForeignKey(t *testing.T) {
	_, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_legacy", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "SET_NULL"},
			{constraintName: "fk_legacy", columnName: "z", referDBName: "db", referTableName: "parent", referColumnName: "z", onDelete: "CASCADE", onUpdate: "SET_NULL"},
		},
	}, "create table child (a int, z int, constraint fk_legacy foreign key (a, z) references parent (z, a))")
	require.ErrorContains(t, err, "cannot reconcile column order")
}

func TestLegacyForeignKeyMetadataUpdatesBackfillUnnamedForeignKey(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "catalog-generated-name", columnName: "parent_id", referDBName: "", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	}, "create table child (parent_id int, foreign key (parent_id) references parent (id))")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-generated-name' AND column_name = 'parent_id'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesLeaveAmbiguousUnnamedForeignKeysToCatalog(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "catalog-fk-a", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "catalog-fk-b", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	}, "create table child (parent_id int, foreign key (parent_id) references parent (id))")
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'CASCADE' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-fk-a' AND column_name = 'parent_id'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-fk-b' AND column_name = 'parent_id'",
	}, legacyForeignKeyMigrationUpdatesForAssertion(updates))
}

func TestLegacyForeignKeyMetadataUpdatesRejectAmbiguousUnnamedCompositeForeignKeys(t *testing.T) {
	_, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db",
		table:    "child",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "catalog-fk-a", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "catalog-fk-a", columnName: "z", referDBName: "db", referTableName: "parent", referColumnName: "z", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "catalog-fk-b", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "catalog-fk-b", columnName: "z", referDBName: "db", referTableName: "parent", referColumnName: "z", onDelete: "CASCADE", onUpdate: "CASCADE"},
		},
	}, "create table child (a int, z int, foreign key (z, a) references parent (z, a))")
	require.ErrorContains(t, err, "cannot reconcile column order")
}

func TestLegacyForeignKeyShowCreateSQLQuotesIdentifiers(t *testing.T) {
	definition := legacyForeignKeyTableDefinition{database: "db`name", table: "child`name"}
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		require.Equal(t, "SHOW CREATE TABLE `db``name`.`child``name`", sql)
		return newShowCreateTableResult(t, "child`name", "create table `child``name` (id int)"), nil
	})
	createSQL, err := getLegacyForeignKeyShowCreateSQL(9, txnExecutor, definition)
	require.NoError(t, err)
	require.Equal(t, "create table `child``name` (id int)", createSQL)
}

func TestLegacyForeignKeyHistoricalCreateSQLQuotesCatalogValues(t *testing.T) {
	definition := legacyForeignKeyTableDefinition{database: "db'name", table: `child\name`}
	txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		require.Equal(t,
			"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE account_id = 9 AND reldatabase = 'db''name' AND relname = 'child\\\\name'",
			sql,
		)
		return newHistoricalCreateSQLResult(t, "create table `child\\name` (id int)"), nil
	})
	createSQL, err := getLegacyForeignKeyHistoricalCreateSQL(9, txnExecutor, definition)
	require.NoError(t, err)
	require.Equal(t, "create table `child\\name` (id int)", createSQL)
}

func newVersionTxnExecutor(t *testing.T, mocker func(string) (executor.Result, error)) executor.TxnExecutor {
	t.Helper()
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return executor.NewMemTxnExecutor(mocker, txnOperator)
}

func newLegacyForeignKeyDefinitionResult(t *testing.T) executor.Result {
	return newLegacyForeignKeyDefinitionResultForDefinitions(t, []legacyForeignKeyTableDefinition{
		{
			database: "db",
			table:    "child",
			foreignKeys: []legacyForeignKeyCatalogRow{
				{constraintName: "fk_child_parent", columnName: "child_id", referDBName: "db", referTableName: "parent", referColumnName: "child_id", onDelete: "CASCADE", onUpdate: "SET_NULL"},
				{constraintName: "fk_child_parent", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "parent_id", onDelete: "CASCADE", onUpdate: "SET_NULL"},
			},
		},
	})
}

func newLegacyForeignKeyDefinitionResultForDefinitions(t *testing.T, definitions []legacyForeignKeyTableDefinition) executor.Result {
	t.Helper()
	rows := make([]legacyForeignKeyTableDefinition, 0)
	for _, definition := range definitions {
		for _, foreignKey := range definition.foreignKeys {
			rows = append(rows, legacyForeignKeyTableDefinition{
				database:    definition.database,
				table:       definition.table,
				foreignKeys: []legacyForeignKeyCatalogRow{foreignKey},
			})
		}
	}
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(len(rows))
	values := make([][]string, 9)
	for _, row := range rows {
		foreignKey := row.foreignKeys[0]
		values[0] = append(values[0], row.database)
		values[1] = append(values[1], row.table)
		values[2] = append(values[2], foreignKey.constraintName)
		values[3] = append(values[3], foreignKey.columnName)
		values[4] = append(values[4], foreignKey.referDBName)
		values[5] = append(values[5], foreignKey.referTableName)
		values[6] = append(values[6], foreignKey.referColumnName)
		values[7] = append(values[7], foreignKey.onDelete)
		values[8] = append(values[8], foreignKey.onUpdate)
	}
	for column, values := range values {
		if err := executor.AppendStringRows(result, column, values); err != nil {
			t.Fatalf("append legacy definition column %d: %v", column, err)
		}
	}
	return result.GetResult()
}

func newShowCreateTableResult(t *testing.T, tableName, createSQL string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(1)
	for column, value := range []string{tableName, createSQL} {
		if err := executor.AppendStringRows(result, column, []string{value}); err != nil {
			t.Fatalf("append SHOW CREATE TABLE column %d: %v", column, err)
		}
	}
	return result.GetResult()
}

func newProtocolVersionResultValue(t *testing.T, value string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{value}))
	return result.GetResult()
}

func newHistoricalCreateSQLResult(t *testing.T, createSQL string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	if err := executor.AppendStringRows(result, 0, []string{createSQL}); err != nil {
		t.Fatalf("append historical CREATE definition: %v", err)
	}
	return result.GetResult()
}

func newLegacyForeignKeyIndexResult(t *testing.T, rows [][]string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(len(rows))
	for column := 0; column < 4; column++ {
		values := make([]string, len(rows))
		for row := range rows {
			values[row] = rows[row][column]
		}
		if err := executor.AppendStringRows(result, column, values); err != nil {
			t.Fatalf("append legacy index column %d: %v", column, err)
		}
	}
	return result.GetResult()
}

func legacyForeignKeyMigrationUpdatesForAssertion(updates []string) []string {
	ret := make([]string, 0, len(updates))
	for _, update := range updates {
		if strings.HasPrefix(update, "SELECT ") {
			continue
		}
		if originStart := strings.Index(update, ", on_delete_origin = '"); originStart >= 0 {
			if whereStart := strings.Index(update[originStart:], " WHERE constraint_id"); whereStart >= 0 {
				update = update[:originStart] + update[originStart+whereStart:]
			}
		}
		ret = append(ret, update)
	}
	return ret
}

func TestEnsureInformationSchemaCharacterSetsTableIsIdempotent(t *testing.T) {
	entry := ensureInformationSchemaCharacterSetsTable()
	exists := false
	stub := gostub.Stub(&versions.CheckTableDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, table string) (bool, error) {
		require.Equal(t, uint32(42), accountID)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "character_sets", table)
		return exists, nil
	})
	defer stub.Reset()

	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		if sql == entry.UpgSql {
			exists = true
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, entry.Upgrade(txn, 42))
	require.Equal(t, []string{sysview.InformationSchemaCharacterSetsDDL}, executed)

	executed = nil
	require.NoError(t, entry.Upgrade(txn, 42))
	require.Empty(t, executed)
}

func TestPopulateInformationSchemaCharacterSetsIsIdempotent(t *testing.T) {
	entry := populateInformationSchemaCharacterSets()
	populated := false
	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		switch {
		case strings.HasPrefix(sql, "SELECT 1 FROM information_schema.CHARACTER_SETS"):
			if populated {
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			}
		case sql == entry.UpgSql:
			populated = true
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, entry.Upgrade(txn, 42))
	require.Len(t, executed, 3)
	require.True(t, strings.HasPrefix(executed[0], "SELECT 1 FROM information_schema.CHARACTER_SETS"))
	require.Equal(t, entry.PreSql, executed[1])
	require.Equal(t, entry.UpgSql, executed[2])

	executed = nil
	require.NoError(t, entry.Upgrade(txn, 42))
	require.Len(t, executed, 1)
	require.True(t, strings.HasPrefix(executed[0], "SELECT 1 FROM information_schema.CHARACTER_SETS"))
}

func TestRetireKafkaSinkDaemonTasks(t *testing.T) {
	const filter = "task_metadata_executor = 4 and task_status in (0, 1, 3, 6, 7, 9)"
	require.Equal(t, filter, activeKafkaSinkTaskFilter())
	require.Equal(t, catalog.MOTaskDB, retireKafkaSinkDaemonTasks.Schema)
	require.Equal(t, catalog.MOSysDaemonTask, retireKafkaSinkDaemonTasks.TableName)
	require.Equal(t, versions.MODIFY_METADATA, retireKafkaSinkDaemonTasks.UpgType)
	require.Equal(t,
		"update mo_task.sys_daemon_task set task_status = 8, update_at = current_timestamp() where "+filter,
		retireKafkaSinkDaemonTasks.UpgSql,
	)

	checkSQL := "select 1 from mo_task.sys_daemon_task where " + filter + " limit 1"
	hasActiveTask := true
	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		switch sql {
		case checkSQL:
			if hasActiveTask {
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			}
		case retireKafkaSinkDaemonTasks.UpgSql:
			hasActiveTask = false
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, retireKafkaSinkDaemonTasks.Upgrade(txn, catalog.System_Account))
	require.Equal(t, []string{checkSQL, retireKafkaSinkDaemonTasks.UpgSql}, executed)

	executed = nil
	require.NoError(t, retireKafkaSinkDaemonTasks.Upgrade(txn, catalog.System_Account))
	require.Equal(t, []string{checkSQL}, executed,
		"an already-retired cluster must not execute the update again")
}

func TestCleanupLegacyOrphanSQLTaskChildren(t *testing.T) {
	entry := cleanupLegacyOrphanSQLTaskChildren
	checkSQL := "select 1 from mo_task.sys_async_task where " +
		legacyOrphanSQLTaskChildPredicate + " limit 1"

	require.Equal(t, catalog.MOTaskDB, entry.Schema)
	require.Equal(t, catalog.MOSysAsyncTask, entry.TableName)
	require.Equal(t, versions.MODIFY_METADATA, entry.UpgType)
	require.Equal(t, int64(defines.MORPCVersion42), entry.RequiredProtocolVersion)
	require.Equal(t,
		"delete from mo_task.sys_async_task where "+legacyOrphanSQLTaskChildPredicate,
		entry.UpgSql)
	require.Contains(t, entry.UpgSql, "task_parent_id like 'sql-task:%'")
	require.Contains(t, entry.UpgSql, "from mo_task.sql_task ")
	require.Contains(t, entry.UpgSql, "from mo_task.sql_task_run")

	t.Run("older CN blocks cleanup", func(t *testing.T) {
		deleted := false
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			switch sql {
			case checkSQL:
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newProtocolVersionResultValue(t,
					`{"method":"GETPROTOCOLVERSION","result":"cn-a:42,cn-b:41"}`), nil
			case entry.UpgSql:
				deleted = true
			}
			return executor.Result{}, nil
		})

		require.Error(t, entry.Upgrade(txn, catalog.System_Account))
		require.False(t, deleted)
	})

	t.Run("empty snapshot still waits for older CN", func(t *testing.T) {
		var executed []string
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			executed = append(executed, sql)
			if sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
				return newProtocolVersionResultValue(t,
					`{"method":"GETPROTOCOLVERSION","result":"cn-a:42,cn-b:41"}`), nil
			}
			return executor.Result{}, nil
		})

		require.Error(t, entry.Upgrade(txn, catalog.System_Account))
		require.Equal(t, []string{
			checkSQL,
			"SELECT mo_ctl('cn', 'GetProtocolVersion', '')",
		}, executed)
	})

	t.Run("all CNs ready and cleanup is idempotent", func(t *testing.T) {
		hasOrphan := true
		var executed []string
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			executed = append(executed, sql)
			switch sql {
			case checkSQL:
				if hasOrphan {
					result := executor.NewMemResult(nil, nil)
					result.NewBatchWithRowCount(1)
					return result.GetResult(), nil
				}
			case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newProtocolVersionResultValue(t,
					`{"method":"GETPROTOCOLVERSION","result":"cn-a:42,cn-b:42"}`), nil
			case entry.UpgSql:
				hasOrphan = false
			}
			return executor.Result{}, nil
		})

		require.NoError(t, entry.Upgrade(txn, catalog.System_Account))
		require.Equal(t, []string{
			checkSQL,
			"SELECT mo_ctl('cn', 'GetProtocolVersion', '')",
			entry.UpgSql,
		}, executed)

		executed = nil
		require.NoError(t, entry.Upgrade(txn, catalog.System_Account))
		require.Equal(t, []string{
			checkSQL,
			"SELECT mo_ctl('cn', 'GetProtocolVersion', '')",
		}, executed)
	})
}
