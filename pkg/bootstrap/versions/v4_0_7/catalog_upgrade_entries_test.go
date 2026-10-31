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
package v4_0_7

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestConsolidatedTenantEntriesKeepWriterProtocolGates(t *testing.T) {
	require.Equal(t, "4.0.7", Handler.Metadata().Version)
	require.Equal(t, uint32(50), Handler.Metadata().VersionOffset)
	require.Len(t, tenantUpgEntries, 36)
	require.Len(t, clusterUpgEntries, 14)
	require.Contains(t, upgradeInformationSchemaStatistics().UpgSql, "sub_part")
	for _, tc := range []struct {
		table   string
		minimum int64
	}{
		{"CHARACTER_SETS", defines.MORPCVersion106},
		{"STATISTICS", defines.MORPCVersion107},
		{"COLLATIONS", defines.MORPCVersion118},
		{"SCHEMATA", defines.MORPCVersion118},
		{"COLUMNS", defines.MORPCVersion118},
		{"TABLES", defines.MORPCVersion118},
		{"VIEWS", defines.MORPCVersion126},
	} {
		t.Run(tc.table, func(t *testing.T) {
			for _, entry := range tenantUpgEntries {
				if entry.Schema == sysview.InformationDBConst && entry.TableName == tc.table {
					require.GreaterOrEqual(t, entry.RequiredProtocolVersion, tc.minimum)
					return
				}
			}
			t.Fatalf("missing consolidated entry for %s", tc.table)
		})
	}
}

func TestAddCdcWatermarkSourceTableIDWaitsForCompatibleWriters(t *testing.T) {
	entry := cdcWatermarkColumn("source_table_id", "bigint unsigned not null default 0 after watermark")
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
			protocols: `{"method":"GETPROTOCOLVERSION","result":"cn-a:106,cn-b:105"}`,
			wantErr:   true,
		},
		{
			name:            "all CNs support explicit-column inserts",
			protocols:       `{"method":"GETPROTOCOLVERSION","result":"cn-a:106,cn-b:106"}`,
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
		{name: "existing index", ddl: "create table t (account_id int, key IDX_ACCOUNT_ID(account_id))", want: true},
		{name: "same name with different column", ddl: "create table t (a int, key IDX_ACCOUNT_ID(a))", wantErr: true},
		{name: "same name but unique", ddl: "create table t (account_id int, unique key IDX_ACCOUNT_ID(account_id))", wantErr: true},
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

func TestColumnsAndTablesAreIncludedInTenantUpgrade(t *testing.T) {
	want := map[string]string{
		"COLUMNS": sysview.InformationSchemaColumnsDDL,
		"TABLES":  sysview.InformationSchemaTablesV118DDL,
	}
	for _, entry := range tenantUpgEntries {
		ddl, ok := want[entry.TableName]
		if !ok || entry.Schema != sysview.InformationDBConst {
			continue
		}
		require.Equal(t, versions.MODIFY_VIEW, entry.UpgType)
		require.Equal(t, strings.Replace(ddl, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1), entry.UpgSql)
		delete(want, entry.TableName)
	}
	require.Empty(t, want, "the tenant upgrade must refresh both persisted views")
}

func TestInformationSchemaMetadataVisibilityUpgradeChecks(t *testing.T) {
	views := []struct {
		entry versions.UpgradeEntry
		ddl   string
	}{
		{versions.InformationSchemaTablesUpgradeEntry(), sysview.InformationSchemaTablesV118DDL},
		{versions.InformationSchemaColumnsUpgradeEntry(), sysview.InformationSchemaColumnsDDL},
		{upgradeInformationSchemaStatistics(), sysview.InformationSchemaStatisticsPrefixSubPartDDL},
		{upgradeInformationSchemaTableConstraints(), sysview.InformationSchemaTableConstraintsDDL},
		{upgradeInformationSchemaKeyColumnUsage(), sysview.InformationSchemaKeyColumnUsageDDL},
		{upgradeInformationSchemaReferentialConstraints(), sysview.InformationSchemaReferentialConstraintsDDL},
		{upgradeInformationSchemaCheckConstraints(), sysview.InformationSchemaCheckConstraintsDDL},
		{upgradeInformationSchemaViews(), sysview.InformationSchemaViewsDDL},
		{upgradeInformationSchemaMetadataVisibilityView("PARTITIONS", sysview.InformationSchemaPartitionsDDL), sysview.InformationSchemaPartitionsDDL},
		{upgradeInformationSchemaMetadataVisibilityView("SCHEMATA", sysview.InformationSchemaSchemataDDL), sysview.InformationSchemaSchemataDDL},
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
			t.Run(view.entry.TableName+"/"+state.name, func(t *testing.T) {
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
					require.True(t, viewName == view.entry.TableName || viewName == strings.ToLower(view.entry.TableName))
					return state.exists, state.definition, state.checkErr
				}
				defer func() { versions.CheckViewDefinition = oldCheck }()

				ok, err := view.entry.CheckFunc(nil, 42)
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
	allocatorIndex := addIcebergCatalogIDAllocatorIndex()
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
				require.Equal(t, strings.ToLower(test.viewName), strings.ToLower(viewName))
				return test.exists, test.viewDef, nil
			})
			defer stub.Reset()

			matched, err := test.entry.CheckFunc(nil, 42)
			require.NoError(t, err)
			require.False(t, matched)
		})
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

	if err := upgradeLegacyForeignKeyMetadataWhere(context.Background(), 9, txnExecutor, ""); err != nil {
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

	require.NoError(t, upgradeLegacyForeignKeyMetadataWhere(context.Background(), 9, txnExecutor, ""))
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

	require.NoError(t, upgradeLegacyForeignKeyMetadataWhere(context.Background(), 9, txnExecutor, ""))
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

	require.NoError(t, upgradeLegacyForeignKeyMetadataWhere(context.Background(), 9, txnExecutor, ""))
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
