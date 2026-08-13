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
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 12)
	require.Len(t, clusterUpgEntries, 4)
	require.Equal(t, retireKafkaSinkDaemonTasks.UpgSql, clusterUpgEntries[0].UpgSql)
	require.Equal(t, catalog.MO_VIEW_DEPENDENCIES, clusterUpgEntries[1].TableName)
	require.Equal(t, catalog.MO_VIEW_REFRESH, clusterUpgEntries[2].TableName)
	for _, entry := range clusterUpgEntries[1:3] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create cluster table mo_catalog.mo_view_")
		require.NotContains(t, strings.ToLower(entry.UpgSql), "\n\t\taccount_id int")
	}
	require.Equal(t, versions.MODIFY_METADATA, clusterUpgEntries[3].UpgType)
	require.Contains(t, strings.ToLower(clusterUpgEntries[3].UpgSql), "replace into")
	require.Contains(t, clusterUpgEntries[3].UpgSql, catalog.ViewRefreshStatusRevalidateScan)
	require.Contains(t, clusterUpgEntries[3].UpgSql, catalog.MoViewDependenciesColumns)
	require.Contains(t, catalog.MoViewDependenciesDDL,
		"primary key(account_id, target_relation_id, dependency_ordinal)")
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries[:2] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}
	characterSets := tenantUpgEntries[7]
	require.Equal(t, sysview.InformationDBConst, characterSets.Schema)
	require.Equal(t, "CHARACTER_SETS", characterSets.TableName)
	require.Equal(t, versions.MODIFY_METADATA, characterSets.UpgType)
	require.Equal(t, sysview.InformationSchemaCharacterSetsData, characterSets.UpgSql)
	require.Contains(t, strings.ToLower(characterSets.PreSql), "delete from information_schema.character_sets")
	columns := tenantUpgEntries[8]
	require.Equal(t, sysview.InformationDBConst, columns.Schema)
	require.Equal(t, "COLUMNS", columns.TableName)
	require.Equal(t, versions.MODIFY_VIEW, columns.UpgType)
	require.Equal(t, sysview.InformationSchemaColumnsDDL, columns.UpgSql)
	require.Contains(t, strings.ToLower(columns.PreSql), "drop view if exists information_schema.columns")
	checkConstraints := tenantUpgEntries[9]
	require.Equal(t, sysview.InformationDBConst, checkConstraints.Schema)
	require.Equal(t, "CHECK_CONSTRAINTS", checkConstraints.TableName)
	require.Equal(t, versions.CREATE_VIEW, checkConstraints.UpgType)
	require.Equal(t, sysview.InformationSchemaCheckConstraintsDDL, checkConstraints.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion16), checkConstraints.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(checkConstraints.PreSql), "drop view if exists information_schema.check_constraints")
	tableConstraints := tenantUpgEntries[10]
	require.Equal(t, sysview.InformationDBConst, tableConstraints.Schema)
	require.Equal(t, "TABLE_CONSTRAINTS", tableConstraints.TableName)
	require.Equal(t, versions.MODIFY_VIEW, tableConstraints.UpgType)
	require.Equal(t, sysview.InformationSchemaTableConstraintsDDL, tableConstraints.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion16), tableConstraints.RequiredProtocolVersion)
	require.Contains(t, strings.ToLower(tableConstraints.PreSql), "drop view if exists information_schema.table_constraints")
	hideInternalColumns := tenantUpgEntries[11]
	require.Equal(t, sysview.InformationDBConst, hideInternalColumns.Schema)
	require.Equal(t, "COLUMNS", hideInternalColumns.TableName)
	require.Equal(t, versions.MODIFY_VIEW, hideInternalColumns.UpgType)
	require.Equal(t, sysview.InformationSchemaColumnsDDL, hideInternalColumns.UpgSql)
	require.Contains(t, strings.ToLower(hideInternalColumns.PreSql), "drop view if exists information_schema.columns")
}

func TestForeignKeyMetadataTenantUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 12)

	for i, column := range []string{"referenced_index_name", "on_delete_origin", "on_update_origin"} {
		entry := tenantUpgEntries[2+i]
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, catalog.MOForeignKeys, entry.TableName)
		require.Contains(t, entry.UpgSql, "add column "+column)
	}

	keyColumnUsage := tenantUpgEntries[5]
	require.Equal(t, versions.CREATE_VIEW, keyColumnUsage.UpgType)
	require.Equal(t, "KEY_COLUMN_USAGE", keyColumnUsage.TableName)
	require.Equal(t, sysview.InformationSchemaKeyColumnUsageDDL, keyColumnUsage.UpgSql)
	require.Contains(t, strings.ToLower(keyColumnUsage.PreSql), "drop table if exists information_schema.key_column_usage")

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
}

func TestTenantViewDefinitionChecks(t *testing.T) {
	entries := []versions.UpgradeEntry{
		upgradeInformationSchemaKeyColumnUsage(),
		upgradeInformationSchemaReferentialConstraints(),
		upgradeInformationSchemaCheckConstraints(),
		upgradeInformationSchemaTableConstraints(),
	}

	for _, entry := range entries {
		t.Run(entry.TableName+"/match", func(t *testing.T) {
			stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
				if accountID != 42 || schema != sysview.InformationDBConst || viewName != entry.TableName {
					t.Fatalf("unexpected view check arguments: account=%d schema=%s view=%s", accountID, schema, viewName)
				}
				return true, entry.UpgSql, nil
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
	matched, err := entries[0].CheckFunc(nil, 42)
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
			case "TABLE_CONSTRAINTS":
				return true, sysview.InformationSchemaTableConstraintsDDL, nil
			case "COLUMNS":
				return true, sysview.InformationSchemaColumnsDDL, nil
			default:
				return false, "", errors.New("unexpected view")
			}
		})
		defer stub.Reset()

		var executed []string
		txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			if strings.Contains(strings.ToLower(sql), "getprotocolversion") {
				return newProtocolVersionResult(t), nil
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

func newProtocolVersionResult(t *testing.T) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	if err := executor.AppendStringRows(result, 0, []string{`{"method":"GETPROTOCOLVERSION","result":"cn-a:13, cn-b:13"}`}); err != nil {
		t.Fatalf("append protocol version result: %v", err)
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
