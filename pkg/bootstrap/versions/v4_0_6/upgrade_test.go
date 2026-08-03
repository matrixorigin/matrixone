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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestMongoDBCatalogUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 4)
	require.Empty(t, clusterUpgEntries)
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries[:2] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}
}

func TestForeignKeyMetadataTenantUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 4)

	keyColumnUsage := tenantUpgEntries[2]
	require.Equal(t, versions.CREATE_VIEW, keyColumnUsage.UpgType)
	require.Equal(t, "KEY_COLUMN_USAGE", keyColumnUsage.TableName)
	require.Equal(t, sysview.InformationSchemaKeyColumnUsageDDL, keyColumnUsage.UpgSql)
	require.Contains(t, strings.ToLower(keyColumnUsage.PreSql), "drop table if exists information_schema.key_column_usage")

	referentialConstraints := tenantUpgEntries[3]
	require.Equal(t, versions.MODIFY_VIEW, referentialConstraints.UpgType)
	require.Equal(t, "REFERENTIAL_CONSTRAINTS", referentialConstraints.TableName)
	require.Equal(t, sysview.InformationSchemaReferentialConstraintsDDL, referentialConstraints.UpgSql)
	require.Contains(t, strings.ToLower(referentialConstraints.PreSql), "drop view if exists information_schema.referential_constraints")
}

func TestLegacyForeignKeyMetadataUpdatesPreserveOrderAndActions(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db'one",
		table:    "child",
		createSQL: "create table child (a int, b int, constraint fk_default foreign key (b, a) references parent (b, a), " +
			"constraint fk_restrict foreign key (a) references parent (a) on delete restrict on update restrict)",
	})
	require.NoError(t, err)
	require.Len(t, updates, 3)

	for _, expected := range []string{
		"constraint_id = 1, on_delete = 'NO_ACTION', on_update = 'NO_ACTION'",
		"constraint_name = 'fk_default' AND column_name = 'b'",
		"constraint_id = 2, on_delete = 'NO_ACTION', on_update = 'NO_ACTION'",
		"constraint_name = 'fk_default' AND column_name = 'a'",
		"constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT'",
		"constraint_name = 'fk_restrict' AND column_name = 'a'",
		"db_name = 'db''one'",
	} {
		require.Contains(t, strings.Join(updates, "\n"), expected)
	}
}

func TestVersionHandleMetadata(t *testing.T) {
	meta := Handler.Metadata()
	require.Equal(t, "4.0.6", meta.Version)
	require.Equal(t, "4.0.5", meta.MinUpgradeVersion)
	require.Equal(t, versions.Yes, meta.UpgradeTenant)
	require.Equal(t, versions.Yes, meta.UpgradeCluster)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), meta.VersionOffset)
	require.Empty(t, clusterUpgEntries)
}

func TestTenantViewDefinitionChecks(t *testing.T) {
	entries := []versions.UpgradeEntry{
		upgradeInformationSchemaKeyColumnUsage(),
		upgradeInformationSchemaReferentialConstraints(),
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
			default:
				return false, "", errors.New("unexpected view")
			}
		})
		defer stub.Reset()

		var executed []string
		txnExecutor := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			executed = append(executed, sql)
			return executor.Result{}, nil
		})

		if err := Handler.Prepare(context.Background(), txnExecutor, true); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if err := Handler.HandleTenantUpgrade(context.Background(), 9, txnExecutor); err != nil {
			t.Fatalf("tenant upgrade: %v", err)
		}
		if len(executed) != 1 || executed[0] != legacyForeignKeyTableDefinitionsSQL {
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
		updates = append(updates, sql)
		return executor.Result{}, nil
	})

	if err := upgradeLegacyForeignKeyMetadata(context.Background(), 9, txnExecutor); err != nil {
		t.Fatalf("upgrade legacy foreign-key metadata: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("expected two metadata updates, got %d: %v", len(updates), updates)
	}
	for _, update := range updates {
		if !strings.Contains(update, "constraint_id =") || !strings.Contains(update, "constraint_name = 'fk_child_parent'") {
			t.Fatalf("unexpected update: %s", update)
		}
	}
}

func TestLegacyForeignKeyMetadataUpdatesRejectInvalidDefinitions(t *testing.T) {
	for _, createSQL := range []string{
		"select 1",
		"create table child (a int); create table another_child (a int)",
	} {
		t.Run(createSQL, func(t *testing.T) {
			_, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
				database:  "db",
				table:     "child",
				createSQL: createSQL,
			})
			if err == nil {
				t.Fatalf("expected invalid persisted definition to fail: %s", createSQL)
			}
		})
	}
}

func TestLegacyForeignKeyMetadataUpdatesBackfillAlterTableForeignKey(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database:  "db",
		table:     "child",
		createSQL: "create table child (b int, a int)",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "fk_added_by_alter", columnName: "a", referDBName: "db", referTableName: "parent", referColumnName: "a", onDelete: "CASCADE", onUpdate: "SET_NULL"},
			{constraintName: "fk_added_by_alter", columnName: "b", referDBName: "db", referTableName: "parent", referColumnName: "b", onDelete: "CASCADE", onUpdate: "SET_NULL"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'a'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 2, on_delete = 'CASCADE', on_update = 'SET_NULL' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'fk_added_by_alter' AND column_name = 'b'",
	}, updates)
}

func TestLegacyForeignKeyMetadataUpdatesBackfillUnnamedForeignKey(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database:  "db",
		table:     "child",
		createSQL: "create table child (parent_id int, foreign key (parent_id) references parent (id))",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "catalog-generated-name", columnName: "parent_id", referDBName: "", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'NO_ACTION', on_update = 'NO_ACTION' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-generated-name' AND column_name = 'parent_id'",
	}, updates)
}

func TestLegacyForeignKeyMetadataUpdatesLeaveAmbiguousUnnamedForeignKeysToCatalog(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database:  "db",
		table:     "child",
		createSQL: "create table child (parent_id int, foreign key (parent_id) references parent (id))",
		foreignKeys: []legacyForeignKeyCatalogRow{
			{constraintName: "catalog-fk-a", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "CASCADE", onUpdate: "CASCADE"},
			{constraintName: "catalog-fk-b", columnName: "parent_id", referDBName: "db", referTableName: "parent", referColumnName: "id", onDelete: "RESTRICT", onUpdate: "RESTRICT"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'CASCADE', on_update = 'CASCADE' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-fk-a' AND column_name = 'parent_id'",
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = 1, on_delete = 'NO_ACTION', on_update = 'NO_ACTION' WHERE constraint_id = 0 AND db_name = 'db' AND table_name = 'child' AND constraint_name = 'catalog-fk-b' AND column_name = 'parent_id'",
	}, updates)
}

func TestReferenceActionName(t *testing.T) {
	for _, test := range []struct {
		action tree.ReferenceOptionType
		want   string
	}{
		{tree.REFERENCE_OPTION_CASCADE, "CASCADE"},
		{tree.REFERENCE_OPTION_SET_NULL, "SET_NULL"},
		{tree.REFERENCE_OPTION_NO_ACTION, "NO_ACTION"},
		{tree.REFERENCE_OPTION_SET_DEFAULT, "SET_DEFAULT"},
		{tree.REFERENCE_OPTION_RESTRICT, "RESTRICT"},
		{tree.ReferenceOptionType(-1), "NO_ACTION"},
	} {
		if got := referenceActionName(test.action); got != test.want {
			t.Fatalf("referenceActionName(%d) = %q, want %q", test.action, got, test.want)
		}
	}
}

func newVersionTxnExecutor(t *testing.T, mocker func(string) (executor.Result, error)) executor.TxnExecutor {
	t.Helper()
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return executor.NewMemTxnExecutor(mocker, txnOperator)
}

func newLegacyForeignKeyDefinitionResult(t *testing.T) executor.Result {
	t.Helper()
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
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(2)
	for column, values := range [][]string{
		{"db", "db"},
		{"child", "child"},
		{
			"create table child (parent_id int, child_id int, constraint fk_child_parent foreign key (child_id, parent_id) references parent (child_id, parent_id) on delete cascade on update set null)",
			"create table child (parent_id int, child_id int, constraint fk_child_parent foreign key (child_id, parent_id) references parent (child_id, parent_id) on delete cascade on update set null)",
		},
		{"fk_child_parent", "fk_child_parent"},
		{"child_id", "parent_id"},
		{"db", "db"},
		{"parent", "parent"},
		{"child_id", "parent_id"},
		{"CASCADE", "CASCADE"},
		{"SET_NULL", "SET_NULL"},
	} {
		if err := executor.AppendStringRows(result, column, values); err != nil {
			t.Fatalf("append legacy definition column %d: %v", column, err)
		}
	}
	return result.GetResult()
}
