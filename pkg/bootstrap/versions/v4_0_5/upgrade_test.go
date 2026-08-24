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

package v4_0_5

import (
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 8 {
		t.Fatalf("expected 8 tenant upgrades, got %d", len(tenantUpgEntries))
	}
	allocator := tenantUpgEntries[0]
	if allocator.UpgType != versions.MODIFY_COLUMN || allocator.TableName != "mo_iceberg_catalogs" {
		t.Fatalf("unexpected catalog allocator upgrade: %+v", allocator)
	}
	allocatorSQL := strings.ToLower(allocator.UpgSql)
	for _, want := range []string{"alter table", "modify catalog_id", "auto_increment"} {
		if !strings.Contains(allocatorSQL, want) {
			t.Fatalf("catalog allocator upgrade SQL missing %q: %s", want, allocator.UpgSql)
		}
	}
	if strings.Contains(allocatorSQL, "drop primary key") {
		t.Fatalf("catalog allocator upgrade should preserve the account-scoped primary key: %s", allocator.UpgSql)
	}
	for _, entry := range tenantUpgEntries[1:4] {
		if entry.UpgType != versions.ADD_COLUMN {
			t.Fatalf("%s should be ADD_COLUMN", entry.TableName)
		}
		lower := strings.ToLower(entry.UpgSql)
		for _, want := range []string{"alter table", "mo_catalog.mo_iceberg_orphan_files", "add column"} {
			if !strings.Contains(lower, want) {
				t.Fatalf("orphan cleanup upgrade SQL missing %q: %s", want, entry.UpgSql)
			}
		}
		if strings.Contains(lower, "drop ") {
			t.Fatalf("orphan cleanup upgrade SQL must not drop objects: %s", entry.UpgSql)
		}
	}
}

func TestInformationSchemaTenantUpgradeEntries(t *testing.T) {
	views := []struct {
		name            string
		ddl             string
		legacyBaseTable bool
	}{
		{name: "TABLES", ddl: sysview.InformationSchemaTablesDDL},
		{name: "COLUMNS", ddl: sysview.InformationSchemaColumnsDDL},
		{name: "STATISTICS", ddl: sysview.InformationSchemaStatisticsDDL},
		{name: "TABLE_CONSTRAINTS", ddl: sysview.InformationSchemaTableConstraintsDDL, legacyBaseTable: true},
	}

	for i, view := range views {
		entry := tenantUpgEntries[4+i]
		if entry.Schema != sysview.InformationDBConst || entry.TableName != view.name || entry.UpgType != versions.MODIFY_VIEW {
			t.Fatalf("unexpected information_schema.%s upgrade: %+v", view.name, entry)
		}
		definitionSQL := entry.UpgSql
		if view.legacyBaseTable {
			definitionSQL = entry.PostSql
		}
		if definitionSQL != view.ddl {
			t.Fatalf("%s upgrade does not use the current view definition: %s", view.name, definitionSQL)
		}
		for _, want := range []string{
			"relkind = 'temporary_table'",
			"rel_createsql",
			"mo_is_legacy_temporary_table",
			"[0-9a-f]{32}",
		} {
			if !strings.Contains(definitionSQL, want) {
				t.Fatalf("%s upgrade is missing legacy temporary-table compatibility %q: %s", view.name, want, definitionSQL)
			}
		}
		if view.legacyBaseTable {
			requireSQLContains(t, entry.PreSql, "drop table if exists information_schema."+strings.ToLower(view.name))
			requireSQLContains(t, entry.UpgSql, "drop view if exists information_schema."+strings.ToLower(view.name))
			continue
		}
		requireSQLContains(t, entry.PreSql, "drop view if exists information_schema."+strings.ToLower(view.name))
		if entry.PostSql != "" {
			t.Fatalf("%s regular view upgrade should not use post-sql: %s", view.name, entry.PostSql)
		}
	}
}

func requireSQLContains(t *testing.T, sql, want string) {
	t.Helper()
	if !strings.Contains(strings.ToLower(sql), want) {
		t.Fatalf("SQL %q does not contain %q", sql, want)
	}
}

func TestInformationSchemaLegacyTableUpgradeIsOrderedAndIdempotent(t *testing.T) {
	entry := upgradeInformationSchemaViewFromLegacyTable("TABLE_CONSTRAINTS", sysview.InformationSchemaTableConstraintsDDL)
	upgraded := false
	stub := gostub.Stub(&versions.CheckViewDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
		if accountID != 42 || schema != sysview.InformationDBConst || viewName != "TABLE_CONSTRAINTS" {
			t.Fatalf("unexpected view check arguments: account=%d schema=%s view=%s", accountID, schema, viewName)
		}
		if upgraded {
			return true, sysview.InformationSchemaTableConstraintsDDL, nil
		}
		return false, "", nil
	})
	defer stub.Reset()

	var executed []string
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnExecutor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		if sql == entry.PostSql {
			upgraded = true
		}
		return executor.Result{}, nil
	}, txnOperator)

	if err := entry.Upgrade(txnExecutor, 42); err != nil {
		t.Fatalf("first upgrade: %v", err)
	}
	if want := []string{entry.PreSql, entry.UpgSql, entry.PostSql}; !equalStrings(executed, want) {
		t.Fatalf("unexpected execution order: got %q, want %q", executed, want)
	}

	executed = nil
	if err := entry.Upgrade(txnExecutor, 42); err != nil {
		t.Fatalf("second upgrade: %v", err)
	}
	if len(executed) != 0 {
		t.Fatalf("matching view should skip DDL, executed %q", executed)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestInformationSchemaTenantUpgradeCheckFunc(t *testing.T) {
	checkErr := errors.New("check view definition")
	views := []struct {
		name string
		ddl  string
	}{
		{name: "TABLES", ddl: sysview.InformationSchemaTablesDDL},
		{name: "COLUMNS", ddl: sysview.InformationSchemaColumnsDDL},
		{name: "STATISTICS", ddl: sysview.InformationSchemaStatisticsDDL},
		{name: "TABLE_CONSTRAINTS", ddl: sysview.InformationSchemaTableConstraintsDDL},
	}

	for i, view := range views {
		t.Run(view.name, func(t *testing.T) {
			tests := []struct {
				name       string
				exists     bool
				definition string
				checkErr   error
				wantOK     bool
			}{
				{name: "matching view", exists: true, definition: view.ddl, wantOK: true},
				{name: "mismatched view", exists: true, definition: "old view definition"},
				{name: "missing view", definition: view.ddl},
				{name: "check error", checkErr: checkErr},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountID uint32, schema, viewName string) (bool, string, error) {
						if txn != nil || accountID != 42 || schema != sysview.InformationDBConst || viewName != view.name {
							t.Fatalf("unexpected view check arguments: txn=%v accountID=%d schema=%q view=%q", txn, accountID, schema, viewName)
						}
						return test.exists, test.definition, test.checkErr
					})
					defer stubs.Reset()

					ok, err := tenantUpgEntries[4+i].CheckFunc(nil, 42)
					if ok != test.wantOK {
						t.Fatalf("unexpected check result: got %v, want %v", ok, test.wantOK)
					}
					if !errors.Is(err, test.checkErr) {
						t.Fatalf("unexpected check error: got %v, want %v", err, test.checkErr)
					}
				})
			}
		})
	}
}

func TestIcebergOrphanCleanupVersionHandleMetadataAndClusterNoop(t *testing.T) {
	meta := Handler.Metadata()
	if meta.Version != "4.0.5" || meta.MinUpgradeVersion != "4.0.4" || meta.UpgradeTenant != versions.Yes {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if meta.VersionOffset != uint32(len(tenantUpgEntries)+len(clusterUpgEntries)) {
		t.Fatalf("unexpected version offset: %+v", meta)
	}
	err := Handler.HandleCreateFrameworkDeps(nil)
	if err == nil || !strings.Contains(err.Error(), "Only v1.2.0") {
		t.Fatalf("unexpected framework deps error: %v", err)
	}
}
