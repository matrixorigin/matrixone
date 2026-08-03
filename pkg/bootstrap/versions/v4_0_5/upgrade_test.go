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

	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) < 8 {
		t.Fatalf("expected at least 8 tenant upgrades, got %d", len(tenantUpgEntries))
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

func TestLifecycleCatalogUpgradeEntries(t *testing.T) {
	const existingTenantEntries = 8
	wantTenantTables := []string{
		"mo_lifecycle_bindings",
		"mo_lifecycle_datasets",
		"mo_lifecycle_ttl_receipts",
		"mo_lifecycle_restore_attempts",
		"mo_lifecycle_restore_chunks",
	}
	if got, want := len(tenantUpgEntries), existingTenantEntries+len(wantTenantTables); got != want {
		t.Fatalf("expected %d tenant upgrades, got %d", want, got)
	}
	for i, name := range wantTenantTables {
		entry := tenantUpgEntries[existingTenantEntries+i]
		if entry.Schema != "mo_catalog" || entry.TableName != name || entry.UpgType != versions.CREATE_NEW_TABLE {
			t.Fatalf("unexpected lifecycle tenant upgrade for %s: %+v", name, entry)
		}
		lower := strings.ToLower(entry.UpgSql)
		for _, required := range []string{"create table", "primary key"} {
			if !strings.Contains(lower, required) {
				t.Fatalf("%s DDL missing %q: %s", name, required, entry.UpgSql)
			}
		}
		for _, forbidden := range []string{"alter table mo_catalog.mo_tables", "alter table mo_catalog.mo_columns", "alter table mo_catalog.mo_stages"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s DDL must not change an existing catalog table: %s", name, entry.UpgSql)
			}
		}
	}

	if len(clusterUpgEntries) != 3 {
		t.Fatalf("expected three lifecycle cluster upgrades, got %d", len(clusterUpgEntries))
	}
	root := clusterUpgEntries[0]
	if root.Schema != "mo_catalog" || root.TableName != "mo_lifecycle_cleanup_roots" || root.UpgType != versions.CREATE_NEW_TABLE {
		t.Fatalf("unexpected lifecycle cleanup-root upgrade: %+v", root)
	}
	rootDDL := strings.ToLower(root.UpgSql)
	for _, required := range []string{"create cluster table", "primary key", "root_id", "attempt_id", "state_version"} {
		if !strings.Contains(rootDDL, required) {
			t.Fatalf("cleanup-root DDL missing %q: %s", required, root.UpgSql)
		}
	}
	activation := clusterUpgEntries[1]
	if activation.TableName != "mo_feature_registry" || activation.UpgType != versions.MODIFY_METADATA {
		t.Fatalf("unexpected lifecycle activation upgrade: %+v", activation)
	}
	for _, required := range []string{
		"lifecycle",
		"false",
		"archive_stages",
		"on duplicate key",
	} {
		if !strings.Contains(strings.ToLower(activation.UpgSql), required) {
			t.Fatalf("lifecycle activation SQL missing %q: %s", required, activation.UpgSql)
		}
	}
	coordinator := clusterUpgEntries[2]
	if coordinator.Schema != "mo_task" ||
		coordinator.TableName != "sys_cron_task" ||
		coordinator.UpgType != versions.MODIFY_METADATA {
		t.Fatalf("unexpected lifecycle coordinator upgrade: %+v", coordinator)
	}
	for _, required := range []string{
		"tae_object_lifecycle",
		"sys_cron_task",
		"on duplicate key",
	} {
		if !strings.Contains(strings.ToLower(coordinator.UpgSql), required) {
			t.Fatalf("lifecycle coordinator SQL missing %q: %s", required, coordinator.UpgSql)
		}
	}
}

func TestLifecycleCatalogRollingUpgradeCompatibility(t *testing.T) {
	for _, tableName := range []string{
		"mo_lifecycle_restore_attempts",
		"mo_lifecycle_restore_chunks",
	} {
		var ddl string
		for _, entry := range tenantUpgEntries {
			if entry.TableName == tableName {
				ddl = strings.ToLower(entry.UpgSql)
				break
			}
		}
		if ddl == "" {
			t.Fatalf("missing Lifecycle tenant upgrade for %s", tableName)
		}
		if !strings.Contains(ddl, "account_id int unsigned not null default 0") {
			t.Fatalf("%s must keep the old-CN DROP ACCOUNT compatibility column: %s", tableName, ddl)
		}
	}

	var cleanupDDL string
	for _, entry := range clusterUpgEntries {
		if entry.TableName == "mo_lifecycle_cleanup_roots" {
			cleanupDDL = strings.ToLower(entry.UpgSql)
			break
		}
	}
	if !strings.Contains(cleanupDDL, "create cluster table") {
		t.Fatalf("Cleanup Root must use the existing Cluster Table tenant filter during rolling upgrade: %s", cleanupDDL)
	}
}

func TestInformationSchemaTenantUpgradeEntries(t *testing.T) {
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
		entry := tenantUpgEntries[4+i]
		if entry.Schema != sysview.InformationDBConst || entry.TableName != view.name || entry.UpgType != versions.MODIFY_VIEW {
			t.Fatalf("unexpected information_schema.%s upgrade: %+v", view.name, entry)
		}
		if entry.UpgSql != view.ddl {
			t.Fatalf("%s upgrade does not use the current view definition: %s", view.name, entry.UpgSql)
		}
		for _, want := range []string{
			"relkind = 'temporary_table'",
			"rel_createsql",
			"mo_is_legacy_temporary_table",
			"[0-9a-f]{32}",
		} {
			if !strings.Contains(entry.UpgSql, want) {
				t.Fatalf("%s upgrade is missing legacy temporary-table compatibility %q: %s", view.name, want, entry.UpgSql)
			}
		}
		wantPreSQL := "drop view if exists information_schema." + strings.ToLower(view.name)
		if !strings.Contains(strings.ToLower(entry.PreSql), wantPreSQL) {
			t.Fatalf("%s upgrade is missing its drop-view precondition: %s", view.name, entry.PreSql)
		}
	}
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
