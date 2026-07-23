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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 6 {
		t.Fatalf("expected 4 Iceberg and 2 workload policy tenant upgrades, got %d", len(tenantUpgEntries))
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

	column := tenantUpgEntries[4]
	if column.UpgType != versions.ADD_COLUMN ||
		column.TableName != "mo_mysql_compatibility_mode" {
		t.Fatalf("unexpected workload policy generated-column upgrade: %+v", column)
	}
	columnSQL := strings.ToLower(column.UpgSql)
	for _, want := range []string{
		"alter table",
		frontend.MoMysqlCompatWorkloadPolicyAccountColumn,
		"generated always as",
		frontend.MoMysqlCompatWorkloadPolicyExpression,
		"stored",
	} {
		if !strings.Contains(columnSQL, strings.ToLower(want)) {
			t.Fatalf("workload policy column upgrade SQL missing %q: %s", want, column.UpgSql)
		}
	}

	index := tenantUpgEntries[5]
	if index.UpgType != versions.ADD_CONSTRAINT_UNIQUE_INDEX ||
		index.TableName != "mo_mysql_compatibility_mode" {
		t.Fatalf("unexpected workload policy unique-index upgrade: %+v", index)
	}
	indexSQL := strings.ToLower(index.UpgSql)
	for _, want := range []string{
		"alter table",
		"add unique key",
		frontend.MoMysqlCompatWorkloadPolicyUniqueIndex,
		frontend.MoMysqlCompatWorkloadPolicyAccountColumn,
	} {
		if !strings.Contains(indexSQL, strings.ToLower(want)) {
			t.Fatalf("workload policy index upgrade SQL missing %q: %s", want, index.UpgSql)
		}
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
