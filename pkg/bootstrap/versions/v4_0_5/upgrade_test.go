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

	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 5 {
		t.Fatalf("expected workload policy plus 4 Iceberg tenant upgrades, got %d", len(tenantUpgEntries))
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

	workloadPolicy := tenantUpgEntries[4]
	if workloadPolicy.UpgType != versions.CREATE_NEW_TABLE ||
		workloadPolicy.TableName != catalog.MO_QUERY_WORKLOAD_POLICY {
		t.Fatalf("unexpected workload policy upgrade: %+v", workloadPolicy)
	}
	for _, want := range []string{
		"create table",
		"primary key(account_id)",
		"revision bigint unsigned",
	} {
		if !strings.Contains(strings.ToLower(workloadPolicy.UpgSql), want) {
			t.Fatalf("workload policy upgrade SQL missing %q: %s", want, workloadPolicy.UpgSql)
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
	if meta.Version != catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION ||
		meta.VersionOffset < catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET {
		t.Fatalf(
			"workload policy capability %s offset %d is not covered by version metadata: %+v",
			catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
			catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET,
			meta,
		)
	}
	err := Handler.HandleCreateFrameworkDeps(nil)
	if err == nil || !strings.Contains(err.Error(), "Only v1.2.0") {
		t.Fatalf("unexpected framework deps error: %v", err)
	}
}

func TestQueryWorkloadPolicyUpgradeChecksTargetTenantTable(t *testing.T) {
	const accountID = uint32(42)
	var called bool
	stub := gostub.Stub(
		&versions.CheckTableDefinition,
		func(
			txn executor.TxnExecutor,
			actualAccountID uint32,
			schema string,
			table string,
		) (bool, error) {
			called = true
			if txn != nil {
				t.Fatal("unexpected transaction")
			}
			if actualAccountID != accountID ||
				schema != catalog.MO_CATALOG ||
				table != catalog.MO_QUERY_WORKLOAD_POLICY {
				t.Fatalf(
					"unexpected table check: account=%d table=%s.%s",
					actualAccountID,
					schema,
					table,
				)
			}
			return true, nil
		},
	)
	defer stub.Reset()

	exists, err := createQueryWorkloadPolicyTable().CheckFunc(nil, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !called {
		t.Fatalf("workload policy upgrade check was not delegated")
	}
}
