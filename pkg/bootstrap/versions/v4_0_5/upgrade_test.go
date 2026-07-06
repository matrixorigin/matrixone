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
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 3 {
		t.Fatalf("expected 3 Iceberg orphan cleanup tenant upgrades, got %d", len(tenantUpgEntries))
	}
	for _, entry := range tenantUpgEntries {
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
