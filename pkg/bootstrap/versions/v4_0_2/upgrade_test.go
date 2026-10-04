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

package v4_0_2

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
)

func TestIcebergTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != len(icebergsql.P0SystemTableDDLs) {
		t.Fatalf("expected %d Iceberg tenant upgrades, got %d", len(icebergsql.P0SystemTableDDLs), len(tenantUpgEntries))
	}
	for _, entry := range tenantUpgEntries {
		if entry.UpgType != versions.CREATE_NEW_TABLE {
			t.Fatalf("%s should be CREATE_NEW_TABLE", entry.TableName)
		}
		if entry.PreSql != "" || entry.PostSql != "" {
			t.Fatalf("%s should not use destructive pre/post SQL", entry.TableName)
		}
		if strings.Contains(strings.ToLower(entry.UpgSql), "drop ") {
			t.Fatalf("%s upgrade SQL must not drop objects: %s", entry.TableName, entry.UpgSql)
		}
	}
}

func TestIcebergVersionHandleMetadataAndClusterNoop(t *testing.T) {
	meta := Handler.Metadata()
	if meta.Version != "4.0.2" || meta.MinUpgradeVersion != "4.0.1" || meta.UpgradeTenant != versions.Yes {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if meta.VersionOffset != uint32(len(tenantUpgEntries)+len(clusterUpgEntries)) {
		t.Fatalf("unexpected version offset: %+v", meta)
	}
	if err := Handler.HandleClusterUpgrade(context.Background(), nil); err != nil {
		t.Fatalf("empty cluster upgrade should be a no-op: %v", err)
	}
	err := Handler.HandleCreateFrameworkDeps(nil)
	if err == nil || !strings.Contains(err.Error(), "Only v1.2.0") {
		t.Fatalf("unexpected framework deps error: %v", err)
	}
}
