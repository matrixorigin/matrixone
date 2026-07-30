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

package v4_0_6

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func TestForeignKeyMetadataTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 2 {
		t.Fatalf("expected 2 foreign-key metadata tenant upgrades, got %d", len(tenantUpgEntries))
	}

	keyColumnUsage := tenantUpgEntries[0]
	if keyColumnUsage.UpgType != versions.CREATE_VIEW || keyColumnUsage.TableName != "KEY_COLUMN_USAGE" {
		t.Fatalf("unexpected KEY_COLUMN_USAGE upgrade: %+v", keyColumnUsage)
	}
	if keyColumnUsage.UpgSql != sysview.InformationSchemaKeyColumnUsageDDL {
		t.Fatalf("unexpected KEY_COLUMN_USAGE definition: %s", keyColumnUsage.UpgSql)
	}
	if !strings.Contains(strings.ToLower(keyColumnUsage.PreSql), "drop table if exists information_schema.key_column_usage") {
		t.Fatalf("KEY_COLUMN_USAGE must replace the legacy table: %s", keyColumnUsage.PreSql)
	}

	referentialConstraints := tenantUpgEntries[1]
	if referentialConstraints.UpgType != versions.MODIFY_VIEW || referentialConstraints.TableName != "REFERENTIAL_CONSTRAINTS" {
		t.Fatalf("unexpected REFERENTIAL_CONSTRAINTS upgrade: %+v", referentialConstraints)
	}
	if referentialConstraints.UpgSql != sysview.InformationSchemaReferentialConstraintsDDL {
		t.Fatalf("unexpected REFERENTIAL_CONSTRAINTS definition: %s", referentialConstraints.UpgSql)
	}
	if !strings.Contains(strings.ToLower(referentialConstraints.PreSql), "drop view if exists information_schema.referential_constraints") {
		t.Fatalf("REFERENTIAL_CONSTRAINTS must replace the previous view: %s", referentialConstraints.PreSql)
	}
}

func TestLegacyForeignKeyMetadataUpdatesPreserveOrderAndActions(t *testing.T) {
	updates, err := legacyForeignKeyMetadataUpdates(legacyForeignKeyTableDefinition{
		database: "db'one",
		table:    "child",
		createSQL: "create table child (a int, b int, constraint fk_default foreign key (b, a) references parent (b, a), " +
			"constraint fk_restrict foreign key (a) references parent (a) on delete restrict on update restrict)",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 3 {
		t.Fatalf("expected 3 legacy foreign-key updates, got %d: %v", len(updates), updates)
	}

	for _, expected := range []string{
		"constraint_id = 1, on_delete = 'NO_ACTION', on_update = 'NO_ACTION'",
		"constraint_name = 'fk_default' AND column_name = 'b'",
		"constraint_id = 2, on_delete = 'NO_ACTION', on_update = 'NO_ACTION'",
		"constraint_name = 'fk_default' AND column_name = 'a'",
		"constraint_id = 1, on_delete = 'RESTRICT', on_update = 'RESTRICT'",
		"constraint_name = 'fk_restrict' AND column_name = 'a'",
		"db_name = 'db''one'",
	} {
		found := false
		for _, update := range updates {
			if strings.Contains(update, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing %q in generated updates: %v", expected, updates)
		}
	}
}

func TestForeignKeyMetadataVersionHandleMetadataAndClusterNoop(t *testing.T) {
	meta := Handler.Metadata()
	if meta.Version != "4.0.6" || meta.MinUpgradeVersion != "4.0.5" || meta.UpgradeTenant != versions.Yes || meta.UpgradeCluster != versions.Yes {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if meta.VersionOffset != uint32(len(tenantUpgEntries)+len(clusterUpgEntries)) {
		t.Fatalf("unexpected version offset: %+v", meta)
	}
	if len(clusterUpgEntries) != 0 {
		t.Fatalf("expected no cluster upgrade entries, got %d", len(clusterUpgEntries))
	}
}
