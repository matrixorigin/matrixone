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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
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
