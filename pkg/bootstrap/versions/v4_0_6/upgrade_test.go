// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/stretchr/testify/require"
)

func TestMongoDBCatalogUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 2)
	require.Empty(t, clusterUpgEntries)
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}

	meta := Handler.Metadata()
	require.Equal(t, "4.0.6", meta.Version)
	require.Equal(t, "4.0.5", meta.MinUpgradeVersion)
	require.Equal(t, versions.Yes, meta.UpgradeTenant)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), meta.VersionOffset)
}
