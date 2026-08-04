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

package v4_0_7

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestPartitionExpressionBinaryStorageUpgrade(t *testing.T) {
	require.Len(t, tenantUpgEntries, 1)
	require.Empty(t, clusterUpgEntries)

	entry := tenantUpgEntries[0]
	require.Equal(t, versions.MODIFY_COLUMN, entry.UpgType)
	require.Equal(t, catalog.MO_CATALOG, entry.Schema)
	require.Equal(t, catalog.MOPartitionTables, entry.TableName)
	require.Contains(t, strings.ToLower(entry.UpgSql), "modify column partition_expression varbinary(2048) not null")

	metadata := Handler.Metadata()
	require.Equal(t, "4.0.7", metadata.Version)
	require.Equal(t, "4.0.6", metadata.MinUpgradeVersion)
	require.Equal(t, uint32(1), metadata.VersionOffset)
}
