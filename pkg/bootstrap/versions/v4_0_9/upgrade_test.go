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

package v4_0_9

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestSnapshotQuotaIndexUpgrade(t *testing.T) {
	metadata := Handler.Metadata()
	require.Equal(t, "4.0.9", metadata.Version)
	require.Equal(t, "4.0.8", metadata.MinUpgradeVersion)
	require.False(t, metadata.CanDirectUpgrade("4.0.6"))
	require.Equal(t, versions.Yes, metadata.UpgradeCluster)
	require.Equal(t, versions.No, metadata.UpgradeTenant)
	require.Len(t, clusterUpgEntries, 1)
	indexEntry := clusterUpgEntries[0]
	require.Equal(t, catalog.MO_SNAPSHOTS, indexEntry.TableName)
	require.Equal(t, versions.ADD_INDEX, indexEntry.UpgType)
	require.Contains(t, indexEntry.UpgSql, "(account_name, level, obj_id, kind)")
}
