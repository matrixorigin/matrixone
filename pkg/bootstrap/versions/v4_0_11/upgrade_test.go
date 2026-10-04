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

package v4_0_11

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestCDCWatermarkColumnsUpgradeMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.11", m.Version)
	require.Equal(t, "4.0.10", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeCluster)
	require.Equal(t, versions.No, m.UpgradeTenant)
	require.Equal(t, defines.MORPCVersion106, m.RequiredProtocolVersion)
	require.Len(t, clusterUpgEntries, 2)
	for _, entry := range clusterUpgEntries {
		require.Equal(t, catalog.MO_CDC_WATERMARK, entry.TableName)
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, int64(defines.MORPCVersion106), entry.RequiredProtocolVersion)
	}
}
