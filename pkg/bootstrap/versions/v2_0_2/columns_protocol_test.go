// Copyright 2024 Matrix Origin
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

package v2_0_2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestHistoricalColumnsUpgradeProtocol(t *testing.T) {
	count := 0
	for _, entry := range tenantUpgEntries {
		if entry.Schema != sysview.InformationDBConst || entry.TableName != "COLUMNS" {
			continue
		}
		count++
		require.Equal(t, defines.MORPCVersion46, entry.RequiredProtocolVersion)
		require.Equal(t, sysview.InformationSchemaColumnsV46UpgradeDDL, entry.UpgSql)
		require.NotContains(t, entry.UpgSql, "WHEN 3 then")
	}
	require.Positive(t, count, "the historical COLUMNS entry must remain in the upgrade list")
}
