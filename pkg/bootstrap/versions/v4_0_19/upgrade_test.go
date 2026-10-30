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

package v4_0_19

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

// The final vehicle hop follows the upstream 4.0.18 handle on the registered
// ladder: its offset equals its own tenant entries only, and it reaches
// accounts exactly through those two shared-builder legs.
func TestHandlerRegistersConsolidatedSuccessorIdentity(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		metadata := Handler.Metadata()
		require.Equal(t, "4.0.19", metadata.Version)
		require.Equal(t, "4.0.7", metadata.MinUpgradeVersion)
		require.Equal(t, versions.No, metadata.UpgradeCluster)
		require.Equal(t, versions.Yes, metadata.UpgradeTenant)
		require.Equal(t, uint32(len(tenantUpgEntries)), metadata.VersionOffset)
		require.Equal(t, int64(defines.MORPCVersion118), metadata.RequiredProtocolVersion)
		require.Len(t, tenantUpgEntries, 2)

		columns, statistics := tenantUpgEntries[0], tenantUpgEntries[1]
		require.Equal(t, sysview.InformationDBConst, columns.Schema)
		require.Equal(t, "COLUMNS", columns.TableName)
		require.Equal(t, sysview.InformationDBConst, statistics.Schema)
		require.Equal(t, "STATISTICS", statistics.TableName)
	})
}

// Both legs rebuild on the upstream shared builder: CREATE OR REPLACE
// authoring, the historical COLUMNS gate (109) and the historical
// prefix-aware STATISTICS gate (41) stand; no new gate exists.
func TestSuccessorEntriesShareTheUpstreamBuilders(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		columns, statistics := tenantUpgEntries[0], tenantUpgEntries[1]
		require.Equal(t,
			strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
			columns.UpgSql)
		require.Equal(t, int64(defines.MORPCVersion109), columns.RequiredProtocolVersion)
		require.Equal(t,
			strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
			statistics.UpgSql)
		require.Equal(t, int64(defines.MORPCVersion41), statistics.RequiredProtocolVersion)
	})
}

// The entries' checks are idempotent against the current published
// definitions and demand the re-publication a stale account needs: an
// account already carrying the corrected identities skips each write, and a
// pre-correction identity reports not done so the hop rewrites it.
func TestSuccessorEntryChecksRecognizeCurrentAndHistoricalIdentities(t *testing.T) {
	for _, tc := range []struct {
		name       string
		entry      versions.UpgradeEntry
		current    string
		historical string
	}{
		{
			name:       "COLUMNS",
			entry:      tenantUpgEntries[0],
			current:    sysview.InformationSchemaColumnsDDL,
			historical: sysview.InformationSchemaColumnsBinaryFamilyNullDDL(),
		},
		{
			name:  "COLUMNS-from-upstream-4.0.18",
			entry: tenantUpgEntries[0],
			// A main-era binary stamped tenants with the published 4.0.18
			// identity; the merged current differs, so the hop rewrites them.
			current:    sysview.InformationSchemaColumnsDDL,
			historical: sysview.InformationSchemaColumns4018DDL(),
		},
		{
			name:       "STATISTICS",
			entry:      tenantUpgEntries[1],
			current:    sysview.InformationSchemaStatisticsPrefixSubPartDDL,
			historical: sysview.InformationSchemaStatisticsDDL,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				served := tc.current
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					require.Contains(t, sql, "SELECT tbl.rel_createsql")
					return newSingleStringResult(t, served), nil
				}, nil)
				done, err := tc.entry.CheckFunc(txn, 7)
				require.NoError(t, err)
				require.True(t, done, "a current definition makes the leg a no-op")

				served = tc.historical
				txn = executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					return newSingleStringResult(t, served), nil
				}, nil)
				done, err = tc.entry.CheckFunc(txn, 7)
				require.NoError(t, err)
				require.False(t, done, "a historical identity must be rewritten by the hop")
			})
		})
	}
}

// newSingleStringResult serves one varchar row: the persisted view
// definition a definition read returns.
func newSingleStringResult(t *testing.T, definition string) executor.Result {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{definition}))
	return result.GetResult()
}
