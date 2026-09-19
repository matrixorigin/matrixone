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
package v4_0_7

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestViewsUpgradeIsScheduledForCompletedV406Tenants(t *testing.T) {
	require.Equal(t, "4.0.7", Handler.Metadata().Version)
	require.Equal(t, "4.0.6", Handler.Metadata().MinUpgradeVersion)
	require.Equal(t, int64(defines.MORPCVersion87), Handler.Metadata().RequiredProtocolVersion)
	require.Len(t, tenantUpgEntries, 1)

	entry := tenantUpgEntries[0]
	require.Equal(t, sysview.InformationDBConst, entry.Schema)
	require.Equal(t, "VIEWS", entry.TableName)
	require.Equal(t, versions.MODIFY_VIEW, entry.UpgType)
	require.Equal(t, sysview.InformationSchemaViewsDDL, entry.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion87), entry.RequiredProtocolVersion)
}

func TestViewsUpgradeBlocksBeforeDropOnOldCN(t *testing.T) {
	for _, peer := range []int64{defines.MORPCVersion86, defines.MORPCVersion87} {
		t.Run(fmt.Sprintf("peer-%d", peer), func(t *testing.T) {
			var executed []string
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				if sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
					return newProtocolVersionResultValue(t,
						fmt.Sprintf("{\"method\":\"GETPROTOCOLVERSION\",\"result\":\"cn-a:%d\"}", peer)), nil
				}
				executed = append(executed, sql)
				return executor.Result{}, nil
			})

			entry := tenantUpgEntries[0]
			entry.CheckFunc = func(executor.TxnExecutor, uint32) (bool, error) {
				return false, nil
			}
			err := entry.Upgrade(txn, 0)
			if peer < defines.MORPCVersion87 {
				require.ErrorContains(t, err, "requires all CNs to support protocol version 87")
				require.Empty(t, executed)
			} else {
				require.NoError(t, err)
				require.Contains(t, executed, entry.PreSql)
				require.Contains(t, executed, entry.UpgSql)
			}
		})
	}
}
