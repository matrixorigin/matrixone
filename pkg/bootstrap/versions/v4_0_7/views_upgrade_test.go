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
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestViewsUpgradeRecognizesCatalogNameCase(t *testing.T) {
	for _, accountID := range []uint32{0, 42} {
		for _, tc := range []struct {
			name       string
			definition string
			current    bool
		}{
			{name: "VIEWS", definition: sysview.InformationSchemaViewsDDL, current: true},
			{name: "views", definition: sysview.InformationSchemaViewsDDL, current: true},
			{name: "views", definition: sysview.InformationSchemaViewsLegacyDDL},
			{name: "views"},
		} {
			t.Run(fmt.Sprintf("account-%d/%s/current-%t/missing-%t", accountID, tc.name, tc.current, tc.definition == ""), func(t *testing.T) {
				oldCheck := versions.CheckViewDefinition
				versions.CheckViewDefinition = func(_ executor.TxnExecutor, id uint32, schema, name string) (bool, string, error) {
					require.Equal(t, accountID, id)
					require.Equal(t, sysview.InformationDBConst, schema)
					if name == tc.name && tc.definition != "" {
						return true, tc.definition, nil
					}
					return false, "", nil
				}
				t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })
				var executed []string
				txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
					executed = append(executed, sql)
					return executor.Result{}, nil
				})
				require.NoError(t, UpgradeInformationSchemaViewsAfterProtocolCheck(txn, accountID))
				if tc.current {
					require.Empty(t, executed, "the committed definition is the durable no-op marker")
				} else {
					require.Equal(t, []string{tenantUpgEntries[0].PreSql, sysview.InformationSchemaViewsDDL}, executed)
				}
			})
		}
	}
}

func TestViewsDefinitionProbePropagatesCatalogErrors(t *testing.T) {
	for _, failName := range []string{"VIEWS", "views"} {
		t.Run(failName, func(t *testing.T) {
			catalogErr := errors.New("catalog unavailable")
			oldCheck := versions.CheckViewDefinition
			versions.CheckViewDefinition = func(_ executor.TxnExecutor, _ uint32, _, name string) (bool, string, error) {
				if name == failName {
					return false, "", catalogErr
				}
				return false, "", nil
			}
			t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })
			var executed []string
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				executed = append(executed, sql)
				return executor.Result{}, nil
			})
			require.ErrorIs(t, UpgradeInformationSchemaViewsAfterProtocolCheck(txn, 0), catalogErr)
			require.Empty(t, executed, "a failed catalog lookup must not stage DROP or CREATE")
		})
	}
}

func TestViewsUpgradeIsScheduledForCompletedV406Tenants(t *testing.T) {
	require.Equal(t, "4.0.7", Handler.Metadata().Version)
	require.Equal(t, "4.0.6", Handler.Metadata().MinUpgradeVersion)
	require.Equal(t, int64(defines.MORPCVersion115), Handler.Metadata().RequiredProtocolVersion)
	require.Len(t, tenantUpgEntries, 1)

	entry := tenantUpgEntries[0]
	require.Equal(t, sysview.InformationDBConst, entry.Schema)
	require.Equal(t, "VIEWS", entry.TableName)
	require.Equal(t, versions.MODIFY_VIEW, entry.UpgType)
	require.Equal(t, sysview.InformationSchemaViewsDDL, entry.UpgSql)
	require.Equal(t, int64(defines.MORPCVersion115), entry.RequiredProtocolVersion)
}

func TestViewsUpgradeBlocksBeforeDropOnOldCN(t *testing.T) {
	for _, peer := range []int64{defines.MORPCVersion104, defines.MORPCVersion105, defines.MORPCVersion106, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115} {
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
			if peer < defines.MORPCVersion115 {
				require.ErrorContains(t, err, "requires all CNs to support protocol version 115")
				require.Empty(t, executed)
			} else {
				require.NoError(t, err)
				require.Contains(t, executed, entry.PreSql)
				require.Contains(t, executed, entry.UpgSql)
			}
		})
	}
}

func TestViewsUpgradeAuthoringProtocolGate(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	old, hadOld := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadOld {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, old)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	txn := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
		return executor.Result{}, nil
	})
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	require.False(t, InformationSchemaViewsAuthoringProtocolReady(txn))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion113))
	require.False(t, InformationSchemaViewsAuthoringProtocolReady(txn))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion114))
	require.False(t, InformationSchemaViewsAuthoringProtocolReady(txn))

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion115))
	require.True(t, InformationSchemaViewsAuthoringProtocolReady(txn))
}
