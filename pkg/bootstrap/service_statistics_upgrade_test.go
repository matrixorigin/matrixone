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

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_8"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestDoCheckUpgradeQueuesStatisticsRefresh(t *testing.T) {
	final := v4_0_8.Handler.Metadata()
	require.Greater(t, versions.Compare(final.Version, "4.0.7"), 0)
	for _, test := range []struct {
		name    string
		version string
		offset  uint32
		upgrade bool
		via407  bool
	}{
		{name: "4.0.6", version: "4.0.6", offset: v4_0_6.Handler.Metadata().VersionOffset, upgrade: true, via407: true},
		{name: "old_4.0.7", version: "4.0.7", upgrade: true},
		{name: "4.0.7_offset_1", version: "4.0.7", offset: 1, upgrade: true},
		{name: "current_4.0.8", version: final.Version, offset: final.VersionOffset},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				var upgrades []string
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case sql == "select version, version_offset, state from mo_version order by create_at desc limit 1":
						mp := mpool.MustNewZeroNoFixed()
						t.Cleanup(func() { mpool.DeleteMPool(mp) })
						res := executor.NewMemResult([]types.Type{
							types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_int32.ToType(),
						}, mp)
						res.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(res, 0, []string{test.version}))
						require.NoError(t, executor.AppendFixedRows(res, 1, []uint32{test.offset}))
						require.NoError(t, executor.AppendFixedRows(res, 2, []int32{versions.StateReady}))
						return res.GetResult(), nil
					case strings.HasPrefix(sql, "select version from mo_version"):
						return newBootstrapStringResult(test.version), nil
					case strings.HasPrefix(sql, "insert into mo_upgrade"):
						upgrades = append(upgrades, sql)
					}
					return executor.Result{}, nil
				}, &testTxnOperator{})
				b := newServiceForTest("", &memLocker{},
					clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
					func(s *service) { s.initUpgrade() })
				defer b.stopper.Stop()
				require.Equal(t, final, b.getFinalVersionHandle().Metadata())
				require.NoError(t, b.doCheckUpgrade(context.Background()))
				if test.upgrade {
					hops := []versions.Version{final}
					if test.via407 {
						hops = append([]versions.Version{v4_0_7.Handler.Metadata()}, hops...)
					}
					var expected []string
					from := test.version
					for order, hop := range hops {
						expected = append(expected, versions.GetVersionUpgradeSQL(versions.VersionUpgrade{
							FromVersion:        from,
							ToVersion:          hop.Version,
							FinalVersion:       final.Version,
							FinalVersionOffset: final.VersionOffset,
							State:              versions.StateCreated,
							UpgradeOrder:       int32(order),
							UpgradeCluster:     hop.UpgradeCluster,
							UpgradeTenant:      hop.UpgradeTenant,
						}))
						from = hop.Version
					}
					require.Equal(t, expected, upgrades)
				} else {
					require.Empty(t, upgrades)
				}
			})
		})
	}
}

func TestStatisticsUpgradeOldWorkerCannotCompleteNewTask(t *testing.T) {
	final := v4_0_8.Handler.Metadata()
	for _, test := range []struct {
		name        string
		target      string
		oldCanClaim bool
	}{
		// Control: the previous offset-only target lets the unchanged old handler
		// finish the task without touching the persisted STATISTICS definition.
		{name: "unsafe_offset_only_control", target: "4.0.7", oldCanClaim: true},
		{name: "new_semantic_version", target: final.Version},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				const (
					upgradeID = uint64(100)
					taskID    = uint64(200)
					tenantID  = int32(10)
				)
				legacy := strings.Replace(sysview.InformationSchemaStatisticsDDL,
					"coalesce(nullif(`idx`.`algo`, ''), 'BTREE')", "`idx`.`algo`", 1)
				require.NotEqual(t, sysview.InformationSchemaStatisticsDDL, legacy)
				definition := legacy
				var claimed, ready bool
				var readyTenants, creates int
				txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where state = 1"):
						if readyTenants == 1 {
							return executor.Result{}, nil
						}
						return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
							"4.0.7", test.target, final.VersionOffset, 0,
							versions.No, versions.Yes, 1, 0), nil
					case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
						claimed = true
						return buildUpgradeTenantTaskRows([]uint64{taskID}, []int32{tenantID}, []int32{tenantID}), nil
					case strings.HasPrefix(sql, "select account_id, create_version from mo_account"):
						return buildUpgradeTenantAccountRows([]int32{tenantID}, []string{"4.0.7"}), nil
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
						if strings.Contains(sql, "tbl.relname = 'STATISTICS'") {
							return executor.Result{}, nil
						}
						return newBootstrapStringResult(definition), nil
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						// Old 4.0.7 and new CNs both speak protocol 61: the pre-existing
						// protocol barrier cannot distinguish their upgrade handlers.
						return newBootstrapStringResult(`{"method":"GETPROTOCOLVERSION","result":"old-cn:61,new-cn:61"}`), nil
					case sql == "DROP VIEW IF EXISTS information_schema.STATISTICS;":
						definition = ""
					case sql == sysview.InformationSchemaStatisticsDDL:
						definition = sql
						creates++
					case strings.HasPrefix(sql, "select distinct t.reldatabase, i.index_table_name, i.algo_table_type"):
						return executor.Result{}, nil // no provenance tables for this tenant
					case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", test.target, tenantID):
						require.Equal(t, !test.oldCanClaim, definition == sysview.InformationSchemaStatisticsDDL)
					case strings.HasPrefix(sql, "update mo_upgrade_tenant set ready = 1"):
						ready = true
					case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where id = 100 for update"):
						return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
							"4.0.7", test.target, final.VersionOffset, 0,
							versions.No, versions.Yes, 1, 0), nil
					case strings.HasPrefix(sql, "update mo_upgrade set total_tenant = 1, ready_tenant = 1"):
						readyTenants = 1
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
					}
					return executor.Result{AffectedRows: 1}, nil
				}, txnOp)

				old := newServiceForTest("", &memLocker{},
					clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
					func(s *service) { s.handles = append(s.handles, v4_0_7.Handler) })
				defer old.stopper.Stop()
				// Both the worker admission code and this restored 4.0.7 handler are
				// unchanged from the old binary; no new-worker-only offset check is used.
				hasWork, err := old.newTenantUpgradePass(t.Context())()
				require.NoError(t, err)
				require.Equal(t, test.oldCanClaim, hasWork)
				require.Equal(t, test.oldCanClaim, claimed)
				require.Equal(t, test.oldCanClaim, ready)
				require.Equal(t, legacy, definition)
				require.Zero(t, creates)

				current := newServiceForTest("", &memLocker{},
					clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
					func(s *service) { s.initUpgrade() })
				defer current.stopper.Stop()
				hasWork, err = current.newTenantUpgradePass(t.Context())()
				require.NoError(t, err)
				require.Equal(t, !test.oldCanClaim, hasWork)
				require.True(t, ready)
				require.Equal(t, 1, readyTenants)
				if test.oldCanClaim {
					require.Equal(t, legacy, definition, "new workers cannot repair an already-completed task")
					require.Zero(t, creates)
				} else {
					require.Equal(t, sysview.InformationSchemaStatisticsDDL, definition)
					require.Equal(t, 1, creates)
				}
			})
		})
	}
}
