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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestDoCheckUpgradeQueuesStatisticsRefresh(t *testing.T) {
	final := v4_0_7.Handler.Metadata()
	require.Greater(t, final.VersionOffset, uint32(0))
	for _, test := range []struct {
		name    string
		version string
		offset  uint32
		upgrade bool
	}{
		{name: "4.0.6", version: "4.0.6", offset: v4_0_6.Handler.Metadata().VersionOffset, upgrade: true},
		{name: "old_4.0.7", version: "4.0.7", upgrade: true},
		{name: "current_4.0.7", version: final.Version, offset: final.VersionOffset},
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
					require.Equal(t, []string{versions.GetVersionUpgradeSQL(versions.VersionUpgrade{
						FromVersion:        test.version,
						ToVersion:          final.Version,
						FinalVersion:       final.Version,
						FinalVersionOffset: final.VersionOffset,
						State:              versions.StateCreated,
						UpgradeCluster:     final.UpgradeCluster,
						UpgradeTenant:      versions.Yes,
					})}, upgrades)
				} else {
					require.Empty(t, upgrades)
				}
			})
		})
	}
}
