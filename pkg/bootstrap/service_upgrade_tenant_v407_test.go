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

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestV407TaskCompletionAfterLastAccountCommit(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		accountVersions := []string{"4.0.7", "4.0.6"}
		taskReady := int32(versions.No)
		taskUpdates := 0
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select from_account_id, to_account_id, upgrade_id, ready"):
				mp := mpool.MustNewZeroNoFixed()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				result := executor.NewMemResult([]types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_int32.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []int32{1}))
				require.NoError(t, executor.AppendFixedRows(result, 1, []int32{2}))
				require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{10}))
				require.NoError(t, executor.AppendFixedRows(result, 3, []int32{taskReady}))
				return result.GetResult(), nil
			case strings.HasPrefix(sql, "select account_id, create_version from mo_account"):
				return ownRecoveryResult(t, buildUpgradeTenantAccountRows([]int32{1, 2}, accountVersions)), nil
			case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "for update"):
				return ownRecoveryResult(t, buildUpgradeVersionResult(10, versions.StateUpgradingTenant, "4.0.6", "4.0.7", 41, 0, versions.Yes, versions.Yes, 2, 2)), nil
			case strings.HasPrefix(sql, "update mo_upgrade_tenant set ready = 1"):
				taskReady = versions.Yes
				taskUpdates++
				return executor.Result{}, nil
			default:
				t.Fatalf("unexpected SQL: %s", sql)
				return executor.Result{}, nil
			}
		}, &testTxnOperator{})
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(*service) {})
		t.Cleanup(s.stopper.Stop)
		selected := tenantUpgradeSelection{upgrade: versions.VersionUpgrade{ID: 10, FromVersion: "4.0.6", ToVersion: "4.0.7"}, taskID: 20}
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Zero(t, taskUpdates, "matching counters must not publish a task containing an old account")
		accountVersions[1] = "4.0.7"
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Equal(t, 1, taskUpdates, "a restart after the final account commit must finish the task")
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Equal(t, 1, taskUpdates, "a repeated completion must be a no-op")
	})
}

// A ready semantic version has no offset progress to replay. A restarted CN
// must skip DDL rather than silently rebuilding existing objects and grants.
func TestManualV407ReadyAccountDoesNotReplay(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				metadata := v4_0_7.Handler.Metadata()
				offset := metadata.VersionOffset
				if mismatch {
					offset++
				}
				reads := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					reads++
					switch sql {
					case "select create_version from mo_account where account_id = 7":
						return newBootstrapStringResult(metadata.Version), nil
					case versions.LatestVersionSQL:
						return statisticsLatestVersionResult(t, versions.Version{Version: metadata.Version, VersionOffset: offset}, versions.StateReady), nil
					default:
						t.Fatalf("ready account must not run migrations: %s", sql)
						return executor.Result{}, nil
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				err := s.UpgradeOneTenant(t.Context(), 7)
				if mismatch {
					require.ErrorContains(t, err, "offset-only")
					require.Empty(t, s.mu.tenants)
				} else {
					require.NoError(t, err)
					require.True(t, s.mu.tenants[7])
					require.NoError(t, s.UpgradeOneTenant(t.Context(), 7))
				}
				require.Equal(t, 2, reads)
			})
		})
	}
}

type systemAccountExecutor struct {
	executor.SQLExecutor
	t     *testing.T
	calls int
}

func (e *systemAccountExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	e.calls++
	require.Equal(e.t, uint32(7), ctx.Value(defines.TenantIDKey{}))
	require.True(e.t, opts.HasAccountID(), "system catalog access must not inherit the calling tenant")
	require.Equal(e.t, uint32(catalog.System_Account), opts.AccountID())
	return e.SQLExecutor.ExecTxn(ctx, fn, opts)
}

func TestV407AccountTransactionsOverrideCallingTenant(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		exec := &systemAccountExecutor{t: t, SQLExecutor: executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			require.Equal(t, "select create_version from mo_account where account_id = 7 for update", sql)
			return newBootstrapStringResult("4.0.6"), nil
		}, &testTxnOperator{})}
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(*service) {})
		t.Cleanup(s.stopper.Stop)
		ctx := context.WithValue(t.Context(), defines.TenantIDKey{}, uint32(7))
		worked := false
		active, err := s.withV407Account(ctx, versions.VersionUpgrade{FromVersion: "4.0.6", ToVersion: "4.0.7"}, 7, func(executor.TxnExecutor) error { worked = true; return nil })
		require.NoError(t, err)
		require.True(t, active)
		require.True(t, worked)
		require.Equal(t, 1, exec.calls)
	})
}
