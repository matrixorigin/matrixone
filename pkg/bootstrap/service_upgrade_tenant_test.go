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

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_asyncUpgradeTenantTask(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Expected no panic")
				}
			}()

			var cnt atomic.Int32
			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if cnt.Load() > 0 {
					return executor.Result{}, nil
				}
				cnt.Add(1)
				return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
			})

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					h1 := newTestVersionHandler("1.2.0", "1.1.0", versions.Yes, versions.No, 10)
					h2 := newTestVersionHandler("2.0.0", "1.2.0", versions.Yes, versions.No, 2)
					s.handles = append(s.handles, h1)
					s.handles = append(s.handles, h2)
				},
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
			defer cancel()

			b.asyncUpgradeTenantTask(ctx)
		},
	)
}

func Test_asyncUpgradeTenantTask_AutoCompletesDeletedTenantTasks(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var finalized atomic.Bool
			var deletedTasksReconciled atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return buildUpgradeTenantTaskRows([]uint64{200}, []int32{10}, []int32{12}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 12"):
					return buildUpgradeTenantAccountRows(nil, nil), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 13"):
					return buildUpgradeTenantTaskRows(nil, nil, nil), nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					if deletedTasksReconciled.Load() {
						return executor.Result{}, nil
					}
					return buildExistsResult(), nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, "where upgrade_id = 100") &&
					strings.Contains(sql, "not exists"):
					deletedTasksReconciled.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, finalized.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_asyncUpgradeTenantTask_ReconcilesReadyCountWhenTasksAlreadyFinished(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var finalized atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, finalized.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func buildUpgradeVersionResult(
	id uint64,
	state int32,
	fromVersion, toVersion string,
	finalVersionOffset uint32,
	upgradeOrder, upgradeCluster, upgradeTenant, totalTenant, readyTenant int32,
) executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_uint64, 64, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_uint32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendFixedRows(memRes, 0, []uint64{id})
	executor.AppendStringRows(memRes, 1, []string{fromVersion})
	executor.AppendStringRows(memRes, 2, []string{toVersion})
	executor.AppendStringRows(memRes, 3, []string{toVersion})
	executor.AppendFixedRows(memRes, 4, []uint32{finalVersionOffset})
	executor.AppendFixedRows(memRes, 5, []int32{state})
	executor.AppendFixedRows(memRes, 6, []int32{upgradeOrder})
	executor.AppendFixedRows(memRes, 7, []int32{upgradeCluster})
	executor.AppendFixedRows(memRes, 8, []int32{upgradeTenant})
	executor.AppendFixedRows(memRes, 9, []int32{totalTenant})
	executor.AppendFixedRows(memRes, 10, []int32{readyTenant})
	return memRes.GetResult()
}

func buildUpgradeTenantTaskRows(taskIDs []uint64, fromIDs, toIDs []int32) executor.Result {
	if len(taskIDs) == 0 {
		return executor.Result{}
	}
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_uint64, 64, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(len(taskIDs))
	executor.AppendFixedRows(memRes, 0, taskIDs)
	executor.AppendFixedRows(memRes, 1, fromIDs)
	executor.AppendFixedRows(memRes, 2, toIDs)
	return memRes.GetResult()
}

func buildUpgradeTenantAccountRows(tenantIDs []int32, createVersions []string) executor.Result {
	if len(tenantIDs) == 0 {
		return executor.Result{}
	}
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_int32, 32, 0),
			types.New(types.T_varchar, 50, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(len(tenantIDs))
	executor.AppendFixedRows(memRes, 0, tenantIDs)
	executor.AppendStringRows(memRes, 1, createVersions)
	return memRes.GetResult()
}

func buildExistsResult() executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendFixedRows(memRes, 0, []int32{1})
	return memRes.GetResult()
}
