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

package versions

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestGetTenantCreateVersionForUpdate(t *testing.T) {
	prefixMatchSQL := fmt.Sprintf("select create_version from mo_account where account_id = %d for update", catalog.System_Account)

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.EqualFold(sql, prefixMatchSQL) {
					return buildTenantVersionRows([]string{"1.2.3"}), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			_, err := GetTenantCreateVersionForUpdate(int32(catalog.System_Account), exec)
			require.NoError(t, err)
		},
	)
}

func TestGetTenantVersion(t *testing.T) {
	prefixMatchSQL := fmt.Sprintf("select create_version from mo_account where account_id = %d", catalog.System_Account)

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.EqualFold(sql, prefixMatchSQL) {
					return buildTenantVersionRows([]string{"1.2.3"}), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			_, err := GetTenantVersion(int32(catalog.System_Account), exec)
			require.NoError(t, err)
		},
	)
}

func TestGetUpgradeTenantTasksSkipsDeletedRanges(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			firstTaskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 0, 10, No)
			firstTenantSQL := "select account_id, create_version from mo_account where account_id >= 1 and account_id <= 4 for update"
			secondTaskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 5, 10, No)
			secondTenantSQL := "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 12 for update"

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.EqualFold(sql, firstTaskSQL):
					return buildUpgradeTenantTaskResult([]uint64{100}, []int32{1}, []int32{4}), nil
				case strings.EqualFold(sql, firstTenantSQL):
					return buildUpgradeTenantRows(nil, nil), nil
				case strings.EqualFold(sql, secondTaskSQL):
					return buildUpgradeTenantTaskResult([]uint64{200}, []int32{10}, []int32{12}), nil
				case strings.EqualFold(sql, secondTenantSQL):
					return buildUpgradeTenantRows([]int32{10, 12}, []string{"3.0.0", "3.0.0"}), nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOperator)

			taskID, tenants, createVersions, hasDeletedTenantTasks, hasConflictTenantTasks, err := GetUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.True(t, hasDeletedTenantTasks)
			require.False(t, hasConflictTenantTasks)
			require.Equal(t, uint64(200), taskID)
			require.Equal(t, []int32{10, 12}, tenants)
			require.Equal(t, []string{"3.0.0", "3.0.0"}, createVersions)
		},
	)
}

func TestGetUpgradeTenantTasksReturnsConflictHint(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			firstTaskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 0, 10, No)
			firstTenantSQL := "select account_id, create_version from mo_account where account_id >= 1 and account_id <= 4 for update"
			secondTaskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 5, 10, No)
			secondTenantSQL := "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 12 for update"
			finalTaskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 13, 10, No)

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.EqualFold(sql, firstTaskSQL):
					return buildUpgradeTenantTaskResult([]uint64{100}, []int32{1}, []int32{4}), nil
				case strings.EqualFold(sql, firstTenantSQL):
					return executor.Result{}, moerr.NewLockConflictNoCtx()
				case strings.EqualFold(sql, secondTaskSQL):
					return buildUpgradeTenantTaskResult([]uint64{200}, []int32{10}, []int32{12}), nil
				case strings.EqualFold(sql, secondTenantSQL):
					return buildUpgradeTenantRows(nil, nil), nil
				case strings.EqualFold(sql, finalTaskSQL):
					return executor.Result{}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOperator)

			taskID, tenants, createVersions, hasDeletedTenantTasks, hasConflictTenantTasks, err := GetUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.True(t, hasDeletedTenantTasks)
			require.True(t, hasConflictTenantTasks)
			require.Zero(t, taskID)
			require.Nil(t, tenants)
			require.Nil(t, createVersions)
		},
	)
}

func TestReconcileDeletedUpgradeTenantTasks(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, "where upgrade_id = 10") &&
					strings.Contains(sql, "not exists") {
					return executor.Result{AffectedRows: 2}, nil
				}
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}, txnOperator)

			affected, err := ReconcileDeletedUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.Equal(t, int64(2), affected)
		},
	)
}

func TestHasUnreadyUpgradeTenantTasks(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 10 and ready = 0 limit 1") {
					return buildExistsResult(), nil
				}
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}, txnOperator)

			hasUnready, err := HasUnreadyUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.True(t, hasUnready)
		},
	)
}

func TestGetUpgradeTenantTasksStopsAtMaxInt32Conflict(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			maxInt32 := int32(^uint32(0) >> 1)
			taskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 0, 10, No)
			tenantSQL := fmt.Sprintf("select account_id, create_version from mo_account where account_id >= %d and account_id <= %d for update",
				maxInt32, maxInt32)

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.EqualFold(sql, taskSQL):
					return buildUpgradeTenantTaskResult([]uint64{100}, []int32{maxInt32}, []int32{maxInt32}), nil
				case strings.EqualFold(sql, tenantSQL):
					return executor.Result{}, moerr.NewLockConflictNoCtx()
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOperator)

			taskID, tenants, createVersions, hasDeletedTenantTasks, hasConflictTenantTasks, err := GetUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.Zero(t, taskID)
			require.Nil(t, tenants)
			require.Nil(t, createVersions)
			require.False(t, hasDeletedTenantTasks)
			require.True(t, hasConflictTenantTasks)
		},
	)
}

func TestGetUpgradeTenantTasksStopsAtMaxInt32DeletedRange(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			maxInt32 := int32(^uint32(0) >> 1)
			taskSQL := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
				catalog.MOUpgradeTenantTable, 0, 10, No)
			tenantSQL := fmt.Sprintf("select account_id, create_version from mo_account where account_id >= %d and account_id <= %d for update",
				maxInt32, maxInt32)

			exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.EqualFold(sql, taskSQL):
					return buildUpgradeTenantTaskResult([]uint64{100}, []int32{maxInt32}, []int32{maxInt32}), nil
				case strings.EqualFold(sql, tenantSQL):
					return executor.Result{}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOperator)

			taskID, tenants, createVersions, hasDeletedTenantTasks, hasConflictTenantTasks, err := GetUpgradeTenantTasks(10, exec)
			require.NoError(t, err)
			require.Zero(t, taskID)
			require.Nil(t, tenants)
			require.Nil(t, createVersions)
			require.True(t, hasDeletedTenantTasks)
			require.False(t, hasConflictTenantTasks)
		},
	)
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

func buildTenantVersionRows(versions []string) executor.Result {
	if len(versions) == 0 {
		return executor.Result{}
	}
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_varchar, 50, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(len(versions))
	executor.AppendStringRows(memRes, 0, versions)
	return memRes.GetResult()
}

func buildUpgradeTenantTaskResult(taskIDs []uint64, fromIDs, toIDs []int32) executor.Result {
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

func buildUpgradeTenantRows(tenantIDs []int32, createVersions []string) executor.Result {
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
