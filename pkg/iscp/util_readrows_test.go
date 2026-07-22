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

package iscp

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestReadSingleTaskRunnerAcceptsSingleRunner(t *testing.T) {
	result, mp := newTaskRunnerResult(t, [][]string{{"cn0"}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	runner, err := readSingleTaskRunner(result)
	require.NoError(t, err)
	require.Equal(t, "cn0", runner)
}

func TestReadSingleTaskRunnerCountsRowsAcrossBatches(t *testing.T) {
	result, mp := newTaskRunnerResult(t, [][]string{{"cn0"}, {"cn1"}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	runner, err := readSingleTaskRunner(result)
	require.Error(t, err)
	require.Empty(t, runner)
	require.Contains(t, err.Error(), "unexpected rows count: 2")
}

func TestReadSingleTaskRunnerAllowsNoRunner(t *testing.T) {
	result, mp := newTaskRunnerResult(t, [][]string{{}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	runner, err := readSingleTaskRunner(result)
	require.NoError(t, err)
	require.Empty(t, runner)
}

func TestReadSingleTaskRunnerRejectsEmptyRunner(t *testing.T) {
	result, mp := newTaskRunnerResult(t, [][]string{{""}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	runner, err := readSingleTaskRunner(result)
	require.Error(t, err)
	require.Empty(t, runner)
	require.Contains(t, err.Error(), "task runner is null")
}

func TestGetTaskRunnerQueriesMOTaskWithSystemAccount(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	result, mp := newTaskRunnerResult(t, [][]string{{"cn1"}})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(ctx context.Context, sql string, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
		accountID, err := defines.GetAccountId(ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(catalog.System_Account), accountID)
		require.Contains(t, sql, "mo_task.sys_daemon_task")
		require.Equal(t, "cn0", cnUUID)
		require.Nil(t, txn)
		return result, nil
	}

	tenantCtx := defines.AttachAccountId(context.Background(), 1001)
	runner, err := GetTaskRunner(tenantCtx, "cn0", nil)
	require.NoError(t, err)
	require.Equal(t, "cn1", runner)
}

func newTaskRunnerResult(t *testing.T, batches [][]string) (executor.Result, *mpool.MPool) {
	t.Helper()

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	for _, batch := range batches {
		memRes.NewBatchWithRowCount(len(batch))
		require.NoError(t, executor.AppendStringRows(memRes, 0, batch))
	}
	return memRes.GetResult(), mp
}
