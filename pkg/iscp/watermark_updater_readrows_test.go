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
	"testing/synctest"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestGetTableIDCountsRowsAcrossBatches(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	result, mp := newTableIDResult(t, [][]uint64{{10}, {20}}, [][]uint64{{100}, {200}})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return result, nil
	}

	_, _, err := getTableID(context.Background(), "", nil, 0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid rows 2")
}

func TestUnregisterJobsByDBNameEscapesDatabaseLiteral(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	mp := mpool.MustNewZero()
	memResult := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	var capturedSQL string
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		capturedSQL = sql
		return memResult.GetResult(), nil
	}

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(7))
	err := unregisterJobsByDBName(ctx, "", nil, `db_'name\path`)

	require.NoError(t, err)
	require.Contains(t, capturedSQL, "account_id = 7")
	require.Contains(t, capturedSQL, `reldatabase = 'db_''name\\path'`)
}

func TestUnregisterJobPropagatesRetryableErrorWithoutLocalRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		oldExecWithResult := ExecWithResult
		defer func() {
			ExecWithResult = oldExecWithResult
		}()

		mp := mpool.MustNewZero()
		defer func() {
			require.Equal(t, int64(0), mp.CurrNB())
			mpool.DeleteMPool(mp)
		}()

		retryErr := moerr.NewTxnNeedRetryNoCtx()
		calls := 0
		ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
			calls++
			switch calls {
			case 1:
				require.Contains(t, sql, "SELECT rel_id")
				result := executor.NewMemResult(
					[]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()},
					mp,
				)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{11}))
				require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{12}))
				return result.GetResult(), nil
			case 2:
				require.Contains(t, sql, "SELECT drop_at, job_id")
				result := executor.NewMemResult(
					[]types.Type{types.T_timestamp.ToType(), types.T_uint64.ToType()},
					mp,
				)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []types.Timestamp{0}))
				require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{13}))
				res := result.GetResult()
				res.Batches[0].Vecs[0].SetNull(0)
				return res, nil
			case 3:
				require.Contains(t, sql, "UPDATE mo_catalog.mo_iscp_log SET drop_at = now()")
				return executor.Result{}, retryErr
			default:
				t.Fatalf("unexpected local retry: SQL execution %d: %s", calls, sql)
				return executor.Result{}, nil
			}
		}

		ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(7))
		_, err := UnregisterJob(ctx, "", nil, &JobID{
			DBName:    "db",
			TableName: "tbl",
			JobName:   "job",
		})

		require.Same(t, retryErr, err)
		require.Equal(t, 3, calls)
	})
}

func newTableIDResult(t *testing.T, tableIDBatches, dbIDBatches [][]uint64) (executor.Result, *mpool.MPool) {
	t.Helper()
	require.Len(t, tableIDBatches, len(dbIDBatches))

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
	for i := range tableIDBatches {
		require.Len(t, tableIDBatches[i], len(dbIDBatches[i]))
		memRes.NewBatchWithRowCount(len(tableIDBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 0, tableIDBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 1, dbIDBatches[i]))
	}
	return memRes.GetResult(), mp
}
