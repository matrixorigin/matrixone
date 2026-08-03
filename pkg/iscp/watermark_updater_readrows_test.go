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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
