// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheKnownPolicyObservationSQL(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
	exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
		require.NotContains(t, sql, "mo_tables")
		require.Contains(t, sql, "where table_id = 42")
		mem := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType()}, mp)
		mem.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendStringRows(mem, 0, []string{"id"}))
		require.NoError(t, executor.AppendFixedRows(mem, 1, []int32{0}))
		require.NoError(t, executor.AppendFixedRows(mem, 2, []uint64{123}))
		require.NoError(t, executor.AppendFixedRows(mem, 3, []uint64{1}))
		require.NoError(t, executor.AppendStringRows(mem, 4, []string{""}))
		return mem.GetResult(), nil
	})
	store := &sqlStore{exec: exec}
	ctx := context.WithValue(t.Context(), autoColumnKnownPolicyKey{}, uint64(1))
	for range 2 {
		cols, err := store.GetColumns(ctx, 42, nil)
		require.NoError(t, err)
		require.Equal(t, []AutoColumn{{TableID: 42, ColName: "id", Offset: 123, Step: 1, CacheSize: 1}}, cols)
		require.Zero(t, mp.CurrNB())
	}
}
