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

package versions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestDatabaseInventoryBackfillPage(t *testing.T) {
	full := make([]uint64, DatabaseInventoryPageSize)
	for i := range full {
		full[i] = uint64(43 + i)
	}
	for _, tc := range []struct {
		name       string
		ids        []uint64
		affected   uint64
		failAt     int
		wantErr    bool
		wantMore   bool
		wantCursor uint64
	}{
		{name: "empty", wantCursor: 42},
		{name: "partial", ids: []uint64{43, 44}, affected: 2, wantCursor: 44},
		{name: "all rows already present", ids: []uint64{43, 44}, wantCursor: 44},
		{name: "full page", ids: full, affected: 128, wantMore: true, wantCursor: 170},
		{name: "duplicate", ids: []uint64{43, 43}, wantErr: true, wantCursor: 42},
		{name: "old identity", ids: []uint64{42}, wantErr: true, wantCursor: 42},
		{name: "oversized", ids: append(append([]uint64{}, full...), 171), wantErr: true, wantCursor: 42},
		{name: "read error", failAt: 1, wantErr: true, wantCursor: 42},
		{name: "write error", ids: []uint64{43}, failAt: 2, wantErr: true, wantCursor: 42},
		{name: "invalid affected count", ids: []uint64{43}, affected: 2, wantErr: true, wantCursor: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			var queries []string
			injected := errors.New("injected page failure")
			tx := executor.NewMemTxnExecutor(func(query string) (executor.Result, error) {
				queries = append(queries, query)
				if len(queries) == tc.failAt {
					return executor.Result{}, injected
				}
				if len(queries) == 2 {
					return executor.Result{AffectedRows: tc.affected}, nil
				}
				result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
				result.NewBatchWithRowCount(len(tc.ids))
				require.NoError(t, executor.AppendFixedRows(result, 0, tc.ids))
				return result.GetResult(), nil
			}, nil)
			next, more, err := BackfillDatabaseInventoryPage(t.Context(), tx, 7, 42)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.failAt > 0 {
				require.ErrorIs(t, err, injected)
			}
			require.Equal(t, tc.wantCursor, next)
			require.Equal(t, tc.wantMore, more)
			require.Contains(t, queries[0], "account_id=7 and dat_id>42")
			require.Contains(t, queries[0], fmt.Sprintf("order by dat_id limit %d", DatabaseInventoryPageSize))
			require.Contains(t, queries[0], "lower(datname) not in (")
			if len(queries) == 2 {
				require.True(t, strings.HasPrefix(queries[1], "insert into mo_catalog.mo_database_inventory(account_id,database_id,epoch) select d.account_id,d.dat_id,1"))
				require.Contains(t, queries[1], "d.account_id=7")
				require.Contains(t, queries[1], "i.account_id=d.account_id and i.database_id=d.dat_id")
				require.Contains(t, queries[1], "i.database_id is null")
			}
			require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB(), "page buffers must be released before the next transaction")
		})
	}
	t.Run("canceled before read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		next, more, err := BackfillDatabaseInventoryPage(ctx, nil, 7, 42)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 42, next)
		require.False(t, more)
	})
}
