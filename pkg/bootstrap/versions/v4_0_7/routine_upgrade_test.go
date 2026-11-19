// Copyright 2026 Matrix Origin
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

package v4_0_7

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRoutineSQLModeUpgrade(t *testing.T) {
	for _, tc := range []struct{ table, after string }{
		{"mo_user_defined_function", "database_collation"},
		{"mo_stored_procedure", "body"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			present := false
			var alters int
			columnResult := func() executor.Result {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				textType := types.T_varchar.ToType()
				result := executor.NewMemResult([]types.Type{textType, textType,
					types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
					types.T_int32.ToType(), textType, textType, textType}, mp)
				result.NewBatchWithRowCount(1)
				for _, i := range []int{0, 1, 7, 8, 9} {
					require.NoError(t, executor.AppendStringRows(result, i, []string{""}))
				}
				for _, i := range []int{2, 3, 4, 5} {
					require.NoError(t, executor.AppendFixedRows(result, i, []int64{0}))
				}
				require.NoError(t, executor.AppendFixedRows(result, 6, []int32{0}))
				return result.GetResult()
			}
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				if strings.HasPrefix(sql, "SELECT mo_show_visible_bin") {
					if present {
						return columnResult(), nil
					}
					return executor.Result{}, nil
				}
				if strings.HasPrefix(sql, "alter table mo_catalog."+tc.table) {
					require.Contains(t, sql, "default 'PIPES_AS_CONCAT' after "+tc.after)
					alters++
					present = true
					return executor.Result{}, nil
				}
				return executor.Result{}, errors.New("unexpected SQL: " + sql)
			})
			entry := routineSQLModeColumn(tc.table, tc.after)
			require.NoError(t, entry.Upgrade(txn, 42))
			require.NoError(t, entry.Upgrade(txn, 42))
			require.Equal(t, 1, alters, "an existing column and its modes must be retained")
		})
	}
	t.Run("catalog read failure does not alter", func(t *testing.T) {
		wantErr := errors.New("catalog unavailable")
		var statements int
		txn := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
			statements++
			return executor.Result{}, wantErr
		})
		entry := routineSQLModeColumn("mo_stored_procedure", "body")
		require.ErrorIs(t, entry.Upgrade(txn, 42), wantErr)
		require.Equal(t, 1, statements)
	})
}
