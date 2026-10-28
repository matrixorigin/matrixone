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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestTenantDataBackfillPagesAdvanceOnlyAfterSuccessfulWork(t *testing.T) {
	for _, tc := range []struct{ name, table string }{{"functions", "mo_user_defined_function"}, {"columns", catalog.MO_COLUMNS}} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				idx := -1
				for i, e := range tenantUpgEntries {
					if e.TableName == tc.table && e.UpgType == versions.MODIFY_METADATA {
						idx = i
						break
					}
				}
				require.NotEqual(t, -1, idx)
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				fail := true
				injected := errors.New("write failure")
				var queries []string
				tx := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
					queries = append(queries, sql)
					lower := strings.ToLower(sql)
					switch {
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						return newProtocolVersionResultValue(t, `{"result":"cn0:126"}`), nil
					case strings.HasPrefix(lower, "select 1 from mo_catalog.mo_user_defined_function where length("):
						return executor.Result{}, nil
					case strings.HasPrefix(lower, "select 1 from mo_catalog.mo_user_defined_function"), strings.HasPrefix(lower, "select 1 from mo_catalog.mo_columns"):
						result := executor.NewMemResult(nil, nil)
						result.NewBatchWithRowCount(1)
						return result.GetResult(), nil
					case strings.HasPrefix(lower, "select function_id from"):
						result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
						if strings.Contains(lower, "function_id > 2") {
							return result.GetResult(), nil
						}
						ids := []int32{1, 2}
						if strings.Contains(lower, "function_id > 0") {
							t.Fatal("unexpected initial cursor")
						}
						result.NewBatchWithRowCount(len(ids))
						require.NoError(t, executor.AppendFixedRows(result, 0, ids))
						return result.GetResult(), nil
					case strings.HasPrefix(lower, "select att_database, att_relname, attname"):
						require.Contains(t, lower, "where account_id = 7")
						result := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
						if strings.Contains(lower, "__mo_cpkey_col > serial(") {
							return result.GetResult(), nil
						}
						result.NewBatchWithRowCount(2)
						require.NoError(t, executor.AppendStringRows(result, 0, []string{"db", "db"}))
						require.NoError(t, executor.AppendStringRows(result, 1, []string{"tab", "tab"}))
						require.NoError(t, executor.AppendStringRows(result, 2, []string{"a", "b"}))
						return result.GetResult(), nil
					case strings.HasPrefix(lower, "update "):
						if tc.table == catalog.MO_COLUMNS {
							require.Contains(t, lower, "account_id = current_account_id()")
						}
						if fail {
							return executor.Result{}, injected
						}
						return executor.Result{AffectedRows: 2}, nil
					default:
						return executor.Result{}, errors.New("unexpected SQL: " + sql)
					}
				})
				var cursor TenantPageCursor
				_, err := Handler.UpgradeTenantEntryPage(context.Background(), 7, idx, tx, 64<<20, &cursor)
				require.ErrorIs(t, err, injected)
				require.False(t, cursor.Set)
				fail = false
				more, err := Handler.UpgradeTenantEntryPage(context.Background(), 7, idx, tx, 64<<20, &cursor)
				require.NoError(t, err)
				require.True(t, more)
				require.True(t, cursor.Set)
				more, err = Handler.UpgradeTenantEntryPage(context.Background(), 7, idx, tx, 64<<20, &cursor)
				require.NoError(t, err)
				require.False(t, more)
				require.NotEmpty(t, queries)
			})
		})
	}
}
