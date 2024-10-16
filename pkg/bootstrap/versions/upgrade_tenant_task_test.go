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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestGetTenantCreateVersionForUpdate(t *testing.T) {
	prefixMatchSql := fmt.Sprintf("select create_version from mo_account where account_id = %d for update", catalog.System_Account)

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.EqualFold(sql, prefixMatchSql) {
					typs := []types.Type{
						types.New(types.T_varchar, 50, 0),
					}

					memRes := executor.NewMemResult(typs, mpool.MustNewZero())
					memRes.NewBatchWithRowCount(1)

					executor.AppendStringRows(memRes, 0, []string{"1.2.3"})
					result := memRes.GetResult()
					return result, nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			_, err := GetTenantCreateVersionForUpdate(int32(catalog.System_Account), executor)
			require.NoError(t, err)
		},
	)
}

func TestGetTenantVersion(t *testing.T) {
	prefixMatchSql := fmt.Sprintf("select create_version from mo_account where account_id = %d", catalog.System_Account)
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.EqualFold(sql, prefixMatchSql) {
					typs := []types.Type{
						types.New(types.T_varchar, 50, 0),
					}

					memRes := executor.NewMemResult(typs, mpool.MustNewZero())
					memRes.NewBatchWithRowCount(1)

					executor.AppendStringRows(memRes, 0, []string{"1.2.3"})
					result := memRes.GetResult()
					return result, nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			_, err := GetTenantVersion(int32(catalog.System_Account), executor)
			require.NoError(t, err)
		},
	)
}
