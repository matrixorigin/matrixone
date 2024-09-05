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

package v1_2_3

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_Upgrade(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				return executor.Result{}, nil
			}, txnOperator)

			metadata := Handler.Metadata()
			t.Logf("version metadata:%v", metadata)

			if err := Handler.Prepare(context.Background(), executor, true); err != nil {
				t.Errorf("Prepare() error = %v", err)
			}

			if err := Handler.HandleCreateFrameworkDeps(executor); err == nil {
				t.Errorf("HandleCreateFrameworkDeps() should report err")
			}

			if err := Handler.HandleClusterUpgrade(context.Background(), executor); err != nil {
				t.Errorf("HandleClusterUpgrade() error = %v", err)
			}

			if err := Handler.HandleTenantUpgrade(context.Background(), 0, executor); err != nil {
				t.Errorf("HandleTenantUpgrade() error = %v", err)
			}
		},
	)
}

func Test_HandleTenantUpgrade(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(strings.ToLower(sql), strings.ToLower("SELECT")) {
					typs := []types.Type{
						types.New(types.T_varchar, 50, 0),
						types.New(types.T_varchar, 50, 0),
						types.New(types.T_int64, 64, 0),
						types.New(types.T_int64, 64, 0),
						types.New(types.T_int64, 64, 0),
						types.New(types.T_int64, 64, 0),
						types.New(types.T_int32, 32, 0),
						types.New(types.T_varchar, 10, 0),
						types.New(types.T_varchar, 10, 0),
						types.New(types.T_varchar, 10, 0),
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{"TIMESTAMP"})
					executor.AppendStringRows(memRes, 1, []string{"YES"})
					executor.AppendFixedRows(memRes, 2, []int64{64})
					executor.AppendFixedRows(memRes, 3, []int64{0})
					executor.AppendFixedRows(memRes, 4, []int64{0})
					executor.AppendFixedRows(memRes, 5, []int64{0})
					executor.AppendFixedRows(memRes, 6, []int32{0})
					executor.AppendStringRows(memRes, 7, []string{""})
					executor.AppendStringRows(memRes, 8, []string{""})
					executor.AppendStringRows(memRes, 9, []string{""})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)

			upg_mo_snapshots.Upgrade(executor, uint32(0))

		},
	)
}
