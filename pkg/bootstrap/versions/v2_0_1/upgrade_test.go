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

package v2_0_1

import (
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

func Test_UpgEntry(t *testing.T) {

	indexCheckPrefixMatchSql := "select distinct `idx`.`name` from `mo_catalog`.`mo_indexes` `idx` " +
		"left join `mo_catalog`.`mo_tables` `tbl` on `idx`.`table_id` = `tbl`.`rel_id` " +
		"where `tbl`.`account_id` = 0 AND `tbl`.`reldatabase`"

	sid := ""

	// test upg_mo_user_add_password_last_changed
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(indexCheckPrefixMatchSql)) {
					typs := []types.Type{
						types.New(types.T_varchar, 64, 0),
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{""})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_mo_user_add_password_last_changed.Upgrade(executor, uint32(0))
		},
	)

	// test upg_mo_user_add_password_history
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(indexCheckPrefixMatchSql)) {
					typs := []types.Type{
						types.New(types.T_varchar, 64, 0),
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{""})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_mo_user_add_password_history.Upgrade(executor, uint32(0))
		},
	)

	// test upg_mo_user_add_login_attempts
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(indexCheckPrefixMatchSql)) {
					typs := []types.Type{
						types.New(types.T_varchar, 64, 0),
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{""})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_mo_user_add_login_attempts.Upgrade(executor, uint32(0))
		},
	)

	// test upg_mo_user_add_lock_time
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(indexCheckPrefixMatchSql)) {
					typs := []types.Type{
						types.New(types.T_varchar, 64, 0),
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{""})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_mo_user_add_lock_time.Upgrade(executor, uint32(0))
		},
	)
}
