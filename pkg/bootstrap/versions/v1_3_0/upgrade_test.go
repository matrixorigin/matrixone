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

package v1_3_0

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

	// test upg_drop_idx_task_status
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
					executor.AppendStringRows(memRes, 0, []string{"idx_task_status"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_drop_idx_task_status.Upgrade(executor, uint32(0))
		},
	)

	// test upg_drop_idx_task_runner
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
					executor.AppendStringRows(memRes, 0, []string{"idx_task_runner"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_drop_idx_task_runner.Upgrade(executor, uint32(0))
		},
	)

	// test upg_drop_idx_task_executor
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
					executor.AppendStringRows(memRes, 0, []string{"idx_task_executor"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_drop_idx_task_executor.Upgrade(executor, uint32(0))
		},
	)

	// test upg_drop_idx_task_epoch
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
					executor.AppendStringRows(memRes, 0, []string{"idx_task_epoch"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_drop_idx_task_epoch.Upgrade(executor, uint32(0))
		},
	)

	// test upg_drop_task_metadata_id
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
					executor.AppendStringRows(memRes, 0, []string{"task_metadata_id"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_drop_task_metadata_id.Upgrade(executor, uint32(0))
		},
	)

	// test upg_information_schema_columns update
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
			upg_information_schema_columns.Upgrade(executor, uint32(0))
		},
	)
}
