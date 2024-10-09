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
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	// test upg_information_schema_schemata update
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
			upg_information_schema_schemata.Upgrade(executor, uint32(0))
		},
	)
}

func Test_versionHandle_HandleClusterUpgrade_InsertInitDataKey(t *testing.T) {
	clusterUpgEntries = []versions.UpgradeEntry{}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v1.3.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor1 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	err := v.HandleClusterUpgrade(
		context.WithValue(context.Background(), KekKey{}, "kek"),
		executor1,
	)
	assert.Error(t, err)
}

func Test_versionHandle_HandleClusterUpgrade(t *testing.T) {
	clusterUpgEntries = []versions.UpgradeEntry{}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v1.3.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	stub1 := gostub.Stub(&InsertInitDataKey,
		func(_ executor.TxnExecutor, _ string) (err error) {
			return nil
		})
	defer stub1.Reset()

	err := v.HandleClusterUpgrade(
		context.WithValue(context.Background(), KekKey{}, "kek"),
		executor2,
	)
	assert.Nil(t, err)
}

func Test_versionHandle_HandleClusterUpgrade1(t *testing.T) {
	clusterUpgEntries = []versions.UpgradeEntry{}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v1.3.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	stub1 := gostub.Stub(&InsertInitDataKey,
		func(_ executor.TxnExecutor, _ string) (err error) {
			return moerr.NewInternalErrorNoCtx("should report error")
		})
	defer stub1.Reset()

	err := v.HandleClusterUpgrade(
		context.WithValue(context.Background(), KekKey{}, "kek"),
		executor2,
	)
	assert.Error(t, err)
}

func Test_HandleClusterUpgrade_upg_system_metrics_schema(t *testing.T) {
	sid := ""

	// test upg_rename_system_stmt_info_120
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := MockExecutor_CheckTableDefinitionExist(txnOperator)
			upg_rename_system_stmt_info_120.Upgrade(executor, uint32(0))
		},
	)

	// test upg_create_system_stmt_info_130
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := MockExecutor_CheckTableDefinitionExist(txnOperator)
			upg_create_system_stmt_info_130.Upgrade(executor, uint32(0))
		},
	)

	// test upg_rename_system_metrics_metric_120
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := MockExecutor_CheckTableDefinitionExist(txnOperator)
			upg_rename_system_metrics_metric_120.Upgrade(executor, uint32(0))
		},
	)

	// test upg_create_system_metrics_metric_130
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := MockExecutor_CheckTableDefinitionExist(txnOperator)
			upg_create_system_metrics_metric_130.Upgrade(executor, uint32(0))
		},
	)

	// test upg_system_metrics_sql_stmt_cu_comment
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := MockExecutor_CheckTableComment(txnOperator, systemMetricSqlStmtCuComment121)
			upg_system_metrics_sql_stmt_cu_comment.Upgrade(executor, uint32(0))
		},
	)

	// test upg_system_metrics_sql_stmt_cu_comment
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			err := moerr.GetOkExpectedEOF()
			executor := MockExecutor_CheckTableComment_Error(txnOperator, systemMetricSqlStmtCuComment121, err)
			got := upg_system_metrics_sql_stmt_cu_comment.Upgrade(executor, uint32(0))
			require.Equal(t, err, got)
		},
	)

}

func MockExecutor_CheckTableDefinitionExist(txnOperator *mock_frontend.MockTxnOperator) executor.TxnExecutor {
	const checkTableDefinitionPrefix = "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables tbl"

	return executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(checkTableDefinitionPrefix)) {
			typs := []types.Type{
				types.New(types.T_varchar, 64, 0), // reldatabase
				types.New(types.T_varchar, 64, 0), // relname
				types.New(types.T_uint64, 0, 0),   // account_id
			}

			memRes := executor.NewMemResult(
				typs,
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{"db_name"})
			executor.AppendStringRows(memRes, 1, []string{"tbl_name"})
			executor.AppendFixedRows(memRes, 2, []uint64{0})
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}, txnOperator)
}

func MockExecutor_CheckTableComment(txnOperator *mock_frontend.MockTxnOperator, comment string) executor.TxnExecutor {
	const checkTableCommentPrefix = "SELECT reldatabase, relname, account_id, rel_comment FROM mo_catalog.mo_tables tbl"

	return executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(checkTableCommentPrefix)) {
			typs := []types.Type{
				types.New(types.T_varchar, 64, 0),   // reldatabase
				types.New(types.T_varchar, 64, 0),   // relname
				types.New(types.T_uint64, 0, 0),     // account_id
				types.New(types.T_varchar, 1024, 0), // rel_comment
			}

			memRes := executor.NewMemResult(
				typs,
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{"db_name"})
			executor.AppendStringRows(memRes, 1, []string{"tbl_name"})
			executor.AppendFixedRows(memRes, 2, []uint64{0})
			executor.AppendStringRows(memRes, 3, []string{comment})
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	}, txnOperator)
}

func MockExecutor_CheckTableComment_Error(txnOperator *mock_frontend.MockTxnOperator, comment string, err error) executor.TxnExecutor {
	const checkTableCommentPrefix = "SELECT reldatabase, relname, account_id, rel_comment FROM mo_catalog.mo_tables tbl"

	return executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(strings.ToLower(sql), strings.ToLower(checkTableCommentPrefix)) {
			typs := []types.Type{
				types.New(types.T_varchar, 64, 0),   // reldatabase
				types.New(types.T_varchar, 64, 0),   // relname
				types.New(types.T_uint64, 0, 0),     // account_id
				types.New(types.T_varchar, 1024, 0), // rel_comment
			}

			memRes := executor.NewMemResult(
				typs,
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{"db_name"})
			executor.AppendStringRows(memRes, 1, []string{"tbl_name"})
			executor.AppendFixedRows(memRes, 2, []uint64{0})
			executor.AppendStringRows(memRes, 3, []string{comment})
			return memRes.GetResult(), err
		}
		return executor.Result{}, nil
	}, txnOperator)
}
