// Copyright 2025 Matrix Origin
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

package v2_1_0

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
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

func Test_versionHandle_HandleClusterUpgrade(t *testing.T) {
	clusterUpgEntries = []versions.UpgradeEntry{}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v2.1.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	err := v.HandleClusterUpgrade(context.Background(),
		executor2,
	)
	assert.Nil(t, err)
}

func Test_HandleTenantUpgrade(t *testing.T) {
	upg_information_schema_table_test := versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "table_test",
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    "create table information_schema.table_test(a int primary,b int,c varchar(20));",
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_test")
		},
	}
	tenantUpgEntries = []versions.UpgradeEntry{upg_information_schema_table_test}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v2.1.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	err := v.HandleTenantUpgrade(context.Background(),
		0,
		executor2,
	)
	assert.Error(t, err)
}

func Test_HandleTenantUpgrade2(t *testing.T) {
	upg_information_schema_table_test := versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "table_test",
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    "create table information_schema.table_test(a int primary,b int,c varchar(20));",
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			return true, nil
		},
	}
	tenantUpgEntries = []versions.UpgradeEntry{upg_information_schema_table_test}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v2.1.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	err := v.HandleTenantUpgrade(context.Background(),
		0,
		executor2,
	)
	assert.Nil(t, err)
}

func Test_UpgEntry(t *testing.T) {
	checkSql := `att_comment AS COLUMN_COMMENT FROM mo_catalog.mo_columns`

	sid := ""
	// test upg_mo_user_add_password_last_changed
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.Contains(strings.ToLower(sql), strings.ToLower(checkSql)) {
					typs := []types.Type{
						types.New(types.T_varchar, 64, 0),   // DATA_TYPE
						types.New(types.T_varchar, 64, 0),   // IS_NULLABLE
						types.New(types.T_int64, 0, 0),      // CHARACTER_MAXIMUM_LENGTH
						types.New(types.T_int64, 0, 0),      // NUMERIC_PRECISION
						types.New(types.T_int64, 0, 0),      // NUMERIC_SCALE
						types.New(types.T_int64, 0, 0),      // NUMERIC_SCALE
						types.New(types.T_int32, 0, 0),      // ORDINAL_POSITION
						types.New(types.T_varchar, 1024, 0), // COLUMN_DEFAULT
						types.New(types.T_varchar, 1024, 0), // EXTRA
						types.New(types.T_varchar, 1024, 0), // COLUMN_COMMENT
					}

					memRes := executor.NewMemResult(
						typs,
						mpool.MustNewZero())
					memRes.NewBatch()
					executor.AppendStringRows(memRes, 0, []string{"DATA_TYPE_str"})
					executor.AppendStringRows(memRes, 1, []string{"YES"}) // or "NO"
					executor.AppendFixedRows(memRes, 2, []int64{0})
					executor.AppendFixedRows(memRes, 3, []int64{0})
					executor.AppendFixedRows(memRes, 4, []int64{0})
					executor.AppendFixedRows(memRes, 5, []int64{0})
					executor.AppendFixedRows(memRes, 6, []int32{0})
					executor.AppendStringRows(memRes, 7, []string{"COLUMN_DEFAULT"})
					executor.AppendStringRows(memRes, 8, []string{"EXTRA"}) // '' or 'auto_increment'
					executor.AppendStringRows(memRes, 9, []string{"COLUMN_COMMENT"})
					return memRes.GetResult(), nil
				}
				return executor.Result{}, nil
			}, txnOperator)
			upg_mo_pitr_add_status.Upgrade(executor, uint32(0))
			upg_mo_pitr_add_status_changed_time.Upgrade(executor, uint32(0))
		},
	)
}
