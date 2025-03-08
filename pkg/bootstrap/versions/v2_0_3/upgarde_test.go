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

package v2_0_3

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
			Version: "v2.0.3",
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
			Version: "v2.0.3",
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
			Version: "v2.0.3",
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
