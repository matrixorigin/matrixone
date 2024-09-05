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
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_versionHandle_HandleTenantUpgrade(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				return executor.Result{}, nil
			}, txnOperator)

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
