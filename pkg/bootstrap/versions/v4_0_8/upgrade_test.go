// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_8

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestClusterUpgradeRejectsOldWritersBeforeCatalogMutation(t *testing.T) {
	for _, protocol := range []string{
		`{"result":"cn0:135,cn1:134,tn0:135"}`,
		`{"result":"cn0:135,tn0:134"}`,
	} {
		t.Run(protocol, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			operator := mockfrontend.NewMockTxnOperator(gomock.NewController(t))
			operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
			var calls int
			txn := executor.NewMemTxnExecutor(func(query string) (executor.Result, error) {
				calls++
				if query != "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
					return executor.Result{}, fmt.Errorf("unexpected catalog mutation or probe: %s", query)
				}
				result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(result, 0, []string{protocol}))
				return result.GetResult(), nil
			}, operator)
			require.Error(t, Handler.HandleClusterUpgrade(context.Background(), txn))
			require.Equal(t, 1, calls, "an old CN or TN must stop all migration work")
		})
	}
}
