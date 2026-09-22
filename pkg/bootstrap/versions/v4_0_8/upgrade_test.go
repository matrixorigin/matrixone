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
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestDurableViewRecoveryUpgrade(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, "4.0.8", Handler.Metadata().Version)
	require.Equal(t, "4.0.7", Handler.Metadata().MinUpgradeVersion)
	exists := map[string]bool{}
	created := 0
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		for _, table := range []struct{ name, ddl string }{{catalog.MO_VIEW_RECOVERY, catalog.MoViewRecoveryDDL}, {catalog.MO_VIEW_RECOVERY_WORK, catalog.MoViewRecoveryWorkDDL}} {
			if sql == table.ddl {
				exists[table.name] = true
				created++
			}
			if strings.Contains(sql, "relname = '"+table.name+"'") && exists[table.name] {
				r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mpool.MustNewZero())
				r.NewBatchWithRowCount(1)
				return r.GetResult(), nil
			}
		}
		return executor.Result{}, nil
	}, nil)
	require.NoError(t, Handler.Prepare(ctx, txn, false))
	require.NoError(t, Handler.HandleClusterUpgrade(ctx, txn))
	require.NoError(t, Handler.HandleClusterUpgrade(ctx, txn))
	require.Equal(t, 2, created)
	// The upstream 4.0.8 handler now performs the tenant STATISTICS migration;
	// its transaction/runtime fixture is covered by statistics_upgrade_test.go.
	// This fixture only exercises the independent cluster migration.
	require.Error(t, Handler.HandleCreateFrameworkDeps(txn))
}

func TestDurableViewRecoveryUpgradeFailure(t *testing.T) {
	for failAt := 1; failAt <= 5; failAt++ {
		calls := 0
		operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
		injected := errors.New("injected upgrade failure")
		txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
			calls++
			if calls == failAt {
				return executor.Result{}, injected
			}
			return executor.Result{}, nil
		}, operator)
		require.ErrorIs(t, Handler.HandleClusterUpgrade(context.Background(), txn), injected)
		require.Equal(t, failAt, calls, "a failed migration must stop, not initialize a partial coordinator")
	}
}
