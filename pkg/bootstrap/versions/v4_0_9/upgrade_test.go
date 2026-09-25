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

package v4_0_9

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestColumnsUpgradeMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.9", m.Version)
	require.Equal(t, "4.0.8", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeTenant)
	require.Equal(t, versions.No, m.UpgradeCluster)
	require.Equal(t, defines.MORPCVersion97, m.RequiredProtocolVersion)
	require.Equal(t, uint32(1), m.VersionOffset)
}

func TestColumnsUpgradeLifecycle(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := context.Background()
		operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
		txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			require.True(t, strings.HasPrefix(sql, "SELECT tbl.rel_createsql"), sql)
			mp := mpool.MustNewZero()
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
			r.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(r, 0, []string{sysview.InformationSchemaColumnsDDL}))
			return r.GetResult(), nil
		}, operator)
		require.NoError(t, Handler.Prepare(ctx, txn, true))
		require.NoError(t, Handler.HandleTenantUpgrade(ctx, int32(catalog.System_Account), txn))
		require.Error(t, Handler.HandleCreateFrameworkDeps(txn))
		injected := errors.New("injected")
		failed := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, injected }, operator)
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failed), injected)
	})
}
