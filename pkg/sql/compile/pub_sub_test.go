// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type createSubscriptionExecutor struct {
	t              *testing.T
	selectResult   executor.Result
	updateAffected uint64
	calls          int
}

func (e *createSubscriptionExecutor) Exec(
	_ context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	e.calls++
	if e.calls == 1 {
		require.Contains(e.t, sql, "SELECT count(1)")
		return e.selectResult, nil
	}
	require.Contains(e.t, sql, "status = 0")
	return executor.Result{AffectedRows: e.updateAffected}, nil
}

func (e *createSubscriptionExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	return nil
}

func TestCreateSubscriptionValidatesCurrentAuthorization(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		updateAffected uint64
		wantErr        bool
	}{
		{name: "authorized", updateAffected: 1},
		{name: "revoked or deleted", updateAffected: 0, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctx := defines.AttachAccountId(context.Background(), catalog.System_Account+1)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)

			memResult := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, proc.Mp())
			memResult.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(memResult, 0, []int64{0}))
			spy := &createSubscriptionExecutor{
				t:              t,
				selectResult:   memResult.GetResult(),
				updateAffected: testCase.updateAffected,
			}
			moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spy)

			c := &Compile{proc: proc, pn: &plan.Plan{}}
			err := createSubscription(ctx, c, "sub", &plan.SubscriptionOption{
				From: "publisher", Publication: "publication",
			})
			if testCase.wantErr {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "not authorized") || strings.Contains(err.Error(), "no longer exists"))
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 2, spy.calls)
		})
	}
}

func Test_updatePubTableList(t *testing.T) {
	err := updatePubTableList(context.Background(), nil, "information_schema", "t1")
	require.NoError(t, err)
}
