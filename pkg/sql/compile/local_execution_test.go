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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestInternalLocalExecutionAvoidsRemoteScheduling(t *testing.T) {
	plan2.SetForceScanOnMultiCN(true)
	defer plan2.SetForceScanOnMultiCN(false)

	for _, local := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "local"}[local], func(t *testing.T) {
			ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
			if local {
				ctx = executor.WithLocalExecution(ctx)
			}
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			txnClient, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnClient, proc.Base.TxnOperator = txnClient, txnOp
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)
			compilerCtx := plan2.NewEmptyCompilerContext(newPlanTestProcess(t))
			compilerCtx.SetContext(ctx)
			stmts, err := mysql.Parse(ctx, "select 1", 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := plan2.NewBaseOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
			backend := &schedulerTestEngine{
				Engine: newStubEngine(),
				nodes: engine.Nodes{
					{Addr: "local:6001", Mcpu: 4},
					{Addr: "remote:6001", Mcpu: 4},
				},
			}
			c := NewCompile("local:6001", "test", "select 1", "", "", backend, proc, stmts[0], false, nil, time.Now())
			defer c.Release()
			require.NoError(t, c.Compile(ctx, queryPlan, nil))
			if local {
				require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
				require.Zero(t, backend.calls, "local SQL must not discover remote query workers")
				require.Len(t, c.cnList, 1)
				require.Equal(t, "local:6001", c.cnList[0].Addr)
			} else {
				require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
				require.Equal(t, 1, backend.calls)
				require.Len(t, c.cnList, 2)
			}
		})
	}
}
