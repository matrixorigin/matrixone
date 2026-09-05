// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type spySQLExecutor struct {
	ctx  context.Context
	opts executor.Options
}

func (s *spySQLExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	s.ctx = ctx
	s.opts = opts
	return executor.Result{}, nil
}

func (s *spySQLExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	s.ctx = ctx
	return nil
}

func TestCompileRunSqlWithResultAndOptionsUsesTopContextWhenProcCtxNil(t *testing.T) {
	proc := testutil.NewProcess(t)
	topCtx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(topCtx)
	proc.Ctx = nil

	spyExec := &spySQLExecutor{}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)

	c := &Compile{proc: proc, pn: &pbplan.Plan{}}

	_, err := c.runSqlWithResultAndOptions("select 1", NoAccountId, executor.StatementOption{})
	require.NoError(t, err)
	require.NotNil(t, spyExec.ctx)

	accountID, err := defines.GetAccountId(spyExec.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(catalog.System_Account), accountID)
}

func TestCompileRunSqlWithResultAndOptionsPreservesSchedulingConstraints(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.CNLabels = map[string]string{"account": "tp", "role": "tp"}
	intent := schedule.SchedulingIntent{
		Explicit:          true,
		PoolFallback:      schedule.PoolFallbackStrict,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
	}
	proc.Base.SessionInfo.QuerySchedulingIntent = intent

	spyExec := &spySQLExecutor{}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
	c := &Compile{proc: proc, pn: &pbplan.Plan{}}

	_, err := c.runSqlWithResultAndOptions("select 1", NoAccountId, executor.StatementOption{})
	require.NoError(t, err)
	require.Equal(t, proc.Base.SessionInfo.CNLabels, spyExec.opts.CNLabels())
	require.Equal(t, intent, spyExec.opts.QuerySchedulingIntent())
}

func TestSQLExecutorExecTxnHandlesNilContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	s := &sqlExecutor{}

	err := s.ExecTxn(nil, func(executor.TxnExecutor) error {
		return nil
	}, executor.Options{}.WithTxn(txnOp))
	require.NoError(t, err)
}
