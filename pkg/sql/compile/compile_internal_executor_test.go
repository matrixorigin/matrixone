// Copyright 2021 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type testSQLExecutor struct {
	lastCtx context.Context
}

func (e *testSQLExecutor) Exec(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
	e.lastCtx = ctx
	return executor.Result{}, nil
}

func (e *testSQLExecutor) ExecTxn(context.Context, func(txn executor.TxnExecutor) error, executor.Options) error {
	return nil
}

type testQueryClient struct {
	serviceID string
}

func (c *testQueryClient) ServiceID() string {
	return c.serviceID
}

func (c *testQueryClient) SendMessage(context.Context, string, *querypb.Request) (*querypb.Response, error) {
	return nil, nil
}

func (c *testQueryClient) NewRequest(querypb.CmdMethod) *querypb.Request {
	return nil
}

func (c *testQueryClient) Release(*querypb.Response) {}

func (c *testQueryClient) Close() error {
	return nil
}

func TestCompileGetInternalSQLExecutorFallback(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	c := &Compile{proc: proc}

	fallback := &testSQLExecutor{}
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, fallback)

	got := c.getInternalSQLExecutor()
	require.Same(t, fallback, got)
}

func TestCompileGetInternalSQLExecutorPreferCompileEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const sid = "compile-executor-test-sid"
	moruntime.SetupServiceBasedRuntime(sid, moruntime.ServiceRuntime(""))
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.LockService, mock_lock.NewMockLockService(ctrl))
	moruntime.ServiceRuntime("").SetGlobalVariables(moruntime.InternalSQLExecutor, &testSQLExecutor{})

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.QueryClient = &testQueryClient{serviceID: sid}
	proc.Base.TxnClient = mock_frontend.NewMockTxnClient(ctrl)

	c := &Compile{
		addr: "test-addr",
		e:    mock_frontend.NewMockEngine(ctrl),
		proc: proc,
	}

	got := c.getInternalSQLExecutor()
	_, ok := got.(*sqlExecutor)
	require.True(t, ok)
}

func TestRunSqlWithResultAndOptionsInjectParameterUnit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	c := &Compile{
		db:   "test",
		proc: proc,
		pn:   &planpb.Plan{},
	}

	fallback := &testSQLExecutor{}
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, fallback)
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables("parameter-unit", &config.ParameterUnit{})

	_, err := c.runSqlWithResultAndOptions("select 1", -1, executor.StatementOption{})
	require.NoError(t, err)
	require.NotNil(t, fallback.lastCtx)
	require.NotNil(t, fallback.lastCtx.Value(config.ParameterUnitKey))
}
