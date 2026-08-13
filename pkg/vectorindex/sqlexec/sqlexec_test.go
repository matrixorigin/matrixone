// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockSQLExecutor struct {
}

func (m *MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {

	return executor.Result{}, nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
func (m *MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return nil
}

type identityCapturingSQLExecutor struct {
	ctx  context.Context
	opts executor.Options
}

func (m *identityCapturingSQLExecutor) Exec(ctx context.Context, _ string, opts executor.Options) (executor.Result, error) {
	m.ctx = ctx
	m.opts = opts
	return executor.Result{}, nil
}

func (m *identityCapturingSQLExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

func TestSqlProcessExecutionIdentityOverride(t *testing.T) {
	uuid := "fulltext-publisher-identity"
	spy := &identityCapturingSQLExecutor{}
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spy)
	moruntime.SetupServiceBasedRuntime(uuid, rt)

	subscriberCtx := defines.AttachAccountId(context.Background(), 7)
	sqlctx := NewSqlContext(subscriberCtx, uuid, nil, 7, nil)
	sqlproc := NewSqlProcessWithContext(sqlctx).WithExecutionIdentity(42, "publisher_db")

	_, err := RunSql(sqlproc, "select 1")
	require.NoError(t, err)
	accountID, err := defines.GetAccountId(spy.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(42), accountID)
	require.Equal(t, uint32(42), spy.opts.AccountID())
	require.Equal(t, "publisher_db", spy.opts.Database())
	require.True(t, spy.opts.StatementOption().HasAccountID())
	require.Equal(t, uint32(42), spy.opts.StatementOption().AccountID())

	streamCh := make(chan executor.Result, 1)
	errCh := make(chan error, 1)
	_, err = RunStreamingSql(subscriberCtx, sqlproc, "select 1", streamCh, errCh)
	require.NoError(t, err)
	accountID, err = defines.GetAccountId(spy.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(42), accountID)
	require.Equal(t, uint32(42), spy.opts.AccountID())
	require.Equal(t, "publisher_db", spy.opts.Database())
	require.True(t, spy.opts.StatementOption().HasAccountID())
	require.Equal(t, uint32(42), spy.opts.StatementOption().AccountID())

	subscriberID, err := defines.GetAccountId(subscriberCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), subscriberID)
	require.Equal(t, uint32(7), sqlctx.AccountId)
}

func TestSqlProcessExecutionIdentityOverrideFromProcess(t *testing.T) {
	const uuid = "fulltext-publisher-process-identity"
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	lockService := mock_lock.NewMockLockService(ctrl)
	lockService.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: uuid}).AnyTimes()
	spy := &identityCapturingSQLExecutor{}
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spy)
	moruntime.SetupServiceBasedRuntime(uuid, rt)

	type contextKey struct{}
	key := contextKey{}
	subscriberCtx := context.WithValue(context.Background(), key, "subscriber")
	subscriberCtx = defines.AttachAccountId(subscriberCtx, 7)
	proc := testutil.NewProcessWithMPool(t, uuid, mpool.MustNewZero())
	proc.Base.LockService = lockService
	proc.Ctx = subscriberCtx
	proc.ReplaceTopCtx(subscriberCtx)
	proc.Base.TxnOperator = txnOp
	proc.Base.SessionInfo.Database = "subscriber_db"
	proc.Base.SessionInfo.TimeZone = time.FixedZone("subscriber-zone", 8*60*60)
	proc.Base.IsFrontend = true
	resolveVariable := func(varName string, _, _ bool) (interface{}, error) {
		return "resolved:" + varName, nil
	}
	proc.SetResolveVariableFunc(resolveVariable)

	sqlproc := NewSqlProcess(proc).WithExecutionIdentity(42, "publisher_db")
	assertExecutionIdentity := func(expectedContextValue string) {
		accountID, err := defines.GetAccountId(spy.ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(42), accountID)
		require.Equal(t, expectedContextValue, spy.ctx.Value(key))
		require.Same(t, txnOp, spy.opts.Txn())
		require.Equal(t, "publisher_db", spy.opts.Database())
		require.Same(t, proc.Base.SessionInfo.TimeZone, spy.opts.GetTimeZone())
		require.True(t, spy.opts.IsFrontend())
		value, err := spy.opts.ResolveVariableFunc()("test_variable", true, false)
		require.NoError(t, err)
		require.Equal(t, "resolved:test_variable", value)
		require.True(t, spy.opts.StatementOption().HasAccountID())
		require.Equal(t, uint32(42), spy.opts.StatementOption().AccountID())
	}

	_, err := RunSql(sqlproc, "select 1")
	require.NoError(t, err)
	assertExecutionIdentity("subscriber")

	streamCtx := context.WithValue(context.Background(), key, "stream")
	streamCtx = defines.AttachAccountId(streamCtx, 7)
	streamCh := make(chan executor.Result, 1)
	errCh := make(chan error, 1)
	_, err = RunStreamingSql(streamCtx, sqlproc, "select 1", streamCh, errCh)
	require.NoError(t, err)
	assertExecutionIdentity("stream")

	subscriberID, err := defines.GetAccountId(subscriberCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), subscriberID)
	streamSubscriberID, err := defines.GetAccountId(streamCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), streamSubscriberID)
	require.Same(t, txnOp, proc.GetTxnOperator())
	require.Equal(t, "subscriber_db", proc.Base.SessionInfo.Database)
	require.Equal(t, "subscriber", proc.GetTopContext().Value(key))
}

func TestSqlTxnError(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := NewSqlProcess(proc)
	assert.Panics(t, func() {
		RunTxn(sqlproc, func(exec executor.TxnExecutor) error {
			return nil
		})
	}, "logserivce panic")
}

func TestSqlTxn(t *testing.T) {

	uuid := ""
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, &MockSQLExecutor{})
	moruntime.SetupServiceBasedRuntime(uuid, rt)

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = context.Background()
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))

	sqlproc := NewSqlProcess(proc)

	err := RunTxn(sqlproc, func(exec executor.TxnExecutor) error {
		return nil
	})
	require.Nil(t, err)
}

func TestFinishTxnWithCleanupContextUsesFreshContext(t *testing.T) {
	bodyErr := context.Canceled
	var rollbackTenant any

	err := finishTxnWithCleanupContext(
		42,
		bodyErr,
		func(context.Context) error {
			t.Fatal("commit must not run on body error")
			return nil
		},
		func(ctx context.Context) error {
			require.NoError(t, ctx.Err())
			rollbackTenant = ctx.Value(defines.TenantIDKey{})
			return nil
		},
	)
	require.ErrorIs(t, err, bodyErr)
	require.Equal(t, uint32(42), rollbackTenant)

	rollbackErr := errors.New("rollback failed")
	err = finishTxnWithCleanupContext(
		42,
		bodyErr,
		func(context.Context) error {
			t.Fatal("commit must not run on body error")
			return nil
		},
		func(context.Context) error {
			return rollbackErr
		},
	)
	require.ErrorIs(t, err, bodyErr)
	require.ErrorIs(t, err, rollbackErr)
}

func TestFinishTxnWithCleanupContextCommitsWithFreshContext(t *testing.T) {
	var commitTenant any

	err := finishTxnWithCleanupContext(
		42,
		nil,
		func(ctx context.Context) error {
			require.NoError(t, ctx.Err())
			commitTenant = ctx.Value(defines.TenantIDKey{})
			return nil
		},
		func(context.Context) error {
			t.Fatal("rollback must not run without body error")
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint32(42), commitTenant)
}

// TestSqlProcessServiceAndAccount covers the GetService/GetAccountID accessors added for the
// cache freshness check (captured at load to re-query in the background).
func TestSqlProcessServiceAndAccount(t *testing.T) {
	// Proc-backed: just exercise the branch (values depend on the test proc).
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sp := NewSqlProcess(proc)
	_ = sp.GetService()
	_, _ = sp.GetAccountID()

	// SqlCtx-backed: deterministic service + account.
	sc := NewSqlContext(context.Background(), "cn-uuid", nil, 42, nil)
	sp2 := NewSqlProcessWithContext(sc)
	require.Equal(t, "cn-uuid", sp2.GetService())
	acc, err := sp2.GetAccountID()
	require.NoError(t, err)
	require.Equal(t, uint32(42), acc)
}
