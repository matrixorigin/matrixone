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

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
