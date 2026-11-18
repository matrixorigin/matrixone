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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockSQLExecutor struct {
}

type labelCapturingSQLExecutor struct {
	execLabels  []map[string]string
	txnLabels   []map[string]string
	execIntents []schedule.SchedulingIntent
	txnIntents  []schedule.SchedulingIntent
}

func (m *labelCapturingSQLExecutor) Exec(_ context.Context, _ string, opts executor.Options) (executor.Result, error) {
	m.execLabels = append(m.execLabels, opts.CNLabels())
	m.execIntents = append(m.execIntents, opts.QuerySchedulingIntent())
	return executor.Result{}, nil
}

func (m *labelCapturingSQLExecutor) ExecTxn(
	_ context.Context,
	_ func(txn executor.TxnExecutor) error,
	opts executor.Options,
) error {
	m.txnLabels = append(m.txnLabels, opts.CNLabels())
	m.txnIntents = append(m.txnIntents, opts.QuerySchedulingIntent())
	return nil
}

func (m *MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {

	return executor.Result{}, nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
func (m *MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return nil
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

func TestProcessBackedSQLPropagatesCNLabels(t *testing.T) {
	const service = ""
	capturing := &labelCapturingSQLExecutor{}
	moruntime.SetupServiceBasedRuntime(service, moruntime.DefaultRuntime())
	rt := moruntime.ServiceRuntime(service)
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, capturing)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		}
	})

	proc := testutil.NewProcessWithMPool(t, service, mpool.MustNewZero())
	proc.Ctx = context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(42))
	proc.Base.SessionInfo.CNLabels = map[string]string{"account": "tp", "role": "tp"}
	intent := schedule.SchedulingIntent{
		Explicit: true, PoolFallback: schedule.PoolFallbackStrict, EmptyWorkerPolicy: schedule.EmptyWorkerFail,
	}
	proc.Base.SessionInfo.QuerySchedulingIntent = intent
	sqlproc := NewSqlProcess(proc)

	_, err := RunSql(sqlproc, "select 1")
	require.NoError(t, err)
	_, err = RunStreamingSql(context.Background(), sqlproc, "select 2", make(chan executor.Result), make(chan error))
	require.NoError(t, err)
	err = RunTxn(sqlproc, func(executor.TxnExecutor) error { return nil })
	require.NoError(t, err)

	// Standalone background contexts have no parent statement and must remain
	// unconstrained rather than borrowing a label from another invocation.
	background := NewSqlProcessWithContext(NewSqlContext(context.Background(), service, nil, 42, nil))
	_, err = RunSql(background, "select background")
	require.NoError(t, err)

	expected := map[string]string{"account": "tp", "role": "tp"}
	require.Equal(t, []map[string]string{expected, expected, nil}, capturing.execLabels)
	require.Equal(t, []map[string]string{expected}, capturing.txnLabels)
	require.Equal(t, []schedule.SchedulingIntent{intent, intent, {}}, capturing.execIntents)
	require.Equal(t, []schedule.SchedulingIntent{intent}, capturing.txnIntents)

	proc.Base.SessionInfo.CNLabels["role"] = "ap"
	require.Equal(t, "tp", capturing.execLabels[0]["role"])
	require.Equal(t, "tp", capturing.txnLabels[0]["role"])
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

type txnStartTestEngine struct {
	engine.Engine
	applied timestamp.Timestamp
	newErr  error
	calls   int
}

func (e *txnStartTestEngine) LatestLogtailAppliedTime() timestamp.Timestamp { return e.applied }
func (e *txnStartTestEngine) New(context.Context, client.TxnOperator) error {
	e.calls++
	return e.newErr
}

type txnStartTestClient struct {
	client.TxnClient
	latest timestamp.Timestamp
	min    timestamp.Timestamp
	op     client.TxnOperator
	err    error
}

func (c *txnStartTestClient) GetLatestCommitTS() timestamp.Timestamp { return c.latest }
func (c *txnStartTestClient) New(_ context.Context, min timestamp.Timestamp, _ ...client.TxnOption) (client.TxnOperator, error) {
	c.min = min
	return c.op, c.err
}

type txnStartTestOperator struct {
	client.TxnOperator
	rollbacks   int
	rollbackErr error
}

func (op *txnStartTestOperator) Rollback(context.Context) error {
	op.rollbacks++
	return op.rollbackErr
}

func TestGetTxnUsesLatestCommitAndRollsBackSetupFailures(t *testing.T) {
	t.Run("latest local commit is a snapshot lower bound", func(t *testing.T) {
		latest := timestamp.Timestamp{PhysicalTime: 20, LogicalTime: 2}
		op := &txnStartTestOperator{}
		txnClient := &txnStartTestClient{latest: latest, op: op}
		txnEngine := &txnStartTestEngine{applied: timestamp.Timestamp{PhysicalTime: 10}}
		got, err := GetTxn(t.Context(), txnEngine, txnClient, "test")
		require.NoError(t, err)
		require.Same(t, op, got)
		require.Equal(t, latest, txnClient.min)
	})

	t.Run("creation returns transaction and error", func(t *testing.T) {
		createErr := errors.New("transaction creation failed")
		op := &txnStartTestOperator{}
		txnClient := &txnStartTestClient{op: op, err: createErr}
		txnEngine := &txnStartTestEngine{}
		got, err := GetTxn(t.Context(), txnEngine, txnClient, "test")
		require.Nil(t, got)
		require.ErrorIs(t, err, createErr)
		require.Equal(t, 1, op.rollbacks)
		require.Zero(t, txnEngine.calls)
	})

	t.Run("engine setup fails", func(t *testing.T) {
		setupErr := errors.New("engine setup failed")
		op := &txnStartTestOperator{}
		txnClient := &txnStartTestClient{op: op}
		txnEngine := &txnStartTestEngine{newErr: setupErr}
		got, err := GetTxn(t.Context(), txnEngine, txnClient, "test")
		require.Nil(t, got)
		require.ErrorIs(t, err, setupErr)
		require.Equal(t, 1, op.rollbacks)
	})
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
