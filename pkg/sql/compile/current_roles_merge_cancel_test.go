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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type reviewWaiterContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (ctx *reviewWaiterContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

type reviewErrorAfterRoleLoad struct {
	*colexec.MockOperator
	started <-chan struct{}
	failed  chan struct{}
	err     error
}

func (op *reviewErrorAfterRoleLoad) Call(proc *process.Process) (vm.CallResult, error) {
	select {
	case <-op.started:
	case <-proc.Ctx.Done():
		return vm.CancelResult, proc.Ctx.Err()
	}
	close(op.failed)
	return vm.CancelResult, op.err
}

func TestReviewLocalMergeFailureCancelsRoleLoader(t *testing.T) {
	t.Run("merge failure", func(t *testing.T) {
		testMergeFailureCancelsRoleLoader(t, errors.New("merge failure"), false)
	})
	t.Run("independent child cancellation", func(t *testing.T) {
		testMergeFailureCancelsRoleLoader(t, context.Canceled, true)
	})
}

func testMergeFailureCancelsRoleLoader(t *testing.T, failure error, childFails bool) {
	t.Helper()
	proc := newIssue27261Process(t)
	ctrl := gomock.NewController(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	started, canceled := make(chan struct{}), make(chan struct{})
	sql := executortest.NewMockSQLExecutor(ctrl)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
	sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return executor.Result{}, ctx.Err()
	}).Times(1)
	tx := mockfrontend.NewMockTxnOperator(ctrl)
	ws := mockfrontend.NewMockWorkspace(ctrl)
	proc.Base.TxnOperator = tx
	tx.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("txn")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
	ws.EXPECT().WriteOffset().Return(uint64(0)).AnyTimes()
	proc.Base.SessionInfo.CurrentRoleContext = process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 9}

	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), 7))
	tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	failed := make(chan struct{})
	sink := &reviewErrorAfterRoleLoad{MockOperator: colexec.NewMockOperator(), started: started, failed: failed, err: failure}
	child := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: tf}
	parent := &Scope{Magic: Merge, Proc: proc.NewNoContextChildProc(0), RootOp: sink, PreScopes: []*Scope{child}}
	if childFails {
		// The dependency returns context.Canceled while both scopes are live.
		// The parent is blocked inside the query-owned loader.
		parent.RootOp, child.RootOp = child.RootOp, parent.RootOp
	}
	c := &Compile{proc: proc, hasMergeOp: true, scopes: []*Scope{parent}, counterSet: &perfcounter.CounterSet{}}
	c.InitPipelineContextToExecuteQuery()
	done := runIssue27261Scope(t, proc, func() error { return parent.MergeRun(c) })
	t.Cleanup(c.closeCurrentRolesExecutionCache)
	select {
	case <-failed:
	case <-time.After(10 * time.Second):
		t.Fatal("sink did not reach error")
	}
	select {
	case <-parent.Proc.Ctx.Done():
		require.ErrorIs(t, context.Cause(parent.Proc.Ctx), failure)
	case <-time.After(2 * time.Second):
		t.Fatal("merge did not cancel pipeline")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("terminal failure did not cancel active role loader")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, failure)
	case <-time.After(2 * time.Second):
		t.Error("MergeRun still waits for role loader after sibling failure")
	}
}

func TestCurrentRolesCacheSurvivesInternalPipelineStop(t *testing.T) {
	proc := newIssue27261Process(t)
	ctrl := gomock.NewController(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	started, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("catalog finished")
	sql := executortest.NewMockSQLExecutor(ctrl)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
	sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
		close(started)
		select {
		case <-release:
			return executor.Result{}, failure
		case <-ctx.Done():
			return executor.Result{}, ctx.Err()
		}
	}).Times(1)
	tx := mockfrontend.NewMockTxnOperator(ctrl)
	ws := mockfrontend.NewMockWorkspace(ctrl)
	proc.Base.TxnOperator = tx
	tx.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("txn")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
	ws.EXPECT().WriteOffset().Return(uint64(0)).AnyTimes()
	proc.Base.SessionInfo.CurrentRoleContext = process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 9}
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), 7))
	tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	consumer := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: tf}
	stopped := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: &scopeRunCancelErrorOperator{
		MockOperator: colexec.NewMockOperator(), runErr: context.Canceled,
	}}
	merge := &Scope{Magic: Merge, Proc: proc.NewNoContextChildProc(0), PreScopes: []*Scope{stopped}}
	c := &Compile{proc: proc, hasMergeOp: true, scopes: []*Scope{consumer, merge}, counterSet: &perfcounter.CounterSet{}}
	c.InitPipelineContextToExecuteQuery()
	require.NoError(t, tf.Prepare(consumer.Proc))
	done := make(chan error, 1)
	go func() { _, err := tf.Call(consumer.Proc); done <- err }()
	defer func() {
		c.closeCurrentRolesExecutionCache()
		proc.Cancel(nil)
		<-done
		tf.Free(consumer.Proc, false, nil)
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not start")
	}
	require.NoError(t, merge.MergeRun(c))
	require.NoError(t, merge.Proc.Ctx.Err())
	close(release)
	select {
	case err := <-done:
		done <- err
		require.ErrorContains(t, err, failure.Error())
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not finish")
	}
}

// TestCurrentRolesMultiRootFailureAndRetry exercises runOnce's multi-root
// error path and verifies the retry context installs a usable new generation.
func TestCurrentRolesMultiRootFailureAndRetry(t *testing.T) {
	proc := newIssue27261Process(t)
	ctrl := gomock.NewController(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	started, canceled := make(chan struct{}), make(chan struct{})
	retryResult := errors.New("retry catalog reached")
	sql := executortest.NewMockSQLExecutor(ctrl)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
	gomock.InOrder(
		sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return executor.Result{}, ctx.Err()
		}),
		sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(executor.Result{}, retryResult),
	)
	tx := mockfrontend.NewMockTxnOperator(ctrl)
	ws := mockfrontend.NewMockWorkspace(ctrl)
	proc.Base.TxnOperator = tx
	tx.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("txn")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
	ws.EXPECT().WriteOffset().Return(uint64(0)).AnyTimes()
	proc.Base.SessionInfo.CurrentRoleContext = process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 9}
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), 7))
	newTF := func() *table_function.TableFunction {
		return &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	}
	tf := newTF()
	failed := make(chan struct{})
	waiting := make(chan struct{})
	loader := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: tf}
	failure := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: &reviewErrorAfterRoleLoad{
		MockOperator: colexec.NewMockOperator(), started: waiting, failed: failed, err: errors.New("review sibling failure"),
	}}
	c := &Compile{proc: proc, scopes: []*Scope{loader, failure}, counterSet: &perfcounter.CounterSet{}}
	c.InitPipelineContextToExecuteQuery()
	done := make(chan error, 1)
	go func() { done <- c.runOnce() }()
	defer func() {
		c.closeCurrentRolesExecutionCache()
		proc.Cancel(nil)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("runOnce cleanup did not join")
		}
	}()
	// A second table function joins the same cache while the first catalog
	// operation is blocked. The error scope waits until its select is reached.
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not start")
	}
	waiterProc := loader.Proc.NewNoContextChildProc(0)
	waiterProc.Ctx = &reviewWaiterContext{Context: loader.Proc.Ctx, waiting: waiting}
	waiterTF := newTF()
	require.NoError(t, waiterTF.Prepare(waiterProc))
	waiterDone := make(chan error, 1)
	go func() { _, err := waiterTF.Call(waiterProc); waiterDone <- err }()
	defer func() {
		c.closeCurrentRolesExecutionCache()
		proc.Cancel(nil)
		select {
		case <-waiterDone:
		case <-time.After(10 * time.Second):
			t.Error("waiter did not join")
		}
		waiterTF.Free(waiterProc, false, nil)
	}()

	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("multi-root failure did not cancel loader")
	}
	select {
	case err := <-waiterDone:
		waiterDone <- err
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("multi-root failure did not wake waiter")
	}
	select {
	case err := <-done:
		done <- err
		require.ErrorContains(t, err, "review sibling failure")
	case <-time.After(10 * time.Second):
		t.Fatal("multi-root execution did not return")
	}
	old := c.currentRolesCache
	retryTF := newTF()
	retryScope := &Scope{Magic: Normal, Proc: proc.NewNoContextChildProc(0), RootOp: retryTF}
	c.scopes = []*Scope{retryScope}
	c.InitPipelineContextToRetryQuery()
	require.NotSame(t, old, c.currentRolesCache)
	require.NoError(t, retryTF.Prepare(retryScope.Proc))
	defer retryTF.Free(retryScope.Proc, false, nil)
	_, err := retryTF.Call(retryScope.Proc)
	require.ErrorIs(t, err, retryResult)
}
