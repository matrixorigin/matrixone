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
	mockmorpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The root cancellation callback must retire the query-scoped cache even when
// StopSending is recorded before the root pipeline itself is registered.
func TestCurrentRolesCacheStopBeforePipelineRegistration(t *testing.T) {
	proc := newIssue27261Process(t)
	ctrl := gomock.NewController(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	started, canceled, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	done := make(chan struct{})
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
	queryCtx := defines.AttachAccountId(context.Background(), 7)
	cache := table_function.NewCurrentRolesExecutionCache(queryCtx)
	defer cache.Close()
	proc.BuildPipelineContext(table_function.WithCurrentRolesExecutionCache(queryCtx, cache))
	stop := context.AfterFunc(proc.Ctx, cache.Close)
	defer stop()
	tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	require.NoError(t, tf.Prepare(proc))
	defer func() {
		cache.Close()
		proc.Cancel(nil)
		<-done
		tf.Free(proc, false, nil)
	}()
	go func() { defer close(done); _, err := tf.Call(proc); result <- err }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not start")
	}
	server := colexec.NewServer("")
	session := mockmorpc.NewMockClientSession(ctrl)
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
	const streamID = uint64(46)
	server.CancelPipelineSending(session, streamID)
	server.RecordBuiltPipeline(session, streamID, proc)
	defer server.RemoveRelatedPipeline(session, streamID)
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("cache loader was not canceled")
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("table function did not return")
	}
	require.NoError(t, queryCtx.Err())
}

// Inject a live cache at admission because the synthetic bounded-import graph
// cannot also contain a role scan. Run still owns its retirement on the real
// DOP recompile failure path.
func TestRunDOPRecompileFailureRetiresLoadingCache(t *testing.T) {
	c := memoryRecompileFixture(t, 4, nil)
	oldScopes, oldPlan := c.scopes, c.pn
	defer func() { c.scopes, c.pn = oldScopes, oldPlan }()
	c.scopes = []*Scope{memorySourceScope(c.addr, 4, 1)}
	c.pn = &plan.Plan{}
	c.proc.ReplaceTopCtx(context.Background())

	proc := newIssue27261Process(t)
	ctrl := gomock.NewController(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	started, canceled, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	allowOldReturn := make(chan struct{})
	var unblockOld sync.Once
	unblock := func() { unblockOld.Do(func() { close(allowOldReturn) }) }
	defer unblock()
	finished := make(chan struct{})
	launched := false
	sql := executortest.NewMockSQLExecutor(ctrl)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
	sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-allowOldReturn
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
	queryCtx := defines.AttachAccountId(context.Background(), 7)
	cache := table_function.NewCurrentRolesExecutionCache(queryCtx)
	defer cache.Close()
	proc.BuildPipelineContext(table_function.WithCurrentRolesExecutionCache(queryCtx, cache))
	tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	require.NoError(t, tf.Prepare(proc))
	defer tf.Free(proc, false, nil)
	admission := &compilerMemoryAdmission{available: 0}
	admission.onTry = func() {
		admission.onTry = nil
		c.currentRolesCache = cache
		launched = true
		go func() { defer close(finished); _, err := tf.Call(proc); done <- err }()
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("loader did not start")
		}
	}
	crt := moruntime.ServiceRuntime(c.proc.GetService())
	prior, _ := crt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	crt.SetGlobalVariables(moruntime.CNMemoryThrottler, admission)
	defer crt.SetGlobalVariables(moruntime.CNMemoryThrottler, prior)
	defer func() {
		unblock()
		cache.Close()
		proc.Cancel(nil)
		if launched {
			<-finished
		}
	}()
	_, err := c.Run(0)
	require.ErrorContains(t, err, "query '' is not yet implemented")
	require.Greater(t, admission.tries, 0)
	require.Nil(t, c.currentRolesCache)
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("loader was not canceled")
	}
	// The old loader is still blocked after Run retires it. Reuse the exact
	// Compile object while the old loader retains only its independent process.
	compileProc := c.proc
	pool := &singleCompilePool{}
	c.releaseToPool(pool)
	next := allocateNewCompileFromPool(compileProc, pool)
	require.Same(t, c, next)
	nextCache := table_function.NewCurrentRolesExecutionCache(queryCtx)
	next.currentRolesCache = nextCache
	defer nextCache.Close()
	unblock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not return")
	}
	require.Same(t, nextCache, next.currentRolesCache)
	// A fresh consumer can still load and cache its own failure after the old
	// generation finishes. A second call must not issue another catalog query.
	newFailure := errors.New("new generation catalog error")
	sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(executor.Result{}, newFailure).Times(1)
	proc.BuildPipelineContext(table_function.WithCurrentRolesExecutionCache(queryCtx, nextCache))
	for i := 0; i < 2; i++ {
		consumer := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
		require.NoError(t, consumer.Prepare(proc))
		_, err := consumer.Call(proc)
		consumer.Free(proc, false, nil)
		require.ErrorContains(t, err, newFailure.Error())
	}
}
