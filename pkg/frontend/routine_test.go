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

package frontend

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	pcg "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	util "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type routineTraceIDGenerator struct{}

func (routineTraceIDGenerator) NewIDs() (trace.TraceID, trace.SpanID) {
	var traceID trace.TraceID
	traceID[len(traceID)-1] = 1
	var spanID trace.SpanID
	spanID[len(spanID)-1] = 1
	return traceID, spanID
}

func (routineTraceIDGenerator) NewSpanID() trace.SpanID {
	var spanID trace.SpanID
	spanID[len(spanID)-1] = 2
	return spanID
}

func TestNewRoutineGeneratesTraceContext(t *testing.T) {
	previous := trace.DefaultTracer()
	trace.SetDefaultTracer(trace.NewNonRecordingTracer(routineTraceIDGenerator{}))
	t.Cleanup(func() { trace.SetDefaultTracer(previous) })

	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	spanContext := trace.SpanFromContext(routine.getCancelRoutineCtx()).SpanContext()
	require.False(t, spanContext.IsEmpty())
}

func TestRequestFinalizationContextPrefersExecutionContext(t *testing.T) {
	type contextKey struct{}
	fallback := context.WithValue(context.Background(), contextKey{}, "fallback")
	enriched := context.WithValue(context.Background(), contextKey{}, "enriched")

	require.Same(t, enriched, requestFinalizationContext(&ExecCtx{reqCtx: enriched}, fallback))
	require.Same(t, fallback, requestFinalizationContext(&ExecCtx{}, fallback))
	require.Same(t, fallback, requestFinalizationContext(nil, fallback))
}

func Test_inc_dec(t *testing.T) {
	rt := &Routine{}
	counter := int32(0)
	eg := errgroup.Group{}

	eg.Go(func() error {
		rt.increaseCount(func() {
			atomic.AddInt32(&counter, 1)
		})
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	eg.Go(func() error {
		rt.decreaseCount(func() {
			atomic.AddInt32(&counter, -1)
		})
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	err := eg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, counter, int32(0))
	assert.False(t, rt.connectionBeCounted.Load())
}

func newUnitTestRoutine(t *testing.T, connID uint32) (*Routine, *MysqlProtocolImpl) {
	t.Helper()
	pu, err := getParameterUnit("test/system_vars_config.toml", nil, nil)
	require.NoError(t, err)
	pu.SV.KillRountinesInterval = 0
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	conn := &Conn{
		conn:       &testConn{},
		localAddr:  "local",
		remoteAddr: "remote",
	}
	proto := NewMysqlClientProtocol("", connID, conn, int(pu.SV.MaxBytesInOutbufToFlush), pu.SV)
	rt := NewRoutine(context.Background(), proto, pu.SV)
	return rt, proto
}

func TestRoutineStateHelpers(t *testing.T) {
	rt, proto := newUnitTestRoutine(t, 42)

	require.True(t, rt.needPrintSessionInfo())
	require.False(t, rt.needPrintSessionInfo())

	rt.setResricted(true)
	require.True(t, rt.isRestricted())
	rt.setResricted(false)
	require.False(t, rt.isRestricted())

	rt.setExpired(true)
	require.True(t, rt.isExpired())
	rt.setExpired(false)
	require.False(t, rt.isExpired())

	require.False(t, rt.setCancelled(true))
	require.True(t, rt.isCancelled())
	require.True(t, rt.setCancelled(false))
	require.False(t, rt.isCancelled())

	require.Same(t, proto, rt.getProtocol())
	require.Equal(t, uint32(42), rt.getConnectionID())
	require.NotZero(t, rt.getGoroutineId())
	require.Same(t, rt.parameters, rt.getParameters())
	require.Nil(t, (*Routine)(nil).getSession())
}

func TestRoutineShouldCloseConnectionIgnoresRequestDeadline(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 46)
	routineCtx := rt.getCancelRoutineCtx()

	requestCtx, cancelRequest := context.WithCancelCause(routineCtx)
	cancelRequest(moerr.CauseNewMOHungSpan)
	require.Error(t, context.Cause(requestCtx))
	require.False(t, rt.shouldCloseConnection())

	rt.setCancelled(true)
	require.True(t, rt.shouldCloseConnection())
	rt.setCancelled(false)

	rt.releaseRoutineCtx()
	require.True(t, rt.shouldCloseConnection())
}

func TestRoutineRequestCallbacksAndCancelContexts(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 43)

	var called int32
	rt.execCallbackBasedOnRequest(false, func() {
		atomic.AddInt32(&called, 1)
	})
	require.Equal(t, int32(1), called)

	rt.setInProcessRequest(true)
	rt.execCallbackBasedOnRequest(false, func() {
		atomic.AddInt32(&called, 1)
	})
	require.Equal(t, int32(1), called)
	rt.execCallbackBasedOnRequest(true, func() {
		atomic.AddInt32(&called, 1)
	})
	require.Equal(t, int32(2), called)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	rt.setCancelRequestFunc(cancelReq)
	rt.cancelRequestCtx()
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)

	routineCtx := rt.getCancelRoutineCtx()
	rt.releaseRoutineCtx()
	require.Eventually(t, func() bool {
		return routineCtx.Err() == context.Canceled
	}, time.Second, 10*time.Millisecond)
}

func TestRoutineKillQueryAndConnection(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 44)
	ses := &Session{}
	ses.SetQueryInExecute(true)
	rt.setSession(ses)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	rt.setCancelRequestFunc(cancelReq)
	rt.killQuery(false, "")
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)
	require.False(t, ses.GetQueryInExecute())

	rt.setCancelled(false)
	routineCtx := rt.getCancelRoutineCtx()
	rt.killConnection(false)
	require.True(t, rt.isCancelled())
	require.ErrorIs(t, routineCtx.Err(), context.Canceled)

	rt.killConnection(false)
	require.True(t, rt.isCancelled())
}

func TestRoutineCleanupContextFallback(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 45)
	require.NotNil(t, rt.getCleanupContext())

	rm := &RoutineManager{}
	ses := &Session{}
	ses.setRoutineManager(rm)
	rt.setSession(ses)
	require.NotNil(t, rt.getCleanupContext())
}

func TestRoutineCleanupWithoutSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := &Routine{
		cancelRoutineCtx:  ctx,
		cancelRoutineFunc: cancel,
		goroutineID:       12345,
		mc:                newMigrateController(),
	}
	rt.protocol.Store(&holder[MysqlRrWr]{value: &testMysqlWriter{}})

	rt.cleanup()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cleanup did not cancel routine context")
	}
	assert.Nil(t, rt.getProtocol())
}

func TestRoutineCleanupCancelsRequestBeforeWaitingForLifecycleOperation(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	routine.setCancelRequestFunc(cancelRequest)
	require.True(t, routine.mc.beginOperation())
	released := false
	defer func() {
		if !released {
			routine.mc.endOperation()
		}
	}()

	cleaned := make(chan struct{})
	go func() {
		routine.cleanup()
		close(cleaned)
	}()

	select {
	case <-requestCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("connection cleanup waited for a lifecycle operation before canceling the query")
	}

	routine.mc.endOperation()
	released = true
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("connection cleanup did not finish after the lifecycle operation ended")
	}
}

func TestRoutineManagerClosedCancelsActiveLifecycleOperation(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	operationCtx, ok := routine.mc.beginOperationWithContext(context.Background())
	require.True(t, ok)

	operationDone := make(chan struct{})
	go func() {
		<-operationCtx.Done()
		routine.mc.endOperation()
		close(operationDone)
	}()

	conn := &Conn{}
	rm := &RoutineManager{
		ctx:              context.Background(),
		clients:          map[*Conn]*Routine{conn: routine},
		routinesByConnID: map[uint32]*Routine{0: routine},
	}
	closed := make(chan struct{})
	go func() {
		rm.Closed(conn)
		close(closed)
	}()

	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("RoutineManager.Closed did not cancel the active lifecycle operation")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("RoutineManager.Closed did not finish after lifecycle cancellation")
	}
}

func TestCanceledMigrationAdmissionDoesNotConsumeMigrateOnce(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, routine.migrateConnectionTo(ctx, &query.MigrateConnToRequest{}), context.Canceled)

	ran := false
	routine.mc.migrateOnce.Do(func() {
		ran = true
	})
	require.True(t, ran, "caller cancellation before admission must allow a later migration retry")
}

func TestCanceledResetAdmissionDoesNotTouchSession(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(
		t,
		routine.resetSessionWithContext(ctx, "", &query.ResetSessionResponse{}),
		context.Canceled,
	)
	require.True(t, routine.mc.tryBeginOperation(), "canceled reset must not retain lifecycle admission")
	routine.mc.endOperation()
}

func TestCanceledResetWaitingForRequestKeepsSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	routine := NewRoutine(context.Background(), oldSession.GetResponser().MysqlRrWr(), &config.FrontendParameters{})
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()
	rm.setBaseService(&testMOServerBaseService{id: ""})
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	t.Cleanup(func() {
		if current := routine.getSession(); current != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})

	oldProc := oldSession.GetProc()
	oldTxnHandler := oldSession.GetTxnHandler()
	require.True(t, routine.mc.tryBeginRequest())
	waitEntered := make(chan struct{})
	routine.mc.requestWaitHook = func() { close(waitEntered) }

	ctx, cancel := context.WithCancel(context.Background())
	resetResult := make(chan error, 1)
	go func() {
		resetResult <- routine.resetSessionWithContext(ctx, "", &query.ResetSessionResponse{})
	}()
	select {
	case err := <-resetResult:
		t.Fatalf("reset returned before the request was canceled: %v", err)
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("reset did not enter the request-only admission wait")
	}

	cancel()
	select {
	case err := <-resetResult:
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, routine.mc.resetWaiterPending)
	case <-time.After(time.Second):
		t.Fatal("reset did not honor cancellation while waiting for request")
	}
	require.Same(t, oldSession, routine.getSession())
	require.Same(t, oldProc, oldSession.GetProc())
	require.Same(t, oldTxnHandler, oldSession.GetTxnHandler())
	routine.mc.endRequest()
}

func TestResetAdmissionRejectsConcurrentLifecycleOperation(t *testing.T) {
	for _, owner := range []string{"reset", "migration"} {
		t.Run(owner, func(t *testing.T) {
			routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
			t.Cleanup(routine.cancelRoutineFunc)
			require.True(t, routine.mc.tryBeginOperation())
			defer routine.mc.endOperation()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, ok := routine.mc.beginOperationAfterRequestWithContext(ctx)
			require.False(t, ok, "reset admission must not queue behind concurrent %s", owner)
		})
	}
}

func TestResetAdmissionRejectsStaleLifecycleAttempt(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()
	routine := NewRoutine(context.Background(), oldSession.GetResponser().MysqlRrWr(), &config.FrontendParameters{})
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	t.Cleanup(func() {
		if current := routine.getSession(); current != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})

	// Model lifecycle A. The reset must reject this generation even if A ends
	// before the reset caller's next scheduling point.
	require.True(t, routine.mc.tryBeginOperation())
	lifecycleHeld := true
	defer func() {
		if lifecycleHeld {
			routine.mc.endOperation()
		}
	}()

	attemptReached := make(chan struct{})
	resumeAttempt := make(chan struct{})
	routine.mc.tryBeginOperationHook = func() {
		close(attemptReached)
		<-resumeAttempt
	}
	waitEntered := make(chan struct{})
	routine.mc.requestWaitHook = func() { close(waitEntered) }

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- routine.resetSessionWithContext(ctx, "", &query.ResetSessionResponse{})
	}()

	select {
	case err := <-result:
		// The fixed path makes one atomic admission decision while lifecycle A
		// is still active and therefore never invokes the optimistic hook.
		require.ErrorContains(t, err, "cannot reset session as routine is closed or busy")
		require.Same(t, oldSession, routine.getSession())
		require.False(t, routine.mc.resetWaiterPending)
	case <-attemptReached:
		// The pre-fix two-step path reaches this hook after its first failed
		// attempt. Let A finish, start request N+1, and resume the stale reset.
		routine.mc.endOperation()
		lifecycleHeld = false
		require.True(t, routine.mc.tryBeginRequest())
		close(resumeAttempt)

		select {
		case err := <-result:
			t.Fatalf("stale reset completed before request N+1 ended: %v", err)
		case <-waitEntered:
		}
		routine.mc.endRequest()

		select {
		case err := <-result:
			require.ErrorContains(t, err, "cannot reset session as routine is closed or busy")
		case <-time.After(time.Second):
			t.Fatal("stale reset did not finish after request N+1 ended")
		}
	}
}

func TestResetAdmissionStopsWhenRoutineClosesDuringRequestWait(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	require.True(t, routine.mc.tryBeginRequest())
	defer routine.mc.endRequest()

	waitEntered := make(chan struct{})
	routine.mc.requestWaitHook = func() { close(waitEntered) }
	result := make(chan bool, 1)
	go func() {
		_, ok := routine.mc.beginOperationAfterRequestWithContext(context.Background())
		result <- ok
	}()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("reset admission did not enter the request-only wait")
	}
	routine.mc.startClose()
	select {
	case ok := <-result:
		require.False(t, ok)
		require.False(t, routine.mc.resetWaiterPending)
	case <-time.After(time.Second):
		t.Fatal("reset admission did not stop after routine close")
	}
}

func TestResetAdmissionAllowsOnlyOnePendingRequestWaiter(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	require.True(t, routine.mc.tryBeginRequest())

	waitEntered := make(chan struct{})
	var waitOnce sync.Once
	routine.mc.requestWaitHook = func() {
		waitOnce.Do(func() { close(waitEntered) })
	}

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), time.Second)
	defer cancelFirst()
	firstResult := make(chan bool, 1)
	go func() {
		_, ok := routine.mc.beginOperationAfterRequestWithContext(firstCtx)
		firstResult <- ok
	}()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("first reset did not enter the request-only admission wait")
	}

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecond()
	secondResult := make(chan bool, 1)
	go func() {
		_, ok := routine.mc.beginOperationAfterRequestWithContext(secondCtx)
		secondResult <- ok
	}()
	select {
	case ok := <-secondResult:
		require.False(t, ok, "a second reset must not reserve another request waiter")
	case <-time.After(100 * time.Millisecond):
		routine.mc.endRequest()
		select {
		case ok := <-firstResult:
			if ok {
				routine.mc.endOperation()
			}
		case <-time.After(time.Second):
			t.Fatal("first reset did not finish after request release")
		}
		select {
		case ok := <-secondResult:
			require.False(t, ok, "second reset waiter must not be admitted after the first completes")
		case <-time.After(time.Second):
			t.Fatal("second reset waiter did not finish after request release")
		}
		t.Fatal("second reset incorrectly waited behind the same request")
	}

	routine.mc.endRequest()
	select {
	case ok := <-firstResult:
		require.True(t, ok)
		routine.mc.endOperation()
	case <-time.After(time.Second):
		t.Fatal("first reset did not finish after request release")
	}
}

func TestRoutineCloseCancelsResetRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	require.NoError(t, oldSession.SetUserDefinedVar("must_not_leak", "previous-client", ""))
	oldSession.GetTxnHandler().Close()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: 5 * time.Minute,
	}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp.EXPECT().GetWorkspace().Return(workspace)
	rollbackStarted := make(chan struct{})
	txnOp.EXPECT().Rollback(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		close(rollbackStarted)
		<-ctx.Done()
		return context.Cause(ctx)
	})
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false

	routine := NewRoutine(
		context.Background(),
		oldSession.GetResponser().MysqlRrWr(),
		&config.FrontendParameters{},
	)
	t.Cleanup(routine.cancelRoutineFunc)
	routine.setSession(oldSession)

	resetDone := make(chan error, 1)
	go func() {
		resetDone <- routine.resetSession("", &query.ResetSessionResponse{})
	}()

	select {
	case <-rollbackStarted:
	case <-time.After(time.Second):
		t.Fatal("reset did not enter transaction rollback")
	}

	closeDone := make(chan struct{})
	go func() {
		routine.beginClose()
		routine.mc.waitAndClose()
		close(closeDone)
	}()

	select {
	case err := <-resetDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("connection close did not cancel reset rollback")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("connection close remained blocked after reset rollback cancellation")
	}
}

func TestMigrateConnectionFromRejectsClosedRoutineBeforeReadingSession(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	routine.mc.waitAndClose()

	err := routine.migrateConnectionFromWithContext(context.Background(), &query.MigrateConnFromResponse{})
	require.Error(t, err)
}

func TestRoutineCleanupCancelsRequestBeforeRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.GetTxnHandler().Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp.EXPECT().GetWorkspace().Return(workspace)

	routine := NewRoutine(context.Background(), ses.GetResponser().MysqlRrWr(), &config.FrontendParameters{})
	routine.setSession(ses)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	routine.setCancelRequestFunc(cancelRequest)
	canceledBeforeRollback := false
	txnOp.EXPECT().Rollback(gomock.Any()).DoAndReturn(func(context.Context) error {
		select {
		case <-requestCtx.Done():
			canceledBeforeRollback = true
		default:
		}
		return nil
	})
	ses.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	ses.txnHandler.shareTxn = false

	routine.cleanup()

	require.True(t, canceledBeforeRollback,
		"rollback must not run ahead of cancellation of the active query")
}

func TestMigrateConnectionFromPreservesLastAffectedRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetLastAffectedRows(7)
	ses.SetLastFoundRows(11)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.Equal(t, int64(7), resp.LastAffectedRows)
	require.Equal(t, uint64(11), resp.FoundRows)
	require.True(t, resp.TempTableStateExported)
}

func TestMigrateConnectionFromExportsTemporaryTablesOnlyToCapableProxy(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.AddTempTable("db.with.dot", "tmp.with.dot", "__mo_tmp_source_db_tmp")
	ses.AddTempIndexTable("db.with.dot", "hidden_idx", "__mo_tmp_source_db_hidden")
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	err := rt.migrateConnectionFromActionWithCapabilities(
		context.Background(),
		query.MigrateConnFromAction_MigrateConnFromExport,
		false,
		&query.MigrateConnFromResponse{},
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))

	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFromActionWithCapabilities(
		context.Background(),
		query.MigrateConnFromAction_MigrateConnFromExport,
		true,
		resp,
	))
	require.True(t, resp.TempTableStateExported)
	require.Equal(t, []*query.MigrateTempTable{{
		Database:     "db.with.dot",
		Alias:        "tmp.with.dot",
		PhysicalName: "__mo_tmp_source_db_tmp",
	}}, resp.TempTables)
}

func TestMigrateConnectionFromRejectsOversizedTemporaryTableSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	for i := 0; i <= maxMigrateTempTableCount; i++ {
		alias := fmt.Sprintf("tmp_%d", i)
		ses.AddTempTable("db", alias, "__mo_tmp_source_"+alias)
	}
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	err := rt.migrateConnectionFromActionWithCapabilities(
		context.Background(),
		query.MigrateConnFromAction_MigrateConnFromExport,
		true,
		resp,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	require.Empty(t, resp.TempTables)
	require.False(t, resp.TempTableStateExported)
}

func TestMigrateConnectionFromRejectsPendingPreparedLongData(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	prepared := &PrepareStmt{
		Name:                GetPrepareStmtName(41),
		getFromSendLongData: map[int]struct{}{0: {}},
	}
	require.NoError(t, ses.SetPrepareStmt(context.Background(), prepared.Name, prepared))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	err := rt.migrateConnectionFrom(&query.MigrateConnFromResponse{})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))

	prepared.resetBinaryParamState()
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.Len(t, resp.PrepareStmts, 1)
	require.Equal(t, prepared.Name, resp.PrepareStmts[0].Name)
}

func TestMigrateConnectionFromExportsEvaluatedUserVariables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	require.NoError(t, ses.setUserDefinedVarWithKindAndReplayability(
		"TS0",
		"2026-08-07 04:20:01.123456",
		"set @ts0 = (select updated_at from src limit 1)",
		false,
		vector.PrepareParamNone,
		true,
	))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.True(t, resp.UserDefinedVarsExported)
	require.Len(t, resp.UserDefinedVars, 1)
	require.Equal(t, "ts0", resp.UserDefinedVars[0].Name)
	require.Equal(t, "set @ts0 = (select updated_at from src limit 1)", resp.UserDefinedVars[0].Sql)
	require.True(t, resp.UserDefinedVarsReplayable)
}

func TestMigrateConnectionFromMarksPreparedUserStateUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	// An empty SQL field models a value produced by a prepared SET. The typed
	// snapshot is valid for v22, but the legacy raw replay stream cannot carry it.
	require.NoError(t, ses.setUserDefinedVarWithKind(
		"prepared", "value", "", false, vector.PrepareParamNone))

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.True(t, resp.UserDefinedVarsExported)
	require.False(t, resp.UserDefinedVarsReplayable)
	require.Len(t, resp.UserDefinedVars, 1)
}

func TestMigrateConnectionFromFailsClosedWhenTypedSnapshotTooLarge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	// Prepared SET assignments are applied by the frontend but are not
	// guaranteed to be captured as replayable raw COM_QUERY statements by the
	// proxy. An oversized typed snapshot must therefore fail closed.
	require.NoError(t, ses.setUserDefinedVarWithKind(
		"large",
		string(make([]byte, maxMigrateUserDefinedVarsSize)),
		"",
		false,
		vector.PrepareParamNone,
	))

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.False(t, resp.UserDefinedVarsExported)
	require.Empty(t, resp.UserDefinedVars)
	require.True(t, resp.UserDefinedVarsSnapshotTooLarge)
	require.False(t, resp.UserDefinedVarsReplayable)
	require.True(t, resp.SystemVariablesExported)
}

func TestMigrateConnectionFromMarksTypedUserSnapshotTooLargeForLegacyReplay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	// A normal COM_QUERY SET is retained by the proxy, so a pre-v22 target can
	// still use the legacy replay stream. A v22 target must reject this marker
	// instead of re-evaluating the raw expression.
	require.NoError(t, ses.setUserDefinedVarWithKindAndReplayability(
		"large",
		string(make([]byte, maxMigrateUserDefinedVarsSize)),
		"set @large = repeat('a', 16777216)",
		false,
		vector.PrepareParamNone,
		true,
	))

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.False(t, resp.UserDefinedVarsExported)
	require.Empty(t, resp.UserDefinedVars)
	require.True(t, resp.UserDefinedVarsSnapshotTooLarge)
	require.True(t, resp.UserDefinedVarsReplayable)
	require.True(t, resp.SystemVariablesExported)
}

func TestMigrateConnectionFromFailsClosedWhenTypedSystemSnapshotTooLargeAndUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	// Prepared system assignments do not produce a proxy raw SET event. Mark
	// the large value as unobserved and require migration to fail closed.
	require.NoError(t, ses.SetSessionSysVar(
		context.Background(), "optimizer_hints", string(make([]byte, maxMigrateUserDefinedVarsSize))))

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	err := rt.migrateConnectionFrom(resp)
	require.ErrorContains(t, err, "size limit")
	require.False(t, resp.UserDefinedVarsExported)
	require.False(t, resp.SystemVariablesExported)
	require.False(t, resp.SystemVariablesReplayable)
}

func TestMigrateConnectionFromMarksTypedSystemSnapshotTooLargeForLegacyReplay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	require.NoError(t, ses.SetSessionSysVar(
		context.Background(), "optimizer_hints", string(make([]byte, maxMigrateUserDefinedVarsSize))))
	// A raw-replayable assignment can be handed to a pre-v22 target, but a v22
	// target must still fail closed because the typed system snapshot is absent.
	ses.markMigrationSystemVarReplayable("optimizer_hints", true)

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.True(t, resp.UserDefinedVarsExported)
	require.Empty(t, resp.UserDefinedVars)
	require.False(t, resp.SystemVariablesExported)
	require.True(t, resp.SystemVariablesSnapshotTooLarge)
	require.True(t, resp.SystemVariablesReplayable)
}

func TestClearPrivilegeCacheRefreshesActiveRoleGrant(t *testing.T) {
	for _, tc := range []struct {
		name       string
		catalogErr error
	}{
		{name: "revoked membership is cached"},
		{name: "catalog errors fail closed", catalogErr: fmt.Errorf("role grant catalog unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ses := newTestSession(t, ctrl)
			ses.SetTenantInfo(&TenantInfo{
				Tenant:        "test_account",
				User:          "reader_user",
				DefaultRole:   "reader_role",
				TenantID:      1,
				UserID:        2,
				DefaultRoleID: 3,
			})
			ctx := defines.AttachAccountId(context.Background(), 1)
			require.NoError(t, ses.SetSessionSysVar(ctx, "enable_privilege_cache", int8(1)))
			ses.GetPrivilegeCache().setActiveRoleGrant(2, 3, true)

			bh := &backgroundExecTest{}
			bh.init()
			roleGrantSQL := getSqlForCheckUserGrantForAuthorization(3, 2)
			if tc.catalogErr != nil {
				bh.sql2err[roleGrantSQL] = tc.catalogErr
			} else {
				bh.sql2result[roleGrantSQL] = newMrsForCheckUserGrant(nil)
			}
			var forcedPessimisticRC bool
			stub := gostub.Stub(&NewBackgroundExec, func(
				_ context.Context,
				_ FeSession,
				opts ...*BackgroundExecOption,
			) BackgroundExec {
				forcedPessimisticRC = len(opts) == 1 && opts[0] != nil && opts[0].forcePessimisticRC
				return bh
			})
			defer stub.Reset()

			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set session clear_privilege_cache = on", 1)
			require.NoError(t, err)
			err = doSetVar(ses, newTestExecCtx(ctx, ctrl), stmt.(*tree.SetVar), "", false)
			require.True(t, forcedPessimisticRC)
			require.Contains(t, bh.executedSQLs, roleGrantSQL)
			if tc.catalogErr != nil {
				require.ErrorIs(t, err, tc.catalogErr)
				_, cached := ses.GetPrivilegeCache().getActiveRoleGrant(2, 3)
				require.False(t, cached)
				return
			}
			require.NoError(t, err)
			valid, cached := ses.GetPrivilegeCache().getActiveRoleGrant(2, 3)
			require.True(t, cached)
			require.False(t, valid)
		})
	}
}

func TestEnablePrivilegeCacheInvalidatesDisabledModeRoleGrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        "test_account",
		User:          "reader_user",
		DefaultRole:   "reader_role",
		TenantID:      1,
		UserID:        2,
		DefaultRoleID: 3,
	})
	ctx := defines.AttachAccountId(context.Background(), 1)
	require.NoError(t, ses.SetSessionSysVar(ctx, "enable_privilege_cache", int8(0)))
	// Model the stale entry produced by the old OFF -> SET ROLE path.
	ses.GetPrivilegeCache().setActiveRoleGrant(2, 3, true)

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set session enable_privilege_cache = on", 1)
	require.NoError(t, err)
	require.NoError(t, doSetVar(ses, newTestExecCtx(ctx, ctrl), stmt.(*tree.SetVar), "", false))
	_, cached := ses.GetPrivilegeCache().getActiveRoleGrant(2, 3)
	require.False(t, cached)

	bh := &backgroundExecTest{}
	bh.init()
	roleGrantSQL := getSqlForCheckUserGrantForAuthorization(3, 2)
	bh.sql2result[roleGrantSQL] = newMrsForCheckUserGrant(nil)
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	valid, _, err := validateActiveRoleGrantForAuthorization(ctx, ses)
	require.NoError(t, err)
	require.False(t, valid)
	require.Contains(t, bh.executedSQLs, roleGrantSQL)
}

func TestCancelledNextTransactionIsolationRemainsReplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)

	nextSQL := "set transaction isolation level read committed"
	nextStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, nextSQL, 1)
	require.NoError(t, err)
	nextExecCtx := newTestExecCtx(ctx, ctrl)
	nextExecCtx.ses = ses
	nextExecCtx.singleStatementQuery = true
	nextExecCtx.sqlOfStmt = nextSQL
	require.NoError(t, handleSetTransaction(ses, nextExecCtx, nextStmt.(*tree.SetTransaction)))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())

	cancelSQL := "set @@session.transaction_isolation = 'REPEATABLE-READ'"
	cancelStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, cancelSQL, 1)
	require.NoError(t, err)
	cancelExecCtx := newTestExecCtx(ctx, ctrl)
	cancelExecCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(
		ses, cancelExecCtx, cancelStmt.(*tree.SetVar), cancelSQL, false))
	require.False(t, ses.hasUnreplayableMigrationSystemVars())

	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.True(t, resp.SystemVariablesReplayable)
}

func TestPreparedSystemAssignmentIsMarkedUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set optimizer_hints = prepared_hint", 1)
	require.NoError(t, err)
	require.NoError(t, doSetVar(
		ses, newTestExecCtx(ctx, ctrl), stmt.(*tree.SetVar), "", true))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestCapturedSystemAssignmentAfterPreparedWriteRemainsUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)

	preparedSQL := "set session sql_mode = 'ANSI_QUOTES'"
	preparedStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, preparedSQL, 1)
	require.NoError(t, err)
	require.NoError(t, doSetVar(
		ses, newTestExecCtx(ctx, ctrl), preparedStmt.(*tree.SetVar), "", true))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())

	rawSQL := "set session sql_mode = @@sql_mode"
	rawStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rawSQL, 1)
	require.NoError(t, err)
	rawExecCtx := newTestExecCtx(ctx, ctrl)
	rawExecCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(
		ses, rawExecCtx, rawStmt.(*tree.SetVar), rawSQL, false))
	// A later captured assignment cannot prove that an earlier prepared
	// assignment was replayable; retain the conservative migration marker.
	require.True(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestCapturedUserAssignmentAfterPreparedWriteRemainsUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)

	preparedSQL := "set @x = 1"
	preparedStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, preparedSQL, 1)
	require.NoError(t, err)
	require.NoError(t, doSetVar(
		ses, newTestExecCtx(ctx, ctrl), preparedStmt.(*tree.SetVar), "", true))
	require.True(t, ses.hasUnreplayableMigrationUserVars())

	rawSQL := "set @x = @x + 1"
	rawStmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rawSQL, 1)
	require.NoError(t, err)
	rawExecCtx := newTestExecCtx(ctx, ctrl)
	rawExecCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(
		ses, rawExecCtx, rawStmt.(*tree.SetVar), rawSQL, false))
	// A later captured write cannot prove that an earlier prepared write was
	// replayable; retain the conservative migration marker for the variable.
	require.True(t, ses.hasUnreplayableMigrationUserVars())
}

func TestPreparedGlobalRuntimeAssignmentIsMarkedUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getSqlForGetSysVarWithAccount(sysAccountID, "optimizer_hints")] =
		newMrsForSystemVariableNameOfAccount([][]interface{}{})
	bh.sql2result[getSqlForInsertSysVarWithAccount(
		sysAccountID, sysAccountName, "optimizer_hints", "prepared_hint")] = nil
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()
	previousExeSqlInBgSes := ExeSqlInBgSes
	ExeSqlInBgSes = func(context.Context, BackgroundExec, string) ([]ExecResult, error) {
		return nil, nil
	}
	t.Cleanup(func() { ExeSqlInBgSes = previousExeSqlInBgSes })
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set global optimizer_hints = prepared_hint", 1)
	require.NoError(t, err)
	require.NoError(t, doSetVar(
		ses, newTestExecCtx(ctx, ctrl), stmt.(*tree.SetVar), "", true))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestMultiStatementGlobalRuntimeAssignmentIsMarkedUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getSqlForGetSysVarWithAccount(sysAccountID, "runtime_filter_limit_in")] =
		newMrsForSystemVariableNameOfAccount([][]interface{}{})
	bh.sql2result[getSqlForInsertSysVarWithAccount(
		sysAccountID, sysAccountName, "runtime_filter_limit_in", "42")] = nil
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()
	previousExeSqlInBgSes := ExeSqlInBgSes
	ExeSqlInBgSes = func(context.Context, BackgroundExec, string) ([]ExecResult, error) {
		return nil, nil
	}
	t.Cleanup(func() { ExeSqlInBgSes = previousExeSqlInBgSes })
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set global runtime_filter_limit_in = 42", 1)
	require.NoError(t, err)
	execCtx := newTestExecCtx(ctx, ctrl)
	// The frontend executes this as part of a multi-statement COM_QUERY, so
	// the proxy does not retain the raw SET for a legacy migration hop.
	execCtx.singleStatementQuery = false
	require.NoError(t, doSetVar(
		ses, execCtx, stmt.(*tree.SetVar), "set global runtime_filter_limit_in = 42", false))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestRawGlobalScopeBothAssignmentIsMarkedUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getSqlForGetSysVarWithAccount(sysAccountID, "autocommit")] =
		newMrsForSystemVariableNameOfAccount([][]interface{}{})
	bh.sql2result[getSqlForInsertSysVarWithAccount(
		sysAccountID, sysAccountName, "autocommit", "0")] = nil
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()
	previousExeSqlInBgSes := ExeSqlInBgSes
	ExeSqlInBgSes = func(context.Context, BackgroundExec, string) ([]ExecResult, error) {
		return nil, nil
	}
	t.Cleanup(func() { ExeSqlInBgSes = previousExeSqlInBgSes })

	sql := "set global autocommit = 0"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(
		ses, execCtx, stmt.(*tree.SetVar), sql, false))
	require.True(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestRawNextTransactionAssignmentIsReplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	sql := "set @@transaction_isolation = 'READ-COMMITTED'"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(
		ses, execCtx, stmt.(*tree.SetVar), sql, false))
	require.False(t, ses.hasUnreplayableMigrationSystemVars())
}

func TestMultiStatementSetAssignmentIsUnreplayable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	sql := "set @large = repeat('a', 16777216)"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	execCtx := newTestExecCtx(ctx, ctrl)
	// The frontend executes this statement as part of a multi-statement
	// COM_QUERY. The proxy does not capture raw replay for that request.
	require.NoError(t, doSetVar(
		ses, execCtx, stmt.(*tree.SetVar), sql, false))
	require.True(t, ses.hasUnreplayableMigrationUserVars())
}

func TestMigrateConnectionFromV20KeepsLegacyUserVariableReplay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	serviceRuntime := moruntime.ServiceRuntime(ses.proc.GetService())
	oldVersion, hadVersion := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
	t.Cleanup(func() {
		if hadVersion {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	require.NoError(t, ses.SetUserDefinedVar("ts0", "stable-value", "set @ts0 = now()"))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.False(t, resp.UserDefinedVarsExported)
	require.Empty(t, resp.UserDefinedVars)
}

func TestRoutineResetSessionKeepsReplacementRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	timeZone := time.FixedZone("reset-session-test", 8*60*60)
	oldSession.SetTimeZone(timeZone)
	require.NoError(t, oldSession.SetUserDefinedVar("must_not_leak", int64(1), "set @must_not_leak = 1"))
	leakedPrepared := &PrepareStmt{
		Name: "must_not_leak",
		cursor: &preparedStmtCursor{
			owner: oldSession,
			bytes: 1,
		},
	}
	oldSession.preparedCursorBytes.Store(1)
	require.NoError(t, oldSession.SetPrepareStmt(
		context.Background(), "must_not_leak", leakedPrepared,
	))
	require.NoError(t, oldSession.SetSessionSysVar(context.Background(), "sql_mode", "ANSI"))
	connectionID := oldSession.GetConnectionID()
	oldSession.GetTxnHandler().Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp.EXPECT().GetWorkspace().Return(workspace)
	txnOp.EXPECT().Rollback(gomock.Any()).Return(nil)
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), oldSession.GetResponser().MysqlRrWr(), parameters)
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	protocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
	oldSession.SetDatabaseName("db1")
	conn := protocol.GetTcpConnection()
	rm.setRoutine(conn, protocol.ConnectionID(), routine)
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)

	require.NoError(t, rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)}))

	newSession := routine.getSession()
	t.Cleanup(func() {
		rm.deleteRoutine(conn)
		rm.sessionManager.RemoveSession(newSession)
		newSession.Close()
		rm.cancelCtx()
	})

	require.NotSame(t, oldSession, newSession)
	require.Equal(t, oldSession.GetUUIDString(), newSession.GetUUIDString())
	require.Equal(t, connectionID, newSession.GetConnectionID())
	require.Equal(t, "db1", newSession.GetDatabaseName(), "COM_RESET_CONNECTION preserves the selected database")
	_, err = newSession.GetUserDefinedVar("must_not_leak")
	require.ErrorContains(t, err, "does not exist")
	_, err = newSession.GetPrepareStmt(context.Background(), "must_not_leak")
	require.Error(t, err)
	require.Nil(t, leakedPrepared.cursor)
	require.Zero(t, oldSession.preparedCursorBytes.Load())
	newSQLMode, err := newSession.GetSessionSysVar("sql_mode")
	require.NoError(t, err)
	require.NotEqual(t, "ANSI", newSQLMode)
	require.NotEqual(t, timeZone.String(), newSession.GetTimeZone().String())

	registered := rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1, "successful reset must keep the replacement session registered")
	require.Same(t, newSession, registered[0])

	firstReplacement := newSession
	require.NoError(t, rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)}))
	newSession = routine.getSession()
	require.NotSame(t, firstReplacement, newSession)
	require.Equal(t, connectionID, newSession.GetConnectionID())
	registered = rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1, "repeated reset must replace rather than accumulate sessions")
	require.Same(t, newSession, registered[0])
}

func TestRoutineResetSessionFailureRestoresProtocolState(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	protocol := oldSession.GetResponser().MysqlRrWr()
	protocol.SetStr(DBNAME, "db1")
	routine := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	t.Cleanup(func() {
		if current := routine.getSession(); current != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		rm.cancelCtx()
	})
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)

	oldSession.GetTxnHandler().Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp.EXPECT().GetWorkspace().Return(workspace)
	txnOp.EXPECT().Rollback(gomock.Any()).Return(assert.AnError)
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false

	err = routine.resetSession("", &query.ResetSessionResponse{})
	require.ErrorIs(t, err, assert.AnError)
	require.ErrorIs(t, err, errSessionResetConnectionMustClose)
	require.Same(t, oldSession, routine.getSession())
	require.Equal(t, "db1", protocol.GetStr(DBNAME))
	registered := rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1)
	require.Same(t, oldSession, registered[0])

	// rollbackUnsafe has invalidated the transaction generation, so the handler
	// must retire this connection instead of attempting another reset on it.
}

func TestRoutineHandleSessionCommandRejectsResetPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := newTestSession(t, ctrl)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), session.GetResponser().MysqlRrWr(), parameters)
	routine.setSession(session)
	t.Cleanup(func() {
		session.Close()
		routine.cancelRoutineFunc()
	})

	err := routine.handleSessionCommand(context.Background(), &Request{
		cmd:  COM_RESET_CONNECTION,
		data: []byte{1},
	})
	require.NoError(t, err, "malformed reset must return an ERR packet")
	require.Same(t, session, routine.getSession(), "malformed reset must not replace the session")
}

func TestRoutineHandleSessionCommandRejectsInvalidStateAndPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := newTestSession(t, ctrl)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), session.GetResponser().MysqlRrWr(), parameters)
	t.Cleanup(func() {
		session.Close()
		routine.cancelRoutineFunc()
	})

	err := routine.handleSessionCommand(context.Background(), &Request{cmd: COM_RESET_CONNECTION})
	require.ErrorContains(t, err, "cannot reset a missing session")

	routine.setSession(session)
	err = routine.handleSessionCommand(context.Background(), &Request{cmd: COM_QUERY})
	require.ErrorContains(t, err, "unsupported session command")

	err = routine.handleSessionCommand(context.Background(), &Request{
		cmd:  COM_CHANGE_USER,
		data: "not a change-user packet",
	})
	require.NoError(t, err, "malformed change-user payload must return an ERR packet")
	require.Same(t, session, routine.getSession(), "malformed change-user payload must not replace the session")
}

type disconnectRecordingProtocol struct {
	MysqlRrWr
	disconnects int
}

func (p *disconnectRecordingProtocol) Disconnect() error {
	p.disconnects++
	return p.MysqlRrWr.Disconnect()
}

func TestRoutineHandleSessionCommandClosesPartialTempTableReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	mysqlProtocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
	protocol := &disconnectRecordingProtocol{MysqlRrWr: mysqlProtocol}
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), protocol, parameters)
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	conn := mysqlProtocol.GetTcpConnection()
	rm.setRoutine(conn, mysqlProtocol.ConnectionID(), routine)

	oldSession.GetTxnHandler().Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp.EXPECT().GetWorkspace().Return(workspace)
	txnOp.EXPECT().Rollback(gomock.Any()).Return(nil)
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false
	oldSession.AddTempTable("db", "first", "first_physical")
	oldSession.AddTempTable("db", "second", "second_physical")

	serviceRuntime := moruntime.ServiceRuntime(oldSession.GetService())
	previousExecutor, hadExecutor := serviceRuntime.GetGlobalVariables(moruntime.InternalSQLExecutor)
	exec := &resetTempTableExecutor{failAt: 2}
	serviceRuntime.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		if hadExecutor {
			serviceRuntime.SetGlobalVariables(moruntime.InternalSQLExecutor, previousExecutor)
		} else {
			serviceRuntime.SetGlobalVariables(moruntime.InternalSQLExecutor, nil)
		}
	})
	t.Cleanup(func() {
		rm.deleteRoutine(conn)
		rm.sessionManager.RemoveSession(oldSession)
		assert.NoError(t, oldSession.resetTempTables(context.Background()))
		oldSession.Close()
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})

	require.NoError(t, rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)}))
	require.Len(t, exec.sql, 2, "the injected failure must follow one successful physical DROP")
	require.Equal(t, 1, protocol.disconnects, "a partially reset connection must not be reusable")
	require.Same(t, oldSession, routine.getSession(), "the replacement generation must remain unpublished")
}

func TestRoutineHandleSessionCommandClosesRollbackFailedReset(t *testing.T) {
	tests := []struct {
		name                 string
		rollback             func(context.Context) error
		cancelDuringRollback bool
	}{
		{
			name:     "storage error",
			rollback: func(context.Context) error { return assert.AnError },
		},
		{
			name:                 "canceled during rollback",
			cancelDuringRollback: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			oldSession := newTestSession(t, ctrl)
			rm, err := NewRoutineManager(context.Background(), "")
			require.NoError(t, err)
			rm.sessionManager = queryservice.NewSessionManager()

			mysqlProtocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
			protocol := &disconnectRecordingProtocol{MysqlRrWr: mysqlProtocol}
			parameters := &config.FrontendParameters{}
			parameters.SetDefaultValues()
			routine := NewRoutine(context.Background(), protocol, parameters)
			oldSession.setRoutineManager(rm)
			oldSession.setRoutine(routine)
			routine.setSession(oldSession)
			rm.sessionManager.AddSession(oldSession)
			conn := mysqlProtocol.GetTcpConnection()
			rm.setRoutine(conn, mysqlProtocol.ConnectionID(), routine)
			t.Cleanup(func() {
				rm.deleteRoutine(conn)
				rm.sessionManager.RemoveSession(oldSession)
				oldSession.Close()
				routine.cancelRoutineFunc()
				rm.cancelCtx()
			})

			oldSession.GetTxnHandler().Close()
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
			workspace := mock_frontend.NewMockWorkspace(ctrl)
			workspace.EXPECT().GetHaveDDL().Return(false)
			txnOp.EXPECT().GetWorkspace().Return(workspace)
			rollbackStarted := make(chan struct{})
			rollback := tc.rollback
			if tc.cancelDuringRollback {
				rollback = func(ctx context.Context) error {
					close(rollbackStarted)
					<-ctx.Done()
					return context.Cause(ctx)
				}
			}
			txnOp.EXPECT().Rollback(gomock.Any()).DoAndReturn(rollback)
			oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
			oldSession.txnHandler.shareTxn = false

			if tc.cancelDuringRollback {
				handlerDone := make(chan error, 1)
				go func() {
					handlerDone <- rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)})
				}()
				handlerFinished := false
				defer func() {
					if handlerFinished {
						return
					}
					routine.cancelRoutineFunc()
					select {
					case <-handlerDone:
					case <-time.After(time.Second):
					}
				}()
				select {
				case <-rollbackStarted:
				case <-time.After(time.Second):
					t.Fatal("session reset did not enter transaction rollback")
				}
				routine.cancelRoutineFunc()
				select {
				case handlerErr := <-handlerDone:
					handlerFinished = true
					require.NoError(t, handlerErr)
				case <-time.After(time.Second):
					t.Fatal("session reset did not finish after rollback cancellation")
				}
			} else {
				require.NoError(t, rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)}))
			}
			require.Equal(t, 1, protocol.disconnects, "a reset that invalidated its transaction generation must close the physical connection")
			require.Same(t, oldSession, routine.getSession(), "a failed reset must not publish a replacement generation")
		})
	}
}

func TestRoutineHandleSessionCommandKeepsConnectionWhenDeadlineExpiresBeforeResetMutation(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	mysqlProtocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
	protocol := &disconnectRecordingProtocol{MysqlRrWr: mysqlProtocol}
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	parameters.SessionTimeout.Duration = 0
	routine := NewRoutine(context.Background(), protocol, parameters)
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	conn := mysqlProtocol.GetTcpConnection()
	rm.setRoutine(conn, mysqlProtocol.ConnectionID(), routine)
	t.Cleanup(func() {
		rm.deleteRoutine(conn)
		rm.sessionManager.RemoveSession(oldSession)
		oldSession.Close()
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})

	oldSession.GetTxnHandler().Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false

	require.NoError(t, rm.Handler(conn, []byte{byte(COM_RESET_CONNECTION)}))
	require.NoError(t, context.Cause(routine.getCancelRoutineCtx()), "the command deadline must not cancel the physical connection")
	require.Zero(t, protocol.disconnects, "a reset whose deadline expires before mutation must keep the physical connection reusable")
	require.Same(t, oldSession, routine.getSession(), "a reset whose deadline expires before mutation must keep the old session generation")
}

func mysqlNativePasswordResponse(password, salt []byte) []byte {
	hash1 := HashSha1(password)
	hash2 := HashSha1(hash1)
	digestInput := append(append([]byte(nil), salt...), hash2...)
	digest := HashSha1(digestInput)
	for i := range digest {
		digest[i] ^= hash1[i]
	}
	return digest
}

func changeUserPacket(username string, authResponse []byte, database string) []byte {
	payload := append([]byte(username), 0)
	payload = append(payload, byte(len(authResponse)))
	payload = append(payload, authResponse...)
	payload = append(payload, []byte(database)...)
	payload = append(payload, 0, byte(Utf8mb4CollationID), 0)
	payload = append(payload, []byte(AuthNativePassword)...)
	payload = append(payload, 0)
	return payload
}

func writeWirePacket(t *testing.T, conn net.Conn, sequence byte, payload []byte) {
	t.Helper()
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}
	for _, data := range [][]byte{header, payload} {
		for len(data) > 0 {
			n, err := conn.Write(data)
			require.NoError(t, err)
			data = data[n:]
		}
	}
}

func readWirePacket(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	header := make([]byte, HeaderLengthOfTheProtocol)
	_, err := io.ReadFull(conn, header)
	require.NoError(t, err)
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	require.NoError(t, err)
	return header[3], payload
}

func wireOKPacketStatus(t *testing.T, payload []byte) uint16 {
	t.Helper()
	require.GreaterOrEqual(t, len(payload), 5)
	require.Equal(t, byte(defines.OKHeader), payload[0])
	// The focused exchange carries zero affected rows and zero last-insert ID,
	// both encoded as one byte. The following two bytes are the server status.
	return binary.LittleEndian.Uint16(payload[3:5])
}

func wireHandshakeResponse(username string) []byte {
	return wireHandshakeResponseWithDatabase(username, "")
}

func wireHandshakeResponseWithDatabase(username, database string) []byte {
	capability := uint32(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH)
	if database != "" {
		capability |= CLIENT_CONNECT_WITH_DB
	}
	payload := make([]byte, 4+4+1+23)
	binary.LittleEndian.PutUint32(payload, capability)
	binary.LittleEndian.PutUint32(payload[4:], 1<<24)
	payload[8] = byte(Utf8mb4CollationID)
	payload = append(payload, username...)
	payload = append(payload, 0, 0) // username terminator and empty auth response
	if database != "" {
		payload = append(payload, database...)
		payload = append(payload, 0)
	}
	payload = append(payload, AuthNativePassword...)
	return append(payload, 0)
}

func wireChangeUserRequest(username, plugin string) []byte {
	payload := append([]byte{byte(COM_CHANGE_USER)}, username...)
	payload = append(payload, 0, 0) // username terminator and empty secure auth response
	payload = append(payload, 0)    // empty database
	payload = append(payload, byte(Utf8mb4CollationID), 0)
	payload = append(payload, plugin...)
	return append(payload, 0)
}

func TestMySQLWireChangeUserAuthSwitchAndRepeatedBorrow(t *testing.T) {
	previousServerVars, ok := serverVarsMap.Load("")
	require.True(t, ok)
	serverVarsMap.Store("", &ServerLevelVariables{})
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	parameters.SkipCheckUser = true
	parameters.KillRountinesInterval = 0
	setPu("", config.NewParameterUnit(parameters, nil, nil, nil))
	setSessionAlloc("", NewLeakCheckAllocator())
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	setRtMgr("", rm)

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		rm.cancelCtx()
		serverVarsMap.Store("", previousServerVars)
	})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		startInnerServer(serverConn)
	}()

	_, handshake := readWirePacket(t, clientConn)
	versionEnd := 1
	for versionEnd < len(handshake) && handshake[versionEnd] != 0 {
		versionEnd++
	}
	require.Greater(t, len(handshake), versionEnd+4)
	connectionID := binary.LittleEndian.Uint32(handshake[versionEnd+1:])
	writeWirePacket(t, clientConn, 1, wireHandshakeResponse("dump"))
	_, response := readWirePacket(t, clientConn)
	require.NotEmpty(t, response)
	require.Equal(t, byte(defines.OKHeader), response[0])

	// Connector/J 8.0.15 falls back to COM_CHANGE_USER and has to answer the
	// server's mysql_native_password authentication-switch packet. Exercise the
	// real packet loop twice to prove a physical connection can be borrowed again.
	for borrow := 0; borrow < 2; borrow++ {
		writeWirePacket(t, clientConn, 0, wireChangeUserRequest("dump", "caching_sha2_password"))
		_, switchRequest := readWirePacket(t, clientConn)
		require.NotEmpty(t, switchRequest)
		require.Equal(t, byte(defines.EOFHeader), switchRequest[0])
		require.Contains(t, string(switchRequest), AuthNativePassword)
		writeWirePacket(t, clientConn, 2, nil)
		_, response = readWirePacket(t, clientConn)
		require.NotEmpty(t, response)
		require.Equal(t, byte(defines.OKHeader), response[0])
		require.NotZero(t, wireOKPacketStatus(t, response)&SERVER_STATUS_AUTOCOMMIT,
			"COM_CHANGE_USER must publish a clean autocommit status to the pooled client")

		routine := rm.getRoutineByConnID(connectionID)
		require.NotNil(t, routine)
		require.Equal(t, connectionID, routine.getConnectionID())
		require.Equal(t, "dump", routine.getSession().GetTenantInfo().GetUser())
	}

	require.NoError(t, clientConn.Close())
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("MySQL wire server did not stop after the client closed")
	}
}

func TestMySQLWireResetConnectionPreservesDatabase(t *testing.T) {
	previousServerVars, ok := serverVarsMap.Load("")
	require.True(t, ok)
	serverVarsMap.Store("", &ServerLevelVariables{})
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	parameters.SkipCheckUser = true
	parameters.KillRountinesInterval = 0
	setPu("", config.NewParameterUnit(parameters, nil, nil, nil))
	setSessionAlloc("", NewLeakCheckAllocator())
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	setRtMgr("", rm)

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		rm.cancelCtx()
		serverVarsMap.Store("", previousServerVars)
	})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		startInnerServer(serverConn)
	}()

	_, handshake := readWirePacket(t, clientConn)
	versionEnd := 1
	for versionEnd < len(handshake) && handshake[versionEnd] != 0 {
		versionEnd++
	}
	require.Greater(t, len(handshake), versionEnd+4)
	connectionID := binary.LittleEndian.Uint32(handshake[versionEnd+1:])
	writeWirePacket(t, clientConn, 1, wireHandshakeResponseWithDatabase("dump", "db1"))
	_, response := readWirePacket(t, clientConn)
	require.NotEmpty(t, response)
	require.Equal(t, byte(defines.OKHeader), response[0])
	// The in-process wire fixture does not install the catalog-backed global
	// system-variable snapshot that a real authenticated session already has.
	// Supply the empty snapshot explicitly so reset does not create a background
	// transaction merely to populate it.
	routine := rm.getRoutineByConnID(connectionID)
	require.NotNil(t, routine)
	routine.getSession().gSysVars = &SystemVariables{mp: map[string]interface{}{}}

	// Connector/J 8.4 and later use COM_RESET_CONNECTION before exposing each
	// logical pooled borrow. A successful reset must retain the URL-selected DB.
	for borrow := 0; borrow < 2; borrow++ {
		writeWirePacket(t, clientConn, 0, []byte{byte(COM_RESET_CONNECTION)})
		_, response = readWirePacket(t, clientConn)
		require.NotEmpty(t, response)
		require.Equal(t, byte(defines.OKHeader), response[0])
		require.NotZero(t, wireOKPacketStatus(t, response)&SERVER_STATUS_AUTOCOMMIT)

		routine := rm.getRoutineByConnID(connectionID)
		require.NotNil(t, routine)
		require.Equal(t, connectionID, routine.getConnectionID())
		require.Equal(t, "db1", routine.getSession().GetDatabaseName())
	}

	require.NoError(t, clientConn.Close())
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("MySQL wire server did not stop after the client closed")
	}
}

func TestRoutineChangeUserAuthenticatesBeforeReplacingSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).
		Times(2).
		Return(timestamp.Timestamp{PhysicalTime: math.MaxInt64}, nil)
	pu := getPu("")
	oldTxnClient, oldStorageEngine := pu.TxnClient, pu.StorageEngine
	t.Cleanup(func() {
		pu.TxnClient = oldTxnClient
		pu.StorageEngine = oldStorageEngine
	})
	pu.TxnClient = txnClient
	pu.StorageEngine = &authenticationBarrierEngine{acquire: func(context.Context) (
		timestamp.Timestamp, error,
	) {
		return timestamp.Timestamp{PhysicalTime: math.MaxInt64}, nil
	}}
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	protocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
	protocol.SetCapability(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH)
	protocol.SetUserName(rootName)
	protocol.SetDatabaseName("old_db")
	salt := []byte("01234567890123456789")
	protocol.SetSalt(salt)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), protocol, parameters)
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	connectionID := routine.getConnectionID()

	const user = "change_user_auth_test"
	password := []byte("secret")
	SetSpecialUser(user, password)
	t.Cleanup(func() {
		specialUsers.Lock()
		delete(specialUsers.users, user)
		specialUsers.Unlock()
		if current := routine.getSession(); current != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})
	stubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
	defer stubs.Reset()

	authResponse := mysqlNativePasswordResponse(password, salt)
	require.NoError(t, routine.changeUserWithContext(
		context.Background(), changeUserPacket(user, authResponse, ""),
	))
	newSession := routine.getSession()
	require.NotSame(t, oldSession, newSession)
	require.Equal(t, user, protocol.GetUserName())
	require.Equal(t, connectionID, routine.getConnectionID())
	require.Equal(t, user, newSession.GetTenantInfo().GetUser())
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)

	// A bad password must not publish the speculative generation or change the
	// authenticated protocol identity. The wire handler sends ERR and closes the
	// connection for this error; the core remains directly testable here.
	err = routine.changeUserWithContext(
		context.Background(), changeUserPacket(user, make([]byte, 20), ""),
	)
	require.ErrorContains(t, err, "check password failed")
	require.Same(t, newSession, routine.getSession())
	require.Equal(t, user, protocol.GetUserName())
	require.Equal(t, connectionID, routine.getConnectionID())
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)
}

func TestRoutineRefreshSessionAuthReauthenticatesCandidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).
		Times(2).
		Return(timestamp.Timestamp{PhysicalTime: math.MaxInt64}, nil)
	pu := getPu("")
	oldTxnClient, oldStorageEngine := pu.TxnClient, pu.StorageEngine
	t.Cleanup(func() {
		pu.TxnClient = oldTxnClient
		pu.StorageEngine = oldStorageEngine
	})
	pu.TxnClient = txnClient
	pu.StorageEngine = &authenticationBarrierEngine{acquire: func(context.Context) (
		timestamp.Timestamp, error,
	) {
		return timestamp.Timestamp{PhysicalTime: math.MaxInt64}, nil
	}}
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()

	protocol := oldSession.GetResponser().MysqlRrWr().(*MysqlProtocolImpl)
	protocol.SetCapability(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH)
	protocol.SetUserName(rootName)
	protocol.SetDatabaseName("old_db")
	oldSalt := []byte("01234567890123456789")
	protocol.SetSalt(oldSalt)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	routine := NewRoutine(context.Background(), protocol, parameters)
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	connectionID := routine.getConnectionID()
	rm.setRoutine(&Conn{id: uint64(connectionID)}, connectionID, routine)

	const user = "refresh_auth_test"
	password := []byte("secret")
	SetSpecialUser(user, password)
	t.Cleanup(func() {
		specialUsers.Lock()
		delete(specialUsers.users, user)
		specialUsers.Unlock()
		if current := routine.getSession(); current != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})
	stubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
	defer stubs.Reset()

	newSalt := []byte("abcdefghijabcdefghij")
	authResponse := mysqlNativePasswordResponse(password, newSalt)
	validReq := &query.RefreshSessionAuthRequest{
		ConnID:        connectionID,
		UserInput:     user,
		Database:      "new_db",
		AuthResponse:  authResponse,
		Salt:          newSalt,
		ClientAddress: "127.0.0.1:3306",
	}
	busyResp := &query.RefreshSessionAuthResponse{}
	require.True(t, routine.mc.beginOperation())
	err = routine.refreshSessionAuthWithContext(context.Background(), validReq, busyResp)
	routine.mc.endOperation()
	require.ErrorContains(t, err, "cannot refresh session authentication as routine is closed or busy")
	require.False(t, busyResp.Success)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, routine.mc.beginOperation())
	err = routine.refreshSessionAuthWithContext(canceledCtx, validReq, &query.RefreshSessionAuthResponse{})
	routine.mc.endOperation()
	require.ErrorIs(t, err, context.Canceled)

	require.ErrorContains(t, routine.refreshSessionAuthWithContext(context.Background(), validReq, nil),
		"refresh session authentication response is nil")
	require.ErrorContains(t, routine.refreshSessionAuthWithContext(
		context.Background(), &query.RefreshSessionAuthRequest{}, &query.RefreshSessionAuthResponse{}),
		"refresh session authentication requires a user")
	require.ErrorContains(t, routine.refreshSessionAuthWithContext(
		context.Background(), &query.RefreshSessionAuthRequest{UserInput: user}, &query.RefreshSessionAuthResponse{}),
		"refresh session authentication requires a salt")

	resp := &query.RefreshSessionAuthResponse{}
	require.NoError(t, rm.RefreshSessionAuthWithContext(
		context.Background(),
		validReq,
		resp,
	))
	refreshed := routine.getSession()
	require.NotSame(t, oldSession, refreshed)
	require.True(t, resp.Success)
	require.NotEmpty(t, resp.AuthString)
	require.Equal(t, user, protocol.GetUserName())
	require.Equal(t, "new_db", refreshed.GetDatabaseName())
	require.Equal(t, newSalt, protocol.GetSalt())
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)
	require.Nil(t, oldSession.GetProc())
	require.Nil(t, oldSession.GetTxnHandler())

	badResp := &query.RefreshSessionAuthResponse{}
	err = rm.RefreshSessionAuthWithContext(
		context.Background(),
		&query.RefreshSessionAuthRequest{
			ConnID:       connectionID,
			UserInput:    user,
			AuthResponse: make([]byte, 20),
			Salt:         []byte("bad-salt-000000000000"),
		},
		badResp,
	)
	require.ErrorContains(t, err, "check password failed")
	require.False(t, badResp.Success)
	require.True(t, badResp.AuthenticationFailed)
	require.Same(t, refreshed, routine.getSession())
	require.Equal(t, user, protocol.GetUserName())
	require.Equal(t, newSalt, protocol.GetSalt())
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)
}

func TestRoutineManagerRefreshSessionAuthRejectsInvalidTarget(t *testing.T) {
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	service := newRoutineManagerTestService(t)
	pu := config.NewParameterUnit(parameters, nil, nil, nil)
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	rm, err := NewRoutineManager(ctx, service)
	require.NoError(t, err)
	t.Cleanup(rm.cancelCtx)

	require.ErrorContains(t, rm.RefreshSessionAuthWithContext(
		context.Background(), nil, &query.RefreshSessionAuthResponse{}),
		"invalid refresh session authentication request")
	require.ErrorContains(t, rm.RefreshSessionAuthWithContext(
		context.Background(), &query.RefreshSessionAuthRequest{}, nil),
		"invalid refresh session authentication request")
	require.ErrorContains(t, rm.RefreshSessionAuthWithContext(
		context.Background(), &query.RefreshSessionAuthRequest{ConnID: 1},
		&query.RefreshSessionAuthResponse{}),
		"cannot get routine to refresh session authentication 1")
}

func TestRoutineResetSessionRejectsLifecycleConflict(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	resp := &query.ResetSessionResponse{}

	require.True(t, routine.mc.beginOperation())
	require.Error(t, routine.resetSession("", resp))
	routine.mc.endOperation()

	routine.mc.waitAndClose()
	require.Error(t, routine.resetSession("", resp))
}

func TestRoutineManagerClosedRemovesResetReplacement(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()
	t.Cleanup(func() {
		oldSession.Close()
		rm.cancel()
	})

	routine := NewRoutine(context.Background(), oldSession.GetResponser().MysqlRrWr(), &config.FrontendParameters{})
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	conn := &Conn{}
	rm.setRoutine(conn, 0, routine)

	// Model a reset that already owns lifecycle admission when the connection
	// close starts. Closed must wait, then unregister the replacement session.
	require.True(t, routine.mc.beginOperation())
	closed := make(chan struct{})
	go func() {
		rm.Closed(conn)
		close(closed)
	}()
	require.Eventually(t, func() bool {
		routine.mc.Lock()
		defer routine.mc.Unlock()
		return routine.mc.closed
	}, time.Second, time.Millisecond)

	newSession := newTestSession(t, ctrl)
	newSession.uuid = oldSession.uuid
	newSession.setRoutineManager(rm)
	newSession.setRoutine(routine)
	routine.setSession(newSession)
	rm.sessionManager.AddSession(newSession)
	routine.mc.endOperation()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("routine close did not finish after reset completed")
	}

	require.Empty(t, rm.sessionManager.GetAllSessions())
	require.Empty(t, rm.sessionManager.GetSessionsByTenant(oldSession.GetTenantName()))
}

const (
	contextCancel int32 = -2
	timeout       int32 = -1
)

type genMrs func(ses *Session) *MysqlResultSet

type result struct {
	gen        genMrs
	isSleepSql bool
	seconds    int
	resultX    atomic.Int32
	startedCh  chan struct{}
}

var newMockWrapper = func(ctrl *gomock.Controller, ses *Session,
	sql2result map[string]*result,
	sql2NoResultSet map[string]bool, sql string, stmt tree.Statement, proc *process.Process) ComputationWrapper {
	var mrs *MysqlResultSet
	var columns []interface{}
	var ok, ok2 bool
	var err error
	var res *result
	if res, ok = sql2result[sql]; ok {
		mrs = res.gen(ses)
		for _, col := range mrs.Columns {
			columns = append(columns, col)
		}
	} else if _, ok2 = sql2NoResultSet[sql]; ok2 {
		//no result set
	} else {
		panic(fmt.Sprintf("there is no mysqlResultset for the sql %s", sql))
	}
	uuid, _ := uuid.NewV7()
	runner := mock_frontend.NewMockComputationRunner(ctrl)
	runner.EXPECT().Run(gomock.Any()).DoAndReturn(func(uint64) (*util.RunResult, error) {
		proto := ses.GetResponser().MysqlRrWr()
		if mrs != nil {
			if res.isSleepSql {
				if res.startedCh != nil {
					select {
					case res.startedCh <- struct{}{}:
					default:
					}
				}
				res.resultX.Store(0)
				topCtx := proc.GetTopContext()
				select {
				case <-time.After(time.Duration(res.seconds) * time.Second):
					res.resultX.Store(timeout)
				case <-topCtx.Done():
					res.resultX.Store(contextCancel)
				}
			}
			err = proto.WriteResultSetRow(mrs, mrs.GetRowCount())
			if err != nil {
				logutil.Errorf("flush error %v", err)
				return nil, err
			}
		}
		return &util.RunResult{AffectRows: 0}, nil
	}).AnyTimes()
	mcw := mock_frontend.NewMockComputationWrapper(ctrl)
	mcw.EXPECT().GetAst().Return(stmt).AnyTimes()
	mcw.EXPECT().GetProcess().Return(proc).AnyTimes()
	mcw.EXPECT().GetColumns(gomock.Any()).Return(columns, nil).AnyTimes()
	mcw.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
	mcw.EXPECT().GetUUID().Return(uuid[:]).AnyTimes()
	mcw.EXPECT().RecordExecPlan(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mcw.EXPECT().RecordCompoundStmt(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mcw.EXPECT().GetLoadTag().Return(false).AnyTimes()
	mcw.EXPECT().Clear().AnyTimes()
	mcw.EXPECT().Free().AnyTimes()
	mcw.EXPECT().Plan().Return(&plan2.Plan{}).AnyTimes()
	mcw.EXPECT().BinaryExecute().Return(false, "").AnyTimes()
	return mcw
}

func Test_ConnectionCount(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p

	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var conn1, conn2 *sql.DB
	var err error

	//before anything using the configuration
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 0
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	noResultSet := make(map[string]bool)
	resultSet := make(map[string]*result)

	var wrapperStubFunc = func(execCtx *ExecCtx, db string, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
		var cw []ComputationWrapper = nil
		var stmts []tree.Statement = nil
		var cmdFieldStmt *InternalCmdFieldList
		var err error
		if isCmdFieldListSql(execCtx.input.getSql()) {
			cmdFieldStmt, err = parseCmdFieldList(execCtx.reqCtx, execCtx.input.getSql())
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, cmdFieldStmt)
		} else {
			stmts, err = parsers.Parse(execCtx.reqCtx, dialect.MYSQL, execCtx.input.getSql(), 1)
			if err != nil {
				return nil, err
			}
		}

		for _, stmt := range stmts {
			cw = append(cw, newMockWrapper(ctrl, ses, resultSet, noResultSet, execCtx.input.getSql(), stmt, proc))
		}
		return cw, nil
	}

	bhStub := gostub.Stub(&GetComputationWrapper, wrapperStubFunc)
	defer bhStub.Reset()

	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	// A mock autoincrcache manager.
	acim := &defines.AutoIncrCacheManager{}
	setAicm("", acim)
	rm, _ := NewRoutineManager(ctx, "")
	setRtMgr("", rm)

	wg := sync.WaitGroup{}
	wg.Add(1)

	mo := createInnerServer()
	//running server
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn)
	}()

	cCounter := metric.ConnectionCounter(sysAccountName, 0)
	cCounter.Set(0)

	conn1, err = openDbConn(t, 6001)
	require.NoError(t, err)

	clientConn2, serverConn2 := net.Pipe()
	defer serverConn2.Close()
	defer clientConn2.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn2)
	}()

	registerConn(clientConn2)

	conn2, err = openDbConn(t, 6001)
	require.NoError(t, err)

	waitForClientCount := func(expected int) {
		require.Eventually(t, func() bool {
			return rm.clientCount() == expected
		}, time.Second, 10*time.Millisecond)
	}

	waitForGauge := func(expected float64) {
		require.Eventually(t, func() bool {
			var metric pcg.Metric
			if err := cCounter.Write(&metric); err != nil {
				return false
			}
			return metric.Gauge.GetValue() == expected
		}, time.Second, 10*time.Millisecond)
	}

	waitForClientCount(2)
	waitForGauge(2)

	time.Sleep(time.Millisecond * 10)

	//close the connection
	closeDbConn(t, conn1)
	closeDbConn(t, conn2)

	//close server
	clientConn.Close()
	serverConn.Close()
	clientConn2.Close()
	serverConn2.Close()

	waitForClientCount(0)
	waitForGauge(0)

	wg.Wait()
}

func TestMigrateConnectionFromRejectsForeignConns(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	// Foreign connections are session-CN-local; a migration snapshot while any
	// exist would strand the client's handle strings on the target CN.
	conn := &fakeForeignConn{}
	_, err := ses.PutForeignConn(context.Background(), "sql:mig", conn)
	require.NoError(t, err)

	err = rt.migrateConnectionFrom(&query.MigrateConnFromResponse{})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))

	// disconnecting clears the block
	removed, ok := ses.RemoveForeignConn("sql:mig")
	require.True(t, ok)
	require.NoError(t, removed.Close())
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
}
