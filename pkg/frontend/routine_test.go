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
	"fmt"
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

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
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

func TestRoutineCloseCancelsResetRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	oldSession.GetTxnHandler().Close()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: 5 * time.Minute,
	}).AnyTimes()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
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
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.Equal(t, int64(7), resp.LastAffectedRows)
}

func TestRoutineResetSessionKeepsReplacementRegistered(t *testing.T) {
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
	require.Len(t, rm.sessionManager.GetAllSessions(), 1)

	resp := &query.ResetSessionResponse{}
	require.NoError(t, routine.resetSession("", resp))

	newSession := routine.getSession()
	t.Cleanup(func() {
		rm.sessionManager.RemoveSession(newSession)
		newSession.Close()
		rm.cancelCtx()
	})

	require.NotSame(t, oldSession, newSession)
	require.Equal(t, oldSession.GetUUIDString(), newSession.GetUUIDString())

	registered := rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1, "successful reset must keep the replacement session registered")
	require.Same(t, newSession, registered[0])
}

func TestRoutineResetSessionFailureRestoresProtocolState(t *testing.T) {
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()
	t.Cleanup(func() {
		oldSession.Close()
		rm.cancelCtx()
	})

	protocol := oldSession.GetResponser().MysqlRrWr()
	protocol.SetStr(DBNAME, "db1")
	routine := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
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
	txnOp.EXPECT().Rollback(gomock.Any()).Return(assert.AnError)
	oldSession.txnHandler = InitTxnHandler("", eng, context.Background(), txnOp)
	oldSession.txnHandler.shareTxn = false

	err = routine.resetSession("", &query.ResetSessionResponse{})
	require.ErrorIs(t, err, assert.AnError)
	require.Same(t, oldSession, routine.getSession())
	require.Equal(t, "db1", protocol.GetStr(DBNAME))
	registered := rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1)
	require.Same(t, oldSession, registered[0])
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
