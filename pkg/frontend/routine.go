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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type holder[T any] struct {
	value T
}

var clientRequestClockOrigin = time.Now()

func clientRequestClockValue(now time.Time) int64 {
	// Reserve zero as the inactive sentinel.
	return now.Sub(clientRequestClockOrigin).Nanoseconds() + 1
}

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol atomic.Pointer[holder[MysqlRrWr]]

	cancelRoutineCtx  context.Context
	cancelRoutineFunc context.CancelFunc
	cancelRequestFunc context.CancelFunc

	parameters *config.FrontendParameters

	ses *Session

	closeOnce sync.Once

	inProcessRequest bool
	requestStartedAt atomic.Int64
	closing          atomic.Bool

	cancelled atomic.Bool

	connectionBeCounted atomic.Bool

	mu sync.Mutex

	// the id of goroutine that executes the request
	goroutineID uint64

	restricted atomic.Bool

	expired atomic.Bool

	printInfoOnce bool

	mc *migrateController
}

func (rt *Routine) needPrintSessionInfo() bool {
	if rt.printInfoOnce {
		rt.printInfoOnce = false
		return true
	}
	return false
}

func (rt *Routine) setResricted(val bool) {
	rt.restricted.Store(val)
}

func (rt *Routine) isRestricted() bool {
	return rt.restricted.Load()
}

func (rt *Routine) setExpired(val bool) {
	rt.expired.Store(val)
}

func (rt *Routine) isExpired() bool {
	return rt.expired.Load()
}

func (rt *Routine) increaseCount(counter func()) {
	if rt.connectionBeCounted.CompareAndSwap(false, true) {
		if counter != nil {
			counter()
		}
	}
}

func (rt *Routine) decreaseCount(counter func()) {
	if rt.connectionBeCounted.CompareAndSwap(true, false) {
		if counter != nil {
			counter()
		}
	}
}

func (rt *Routine) setCancelled(b bool) bool {
	return rt.cancelled.Swap(b)
}

func (rt *Routine) isCancelled() bool {
	return rt.cancelled.Load()
}

func (rt *Routine) setInProcessRequest(b bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.inProcessRequest = b
	if b {
		rt.requestStartedAt.Store(clientRequestClockValue(time.Now()))
	} else {
		rt.requestStartedAt.Store(0)
	}
}

func (rt *Routine) requestRunningLongerThan(nowValue int64, minimum time.Duration) bool {
	if rt.closing.Load() {
		return false
	}
	startedAt := rt.requestStartedAt.Load()
	if startedAt == 0 {
		return false
	}
	return nowValue-startedAt >= minimum.Nanoseconds()
}

// execCallbackInProcessRequestOnly denotes if inProcessRequest is true,
// then the callback will be called.
// It has used the mutex.
func (rt *Routine) execCallbackBasedOnRequest(want bool, callback func()) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.inProcessRequest == want {
		if callback != nil {
			callback()
		}
	}
}

func (rt *Routine) releaseRoutineCtx() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.cancelRoutineFunc != nil {
		rt.cancelRoutineFunc()
	}
}

func (rt *Routine) getCancelRoutineCtx() context.Context {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.cancelRoutineCtx
}

func (rt *Routine) getProtocol() MysqlRrWr {
	return rt.protocol.Load().value
}

func (rt *Routine) getConnectionID() uint32 {
	return rt.getProtocol().GetU32(CONNID)
}

func (rt *Routine) getGoroutineId() uint64 {
	if rt.goroutineID == 0 {
		rt.goroutineID = GetRoutineId()
	}
	return rt.goroutineID
}

func (rt *Routine) getParameters() *config.FrontendParameters {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.parameters
}

func (rt *Routine) setSession(ses *Session) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ses = ses
}

func (rt *Routine) getSession() *Session {
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.ses
}

func (rt *Routine) setCancelRequestFunc(cf context.CancelFunc) {
	rt.mu.Lock()
	rt.cancelRequestFunc = cf
	closing := rt.closing.Load()
	rt.mu.Unlock()
	if closing && cf != nil {
		cf()
	}
}

func (rt *Routine) cancelRequestCtx() {
	rt.mu.Lock()
	cancel := rt.cancelRequestFunc
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// beginClose seals new lifecycle work and cancels current request/lifecycle
// contexts without waiting for them. Resource cleanup remains owned by
// cleanup's closeOnce path.
func (rt *Routine) beginClose() {
	rt.mu.Lock()
	rt.closing.Store(true)
	cancelRequest := rt.cancelRequestFunc
	rt.mu.Unlock()

	if cancelRequest != nil {
		cancelRequest()
	}
	rt.mc.startClose()
}

func (rt *Routine) getCleanupContext() context.Context {
	if ses := rt.getSession(); ses != nil {
		if txnHandler := ses.GetTxnHandler(); txnHandler != nil {
			if ctx := txnHandler.GetTxnCtx(); ctx != nil {
				return ctx
			}
		}
	}
	return context.Background()
}

func (rt *Routine) handleRequest(req *Request) error {
	var routineCtx context.Context
	var err error
	var resp *Response
	var quit bool

	ses := rt.getSession()

	execCtx := ExecCtx{
		ses: ses,
	}
	defer execCtx.Close()
	v2.StartHandleRequestCounter.Inc()
	defer func() {
		v2.EndHandleRequestCounter.Inc()
	}()

	reqBegin := time.Now()
	// WithHungThreshold used to add this deadline as a side effect of starting
	// a Span. Preserve the request cleanup guard independently of the retired
	// Span recording and profiling runtime.
	routineCtx, cancelHungRequest := context.WithTimeoutCause(
		rt.getCancelRoutineCtx(),
		30*time.Minute,
		moerr.CauseNewMOHungSpan,
	)
	defer cancelHungRequest()

	parameters := rt.getParameters()
	//all offspring related to the request inherit the txnCtx
	cancelRequestCtx, cancelRequestFunc := context.WithTimeoutCause(ses.GetTxnHandler().GetTxnCtx(), parameters.SessionTimeout.Duration, moerr.CauseHandleRequest)
	rt.setCancelRequestFunc(cancelRequestFunc)
	ses.EnterFPrint(FPHandleRequest)
	defer ses.ExitFPrint(FPHandleRequest)

	if rt.needPrintSessionInfo() {
		ses.Debug(routineCtx, "mo received first request")
	}

	tenant := ses.GetTenantInfo()
	nodeCtx := cancelRequestCtx
	if ses.getRoutineManager().baseService != nil {
		nodeCtx = context.WithValue(cancelRequestCtx, defines.NodeIDKey{}, ses.getRoutineManager().baseService.ID())
	}
	tenantCtx := defines.AttachAccount(nodeCtx, tenant.GetTenantID(), tenant.GetUserID(), tenant.GetDefaultRoleID())

	rt.increaseCount(func() {
		metric.ConnectionCounter(ses.GetTenantInfo().GetTenant(), ses.GetTenantInfo().GetTenantID()).Inc()
	})

	execCtx.reqCtx = tenantCtx
	ses.beginResponseAccounting()
	if resp, err = ExecRequest(ses, &execCtx, req); err != nil {
		err = moerr.AttachCause(tenantCtx, err)
		if !skipClientQuit(err.Error()) {
			ses.Error(tenantCtx,
				"Failed to execute request",
				zap.Error(err))
		}
	}

	if resp != nil {
		if err = rt.getProtocol().WriteResponse(tenantCtx, resp); err != nil {
			err = moerr.AttachCause(tenantCtx, err)
			if resp.isIssue3482 {
				ses.Error(tenantCtx,
					"Failed to send response",
					zap.String("response", fmt.Sprintf("%v", resp)),
					zap.String("load local ", resp.loadLocalFile),
					zap.Error(err))
			} else {
				ses.Error(tenantCtx,
					"Failed to send response",
					zap.String("response", fmt.Sprintf("%v", resp)),
					zap.Error(err))
			}
		}
		if resp.isIssue3482 {
			ses.Infof(tenantCtx, "load local '%s' exec failed. response error success", resp.loadLocalFile)
		}
	}
	responseFailed := err != nil
	responseErr := err
	if resp != nil && resp.category == ErrorResponse {
		responseFailed = true
		if execErr, ok := resp.data.(error); ok && execErr != nil {
			responseErr = execErr
		}
	}
	ses.finishResponseAccounting(tenantCtx, responseErr, responseFailed)

	ses.Debugf(tenantCtx, "the time of handling the request %s", time.Since(reqBegin).String())

	cancelRequestFunc()

	//check the connection has been already canceled or not.
	select {
	case <-routineCtx.Done():
		quit = true
	default:
	}

	quit = quit || rt.isCancelled()

	if quit {
		rt.decreaseCount(func() {
			metric.ConnectionCounter(ses.GetTenantInfo().GetTenant(), ses.GetTenantInfo().GetTenantID()).Dec()
		})

		//ensure cleaning the transaction
		ses.Error(tenantCtx, "rollback the txn.")
		tempExecCtx := ExecCtx{
			reqCtx: rt.getCleanupContext(),
			ses:    ses,
			txnOpt: FeTxnOption{byRollback: true},
		}
		defer tempExecCtx.Close()
		err = ses.GetTxnHandler().Rollback(&tempExecCtx)
		if err != nil {
			ses.Error(tenantCtx,
				"Failed to rollback txn",
				zap.Error(err))
		}

		//close the network connection
		proto := rt.getProtocol()
		if proto != nil {
			proto.Close()
		}
	}

	return err
}

// killQuery if there is a running query, just cancel it.
func (rt *Routine) killQuery(killMyself bool, statementId string) {
	if !killMyself {
		//1,cancel request ctx
		rt.cancelRequestCtx()
		//2.update execute state
		ses := rt.getSession()
		if ses != nil {
			ses.SetQueryInExecute(false)
		}
	}
}

// killConnection close the network connection
// myself: true -- the client kill itself.
// myself: false -- the client kill another connection.
func (rt *Routine) killConnection(killMyself bool) {
	//Case 1: kill the connection itself. Do not close the network connection here.
	//label the connection with the cancelled tag
	//if it was cancelled, do nothing
	if rt.setCancelled(true) {
		return
	}

	//Case 2: kill another connection. Close the network here.
	//    if the connection is processing the request, the response may be dropped.
	//    if the connection is not processing the request, it has no effect.
	if !killMyself {
		//If it is in processing the request, cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		//After the context is cancelled. In handleRequest, the network
		//will be closed finally.
		rt.releaseRoutineCtx()

		//If it is in processing the request, it responds to the client normally
		//before closing the network to avoid the mysql client to be hung.
		closeConn := func() {
			//If it is not in processing the request, just close the network
			proto := rt.getProtocol()
			if proto != nil {
				proto.Close()
			}
		}

		rt.execCallbackBasedOnRequest(false, closeConn)
	}
}

// cleanup When the io is closed, the cleanup will be called in callback Closed().
// cleanup releases the resources only once.
// both the client and the server can close the connection.
func (rt *Routine) cleanup() {
	//step 1: cancel the query if there is a running query.
	//step 2: close the connection.
	rt.closeOnce.Do(func() {
		// Seal and cancel before waiting. beginClose itself never waits, so it is
		// safe to call from the connection-liveness control path.
		rt.beginClose()
		rt.mc.waitAndClose()
		rt.killQuery(false, "")

		var txnMeta string
		ses := rt.getSession()
		//step A: rollback the txn
		if ses != nil {
			ses.EnterFPrint(FPCleanup)
			defer ses.ExitFPrint(FPCleanup)
			tempExecCtx := ExecCtx{
				reqCtx: rt.getCleanupContext(),
				ses:    ses,
				txnOpt: FeTxnOption{byRollback: true},
			}
			defer tempExecCtx.Close()
			txnHandler := ses.GetTxnHandler()
			err := txnHandler.Rollback(&tempExecCtx)
			if err != nil {
				ses.Error(tempExecCtx.reqCtx,
					"Failed to rollback txn",
					zap.Error(err))
			}
			if txnHandler != nil && txnHandler.GetTxn() != nil {
				txnOp := txnHandler.GetTxn()
				txnMeta = txnOp.Txn().DebugString()
			}
			ses.Info(tempExecCtx.reqCtx, "routine cleanup", zap.Uint64("routine go id", rt.goroutineID), zap.String("last txnMeta", txnMeta))
		} else {
			logutil.Info("routine cleanup without session", zap.Uint64("routine go id", rt.goroutineID))
		}

		//step B: cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		rt.releaseRoutineCtx()

		//step C: clean protocol
		rt.getProtocol().Close()
		rt.protocol.Store(&holder[MysqlRrWr]{})

		//step D: release the resources related to the session
		if ses != nil {
			ses.Close()
			rt.ses = nil
		}
	})
}

func (rt *Routine) migrateConnectionTo(ctx context.Context, req *query.MigrateConnToRequest) error {
	operationCtx, ok := rt.mc.beginOperationWithContext(ctx)
	if !ok {
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		}
		return moerr.NewInternalErrorNoCtx("cannot start migrate as routine has been closed")
	}
	defer rt.mc.endOperation()

	rt.mc.migrateOnce.Do(func() {
		ses := rt.getSession()
		ses.UpdateDebugString()
		rt.mc.migrateErr = Migrate(operationCtx, ses, req)
	})
	return rt.mc.migrateErr
}

func (rt *Routine) migrateConnectionFrom(resp *query.MigrateConnFromResponse) error {
	return rt.migrateConnectionFromWithContext(rt.getCancelRoutineCtx(), resp)
}

func (rt *Routine) migrateConnectionFromWithContext(
	ctx context.Context,
	resp *query.MigrateConnFromResponse,
) error {
	operationCtx, ok := rt.mc.beginOperationWithContext(ctx)
	if !ok {
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		}
		return moerr.NewInternalErrorNoCtx("cannot migrate from a routine that has been closed")
	}
	defer rt.mc.endOperation()

	ses := rt.getSession()
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}
	resp.DB = ses.GetDatabaseName()
	resp.LastAffectedRows = ses.GetLastAffectedRows()
	for _, st := range ses.GetPrepareStmts() {
		resp.PrepareStmts = append(resp.PrepareStmts, &query.PrepareStmt{
			Name:       st.Name,
			SQL:        st.Sql,
			ParamTypes: st.ParamTypes,
		})
	}
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}
	return nil
}

func (rt *Routine) resetSession(baseServiceID string, resp *query.ResetSessionResponse) error {
	return rt.resetSessionWithContext(rt.getCancelRoutineCtx(), baseServiceID, resp)
}

func (rt *Routine) resetSessionWithContext(
	ctx context.Context,
	baseServiceID string,
	resp *query.ResetSessionResponse,
) error {
	operationCtx, ok := rt.mc.tryBeginOperationWithContext(ctx)
	if !ok {
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		}
		return moerr.NewInternalErrorNoCtx("cannot reset session as routine is closed or busy")
	}
	defer rt.mc.endOperation()
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}

	// retrieve the old session.
	oldSession := rt.getSession()

	// create a new session with a new context.
	cancelCtx := rt.getCancelRoutineCtx()
	cancelCtx = context.WithValue(cancelCtx, defines.NodeIDKey{}, baseServiceID)

	// before create new session, we should reset the database on the protocol.
	protocol := rt.getProtocol()
	previousDB := protocol.GetStr(DBNAME)
	protocol.SetStr(DBNAME, "")

	newSession := NewSession(cancelCtx, baseServiceID, protocol, nil)
	resetCommitted := false
	defer func() {
		if !resetCommitted {
			protocol.SetStr(DBNAME, previousDB)
			// The protocol still belongs to the old routine/session on this
			// path. Release the speculative session without closing the
			// connection.
			newSession.ReserveConn()
			newSession.Close()
		}
	}()

	// reset the old and new session.
	if err := newSession.reset(operationCtx, oldSession); err != nil {
		return err
	}

	// some cleanups in the routine.
	rt.killQuery(false, "")

	// reset the new session in other instances.
	protocol.Reset(newSession)
	rt.setSession(newSession)
	newSession.getRoutineManager().sessionManager.AddSession(newSession)
	resetCommitted = true

	// update the password filed in response.
	resp.AuthString = []byte(protocol.GetStr(AuthString))

	return nil
}

func NewRoutine(ctx context.Context, protocol MysqlRrWr, parameters *config.FrontendParameters) *Routine {
	ctx = trace.Generate(ctx) // fill span{trace_id} in ctx
	cancelRoutineCtx, cancelRoutineFunc := context.WithCancel(ctx)
	ri := &Routine{
		cancelRoutineCtx:  cancelRoutineCtx,
		cancelRoutineFunc: cancelRoutineFunc,
		parameters:        parameters,
		printInfoOnce:     true,
		mc:                newMigrateController(),
		goroutineID:       GetRoutineId(),
	}
	ri.protocol.Store(&holder[MysqlRrWr]{value: protocol})
	protocol.UpdateCtx(cancelRoutineCtx)

	return ri
}
