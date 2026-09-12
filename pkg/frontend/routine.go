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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

// shouldCloseConnection reports whether the connection lifecycle itself has
// been cancelled. Request-scoped deadlines must not close an otherwise healthy
// connection after a long-running request completes successfully.
func (rt *Routine) shouldCloseConnection() bool {
	if rt.isCancelled() {
		return true
	}
	routineCtx := rt.getCancelRoutineCtx()
	return routineCtx != nil && context.Cause(routineCtx) != nil
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
	var err error
	var resp *Response

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
	routineCtx := rt.getCancelRoutineCtx()

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
	responseAccountingOpen := true
	defer func() {
		if recovered := recover(); recovered != nil {
			if responseAccountingOpen {
				accountingCtx := requestFinalizationContext(&execCtx, tenantCtx)
				ses.finishResponseAccounting(
					accountingCtx,
					moerr.ConvertPanicError(accountingCtx, recovered),
					true,
				)
			}
			panic(recovered)
		}
	}()
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
	ses.finishResponseAccounting(
		requestFinalizationContext(&execCtx, tenantCtx),
		responseErr,
		responseFailed,
	)
	responseAccountingOpen = false

	ses.Debugf(tenantCtx, "the time of handling the request %s", time.Since(reqBegin).String())

	cancelRequestFunc()

	// A completed request may have run longer than an observability threshold,
	// but only connection-lifecycle cancellation should retire the connection.
	if rt.shouldCloseConnection() {
		rt.decreaseCount(func() {
			metric.ConnectionCounter(ses.GetTenantInfo().GetTenant(), ses.GetTenantInfo().GetTenantID()).Dec()
		})

		//ensure cleaning the transaction
		ses.Error(tenantCtx, "rollback the txn.",
			zap.Error(context.Cause(rt.getCancelRoutineCtx())),
			zap.Bool("routine cancelled", rt.isCancelled()))
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

func requestFinalizationContext(execCtx *ExecCtx, fallback context.Context) context.Context {
	if execCtx != nil && execCtx.reqCtx != nil {
		return execCtx.reqCtx
	}
	return fallback
}

func (rt *Routine) countConnectionIfNeeded(ses *Session) {
	if ses == nil || ses.GetTenantInfo() == nil {
		return
	}
	rt.increaseCount(func() {
		tenant := ses.GetTenantInfo()
		metric.ConnectionCounter(tenant.GetTenant(), tenant.GetTenantID()).Inc()
	})
}

// handleSessionCommand executes commands that replace the Session generation.
// They cannot run through handleRequest because request admission pins the old
// generation until that function returns.
func (rt *Routine) handleSessionCommand(ctx context.Context, req *Request) error {
	oldSession := rt.getSession()
	if oldSession == nil {
		return moerr.NewInternalError(ctx, "cannot reset a missing session")
	}

	parameters := rt.getParameters()
	commandCtx, cancel := context.WithTimeoutCause(
		rt.getCancelRoutineCtx(),
		parameters.SessionTimeout.Duration,
		moerr.CauseHandleRequest,
	)
	defer cancel()

	rt.setInProcessRequest(true)
	defer rt.setInProcessRequest(false)
	v2.StartHandleRequestCounter.Inc()
	defer v2.EndHandleRequestCounter.Inc()

	var commandErr error
	switch req.GetCmd() {
	case COM_RESET_CONNECTION:
		if data, ok := req.GetData().([]byte); !ok || len(data) != 0 {
			commandErr = moerr.NewInvalidInput(commandCtx, "COM_RESET_CONNECTION must not contain a payload")
		} else {
			commandErr = rt.resetConnectionWithContext(commandCtx, oldSession.GetService(), &query.ResetSessionResponse{})
		}
	case COM_CHANGE_USER:
		data, ok := req.GetData().([]byte)
		if !ok {
			commandErr = moerr.NewInvalidInput(commandCtx, "invalid COM_CHANGE_USER payload")
		} else {
			commandErr = rt.changeUserWithContext(commandCtx, data)
		}
	default:
		return moerr.NewInternalErrorf(commandCtx, "unsupported session command 0x%x", req.GetCmd())
	}

	currentSession := rt.getSession()
	rt.countConnectionIfNeeded(currentSession)
	status := uint16(0)
	if currentSession != nil && currentSession.GetTxnHandler() != nil {
		status = currentSession.GetTxnHandler().GetServerStatus()
	}
	var resp *Response
	if commandErr != nil {
		resp = NewGeneralErrorResponse(req.GetCmd(), status, commandErr)
		oldSession.Error(commandCtx, "failed to execute session command",
			zap.String("command", req.GetCmd().String()), zap.Error(commandErr))
	} else {
		resp = NewGeneralOkResponse(req.GetCmd(), status)
	}
	writeErr := rt.getProtocol().WriteResponse(commandCtx, resp)
	if commandErr != nil && (req.GetCmd() == COM_CHANGE_USER || errors.Is(commandErr, errSessionResetConnectionMustClose)) {
		// MySQL terminates the connection after a failed change-user
		// authentication. A reset that has already retired part of the old
		// generation must do the same: its retained aliases could no longer
		// describe the physical temporary tables. Do this only after the ERR
		// packet has been attempted.
		disconnectErr := rt.getProtocol().Disconnect()
		if writeErr == nil {
			writeErr = disconnectErr
		}
	}
	return writeErr
}

func (rt *Routine) changeUserWithContext(ctx context.Context, data []byte) error {
	operationCtx, ok := rt.mc.tryBeginOperationWithContext(ctx)
	if !ok {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return moerr.NewInternalErrorNoCtx("cannot change user as routine is closed or busy")
	}
	defer rt.mc.endOperation()

	protocol, ok := rt.getProtocol().(*MysqlProtocolImpl)
	if !ok {
		return moerr.NewInternalError(operationCtx, "COM_CHANGE_USER requires the MySQL wire protocol")
	}
	change, err := protocol.parseChangeUserRequest(operationCtx, data)
	if err != nil {
		return err
	}
	if change.clientPluginName != "" && change.clientPluginName != AuthNativePassword {
		change.authResponse, err = protocol.negotiateAuthenticationMethod(operationCtx)
		if err != nil {
			return moerr.NewInternalErrorf(operationCtx, "negotiate authentication method failed: %v", err)
		}
	}
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}

	oldSession := rt.getSession()
	oldTenant := oldSession.GetTenantInfo()
	routineManager := oldSession.getRoutineManager()
	oldRestricted := rt.isRestricted()
	oldExpired := rt.isExpired()
	previousProtocolState := protocol.snapshotSessionState()

	newSession := NewSession(rt.getCancelRoutineCtx(), oldSession.GetService(), protocol, nil)
	newSession.inheritPhysicalConnection(oldSession)
	committed := false
	defer func() {
		if committed {
			return
		}
		if tenant := newSession.GetTenantInfo(); tenant != nil && newSession.getRoutineManager() != nil &&
			(oldTenant == nil || tenant.GetTenantID() != oldTenant.GetTenantID()) {
			// Authentication records the routine in the candidate account. For a
			// same-account change this is the old session's existing entry, so only
			// undo registrations made in a different account.
			newSession.getRoutineManager().accountRoutine.deleteRoutine(int64(tenant.GetTenantID()), rt)
		}
		protocol.setSessionState(previousProtocolState)
		rt.setResricted(oldRestricted)
		rt.setExpired(oldExpired)
		newSession.ReserveConn()
		newSession.Close()
	}()

	rt.setResricted(false)
	rt.setExpired(false)
	protocol.setChangeUserState(newSession, change)
	if err = protocol.authenticateUser(operationCtx, change.authResponse); err != nil {
		return err
	}
	newSession.SetDatabaseName(change.database)
	allowedPacketSize, err := newSession.GetSessionSysVar("max_allowed_packet")
	if err != nil {
		return err
	}
	maxPacketSize, ok := allowedPacketSize.(int64)
	if !ok {
		return moerr.NewInternalErrorf(operationCtx, "invalid max_allowed_packet value %T", allowedPacketSize)
	}
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}
	if err = oldSession.closeForReset(operationCtx); err != nil {
		return err
	}

	newTenant := newSession.GetTenantInfo()
	if oldTenant != nil && newTenant != nil && oldTenant.GetTenantID() != newTenant.GetTenantID() {
		routineManager.accountRoutine.deleteRoutine(int64(oldTenant.GetTenantID()), rt)
		if rt.connectionBeCounted.Load() {
			metric.ConnectionCounter(oldTenant.GetTenant(), oldTenant.GetTenantID()).Dec()
			metric.ConnectionCounter(newTenant.GetTenant(), newTenant.GetTenantID()).Inc()
		}
	}
	if protocol.tcpConn != nil {
		protocol.tcpConn.allowedPacketSize = int(maxPacketSize)
	}
	rt.setSession(newSession)
	newSession.getRoutineManager().sessionManager.AddSession(newSession)
	committed = true
	return nil
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
	action := query.MigrateConnFromAction_MigrateConnFromExport
	if resp == nil {
		action = query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease
	}
	return rt.migrateConnectionFromActionWithContext(ctx, action, resp)
}

func (rt *Routine) migrateConnectionFromActionWithContext(
	ctx context.Context,
	action query.MigrateConnFromAction,
	resp *query.MigrateConnFromResponse,
) error {
	return rt.migrateConnectionFromActionWithCapabilities(
		ctx, action, true, resp,
	)
}

func (rt *Routine) migrateConnectionFromActionWithCapabilities(
	ctx context.Context,
	action query.MigrateConnFromAction,
	tempTableMigrationSupported bool,
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

	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}
	ses := rt.getSession()
	switch action {
	case query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease:
		if states := function.UserLevelLocksForMigration(ses.proc); len(states) > 0 {
			return moerr.NewInternalErrorNoCtx("cannot migrate connection while user-level locks are held")
		}
		ses.userLevelLocksMigrated = true
		return nil
	case query.MigrateConnFromAction_MigrateConnFromEnableUserLevelLockRelease:
		ses.userLevelLocksMigrated = false
		return nil
	}
	if states := function.UserLevelLocksForMigration(ses.proc); len(states) > 0 {
		return moerr.NewInternalErrorNoCtx("cannot migrate connection while user-level locks are held")
	}
	// Foreign connections (esql_tvf/sql_tvf and ENGINE=ESQL|SQL scans) are
	// session-CN-local: neither the pools/transports nor the handle->conn map
	// migrate, so a transferred client would keep handle strings that resolve
	// to nothing on the target. Fail closed like pending long-data below;
	// the client can disconnect its handles and retry.
	if ses.hasForeignConns() {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	tempTables, err := ses.snapshotTempTablesForMigration(operationCtx)
	if err != nil {
		if isMigrationSnapshotSizeLimitError(err) {
			// Do not send an impossible clone batch to a target CN. The current
			// session remains usable on the source and a later transfer can be
			// admitted after the client reduces its temporary-table state.
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		return err
	}
	if len(tempTables) > 0 && !tempTableMigrationSupported {
		// An older Proxy would silently omit the alias map and let source-session
		// cleanup delete the physical tables after handoff. Keep the connection
		// on this CN until a capable Proxy performs a lossless migration.
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	if tempTableMigrationSupported {
		resp.TempTables = tempTables
		resp.TempTableStateExported = true
	}
	resp.UserLevelLockReleaseSupported = true
	resp.DB = ses.GetDatabaseName()
	resp.LastAffectedRows = ses.GetLastAffectedRows()
	prepareStmts := ses.GetPrepareStmts()
	for _, st := range prepareStmts {
		// COM_STMT_SEND_LONG_DATA has no protocol response and its parameter
		// buffers are not part of the migration payload. Reject the snapshot at
		// the authoritative session owner instead of relying on the proxy to
		// infer SQL PREPARE/DEALLOCATE lifecycle changes from COM_QUERY text.
		if st.hasPendingLongData() {
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
	}
	resp.PreparedStmtLongDataChecked = true
	resp.FoundRows = ses.GetLastFoundRows()
	if currentProtocolVersion(ses.proc) >= defines.MORPCVersion22 {
		// Typed snapshots can only be replayed by a v22 target when the proxy's
		// raw COM_QUERY history did not observe every assignment (for example,
		// prepared SET values). Keep that fact explicit for target negotiation.
		resp.UserDefinedVarsReplayable = !ses.hasUnreplayableMigrationUserVars()
		resp.SystemVariablesReplayable = !ses.hasUnreplayableMigrationSystemVars()
		var userVars []*query.MigrateUserDefinedVar
		var userVarsExported bool
		vars, err := ses.snapshotUserDefinedVars(operationCtx)
		if err != nil {
			// A v22 target must not re-evaluate raw SET expressions when the
			// evaluated user-variable snapshot is omitted. Keep the overflow
			// reason explicit so the proxy can fail closed for v22 while still
			// allowing complete raw replay to legacy targets.
			if !isMigrationSnapshotSizeLimitError(err) {
				return err
			}
			resp.UserDefinedVarsSnapshotTooLarge = true
		} else {
			userVars = vars
			userVarsExported = true
		}
		var systemVars []*query.MigrateSystemVariable
		var systemVarsExported bool
		systemVars, err = ses.snapshotSessionSystemVars(operationCtx)
		if err != nil {
			// A complete raw replay remains valid for a pre-v22 target, but a
			// v22 target cannot safely consume the system-only projection. Keep
			// the reason explicit so the proxy can negotiate that distinction.
			if !isMigrationSnapshotSizeLimitError(err) ||
				ses.hasUnreplayableMigrationSystemVars() {
				return err
			}
			resp.SystemVariablesSnapshotTooLarge = true
		} else {
			systemVarsExported = true
		}
		resp.UserDefinedVars = userVars
		resp.UserDefinedVarsExported = userVarsExported
		resp.SystemVariables = systemVars
		resp.SystemVariablesExported = systemVarsExported
	}
	for _, st := range prepareStmts {
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
	return rt.resetSessionWithAdmission(
		rt.getCancelRoutineCtx(), baseServiceID, resp, false, false,
	)
}

func (rt *Routine) resetSessionWithContext(
	ctx context.Context,
	baseServiceID string,
	resp *query.ResetSessionResponse,
) error {
	return rt.resetSessionWithAdmission(ctx, baseServiceID, resp, true, false)
}

// refreshSessionAuthWithContext reauthenticates a backend that was reset for
// cache reuse. ResetSession intentionally keeps the physical protocol alive,
// but its credential and resolved-role snapshot can become stale while the
// backend is idle. Build a candidate session, authenticate it against the
// current catalog, and publish it only after the old generation is retired.
func (rt *Routine) refreshSessionAuthWithContext(
	ctx context.Context,
	req *query.RefreshSessionAuthRequest,
	resp *query.RefreshSessionAuthResponse,
) error {
	if resp != nil {
		resp.Success = false
		resp.AuthString = nil
		resp.AuthenticationFailed = false
		resp.RequestRejected = false
	}
	operationCtx, ok := rt.mc.tryBeginOperationWithContext(ctx)
	if !ok {
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		}
		return moerr.NewInternalErrorNoCtx("cannot refresh session authentication as routine is closed or busy")
	}
	defer rt.mc.endOperation()
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}

	oldSession := rt.getSession()
	if oldSession == nil {
		return moerr.NewInternalError(operationCtx, "cannot refresh authentication for a missing session")
	}
	protocolValue := rt.getProtocol()
	if protocolValue == nil {
		return moerr.NewInternalError(operationCtx, "cannot refresh authentication without a protocol")
	}
	protocol, ok := protocolValue.(*MysqlProtocolImpl)
	if !ok {
		return moerr.NewInternalError(operationCtx, "refresh session authentication requires the MySQL wire protocol")
	}
	if resp == nil {
		return moerr.NewInvalidInput(operationCtx, "refresh session authentication response is nil")
	}
	if req == nil || req.UserInput == "" {
		return moerr.NewInvalidInput(operationCtx, "refresh session authentication requires a user")
	}
	if len(req.Salt) == 0 {
		return moerr.NewInvalidInput(operationCtx, "refresh session authentication requires a salt")
	}

	oldTenant := oldSession.GetTenantInfo()
	routineManager := oldSession.getRoutineManager()
	oldRestricted := rt.isRestricted()
	oldExpired := rt.isExpired()
	previousProtocolState := protocol.snapshotSessionState()

	newSession := NewSession(rt.getCancelRoutineCtx(), oldSession.GetService(), protocol, nil)
	newSession.inheritPhysicalConnection(oldSession)
	// Never inherit the previous client's host admission input. An empty value
	// is deliberately fail-closed when host checks are enabled.
	newSession.clientAddr = req.ClientAddress
	previousSalt := append([]byte(nil), protocol.GetSalt()...)
	change := changeUserRequest{
		username:     req.UserInput,
		database:     req.Database,
		authResponse: append([]byte(nil), req.AuthResponse...),
	}
	protocol.setChangeUserState(newSession, change)
	committed := false
	defer func() {
		if committed {
			return
		}
		if tenant := newSession.GetTenantInfo(); tenant != nil && newSession.getRoutineManager() != nil &&
			(oldTenant == nil || tenant.GetTenantID() != oldTenant.GetTenantID()) {
			newSession.getRoutineManager().accountRoutine.deleteRoutine(int64(tenant.GetTenantID()), rt)
		}
		protocol.setSessionState(previousProtocolState)
		protocol.SetSalt(previousSalt)
		rt.setResricted(oldRestricted)
		rt.setExpired(oldExpired)
		newSession.ReserveConn()
		newSession.Close()
	}()

	rt.setResricted(false)
	rt.setExpired(false)
	// A cached backend retains the salt from its previous client. Rebind the
	// physical protocol to the current handshake salt before invoking the
	// canonical authentication path; this also validates special users and
	// initializes system variables exactly as a fresh login does.
	protocol.SetSalt(append([]byte(nil), req.Salt...))
	if err := protocol.authenticateUser(operationCtx, change.authResponse); err != nil {
		resp.AuthenticationFailed = isAuthenticationRejected(err) ||
			needConvertedToAccessDeniedError(strings.ToLower(err.Error()))
		resp.RequestRejected = isAuthenticationRequestRejected(err)
		return err
	}
	authString := append([]byte(nil), protocol.GetAuthString()...)
	newSession.SetDatabaseName(req.Database)
	allowedPacketSize, err := newSession.GetSessionSysVar("max_allowed_packet")
	if err != nil {
		return err
	}
	maxPacketSize, ok := allowedPacketSize.(int64)
	if !ok {
		return moerr.NewInternalErrorf(operationCtx, "invalid max_allowed_packet value %T", allowedPacketSize)
	}
	if cause := context.Cause(operationCtx); cause != nil {
		return cause
	}
	if err = oldSession.closeForReset(operationCtx); err != nil {
		return err
	}

	newTenant := newSession.GetTenantInfo()
	if oldTenant != nil && newTenant != nil && oldTenant.GetTenantID() != newTenant.GetTenantID() {
		routineManager.accountRoutine.deleteRoutine(int64(oldTenant.GetTenantID()), rt)
		if rt.connectionBeCounted.Load() {
			metric.ConnectionCounter(oldTenant.GetTenant(), oldTenant.GetTenantID()).Dec()
			metric.ConnectionCounter(newTenant.GetTenant(), newTenant.GetTenantID()).Inc()
		}
	}
	if protocol.tcpConn != nil {
		protocol.tcpConn.allowedPacketSize = int(maxPacketSize)
	}
	protocol.m.Lock()
	protocol.authString = append(protocol.authString[:0], authString...)
	protocol.m.Unlock()
	rt.setSession(newSession)
	newSession.getRoutineManager().sessionManager.AddSession(newSession)
	resp.Success = true
	resp.AuthString = append([]byte(nil), authString...)
	committed = true
	return nil
}

func (rt *Routine) resetConnectionWithContext(
	ctx context.Context,
	baseServiceID string,
	resp *query.ResetSessionResponse,
) error {
	// COM_RESET_CONNECTION retains the selected database. QueryService
	// ResetSession has a distinct Proxy handoff contract and starts its
	// replacement generation without one.
	return rt.resetSessionWithAdmission(ctx, baseServiceID, resp, true, true)
}

func (rt *Routine) resetSessionWithAdmission(
	ctx context.Context,
	baseServiceID string,
	resp *query.ResetSessionResponse,
	waitForRequest bool,
	preserveDatabase bool,
) error {
	var operationCtx context.Context
	var ok bool
	if waitForRequest {
		// ResetSession is sent after Proxy has sealed the client generation, so
		// waiting here lets an already-running request finish before the old
		// session is replaced. The QueryService caller supplies the bounded
		// context; the no-context helper above intentionally remains fail-fast.
		operationCtx, ok = rt.mc.beginOperationAfterRequestWithContext(ctx)
	} else {
		operationCtx, ok = rt.mc.tryBeginOperationWithContext(ctx)
	}
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

	// Proxy reset deliberately starts without a selected database. The MySQL wire
	// COM_RESET_CONNECTION contract preserves it across a session reset.
	protocol := rt.getProtocol()
	previousDB := protocol.GetStr(DBNAME)
	if !preserveDatabase {
		protocol.SetStr(DBNAME, "")
	}

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
