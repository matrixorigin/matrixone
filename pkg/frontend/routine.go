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

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol MysqlProtocol

	cancelRoutineCtx  context.Context
	cancelRoutineFunc context.CancelFunc
	cancelRequestFunc context.CancelFunc

	parameters *config.FrontendParameters

	ses *Session

	closeOnce sync.Once

	inProcessRequest bool

	cancelled atomic.Bool

	connectionBeCounted atomic.Bool

	mu sync.Mutex

	// the id of goroutine that executes the request
	goroutineID uint64

	restricted atomic.Bool

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

func (rt *Routine) getProtocol() MysqlProtocol {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.protocol
}

func (rt *Routine) getConnectionID() uint32 {
	return rt.getProtocol().ConnectionID()
}

func (rt *Routine) updateGoroutineId() {
	if rt.goroutineID == 0 {
		rt.goroutineID = GetRoutineId()
	}
}

func (rt *Routine) getGoroutineId() uint64 {
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
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.ses
}

func (rt *Routine) setCancelRequestFunc(cf context.CancelFunc) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.cancelRequestFunc = cf
}

func (rt *Routine) cancelRequestCtx() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.cancelRequestFunc != nil {
		rt.cancelRequestFunc()
	}
}

func (rt *Routine) reportSystemStatus() (r bool) {
	ss := rt.ses
	if ss == nil {
		return
	}
	rm := ss.getRoutineManager()
	if rm == nil {
		return
	}

	now := time.Now()
	defer func() {
		if r {
			rm.reportSystemStatusTime.Store(&now)
		}
	}()
	last := rm.reportSystemStatusTime.Load()
	if last == nil {
		r = true
		return
	}
	if now.Sub(*last) > time.Minute {
		r = true
		return
	}
	return
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

	v2.StartHandleRequestCounter.Inc()
	defer func() {
		v2.EndHandleRequestCounter.Inc()
	}()

	reqBegin := time.Now()
	var span trace.Span
	routineCtx, span = trace.Start(rt.getCancelRoutineCtx(), "Routine.handleRequest",
		trace.WithHungThreshold(30*time.Minute),
		trace.WithProfileGoroutine(),
		trace.WithProfileSystemStatus(func() ([]byte, error) {
			ss, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.StatusServer)
			if !ok {
				return nil, nil
			}
			if !rt.reportSystemStatus() {
				return nil, nil
			}
			data, err := ss.(*status.Server).Dump()
			return data, err
		}),
	)
	defer span.End()

	parameters := rt.getParameters()
	//all offspring related to the request inherit the txnCtx
	cancelRequestCtx, cancelRequestFunc := context.WithTimeout(ses.GetTxnHandler().GetTxnCtx(), parameters.SessionTimeout.Duration)
	rt.setCancelRequestFunc(cancelRequestFunc)
	ses.UpdateDebugString()

	if rt.needPrintSessionInfo() {
		logInfof(ses.GetDebugString(), "mo received first request")
	}

	tenant := ses.GetTenantInfo()
	nodeCtx := cancelRequestCtx
	if ses.getRoutineManager().baseService != nil {
		nodeCtx = context.WithValue(cancelRequestCtx, defines.NodeIDKey{}, ses.getRoutineManager().baseService.ID())
	}
	tenantCtx := defines.AttachAccount(nodeCtx, tenant.GetTenantID(), tenant.GetUserID(), tenant.GetDefaultRoleID())

	rt.increaseCount(func() {
		metric.ConnectionCounter(ses.GetTenantInfo().GetTenant()).Inc()
	})

	execCtx.reqCtx = tenantCtx
	if resp, err = ExecRequest(ses, &execCtx, req); err != nil {
		if !skipClientQuit(err.Error()) {
			logError(ses, ses.GetDebugString(),
				"Failed to execute request",
				zap.Error(err))
		}
	}

	if resp != nil {
		if err = rt.getProtocol().SendResponse(tenantCtx, resp); err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to send response",
				zap.String("response", fmt.Sprintf("%v", resp)),
				zap.Error(err))
		}
	}

	logDebugf(ses.GetDebugString(), "the time of handling the request %s", time.Since(reqBegin).String())

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
			metric.ConnectionCounter(ses.GetTenantInfo().GetTenant()).Dec()
		})

		//ensure cleaning the transaction
		logError(ses, ses.GetDebugString(), "rollback the txn.")
		tempExecCtx := ExecCtx{
			ses:    ses,
			txnOpt: FeTxnOption{byRollback: true},
		}
		err = ses.GetTxnHandler().Rollback(&tempExecCtx)
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to rollback txn",
				zap.Error(err))
		}

		//close the network connection
		proto := rt.getProtocol()
		if proto != nil {
			proto.Quit()
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
			proto := rt.protocol
			if proto != nil {
				proto.Quit()
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
		// we should wait for the migration and close the migration controller.
		rt.mc.waitAndClose()

		ses := rt.getSession()
		//step A: rollback the txn
		if ses != nil {
			tempExecCtx := ExecCtx{
				ses:    ses,
				txnOpt: FeTxnOption{byRollback: true},
			}
			err := ses.GetTxnHandler().Rollback(&tempExecCtx)
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to rollback txn",
					zap.Error(err))
			}
		}

		//step B: cancel the query
		rt.killQuery(false, "")

		//step C: cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		rt.releaseRoutineCtx()

		//step D: clean protocol
		rt.protocol.Quit()
		rt.protocol = nil

		//step E: release the resources related to the session
		if ses != nil {
			ses.Close()
			rt.ses = nil
		}
	})
}

func (rt *Routine) migrateConnectionTo(ctx context.Context, req *query.MigrateConnToRequest) error {
	var err error
	rt.mc.migrateOnce.Do(func() {
		if !rt.mc.beginMigrate() {
			err = moerr.NewInternalErrorNoCtx("cannot start migrate as routine has been closed")
			return
		}
		defer rt.mc.endMigrate()
		ses := rt.getSession()
		err = Migrate(ses, req)
	})
	return err
}

func (rt *Routine) migrateConnectionFrom(resp *query.MigrateConnFromResponse) error {
	ses := rt.getSession()
	resp.DB = ses.GetDatabaseName()
	for _, st := range ses.GetPrepareStmts() {
		resp.PrepareStmts = append(resp.PrepareStmts, &query.PrepareStmt{
			Name:       st.Name,
			SQL:        st.Sql,
			ParamTypes: st.ParamTypes,
		})
	}
	return nil
}

func NewRoutine(ctx context.Context, protocol MysqlProtocol, parameters *config.FrontendParameters, rs goetty.IOSession) *Routine {
	ctx = trace.Generate(ctx) // fill span{trace_id} in ctx
	cancelRoutineCtx, cancelRoutineFunc := context.WithCancel(ctx)
	ri := &Routine{
		protocol:          protocol,
		cancelRoutineCtx:  cancelRoutineCtx,
		cancelRoutineFunc: cancelRoutineFunc,
		parameters:        parameters,
		printInfoOnce:     true,
		mc:                newMigrateController(),
	}
	protocol.UpdateCtx(cancelRoutineCtx)

	return ri
}
