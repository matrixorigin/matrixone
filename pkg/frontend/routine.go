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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol MysqlProtocol

	//execution layer
	executor CmdExecutor

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

func (rt *Routine) getCmdExecutor() CmdExecutor {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.executor
}

func (rt *Routine) getConnectionID() uint32 {
	return rt.getProtocol().ConnectionID()
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

func (rt *Routine) handleRequest(req *Request) error {
	var ses *Session
	var routineCtx context.Context
	var err error
	var resp *Response
	var quit bool
	reqBegin := time.Now()
	routineCtx = rt.getCancelRoutineCtx()
	parameters := rt.getParameters()
	mpi := rt.getProtocol()
	mpi.SetSequenceID(req.seq)
	cancelRequestCtx, cancelRequestFunc := context.WithTimeout(routineCtx, parameters.SessionTimeout.Duration)
	executor := rt.getCmdExecutor()
	executor.SetCancelFunc(cancelRequestFunc)
	rt.setCancelRequestFunc(cancelRequestFunc)
	ses = rt.getSession()
	ses.UpdateDebugString()
	tenant := ses.GetTenantInfo()
	tenantCtx := context.WithValue(cancelRequestCtx, defines.TenantIDKey{}, tenant.GetTenantID())
	tenantCtx = context.WithValue(tenantCtx, defines.UserIDKey{}, tenant.GetUserID())
	tenantCtx = context.WithValue(tenantCtx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())
	tenantCtx = trace.ContextWithSpanContext(tenantCtx, trace.SpanContextWithID(trace.TraceID(ses.uuid), trace.SpanKindSession))
	ses.SetRequestContext(tenantCtx)
	executor.SetSession(rt.getSession())

	rt.increaseCount(func() {
		metric.ConnectionCounter(ses.GetTenantInfo().GetTenant()).Inc()
	})

	if resp, err = executor.ExecRequest(tenantCtx, ses, req); err != nil {
		logErrorf(ses.GetDebugString(), "rt execute request failed. error:%v \n", err)
	}

	if resp != nil {
		if err = rt.getProtocol().SendResponse(tenantCtx, resp); err != nil {
			logErrorf(ses.GetDebugString(), "rt send response failed %v. error:%v ", resp, err)
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
		logErrorf(ses.GetDebugString(), "rollback the txn.")
		err = ses.TxnRollback()
		if err != nil {
			logErrorf(ses.GetDebugString(), "rollback txn failed.error:%v", err)
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
		//2.cancel txn ctx
		ses := rt.getSession()
		if ses != nil {
			txnHandler := ses.GetTxnHandler()
			if txnHandler != nil {
				txnHandler.cancelTxnCtx()
			}
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
		//step A: release the mempool related to the session
		ses := rt.getSession()
		if ses != nil {
			ses.Close()
		}

		//step B: cancel the query
		rt.killQuery(false, "")

		//step C: cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		rt.releaseRoutineCtx()

		//step D: clean protocol
		rt.protocol.Quit()
	})
}

func NewRoutine(ctx context.Context, protocol MysqlProtocol, executor CmdExecutor, parameters *config.FrontendParameters, rs goetty.IOSession) *Routine {
	ctx = trace.Generate(ctx) // fill span{trace_id} in ctx
	cancelRoutineCtx, cancelRoutineFunc := context.WithCancel(ctx)
	ri := &Routine{
		protocol:          protocol,
		executor:          executor,
		cancelRoutineCtx:  cancelRoutineCtx,
		cancelRoutineFunc: cancelRoutineFunc,
		parameters:        parameters,
	}

	return ri
}
