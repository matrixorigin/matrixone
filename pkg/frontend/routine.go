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
	"time"

	"go.uber.org/zap"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
)

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol MysqlProtocol

	//execution layer
	executor CmdExecutor

	//channel of request
	requestChan chan *Request

	cancelRoutineCtx  context.Context
	cancelRoutineFunc context.CancelFunc

	rs         goetty.IOSession
	parameters *config.FrontendParameters

	ses *Session
	// TODO: the initialization and closure of application in goetty should be clear in 0.7
	closeOnce sync.Once

	mu sync.Mutex
}

func (routine *Routine) GetCancelRoutineFunc() context.CancelFunc {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.cancelRoutineFunc
}

func (routine *Routine) GetCancelRoutineCtx() context.Context {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.cancelRoutineCtx
}

func (routine *Routine) GetClientProtocol() Protocol {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.protocol
}

func (routine *Routine) GetCmdExecutor() CmdExecutor {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.executor
}

func (routine *Routine) getConnID() uint32 {
	return routine.GetClientProtocol().ConnectionID()
}

func (routine *Routine) getParameters() *config.FrontendParameters {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.parameters
}

func (routine *Routine) SetSession(ses *Session) {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	routine.ses = ses
}

func (routine *Routine) GetSession() *Session {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.ses
}

func (routine *Routine) GetRequestChannel() chan *Request {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.requestChan
}

func (routine *Routine) getIOSession() goetty.IOSession {
	routine.mu.Lock()
	defer routine.mu.Unlock()
	return routine.rs
}

/*
After the handshake with the client is done, the routine goes into processing loop.
*/
func (routine *Routine) Loop(routineCtx context.Context) {
	var req *Request = nil
	var err error
	var resp *Response
	var counted bool
	var requestChan = routine.GetRequestChannel()
	var ses *Session
	rs := routine.getIOSession()
	defer func(rs goetty.IOSession) {
		if err := rs.Close(); err != nil {
			logErrorf(routine.GetSession().GetConciseProfile(), "failed to close io session", zap.Error(err))
		}
	}(rs)

	//session for the connection
	for {
		quit := false
		select {
		case <-routineCtx.Done():
			logDebugf(routine.GetSession().GetConciseProfile(), "-----cancel routine")
			quit = true
			if counted {
				metric.ConnectionCounter(routine.GetSession().GetTenantInfo().Tenant).Dec()
			}
		case req = <-requestChan:
			if !counted {
				counted = true
				metric.ConnectionCounter(routine.GetSession().GetTenantInfo().Tenant).Inc()
			}
		}

		if quit {
			break
		}

		reqBegin := time.Now()

		parameters := routine.getParameters()
		mpi := routine.GetClientProtocol().(*MysqlProtocolImpl)
		mpi.SetSequenceID(req.seq)

		cancelRequestCtx, cancelRequestFunc := context.WithTimeout(routineCtx, parameters.SessionTimeout.Duration)
		executor := routine.GetCmdExecutor()
		executor.(*MysqlCmdExecutor).setCancelRequestFunc(cancelRequestFunc)
		ses = routine.GetSession()
		ses.MakeProfile()
		tenant := ses.GetTenantInfo()
		tenantCtx := context.WithValue(cancelRequestCtx, defines.TenantIDKey{}, tenant.GetTenantID())
		tenantCtx = context.WithValue(tenantCtx, defines.UserIDKey{}, tenant.GetUserID())
		tenantCtx = context.WithValue(tenantCtx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())
		ses.SetRequestContext(tenantCtx)
		executor.PrepareSessionBeforeExecRequest(routine.GetSession())

		if resp, err = executor.ExecRequest(tenantCtx, req); err != nil {
			logErrorf(ses.GetConciseProfile(), "routine execute request failed. error:%v \n", err)
		}

		if resp != nil {
			if err = routine.GetClientProtocol().SendResponse(resp); err != nil {
				logErrorf(ses.GetConciseProfile(), "routine send response failed %v. error:%v ", resp, err)
			}
		}

		if !parameters.DisableRecordTimeElapsedOfSqlRequest {
			logDebugf(ses.GetConciseProfile(), "the time of handling the request %s", time.Since(reqBegin).String())
		}

		cancelRequestFunc()
	}

	ses = routine.GetSession()
	//ensure cleaning the transaction
	if ses != nil {
		logErrorf(ses.GetConciseProfile(), "rollback the txn.")
		err = ses.TxnRollback()
		if err != nil {
			logErrorf(ses.GetConciseProfile(), "rollback txn failed.error:%v", err)
		}
	}
}

/*
When the io is closed, the Quit will be called.
*/
func (routine *Routine) Quit() {
	routine.closeOnce.Do(func() {
		ses := routine.GetSession()
		if ses != nil {
			ses.Dispose()
		}
		routine.notifyClose()

		cancel := routine.GetCancelRoutineFunc()
		if cancel != nil {
			cancel()
		}

		proto := routine.GetClientProtocol()
		if proto != nil {
			proto.Quit()
		}
	})
}

/*
notify routine to quit
*/
func (routine *Routine) notifyClose() {
	executor := routine.GetCmdExecutor()
	if executor != nil {
		executor.Close()
	}
}

func (routine *Routine) notifyDone() {
	executor := routine.GetCmdExecutor()
	if executor != nil {
		cancal := executor.(*MysqlCmdExecutor).getCancelRequestFunc()
		if cancal != nil {
			cancal()
		}
	}
}

func NewRoutine(ctx context.Context, protocol MysqlProtocol, executor CmdExecutor, parameters *config.FrontendParameters, rs goetty.IOSession) *Routine {
	cancelRoutineCtx, cancelRoutineFunc := context.WithCancel(ctx)
	ri := &Routine{
		protocol:          protocol,
		executor:          executor,
		requestChan:       make(chan *Request, 1),
		cancelRoutineCtx:  cancelRoutineCtx,
		cancelRoutineFunc: cancelRoutineFunc,
		parameters:        parameters,
		rs:                rs,
	}
	rs.Ref()

	//async process request
	go ri.Loop(cancelRoutineCtx)

	return ri
}
