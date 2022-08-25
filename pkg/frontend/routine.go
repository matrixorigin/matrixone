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
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"time"
)

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol MysqlProtocol

	//execution layer
	executor CmdExecutor

	//for computation
	guestMmu *guest.Mmu
	mempool  *mempool.Mempool

	//channel of request
	requestChan chan *Request

	cancelRoutineCtx  context.Context
	cancelRoutineFunc context.CancelFunc

	routineMgr *RoutineManager

	ses *Session
}

func (routine *Routine) GetClientProtocol() Protocol {
	return routine.protocol
}

func (routine *Routine) GetCmdExecutor() CmdExecutor {
	return routine.executor
}

func (routine *Routine) getConnID() uint32 {
	return routine.protocol.ConnectionID()
}

func (routine *Routine) SetRoutineMgr(rtMgr *RoutineManager) {
	routine.routineMgr = rtMgr
}

func (routine *Routine) GetRoutineMgr() *RoutineManager {
	return routine.routineMgr
}

func (routine *Routine) SetSession(ses *Session) {
	routine.ses = ses
}

func (routine *Routine) GetSession() *Session {
	return routine.ses
}

/*
After the handshake with the client is done, the routine goes into processing loop.
*/
func (routine *Routine) Loop(routineCtx context.Context) {
	var req *Request = nil
	var err error
	var resp *Response
	defer routine.Quit()
	//session for the connection
	for {
		quit := false
		select {
		case <-routineCtx.Done():
			logutil.Infof("-----cancel routine")
			quit = true
		case req = <-routine.requestChan:
		}

		if quit {
			break
		}

		reqBegin := time.Now()

		mgr := routine.GetRoutineMgr()

		mpi := routine.protocol.(*MysqlProtocolImpl)
		mpi.sequenceId = req.seq

		cancelRequestCtx, cancelRequestFunc := context.WithCancel(routineCtx)
		routine.executor.(*MysqlCmdExecutor).setCancelRequestFunc(cancelRequestFunc)
		ses := routine.GetSession()
		ses.SetRequestContext(cancelRequestCtx)
		routine.executor.PrepareSessionBeforeExecRequest(routine.GetSession())

		if resp, err = routine.executor.ExecRequest(cancelRequestCtx, req); err != nil {
			logutil.Errorf("routine execute request failed. error:%v \n", err)
		}

		if resp != nil {
			if err = routine.protocol.SendResponse(resp); err != nil {
				logutil.Errorf("routine send response failed %v. error:%v ", resp, err)
			}
		}

		if !mgr.getParameterUnit().SV.DisableRecordTimeElapsedOfSqlRequest {
			logutil.Infof("connection id %d , the time of handling the request %s", routine.getConnID(), time.Since(reqBegin).String())
		}

		cancelRequestFunc()
	}
}

/*
When the io is closed, the Quit will be called.
*/
func (routine *Routine) Quit() {
	routine.notifyClose()

	if routine.cancelRoutineFunc != nil {
		routine.cancelRoutineFunc()
	}

	if routine.protocol != nil {
		routine.protocol.Quit()
	}
}

/*
notify routine to quit
*/
func (routine *Routine) notifyClose() {
	if routine.executor != nil {
		routine.executor.Close()
	}
}

func NewRoutine(ctx context.Context, protocol MysqlProtocol, executor CmdExecutor, pu *config.ParameterUnit) *Routine {
	cancelRoutineCtx, cancelRoutineFunc := context.WithCancel(ctx)
	ri := &Routine{
		protocol:          protocol,
		executor:          executor,
		requestChan:       make(chan *Request, 1),
		guestMmu:          guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu),
		mempool:           pu.Mempool,
		cancelRoutineCtx:  cancelRoutineCtx,
		cancelRoutineFunc: cancelRoutineFunc,
	}

	//async process request
	go ri.Loop(cancelRoutineCtx)

	return ri
}
