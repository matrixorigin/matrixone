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
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"sync"
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

	//channel of notify
	notifyChan chan interface{}

	onceCloseNotifyChan sync.Once

	routineMgr *RoutineManager
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

/*
After the handshake with the client is done, the routine goes into processing loop.
*/
func (routine *Routine) Loop() {
	var req *Request = nil
	var err error
	var resp *Response
	defer routine.Quit()
	//session for the connection
	var ses *Session = nil
	for {
		quit := false
		select {
		case <-routine.notifyChan:
			logutil.Infof("-----routine quit")
			quit = true
		case req = <-routine.requestChan:
		}

		if quit {
			break
		}

		reqBegin := time.Now()

		mgr := routine.GetRoutineMgr()

		routine.protocol.(*MysqlProtocolImpl).sequenceId = req.seq

		if ses == nil {
			ses = NewSession(routine.protocol, routine.guestMmu, routine.mempool, mgr.getParameterUnit(), gSysVariables)
		}

		routine.executor.PrepareSessionBeforeExecRequest(ses)

		if resp, err = routine.executor.ExecRequest(req); err != nil {
			logutil.Errorf("routine execute request failed. error:%v \n", err)
		}

		if resp != nil {
			if err = routine.protocol.SendResponse(resp); err != nil {
				logutil.Errorf("routine send response failed %v. error:%v ", resp, err)
			}
		}

		if mgr.getParameterUnit().SV.GetRecordTimeElapsedOfSqlRequest() {
			logutil.Infof("connection id %d , the time of handling the request %s", routine.getConnID(), time.Since(reqBegin).String())
		}
	}
}

/*
When the io is closed, the Quit will be called.
*/
func (routine *Routine) Quit() {
	routine.notifyClose()

	routine.onceCloseNotifyChan.Do(func() {
		//logutil.Infof("---------notify close")
		close(routine.notifyChan)
	})

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

func NewRoutine(protocol MysqlProtocol, executor CmdExecutor, pu *config.ParameterUnit) *Routine {
	ri := &Routine{
		protocol:    protocol,
		executor:    executor,
		requestChan: make(chan *Request, 1),
		notifyChan:  make(chan interface{}),
		guestMmu:    guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu),
		mempool:     pu.Mempool,
	}

	//async process request
	go ri.Loop()

	return ri
}
