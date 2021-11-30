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
	"github.com/fagongzi/goetty"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"net"
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

	//io data
	io goetty.IOSession

	//the related session
	ses *Session

	// whether the handshake succeeded
	established bool

	// current username
	user string

	// current db name
	db string

	//epoch gc handler
	pdHook *PDCallbackImpl

	//channel of request
	requestChan chan *Request

	//channel of notify
	notifyChan chan interface{}
}

func (routine *Routine) GetClientProtocol() Protocol {
	return routine.protocol
}

func (routine *Routine) GetCmdExecutor() CmdExecutor {
	return routine.executor
}

func (routine *Routine) GetSession() *Session {
	return routine.ses
}

func (routine *Routine) GetPDCallback() pConfig.ContainerHeartbeatDataProcessor {
	return routine.pdHook
}

func (routine *Routine) getConnID() uint32 {
	return routine.protocol.ConnectionID()
}

/*
After the handshake with the client is done, the routine goes into processing loop.
 */
func (routine *Routine) Loop() {
	var req *Request = nil
	var err error
	var resp *Response
	for{
		quit := false
		select {
		case <- routine.notifyChan:
			quit = true
		case req = <- routine.requestChan:
		}

		if quit{
			break
		}

		reqBegin := time.Now()
		if resp, err = routine.executor.ExecRequest(req); err != nil {
			logutil.Errorf("routine execute request failed. error:%v \n", err)
		}

		if resp != nil {
			if err = routine.protocol.SendResponse(resp); err != nil {
				logutil.Errorf("routine send response failed %v. error:%v ", resp, err)
			}
		}

		if routine.ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
			logutil.Infof("connection id %d , the time of handling the request %s", routine.getConnID(), time.Since(reqBegin).String())
		}
	}
}

/*
When the io is closed, the Quit will be called.
 */
func (routine *Routine) Quit() {
	if routine.io != nil {
		_ = routine.io.Close()
	}
	close(routine.notifyChan)
	if routine.executor != nil {
		routine.executor.Close()
	}
}

// Peer gets the address [Host:Port] of the client
func (routine *Routine) Peer() (string, string) {
	addr := routine.io.RemoteAddr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logutil.Errorf("get peer host:port failed. error:%v ", err)
		return "failed", "0"
	}
	return host, port
}

func (routine *Routine) ChangeDB(db string) error {
	//TODO: check meta data
	if _, err := routine.ses.Pu.StorageEngine.Database(db); err != nil {
		//echo client. no such database
		return NewMysqlError(ER_BAD_DB_ERROR, db)
	}
	oldDB := routine.db
	routine.db = db

	logutil.Infof("User %s change database from [%s] to [%s]\n", routine.user, oldDB, routine.db)

	return nil
}

func (routine *Routine) Establish(proto MysqlProtocol) {
	pro := proto.(*MysqlProtocolImpl)
	routine.user = pro.username
	routine.db = pro.database
	logutil.Infof("SWITCH ESTABLISHED to true")
	routine.established = true
}

func NewRoutine(rs goetty.IOSession, protocol MysqlProtocol, executor CmdExecutor, session *Session) *Routine {
	ri := &Routine{
		protocol:    protocol,
		executor:    executor,
		ses:         session,
		io:          rs,
		established: false,
		requestChan: make(chan *Request,1),
		notifyChan: make(chan interface{}),
	}

	if protocol != nil {
		protocol.SetRoutine(ri)
	}

	if executor != nil {
		executor.SetRoutine(ri)
	}

	//async process request
	go ri.Loop()

	return ri
}
