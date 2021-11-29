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
	"errors"
	"fmt"
	"github.com/fagongzi/goetty"
	"matrixone/pkg/config"
	"matrixone/pkg/logutil"
	"sync"
)

type RoutineManager struct {
	rwlock  sync.RWMutex
	clients map[goetty.IOSession]*Routine

	//epoch gc handler
	pdHook *PDCallbackImpl

	pu *config.ParameterUnit
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	defer func() {
		if err := recover(); err != nil {
			logutil.Errorf("create routine manager failed. err:%v",err)
		}
	}()
	IO := NewIOPackage(true)
	pro := NewMysqlClientProtocol(IO, nextConnectionID())
	exe := NewMysqlCmdExecutor()
	ses := NewSessionWithParameterUnit(rm.pu)
	routine := NewRoutine(rs, pro, exe, ses)
	routine.pdHook = rm.pdHook

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		panic(err)
	}

	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()

	rm.clients[rs] = routine
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	defer func() {
		if err := recover(); err != nil {
			logutil.Errorf("close routine manager failed. err:%v",err)
		}
	}()
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	defer delete(rm.clients, rs)

	rt, ok :=rm.clients[rs]
	if !ok {
		return
	}
	logutil.Infof("will close iosession")
	rt.Quit()
}

func (rm *RoutineManager) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	defer func() {
		if err := recover(); err != nil {
			logutil.Errorf("handle message failed. err:%v",err)
		}
	}()
	if rm.pu.SV.GetRejectWhenHeartbeatFromPDLeaderIsTimeout() {
		if !rm.pdHook.CanAcceptSomething() {
			fmt.Printf("The Heartbeat From PDLeader Is Timeout. The Server Go Offline.\n")
			return errors.New("The Heartbeat From PDLeader Is Timeout. The Server Reject Connection.\n")
		}
	}

	rm.rwlock.RLock()
	routine, ok := rm.clients[rs]
	rm.rwlock.RUnlock()
	if !ok {
		return errors.New("routine does not exist")
	}

	protocol := routine.protocol.(*MysqlProtocolImpl)

	packet, ok := msg.(*Packet)
	protocol.sequenceId = uint8(packet.SequenceID + 1)
	if !ok {
		return errors.New("message is not Packet")
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		var err error
		msg, err = routine.io.Read()
		if err != nil {
			return errors.New("read msg error")
		}

		packet, ok = msg.(*Packet)
		if !ok {
			return errors.New("message is not Packet")
		}

		protocol.sequenceId = uint8(packet.SequenceID + 1)
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !routine.established {
		logutil.Infof("HANDLE HANDSHAKE")

		/*
		di := MakeDebugInfo(payload,80,8)
		logutil.Infof("RP[%v] Payload80[%v]",rs.RemoteAddr(),di)
		*/

		err := protocol.handleHandshake(payload)
		if err != nil {
			return err
		}
		routine.Establish(protocol)
		return nil
	}

	req := routine.protocol.GetRequest(payload)
	routine.requestChan <- req

	return nil
}

func NewRoutineManager(pu *config.ParameterUnit, pdHook *PDCallbackImpl) *RoutineManager {
	rm := &RoutineManager{
		clients: make(map[goetty.IOSession]*Routine),

		pdHook: pdHook,
		pu:     pu,
	}
	return rm
}
