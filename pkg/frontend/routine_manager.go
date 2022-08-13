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
	"sync"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type RoutineManager struct {
	rwlock        sync.RWMutex
	clients       map[goetty.IOSession]*Routine
	pu            *config.ParameterUnit
	skipCheckUser bool
}

func (rm *RoutineManager) SetSkipCheckUser(b bool) {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	rm.skipCheckUser = b
}

func (rm *RoutineManager) GetSkipCheckUser() bool {
	rm.rwlock.RLock()
	defer rm.rwlock.RUnlock()
	return rm.skipCheckUser
}

func (rm *RoutineManager) getParameterUnit() *config.ParameterUnit {
	return rm.pu
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	pro := NewMysqlClientProtocol(nextConnectionID(), rs, int(rm.pu.SV.GetMaxBytesInOutbufToFlush()), rm.pu.SV)
	pro.SetSkipCheckUser(rm.GetSkipCheckUser())
	exe := NewMysqlCmdExecutor()
	exe.SetRoutineManager(rm)

	routine := NewRoutine(pro, exe, rm.pu)
	routine.SetRoutineMgr(rm)
	ses := NewSession(routine.protocol, routine.guestMmu, routine.mempool, rm.pu, gSysVariables)
	routine.SetSession(ses)
	pro.SetSession(ses)

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		panic(err)
	}

	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	rs.Ref()
	rm.clients[rs] = routine
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	defer delete(rm.clients, rs)

	rt, ok := rm.clients[rs]
	if !ok {
		return
	}
	logutil.Infof("will close iosession")
	rt.Quit()
}

/*
KILL statement
*/
func (rm *RoutineManager) killStatement(id uint64) error {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	var rt *Routine = nil
	for _, value := range rm.clients {
		if uint64(value.getConnID()) == id {
			rt = value
			break
		}
	}

	if rt != nil {
		logutil.Infof("will close the statement %d", id)
		rt.notifyClose()
	}
	return nil
}

func (rm *RoutineManager) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	rm.rwlock.RLock()
	routine, ok := rm.clients[rs]
	rm.rwlock.RUnlock()
	if !ok {
		return errors.New("routine does not exist")
	}

	protocol := routine.protocol.(*MysqlProtocolImpl)

	packet, ok := msg.(*Packet)

	protocol.m.Lock()
	protocol.sequenceId = uint8(packet.SequenceID + 1)
	var seq = protocol.sequenceId
	protocol.m.Unlock()
	if !ok {
		return errors.New("message is not Packet")
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		var err error
		msg, err = protocol.tcpConn.Read(goetty.ReadOptions{})
		if err != nil {
			return errors.New("read msg error")
		}

		packet, ok = msg.(*Packet)
		if !ok {
			return errors.New("message is not Packet")
		}

		protocol.sequenceId = uint8(packet.SequenceID + 1)
		seq = protocol.sequenceId
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !protocol.IsEstablished() {
		logutil.Infof("HANDLE HANDSHAKE")

		/*
			di := MakeDebugInfo(payload,80,8)
			logutil.Infof("RP[%v] Payload80[%v]",rs.RemoteAddr(),di)
		*/

		err := protocol.handleHandshake(payload)
		if err != nil {
			return err
		}
		protocol.SetEstablished()
		return nil
	}

	req := routine.protocol.GetRequest(payload)
	req.seq = seq
	routine.requestChan <- req

	return nil
}

func NewRoutineManager(pu *config.ParameterUnit) *RoutineManager {
	rm := &RoutineManager{
		clients: make(map[goetty.IOSession]*Routine),
		pu:      pu,
	}
	return rm
}
