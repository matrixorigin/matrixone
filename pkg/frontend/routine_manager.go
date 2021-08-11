package frontend

import (
	"errors"
	"fmt"
	"matrixone/pkg/config"
	"sync"

	"github.com/fagongzi/goetty"
)

type RoutineManager struct {
	rwlock  sync.RWMutex
	clients map[goetty.IOSession]*Routine

	//epoch gc handler
	pdHook *PDCallbackImpl

	pu *config.ParameterUnit
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	fmt.Println("Created a new Routine")
	IO := NewIOPackage(true)
	pro := NewMysqlClientProtocol(IO, nextConnectionID())
	exe := NewMysqlCmdExecutor()
	ses := NewSessionWithParameterUnit(rm.pu)
	routine := NewRoutine(rs, pro, exe, ses)
	routine.pdHook = rm.pdHook

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := routine.io.WriteAndFlush(pro.makePackets(hsV10pkt))
	if err != nil {
		return
	}

	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()

	rm.clients[rs] = routine
}

func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()

	fmt.Println("Closed a Routine")
	delete(rm.clients, rs)
}

func (rm *RoutineManager) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	rm.rwlock.RLock()
	routine, ok := rm.clients[rs]
	rm.rwlock.RUnlock()
	if !ok {
		return errors.New("routine does not exist")
	}

	fmt.Println("Handler Received:", msg)
	protocol := routine.protocol

	packet, ok := msg.(*Packet)
	protocol.sequenceId++
	if !ok {
		return errors.New("message is not Packet")
	}

	// finish handshake process
	if !routine.established {
		fmt.Println("HANDLE HANDSHAKE")
		payload := packet.Payload
		err := routine.handleHandshake(payload)
		if err != nil {
			return err
		}
		return nil
	}

	var err error
	var resp *Response

	req := routine.protocol.GetRequest(packet)
	if resp, err = routine.executor.ExecRequest(req); err != nil {
		fmt.Printf("routine execute request failed. error:%v \n", err)
		return nil
	}

	if resp != nil {
		if err = routine.protocol.SendResponse(resp); err != nil {
			fmt.Printf("routine send response failed %v. error:%v ", resp, err)
			return nil
		}
	}

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