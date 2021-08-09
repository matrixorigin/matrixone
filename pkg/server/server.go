package server

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
	"net"
	"sync"
	"sync/atomic"
)

//ID counter for the new connection
var initConnectionID uint32 = 1000

// Server the Server is an abstract of handling connections from clients repeatedly.
type Server interface {
	// Loop handle something repeatedly
	Loop()

	// Quit quit the execution loop
	Quit()
}

type ServerImpl struct {
	client.CloseFlag

	//mutex for shared data structure
	rwlock sync.RWMutex

	//connection listener
	listener net.Listener

	//clients who has connected with server
	clients map[uint64]client.Routine

	//config
	address string

	//epoch gc handler
	pdHook *client.PDCallbackImpl

	pu *config.ParameterUnit
}

//allocate resources for processing the connection
func (si *ServerImpl) newConnection(cnn net.Conn) client.Routine {
	var IO client.IOPackage = client.NewIOPackage(cnn, client.DefaultReadBufferSize, client.DefaultWriteBufferSize, true)
	pro := client.NewMysqlClientProtocol(IO, nextConnectionID())
	exe := NewMysqlCmdExecutor()
	ses := client.NewSessionWithParameterUnit(si.pu)
	rt := client.NewRoutine(pro, exe, ses, si.pdHook)

	si.rwlock.Lock()
	si.clients[uint64(rt.ID())] = rt
	si.rwlock.Unlock()

	return rt
}

//handle the connection
func (si *ServerImpl) handleConnection(routine client.Routine) {
	routine.Loop()

	//the routine has exited
	si.rwlock.Lock()
	delete(si.clients, uint64(routine.ID()))
	si.rwlock.Unlock()
}

func (si *ServerImpl) Loop() {
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("Server Listening on : %s \n", si.address)
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	for si.IsOpened() {
		if !si.pdHook.CanAcceptSomething() {
			fmt.Printf("The Heartbeat From PDLeader Is Timeout. The Server Go Offline.")
			break
		}
		cnn, err := si.listener.Accept()
		if err != nil {
			fmt.Printf("server listen failed. error:%v", err)
			break
		}

		rt := si.newConnection(cnn)
		go si.handleConnection(rt)
	}
	fmt.Printf("Server Quit\n")
}

func (si *ServerImpl) Quit() {
	si.rwlock.Lock()
	defer si.rwlock.Unlock()

	si.Close()
	if si.listener != nil {
		if err := si.listener.Close(); err != nil {
			fmt.Printf("close listener failed. error:%v ", err)
			si.listener = nil
		}
	}

	for _, client := range si.clients {
		client.Quit()
	}
}

func nextConnectionID() uint32 {
	return atomic.AddUint32(&initConnectionID, 1)
}

func NewServer(address string, pu *config.ParameterUnit, pdHook *client.PDCallbackImpl) Server {
	var err error
	svr := &ServerImpl{
		clients: make(map[uint64]client.Routine),
		address: address,
		pdHook: pdHook,
		pu: pu,
	}

	if svr.listener, err = net.Listen("tcp", address); err != nil {
		fmt.Printf("server can not listen on the address - %s.error:%v", address, err)
		return nil
	}
	return svr
}
