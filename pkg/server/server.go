package server

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
	"net"
	"sync"
	"sync/atomic"
)

//ID counter for the new connection
var initConnectionID uint32 = 1000

//host memory
var HostMmu *host.Mmu = nil

//Storage Engine
var StorageEngine engine.Engine

//Cluster Nodes
var ClusterNodes metadata.Nodes

//the Server is an abstract of handling connections from clients repeatedly.
type Server interface {
	//handle something repeatedly
	Loop()

	//quit the execution loop
	Quit()
}

type ServerImpl struct {
	CloseFlag

	//mutex for shared data structure
	rwlock sync.RWMutex

	//connection listener
	listener net.Listener

	//clients who has connected with server
	clients map[uint64] Routine

	//config
}

//allocate resources for processing the connection
func (si *ServerImpl) newConnection(cnn net.Conn) Routine {
	var IO IOPackage = NewIOPackage(cnn,defaultReadBufferSize,defaultWriteBufferSize,true)
	pro := NewMysqlClientProtocol(IO,nextConnectionID())
	exe := NewMysqlCmdExecutor()
	ses := NewSession()
	rt := NewRoutine(pro,exe,ses)

	si.rwlock.Lock()
	si.clients[uint64(rt.ID())] = rt
	si.rwlock.Unlock()

	return rt
}

//handle the connection
func (si *ServerImpl) handleConnection(routine Routine) {
	routine.Loop()

	//the routine has exited
	si.rwlock.Lock()
	delete(si.clients, uint64(routine.ID()))
	si.rwlock.Unlock()
}

func (si *ServerImpl) Loop() {
	for si.isOpened(){
		cnn,err := si.listener.Accept()
		if err != nil{
			fmt.Printf("server listen failed. error:%v",err)
			break
		}

		rt := si.newConnection(cnn)
		go si.handleConnection(rt)
	}
}

func (si *ServerImpl) Quit() {
	si.rwlock.Lock()
	defer si.rwlock.Unlock()

	si.Close()
	if si.listener != nil{
		if err := si.listener.Close(); err != nil{
			fmt.Printf("close listener failed. error:%v ",err)
			si.listener = nil
		}
	}
}

func nextConnectionID()uint32{
	return atomic.AddUint32(&initConnectionID,1)
}

func NewServer(address string)Server{
	var err error
	svr := &ServerImpl{
		clients : make(map[uint64]Routine),
	}

	if svr.listener,err = net.Listen("tcp",address);err!=nil{
		fmt.Printf("server can not listen on the address - %s.error:%v",address,err)
		return nil
	}
	return svr
}