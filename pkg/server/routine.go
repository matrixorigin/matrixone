package server

import (
	"fmt"
	"matrixone/pkg/config"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

type Session struct {
	//variables
	user string
	dbname string

	guestMmu *guest.Mmu
	mempool *mempool.Mempool

	sessionVars config.SystemVariables
}

//the routine is an abstract of handling something repeatedly.
type Routine interface {
	//the identity
	ID()uint32

	//handle something repeatedly
	Loop()

	//quit the execution routine
	Quit()

	//get the session
	GetSession()*Session

	//get the clientprotocol
	GetClientProtocol()ClientProtocol

	//get the cmdexecutor
	GetCmdExecutor()CmdExecutor
}

//handle requests repeatedly.
//reads requests from the protocol layer,
//uses the executor to handle requests, and response them.
type RoutineImpl struct {
	CloseFlag
	//protocol layer
	protocol ClientProtocol

	//execution layer
	executor CmdExecutor

	//the related session
	ses *Session
}

func (ri *RoutineImpl) GetClientProtocol() ClientProtocol {
	return ri.protocol
}

func (ri *RoutineImpl) GetCmdExecutor() CmdExecutor {
	return ri.executor
}

func (ri *RoutineImpl) ID()uint32  {
	return ri.protocol.ConnectionID()
}

func (ri *RoutineImpl) Loop() {
	defer ri.protocol.Close()
	defer ri.executor.Close()
	ri.Open()
	var err error
	var req *Request
	var resp *Response
	if err = ri.protocol.Handshake(); err !=nil{
		fmt.Printf("routine handshake failed. error:%v",err)
		return
	}

	for ri.isOpened(){
		if req,err = ri.protocol.ReadRequest();err!=nil{
			fmt.Printf("routine read request failed. error:%v ",err)
			break
		}

		if resp,err = ri.executor.ExecRequest(req); err!=nil{
			fmt.Printf("routine execute request failed. error:%v ",err)
			break
		}

		if resp != nil {
			if err = ri.protocol.SendResponse(resp);err != nil{
				fmt.Printf("routine send response failed. error:%v ",err)
				break
			}
		}

		//mysql client protocol: quit command
		if _,ok := ri.protocol.(*MysqlClientProtocol); ok{
			if uint8(req.GetCmd()) == COM_QUIT{
				break
			}
		}
	}
}

func (ri *RoutineImpl) Quit() {
	ri.Close()
}

func (ri *RoutineImpl) GetSession() *Session {
	return ri.ses
}

func NewSession()*Session{
	return &Session{
		guestMmu: guest.New(1<<40, HostMmu),
		mempool: mempool.New(1<<40, 8),
	}
}

func NewRoutine(protocol ClientProtocol,executor CmdExecutor,session *Session)Routine{
	ri := &RoutineImpl{
		protocol: protocol,
		executor: executor,
		ses:session,
	}

	protocol.SetRoutine(ri)
	executor.SetRoutine(ri)

	return ri
}