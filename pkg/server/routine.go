package server

import (
	"fmt"
	"matrixone/pkg/config"
)

type Session interface {

}

type SessionImpl struct {
	Session
	//variables
	user string
	dbname string
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
	ses Session
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

		if err = ri.protocol.SendResponse(resp);err != nil{
			fmt.Printf("routine send response failed. error:%v ",err)
			break
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

func NewSession()Session{
	return &SessionImpl{}
}

func NewRoutine(protocol ClientProtocol,executor CmdExecutor,session Session)Routine{
	return &RoutineImpl{
		protocol: protocol,
		executor: executor,
		ses:session,
	}
}