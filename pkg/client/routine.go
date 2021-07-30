package client

import (
	"fmt"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"matrixone/pkg/config"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

type Session struct {
	//variables
	User   string
	Dbname string

	//cmd from the client
	Cmd int

	//for test
	Mrs *MysqlResultSet

	GuestMmu *guest.Mmu
	Mempool  *mempool.Mempool

	sessionVars config.SystemVariables

	Pu *config.ParameterUnit
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

	//get the server
	GetPDCallback() pConfig.ContainerHeartbeatDataProcessor
}

//for test
var Rt Routine = nil

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

	//pd callback
	pdHook *PDCallbackImpl
}

func (ri *RoutineImpl) GetPDCallback() pConfig.ContainerHeartbeatDataProcessor {
	return ri.pdHook
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
	defer ri.Close()
	ri.Open()
	var err error
	var req *Request
	var resp *Response
	if err = ri.protocol.Handshake(); err !=nil{
		fmt.Printf("routine handshake failed. error:%v",err)
		return
	}

	for ri.IsOpened(){
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
				fmt.Printf("routine send response failed %v. error:%v ",resp,err)
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
		GuestMmu: guest.New(config.GlobalSystemVariables.GetGuestMmuLimitation(), config.HostMmu),
		Mempool:  mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()),
			int(config.GlobalSystemVariables.GetMempoolFactor())),
	}
}

func NewSessionWithParameterUnit(pu *config.ParameterUnit) *Session {
	return &Session{
		GuestMmu: guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu),
		Mempool:  mempool.New(int(pu.SV.GetMempoolMaxSize()), int(pu.SV.GetMempoolFactor())),
		Pu:       pu,
	}
}

func NewRoutine(protocol ClientProtocol, executor CmdExecutor, session *Session, pdCb *PDCallbackImpl) Routine {
	ri := &RoutineImpl{
		protocol: protocol,
		executor: executor,
		ses:session,
		pdHook: pdCb,
	}

	if protocol != nil{
		protocol.SetRoutine(ri)
	}

	if executor != nil{
		executor.SetRoutine(ri)
	}

	return ri
}