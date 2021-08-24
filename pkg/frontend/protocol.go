package frontend

import (
	"fmt"
	"sync"
)

// Response Categories
const (
	// OkResponse OK message
	OkResponse = iota
	// ErrorResponse Error message
	ErrorResponse
	// EoFResponse EOF message
	EoFResponse
	// ResultResponse result message
	ResultResponse
)

type Request struct {
	//the command type from the client
	cmd int
	//the data from the client
	data interface{}
}

func (req *Request) GetData() interface{} {
	return req.data
}

func (req *Request) SetData(data interface{}) {
	req.data = data
}

func (req *Request) GetCmd() int {
	return req.cmd
}

func (req *Request) SetCmd(cmd int) {
	req.cmd = cmd
}

type Response struct {
	//the category of the response
	category int
	//the status of executing the peer request
	status int
	//the command type which generates the response
	cmd int
	//the data of the response
	data interface{}
}

func NewResponse(category,status,cmd int,d interface{})*Response {
	return &Response{
		category: category,
		status:   status,
		cmd:      cmd,
		data:     d,
	}
}

func (resp *Response) GetData() interface{} {
	return resp.data
}

func (resp *Response) SetData(data interface{}) {
	resp.data = data
}

func (resp *Response) GetStatus() int {
	return resp.status
}

func (resp *Response) SetStatus(status int) {
	resp.status = status
}

func (resp *Response) GetCategory() int {
	return resp.category
}

func (resp *Response) SetCategory(category int) {
	resp.category = category
}

type Protocol interface {
	// GetRequest gets Request from Packet
	GetRequest(payload []byte) *Request

	// SendResponse sends a response to the client for the application request
	SendResponse(*Response) error

	// ConnectionID the identity of the client
	ConnectionID()uint32

	// SetRoutine sets RoutineInterface
	SetRoutine(*Routine)
}

type ProtocolImpl struct{
	//mutex
	lock sync.Mutex

	io IOPackage

	//random bytes
	salt []byte

	//the id of the connection
	connectionID uint32

	//routine
	routine *Routine
}

func (cpi *ProtocolImpl) ConnectionID() uint32 {
	return cpi.connectionID
}

func (cpi *ProtocolImpl) SetRoutine(r *Routine)  {
	cpi.routine = r
}

func (cpi *ProtocolImpl) GetLock() sync.Locker {
	return &cpi.lock
}

func (mp *MysqlProtocol) GetRequest(payload []byte) *Request {
	req := &Request{
		cmd:  int(payload[0]),
		data: payload[1:],
	}

	return req
}

func (mp *MysqlProtocol) SendResponse(resp *Response) error {
	mp.GetLock().Lock()
	defer mp.GetLock().Unlock()

	switch resp.category {
	case OkResponse:
		return mp.sendOKPacket(0, 0, uint16(resp.status), 0, "")
	case EoFResponse:
		return mp.sendEOFPacket(0, uint16(resp.status))
	case ErrorResponse:
		err := resp.data.(error)
		if err == nil {
			return mp.sendOKPacket(0, 0, uint16(resp.status), 0, "")
		}
		switch myerr := err.(type) {
		case *MysqlError:
			return mp.sendErrPacket(myerr.ErrorCode, myerr.SqlState, myerr.Error())
		}
		return mp.sendErrPacket(ER_UNKNOWN_ERROR, DefaultMySQLState, fmt.Sprintf("unkown error:%v", err))
	case ResultResponse:
		mer := resp.data.(*MysqlExecutionResult)
		if mer == nil {
			return mp.sendOKPacket(0, 0, uint16(resp.status), 0, "")
		}
		if mer.Mrs() == nil {
			return mp.sendOKPacket(mer.AffectedRows(), mer.InsertID(), uint16(resp.status), mer.Warnings(), "")
		}
		return mp.sendResultSet(mer.Mrs(), resp.cmd, mer.Warnings(), uint16(resp.status))
	default:
		return fmt.Errorf("unsupported response:%d ", resp.category)
	}
}
