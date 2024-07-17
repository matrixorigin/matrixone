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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
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
	// LocalInfileRequest local infile message
	LocalInfileRequest
)

const (
	ConnectionInfoKey = "connection_info"
)

type Request struct {
	//the command type from the client
	cmd CommandType
	//the data from the client
	data interface{}
}

func (req *Request) GetData() interface{} {
	return req.data
}

func (req *Request) SetData(data interface{}) {
	req.data = data
}

func (req *Request) GetCmd() CommandType {
	return req.cmd
}

func (req *Request) SetCmd(cmd CommandType) {
	req.cmd = cmd
}

type Response struct {
	//the category of the response
	category int
	//the status of executing the peer request
	status uint16
	//the command type which generates the response
	cmd int
	//the data of the response
	data interface{}

	/*
		ok response
	*/
	affectedRows, lastInsertId uint64
	warnings                   uint16
	isIssue3482                bool
	loadLocalFile              string
}

func NewResponse(category int, affectedRows, lastInsertId uint64, warnings, status uint16, cmd int, d interface{}) *Response {
	return &Response{
		category:     category,
		affectedRows: affectedRows,
		lastInsertId: lastInsertId,
		warnings:     warnings,
		status:       status,
		cmd:          cmd,
		data:         d,
	}
}

func NewGeneralErrorResponse(cmd CommandType, status uint16, err error) *Response {
	return NewResponse(ErrorResponse, 0, 0, 0, status, int(cmd), err)
}

func NewGeneralOkResponse(cmd CommandType, status uint16) *Response {
	return NewResponse(OkResponse, 0, 0, 0, status, int(cmd), nil)
}

func NewOkResponse(affectedRows, lastInsertId uint64, warnings, status uint16, cmd int, d interface{}) *Response {
	return NewResponse(OkResponse, affectedRows, lastInsertId, warnings, status, cmd, d)
}

func (resp *Response) GetData() interface{} {
	return resp.data
}

func (resp *Response) SetData(data interface{}) {
	resp.data = data
}

func (resp *Response) GetStatus() uint16 {
	return resp.status
}

func (resp *Response) SetStatus(status uint16) {
	resp.status = status
}

func (resp *Response) GetCategory() int {
	return resp.category
}

func (resp *Response) SetCategory(category int) {
	resp.category = category
}

func (mp *MysqlProtocolImpl) UpdateCtx(ctx context.Context) {
	mp.ctx = ctx
}

func (mp *MysqlProtocolImpl) setQuit(b bool) bool {
	return mp.quit.Swap(b)
}

func (mp *MysqlProtocolImpl) GetSequenceId() uint8 {
	return uint8(mp.sequenceId.Load())
}

func (mp *MysqlProtocolImpl) getDebugStringUnsafe() string {
	if mp.tcpConn != nil {
		return fmt.Sprintf("connectionId %d|%s", mp.connectionID, mp.tcpConn.RemoteAddress())
	}
	return ""
}

func (mp *MysqlProtocolImpl) GetDebugString() string {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.getDebugStringUnsafe()
}

func (mp *MysqlProtocolImpl) GetSalt() []byte {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.salt
}

// SetSalt updates the salt value. This happens with proxy mode enabled.
func (mp *MysqlProtocolImpl) SetSalt(s []byte) {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.salt = s
}

func (mp *MysqlProtocolImpl) IsEstablished() bool {
	return mp.established.Load()
}

func (mp *MysqlProtocolImpl) SetEstablished() {
	getLogger().Debug("SWITCH ESTABLISHED to true", zap.String(ConnectionInfoKey, mp.GetDebugString()))
	mp.established.Store(true)
}

func (mp *MysqlProtocolImpl) IsTlsEstablished() bool {
	return mp.tlsEstablished.Load()
}

func (mp *MysqlProtocolImpl) SetTlsEstablished() {
	getLogger().Debug("SWITCH TLS_ESTABLISHED to true", zap.String(ConnectionInfoKey, mp.GetDebugString()))
	mp.tlsEstablished.Store(true)
}

func (mp *MysqlProtocolImpl) ConnectionID() uint32 {
	return mp.connectionID
}

// Quit kill tcpConn still connected.
// before calling NewMysqlClientProtocol, tcpConn.Connected() must be true
// please check goetty/application.go::doStart() and goetty/application.go::NewIOSession(...) for details
func (mp *MysqlProtocolImpl) safeQuit() {
	//if it was quit, do nothing
	if mp.setQuit(true) {
		return
	}
	if mp.tcpConn != nil {
		if err := mp.tcpConn.Disconnect(); err != nil {
			return
		}
	}
	//release salt
	if mp.salt != nil {
		mp.salt = nil
	}
}

func (mp *MysqlProtocolImpl) GetTcpConnection() *Conn {
	return mp.tcpConn
}

func (mp *MysqlProtocolImpl) Peer() string {
	tcp := mp.GetTcpConnection()
	if tcp == nil {
		return ""
	}
	return tcp.RemoteAddress()
}

func (mp *MysqlProtocolImpl) SendResponse(ctx context.Context, resp *Response) error {
	//move here to prohibit potential recursive lock
	var attachAbort string

	mp.m.Lock()
	defer mp.m.Unlock()

	switch resp.category {
	case OkResponse:
		s, ok := resp.data.(string)
		if !ok {
			return mp.sendOKPacket(resp.affectedRows, resp.lastInsertId, uint16(resp.status), resp.warnings, "")
		}
		return mp.sendOKPacket(resp.affectedRows, resp.lastInsertId, uint16(resp.status), resp.warnings, s)
	case EoFResponse:
		return mp.sendEOFPacket(0, uint16(resp.status))
	case ErrorResponse:
		err := resp.data.(error)
		if err == nil {
			return mp.sendOKPacket(0, 0, uint16(resp.status), 0, "")
		}
		switch myerr := err.(type) {
		case *moerr.Error:
			var code uint16
			if myerr.MySQLCode() != moerr.ER_UNKNOWN_ERROR {
				code = myerr.MySQLCode()
			} else {
				code = myerr.ErrorCode()
			}
			errMsg := myerr.Error()
			if attachAbort != "" {
				errMsg = fmt.Sprintf("%s\n%s", myerr.Error(), attachAbort)
			}
			return mp.sendErrPacket(code, myerr.SqlState(), errMsg)
		}
		errMsg := ""
		if attachAbort != "" {
			errMsg = fmt.Sprintf("%s\n%s", err, attachAbort)
		} else {
			errMsg = fmt.Sprintf("%v", err)
		}
		return mp.sendErrPacket(moerr.ER_UNKNOWN_ERROR, DefaultMySQLState, errMsg)
	case ResultResponse:
		mer := resp.data.(*MysqlExecutionResult)
		if mer == nil {
			return mp.sendOKPacket(0, 0, uint16(resp.status), 0, "")
		}
		if mer.Mrs() == nil {
			return mp.sendOKPacket(mer.AffectedRows(), mer.InsertID(), uint16(resp.status), mer.Warnings(), "")
		}
		return mp.sendResultSet(ctx, mer.Mrs(), resp.cmd, mer.Warnings(), uint16(resp.status))
	case LocalInfileRequest:
		s, _ := resp.data.(string)
		return mp.WriteLocalInfileRequest(s)
	default:
		return moerr.NewInternalError(ctx, "unsupported response:%d ", resp.category)
	}
}
