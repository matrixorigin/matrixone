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
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/fagongzi/goetty/v2"

	"github.com/matrixorigin/matrixone/pkg/logutil"
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

type Request struct {
	//the command type from the client
	cmd CommandType
	// sequence num
	seq uint8
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

type Protocol interface {
	IsEstablished() bool

	SetEstablished()

	// GetRequest gets Request from Packet
	GetRequest(payload []byte) *Request

	// SendResponse sends a response to the client for the application request
	SendResponse(context.Context, *Response) error

	// ConnectionID the identity of the client
	ConnectionID() uint32

	// Peer gets the address [Host:Port,Host:Port] of the client and the server
	Peer() string

	GetDatabaseName() string

	SetDatabaseName(string)

	GetUserName() string

	SetUserName(string)

	GetSequenceId() uint8

	SetSequenceID(value uint8)

	GetDebugString() string

	GetTcpConnection() goetty.IOSession

	GetCapability() uint32

	SetCapability(uint32)

	GetConnectAttrs() map[string]string

	IsTlsEstablished() bool

	SetTlsEstablished()

	HandleHandshake(ctx context.Context, payload []byte) (bool, error)

	Authenticate(ctx context.Context) error

	SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error

	Quit()

	incDebugCount(int)

	resetDebugCount() []uint64

	UpdateCtx(context.Context)
}

type ProtocolImpl struct {
	m sync.Mutex

	io IOPackage

	tcpConn goetty.IOSession

	quit atomic.Bool

	//random bytes
	salt []byte

	//the id of the connection
	connectionID uint32

	// whether the handshake succeeded
	established atomic.Bool

	// whether the tls handshake succeeded
	tlsEstablished atomic.Bool

	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId atomic.Uint32

	//for debug
	debugCount [16]uint64

	ctx context.Context
}

func (pi *ProtocolImpl) UpdateCtx(ctx context.Context) {
	pi.ctx = ctx
}

func (pi *ProtocolImpl) incDebugCount(i int) {
	if i >= 0 && i < len(pi.debugCount) {
		atomic.AddUint64(&pi.debugCount[i], 1)
	}
}

func (pi *ProtocolImpl) resetDebugCount() []uint64 {
	ret := make([]uint64, len(pi.debugCount))
	for i := 0; i < len(pi.debugCount); i++ {
		ret[i] = atomic.LoadUint64(&pi.debugCount[i])
	}
	return ret
}

func (pi *ProtocolImpl) setQuit(b bool) bool {
	return pi.quit.Swap(b)
}

func (pi *ProtocolImpl) GetSequenceId() uint8 {
	return uint8(pi.sequenceId.Load())
}

func (pi *ProtocolImpl) getDebugStringUnsafe() string {
	if pi.tcpConn != nil {
		return fmt.Sprintf("connectionId %d|%s", pi.connectionID, pi.tcpConn.RemoteAddress())
	}
	return ""
}

func (pi *ProtocolImpl) GetDebugString() string {
	pi.m.Lock()
	defer pi.m.Unlock()
	return pi.getDebugStringUnsafe()
}

func (pi *ProtocolImpl) GetSalt() []byte {
	pi.m.Lock()
	defer pi.m.Unlock()
	return pi.salt
}

// SetSalt updates the salt value. This happens with proxy mode enabled.
func (pi *ProtocolImpl) SetSalt(s []byte) {
	pi.m.Lock()
	defer pi.m.Unlock()
	pi.salt = s
}

func (pi *ProtocolImpl) IsEstablished() bool {
	return pi.established.Load()
}

func (pi *ProtocolImpl) SetEstablished() {
	logDebugf(pi.GetDebugString(), "SWITCH ESTABLISHED to true")
	pi.established.Store(true)
}

func (pi *ProtocolImpl) IsTlsEstablished() bool {
	return pi.tlsEstablished.Load()
}

func (pi *ProtocolImpl) SetTlsEstablished() {
	logutil.Debugf("SWITCH TLS_ESTABLISHED to true")
	pi.tlsEstablished.Store(true)
}

func (pi *ProtocolImpl) ConnectionID() uint32 {
	return pi.connectionID
}

// Quit kill tcpConn still connected.
// before calling NewMysqlClientProtocol, tcpConn.Connected() must be true
// please check goetty/application.go::doStart() and goetty/application.go::NewIOSession(...) for details
func (pi *ProtocolImpl) Quit() {
	//if it was quit, do nothing
	if pi.setQuit(true) {
		return
	}
	if pi.tcpConn != nil {
		if err := pi.tcpConn.Disconnect(); err != nil {
			return
		}
	}
	//release salt
	if pi.salt != nil {
		pi.salt = nil
	}
}

func (pi *ProtocolImpl) GetTcpConnection() goetty.IOSession {
	return pi.tcpConn
}

func (pi *ProtocolImpl) Peer() string {
	tcp := pi.GetTcpConnection()
	if tcp == nil {
		return ""
	}
	return tcp.RemoteAddress()
}

func (mp *MysqlProtocolImpl) GetRequest(payload []byte) *Request {
	req := &Request{
		cmd:  CommandType(payload[0]),
		data: payload[1:],
	}

	return req
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
		return mp.sendLocalInfileRequest(s)
	default:
		return moerr.NewInternalError(ctx, "unsupported response:%d ", resp.category)
	}
}

func (mp *MysqlProtocolImpl) DisableAutoFlush() {
	mp.disableAutoFlush = true
}

func (mp *MysqlProtocolImpl) EnableAutoFlush() {
	mp.disableAutoFlush = false
}

func (mp *MysqlProtocolImpl) Flush() error {
	return nil
}

var _ MysqlProtocol = &FakeProtocol{}

const (
	fakeConnectionID uint32 = math.MaxUint32
)

// FakeProtocol works for the background transaction that does not use the network protocol.
type FakeProtocol struct {
	username string
	database string
	ioses    goetty.IOSession
}

func (fp *FakeProtocol) UpdateCtx(ctx context.Context) {

}

func (fp *FakeProtocol) GetCapability() uint32 {
	return DefaultCapability
}

func (fp *FakeProtocol) SetCapability(uint32) {

}

func (fp *FakeProtocol) IsTlsEstablished() bool {
	return true
}

func (fp *FakeProtocol) SetTlsEstablished() {

}

func (fp *FakeProtocol) HandleHandshake(ctx context.Context, payload []byte) (bool, error) {
	return false, nil
}

func (fp *FakeProtocol) Authenticate(ctx context.Context) error {
	return nil
}

func (fp *FakeProtocol) GetTcpConnection() goetty.IOSession {
	return fp.ioses
}

func (fp *FakeProtocol) GetDebugString() string {
	return "fake protocol"
}

func (fp *FakeProtocol) GetSequenceId() uint8 {
	return 0
}

func (fp *FakeProtocol) SetSequenceID(value uint8) {
}

func (fp *FakeProtocol) GetConnectAttrs() map[string]string {
	return nil
}

func (fp *FakeProtocol) SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	return nil
}

func (fp *FakeProtocol) ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (fp *FakeProtocol) ParseExecuteData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (fp *FakeProtocol) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (fp *FakeProtocol) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (fp *FakeProtocol) SendColumnDefinitionPacket(ctx context.Context, column Column, cmd int) error {
	return nil
}

func (fp *FakeProtocol) SendColumnCountPacket(count uint64) error {
	return nil
}

func (fp *FakeProtocol) SendEOFPacketIf(warnings uint16, status uint16) error {
	return nil
}

func (fp *FakeProtocol) sendOKPacket(affectedRows uint64, lastInsertId uint64, status uint16, warnings uint16, message string) error {
	return nil
}

func (fp *FakeProtocol) sendEOFOrOkPacket(warnings uint16, status uint16) error {
	return nil
}

func (fp *FakeProtocol) ResetStatistics() {}

func (fp *FakeProtocol) GetStats() string {
	return ""
}

func (fp *FakeProtocol) CalculateOutTrafficBytes(reset bool) (int64, int64) { return 0, 0 }

func (fp *FakeProtocol) IsEstablished() bool {
	return true
}

func (fp *FakeProtocol) SetEstablished() {}

func (fp *FakeProtocol) GetRequest(payload []byte) *Request {
	return nil
}

func (fp *FakeProtocol) SendResponse(ctx context.Context, resp *Response) error {
	return nil
}

func (fp *FakeProtocol) ConnectionID() uint32 {
	return fakeConnectionID
}

func (fp *FakeProtocol) Peer() string {
	return "0.0.0.0:0"
}

func (fp *FakeProtocol) GetDatabaseName() string {
	return fp.database
}

func (fp *FakeProtocol) SetDatabaseName(s string) {
	fp.database = s
}

func (fp *FakeProtocol) GetUserName() string {
	return fp.username
}

func (fp *FakeProtocol) SetUserName(s string) {
	fp.username = s
}

func (fp *FakeProtocol) Quit() {}

func (fp *FakeProtocol) sendLocalInfileRequest(filename string) error {
	return nil
}

func (fp *FakeProtocol) incDebugCount(int) {}

func (fp *FakeProtocol) resetDebugCount() []uint64 {
	return nil
}

func (fp *FakeProtocol) DisableAutoFlush() {
}

func (fp *FakeProtocol) EnableAutoFlush() {
}

func (fp *FakeProtocol) Flush() error {
	return nil
}
