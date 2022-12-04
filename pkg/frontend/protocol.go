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
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

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

func NewResponse(category int, status uint16, cmd int, d interface{}) *Response {
	return &Response{
		category: category,
		status:   status,
		cmd:      cmd,
		data:     d,
	}
}

func NewGeneralErrorResponse(cmd CommandType, err error) *Response {
	return NewResponse(ErrorResponse, 0, int(cmd), err)
}

func NewGeneralOkResponse(cmd CommandType) *Response {
	return NewResponse(OkResponse, 0, int(cmd), nil)
}

func NewOkResponse(affectedRows, lastInsertId uint64, warnings, status uint16, cmd int, d interface{}) *Response {
	resp := &Response{
		category:     OkResponse,
		status:       status,
		cmd:          cmd,
		data:         d,
		affectedRows: affectedRows,
		lastInsertId: lastInsertId,
		warnings:     warnings,
	}

	return resp
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
	profile
	IsEstablished() bool

	SetEstablished()

	// GetRequest gets Request from Packet
	GetRequest(payload []byte) *Request

	// SendResponse sends a response to the client for the application request
	SendResponse(context.Context, *Response) error

	// ConnectionID the identity of the client
	ConnectionID() uint32

	// Peer gets the address [Host:Port,Host:Port] of the client and the server
	Peer() (string, string, string, string)

	GetDatabaseName() string

	SetDatabaseName(string)

	GetUserName() string

	SetUserName(string)

	// Quit
	Quit()

	SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error
}

type ProtocolImpl struct {
	//mutex
	lock sync.Mutex

	io IOPackage

	tcpConn goetty.IOSession

	//random bytes
	salt []byte

	//the id of the connection
	connectionID uint32

	// whether the handshake succeeded
	established atomic.Bool

	// whether the tls handshake succeeded
	tlsEstablished atomic.Bool

	profiles [8]string
}

func (cpi *ProtocolImpl) makeProfile(profileTyp profileType) {
	var mask profileType
	var profile string
	for i := uint8(0); i < 8; i++ {
		mask = 1 << i
		switch mask & profileTyp {
		case profileTypeConnectionWithId:
			if cpi.tcpConn != nil {
				profile = fmt.Sprintf("connectionId %d", cpi.connectionID)
			}
		case profileTypeConnectionWithIp:
			if cpi.tcpConn != nil {
				client := cpi.tcpConn.RemoteAddress()
				profile = "client " + client
			}
		default:
			profile = ""
		}
		cpi.profiles[i] = profile
	}
}

func (cpi *ProtocolImpl) getProfile(profileTyp profileType) string {
	var mask profileType
	sb := bytes.Buffer{}
	for i := uint8(0); i < 8; i++ {
		mask = 1 << i
		if mask&profileTyp != 0 {
			if sb.Len() != 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(cpi.profiles[i])
		}
	}
	return sb.String()
}

func (cpi *ProtocolImpl) MakeProfile() {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	cpi.makeProfile(profileTypeAll)
}

func (cpi *ProtocolImpl) GetConciseProfile() string {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	return cpi.getProfile(profileTypeConcise)
}

func (cpi *ProtocolImpl) GetSalt() []byte {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	return cpi.salt
}

func (cpi *ProtocolImpl) IsEstablished() bool {
	return cpi.established.Load()
}

func (cpi *ProtocolImpl) SetEstablished() {
	logDebugf(cpi.GetConciseProfile(), "SWITCH ESTABLISHED to true")
	cpi.established.Store(true)
}

func (cpi *ProtocolImpl) IsTlsEstablished() bool {
	return cpi.tlsEstablished.Load()
}

func (cpi *ProtocolImpl) SetTlsEstablished() {
	logutil.Debugf("SWITCH TLS_ESTABLISHED to true")
	cpi.tlsEstablished.Store(true)
}

func (cpi *ProtocolImpl) ConnectionID() uint32 {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	return cpi.connectionID
}

// Quit kill tcpConn still connected.
// before calling NewMysqlClientProtocol, tcpConn.Connected() must be true
// please check goetty/application.go::doStart() and goetty/application.go::NewIOSession(...) for details
func (cpi *ProtocolImpl) Quit() {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	if cpi.tcpConn != nil {
		if !cpi.tcpConn.Connected() {
			return
		}
		err := cpi.tcpConn.Close()
		if err != nil {
			logutil.Errorf("close tcp conn failed. error:%v", err)
		}
	}
}

func (cpi *ProtocolImpl) GetLock() sync.Locker {
	return &cpi.lock
}

func (cpi *ProtocolImpl) GetTcpConnection() goetty.IOSession {
	cpi.lock.Lock()
	defer cpi.lock.Unlock()
	return cpi.tcpConn
}

func (cpi *ProtocolImpl) Peer() (string, string, string, string) {
	tcp := cpi.GetTcpConnection()
	if tcp == nil {
		return "", "", "", ""
	}
	addr := tcp.RemoteAddress()
	rawConn := tcp.RawConn()
	var local net.Addr
	if rawConn != nil {
		local = rawConn.LocalAddr()
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logutil.Errorf("get peer host:port failed. error:%v ", err)
		return "failed", "0", "", ""
	}
	localHost, localPort, err := net.SplitHostPort(local.String())
	if err != nil {
		logutil.Errorf("get peer host:port failed. error:%v ", err)
		return "failed", "0", "failed", "0"
	}
	return host, port, localHost, localPort
}

func (mp *MysqlProtocolImpl) GetRequest(payload []byte) *Request {
	req := &Request{
		cmd:  CommandType(payload[0]),
		data: payload[1:],
	}

	return req
}

func (mp *MysqlProtocolImpl) getAbortTransactionErrorInfo() string {
	ses := mp.GetSession()
	//update error message in Case1,Case3,Case4.
	if ses != nil && ses.OptionBitsIsSet(OPTION_ATTACH_ABORT_TRANSACTION_ERROR) {
		ses.ClearOptionBits(OPTION_ATTACH_ABORT_TRANSACTION_ERROR)
		return abortTransactionErrorInfo()
	}
	return ""
}

func (mp *MysqlProtocolImpl) SendResponse(ctx context.Context, resp *Response) error {
	mp.GetLock().Lock()
	defer mp.GetLock().Unlock()

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
		attachAbort := mp.getAbortTransactionErrorInfo()
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
	default:
		return moerr.NewInternalError(ctx, "unsupported response:%d ", resp.category)
	}
}

var _ Protocol = &FakeProtocol{}
var _ MysqlProtocol = &FakeProtocol{}

const (
	fakeConnectionID uint32 = math.MaxUint32
)

// FakeProtocol works for the background transaction that does not use the network protocol.
type FakeProtocol struct {
	username string
	database string
}

func (fp *FakeProtocol) makeProfile(profileTyp profileType) {
}

func (fp *FakeProtocol) getProfile(profileTyp profileType) string {
	return ""
}

func (fp *FakeProtocol) SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	return nil
}

func (fp *FakeProtocol) ParseExecuteData(ctx context.Context, stmt *PrepareStmt, data []byte, pos int) (names []string, vars []any, err error) {
	return nil, nil, nil
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

func (fp *FakeProtocol) PrepareBeforeProcessingResultSet() {}

func (fp *FakeProtocol) GetStats() string {
	return ""
}

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

func (fp *FakeProtocol) Peer() (string, string, string, string) {
	return "", "", "", ""
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
