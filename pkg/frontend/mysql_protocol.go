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
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	planPb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/proxy"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DefaultCapability means default capabilities of the server
var DefaultCapability = CLIENT_LONG_PASSWORD |
	CLIENT_FOUND_ROWS |
	CLIENT_LONG_FLAG |
	CLIENT_CONNECT_WITH_DB |
	CLIENT_LOCAL_FILES |
	CLIENT_PROTOCOL_41 |
	CLIENT_INTERACTIVE |
	CLIENT_TRANSACTIONS |
	CLIENT_SECURE_CONNECTION |
	CLIENT_MULTI_STATEMENTS |
	CLIENT_MULTI_RESULTS |
	CLIENT_PLUGIN_AUTH |
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA |
	CLIENT_DEPRECATE_EOF |
	CLIENT_CONNECT_ATTRS

// DefaultClientConnStatus default server status
var DefaultClientConnStatus = SERVER_STATUS_AUTOCOMMIT

var serverVersion atomic.Value

const defaultSaltReadTimeout = time.Millisecond * 200

const charsetBinary = 0x3f
const charsetVarchar = 0x21
const boolColumnLength = 12

func init() {
	serverVersion.Store("0.5.0")
}

func InitServerVersion(v string) {
	if len(v) > 0 {
		switch v[0] {
		case 'v': // format 'v1.1.1'
			v = v[1:]
			serverVersion.Store(v)
		default:
			vv := []byte(v)
			for i := 0; i < len(vv); i++ {
				if !unicode.IsDigit(rune(vv[i])) && vv[i] != '.' {
					vv = append(vv[:i], vv[i+1:]...)
					i--
				}
			}
			serverVersion.Store(string(vv))
		}
	} else {
		serverVersion.Store("0.5.0")
	}
}

const (
	clientProtocolVersion uint8 = 10

	/**
	An answer talks about the charset utf8mb4.
	https://stackoverflow.com/questions/766809/whats-the-difference-between-utf8-general-ci-and-utf8-unicode-ci
	It recommends the charset utf8mb4_0900_ai_ci.
	Maybe we can support utf8mb4_0900_ai_ci in the future.

	A concise research in the Mysql 8.0.23.

	the charset in sever level
	======================================

	mysql> show variables like 'character_set_server';
	+----------------------+---------+
	| Variable_name        | Value   |
	+----------------------+---------+
	| character_set_server | utf8mb4 |
	+----------------------+---------+

	mysql> show variables like 'collation_server';
	+------------------+--------------------+
	| Variable_name    | Value              |
	+------------------+--------------------+
	| collation_server | utf8mb4_0900_ai_ci |
	+------------------+--------------------+

	the charset in database level
	=====================================
	mysql> show variables like 'character_set_database';
	+------------------------+---------+
	| Variable_name          | Value   |
	+------------------------+---------+
	| character_set_database | utf8mb4 |
	+------------------------+---------+

	mysql> show variables like 'collation_database';
	+--------------------+--------------------+
	| Variable_name      | Value              |
	+--------------------+--------------------+
	| collation_database | utf8mb4_0900_ai_ci |
	+--------------------+--------------------+

	*/
	// DefaultCollationID is utf8mb4_bin(46)
	utf8mb4BinCollationID uint8 = 46

	Utf8mb4CollationID uint8 = 45

	AuthNativePassword string = "mysql_native_password"

	//the length of the mysql protocol header
	HeaderLengthOfTheProtocol int = 4
	HeaderOffset              int = 0

	// MaxPayloadSize If the payload is larger than or equal to 2^24−1 bytes the length is set to 2^24−1 (ff ff ff)
	//and additional packets are sent with the rest of the payload until the payload of a packet
	//is less than 2^24−1 bytes.
	MaxPayloadSize uint32 = (1 << 24) - 1

	// DefaultMySQLState is the default state of the mySQL
	DefaultMySQLState string = "HY000"
)

var _ MysqlRrWr = &MysqlProtocolImpl{}

type debugStats struct {
	writeCount uint64
	// record 2 cases data, all belong to flush op.
	// 1) MysqlProtocolImpl.flushOutBuffer do flush buffer op
	// 2) MysqlProtocolImpl.writePackets do write with goetty.WriteOptions{Flush: true}
	writeBytes uint64
}

func (ds *debugStats) ResetStats() {
	ds.writeCount = 0
	ds.writeBytes = 0
}

func (ds *debugStats) String() string {
	if ds.writeCount <= 0 {
		ds.writeCount = 1
	}
	return fmt.Sprintf(
		"writeCount %v \n"+
			"writeBytes %v %v MB\n",
		ds.writeCount,
		ds.writeBytes, ds.writeBytes/(1024*1024.0),
	)
}

func (ds *debugStats) AddFlushBytes(b uint64) {
	ds.writeBytes += b
}

/*
rowHandler maintains the states in encoding the result row
*/
type rowHandler struct {
	//the begin position of writing.
	//the range [beginWriteIndex,beginWriteIndex+3]
	//for the length and sequenceId of the mysql protocol packet
	beginOffset int
	//the bytes in the outbuffer
	bytesInOutBuffer int
	//when the number of bytes in the outbuffer exceeds the it,
	//the outbuffer will be flushed.
	untilBytesInOutbufToFlush int
	//the count of the flush
	flushCount int
	//the bytes have been response
	startOffsetInBuffer int
}

/*
isInPacket means it is compositing a packet now
*/
func (rh *rowHandler) isInPacket() bool {
	return rh.beginOffset >= 0
}

/*
resetPacket reset the beginWriteIndex
*/
func (rh *rowHandler) resetPacket() {
	rh.beginOffset = -1
}

/*
resetFlushOutBuffer clears the bytesInOutBuffer
*/
func (rh *rowHandler) resetFlushOutBuffer() {
	rh.bytesInOutBuffer = 0
}

/*
resetFlushCount reset flushCount
*/
func (rh *rowHandler) resetFlushCount() {
	rh.flushCount = 0
}

// resetStartOffset reset the startOffsetInBuffer
// How rowHandler.resetStartOffset, debugStats.writeBytes and MysqlProtocolImpl.CalculateOutTrafficBytes work together ?
// 0. init. call rowHandler.resetStartOffset at the beginning of query, record the beginning offset of the current buffer.
// 1. batch write. inc debugStats.writeBytes and do resetFlushOutBuffer()
// 2. last data. MysqlProtocolImpl.CalculateOutTrafficBytes() with debugStats.writeBytes, rowHandler.startOffsetInBuffer and rowHandler.bytesInOutBuffer
func (rh *rowHandler) resetStartOffset() {
	rh.startOffsetInBuffer = rh.bytesInOutBuffer
}

type MysqlProtocolImpl struct {
	m sync.Mutex

	//TODO: make it global
	io IOPackage

	tcpConn *Conn

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

	ctx context.Context

	//joint capability shared by the server and the client
	capability uint32

	//collation id
	collationID int

	//collation name
	collationName string

	//character set
	charset string

	//max packet size of the client
	maxClientPacketSize uint32

	//the user of the client
	username string

	// opaque authentication response data generated by Authentication Method
	// indicated by the plugin name field.
	authResponse []byte

	//the default database for the client
	database string

	// Connection attributes are key-value pairs that application programs
	// can pass to the server at connect time.
	connectAttrs map[string]string

	//for debug
	debugStats

	//for converting the data into string
	strconvBuffer []byte

	//for encoding the length into bytes
	lenEncBuffer []byte

	//for encoding the null bytes in binary row
	binaryNullBuffer []byte

	SV *config.FrontendParameters

	ses *Session

	disableAutoFlush bool
}

func (mp *MysqlProtocolImpl) GetStr(id PropertyID) string {
	switch id {
	case USERNAME:
		return mp.GetUserName()
	case DBNAME:
		return mp.GetDatabaseName()
	}
	return ""
}
func (mp *MysqlProtocolImpl) SetStr(id PropertyID, val string) {
	switch id {
	case USERNAME:
		mp.SetUserName(val)
	case DBNAME:
		mp.SetDatabaseName(val)
	}
}
func (mp *MysqlProtocolImpl) SetU32(PropertyID, uint32) {}
func (mp *MysqlProtocolImpl) GetU32(id PropertyID) uint32 {
	switch id {
	case CONNID:
		return mp.ConnectionID()
	}
	return math.MaxUint32
}
func (mp *MysqlProtocolImpl) SetU8(id PropertyID, val uint8) {
	switch id {
	case SEQUENCEID:
		mp.SetSequenceID(val)
	}
}

func (mp *MysqlProtocolImpl) GetU8(id PropertyID) uint8 {
	switch id {
	case SEQUENCEID:
		return mp.GetSequenceId()
	}
	return 0
}
func (mp *MysqlProtocolImpl) SetBool(id PropertyID, val bool) {
	switch id {
	case ESTABLISHED:
		if val {
			mp.SetEstablished()
		}
	case TLS_ESTABLISHED:
		if val {
			mp.SetTlsEstablished()
		}
	}
}
func (mp *MysqlProtocolImpl) GetBool(id PropertyID) bool {
	switch id {
	case ESTABLISHED:
		return mp.IsEstablished()
	case TLS_ESTABLISHED:
		return mp.IsTlsEstablished()
	}
	return false
}

func (mp *MysqlProtocolImpl) Write(execCtx *ExecCtx, bat *batch.Batch) error {
	const countOfResultSet = 1
	n := bat.Vecs[0].Length()
	//TODO: remove this MRS here
	//Create a new temporary result set per pipeline thread.
	mrs := MysqlResultSet{}
	//Warning: Don't change ResultColumns in this.
	//Reference the shared ResultColumns of the session among multi-thread.
	sesMrs := execCtx.ses.GetMysqlResultSet()
	mrs.Columns = sesMrs.Columns
	mrs.Name2Index = sesMrs.Name2Index

	//group row
	mrs.Data = make([][]interface{}, countOfResultSet)
	for i := 0; i < countOfResultSet; i++ {
		mrs.Data[i] = make([]interface{}, len(bat.Vecs))
	}
	ses := execCtx.ses.(*Session)
	isShowTableStatus := ses.GetShowStmtType() == ShowTableStatus
	for j := 0; j < n; j++ { //row index
		err := extractRowFromEveryVector(execCtx.reqCtx, execCtx.ses, bat, j, mrs.Data[0])
		if err != nil {
			return err
		}
		if isShowTableStatus {
			row2 := make([]interface{}, len(mrs.Data[0]))
			copy(row2, mrs.Data[0])
			ses.AppendData(row2)
		} else {
			if err = mp.WriteResultSetRow(&mrs, 1); err != nil {
				execCtx.ses.Error(execCtx.reqCtx,
					"Flush error",
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (mp *MysqlProtocolImpl) WriteHandshake() error {
	hsV10pkt := mp.makeHandshakeV10Payload()
	return mp.writePackets(hsV10pkt)
}

func (mp *MysqlProtocolImpl) WriteOK(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	return mp.sendOKPacket(affectedRows, lastInsertId, status, warnings, message)
}

func (mp *MysqlProtocolImpl) WriteOKtWithEOF(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	return mp.sendOKPacketWithEof(affectedRows, lastInsertId, status, warnings, message)
}

func (mp *MysqlProtocolImpl) WriteEOF(warnings, status uint16) error {
	return mp.sendEOFPacket(warnings, status)
}

func (mp *MysqlProtocolImpl) WriteEOFIF(warnings uint16, status uint16) error {
	return mp.SendEOFPacketIf(warnings, status)
}

func (mp *MysqlProtocolImpl) WriteEOFOrOK(warnings uint16, status uint16) error {
	return mp.sendEOFOrOkPacket(warnings, status)
}

func (mp *MysqlProtocolImpl) WriteERR(errorCode uint16, sqlState, errorMessage string) error {
	return mp.sendErrPacket(errorCode, sqlState, errorMessage)
}

func (mp *MysqlProtocolImpl) WriteLengthEncodedNumber(u uint64) error {
	return mp.SendColumnCountPacket(u)
}

func (mp *MysqlProtocolImpl) WriteColumnDef(ctx context.Context, column Column, i int) error {
	return mp.SendColumnDefinitionPacket(ctx, column, i)
}

func (mp *MysqlProtocolImpl) WriteRow() error {
	//TODO implement me
	panic("implement me")
}

func (mp *MysqlProtocolImpl) WriteTextRow() error {
	//TODO implement me
	panic("implement me")
}

func (mp *MysqlProtocolImpl) WriteBinaryRow() error {
	//TODO implement me
	panic("implement me")
}

func (mp *MysqlProtocolImpl) WriteResponse(ctx context.Context, resp *Response) error {
	return mp.SendResponse(ctx, resp)
}

func (mp *MysqlProtocolImpl) WritePrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	return mp.SendPrepareResponse(ctx, stmt)
}

func (mp *MysqlProtocolImpl) Read() ([]byte, error) {
	return mp.tcpConn.Read()
}

func (mp *MysqlProtocolImpl) Free(buf []byte) {
	mp.tcpConn.allocator.Free(buf)
}

func (mp *MysqlProtocolImpl) GetSession() *Session {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.ses
}

func (mp *MysqlProtocolImpl) GetCapability() uint32 {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.capability
}

func (mp *MysqlProtocolImpl) SetCapability(cap uint32) {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.capability = cap
}

func (mp *MysqlProtocolImpl) AddSequenceId(a uint8) {
	mp.tcpConn.sequenceId += a
}

func (mp *MysqlProtocolImpl) SetSequenceID(value uint8) {
	mp.tcpConn.sequenceId = value
}

func (mp *MysqlProtocolImpl) GetDatabaseName() string {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.database
}

func (mp *MysqlProtocolImpl) SetDatabaseName(s string) {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.database = s
}

func (mp *MysqlProtocolImpl) GetUserName() string {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.username
}

func (mp *MysqlProtocolImpl) SetUserName(s string) {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.username = s
}

const bit4TcpWriteCopy = 12 // 1<<12 == 4096

// CalculateOutTrafficBytes calculate the bytes of the last out traffic, the number of mysql packets
// return 0 value, if the connection is closed.
// return -1 value, if the session is nil unexpected.
//
// packet cnt has 3 part:
// 1st part: flush op cnt.
// 2nd part: upload part, calculation = payload / 16KiB
// 3rd part: response part, calculation = sendByte / 4KiB
//   - ioCopyBufferSize currently is 4096 Byte, which is the option for goetty_buf.ByteBuf, set by goetty_buf.WithIOCopyBufferSize(...).
//     goetty_buf.ByteBuf.WriteTo(...) will call by io.CopyBuffer(...) if do Conn.Flush().
//   - If ioCopyBufferSize is changed, you should see the calling of goetty.NewApplicationWithListenAddress(...) in NewMOServer()
func (mp *MysqlProtocolImpl) CalculateOutTrafficBytes(reset bool) (bytes int64, packets int64) {
	ses := mp.GetSession()
	if ses == nil {
		if mp.quit.Load() {
			return 0, 0
		} else {
			return -1, -1
		}
	}
	// Case 1: send data as ResultSet
	resultSetPart := int64(mp.writeBytes)
	// Case 2: send data as CSV
	csvPart := ses.writeCsvBytes.Load()
	bytes = resultSetPart + csvPart
	tcpPkgCnt := ses.GetPacketCnt()
	packets = tcpPkgCnt /*1st part*/ +
		int64(len(ses.sql)>>14) + int64(ses.payloadCounter>>14) + /*2nd part*/
		resultSetPart>>bit4TcpWriteCopy + int64((csvPart>>20)/getGlobalPu().SV.ExportDataDefaultFlushSize) /*3rd part*/
	if reset {
		ses.ResetPacketCounter()
	}
	return
}

func (mp *MysqlProtocolImpl) ResetStatistics() {
	mp.ResetStats()
}

func (mp *MysqlProtocolImpl) GetConnectAttrs() map[string]string {
	mp.m.Lock()
	defer mp.m.Unlock()
	return mp.connectAttrs
}

func (mp *MysqlProtocolImpl) Close() {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.safeQuit()
	if mp.strconvBuffer != nil {
		mp.strconvBuffer = nil
	}
	if mp.lenEncBuffer != nil {
		mp.lenEncBuffer = nil
	}
	if mp.binaryNullBuffer != nil {
		mp.binaryNullBuffer = nil
	}
	mp.ses = nil
}

func (mp *MysqlProtocolImpl) SetSession(ses *Session) {
	mp.m.Lock()
	defer mp.m.Unlock()
	mp.ses = ses
	mp.tcpConn.ses = ses
}

// handshake response 41
type response41 struct {
	capabilities      uint32
	maxPacketSize     uint32
	collationID       uint8
	username          string
	authResponse      []byte
	database          string
	clientPluginName  string
	isAskForTlsHeader bool
	connectAttrs      map[string]string
}

// handshake response 320
type response320 struct {
	capabilities      uint32
	maxPacketSize     uint32
	username          string
	authResponse      []byte
	database          string
	isAskForTlsHeader bool
}

func (mp *MysqlProtocolImpl) SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	dcPrepare, ok := stmt.PreparePlan.GetDcl().Control.(*planPb.DataControl_Prepare)
	if !ok {
		return moerr.NewInternalError(ctx, "can not get Prepare plan in prepareStmt")
	}
	stmtID, err := GetPrepareStmtID(ctx, stmt.Name)
	if err != nil {
		return moerr.NewInternalError(ctx, "can not get Prepare stmtID")
	}
	paramTypes := dcPrepare.Prepare.ParamTypes
	numParams := len(paramTypes)
	columns := plan2.GetResultColumnsFromPlan(dcPrepare.Prepare.Plan)
	numColumns := len(columns)

	var data []byte
	// status ok
	data = append(data, 0)
	// stmt id
	data = mp.io.AppendUint32(data, uint32(stmtID))
	// number columns
	data = mp.io.AppendUint16(data, uint16(numColumns))
	// number params
	data = mp.io.AppendUint16(data, uint16(numParams))
	// filter [00]
	data = append(data, 0)
	// warning count
	data = append(data, 0, 0) // TODO support warning count
	if err := mp.writePackets(data); err != nil {
		return err
	}

	cmd := int(COM_STMT_PREPARE)
	for i := 0; i < numParams; i++ {
		column := new(MysqlColumn)
		column.SetName("?")

		err = convertEngineTypeToMysqlType(ctx, types.T(paramTypes[i]), column)
		if err != nil {
			return err
		}

		err = mp.SendColumnDefinitionPacket(ctx, column, cmd)
		if err != nil {
			return err
		}
	}
	if numParams > 0 {
		if err := mp.SendEOFPacketIf(0, mp.GetSession().GetTxnHandler().GetServerStatus()); err != nil {
			return err
		}
	}

	for i := 0; i < numColumns; i++ {
		column := new(MysqlColumn)
		column.SetName(columns[i].Name)

		err = convertEngineTypeToMysqlType(ctx, types.T(columns[i].Typ.Id), column)
		if err != nil {
			return err
		}

		err = mp.SendColumnDefinitionPacket(ctx, column, cmd)
		if err != nil {
			return err
		}
	}
	if numColumns > 0 {
		if err := mp.SendEOFPacketIf(0, mp.GetSession().GetTxnHandler().GetServerStatus()); err != nil {
			return err
		}
	}

	return nil
}

func (mp *MysqlProtocolImpl) ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	var err error
	dcPrepare, ok := stmt.PreparePlan.GetDcl().Control.(*planPb.DataControl_Prepare)
	if !ok {
		return moerr.NewInternalError(ctx, "can not get Prepare plan in prepareStmt")
	}
	numParams := len(dcPrepare.Prepare.ParamTypes)

	paramIdx, newPos, ok := mp.io.ReadUint16(data, pos)
	if !ok {
		return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
	}
	pos = newPos
	if int(paramIdx) >= numParams {
		return moerr.NewInternalError(ctx, "get param index out of range. get %d, param length is %d", paramIdx, numParams)
	}

	if stmt.params == nil {
		stmt.params = proc.GetVector(types.T_text.ToType())
		for i := 0; i < numParams; i++ {
			err = vector.AppendBytes(stmt.params, []byte{}, false, proc.GetMPool())
			if err != nil {
				return err
			}
		}
	}

	length := len(data) - pos
	val, _, ok := mp.readCountOfBytes(data, pos, length)
	if !ok {
		return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
	}
	stmt.getFromSendLongData[int(paramIdx)] = struct{}{}
	return util.SetAnyToStringVector(proc, val, stmt.params, int(paramIdx))
}

func (mp *MysqlProtocolImpl) ParseExecuteData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	var err error
	dcPrepare, ok := stmt.PreparePlan.GetDcl().Control.(*planPb.DataControl_Prepare)
	if !ok {
		return moerr.NewInternalError(ctx, "can not get Prepare plan in prepareStmt")
	}
	numParams := len(dcPrepare.Prepare.ParamTypes)

	if stmt.params == nil {
		stmt.params = proc.GetVector(types.T_text.ToType())
		for i := 0; i < numParams; i++ {
			err = vector.AppendBytes(stmt.params, []byte{}, false, proc.GetMPool())
			if err != nil {
				return err
			}
		}
	}

	var flag uint8
	flag, pos, ok = mp.io.ReadUint8(data, pos)
	if !ok {
		return moerr.NewInternalError(ctx, "malform packet")

	}
	if flag != 0 {
		// TODO only support CURSOR_TYPE_NO_CURSOR flag now
		return moerr.NewInvalidInput(ctx, "unsupported Prepare flag '%v'", flag)
	}

	// skip iteration-count, always 1
	pos += 4

	if numParams > 0 {
		var nullBitmaps []byte
		var paramTypes []byte
		nullBitmapLen := (numParams + 7) >> 3
		nullBitmaps, pos, ok = mp.readCountOfBytes(data, pos, nullBitmapLen)
		if !ok {
			return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
		}

		// new param bound flag
		if data[pos] == 1 {
			pos++

			// Just the first StmtExecute packet contain parameters type,
			// we need save it for further use.
			paramTypes, pos, ok = mp.readCountOfBytes(data, pos, numParams<<1)
			stmt.ParamTypes = make([]byte, len(paramTypes))
			copy(stmt.ParamTypes, paramTypes)
			if !ok {
				return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
			}
		} else {
			pos++
		}

		// get paramters and set value to session variables
		for i := 0; i < numParams; i++ {
			// if params had received via COM_STMT_SEND_LONG_DATA, use them directly(we set the params when deal with COM_STMT_SEND_LONG_DATA).
			// ref https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
			if _, ok := stmt.getFromSendLongData[i]; ok {
				continue
			}

			if nullBitmaps[i>>3]&(1<<(uint(i)%8)) > 0 {
				err = util.SetAnyToStringVector(proc, nil, stmt.params, i)
				if err != nil {
					return err
				}
				continue
			}

			if (i<<1)+1 >= len(stmt.ParamTypes) {
				return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

			}
			tp := stmt.ParamTypes[i<<1]
			isUnsigned := (stmt.ParamTypes[(i<<1)+1] & 0x80) > 0

			switch defines.MysqlType(tp) {
			case defines.MYSQL_TYPE_NULL:
				err = util.SetAnyToStringVector(proc, nil, stmt.params, i)

			case defines.MYSQL_TYPE_BIT:
				val, newPos, ok := mp.io.ReadUint64(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}

				pos = newPos
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			case defines.MYSQL_TYPE_TINY:
				val, newPos, ok := mp.io.ReadUint8(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

				}

				pos = newPos
				if isUnsigned {
					// vars[i] = val
					err = util.SetAnyToStringVector(proc, val, stmt.params, i)
				} else {
					// vars[i] = int8(val)
					err = util.SetAnyToStringVector(proc, int8(val), stmt.params, i)
				}

			case defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_YEAR:
				val, newPos, ok := mp.io.ReadUint16(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}

				pos = newPos
				if isUnsigned {
					err = util.SetAnyToStringVector(proc, val, stmt.params, i)
				} else {
					err = util.SetAnyToStringVector(proc, int16(val), stmt.params, i)
				}

			case defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
				val, newPos, ok := mp.io.ReadUint32(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}

				pos = newPos
				if isUnsigned {
					err = util.SetAnyToStringVector(proc, val, stmt.params, i)
				} else {
					err = util.SetAnyToStringVector(proc, int32(val), stmt.params, i)
				}

			case defines.MYSQL_TYPE_LONGLONG:
				val, newPos, ok := mp.io.ReadUint64(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}

				pos = newPos
				if isUnsigned {
					err = util.SetAnyToStringVector(proc, val, stmt.params, i)
				} else {
					err = util.SetAnyToStringVector(proc, int64(val), stmt.params, i)
				}

			case defines.MYSQL_TYPE_FLOAT:
				val, newPos, ok := mp.io.ReadUint32(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

				}
				pos = newPos
				err = util.SetAnyToStringVector(proc, math.Float32frombits(val), stmt.params, i)

			case defines.MYSQL_TYPE_DOUBLE:
				val, newPos, ok := mp.io.ReadUint64(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}
				pos = newPos
				err = util.SetAnyToStringVector(proc, math.Float64frombits(val), stmt.params, i)

			// Binary/varbinary has mysql_type_varchar.
			case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING, defines.MYSQL_TYPE_DECIMAL,
				defines.MYSQL_TYPE_ENUM, defines.MYSQL_TYPE_SET, defines.MYSQL_TYPE_GEOMETRY:
				val, newPos, ok := mp.readStringLenEnc(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}
				pos = newPos
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			case defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TINY_BLOB, defines.MYSQL_TYPE_MEDIUM_BLOB, defines.MYSQL_TYPE_LONG_BLOB, defines.MYSQL_TYPE_TEXT:
				val, newPos, ok := mp.readStringLenEnc(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}
				pos = newPos
				// vars[i] = []byte(val)
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			case defines.MYSQL_TYPE_TIME:
				// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
				// for more details.
				length, newPos, ok := mp.io.ReadUint8(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}
				pos = newPos
				var val string
				switch length {
				case 0:
					val = "0d 00:00:00"
				case 8, 12:
					pos, val = mp.readTime(data, pos, length)
				default:
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")
				}
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			case defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP:
				// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
				// for more details.
				length, newPos, ok := mp.io.ReadUint8(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

				}
				pos = newPos
				var val string
				switch length {
				case 0:
					val = "0000-00-00 00:00:00"
				case 4:
					pos, val = mp.readDate(data, pos)
				case 7:
					pos, val = mp.readDateTime(data, pos)
				case 11:
					pos, val = mp.readTimestamp(data, pos)
				default:
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

				}
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			case defines.MYSQL_TYPE_NEWDECIMAL:
				// use string for decimal.  Not tested
				val, newPos, ok := mp.readStringLenEnc(data, pos)
				if !ok {
					return moerr.NewInvalidInput(ctx, "mysql protocol error, malformed packet")

				}
				pos = newPos
				err = util.SetAnyToStringVector(proc, val, stmt.params, i)

			default:
				return moerr.NewInternalError(ctx, "unsupport parameter type")

			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (mp *MysqlProtocolImpl) readDate(data []byte, pos int) (int, string) {
	year, pos, _ := mp.io.ReadUint16(data, pos)
	month := data[pos]
	pos++
	day := data[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func (mp *MysqlProtocolImpl) readTime(data []byte, pos int, len uint8) (int, string) {
	var retStr string
	negate := data[pos]
	pos++
	if negate == 1 {
		retStr += "-"
	}
	day, pos, _ := mp.io.ReadUint32(data, pos)
	if day > 0 {
		retStr += fmt.Sprintf("%dd ", day)
	}
	hour := data[pos]
	pos++
	minute := data[pos]
	pos++
	second := data[pos]
	pos++

	if len == 12 {
		ms, _, _ := mp.io.ReadUint32(data, pos)
		retStr += fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, ms)
	} else {
		retStr += fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	}

	return pos, retStr
}

func (mp *MysqlProtocolImpl) readDateTime(data []byte, pos int) (int, string) {
	pos, date := mp.readDate(data, pos)
	hour := data[pos]
	pos++
	minute := data[pos]
	pos++
	second := data[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func (mp *MysqlProtocolImpl) readTimestamp(data []byte, pos int) (int, string) {
	pos, dateTime := mp.readDateTime(data, pos)
	microSecond, pos, _ := mp.io.ReadUint32(data, pos)
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

// read an int with length encoded from the buffer at the position
// return the int ; position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (mp *MysqlProtocolImpl) readIntLenEnc(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	switch data[pos] {
	case 0xfb:
		//zero, one byte
		return 0, pos + 1, true
	case 0xfc:
		// int in two bytes
		if pos+2 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8
		return value, pos + 3, true
	case 0xfd:
		// int in three bytes
		if pos+3 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16
		return value, pos + 4, true
	case 0xfe:
		// int in eight bytes
		if pos+8 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 |
			uint64(data[pos+5])<<32 |
			uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 |
			uint64(data[pos+8])<<56
		return value, pos + 9, true
	}
	// 0-250
	return uint64(data[pos]), pos + 1, true
}

func (mp *MysqlProtocolImpl) ReadIntLenEnc(data []byte, pos int) (uint64, int, bool) {
	return mp.readIntLenEnc(data, pos)
}

// write an int with length encoded into the buffer at the position
// return position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (mp *MysqlProtocolImpl) writeIntLenEnc(data []byte, pos int, value uint64) int {
	switch {
	case value < 251:
		data[pos] = byte(value)
		return pos + 1
	case value < (1 << 16):
		data[pos] = 0xfc
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		return pos + 3
	case value < (1 << 24):
		data[pos] = 0xfd
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		data[pos+4] = byte(value >> 24)
		data[pos+5] = byte(value >> 32)
		data[pos+6] = byte(value >> 40)
		data[pos+7] = byte(value >> 48)
		data[pos+8] = byte(value >> 56)
		return pos + 9
	}
}

// append an int with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendIntLenEnc(value uint64) error {
	mp.lenEncBuffer = mp.lenEncBuffer[:9]
	pos := mp.writeIntLenEnc(mp.lenEncBuffer, 0, value)
	err := mp.append(mp.lenEncBuffer[:pos]...)
	if err != nil {
		return err
	}
	return nil
}

// read the count of bytes from the buffer at the position
// return bytes slice ; position + count ; true - succeeded or false - failed
func (mp *MysqlProtocolImpl) readCountOfBytes(data []byte, pos int, count int) ([]byte, int, bool) {
	if pos+count-1 >= len(data) {
		return nil, 0, false
	}
	return data[pos : pos+count], pos + count, true
}

// write the count of bytes into the buffer at the position
// return position + the number of bytes
func (mp *MysqlProtocolImpl) writeCountOfBytes(data []byte, pos int, value []byte) int {
	pos += copy(data[pos:], value)
	return pos
}

// append the count of bytes to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendCountOfBytes(value []byte) error {
	err := mp.append(value...)
	if err != nil {
		return err
	}
	return nil
}

// read a string with fixed length from the buffer at the position
// return string ; position + length ; true - succeeded or false - failed
func (mp *MysqlProtocolImpl) readStringFix(data []byte, pos int, length int) (string, int, bool) {
	var sdata []byte
	var ok bool
	sdata, pos, ok = mp.readCountOfBytes(data, pos, length)
	if !ok {
		return "", 0, false
	}
	return string(sdata), pos, true
}

// write a string with fixed length into the buffer at the position
// return pos + string.length
func (mp *MysqlProtocolImpl) writeStringFix(data []byte, pos int, value string, length int) int {
	pos += copy(data[pos:], value[0:length])
	return pos
}

// append a string with fixed length to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendStringFix(value string, length int) error {
	err := mp.append([]byte(value[:length])...)
	if err != nil {
		return err
	}
	return nil
}

// read a string appended with zero from the buffer at the position
// return string ; position + length of the string + 1; true - succeeded or false - failed
func (mp *MysqlProtocolImpl) readStringNUL(data []byte, pos int) (string, int, bool) {
	zeroPos := bytes.IndexByte(data[pos:], 0)
	if zeroPos == -1 {
		return "", 0, false
	}
	return string(data[pos : pos+zeroPos]), pos + zeroPos + 1, true
}

// write a string into the buffer at the position, then appended with 0
// return pos + string.length + 1
func (mp *MysqlProtocolImpl) writeStringNUL(data []byte, pos int, value string) int {
	pos = mp.writeStringFix(data, pos, value, len(value))
	data[pos] = 0
	return pos + 1
}

// read a string with length encoded from the buffer at the position
// return string ; position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string; true - succeeded or false - failed
func (mp *MysqlProtocolImpl) readStringLenEnc(data []byte, pos int) (string, int, bool) {
	var value uint64
	var ok bool
	value, pos, ok = mp.readIntLenEnc(data, pos)
	if !ok {
		return "", 0, false
	}
	sLength := int(value)
	if pos+sLength-1 >= len(data) {
		return "", 0, false
	}
	return string(data[pos : pos+sLength]), pos + sLength, true
}

// write a string with length encoded into the buffer at the position
// return position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string;
func (mp *MysqlProtocolImpl) writeStringLenEnc(data []byte, pos int, value string) int {
	pos = mp.writeIntLenEnc(data, pos, uint64(len(value)))
	return mp.writeStringFix(data, pos, value, len(value))
}

// append a string with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendStringLenEnc(value string) error {
	err := mp.appendIntLenEnc(uint64(len(value)))
	if err != nil {
		return err
	}
	err = mp.appendStringFix(value, len(value))
	if err != nil {
		return err
	}
	return nil
}

// append bytes with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendCountOfBytesLenEnc(value []byte) error {
	err := mp.appendIntLenEnc(uint64(len(value)))
	if err != nil {
		return err
	}
	err = mp.appendCountOfBytes(value)
	if err != nil {
		return err
	}
	return nil
}

// append an int64 value converted to string with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendStringLenEncOfInt64(value int64) error {
	mp.strconvBuffer = mp.strconvBuffer[:0]
	mp.strconvBuffer = strconv.AppendInt(mp.strconvBuffer, value, 10)
	err := mp.appendCountOfBytesLenEnc(mp.strconvBuffer)
	if err != nil {
		return err
	}
	return nil
}

// append an uint64 value converted to string with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendStringLenEncOfUint64(value uint64) error {
	mp.strconvBuffer = mp.strconvBuffer[:0]
	mp.strconvBuffer = strconv.AppendUint(mp.strconvBuffer, value, 10)
	err := mp.appendCountOfBytesLenEnc(mp.strconvBuffer)
	if err != nil {
		return err
	}
	return nil
}

// append an float32 value converted to string with length encoded to the buffer
// return the buffer
func (mp *MysqlProtocolImpl) appendStringLenEncOfFloat64(value float64, bitSize int) error {
	mp.strconvBuffer = mp.strconvBuffer[:0]
	if !math.IsInf(value, 0) {
		mp.strconvBuffer = strconv.AppendFloat(mp.strconvBuffer, value, 'f', -1, bitSize)
	} else {
		if math.IsInf(value, 1) {
			mp.strconvBuffer = append(mp.strconvBuffer, []byte("+Infinity")...)
		} else {
			mp.strconvBuffer = append(mp.strconvBuffer, []byte("-Infinity")...)
		}
	}
	err := mp.appendCountOfBytesLenEnc(mp.strconvBuffer)
	if err != nil {
		return err
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendUint8(e uint8) error {
	err := mp.append(e)
	if err != nil {
		return err
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendUint16(e uint16) error {
	buf := mp.lenEncBuffer[:2]
	pos := mp.io.WriteUint16(buf, 0, e)
	err := mp.append(buf[:pos]...)
	if err != nil {
		return err
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendUint32(e uint32) error {
	buf := mp.lenEncBuffer[:4]
	pos := mp.io.WriteUint32(buf, 0, e)
	err := mp.append(buf[:pos]...)
	if err != nil {
		return err
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendUint64(e uint64) error {
	buf := mp.lenEncBuffer[:8]
	pos := mp.io.WriteUint64(buf, 0, e)
	err := mp.append(buf[:pos]...)
	if err != nil {
		return err
	}
	return nil
}

// write the count of zeros into the buffer at the position
// return pos + count
func (mp *MysqlProtocolImpl) writeZeros(data []byte, pos int, count int) int {
	for i := 0; i < count; i++ {
		data[pos+i] = 0
	}
	return pos + count
}

// the server get the auth string from HandShakeResponse
// pwd is SHA1(SHA1(password)), AUTH is from client
// hash1 = AUTH XOR SHA1( slat + pwd)
// hash2 = SHA1(hash1)
// check(hash2, hpwd)
func (mp *MysqlProtocolImpl) checkPassword(pwd, salt, auth []byte) bool {
	ses := mp.GetSession()
	sha := sha1.New()
	_, err := sha.Write(salt)
	if err != nil {
		ses.Error(mp.ctx, "SHA1(salt) failed.")
		return false
	}
	_, err = sha.Write(pwd)
	if err != nil {
		ses.Error(mp.ctx, "SHA1(hpwd) failed.")
		return false
	}
	hash1 := sha.Sum(nil)

	if len(auth) != len(hash1) {
		return false
	}

	for i := range hash1 {
		hash1[i] ^= auth[i]
	}

	hash2 := HashSha1(hash1)
	return bytes.Equal(pwd, hash2)
}

// the server authenticate that the client can connect and use the database
func (mp *MysqlProtocolImpl) authenticateUser(ctx context.Context, authResponse []byte) error {
	var psw []byte
	var err error
	var tenant *TenantInfo

	ses := mp.GetSession()
	if !mp.SV.SkipCheckUser {
		ses.Debugf(ctx, "authenticate user 1")
		psw, err = ses.AuthenticateUser(ctx, mp.GetUserName(), mp.GetDatabaseName(), mp.authResponse, mp.GetSalt(), mp.checkPassword)
		if err != nil {
			return err
		}
		ses.Debugf(ctx, "authenticate user 2")

		//TO Check password
		if mp.checkPassword(psw, mp.GetSalt(), authResponse) {
			ses.Debugf(ctx, "check password succeeded")
			if err = ses.InitSystemVariables(ctx); err != nil {
				return err
			}
		} else {
			return moerr.NewInternalError(ctx, "check password failed")
		}
	} else {
		ses.Debugf(ctx, "skip authenticate user")
		//Get tenant info
		tenant, err = GetTenantInfo(ctx, mp.GetUserName())
		if err != nil {
			return err
		}

		if ses != nil {
			ses.SetTenantInfo(tenant)

			//TO Check password
			if len(psw) == 0 || mp.checkPassword(psw, mp.GetSalt(), authResponse) {
				mp.ses.Info(ctx, "check password succeeded")
			} else {
				return moerr.NewInternalError(ctx, "check password failed")
			}
		}
	}
	return nil
}

func (mp *MysqlProtocolImpl) HandleHandshake(ctx context.Context, payload []byte) (bool, error) {
	var err error
	if len(payload) < 2 {
		return false, moerr.NewInternalError(ctx, "received a broken response packet")
	}

	if capabilities, _, ok := mp.io.ReadUint16(payload, 0); !ok {
		return false, moerr.NewInternalError(ctx, "read capabilities from response packet failed")
	} else if uint32(capabilities)&CLIENT_PROTOCOL_41 != 0 {
		var resp41 response41
		var ok2 bool
		mp.GetSession().Debug(ctx, "analyse handshake response")
		if ok2, resp41, err = mp.analyseHandshakeResponse41(ctx, payload); !ok2 {
			return false, err
		}

		// client ask server to upgradeTls
		if resp41.isAskForTlsHeader {
			return true, nil
		}

		mp.authResponse = resp41.authResponse
		mp.capability = mp.capability & resp41.capabilities

		if nameAndCharset, ok3 := collationID2CharsetAndName[int(resp41.collationID)]; !ok3 {
			return false, moerr.NewInternalError(ctx, "get collationName and charset failed")
		} else {
			mp.collationID = int(resp41.collationID)
			mp.collationName = nameAndCharset.collationName
			mp.charset = nameAndCharset.charset
		}

		mp.maxClientPacketSize = resp41.maxPacketSize
		mp.username = resp41.username
		mp.database = resp41.database
		mp.connectAttrs = resp41.connectAttrs
	} else {
		var resp320 response320
		var ok2 bool
		if ok2, resp320, err = mp.analyseHandshakeResponse320(ctx, payload); !ok2 {
			return false, err
		}

		// client ask server to upgradeTls
		if resp320.isAskForTlsHeader {
			return true, nil
		}

		mp.authResponse = resp320.authResponse
		mp.capability = mp.capability & resp320.capabilities
		mp.collationID = int(Utf8mb4CollationID)
		mp.collationName = "utf8mb4_general_ci"
		mp.charset = "utf8mb4"

		mp.maxClientPacketSize = resp320.maxPacketSize
		mp.username = resp320.username
		mp.database = resp320.database
	}
	return false, nil
}

func (mp *MysqlProtocolImpl) Authenticate(ctx context.Context) error {
	ses := mp.GetSession()
	ses.timestampMap[TSAuthenticateStart] = time.Now()
	defer func() {
		ses.timestampMap[TSAuthenticateEnd] = time.Now()
		v2.AuthenticateDurationHistogram.Observe(ses.timestampMap[TSAuthenticateEnd].Sub(ses.timestampMap[TSAuthenticateStart]).Seconds())
	}()

	ses.Debugf(ctx, "authenticate user")
	if err := mp.authenticateUser(ctx, mp.authResponse); err != nil {
		ses.Errorf(ctx, "authenticate user failed.error:%v", err)
		errorCode, sqlState, msg := RewriteError(err, mp.username)
		ses.timestampMap[TSSendErrPacketStart] = time.Now()
		err2 := mp.sendErrPacket(errorCode, sqlState, msg)
		ses.timestampMap[TSSendErrPacketEnd] = time.Now()
		v2.SendErrPacketDurationHistogram.Observe(ses.timestampMap[TSSendErrPacketEnd].Sub(ses.timestampMap[TSSendErrPacketStart]).Seconds())
		if err2 != nil {
			ses.Errorf(ctx, "send err packet failed.error:%v", err2)
			return err2
		}
		return err
	}

	ses.Debugf(ctx, "handle handshake end")
	ses.timestampMap[TSSendOKPacketStart] = time.Now()
	err := mp.sendOKPacket(0, 0, 0, 0, "")
	ses.timestampMap[TSSendOKPacketEnd] = time.Now()
	v2.SendOKPacketDurationHistogram.Observe(ses.timestampMap[TSSendOKPacketEnd].Sub(ses.timestampMap[TSSendOKPacketStart]).Seconds())
	ses.Debugf(ctx, "handle handshake response ok")
	if err != nil {
		return err
	}
	return nil
}

// the server makes a handshake v10 packet
// return handshake packet
func (mp *MysqlProtocolImpl) makeHandshakeV10Payload() []byte {
	var data = make([]byte, HeaderOffset+256)
	var pos = HeaderOffset
	//int<1> protocol version
	pos = mp.io.WriteUint8(data, pos, clientProtocolVersion)

	pos = mp.writeStringNUL(data, pos, mp.SV.ServerVersionPrefix+serverVersion.Load().(string))

	//int<4> connection id
	pos = mp.io.WriteUint32(data, pos, mp.ConnectionID())

	//string[8] auth-plugin-data-part-1
	pos = mp.writeCountOfBytes(data, pos, mp.GetSalt()[0:8])

	//int<1> filler 0
	pos = mp.io.WriteUint8(data, pos, 0)

	//int<2>              capabilities flags (lower 2 bytes)
	pos = mp.io.WriteUint16(data, pos, uint16(mp.capability&0xFFFF))

	//int<1>              character set
	pos = mp.io.WriteUint8(data, pos, utf8mb4BinCollationID)

	//int<2>              status flags
	pos = mp.io.WriteUint16(data, pos, DefaultClientConnStatus)

	//int<2>              capabilities flags (upper 2 bytes)
	pos = mp.io.WriteUint16(data, pos, uint16((DefaultCapability>>16)&0xFFFF))

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0 {
		//int<1>              length of auth-plugin-data
		//set 21 always
		pos = mp.io.WriteUint8(data, pos, uint8(len(mp.GetSalt())+1))
	} else {
		//int<1>              [00]
		//set 0 always
		pos = mp.io.WriteUint8(data, pos, 0)
	}

	//string[10]     reserved (all [00])
	pos = mp.writeZeros(data, pos, 10)

	if (DefaultCapability & CLIENT_SECURE_CONNECTION) != 0 {
		//string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
		pos = mp.writeCountOfBytes(data, pos, mp.GetSalt()[8:])
		pos = mp.io.WriteUint8(data, pos, 0)
	}

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0 {
		//string[NUL]    auth-plugin name
		pos = mp.writeStringNUL(data, pos, AuthNativePassword)
	}

	return data[:pos]
}

// the server analyses handshake response41 info from the client
// return true - analysed successfully / false - failed ; response41 ; error
func (mp *MysqlProtocolImpl) analyseHandshakeResponse41(ctx context.Context, data []byte) (bool, response41, error) {
	var pos = 0
	var ok bool
	var info response41

	//int<4>             capabilities flags of the client, CLIENT_PROTOCOL_41 always set
	info.capabilities, pos, ok = mp.io.ReadUint32(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get capabilities failed")
	}

	if (info.capabilities & CLIENT_PROTOCOL_41) == 0 {
		return false, info, moerr.NewInternalError(ctx, "capabilities does not have protocol 41")
	}

	//int<4>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxPacketSize, pos, ok = mp.io.ReadUint32(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get max packet size failed")
	}

	//int<1>             character set
	//connection's default character set
	info.collationID, pos, ok = mp.io.ReadUint8(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get character set failed")
	}

	if pos+22 >= len(data) {
		return false, info, moerr.NewInternalError(ctx, "skip reserved failed")
	}
	//string[23]         reserved (all [0])
	//just skip it
	pos += 23

	// if client reply for upgradeTls, then data will contains header only.
	if pos == len(data) && (info.capabilities&CLIENT_SSL) != 0 {
		info.isAskForTlsHeader = true
		return true, info, nil
	}

	//string[NUL]        username
	info.username, pos, ok = mp.readStringNUL(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get username failed")
	}

	/*
		if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
			lenenc-int         length of auth-response
			string[n]          auth-response
		} else if capabilities & CLIENT_SECURE_CONNECTION {
			int<1>             length of auth-response
			string[n]           auth-response
		} else {
			string[NUL]        auth-response
		}
	*/
	if (info.capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
		var l uint64
		l, pos, ok = mp.readIntLenEnc(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of auth-response failed")
		}
		info.authResponse, pos, ok = mp.readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	} else if (info.capabilities & CLIENT_SECURE_CONNECTION) != 0 {
		var l uint8
		l, pos, ok = mp.io.ReadUint8(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of auth-response failed")
		}
		info.authResponse, pos, ok = mp.readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	} else {
		var auth string
		auth, pos, ok = mp.readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
		info.authResponse = []byte(auth)
	}

	if (info.capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
		info.database, pos, ok = mp.readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get database failed")
		}
	}

	if (info.capabilities & CLIENT_PLUGIN_AUTH) != 0 {
		info.clientPluginName, pos, ok = mp.readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth plugin name failed")
		}

		//to switch authenticate method
		if info.clientPluginName != AuthNativePassword {
			var err error
			if info.authResponse, err = mp.negotiateAuthenticationMethod(ctx); err != nil {
				return false, info, moerr.NewInternalError(ctx, "negotiate authentication method failed. error:%v", err)
			}
			info.clientPluginName = AuthNativePassword
		}
	}

	// client connection attributes
	info.connectAttrs = make(map[string]string)
	if info.capabilities&CLIENT_CONNECT_ATTRS != 0 {
		var l uint64
		var ok bool
		l, pos, ok = mp.readIntLenEnc(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of client-connect-attrs failed")
		}
		endPos := pos + int(l)
		var key, value string
		for pos < endPos {
			key, pos, ok = mp.readStringLenEnc(data, pos)
			if !ok {
				return false, info, moerr.NewInternalError(ctx, "get connect-attrs key failed")
			}
			value, pos, ok = mp.readStringLenEnc(data, pos)
			if !ok {
				return false, info, moerr.NewInternalError(ctx, "get connect-attrs value failed")
			}
			info.connectAttrs[key] = value
		}
	}

	return true, info, nil
}

/*
//the server does something after receiving a handshake response41 from the client
//like check user and password
//and other things
func (mp *MysqlProtocolImpl) handleClientResponse41(resp41 response41) error {
	//to do something else
	//logutil.Infof("capabilities 0x%x\n", resp41.capabilities)
	//logutil.Infof("maxPacketSize %d\n", resp41.maxPacketSize)
	//logutil.Infof("collationID %d\n", resp41.collationID)
	//logutil.Infof("username %s\n", resp41.username)
	//logutil.Infof("authResponse: \n")
	//update the capabilities with client's capabilities
	mp.capability = DefaultCapability & resp41.capabilities

	//character set
	if nameAndCharset, ok := collationID2CharsetAndName[int(resp41.collationID)]; !ok {
		return moerr.NewInternalError(requestCtx, "get collationName and charset failed")
	} else {
		mp.collationID = int(resp41.collationID)
		mp.collationName = nameAndCharset.collationName
		mp.charset = nameAndCharset.charset
	}

	mp.maxClientPacketSize = resp41.maxPacketSize
	mp.username = resp41.username
	mp.database = resp41.database

	//logutil.Infof("collationID %d collatonName %s charset %s \n", mp.collationID, mp.collationName, mp.charset)
	//logutil.Infof("database %s \n", resp41.database)
	//logutil.Infof("clientPluginName %s \n", resp41.clientPluginName)
	return nil
}
*/

// the server analyses handshake response320 info from the old client
// return true - analysed successfully / false - failed ; response320 ; error
func (mp *MysqlProtocolImpl) analyseHandshakeResponse320(ctx context.Context, data []byte) (bool, response320, error) {
	var pos = 0
	var ok bool
	var info response320
	var capa uint16

	//int<2>             capabilities flags, CLIENT_PROTOCOL_41 never set
	capa, pos, ok = mp.io.ReadUint16(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get capabilities failed")
	}
	info.capabilities = uint32(capa)

	if pos+2 >= len(data) {
		return false, info, moerr.NewInternalError(ctx, "get max-packet-size failed")
	}

	//int<3>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxPacketSize = uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16
	pos += 3

	// if client reply for upgradeTls, then data will contains header only.
	if pos == len(data) && (info.capabilities&CLIENT_SSL) != 0 {
		info.isAskForTlsHeader = true
		return true, info, nil
	}

	//string[NUL]        username
	info.username, pos, ok = mp.readStringNUL(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get username failed")
	}

	if (info.capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
		var auth string
		auth, pos, ok = mp.readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
		info.authResponse = []byte(auth)

		info.database, _, ok = mp.readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get database failed")
		}
	} else {
		info.authResponse, _, ok = mp.readCountOfBytes(data, pos, len(data)-pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	}

	return true, info, nil
}

/*
//the server does something after receiving a handshake response320 from the client
//like check user and password
//and other things
func (mp *MysqlProtocolImpl) handleClientResponse320(resp320 response320) error {
	//to do something else
	//logutil.Infof("capabilities 0x%x\n", resp320.capabilities)
	//logutil.Infof("maxPacketSize %d\n", resp320.maxPacketSize)
	//logutil.Infof("username %s\n", resp320.username)
	//logutil.Infof("authResponse: \n")

	//update the capabilities with client's capabilities
	mp.capability = DefaultCapability & resp320.capabilities

	//if the client does not notice its default charset, the server gives a default charset.
	//Run the sql in mysql 8.0.23 to get the charset
	//the sql: select * from information_schema.collations where collation_name = 'utf8mb4_general_ci';
	mp.collationID = int(Utf8mb4CollationID)
	mp.collationName = "utf8mb4_general_ci"
	mp.charset = "utf8mb4"

	mp.maxClientPacketSize = resp320.maxPacketSize
	mp.username = resp320.username
	mp.database = resp320.database

	//logutil.Infof("collationID %d collatonName %s charset %s \n", mp.collationID, mp.collationName, mp.charset)
	//logutil.Infof("database %s \n", resp320.database)
	return nil
}
*/

// the server makes a AuthSwitchRequest that asks the client to authenticate the data with new method
func (mp *MysqlProtocolImpl) makeAuthSwitchRequestPayload(authMethodName string) []byte {
	data := make([]byte, HeaderOffset+1+len(authMethodName)+1+len(mp.GetSalt())+1)
	pos := HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.EOFHeader)
	pos = mp.writeStringNUL(data, pos, authMethodName)
	pos = mp.writeCountOfBytes(data, pos, mp.GetSalt())
	pos = mp.io.WriteUint8(data, pos, 0)
	return data[:pos]
}

// the server can send AuthSwitchRequest to ask client to use designated authentication method,
// if both server and client support CLIENT_PLUGIN_AUTH capability.
// return data authenticated with new method
func (mp *MysqlProtocolImpl) negotiateAuthenticationMethod(ctx context.Context) ([]byte, error) {
	var err error
	aswPkt := mp.makeAuthSwitchRequestPayload(AuthNativePassword)
	err = mp.writePackets(aswPkt)
	if err != nil {
		return nil, err
	}

	data, err := mp.tcpConn.Read()
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, moerr.NewInternalError(ctx, "packet is null")
	}

	return data, nil
}

// extendStatus extends a flag to the status variable.
// SERVER_QUERY_WAS_SLOW and SERVER_STATUS_NO_GOOD_INDEX_USED is not used in other modules,
// so we use it to mark the packet MUST be OK/EOF.
func extendStatus(old uint16) uint16 {
	return old | SERVER_QUERY_WAS_SLOW | SERVER_STATUS_NO_GOOD_INDEX_USED
}

// make a OK packet
func (mp *MysqlProtocolImpl) makeOKPayload(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	data := make([]byte, HeaderOffset+128+len(message)+10)
	var pos = HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.OKHeader)
	pos = mp.writeIntLenEnc(data, pos, affectedRows)
	pos = mp.writeIntLenEnc(data, pos, lastInsertId)
	statusFlags = extendStatus(statusFlags)
	if (mp.capability & CLIENT_PROTOCOL_41) != 0 {
		pos = mp.io.WriteUint16(data, pos, statusFlags)
		pos = mp.io.WriteUint16(data, pos, warnings)
	} else if (mp.capability & CLIENT_TRANSACTIONS) != 0 {
		pos = mp.io.WriteUint16(data, pos, statusFlags)
	}

	if mp.capability&CLIENT_SESSION_TRACK != 0 {
		//TODO:implement it
	} else {
		//string<lenenc> instead of string<EOF> in the manual of mysql
		pos = mp.writeStringLenEnc(data, pos, message)
		return data[:pos]
	}
	return data[:pos]
}

func (mp *MysqlProtocolImpl) makeOKPayloadWithEof(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	data := make([]byte, HeaderOffset+128+len(message)+10)
	var pos = HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.EOFHeader)
	pos = mp.writeIntLenEnc(data, pos, affectedRows)
	pos = mp.writeIntLenEnc(data, pos, lastInsertId)
	if (mp.capability & CLIENT_PROTOCOL_41) != 0 {
		pos = mp.io.WriteUint16(data, pos, statusFlags)
		pos = mp.io.WriteUint16(data, pos, warnings)
	} else if (mp.capability & CLIENT_TRANSACTIONS) != 0 {
		pos = mp.io.WriteUint16(data, pos, statusFlags)
	}

	if mp.capability&CLIENT_SESSION_TRACK != 0 {
		//TODO:implement it
	} else {
		//string<lenenc> instead of string<EOF> in the manual of mysql
		pos = mp.writeStringLenEnc(data, pos, message)
		return data[:pos]
	}
	return data[:pos]
}

func (mp *MysqlProtocolImpl) makeLocalInfileRequestPayload(filename string) []byte {
	data := make([]byte, HeaderOffset+1+len(filename)+1)
	pos := HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.LocalInFileHeader)
	pos = mp.writeStringFix(data, pos, filename, len(filename))
	return data[:pos]
}

func (mp *MysqlProtocolImpl) WriteLocalInfileRequest(filename string) error {
	req := mp.makeLocalInfileRequestPayload(filename)
	return mp.writePackets(req)
}

func (mp *MysqlProtocolImpl) sendOKPacketWithEof(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	okPkt := mp.makeOKPayloadWithEof(affectedRows, lastInsertId, status, warnings, message)
	return mp.writePackets(okPkt)
}

// send OK packet to the client
func (mp *MysqlProtocolImpl) sendOKPacket(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	okPkt := mp.makeOKPayload(affectedRows, lastInsertId, status, warnings, message)
	return mp.writePackets(okPkt)
}

// make Err packet
func (mp *MysqlProtocolImpl) makeErrPayload(errorCode uint16, sqlState, errorMessage string) []byte {
	data := make([]byte, HeaderOffset+9+len(errorMessage))
	pos := HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.ErrHeader)
	pos = mp.io.WriteUint16(data, pos, errorCode)
	if mp.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = mp.io.WriteUint8(data, pos, '#')
		if len(sqlState) < 5 {
			stuff := "      "
			sqlState += stuff[:5-len(sqlState)]
		}
		pos = mp.writeStringFix(data, pos, sqlState, 5)
	}
	pos = mp.writeStringFix(data, pos, errorMessage, len(errorMessage))
	return data[:pos]
}

/*
the server sends the Error packet

information from https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
mysql version 8.0.23
usually it is in the directory /usr/local/include/mysql/mysqld_error.h

Error information includes several elements: an error code, SQLSTATE value, and message string.

	Error code: This value is numeric. It is MySQL-specific and is not portable to other database systems.
	SQLSTATE value: This value is a five-character string (for example, '42S02'). SQLSTATE values are taken from ANSI SQL and ODBC and are more standardized than the numeric error codes.
	Message string: This string provides a textual description of the error.
*/
func (mp *MysqlProtocolImpl) sendErrPacket(errorCode uint16, sqlState, errorMessage string) error {
	if mp.ses != nil {
		mp.ses.GetErrInfo().push(errorCode, errorMessage)
	}
	errPkt := mp.makeErrPayload(errorCode, sqlState, errorMessage)
	return mp.writePackets(errPkt)
}

func (mp *MysqlProtocolImpl) makeEOFPayload(warnings, status uint16) []byte {
	data := make([]byte, HeaderOffset+10)
	pos := HeaderOffset
	pos = mp.io.WriteUint8(data, pos, defines.EOFHeader)
	if mp.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = mp.io.WriteUint16(data, pos, warnings)
		pos = mp.io.WriteUint16(data, pos, status)
	}
	return data[:pos]
}

func (mp *MysqlProtocolImpl) sendEOFPacket(warnings, status uint16) error {
	data := mp.makeEOFPayload(warnings, status)
	return mp.writePackets(data)
}

func (mp *MysqlProtocolImpl) SendEOFPacketIf(warnings, status uint16) error {
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is not set, EOF_Packet
	if mp.capability&CLIENT_DEPRECATE_EOF == 0 {
		return mp.sendEOFPacket(warnings, status)
	} else {
		return mp.tcpConn.Flush()
	}
}

// the OK or EOF packet
// thread safe
func (mp *MysqlProtocolImpl) sendEOFOrOkPacket(warnings, status uint16) error {
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is set, OK_Packet; else EOF_Packet.
	if mp.capability&CLIENT_DEPRECATE_EOF != 0 {
		return mp.sendOKPacketWithEof(0, 0, status, 0, "")
	} else {
		return mp.sendEOFPacket(warnings, status)
	}
}

func setColLength(column *MysqlColumn, width int32) {
	column.length = column.columnType.GetLength(width)
}

func setColFlag(column *MysqlColumn) {
	if column.auto_incr {
		column.flag |= uint16(defines.AUTO_INCREMENT_FLAG)
	}
}

func setCharacter(column *MysqlColumn) {
	switch column.columnType {
	// blob type should use 0x3f to show the binary data
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_STRING, defines.MYSQL_TYPE_TEXT:
		column.SetCharset(charsetVarchar)
	case defines.MYSQL_TYPE_VAR_STRING:
		column.SetCharset(charsetVarchar)
	default:
		column.SetCharset(charsetBinary)
	}
}

// make the column information with the format of column definition41
func (mp *MysqlProtocolImpl) makeColumnDefinition41Payload(column *MysqlColumn, cmd int) []byte {
	space := HeaderOffset + 8*9 + //lenenc bytes of 8 fields
		21 + //fixed-length fields
		3 + // catalog "def"
		len(column.Schema()) +
		len(column.Table()) +
		len(column.OrgTable()) +
		len(column.Name()) +
		len(column.OrgName()) +
		len(column.DefaultValue()) +
		100 // for safe

	data := make([]byte, space)
	pos := HeaderOffset

	//lenenc_str     catalog(always "def")
	pos = mp.writeStringLenEnc(data, pos, "def")

	//lenenc_str     schema
	pos = mp.writeStringLenEnc(data, pos, column.Schema())

	//lenenc_str     table
	pos = mp.writeStringLenEnc(data, pos, column.Table())

	//lenenc_str     org_table
	pos = mp.writeStringLenEnc(data, pos, column.OrgTable())

	//lenenc_str     name
	pos = mp.writeStringLenEnc(data, pos, column.Name())

	//lenenc_str     org_name
	pos = mp.writeStringLenEnc(data, pos, column.OrgName())

	//lenenc_int     length of fixed-length fields [0c]
	pos = mp.io.WriteUint8(data, pos, 0x0c)

	if column.ColumnType() == defines.MYSQL_TYPE_BOOL {
		//int<2>              character set
		pos = mp.io.WriteUint16(data, pos, charsetVarchar)
		//int<4>              column length
		pos = mp.io.WriteUint32(data, pos, boolColumnLength)
		//int<1>              type
		pos = mp.io.WriteUint8(data, pos, uint8(defines.MYSQL_TYPE_VARCHAR))
	} else {
		//int<2>              character set
		pos = mp.io.WriteUint16(data, pos, column.Charset())
		//int<4>              column length
		pos = mp.io.WriteUint32(data, pos, column.Length())
		//int<1>              type
		pos = mp.io.WriteUint8(data, pos, uint8(column.ColumnType()))
	}

	//int<2>              flags
	pos = mp.io.WriteUint16(data, pos, column.Flag())

	//int<1>              decimals
	pos = mp.io.WriteUint8(data, pos, column.Decimal())

	//int<2>              filler [00] [00]
	pos = mp.io.WriteUint16(data, pos, 0)

	if CommandType(cmd) == COM_FIELD_LIST {
		pos = mp.writeIntLenEnc(data, pos, uint64(len(column.DefaultValue())))
		pos = mp.writeCountOfBytes(data, pos, column.DefaultValue())
	}

	return data[:pos]
}

// SendColumnDefinitionPacket the server send the column definition to the client
func (mp *MysqlProtocolImpl) SendColumnDefinitionPacket(ctx context.Context, column Column, cmd int) error {
	mysqlColumn, ok := column.(*MysqlColumn)
	if !ok {
		return moerr.NewInternalError(ctx, "sendColumn need MysqlColumn")
	}

	var data []byte
	if mp.capability&CLIENT_PROTOCOL_41 != 0 {
		data = mp.makeColumnDefinition41Payload(mysqlColumn, cmd)
	}

	return mp.appendPacket(data)
}

// SendColumnCountPacket makes the column count packet
func (mp *MysqlProtocolImpl) SendColumnCountPacket(count uint64) error {
	data := make([]byte, HeaderOffset+20)
	pos := HeaderOffset
	pos = mp.writeIntLenEnc(data, pos, count)
	return mp.appendPacket(data[:pos])
}

func (mp *MysqlProtocolImpl) sendColumns(ctx context.Context, mrs *MysqlResultSet, cmd int, warnings, status uint16) error {
	//column_count * Protocol::ColumnDefinition packets
	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		var col Column
		col, err := mrs.GetColumn(ctx, i)
		if err != nil {
			return err
		}

		err = mp.SendColumnDefinitionPacket(ctx, col, cmd)
		if err != nil {
			return err
		}
	}

	//If the CLIENT_DEPRECATE_EOF client capabilities flag is not set, EOF_Packet
	if mp.capability&CLIENT_DEPRECATE_EOF == 0 {
		err := mp.sendEOFPacket(warnings, status)
		if err != nil {
			return err
		}
	}
	return nil
}
func (mp *MysqlProtocolImpl) appendNullBitMap(mrs *MysqlResultSet, columnsLength, rowIdx uint64) error {
	buffer := mp.binaryNullBuffer[:0]

	numBytes4Null := (columnsLength + 7 + 2) / 8
	for i := uint64(0); i < numBytes4Null; i++ {
		buffer = append(buffer, 0)
	}
	for i := uint64(0); i < columnsLength; i++ {
		if isNil, err := mrs.ColumnIsNull(mp.ctx, rowIdx, i); err != nil {
			return err
		} else if isNil {
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			idx := int(bytePos)
			buffer[idx] |= 1 << bitPos
			continue
		}
	}
	err := mp.append(buffer...)
	if err != nil {
		return err
	}
	return nil
}

// the server convert every row of the result set into the format that mysql protocol needs
func (mp *MysqlProtocolImpl) appendResultSetBinaryRow(mrs *MysqlResultSet, rowIdx uint64) error {
	err := mp.beginPacket()
	if err != nil {
		return err
	}
	err = mp.append(defines.OKHeader) // append OkHeader
	if err != nil {
		return err
	}
	columnsLength := mrs.GetColumnCount()
	// get null buffer
	err = mp.appendNullBitMap(mrs, columnsLength, rowIdx)
	if err != nil {
		return err
	}

	for i := uint64(0); i < columnsLength; i++ {
		if isNil, err := mrs.ColumnIsNull(mp.ctx, rowIdx, i); err != nil {
			return err
		} else if isNil {
			continue
		}

		column, err := mrs.GetColumn(mp.ctx, uint64(i))
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return moerr.NewInternalError(mp.ctx, "sendColumn need MysqlColumn")
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_BOOL:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TINY:
			if value, err := mrs.GetInt64(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendUint8(uint8(value))
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_YEAR:
			if value, err := mrs.GetInt64(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendUint16(uint16(value))
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
			if value, err := mrs.GetInt64(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendUint32(uint32(value))
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if value, err := mrs.GetUint64(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendUint64(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_FLOAT:
			if value, err := mrs.GetValue(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				switch v := value.(type) {
				case float32:
					err = mp.appendUint32(math.Float32bits(v))
					if err != nil {
						return err
					}
				case float64:
					err = mp.appendUint32(math.Float32bits(float32(v)))
					if err != nil {
						return err
					}
				default:
				}
			}
		case defines.MYSQL_TYPE_DOUBLE:
			if value, err := mrs.GetValue(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				switch v := value.(type) {
				case float32:
					err = mp.appendUint64(math.Float64bits(float64(v)))
					if err != nil {
						return err
					}
				case float64:
					err = mp.appendUint64(math.Float64bits(v))
					if err != nil {
						return err
					}
				default:
				}
			}

		// Binary/varbinary will be sent out as varchar type.
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT, defines.MYSQL_TYPE_JSON:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		// TODO: some type, we use string now. someday need fix it
		case defines.MYSQL_TYPE_DECIMAL:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_UUID:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATE:
			if value, err := mrs.GetValue(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				err = mp.appendDate(value.(types.Date))
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TIME:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				var t types.Time
				var err error
				idx := strings.Index(value, ".")
				if idx == -1 {
					t, err = types.ParseTime(value, 0)
				} else {
					t, err = types.ParseTime(value, int32(len(value)-idx-1))
				}
				err = mp.appendTime(t)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP:
			if value, err := mrs.GetString(mp.ctx, rowIdx, i); err != nil {
				return err
			} else {
				var dt types.Datetime
				var err error
				idx := strings.Index(value, ".")
				if idx == -1 {
					dt, err = types.ParseDatetime(value, 0)
				} else {
					dt, err = types.ParseDatetime(value, int32(len(value)-idx-1))
				}
				err = mp.appendDatetime(dt)
				if err != nil {
					return err
				}
			}
			// case defines.MYSQL_TYPE_TIMESTAMP:
			// 	if value, err := mrs.GetString(rowIdx, i); err != nil {
			// 		return nil, err
			// 	} else {
			// 		data = err = mp.appendStringLenEnc(data, value)
			if err != nil {
				return err
			}
		// 	}
		default:
			return moerr.NewInternalError(mp.ctx, "type is not supported in binary text result row")
		}
	}

	err = mp.finishedPacket()
	if err != nil {
		return err
	}
	return nil
}

// the server convert every row of the result set into the format that mysql protocol needs
func (mp *MysqlProtocolImpl) appendResultSetTextRow(mrs *MysqlResultSet, r uint64) error {
	err := mp.beginPacket()
	if err != nil {
		return err
	}
	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		column, err := mrs.GetColumn(mp.ctx, i)
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return moerr.NewInternalError(mp.ctx, "sendColumn need MysqlColumn")
		}

		if isNil, err1 := mrs.ColumnIsNull(mp.ctx, r, i); err1 != nil {
			return err1
		} else if isNil {
			//NULL is sent as 0xfb
			err = mp.appendUint8(0xFB)
			if err != nil {
				return err
			}
			continue
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_JSON:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_BOOL:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_BIT:
			if value, err2 := mrs.GetUint64(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				bitLength := mysqlColumn.ColumnImpl.Length()
				byteLength := (bitLength + 7) / 8
				b := types.EncodeUint64(&value)[:byteLength]
				slices.Reverse(b)
				err = mp.appendStringLenEnc(string(b))
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DECIMAL:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_UUID:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			if value, err2 := mrs.GetInt64(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
					if value == 0 {
						err = mp.appendStringLenEnc("0000")
						if err != nil {
							return err
						}
					} else {
						err = mp.appendStringLenEncOfInt64(value)
						if err != nil {
							return err
						}
					}
				} else {
					err = mp.appendStringLenEncOfInt64(value)
					if err != nil {
						return err
					}
				}
			}
		case defines.MYSQL_TYPE_FLOAT:
			if value, err := mrs.GetValue(mp.ctx, r, i); err != nil {
				return err
			} else {
				switch v := value.(type) {
				case float32:
					err = mp.appendStringLenEncOfFloat64(float64(v), 32)
					if err != nil {
						return err
					}
				case float64:
					err = mp.appendStringLenEncOfFloat64(v, 32)
					if err != nil {
						return err
					}
				default:
				}
			}
		case defines.MYSQL_TYPE_DOUBLE:
			if value, err := mrs.GetValue(mp.ctx, r, i); err != nil {
				return err
			} else {
				switch v := value.(type) {
				case float32:
					err = mp.appendStringLenEncOfFloat64(float64(v), 64)
					if err != nil {
						return err
					}
				case float64:
					err = mp.appendStringLenEncOfFloat64(v, 64)
					if err != nil {
						return err
					}
				default:
				}
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err2 := mrs.GetUint64(mp.ctx, r, i); err2 != nil {
					return err2
				} else {
					err = mp.appendStringLenEncOfUint64(value)
					if err != nil {
						return err
					}
				}
			} else {
				if value, err2 := mrs.GetInt64(mp.ctx, r, i); err2 != nil {
					return err2
				} else {
					err = mp.appendStringLenEncOfInt64(value)
					if err != nil {
						return err
					}
				}
			}
		// Binary/varbinary will be sent out as varchar type.
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATE:
			if value, err2 := mrs.GetValue(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value.(types.Date).String())
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATETIME:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TIME:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_ENUM:
			if value, err2 := mrs.GetString(mp.ctx, r, i); err2 != nil {
				return err2
			} else {
				err = mp.appendStringLenEnc(value)
				if err != nil {
					return err
				}
			}
		default:
			return moerr.NewInternalError(mp.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	err = mp.finishedPacket()
	if err != nil {
		return err
	}
	return nil
}

// the server send group row of the result set as an independent packet
// thread safe
func (mp *MysqlProtocolImpl) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	if cnt == 0 {
		return nil
	}

	mp.m.Lock()
	defer mp.m.Unlock()
	var err error = nil

	for i := uint64(0); i < cnt; i++ {
		if err = mp.sendResultSetTextRow(mrs, i); err != nil {
			return err
		}
	}
	return err
}

func (mp *MysqlProtocolImpl) WriteResultSetRow(mrs *MysqlResultSet, cnt uint64) error {
	if cnt == 0 {
		return nil
	}

	cmd := mp.GetSession().GetCmd()
	mp.m.Lock()
	defer mp.m.Unlock()
	var err error = nil

	// XXX now we known COM_QUERY will use textRow, COM_STMT_EXECUTE use binaryRow
	useBinaryRow := cmd == COM_STMT_EXECUTE

	//make rows into the batch
	for i := uint64(0); i < cnt; i++ {
		//begin1 := time.Now()
		if useBinaryRow {
			err = mp.appendResultSetBinaryRow(mrs, i)
		} else {
			err = mp.appendResultSetTextRow(mrs, i)
		}
		if err != nil {
			//ERR_Packet in case of error
			err1 := mp.sendErrPacket(moerr.ER_UNKNOWN_ERROR, DefaultMySQLState, err.Error())
			if err1 != nil {
				return err1
			}
			return err
		}
	}

	return err
}
func (mp *MysqlProtocolImpl) UseConn(conn net.Conn) {
	mp.tcpConn.UseConn(conn)
}

func (mp *MysqlProtocolImpl) beginPacket() error {
	err := mp.tcpConn.BeginPacket()
	return err
}

func (mp *MysqlProtocolImpl) finishedPacket() error {
	err := mp.tcpConn.FinishedPacket()
	return err
}

func (mp *MysqlProtocolImpl) appendPacket(payload []byte) error {
	err := mp.beginPacket()
	if err != nil {
		return err
	}

	err = mp.append(payload...)
	if err != nil {
		return err
	}

	err = mp.finishedPacket()
	if err != nil {
		return err
	}
	return nil
}

func (mp *MysqlProtocolImpl) append(elems ...byte) error {
	err := mp.tcpConn.Append(elems...)
	return err
}

func (mp *MysqlProtocolImpl) appendDatetime(dt types.Datetime) error {
	if dt.MicroSec() != 0 {
		err := mp.append(11)
		if err != nil {
			return err
		}
		err = mp.appendUint16(uint16(dt.Year()))
		if err != nil {
			return err
		}
		err = mp.append(dt.Month(), dt.Day(), byte(dt.Hour()), byte(dt.Minute()), byte(dt.Sec()))
		if err != nil {
			return err
		}
		err = mp.appendUint32(uint32(dt.MicroSec()))
		if err != nil {
			return err
		}
	} else if dt.Hour() != 0 || dt.Minute() != 0 || dt.Sec() != 0 {
		err := mp.append(7)
		if err != nil {
			return err
		}
		err = mp.appendUint16(uint16(dt.Year()))
		if err != nil {
			return err
		}
		err = mp.append(dt.Month(), dt.Day(), byte(dt.Hour()), byte(dt.Minute()), byte(dt.Sec()))
		if err != nil {
			return err
		}
	} else {
		err := mp.append(4)
		if err != nil {
			return err
		}
		err = mp.appendUint16(uint16(dt.Year()))
		if err != nil {
			return err
		}
		err = mp.append(dt.Month(), dt.Day())
		if err != nil {
			return err
		}
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendTime(t types.Time) error {
	if int64(t) == 0 {
		err := mp.append(0)
		if err != nil {
			return err
		}
	} else {
		hour, minute, sec, msec, isNeg := t.ClockFormat()
		day := uint32(hour / 24)
		hour = hour % 24
		if msec != 0 {
			err := mp.append(12)
			if err != nil {
				return err
			}
			if isNeg {
				err = mp.append(1)
				if err != nil {
					return err
				}
			} else {
				err = mp.append(0)
				if err != nil {
					return err
				}
			}
			err = mp.appendUint32(day)
			if err != nil {
				return err
			}
			err = mp.append(uint8(hour), minute, sec)
			if err != nil {
				return err
			}
			err = mp.appendUint64(msec)
			if err != nil {
				return err
			}
		} else {
			err := mp.append(8)
			if err != nil {
				return err
			}
			if isNeg {
				err = mp.append(1)
				if err != nil {
					return err
				}
			} else {
				err = mp.append(0)
				if err != nil {
					return err
				}
			}
			err = mp.appendUint32(day)
			if err != nil {
				return err
			}
			err = mp.append(uint8(hour), minute, sec)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (mp *MysqlProtocolImpl) appendDate(value types.Date) error {
	if int32(value) == 0 {
		err := mp.append(0)
		if err != nil {
			return err
		}
	} else {
		err := mp.append(4)
		if err != nil {
			return err
		}
		err = mp.appendUint16(value.Year())
		if err != nil {
			return err
		}
		err = mp.append(value.Month(), value.Day())
		if err != nil {
			return err
		}
	}
	return nil
}

// the server send every row of the result set as an independent packet
// thread safe
func (mp *MysqlProtocolImpl) SendResultSetTextRow(mrs *MysqlResultSet, r uint64) error {
	mp.m.Lock()
	defer mp.m.Unlock()

	return mp.sendResultSetTextRow(mrs, r)
}

// the server send every row of the result set as an independent packet
func (mp *MysqlProtocolImpl) sendResultSetTextRow(mrs *MysqlResultSet, r uint64) error {
	var err error
	if err = mp.appendResultSetTextRow(mrs, r); err != nil {
		//ERR_Packet in case of error
		err1 := mp.sendErrPacket(moerr.ER_UNKNOWN_ERROR, DefaultMySQLState, err.Error())
		if err1 != nil {
			return err1
		}
		return err
	}
	return nil
}

// the server send the result set of execution the client
// the routine follows the article: https://dev.mysql.com/doc/internals/en/com-query-response.html
func (mp *MysqlProtocolImpl) sendResultSet(ctx context.Context, set ResultSet, cmd int, warnings, status uint16) error {
	mysqlRS, ok := set.(*MysqlResultSet)
	if !ok {
		return moerr.NewInternalError(ctx, "sendResultSet need MysqlResultSet")
	}

	//A packet containing a Protocol::LengthEncodedInteger column_count
	err := mp.SendColumnCountPacket(mysqlRS.GetColumnCount())
	if err != nil {
		return err
	}

	if err = mp.sendColumns(ctx, mysqlRS, cmd, warnings, status); err != nil {
		return err
	}

	//One or more ProtocolText::ResultsetRow packets, each containing column_count values
	for i := uint64(0); i < mysqlRS.GetRowCount(); i++ {
		if err = mp.sendResultSetTextRow(mysqlRS, i); err != nil {
			return err
		}
	}

	//If the CLIENT_DEPRECATE_EOF client capabilities flag is set, OK_Packet; else EOF_Packet.
	if mp.capability&CLIENT_DEPRECATE_EOF != 0 {
		err := mp.sendOKPacketWithEof(0, 0, extendStatus(status), 0, "")
		if err != nil {
			return err
		}
	} else {
		err := mp.sendEOFPacket(warnings, extendStatus(status))
		if err != nil {
			return err
		}
	}

	return nil
}

// the server sends the payload to the client
func (mp *MysqlProtocolImpl) writePackets(payload []byte) error {
	err := mp.tcpConn.Write(payload)
	if err != nil {
		return err
	}
	return nil
}

// MakeHandshakePayload exposes (*MysqlProtocolImpl).makeHandshakeV10Payload() function.
func (mp *MysqlProtocolImpl) MakeHandshakePayload() []byte {
	return mp.makeHandshakeV10Payload()
}

// WritePacket exposes (*MysqlProtocolImpl).writePackets() function.
func (mp *MysqlProtocolImpl) WritePacket(payload []byte) error {
	return mp.writePackets(payload)
}

// MakeOKPayload exposes (*MysqlProtocolImpl).makeOKPayload() function.
func (mp *MysqlProtocolImpl) MakeOKPayload(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	return mp.makeOKPayload(affectedRows, lastInsertId, statusFlags, warnings, message)
}

// MakeErrPayload exposes (*MysqlProtocolImpl).makeErrPayload() function.
func (mp *MysqlProtocolImpl) MakeErrPayload(errCode uint16, sqlState, errorMessage string) []byte {
	return mp.makeErrPayload(errCode, sqlState, errorMessage)
}

// MakeEOFPayload exposes (*MysqlProtocolImpl).makeEOFPayload() function.
func (mp *MysqlProtocolImpl) MakeEOFPayload(warnings, status uint16) []byte {
	return mp.makeEOFPayload(warnings, status)
}

// receiveExtraInfo tries to receive salt and labels read from proxy module.
func (mp *MysqlProtocolImpl) receiveExtraInfo(rs *Conn) {
	// TODO(volgariver6): when proxy is stable, remove this deadline setting.
	if err := rs.RawConn().SetReadDeadline(time.Now().Add(defaultSaltReadTimeout)); err != nil {
		mp.ses.Debugf(mp.ctx, "failed to set deadline for salt updating: %v", err)
		return
	}
	var i proxy.ExtraInfo
	reader := bufio.NewReader(rs.RawConn())
	if err := i.Decode(reader); err != nil {
		// If the error is timeout, we treat it as normal case and do not update extra info.
		if err, ok := err.(net.Error); ok && err.Timeout() {
			mp.ses.Info(mp.ctx, "cannot get salt, maybe not use proxy",
				zap.Error(err))
		} else {
			mp.ses.Error(mp.ctx, "failed to get extra info",
				zap.Error(err))
		}
		return
	}

	// must from proxy if extraInfo is received
	mp.GetSession().fromProxy = true
	mp.SetSalt(i.Salt)
	mp.connectionID = i.ConnectionID
	ses := mp.GetSession()
	ses.requestLabel = i.Label
	if i.InternalConn {
		ses.connType = ConnTypeInternal
	} else {
		ses.connType = ConnTypeExternal
	}
	ses.clientAddr = i.ClientAddr
	ses.proxyAddr = mp.Peer()
}

func generate_salt(n int) []byte {
	buf := make([]byte, n)
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	r.Read(buf)
	for i := 0; i < n; i++ {
		buf[i] &= 0x7f
		if buf[i] == 0 || buf[i] == '$' {
			buf[i]++
		}
	}
	return buf
}
func NewMysqlClientProtocol(connectionID uint32, tcp *Conn, maxBytesToFlush int, SV *config.FrontendParameters) *MysqlProtocolImpl {
	salt := generate_salt(20)
	mysql := &MysqlProtocolImpl{
		io:               NewIOPackage(true),
		tcpConn:          tcp,
		salt:             salt,
		connectionID:     connectionID,
		charset:          "utf8mb4",
		capability:       DefaultCapability,
		strconvBuffer:    make([]byte, 0, 16*1024),
		lenEncBuffer:     make([]byte, 0, 10),
		binaryNullBuffer: make([]byte, 0, 512),
		SV:               SV,
	}

	if SV.EnableTls {
		mysql.capability = mysql.capability | CLIENT_SSL
	}

	return mysql
}

// HashSha1 is used to calcute a sha1 hash
// SHA1()
func HashSha1(toHash []byte) []byte {
	sha := sha1.New()
	_, err := sha.Write(toHash)
	if err != nil {
		logutil.Errorf("SHA1 failed.")
		return nil
	}
	return sha.Sum(nil)
}

// HashPassWord is uesed to hash password
// *SHA1(SHA1(password))
func HashPassWord(pwd string) string {
	if len(pwd) == 0 {
		return ""
	}
	hash1 := HashSha1([]byte(pwd))
	hash2 := HashSha1(hash1)

	return fmt.Sprintf("*%X", hash2)
}

func HashPassWordWithByte(pwd []byte) string {
	if len(pwd) == 0 {
		return ""
	}
	hash1 := HashSha1(pwd)
	hash2 := HashSha1(hash1)

	return fmt.Sprintf("*%X", hash2)
}

// GetPassWord is used to get hash byte password
// SHA1(SHA1(password))
func GetPassWord(pwd string) ([]byte, error) {
	pwdByte, err := hex.DecodeString(pwd[1:])
	if err != nil {
		logutil.Errorf("GetPassWord failed.")
	}
	return pwdByte, nil
}
