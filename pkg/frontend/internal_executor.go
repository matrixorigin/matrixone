// Copyright 2022 Matrix Origin
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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

func applyOverride(sess *Session, opts ie.SessionOverrideOptions) {
	if opts.Database != nil {
		sess.SetDatabaseName(*opts.Database)
	}

	if opts.Username != nil {
		sess.protocol.SetUserName(*opts.Username)
	}

	if opts.IsInternal != nil {
		sess.IsInternal = *opts.IsInternal
	}
}

type internalMiniExec interface {
	doComQuery(requestCtx context.Context, sql string) error
	PrepareSessionBeforeExecRequest(*Session)
}

type internalExecutor struct {
	sync.Mutex
	proto        *internalProtocol
	executor     internalMiniExec // MySqlCmdExecutor struct impls miniExec
	pu           *config.ParameterUnit
	baseSessOpts ie.SessionOverrideOptions
}

func NewInternalExecutor(pu *config.ParameterUnit) *internalExecutor {
	return newIe(pu, NewMysqlCmdExecutor())
}

func newIe(pu *config.ParameterUnit, inner internalMiniExec) *internalExecutor {
	proto := &internalProtocol{result: &internalExecResult{}}
	ret := &internalExecutor{
		proto:        proto,
		executor:     inner,
		pu:           pu,
		baseSessOpts: ie.NewOptsBuilder().Finish(),
	}
	return ret
}

type internalExecResult struct {
	affectedRows uint64
	resultSet    *MysqlResultSet
	dropped      uint64
	err          error
}

func (res *internalExecResult) Error() error {
	return res.err
}

func (res *internalExecResult) ColumnCount() uint64 {
	return res.resultSet.GetColumnCount()
}

func (res *internalExecResult) Column(i uint64) (name string, typ uint8, signed bool, err error) {
	col, err := res.resultSet.GetColumn(i)
	if err == nil {
		name = col.Name()
		typ = col.ColumnType()
		signed = col.IsSigned()
	}
	return
}

func (res *internalExecResult) RowCount() uint64 {
	return res.resultSet.GetRowCount()
}

func (res *internalExecResult) Row(i uint64) ([]interface{}, error) {
	return res.resultSet.GetRow(i)
}

func (res *internalExecResult) Value(ridx uint64, cidx uint64) (interface{}, error) {
	return res.resultSet.GetValue(ridx, cidx)
}

func (res *internalExecResult) ValueByName(ridx uint64, col string) (interface{}, error) {
	return res.resultSet.GetValueByName(ridx, col)
}

func (res *internalExecResult) StringValueByName(ridx uint64, col string) (string, error) {
	if cidx, err := res.resultSet.columnName2Index(col); err != nil {
		return "", err
	} else {
		return res.resultSet.GetString(ridx, cidx)
	}
}

func (ie *internalExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (err error) {
	ie.Lock()
	defer ie.Unlock()
	sess := ie.newCmdSession(ctx, opts)
	defer sess.Dispose()
	ie.executor.PrepareSessionBeforeExecRequest(sess)
	ie.proto.stashResult = false
	return ie.executor.doComQuery(ctx, sql)
}

func (ie *internalExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	ie.Lock()
	defer ie.Unlock()
	sess := ie.newCmdSession(ctx, opts)
	defer sess.Dispose()
	ie.executor.PrepareSessionBeforeExecRequest(sess)
	ie.proto.stashResult = true
	err := ie.executor.doComQuery(ctx, sql)
	res := ie.proto.swapOutResult()
	res.err = err
	return res
}

func (ie *internalExecutor) newCmdSession(ctx context.Context, opts ie.SessionOverrideOptions) *Session {
	// Use the Mid configuration for session. We can make Mid a configuration
	// param, or, compute from GuestMmuLimitation.   Lazy.
	//
	// XXX MPOOL
	// Cannot use Mid.   Turns out we create a Session for *EVERY QUERY*
	// If we preallocate anything, we will explode.
	//
	// Session does not have a close call.   We need a Close() call in the Exec/Query method above.
	//
	mp, err := mpool.NewMPool("internal_exec_cmd_session", ie.pu.SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		logutil.Fatalf("internalExecutor cannot create mpool in newCmdSession")
		panic(err)
	}
	sess := NewSession(ie.proto, mp, ie.pu, gSysVariables)
	sess.SetRequestContext(ctx)
	applyOverride(sess, ie.baseSessOpts)
	applyOverride(sess, opts)
	return sess
}

func (ie *internalExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {
	ie.baseSessOpts = opts
}

// func showCaller() {
// 	pc, _, _, _ := runtime.Caller(1)
// 	callFunc := runtime.FuncForPC(pc)
// 	logutil.Infof("[Metric] called: %s", callFunc.Name())
// }

var _ MysqlProtocol = &internalProtocol{}

type internalProtocol struct {
	sync.Mutex
	stashResult bool
	result      *internalExecResult
	database    string
	username    string
}

func (ip *internalProtocol) IsEstablished() bool {
	return true
}

func (ip *internalProtocol) ParseExecuteData(stmt *PrepareStmt, data []byte, pos int) (names []string, vars []any, err error) {
	return nil, nil, nil
}

func (ip *internalProtocol) SendPrepareResponse(stmt *PrepareStmt) error {
	return nil
}

func (ip *internalProtocol) SetEstablished() {}

func (ip *internalProtocol) GetRequest(payload []byte) *Request {
	panic("not impl")
}

// ConnectionID the identity of the client
func (ip *internalProtocol) ConnectionID() uint32 {
	return 74751101
}

// Peer gets the address [Host:Port] of the client
func (ip *internalProtocol) Peer() (string, string) {
	panic("not impl")
}

func (ip *internalProtocol) GetDatabaseName() string {
	return ip.database
}

func (ip *internalProtocol) SetDatabaseName(database string) {
	ip.database = database
}

func (ip *internalProtocol) GetUserName() string {
	return ip.username
}

func (ip *internalProtocol) SetUserName(username string) {
	ip.username = username
}

func (ip *internalProtocol) Quit() {}

func (ip *internalProtocol) sendRows(mrs *MysqlResultSet, cnt uint64) error {
	if ip.stashResult {
		res := ip.result.resultSet
		if res == nil {
			res = &MysqlResultSet{}
			ip.result.resultSet = res
		}

		if res.GetRowCount() > 100 {
			ip.result.dropped += cnt
			return nil
		}

		if res.GetColumnCount() == 0 {
			for _, col := range mrs.Columns {
				res.AddColumn(col)
			}
		}
		colCnt := res.GetColumnCount()
		for i := uint64(0); i < cnt; i++ {
			row := make([]any, colCnt)
			copy(row, mrs.Data[i])
			res.Data = append(res.Data, row)
		}
	}

	ip.result.affectedRows += cnt
	return nil
}

func (ip *internalProtocol) swapOutResult() *internalExecResult {
	ret := ip.result
	if ret.resultSet == nil {
		ret.resultSet = &MysqlResultSet{}
	}
	ip.result = &internalExecResult{}
	return ret
}

// the server send group row of the result set as an independent packet thread safe
func (ip *internalProtocol) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	ip.Lock()
	defer ip.Unlock()
	return ip.sendRows(mrs, cnt)
}

func (ip *internalProtocol) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, cnt uint64) error {
	ip.Lock()
	defer ip.Unlock()
	return ip.sendRows(mrs, cnt)
}

// SendColumnDefinitionPacket the server send the column definition to the client
func (ip *internalProtocol) SendColumnDefinitionPacket(column Column, cmd int) error {
	return nil
}

// SendColumnCountPacket makes the column count packet
func (ip *internalProtocol) SendColumnCountPacket(count uint64) error {
	return nil
}

// SendResponse sends a response to the client for the application request
func (ip *internalProtocol) SendResponse(resp *Response) error {
	ip.Lock()
	defer ip.Unlock()
	ip.PrepareBeforeProcessingResultSet()
	if resp.category == ResultResponse {
		if mer := resp.data.(*MysqlExecutionResult); mer != nil && mer.Mrs() != nil {
			ip.sendRows(mer.Mrs(), mer.affectedRows)
		}
	} else {
		// OkResponse. this is NOT ErrorResponse because error will be returned by doComQuery
		ip.result.affectedRows = resp.affectedRows
	}
	return nil
}

// SendEOFPacketIf ends the sending of columns definations
func (ip *internalProtocol) SendEOFPacketIf(warnings uint16, status uint16) error {
	return nil
}

// sendOKPacket sends OK packet to the client, used in the end of sql like use <database>
func (ip *internalProtocol) sendOKPacket(affectedRows uint64, lastInsertId uint64, status uint16, warnings uint16, message string) error {
	ip.result.affectedRows = affectedRows
	return nil
}

// sendEOFOrOkPacket sends the OK or EOF packet thread safe, and ends the sending of result set
func (ip *internalProtocol) sendEOFOrOkPacket(warnings uint16, status uint16) error {
	return nil
}

func (ip *internalProtocol) PrepareBeforeProcessingResultSet() {
	ip.result.affectedRows = 0
	ip.result.dropped = 0
	ip.result.err = nil
	ip.result.resultSet = nil
}

func (ip *internalProtocol) GetStats() string { return "internal unknown stats" }
