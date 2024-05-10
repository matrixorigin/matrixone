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

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const DefaultTenantMoAdmin = "sys:internal:moadmin"

func applyOverride(sess *Session, opts ie.SessionOverrideOptions) {
	if opts.Database != nil {
		sess.SetDatabaseName(*opts.Database)
	}

	if opts.Username != nil {
		sess.GetMysqlProtocol().SetUserName(*opts.Username)
	}

	if opts.IsInternal != nil {
		sess.isInternal = *opts.IsInternal
	}

	acc := sess.GetTenantInfo()
	if acc != nil {
		if opts.AccountId != nil {
			acc.SetTenantID(*opts.AccountId)
		}

		if opts.UserId != nil {
			acc.SetUserID(*opts.UserId)
		}

		if opts.DefaultRoleId != nil {
			acc.SetDefaultRoleID(*opts.DefaultRoleId)
		}
	}

}

type internalExecutor struct {
	sync.Mutex
	proto        *internalProtocol
	baseSessOpts ie.SessionOverrideOptions
}

func NewInternalExecutor() *internalExecutor {
	return newIe()
}

func newIe() *internalExecutor {
	proto := &internalProtocol{result: &internalExecResult{}}
	ret := &internalExecutor{
		proto:        proto,
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

func (res *internalExecResult) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	col, err := res.resultSet.GetColumn(ctx, i)
	if err == nil {
		name = col.Name()
		typ = uint8(col.ColumnType())
		signed = col.IsSigned()
	}
	return
}

func (res *internalExecResult) RowCount() uint64 {
	return res.resultSet.GetRowCount()
}

func (res *internalExecResult) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return res.resultSet.GetRow(ctx, i)
}

func (res *internalExecResult) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return res.resultSet.GetValue(ctx, ridx, cidx)
}

func (res *internalExecResult) ValueByName(ctx context.Context, ridx uint64, col string) (interface{}, error) {
	return res.resultSet.GetValueByName(ctx, ridx, col)
}

func (res *internalExecResult) StringValueByName(ctx context.Context, ridx uint64, col string) (string, error) {
	if cidx, err := res.resultSet.columnName2Index(ctx, col); err != nil {
		return "", err
	} else {
		return res.resultSet.GetString(ctx, ridx, cidx)
	}
}

func (res *internalExecResult) Float64ValueByName(ctx context.Context, ridx uint64, col string) (float64, error) {
	if cidx, err := res.resultSet.columnName2Index(ctx, col); err != nil {
		return 0.0, err
	} else {
		return res.resultSet.GetFloat64(ctx, ridx, cidx)
	}
}

func (ie *internalExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (err error) {
	ie.Lock()
	defer ie.Unlock()
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, getGlobalPu().SV.SessionTimeout.Duration)
	defer cancel()
	sess := ie.newCmdSession(ctx, opts)
	defer func() {
		sess.Close()
	}()
	ie.proto.stashResult = false
	if sql == "" {
		return
	}
	tempExecCtx := ExecCtx{
		reqCtx: ctx,
		ses:    sess,
	}
	return doComQuery(sess, &tempExecCtx, &UserInput{sql: sql})
}

func (ie *internalExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	ie.Lock()
	defer ie.Unlock()
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, getGlobalPu().SV.SessionTimeout.Duration)
	defer cancel()
	sess := ie.newCmdSession(ctx, opts)
	defer sess.Close()
	ie.proto.stashResult = true
	logutil.Info("internalExecutor new session", trace.ContextField(ctx), zap.String("session uuid", sess.uuid.String()))
	tempExecCtx := ExecCtx{
		reqCtx: ctx,
		ses:    sess,
	}
	err := doComQuery(sess, &tempExecCtx, &UserInput{sql: sql})
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
	mp, err := mpool.NewMPool("internal_exec_cmd_session", getGlobalPu().SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		logutil.Fatalf("internalExecutor cannot create mpool in newCmdSession")
		panic(err)
	}
	sess := NewSession(ctx, ie.proto, mp, GSysVariables, true, nil)
	sess.disableTrace = true

	var t *TenantInfo
	if accountId, err := defines.GetAccountId(ctx); err == nil {
		t = &TenantInfo{
			TenantID:      accountId,
			UserID:        defines.GetUserId(ctx),
			DefaultRoleID: defines.GetRoleId(ctx),
		}
		if accountId == sysAccountID {
			t.Tenant = sysAccountName // fixme: fix empty tencent value, while do metric collection.
			t.User = "internal"
			// more details in authenticateUserCanExecuteStatementWithObjectTypeNone()
			t.DefaultRole = moAdminRoleName
		}
	} else {
		t, _ = GetTenantInfo(ctx, DefaultTenantMoAdmin)
	}
	sess.SetTenantInfo(t)
	applyOverride(sess, ie.baseSessOpts)
	applyOverride(sess, opts)

	//make sure init tasks can see the prev task's data
	now, _ := runtime.ProcessLevelRuntime().Clock().Now()
	sess.lastCommitTS = now
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

func (ip *internalProtocol) UpdateCtx(ctx context.Context) {

}

func (ip *internalProtocol) GetCapability() uint32 {
	return DefaultCapability
}

func (ip *internalProtocol) SetCapability(uint32) {

}

func (ip *internalProtocol) IsTlsEstablished() bool {
	return true
}

func (ip *internalProtocol) SetTlsEstablished() {
}

func (ip *internalProtocol) HandleHandshake(ctx context.Context, payload []byte) (bool, error) {
	return false, nil
}

func (ip *internalProtocol) Authenticate(ctx context.Context) error {
	return nil
}

func (ip *internalProtocol) GetTcpConnection() goetty.IOSession {
	return nil
}

func (ip *internalProtocol) GetDebugString() string {
	return "internal protocol"
}

func (ip *internalProtocol) GetSequenceId() uint8 {
	return 0
}

func (ip *internalProtocol) GetConnectAttrs() map[string]string {
	return nil
}

func (ip *internalProtocol) SetSequenceID(value uint8) {
}

func (ip *internalProtocol) IsEstablished() bool {
	return true
}

func (ip *internalProtocol) ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (ip *internalProtocol) ParseExecuteData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (ip *internalProtocol) SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
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
func (ip *internalProtocol) Peer() string {
	return "0.0.0.0:0"
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
func (ip *internalProtocol) SendColumnDefinitionPacket(ctx context.Context, column Column, cmd int) error {
	return nil
}

// SendColumnCountPacket makes the column count packet
func (ip *internalProtocol) SendColumnCountPacket(count uint64) error {
	return nil
}

// SendResponse sends a response to the client for the application request
func (ip *internalProtocol) SendResponse(ctx context.Context, resp *Response) error {
	ip.Lock()
	defer ip.Unlock()
	ip.ResetStatistics()
	if resp.category == ResultResponse {
		if mer := resp.data.(*MysqlExecutionResult); mer != nil && mer.Mrs() != nil {
			ip.sendRows(mer.Mrs(), mer.mrs.GetRowCount())
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

func (ip *internalProtocol) ResetStatistics() {
	ip.result.affectedRows = 0
	ip.result.dropped = 0
	ip.result.err = nil
	ip.result.resultSet = nil
}

func (ip *internalProtocol) GetStats() string { return "internal unknown stats" }

func (ip *internalProtocol) CalculateOutTrafficBytes(reset bool) (int64, int64) { return 0, 0 }

func (ip *internalProtocol) sendLocalInfileRequest(filename string) error {
	return nil
}

func (ip *internalProtocol) incDebugCount(int) {}

func (ip *internalProtocol) resetDebugCount() []uint64 {
	return nil
}

func (ip *internalProtocol) DisableAutoFlush() {
}

func (ip *internalProtocol) EnableAutoFlush() {
}

func (ip *internalProtocol) Flush() error {
	return nil
}
