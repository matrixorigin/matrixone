// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"io"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util"
)

const (
	DefaultRpcBufferSize = 1 << 10
)

type (
	TxnOperator = client.TxnOperator
	TxnClient   = client.TxnClient
	TxnOption   = client.TxnOption
)

type ComputationRunner interface {
	Run(ts uint64) (*util.RunResult, error)
}

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	ComputationRunner
	GetAst() tree.Statement

	GetProcess() *process.Process

	GetColumns(ctx context.Context) ([]interface{}, error)

	Compile(any any, fill func(*batch.Batch) error) (interface{}, error)

	GetUUID() []byte

	RecordExecPlan(ctx context.Context) error

	GetLoadTag() bool

	GetServerStatus() uint16
	Clear()
	Plan() *plan.Plan
	ResetPlanAndStmt(stmt tree.Statement)
	Free()
	ParamVals() []any
}

type ColumnInfo interface {
	GetName() string

	GetType() types.T
}

var _ ColumnInfo = &engineColumnInfo{}

type TableInfo interface {
	GetColumns()
}

type engineColumnInfo struct {
	name string
	typ  types.Type
}

func (ec *engineColumnInfo) GetName() string {
	return ec.name
}

func (ec *engineColumnInfo) GetType() types.T {
	return ec.typ.Oid
}

type PrepareStmt struct {
	Name           string
	Sql            string
	PreparePlan    *plan.Plan
	PrepareStmt    tree.Statement
	ParamTypes     []byte
	IsInsertValues bool
	InsertBat      *batch.Batch
	proc           *process.Process

	exprList [][]colexec.ExpressionExecutor

	params              *vector.Vector
	getFromSendLongData map[int]struct{}
}

/*
Disguise the COMMAND CMD_FIELD_LIST as sql query.
*/
const (
	cmdFieldListSql    = "__++__internal_cmd_field_list"
	cmdFieldListSqlLen = len(cmdFieldListSql)
	cloudUserTag       = "cloud_user"
	cloudNoUserTag     = "cloud_nonuser"
	saveResultTag      = "save_result"
)

var _ tree.Statement = &InternalCmdFieldList{}

// InternalCmdFieldList the CMD_FIELD_LIST statement
type InternalCmdFieldList struct {
	tableName string
}

// Free implements tree.Statement.
func (icfl *InternalCmdFieldList) Free() {
}

func (icfl *InternalCmdFieldList) String() string {
	return makeCmdFieldListSql(icfl.tableName)
}

func (icfl *InternalCmdFieldList) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(makeCmdFieldListSql(icfl.tableName))
}

func (icfl *InternalCmdFieldList) StmtKind() tree.StmtKind {
	return tree.MakeStmtKind(tree.OUTPUT_STATUS, tree.RESP_BY_SITUATION, tree.EXEC_IN_FRONTEND)
}

func (icfl *InternalCmdFieldList) GetStatementType() string { return "InternalCmd" }
func (icfl *InternalCmdFieldList) GetQueryType() string     { return tree.QueryTypeDQL }

// ExecResult is the result interface of the execution
type ExecResult interface {
	GetRowCount() uint64

	GetString(ctx context.Context, rindex, cindex uint64) (string, error)

	GetUint64(ctx context.Context, rindex, cindex uint64) (uint64, error)

	GetInt64(ctx context.Context, rindex, cindex uint64) (int64, error)
}

func execResultArrayHasData(arr []ExecResult) bool {
	return len(arr) != 0 && arr[0].GetRowCount() != 0
}

// BackgroundExec executes the sql in background session without network output.
type BackgroundExec interface {
	Close()
	Exec(context.Context, string) error
	ExecRestore(context.Context, string, uint32, uint32) error
	ExecStmt(context.Context, tree.Statement) error
	GetExecResultSet() []interface{}
	ClearExecResultSet()

	GetExecResultBatches() []*batch.Batch
	ClearExecResultBatches()
	Clear()
}

var _ BackgroundExec = &backExec{}

type unknownStatementType struct {
	tree.StatementType
}

func (unknownStatementType) GetStatementType() string { return "Unknown" }
func (unknownStatementType) GetQueryType() string     { return tree.QueryTypeOth }

func getStatementType(stmt tree.Statement) tree.StatementType {
	switch stmt.(type) {
	case tree.StatementType:
		return stmt
	default:
		return unknownStatementType{}
	}
}

// TableInfoCache tableInfos of a database
//type TableInfoCache struct {
//	db         string
//	tableInfos map[string][]ColumnInfo
//}

// outputPool outputs the data
type outputPool interface {
	resetLineStr()

	reset()

	getEmptyRow() ([]interface{}, error)

	flush() error
}

func (prepareStmt *PrepareStmt) Close() {
	if prepareStmt.params != nil {
		prepareStmt.params.Free(prepareStmt.proc.Mp())
	}
	if prepareStmt.InsertBat != nil {
		prepareStmt.InsertBat.SetCnt(1)
		prepareStmt.InsertBat.Clean(prepareStmt.proc.Mp())
		prepareStmt.InsertBat = nil
	}
	if prepareStmt.exprList != nil {
		for _, exprs := range prepareStmt.exprList {
			for _, expr := range exprs {
				expr.Free()
			}
		}
	}
	if prepareStmt.PrepareStmt != nil {
		prepareStmt.PrepareStmt.Free()
	}
}

var _ buf.Allocator = &SessionAllocator{}

type SessionAllocator struct {
	mp *mpool.MPool
}

func NewSessionAllocator(pu *config.ParameterUnit) *SessionAllocator {
	pool, err := mpool.NewMPool("frontend-goetty-pool-cn-level", pu.SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	ret := &SessionAllocator{mp: pool}
	return ret
}

func (s *SessionAllocator) Alloc(capacity int) []byte {
	alloc, err := s.mp.Alloc(capacity)
	if err != nil {
		panic(err)
	}
	return alloc
}

func (s SessionAllocator) Free(bs []byte) {
	s.mp.Free(bs)
}

var _ FeSession = &Session{}
var _ FeSession = &backSession{}

type FeSession interface {
	GetTimeZone() *time.Location
	GetStatsCache() *plan2.StatsCache
	GetUserName() string
	GetSql() string
	GetAccountId() uint32
	GetTenantInfo() *TenantInfo
	GetConfig(ctx context.Context, dbName, varName string) (any, error)
	GetBackgroundExec(ctx context.Context) BackgroundExec
	GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec
	GetGlobalSystemVariableValue(ctx context.Context, name string) (interface{}, error)
	GetSessionVar(ctx context.Context, name string) (interface{}, error)
	GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error)
	GetDebugString() string
	GetFromRealUser() bool
	getLastCommitTS() timestamp.Timestamp
	GetTenantName() string
	SetTxnId(i []byte)
	GetTxnId() uuid.UUID
	GetStmtId() uuid.UUID
	GetSqlOfStmt() string
	updateLastCommitTS(ts timestamp.Timestamp)
	GetMysqlProtocol() MysqlProtocol
	GetTxnHandler() *TxnHandler
	GetDatabaseName() string
	SetDatabaseName(db string)
	GetMysqlResultSet() *MysqlResultSet
	GetGlobalVar(ctx context.Context, name string) (interface{}, error)
	SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response
	GetTxnCompileCtx() *TxnCompilerContext
	GetCmd() CommandType
	IsBackgroundSession() bool
	GetPrepareStmt(ctx context.Context, name string) (*PrepareStmt, error)
	CountPayload(i int)
	RemovePrepareStmt(name string)
	SetShowStmtType(statement ShowStatementType)
	SetSql(sql string)
	GetMemPool() *mpool.MPool
	GetProc() *process.Process
	GetLastInsertID() uint64
	GetSqlHelper() *SqlHelper
	GetBuffer() *buffer.Buffer
	GetStmtProfile() *process.StmtProfile
	CopySeqToProc(proc *process.Process)
	getQueryId(internal bool) []string
	SetMysqlResultSet(mrs *MysqlResultSet)
	GetConnectionID() uint32
	IsDerivedStmt() bool
	SetAccountId(uint32)
	SetPlan(plan *plan.Plan)
	SetData([][]interface{})
	GetIsInternal() bool
	getCNLabels() map[string]string
	GetUpstream() FeSession
	cleanCache()
	getNextProcessId() string
	GetSqlCount() uint64
	addSqlCount(a uint64)
	GetStmtInfo() *motrace.StatementInfo
	GetTxnInfo() string
	GetUUID() []byte
	SendRows() int64
	SetTStmt(stmt *motrace.StatementInfo)
	GetUUIDString() string
	DisableTrace() bool
	Close()
	Clear()
	getCachedPlan(sql string) *cachedPlan
	GetFPrints() footPrints
	ResetFPrints()
	EnterFPrint(idx int)
	ExitFPrint(idx int)
	SetStaticTxnId(id []byte)
	GetStaticTxnId() uuid.UUID
	GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec
	SessionLogger
}

type SessionLogger interface {
	SessionLoggerGetter
	Info(ctx context.Context, msg string, fields ...zap.Field)
	Error(ctx context.Context, msg string, fields ...zap.Field)
	Warn(ctx context.Context, msg string, fields ...zap.Field)
	Fatal(ctx context.Context, msg string, fields ...zap.Field)
	Debug(ctx context.Context, msg string, fields ...zap.Field)
	Infof(ctx context.Context, msg string, args ...any)
	Errorf(ctx context.Context, msg string, args ...any)
	Warnf(ctx context.Context, msg string, args ...any)
	Fatalf(ctx context.Context, msg string, args ...any)
	Debugf(ctx context.Context, msg string, args ...any)
	GetLogger() SessionLogger
}

type SessionLoggerGetter interface {
	GetSessId() uuid.UUID
	GetStmtId() uuid.UUID
	GetTxnId() uuid.UUID
	GetLogLevel() zapcore.Level
}

type ExecCtx struct {
	reqCtx      context.Context
	prepareStmt *PrepareStmt
	runResult   *util.RunResult
	//stmt will be replaced by the Execute
	stmt tree.Statement
	//isLastStmt : true denotes the last statement in the query
	isLastStmt bool
	// tenant name
	tenant          string
	userName        string
	sqlOfStmt       string
	cw              ComputationWrapper
	runner          ComputationRunner
	loadLocalWriter *io.PipeWriter
	proc            *process.Process
	proto           MysqlProtocol
	ses             FeSession
	txnOpt          FeTxnOption
	cws             []ComputationWrapper
	input           *UserInput
	//In the session migration, skip the response to the client
	skipRespClient bool
	//In the session migration, executeParamTypes for the EXECUTE stmt should be migrated
	//from the old session to the new session.
	executeParamTypes []byte
}

// outputCallBackFunc is the callback function to send the result to the client.
// parameters:
//
//	FeSession
//	ExecCtx
//	batch.Batch
type outputCallBackFunc func(FeSession, *ExecCtx, *batch.Batch) error

// TODO: shared component among the session implmentation
type feSessionImpl struct {
	pool          *mpool.MPool
	proto         MysqlProtocol
	buf           *buffer.Buffer
	stmtProfile   process.StmtProfile
	tenant        *TenantInfo
	txnHandler    *TxnHandler
	txnCompileCtx *TxnCompilerContext
	mrs           *MysqlResultSet
	//it gets the result set from the pipeline and send it to the client
	outputCallback outputCallBackFunc

	//all the result set of executing the sql in background task
	allResultSet []*MysqlResultSet
	rs           *plan.ResultColDef

	// result batches of executing the sql in background task
	// set by func batchFetcher
	resultBatches []*batch.Batch

	//derivedStmt denotes the sql or statement that derived from the user input statement.
	//a new internal statement derived from the statement the user input and executed during
	// the execution of it in the same transaction.
	//
	//For instance
	//	select nextval('seq_15')
	//  nextval internally will derive two sql (a select and an update). the two sql are executed
	//	in the same transaction.
	derivedStmt bool

	gSysVars *GlobalSystemVariables
	// when starting a transaction in session, the snapshot ts of the transaction
	// is to get a TN push to CN to get the maximum commitTS. but there is a problem,
	// when the last transaction ends and the next one starts, it is possible that the
	// log of the last transaction has not been pushed to CN, we need to wait until at
	// least the commit of the last transaction log of the previous transaction arrives.
	lastCommitTS timestamp.Timestamp
	upstream     *Session
	sql          string
	accountId    uint32
	label        map[string]string
	timeZone     *time.Location

	sqlCount     uint64
	uuid         uuid.UUID
	debugStr     string
	disableTrace bool
	fprints      footPrints
	//refreshed onc
	staticTxnId uuid.UUID
}

func (ses *feSessionImpl) EnterFPrint(idx int) {
	if ses != nil {
		ses.fprints.addEnter(idx)
	}
}

func (ses *feSessionImpl) ExitFPrint(idx int) {
	if ses != nil {
		ses.fprints.addExit(idx)
	}
}

func (ses *feSessionImpl) Close() {
	ses.proto = nil
	ses.mrs = nil
	if ses.txnHandler != nil {
		ses.txnHandler = nil
	}
	if ses.txnCompileCtx != nil {
		ses.txnCompileCtx.execCtx = nil
		ses.txnCompileCtx.snapshot = nil
		ses.txnCompileCtx.views = nil
		ses.txnCompileCtx = nil
	}
	ses.sql = ""
	ses.gSysVars = nil
	ses.allResultSet = nil
	ses.tenant = nil
	ses.debugStr = ""
	ses.rs = nil
	ses.ClearStmtProfile()
	for _, bat := range ses.resultBatches {
		bat.Clean(ses.pool)
	}
	if ses.buf != nil {
		ses.buf.Free()
		ses.buf = nil
	}
	ses.upstream = nil
}

func (ses *feSessionImpl) Clear() {
	if ses == nil {
		return
	}
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()
}

func (ses *feSessionImpl) ResetFPrints() {
	ses.fprints.reset()
}

func (ses *feSessionImpl) GetFPrints() footPrints {
	return ses.fprints
}

func (ses *feSessionImpl) SetDatabaseName(db string) {
	ses.proto.SetDatabaseName(db)
	ses.txnCompileCtx.SetDatabase(db)
}

func (ses *feSessionImpl) GetDatabaseName() string {
	return ses.proto.GetDatabaseName()
}

func (ses *feSessionImpl) GetUserName() string {
	return ses.proto.GetUserName()
}

func (ses *feSessionImpl) DisableTrace() bool {
	return ses.disableTrace
}

func (ses *feSessionImpl) SetMemPool(mp *mpool.MPool) {
	ses.pool = mp
}

func (ses *feSessionImpl) GetMemPool() *mpool.MPool {
	return ses.pool
}

func (ses *feSessionImpl) GetMysqlProtocol() MysqlProtocol {
	return ses.proto
}

func (ses *feSessionImpl) ReplaceProtocol(proto MysqlProtocol) MysqlProtocol {
	old := ses.proto
	ses.proto = proto
	return old
}

func (ses *feSessionImpl) GetBuffer() *buffer.Buffer {
	return ses.buf
}

func (ses *feSessionImpl) GetStmtProfile() *process.StmtProfile {
	return &ses.stmtProfile
}

func (ses *feSessionImpl) ClearStmtProfile() {
	ses.stmtProfile.Clear()
}

func (ses *feSessionImpl) SetTxnId(id []byte) {
	ses.stmtProfile.SetTxnId(id)
}

func (ses *feSessionImpl) GetTxnId() uuid.UUID {
	return ses.stmtProfile.GetTxnId()
}

func (ses *feSessionImpl) SetStmtId(id uuid.UUID) {
	ses.stmtProfile.SetStmtId(id)
}

func (ses *feSessionImpl) GetStmtId() uuid.UUID {
	return ses.stmtProfile.GetStmtId()
}

func (ses *feSessionImpl) SetStmtType(st string) {
	ses.stmtProfile.SetStmtType(st)
}

func (ses *feSessionImpl) GetStmtType() string {
	return ses.stmtProfile.GetStmtType()
}

func (ses *feSessionImpl) SetQueryType(qt string) {
	ses.stmtProfile.SetQueryType(qt)
}

func (ses *feSessionImpl) GetQueryType() string {
	return ses.stmtProfile.GetQueryType()
}

func (ses *feSessionImpl) SetSqlSourceType(st string) {
	ses.stmtProfile.SetSqlSourceType(st)
}

func (ses *feSessionImpl) GetSqlSourceType() string {
	return ses.stmtProfile.GetSqlSourceType()
}

func (ses *feSessionImpl) SetQueryStart(t time.Time) {
	ses.stmtProfile.SetQueryStart(t)
}

func (ses *feSessionImpl) GetQueryStart() time.Time {
	return ses.stmtProfile.GetQueryStart()
}

func (ses *feSessionImpl) SetSqlOfStmt(sot string) {
	ses.stmtProfile.SetSqlOfStmt(sot)
}

func (ses *feSessionImpl) GetSqlOfStmt() string {
	return ses.stmtProfile.GetSqlOfStmt()
}

func (ses *feSessionImpl) GetTenantInfo() *TenantInfo {
	return ses.tenant
}

func (ses *feSessionImpl) SetTenantInfo(ti *TenantInfo) {
	ses.tenant = ti
}

func (ses *feSessionImpl) GetTxnHandler() *TxnHandler {
	return ses.txnHandler
}

func (ses *feSessionImpl) GetTxnCompileCtx() *TxnCompilerContext {
	return ses.txnCompileCtx
}

func (ses *feSessionImpl) SetMysqlResultSet(mrs *MysqlResultSet) {
	ses.mrs = mrs
}

func (ses *feSessionImpl) GetMysqlResultSet() *MysqlResultSet {
	return ses.mrs
}

func (ses *feSessionImpl) SetOutputCallback(callback outputCallBackFunc) {
	ses.outputCallback = callback
}

func (ses *feSessionImpl) SetMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	if len(ses.allResultSet) == 0 {
		ses.allResultSet = append(ses.allResultSet, mrs)
	}
}

func (ses *feSessionImpl) GetAllMysqlResultSet() []*MysqlResultSet {
	return ses.allResultSet
}

func (ses *feSessionImpl) ClearAllMysqlResultSet() {
	if ses.allResultSet != nil {
		ses.allResultSet = ses.allResultSet[:0]
	}
}

func (ses *feSessionImpl) SaveResultSet() {
	if len(ses.allResultSet) == 0 && ses.mrs != nil {
		ses.allResultSet = []*MysqlResultSet{ses.mrs}
	}
}

func (ses *feSessionImpl) IsDerivedStmt() bool {
	return ses.derivedStmt
}

// ReplaceDerivedStmt sets the derivedStmt and returns the previous value.
// if b is true, executing a derived statement.
func (ses *feSessionImpl) ReplaceDerivedStmt(b bool) bool {
	prev := ses.derivedStmt
	ses.derivedStmt = b
	return prev
}

func (ses *feSessionImpl) updateLastCommitTS(lastCommitTS timestamp.Timestamp) {
	if lastCommitTS.Greater(ses.lastCommitTS) {
		ses.lastCommitTS = lastCommitTS
	}
	if ses.upstream != nil {
		ses.upstream.updateLastCommitTS(lastCommitTS)
	}
}

func (ses *feSessionImpl) getLastCommitTS() timestamp.Timestamp {
	minTS := ses.lastCommitTS
	if ses.upstream != nil {
		v := ses.upstream.getLastCommitTS()
		if v.Greater(minTS) {
			minTS = v
		}
	}
	return minTS
}

func (ses *feSessionImpl) GetUpstream() FeSession {
	return ses.upstream
}

// ClearResultBatches does not call Batch.Clear().
func (ses *feSessionImpl) ClearResultBatches() {
	ses.resultBatches = nil
}

func (ses *feSessionImpl) GetResultBatches() []*batch.Batch {
	return ses.resultBatches
}

func (ses *feSessionImpl) AppendResultBatch(bat *batch.Batch) error {
	copied, err := bat.Dup(ses.pool)
	if err != nil {
		return err
	}
	ses.resultBatches = append(ses.resultBatches, copied)
	return nil
}

func (ses *feSessionImpl) GetGlobalSysVars() *GlobalSystemVariables {
	return ses.gSysVars
}

func (ses *feSessionImpl) SetSql(sql string) {
	ses.sql = sql
}

func (ses *feSessionImpl) GetSql() string {
	return ses.sql
}

func (ses *feSessionImpl) GetAccountId() uint32 {
	return ses.accountId
}

func (ses *feSessionImpl) SetAccountId(u uint32) {
	ses.accountId = u
}

func (ses *feSessionImpl) SetTimeZone(loc *time.Location) {
	ses.timeZone = loc
}

func (ses *feSessionImpl) GetTimeZone() *time.Location {
	return ses.timeZone
}

func (ses *feSessionImpl) GetSqlCount() uint64 {
	return ses.sqlCount
}

func (ses *feSessionImpl) addSqlCount(a uint64) {
	ses.sqlCount += a
}

func (ses *feSessionImpl) GetUUID() []byte {
	return ses.uuid[:]
}

func (ses *feSessionImpl) GetUUIDString() string {
	return ses.uuid.String()
}

func (ses *feSessionImpl) SetStaticTxnId(id []byte) {
	copy(ses.staticTxnId[:], id)
}
func (ses *feSessionImpl) GetStaticTxnId() uuid.UUID {
	return ses.staticTxnId
}

func (ses *Session) GetDebugString() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.debugStr
}
