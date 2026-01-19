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
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	DefaultRpcBufferSize = 1 << 10
)

const (
	FPHandleRequest = iota
	FPExecRequest
	FPDoComQuery
	FPDoComQueryInBack
	FPStmtWithResponse
	FPStmtWithResponseCreateAsSelect
	FPDispatchStmt
	FPExecStmt
	FPExecStmtBeforeCompile
	FPExecStmtInBack
	FPExecStmtInBackBeforeCompile
	FPExecStmtWithTxn
	FPExecStmtWithWorkspace
	FPExecStmtWithWorkspaceBeforeStart
	FPExecStmtWithWorkspaceBeforeEnd
	FPExecStmtWithIncrStmt
	FPExecStmtWithIncrStmtBeforeIncr
	FPExecStmtInSameSession
	FPExecInFrontEnd
	FPExecInFrontEndInBack
	FPInBackUse
	FPInBackCreateDatabase
	FPInBackDropDatabase
	FPInBackGrant
	FPInBackRevoke
	FPStatusStmtInBack
	FPCleanup
	FPBeginTxn
	FPSetRole
	FPUse
	FPPrepareStmt
	FPPrepareString
	FPCreateConnector
	FPPauseDaemonTask
	FPCancelDaemonTask
	FPResumeDaemonTask
	FPDropConnector
	FPShowConnectors
	FPDeallocate
	FPReset
	FPSetVar
	FPShowVariables
	FPShowErrors
	FPAnalyzeStmt
	FPExplainStmt
	FPInternalCmdFieldList
	FPCreatePublication
	FPAlterPublication
	FPDropPublication
	FPShowSubscriptions
	FPCreateStage
	FPDropStage
	FPAlterStage
	FPRemoveStageFiles
	FPCreateAccount
	FPDropAccount
	FPAlterAccount
	FPAlterDataBaseConfig
	FPCreateUser
	FPDropUser
	FPAlterUser
	FPAlterRole
	FPCreateRole
	FPDropRole
	FPCreateFunction
	FPDropFunction
	FPCreateProcedure
	FPDropProcedure
	FPCallStmt
	FPGrant
	FPRevoke
	FPKill
	FPShowAccounts
	FPShowCollation
	FPShowBackendServers
	FPSetTransaction
	FPBackupStart
	FPCreateSnapShot
	FPDropSnapShot
	FPRestoreSnapShot
	FPUpgradeStatement
	FPCreatePitr
	FPDropPitr
	FPAlterPitr
	FPRestorePitr
	FPSetConnectionID
	FPRollbackTxn
	FPCommitTxn
	FPFinishTxn
	FPCommit
	FPCommitBeforeCommitUnsafe
	FPCommitUnsafe
	FPCommitUnsafeBeforeCommit
	FPCommitUnsafeBeforeCommitWithTxn
	FPRollback
	FPRollbackUnsafe1
	FPRollbackUnsafe2
	FPRollbackUnsafe
	FPRollbackUnsafeBeforeRollback
	FPRollbackUnsafeBeforeRollbackWithTxn
	FPSetAutoCommit
	FPResultRowStmt
	FPResultRowStmtInBack
	FPResultRowStmtSelect1
	FPResultRowStmtSelect2
	FPResultRowStmtExplainAnalyze1
	FPResultRowStmtExplainAnalyze2
	FPResultRowStmtDefault1
	FPResultRowStmtDefault2
	FPRespStreamResultRow
	FPrespPrebuildResultRow
	FPrespMixedResultRow
	FPRespStatus
	FPMigrate
	FPMigrateDB
	FPMigratePrepareStmt
	FPGetBackgroundExec
	FPGetShareTxnBackgroundExec
	FPGetRawBatchBackgroundExec
	FPBackExecExec
	FPBackExecRestore
	FPGetShareTxnBackgroundExecInBackSession
	FPGetBackgroundExecInBackSession
	FPInternalExecutorExec
	FPInternalExecutorQuery
	FPHandleAnalyzeStmt
	FPShowPublications
	FPCreateCDC
	FPPauseCDC
	FPDropCDC
	FPRestartCDC
	FPResumeCDC
	FPShowCDC
	FPCommitUnsafeBeforeRollbackWhenCommitPanic
	FPShowRecoveryWindow
	FPCloneDatabase
	FPCloneTable
	FPDataBranch
)

type (
	TxnOperator = client.TxnOperator
	TxnClient   = client.TxnClient
	TxnOption   = client.TxnOption
)

type ComputationRunner interface {
	// todo: remove the ts next day, that's useless.
	Run(ts uint64) (*util.RunResult, error)
}

// compile.Compile should implement ComputationRunner to support Run method.
var _ ComputationRunner = &compile.Compile{}

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	ComputationRunner
	GetAst() tree.Statement

	GetProcess() *process.Process

	GetColumns(ctx context.Context) ([]interface{}, error)

	Compile(any any, fill func(*batch.Batch, *perfcounter.CounterSet) error) (interface{}, error)

	GetUUID() []byte

	// RecordExecPlan records the execution plan and calculates CU resources, and stores them into statementinfo
	RecordExecPlan(ctx context.Context, phyPlan *models.PhyPlan) error

	// RecordCompoundStmt calculates the CU resources of composite statements, and stores them into statementinfo
	RecordCompoundStmt(ctx context.Context, statsBytes statistic.StatsArray) error

	// StatsCompositeSubStmtResource Statistics on CU resources of sub statements in composite statements
	StatsCompositeSubStmtResource(ctx context.Context) (statsByte statistic.StatsArray)

	SetExplainBuffer(buf *bytes.Buffer)

	GetLoadTag() bool

	GetServerStatus() uint16
	Clear()
	Plan() *plan.Plan
	ResetPlanAndStmt(stmt tree.Statement)
	Free()
	ParamVals() []any
	BinaryExecute() (bool, string) //binary execute for COM_STMT_EXECUTE
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
	ColDefData     [][]byte
	IsCloudNonuser bool
	proc           *process.Process

	params              *vector.Vector
	getFromSendLongData map[int]struct{}

	compile *compile.Compile
	Ts      timestamp.Timestamp
}

/*
Disguise the COMMAND CMD_FIELD_LIST as sql query.
*/
const (
	cmdFieldListSql           = "__++__internal_cmd_field_list"
	cmdFieldListSqlLen        = len(cmdFieldListSql)
	cloudUserTag              = "cloud_user"
	cloudNoUserTag            = "cloud_nonuser"
	saveResultTag             = "save_result"
	validatePasswordPolicyTag = "validate_password.policy"
	validatePasswordPolicyLow = "low"
	validatePasswordPolicyMed = "medium"
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
	return tree.MakeStmtKind(tree.OUTPUT_STATUS, tree.RESP_STATUS, tree.EXEC_IN_FRONTEND)
}

func (icfl *InternalCmdFieldList) GetStatementType() string { return "InternalCmd" }
func (icfl *InternalCmdFieldList) GetQueryType() string     { return tree.QueryTypeDQL }

// ExecResult is the result interface of the execution
type ExecResult interface {
	GetRowCount() uint64

	GetColumnCount() uint64

	GetString(ctx context.Context, rindex, cindex uint64) (string, error)

	GetUint64(ctx context.Context, rindex, cindex uint64) (uint64, error)

	GetInt64(ctx context.Context, rindex, cindex uint64) (int64, error)

	ColumnIsNull(ctx context.Context, rindex, cindex uint64) (bool, error)
}

func execResultArrayHasData(arr []ExecResult) bool {
	return len(arr) != 0 && arr[0].GetRowCount() != 0
}

type BackgroundExecOption struct {
	fromRealUser bool
}

// BackgroundExec executes the sql in background session without network output.
type BackgroundExec interface {
	Close()
	Exec(context.Context, string) error
	ExecRestore(context.Context, string, uint32, uint32) error
	ExecStmt(context.Context, tree.Statement) error
	GetExecResultSet() []interface{}
	ClearExecResultSet()
	GetExecStatsArray() statistic.StatsArray
	SetRestore(b bool)

	GetExecResultBatches() []*batch.Batch
	ClearExecResultBatches()
	Clear()
	Service() string
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

func (prepareStmt *PrepareStmt) Close() {
	if prepareStmt.params != nil {
		prepareStmt.params.Free(prepareStmt.proc.Mp())
	}

	if prepareStmt.compile != nil {
		prepareStmt.compile.FreeOperator()
		prepareStmt.compile.SetIsPrepare(false)
		prepareStmt.compile.Release()
		prepareStmt.compile = nil
	}
	if prepareStmt.PrepareStmt != nil {
		prepareStmt.PrepareStmt.Free()
	}
	if prepareStmt.ParamTypes != nil {
		prepareStmt.ParamTypes = nil
	}
	if prepareStmt.ColDefData != nil {
		prepareStmt.ColDefData = nil
	}
}

type Allocator interface {
	// Alloc allocate a []byte with len(data) >= size, and the returned []byte cannot
	// be expanded in use.
	Alloc(capacity int) ([]byte, error)
	// Free the allocated memory
	Free([]byte)
}

var _ Allocator = &SessionAllocator{}

type SessionAllocator struct {
	allocator *malloc.ManagedAllocator[malloc.Allocator]
}

var baseSessionAllocator = sync.OnceValue(func() malloc.Allocator {
	// default
	allocator := malloc.GetDefault(nil)
	// with metrics
	allocator = malloc.NewMetricsAllocator(
		allocator,
		metric.MallocCounter.WithLabelValues("session-allocate"),
		metric.MallocGauge.WithLabelValues("session-inuse"),
		metric.MallocCounter.WithLabelValues("session-allocate-objects"),
		metric.MallocGauge.WithLabelValues("session-inuse-objects"),
		metric.OffHeapInuseGauge.WithLabelValues("session"),
	)
	return allocator
})

func NewSessionAllocator(pu *config.ParameterUnit) *SessionAllocator {
	// base
	allocator := baseSessionAllocator()
	// size bounded
	allocator = malloc.NewSizeBoundedAllocator(
		allocator,
		uint64(pu.SV.GuestMmuLimitation),
		nil,
	)
	ret := &SessionAllocator{
		// managed
		allocator: malloc.NewManagedAllocator(allocator),
	}
	return ret
}

func (s *SessionAllocator) Alloc(capacity int) ([]byte, error) {
	return s.allocator.Allocate(uint64(capacity), malloc.NoClear)
}

func (s SessionAllocator) Free(bs []byte) {
	s.allocator.Deallocate(bs)
}

var _ FeSession = &Session{}
var _ FeSession = &backSession{}

type FeSession interface {
	GetService() string
	GetTimeZone() *time.Location
	GetStatsCache() *plan2.StatsCache
	GetUserName() string
	GetSql() string
	GetAccountId() uint32
	GetTenantInfo() *TenantInfo
	GetConfig(ctx context.Context, varName, dbName, tblName string) (any, error)
	GetBackgroundExec(ctx context.Context, opts ...*BackgroundExecOption) BackgroundExec
	GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec
	GetGlobalSysVars() *SystemVariables
	GetGlobalSysVar(name string) (interface{}, error)
	GetSessionSysVars() *SystemVariables
	GetSessionSysVar(name string) (interface{}, error)
	GetUserDefinedVar(name string) (*UserDefinedVar, error)
	SetUserDefinedVar(name string, value interface{}, sql string) error
	GetDebugString() string
	GetFromRealUser() bool
	getLastCommitTS() timestamp.Timestamp
	GetTenantName() string
	SetTxnId(i []byte)
	GetTxnId() uuid.UUID
	GetStmtId() uuid.UUID
	GetSqlOfStmt() string
	updateLastCommitTS(ts timestamp.Timestamp)
	GetResponser() Responser
	GetTxnHandler() *TxnHandler
	GetDatabaseName() string
	SetDatabaseName(db string)
	GetMysqlResultSet() *MysqlResultSet
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
	EnterFPrint(idx int)
	ExitFPrint(idx int)
	SetStaticTxnInfo(string)
	GetStaticTxnInfo() string
	GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec
	GetMySQLParser() *mysql.MySQLParser
	InitBackExec(txnOp TxnOperator, db string, callBack outputCallBackFunc, opts ...*BackgroundExecOption) BackgroundExec
	GetTempTable(dbName, alias string) (string, bool)
	AddTempTable(dbName, alias, realName string)
	RemoveTempTable(dbName, alias string)
	RemoveTempTableByRealName(realName string)
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
	LogDebug() bool
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
	ses             FeSession
	txnOpt          FeTxnOption
	cws             []ComputationWrapper
	input           *UserInput
	//In the session migration, skip the response to the client
	inMigration bool
	//In the session migration, executeParamTypes for the EXECUTE stmt should be migrated
	//from the old session to the new session.
	executeParamTypes []byte
	resper            Responser
	results           []ExecResult
	prepareColDef     [][]byte
	isIssue3482       bool
}

func (execCtx *ExecCtx) Close() {
	execCtx.reqCtx = nil
	execCtx.prepareStmt = nil
	execCtx.runResult = nil
	execCtx.stmt = nil
	execCtx.tenant = ""
	execCtx.userName = ""
	execCtx.sqlOfStmt = ""
	execCtx.cw = nil
	execCtx.runner = nil
	execCtx.loadLocalWriter = nil
	execCtx.proc = nil
	execCtx.ses = nil
	execCtx.cws = nil
	execCtx.input = nil
	execCtx.executeParamTypes = nil
	execCtx.resper = nil
	execCtx.results = nil
	execCtx.prepareColDef = nil
}

// outputCallBackFunc is the callback function to send the result to the client.
// parameters:
//
//	FeSession
//	ExecCtx
//	batch.Batch
type outputCallBackFunc func(FeSession, *ExecCtx, *batch.Batch, *perfcounter.CounterSet) error

type feSessionImpl struct {
	pool          *mpool.MPool
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

	// gSysVars is a pointer to account's sys vars (saved in GSysVarsMgr)
	gSysVars *SystemVariables
	// sesSysVars is session level sys vars; init as a copy of account's sys vars
	sesSysVars *SystemVariables

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
	respr        Responser
	runSQLTokens []uint64
	//refreshed once
	staticTxnInfo string
	// mysql parser
	mysqlParser mysql.MySQLParser
	// reserveCOnn is set true when TCP network on the session/routine should be
	// reserved because the connection is still in use in proxy's connection cache.
	// Default is false, means that the network connection should be closed.
	reserveConn bool
	service     string

	//fromRealUser distinguish the sql that the user inputs from the one
	//that the internal or background program executes
	fromRealUser bool

	//isRestore denotes the session is used to restore the snapshot
	isRestore bool

	//isRestoreFail
	isRestoreFail bool
}

func (ses *feSessionImpl) GetService() string {
	return ses.service
}

func (ses *feSessionImpl) GetMySQLParser() *mysql.MySQLParser {
	return &ses.mysqlParser
}

func (ses *feSessionImpl) EnterFPrint(idx int) {
	if ses != nil {
		if ses.txnHandler != nil && ses.txnHandler.txnOp != nil {
			ses.txnHandler.txnOp.SetFootPrints(idx, true)
		}
	}
}

func (ses *feSessionImpl) ExitFPrint(idx int) {
	if ses != nil {
		if ses.txnHandler != nil && ses.txnHandler.txnOp != nil {
			ses.txnHandler.txnOp.SetFootPrints(idx, false)
		}
	}
}

func (ses *feSessionImpl) pushRunSQLToken(token uint64) {
	if token == 0 {
		return
	}
	ses.runSQLTokens = append(ses.runSQLTokens, token)
}

func (ses *feSessionImpl) popRunSQLToken() uint64 {
	n := len(ses.runSQLTokens)
	if n == 0 {
		return 0
	}
	token := ses.runSQLTokens[n-1]
	ses.runSQLTokens = ses.runSQLTokens[:n-1]
	return token
}

func (ses *feSessionImpl) currentRunSQLToken() uint64 {
	n := len(ses.runSQLTokens)
	if n == 0 {
		return 0
	}
	return ses.runSQLTokens[n-1]
}

// Close releases all reference.
// close txn handler also
func (ses *feSessionImpl) Close() {
	if ses.respr != nil && !ses.reserveConn {
		ses.respr.Close()
	}
	ses.Reset()
}

// Reset release resources like buffer,memory,handles,etc.
//
//		It also reserves some necessary resources that are carefully designed.
//	 	does not close txn handler here.
func (ses *feSessionImpl) Reset() {
	if ses == nil {
		return
	}
	ses.Clear()

	ses.mrs = nil
	if ses.txnHandler != nil {
		ses.txnHandler.Close()
		ses.txnHandler = nil
	}
	if ses.txnCompileCtx != nil {
		ses.txnCompileCtx.Close()
		ses.txnCompileCtx = nil
	}
	ses.sql = ""
	ses.runSQLTokens = nil
	ses.gSysVars = nil
	ses.sesSysVars = nil
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
	ses.fromRealUser = false
}

// Clear clean result only
func (ses *feSessionImpl) Clear() {
	if ses == nil {
		return
	}
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()
}

func (ses *feSessionImpl) SetDatabaseName(db string) {
	ses.respr.SetStr(DBNAME, db)
	ses.txnCompileCtx.SetDatabase(db)
}

func (ses *feSessionImpl) GetDatabaseName() string {
	return ses.respr.GetStr(DBNAME)
}

func (ses *feSessionImpl) GetUserName() string {
	return ses.respr.GetStr(USERNAME)
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
	for _, bat := range ses.resultBatches {
		if bat != nil {
			bat.Clean(ses.pool)
		}
	}
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

func (ses *feSessionImpl) GetGlobalSysVars() *SystemVariables {
	return ses.gSysVars
}

func (ses *feSessionImpl) GetGlobalSysVar(name string) (interface{}, error) {
	name = strings.ToLower(name)
	if sv, ok := gSysVarsDefs[name]; !ok {
		return nil, moerr.NewInternalErrorNoCtx(errorSystemVariableDoesNotExist())
	} else if sv.Scope == ScopeSession {
		return nil, moerr.NewInternalErrorNoCtx(errorSystemVariableIsSession())
	}

	return ses.gSysVars.Get(name), nil
}

func (ses *Session) SetGlobalSysVar(ctx context.Context, name string, val interface{}) (err error) {
	name = strings.ToLower(name)

	def, ok := gSysVarsDefs[name]
	if !ok {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableDoesNotExist())
	}

	if def.Scope == ScopeSession {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableIsSession())
	}

	if !def.GetDynamic() {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableIsReadOnly())
	}

	// special handle for validate_password.policy
	if policy, ok := val.(string); ok && name == validatePasswordPolicyTag {
		if strings.ToLower(policy) == validatePasswordPolicyLow {
			// convert to 0
			val = int64(0)
		} else if strings.ToLower(policy) == validatePasswordPolicyMed {
			// convert to 1
			val = int64(1)
		}
	}

	// special check for invited_nodes
	if invitedlist, ok := val.(string); ok && name == InvitedNodes {
		err = checkInvitedNodes(ctx, invitedlist)
		if err != nil {
			return err
		}
	}

	if val, err = def.GetType().Convert(val); err != nil {
		return err
	}

	// save to table first
	if err = doSetGlobalSystemVariable(ctx, ses, name, val); err != nil {
		return
	}
	ses.gSysVars.Set(name, val)
	return
}

func (ses *feSessionImpl) GetSessionSysVars() *SystemVariables {
	return ses.sesSysVars
}

func (ses *Session) GetSessionSysVar(name string) (interface{}, error) {
	name = strings.ToLower(name)
	if _, ok := gSysVarsDefs[name]; !ok {
		return nil, moerr.NewInternalErrorNoCtx(errorSystemVariableDoesNotExist())
	}

	// init SystemVariables GlobalSysVarsMgr need to read table, read table need to use SessionSysVar
	// when ses.sesSysVars is nil
	// in this scenario, use Default value in gSysVarsDefs
	if ses.sesSysVars == nil {
		return gSysVarsDefs[name].Default, nil
	}
	return ses.sesSysVars.Get(name), nil
}

func (ses *Session) SetSessionSysVar(ctx context.Context, name string, val interface{}) (err error) {
	name = strings.ToLower(name)

	def, ok := gSysVarsDefs[name]
	if !ok {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableDoesNotExist())
	}

	if def.Scope == ScopeGlobal {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableIsGlobal())
	}

	if !def.GetDynamic() {
		return moerr.NewInternalErrorNoCtx(errorSystemVariableIsReadOnly())
	}

	if val, err = def.GetType().Convert(val); err != nil {
		return
	}

	// ensure session system variables container exists in embed/basic cluster
	if ses.sesSysVars == nil {
		ses.sesSysVars = &SystemVariables{mp: make(map[string]interface{})}
	}

	if def.UpdateSessVar != nil {
		err = def.UpdateSessVar(ctx, ses, ses.sesSysVars, name, val)
	} else {
		ses.sesSysVars.Set(name, val)
	}
	return
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

func (ses *feSessionImpl) ReplaceResponser(resper Responser) Responser {
	old := ses.respr
	ses.respr = resper
	return old
}

func (ses *feSessionImpl) GetResponser() Responser {
	return ses.respr
}

func (ses *feSessionImpl) SetStaticTxnInfo(info string) {
	ses.staticTxnInfo = info
}
func (ses *feSessionImpl) GetStaticTxnInfo() string {
	return ses.staticTxnInfo
}

func (ses *feSessionImpl) ReserveConn() {
	ses.reserveConn = true
}

func (ses *Session) GetDebugString() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.debugStr
}

func (ses *Session) SetRestore(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.isRestore = b
}

func (ses *Session) IsRestore() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isRestore
}

func (ses *Session) SetRestoreFail(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.isRestoreFail = b
}

func (ses *Session) IsRestoreFail() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isRestoreFail
}

type PropertyID int

const (
	USERNAME PropertyID = iota + 1
	DBNAME
	//Connection id
	CONNID
	//Peer address
	PEER
	//Seqeunce id
	SEQUENCEID
	//capability bits
	CAPABILITY
	ESTABLISHED
	TLS_ESTABLISHED

	// AuthString is the property authString in MysqlProtocolImpl.
	AuthString
)

type Property interface {
	GetStr(PropertyID) string
	SetStr(PropertyID, string)
	SetU32(PropertyID, uint32)
	GetU32(PropertyID) uint32
	SetU8(PropertyID, uint8)
	GetU8(PropertyID) uint8
	SetBool(PropertyID, bool)
	GetBool(PropertyID) bool
}

type Responser interface {
	Property
	RespPreMeta(*ExecCtx, any) error
	RespResult(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error
	RespPostMeta(*ExecCtx, any) error
	MysqlRrWr() MysqlRrWr
	Close()
	ResetStatistics()
}

type MediaReader interface {
}

type MediaWriter interface {
	Write(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error
	Close()
}

// MysqlReader read packet using mysql format
type MysqlReader interface {
	MediaReader
	Property
	Read() ([]byte, error)
	ReadLoadLocalPacket() ([]byte, error)
	FreeLoadLocal()
	Free(buf []byte)
	HandleHandshake(ctx context.Context, payload []byte) (bool, error)
	Authenticate(ctx context.Context) error
	ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error
	ParseExecuteData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error
	// Disconnect closes the underlying network connection to forcefully disconnect the client.
	Disconnect() error
}

// MysqlWriter write batch & control packets using mysql protocol format
type MysqlWriter interface {
	MediaWriter
	Property
	WriteHandshake() error
	WriteOK(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error
	WriteOKtWithEOF(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error
	WriteEOF(warnings, status uint16) error
	WriteEOFIF(warnings uint16, status uint16) error
	WriteEOFIFAndNoFlush(warnings uint16, status uint16) error
	WriteEOFOrOK(warnings uint16, status uint16) error
	WriteERR(errorCode uint16, sqlState, errorMessage string) error
	WriteLengthEncodedNumber(uint64) error
	WriteColumnDef(context.Context, Column, int) error
	WriteColumnDefBytes([]byte) error
	WriteRow() error
	WriteTextRow() error
	WriteBinaryRow() error
	WriteResultSetRow(mrs *MysqlResultSet, count uint64) error
	WriteResultSetRow2(mrs *MysqlResultSet, colSlices *ColumnSlices, count uint64) error
	WriteResponse(context.Context, *Response) error
	WritePrepareResponse(ctx context.Context, stmt *PrepareStmt) error
	WriteLocalInfileRequest(filepath string) error

	CalculateOutTrafficBytes(b bool) (int64, int64)
	ResetStatistics()
	UpdateCtx(ctx context.Context)
	// Reset sets the session and reset some fields and stats.
	Reset(ses *Session)
}

type MysqlHelper interface {
	MakeColumnDefData(context.Context, []*plan.ColDef) ([][]byte, error)
}

type MysqlRrWr interface {
	MysqlReader
	MysqlWriter
	MysqlHelper
}

// MysqlPayloadWriter make final payload for the packet
type MysqlPayloadWriter interface {
	OpenRow() error
	CloseRow() error
	OpenPayload() error
	FillPayload() error
	ClosePayload(bool) error
}

// BinaryWriter write batch into fileservice
type BinaryWriter interface {
	MediaWriter
}

// CsvWriter write batch into csv file
type CsvWriter interface {
	MediaWriter
}

// MemWriter write batch into memory pool
type MemWriter interface {
	MediaWriter
}

// ServerLevelVariables holds the variables are shared in single frontend mo server instance.
// these variables should be initialized at the server startup.
type ServerLevelVariables struct {
	RtMgr           atomic.Value
	Pu              atomic.Value
	Aicm            atomic.Value
	moServerStarted atomic.Bool
	sessionAlloc    atomic.Value
}
