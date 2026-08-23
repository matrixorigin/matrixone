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

package process

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hayageek/threadsafe"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"go.uber.org/zap/zapcore"
)

var (
	NormalEndRegisterMessage = NewRegMsg(nil)
)

// EmptySqlModeSentinel is used to distinguish an explicitly-empty (non-strict)
// sql_mode from an unset field during serialization. When resolveSqlMode
// successfully resolves sql_mode="" it stores this sentinel so the remote CN
// can tell "explicitly non-strict" apart from "never captured".
const EmptySqlModeSentinel = "\x00MO_EMPTY_SQL_MODE\x00"

// RegisterMessage channel data
// Err == nil means pipeline finish with error
// Batch == nil means pipeline finish without error
// Batch != nil means pipeline is running
type RegisterMessage struct {
	Batch *batch.Batch
	Err   error
}

func NewRegMsg(bat *batch.Batch) *RegisterMessage {
	return &RegisterMessage{
		Batch: bat,
	}
}

// WaitRegister is the historical name for a pipeline edge.
//
// Keep the old type name at API boundaries, but do not keep a second state
// object. Ch2, the nil-batch counter, and typed terminal state all live in
// PipelineEdge.
type WaitRegister = PipelineEdge

// Register used in execution pipeline and shared with all operators of the same pipeline.
type Register struct {
	// MergeReceivers, receives result of multi previous operators from other pipelines
	// e.g. merge operator.
	MergeReceivers []*WaitRegister
}

// Limitation specifies the maximum resources that can be used in one query.
type Limitation struct {
	// Size, memory threshold for operator.
	Size int64
	// BatchRows, max rows for batch.
	BatchRows int64
	// BatchSize, max size for batch.
	BatchSize int64
	// PartitionRows, max rows for partition.
	PartitionRows int64
	// ReaderSize, memory threshold for storage's reader
	ReaderSize int64
	// SpillSize, query spill-disk byte cap. Zero selects the bounded default.
	SpillSize int64
	// MaxMessageSize max size for read messages from dn
	MaxMsgSize uint64
}

// SessionInfo session information
type SessionInfo struct {
	Account             string
	User                string
	Host                string
	Role                string
	ConnectionID        uint64
	LastInsertID        uint64
	Database            string
	Version             string
	TimeZone            *time.Location
	LockWaitTimeout     int64
	LockWaitTimeoutSet  bool // distinguishes an explicit zero from an unset value
	MatrixOneNativeMode bool
	// IsRestore identifies catalog DDL executed by snapshot/PITR restore. Such
	// DDL rebuilds persisted View metadata through legacy discovery after the
	// restore transaction, rather than running dependency hooks while catalog
	// identities are being replaced.
	IsRestore bool
	// ExplicitZeroTemporalCastReturnsNull is resolved on the initiating CN and
	// carried in the remote process snapshot because remote CNs have no session
	// variable resolver.
	ExplicitZeroTemporalCastReturnsNull bool
	// SqlMode is captured on the initiating CN and used when a remote process has
	// no session variable resolver.
	SqlMode string
	// ApplySQLSelectLimit distinguishes client statements from frontend
	// background SQL, which may inherit a session-variable resolver but must not
	// be affected by a client's row cap.
	ApplySQLSelectLimit bool
	// CountUpdateChangedRows requests MySQL changed-row semantics for UPDATE.
	// Frontend sessions set it when CLIENT_FOUND_ROWS was not negotiated.
	CountUpdateChangedRows bool
	StorageEngine          engine.Engine
	QueryId                []string
	ResultColTypes         []types.Type
	SeqCurValues           map[uint64]string
	SeqDeleteKeys          []uint64
	SeqAddValues           map[uint64]string
	SeqLastValue           []string
	SqlHelper              sqlHelper
	Buf                    *buffer.Buffer
	LogLevel               zapcore.Level
	SessionId              uuid.UUID
}

type Session interface {
	GetTempTable(dbName, alias string) (string, bool)
	AddTempTable(dbName, alias, realName string)
	RemoveTempTable(dbName, alias string)
	RemoveTempTableByRealName(realName string)
	// GetSqlModeNoAutoValueOnZero reports whether sql_mode contains NO_AUTO_VALUE_ON_ZERO.
	// ok=false means the session doesn't support the cache.
	GetSqlModeNoAutoValueOnZero() (bool, bool)
}

// ForeignConn is a connection to a foreign data source (Elasticsearch, an
// external SQL database, ...) cached on an interactive session for esql_tvf /
// sql_tvf. The session owns its lifetime and closes it when the session ends.
// Close must be safe to call more than once.
type ForeignConn interface {
	Close() error
}

// ForeignConnCache is an OPTIONAL capability implemented only by the interactive
// frontend session. esql_tvf / sql_tvf and their connect/disconnect builtins
// reach it via proc.GetSession().(ForeignConnCache); a session that does not
// implement it (internal executor, background session) cannot use those TVFs.
// A handle is derived from the connection config, so reconnecting with the same
// config yields the same handle and reuses the cached connection.
type ForeignConnCache interface {
	// PutForeignConn stores conn under handle unless an entry already exists,
	// and returns the entry that is cached after the call (first-wins). Two
	// scans sharing one config can race to connect; the loser must close its
	// own conn and use the returned winner — the cache never closes a
	// connection another operator may already be using. Admission is bounded:
	// when the cache is full a non-nil error is returned and nothing is
	// stored; the caller owns (and must close) the rejected conn.
	PutForeignConn(handle string, conn ForeignConn) (ForeignConn, error)
	GetForeignConn(handle string) (ForeignConn, bool)
	// RemoveForeignConn detaches and returns the connection for handle so the
	// caller can close it; ok=false if no such handle.
	RemoveForeignConn(handle string) (ForeignConn, bool)
}

type ExecStatus int

const (
	ExecStop = iota
	ExecNext
	ExecHasMore
)

// StmtProfile will be clear for every statement
type StmtProfile struct {
	mu sync.Mutex
	// sqlSourceType denotes where the sql
	sqlSourceType string
	txnId         uuid.UUID
	stmtId        uuid.UUID
	// stmtType
	stmtType string
	// queryType
	queryType string
	// queryStart is the time when the query starts.
	queryStart time.Time
	//the sql from user may have multiple statements
	//sqlOfStmt is the text part of one statement in the sql
	sqlOfStmt string

	// statement runtime metadata avoids contaminating the session's main
	// statement profile when PREPARE / EXECUTE runs an inner INSERT / UPDATE.
	statementRuntimeStmtType  string
	statementRuntimeQueryType string
	statementRuntimeIgnore    bool
}

func NewStmtProfile(txnId, stmtId uuid.UUID) *StmtProfile {
	return &StmtProfile{
		txnId:  txnId,
		stmtId: stmtId,
	}
}

func (sp *StmtProfile) Clear() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.sqlSourceType = ""
	sp.txnId = uuid.UUID{}
	sp.stmtId = uuid.UUID{}
	sp.stmtType = ""
	sp.queryType = ""
	sp.sqlOfStmt = ""
	sp.clearStatementRuntimeProfileLocked()
}

func (sp *StmtProfile) SetSqlOfStmt(sot string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.sqlOfStmt = sot
}

func (sp *StmtProfile) GetSqlOfStmt() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.sqlOfStmt
}

func (sp *StmtProfile) SetQueryStart(t time.Time) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.queryStart = t
}

func (sp *StmtProfile) GetQueryStart() time.Time {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.queryStart
}

func (sp *StmtProfile) SetSqlSourceType(st string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.sqlSourceType = st
}

func (sp *StmtProfile) GetSqlSourceType() string {
	return sp.sqlSourceType
}

func (sp *StmtProfile) SetQueryType(qt string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.queryType = qt
}

func (sp *StmtProfile) GetQueryType() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.queryType
}

func (sp *StmtProfile) SetStmtType(st string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.stmtType = st
}

func (sp *StmtProfile) GetStmtType() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.stmtType
}

func (sp *StmtProfile) GetStatementIgnore() bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.statementRuntimeIgnore
}

func (sp *StmtProfile) SetStatementRuntimeProfile(stmtType, queryType string, ignore bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.statementRuntimeStmtType = stmtType
	sp.statementRuntimeQueryType = queryType
	sp.statementRuntimeIgnore = ignore
}

func (sp *StmtProfile) GetStatementRuntimeProfile() (stmtType, queryType string, ignore bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.statementRuntimeStmtType, sp.statementRuntimeQueryType, sp.statementRuntimeIgnore
}

func (sp *StmtProfile) clearStatementRuntimeProfileLocked() {
	sp.statementRuntimeStmtType = ""
	sp.statementRuntimeQueryType = ""
	sp.statementRuntimeIgnore = false
}

func (sp *StmtProfile) clearStatementRuntimeProfile() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.clearStatementRuntimeProfileLocked()
}

func (sp *StmtProfile) GetDivByZeroIgnore() bool {
	return sp.GetStatementIgnore()
}

func (sp *StmtProfile) SetDivByZeroRuntimeProfile(stmtType, queryType string, ignore bool) {
	sp.SetStatementRuntimeProfile(stmtType, queryType, ignore)
}

func (sp *StmtProfile) GetDivByZeroRuntimeProfile() (stmtType, queryType string, ignore bool) {
	return sp.GetStatementRuntimeProfile()
}

func (sp *StmtProfile) SetTxnId(id []byte) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	copy(sp.txnId[:], id)
}

func (sp *StmtProfile) GetTxnId() uuid.UUID {
	if sp == nil {
		return uuid.UUID{}
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.txnId
}

func (sp *StmtProfile) SetStmtId(id uuid.UUID) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	copy(sp.stmtId[:], id[:])
}

func (sp *StmtProfile) GetStmtId() uuid.UUID {
	if sp == nil {
		return uuid.UUID{}
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.stmtId
}

type BaseProcess struct {
	// sqlContext includes the client context and the query context.
	sqlContext QueryBaseContext
	// atRuntime indicates whether the process is running in runtime.
	atRuntime bool
	LoadTag   bool

	StmtProfile *StmtProfile
	// Id, query id.
	Id  string
	Lim Limitation
	mp  *mpool.MPool
	// unix timestamp
	UnixTime         int64
	TxnClient        client.TxnClient
	SessionInfo      SessionInfo
	FileService      fileservice.FileService
	LockService      lockservice.LockService
	TaskService      taskservice.TaskService
	PartitionService partitionservice.PartitionService
	IncrService      incrservice.AutoIncrementService

	LastInsertID *uint64
	// AffectedRows carries the number of rows affected by the previous
	// statement in the same session, used by the ROW_COUNT() builtin.
	// It follows MySQL semantics: -1 after a result-set statement (e.g. SELECT),
	// 0 after DDL, and the affected row count after DML.
	AffectedRows                        *int64
	LoadLocalReader                     *io.PipeReader
	Aicm                                *defines.AutoIncrCacheManager
	resolveVariableFunc                 func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	resolveVariableIsBinFunc            func(varName string, isSystemVar, isGlobalVar bool) (bool, error)
	resolveVariableBinaryStringFunc     func(varName string, isSystemVar, isGlobalVar bool) (bool, error)
	resolveVariablePrepareParamKindFunc func(varName string, isSystemVar, isGlobalVar bool) (vector.PrepareParamKind, error)
	prepareParams                       *vector.Vector
	prepareParamsIsBin                  []bool
	prepareParamsBinaryString           []bool
	prepareParamsOwned                  bool
	QueryClient                         qclient.QueryClient
	Hakeeper                            logservice.CNHAKeeperClient
	UdfService                          udf.Service
	WaitPolicy                          lock.WaitPolicy
	messageBoard                        *message.MessageBoard
	executionResourceBudgetMu           sync.Mutex
	executionResourceBudget             *ExecutionResourceGeneration
	cteMemoryBudgetMu                   sync.Mutex
	cteMemoryBudget                     *CTEMemoryBudget
	logger                              *log.MOLogger
	TxnOperator                         client.TxnOperator
	CloneTxnOperator                    client.TxnOperator
	// userLevelLockIdentity is session-scoped rather than statement-scoped.
	// SessionInfo is rebuilt before every statement, so keeping this identity
	// there would lose the synthetic transaction owner while locks are held.
	userLevelLockIdentityMu sync.Mutex
	userLevelLockOwner      string
	userLevelLockConnID     uint64
	userLevelLockGeneration string
	// incrStatementDisabled marks a process that executes internal SQL on a
	// caller-owned transaction without opening a statement of its own
	// (executor.Options.WithDisableIncrStatement). Compiles on such a process
	// must not advance the workspace snapshot write offset: that is a
	// statement-boundary action, and moving the boundary mid-statement breaks
	// the positional visibility of the caller's workspace entries.
	incrStatementDisabled bool

	// post dml sqls run right after all pipelines finished.
	PostDmlSqlList *threadsafe.Slice[string]

	// stage cache to avoid to run same stage SQL repeatedly
	StageCache *threadsafe.Map[string, stage.StageDef]

	// DivByZeroErrorMode caches whether division by zero should error (true) or return NULL (false)
	// -1: not initialized, 0: return NULL, 1: return error
	DivByZeroErrorMode int32

	// IsFrontend reports whether this proc is attached to a frontend
	// client session (mysql client query or the in-frontend backSession
	// that pkg/frontend/back_exec.go drives). Defaults false — every
	// other proc (internal SQL executor invocations from idxcron,
	// ProcessInitSQL, bootstrap, cron jobs, task service, …) is
	// background. pkg/sql/compile/sql_executor.go's NewTopProcess sets
	// this from opts.IsFrontend(); the two frontend proc-construction
	// sites in pkg/frontend (mysql_cmd_executor, back_exec) set it
	// directly. This is the canonical signal for code that needs to
	// distinguish "have a session" from "don't" — relying on
	// proc.resolveVariableFunc being nil is unreliable because
	// background paths also attach resolvers (idxcron via the task's
	// captured Metadata, ProcessInitSQL via executor.DefaultResolveVariable).
	IsFrontend bool
}

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	// BaseProcess is the common part of one process, and it's shared by all its children processes.
	Base *BaseProcess
	Reg  Register

	// planSnapshotTS is the snapshot against which this execution generation's
	// plan was bound. Unlike the transaction snapshot, it must not advance while
	// RC lock handling refreshes visibility. It belongs to Process rather than
	// shared BaseProcess so nested or overlapping pipeline generations cannot
	// overwrite each other's definition-fence reference point. Child processes
	// share this immutable object, keeping the per-pipeline footprint to one
	// pointer rather than one protobuf timestamp.
	planSnapshotTS *timestamp.Timestamp

	// Ctx and Cancel are pipeline's context and cancel function.
	// Every pipeline has its own context, and the lifecycle of the pipeline is controlled by the context.
	Ctx     context.Context
	Cancel  context.CancelCauseFunc
	Session Session
}

type sqlHelper interface {
	GetCompilerContext() any
	ExecSql(string) ([][]interface{}, error)
	ExecSqlWithCtx(context.Context, string) ([][]interface{}, error)
	GetSubscriptionMeta(string) (sub *plan.SubscriptionMeta, err error)
}

// WrapCs record information about pipeline's remote receiver.
type WrapCs struct {
	sync.RWMutex
	ReceiverDone  bool
	MsgId         uint64
	Uid           uuid.UUID
	Cs            morpc.ClientSession
	Err           chan error
	ReserveBatch  func(context.Context, uint64) (uint64, error)
	RollbackBatch func(uint64)
	BatchCredits  uint32
	ByteCredits   uint64
}

// RemotePipelineInformationChannel used to deliver remote receiver pipeline's information.
//
// remote run Server will use this channel to send information to dispatch operator.
type RemotePipelineInformationChannel chan *WrapCs

func (proc *Process) GetSession() Session {
	return proc.Session
}

func (proc *Process) GetMessageBoard() *message.MessageBoard {
	return proc.Base.messageBoard
}

func (proc *Process) SetMessageBoard(mb *message.MessageBoard) {
	proc.Base.messageBoard = mb
}

func (proc *Process) SetStmtProfile(sp *StmtProfile) {
	proc.Base.executionResourceBudgetMu.Lock()
	if proc.Base.executionResourceBudget != nil {
		proc.Base.executionResourceBudget.Close()
		proc.Base.executionResourceBudget = nil
	}
	proc.Base.executionResourceBudgetMu.Unlock()
	proc.Base.cteMemoryBudgetMu.Lock()
	if proc.Base.cteMemoryBudget != nil {
		proc.Base.cteMemoryBudget.Close()
		proc.Base.cteMemoryBudget = nil
	}
	proc.Base.cteMemoryBudgetMu.Unlock()
	proc.Base.StmtProfile = sp
	// Reset division by zero cache for new statement
	// Each statement must recompute based on its own type and sql_mode
	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, -1)
	if sp != nil {
		sp.clearStatementRuntimeProfile()
	}
}

func (proc *Process) GetStmtProfile() *StmtProfile {
	if proc.Base.StmtProfile != nil {
		return proc.Base.StmtProfile
	}
	return &StmtProfile{}
}

func (proc *Process) InitSeq() {
	proc.Base.SessionInfo.SeqCurValues = make(map[uint64]string)
	proc.Base.SessionInfo.SeqLastValue = make([]string, 1)
	proc.Base.SessionInfo.SeqLastValue[0] = ""
	proc.Base.SessionInfo.SeqAddValues = make(map[uint64]string)
	proc.Base.SessionInfo.SeqDeleteKeys = make([]uint64, 0)
}

func (proc *Process) SetMPool(mp *mpool.MPool) {
	proc.Base.mp = mp
}

func (proc *Process) SetFileService(fs fileservice.FileService) {
	proc.Base.FileService = fs
}

func (proc *Process) GetPrepareParamsAt(i int) ([]byte, error) {
	if proc.Base.prepareParams == nil || i < 0 || i >= proc.Base.prepareParams.Length() {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "get prepare params error, index %d not exists", i)
	}
	if proc.Base.prepareParams.IsNull(uint64(i)) {
		return nil, nil
	} else {
		val := proc.Base.prepareParams.GetRawBytesAt(i)
		return val, nil
	}
}

func (proc *Process) GetPrepareParamIsBin(i int) bool {
	return proc.getPrepareParamMeta(i, 0)
}

func (proc *Process) GetPrepareParamKind(i int) vector.PrepareParamKind {
	var kind vector.PrepareParamKind
	if proc.getPrepareParamMeta(i, 1) {
		kind |= 1
	}
	if proc.getPrepareParamMeta(i, 2) {
		kind |= 2
	}
	if proc.getPrepareParamMeta(i, 3) {
		kind |= 4
	}
	return kind
}

func (proc *Process) getPrepareParamMeta(i, section int) bool {
	paramCount := 0
	if proc.Base.prepareParams != nil {
		paramCount = proc.Base.prepareParams.Length()
	}
	offset := section*paramCount + i
	return section >= 0 && i >= 0 && i < paramCount && offset < len(proc.Base.prepareParamsIsBin) &&
		proc.Base.prepareParamsIsBin[offset]
}

func (proc *Process) GetPrepareParamIsBinaryString(i int) bool {
	return i >= 0 && i < len(proc.Base.prepareParamsBinaryString) && proc.Base.prepareParamsBinaryString[i]
}

// SetIncrStatementDisabled marks this process (and every child process
// sharing its BaseProcess) as running internal SQL that must not advance the
// workspace snapshot write offset. See BaseProcess.incrStatementDisabled.
func (proc *Process) SetIncrStatementDisabled(disabled bool) {
	proc.Base.incrStatementDisabled = disabled
}

// IncrStatementDisabled reports whether compiles on this process must skip
// advancing the workspace snapshot write offset.
func (proc *Process) IncrStatementDisabled() bool {
	return proc.Base.incrStatementDisabled
}

func (proc *Process) SetResolveVariableFunc(f func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)) {
	proc.Base.resolveVariableFunc = f
}

func (proc *Process) GetResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return proc.Base.resolveVariableFunc
}

func (proc *Process) SetResolveVariableIsBinFunc(f func(varName string, isSystemVar, isGlobalVar bool) (bool, error)) {
	proc.Base.resolveVariableIsBinFunc = f
}

func (proc *Process) GetResolveVariableIsBinFunc() func(varName string, isSystemVar, isGlobalVar bool) (bool, error) {
	return proc.Base.resolveVariableIsBinFunc
}

func (proc *Process) SetResolveVariableBinaryStringFunc(f func(varName string, isSystemVar, isGlobalVar bool) (bool, error)) {
	proc.Base.resolveVariableBinaryStringFunc = f
}

func (proc *Process) GetResolveVariableBinaryStringFunc() func(varName string, isSystemVar, isGlobalVar bool) (bool, error) {
	return proc.Base.resolveVariableBinaryStringFunc
}

func (proc *Process) SetResolveVariablePrepareParamKindFunc(
	f func(varName string, isSystemVar, isGlobalVar bool) (vector.PrepareParamKind, error),
) {
	proc.Base.resolveVariablePrepareParamKindFunc = f
}

func (proc *Process) GetResolveVariablePrepareParamKindFunc() func(
	varName string,
	isSystemVar, isGlobalVar bool,
) (vector.PrepareParamKind, error) {
	return proc.Base.resolveVariablePrepareParamKindFunc
}

func (proc *Process) SetLastInsertID(num uint64) {
	if proc.Base.LastInsertID != nil {
		atomic.StoreUint64(proc.Base.LastInsertID, num)
	}
}

func (proc *Process) GetSessionInfo() *SessionInfo {
	return &proc.Base.SessionInfo
}

func (proc *Process) GetLastInsertID() uint64 {
	if proc.Base.LastInsertID != nil {
		num := atomic.LoadUint64(proc.Base.LastInsertID)
		return num
	}
	return 0
}

func (proc *Process) SetAffectedRows(num int64) {
	if proc.Base.AffectedRows != nil {
		atomic.StoreInt64(proc.Base.AffectedRows, num)
	}
}

func (proc *Process) GetAffectedRows() int64 {
	if proc.Base.AffectedRows != nil {
		return atomic.LoadInt64(proc.Base.AffectedRows)
	}
	return 0
}

func (proc *Process) SetCacheForAutoCol(name string) {
	aicm := proc.Base.Aicm
	aicm.Mu.Lock()
	defer aicm.Mu.Unlock()
	aicm.AutoIncrCaches[name] = defines.AutoIncrCache{CurNum: 0, MaxNum: aicm.MaxSize, Step: 1}
}

func (proc *Process) SetCloneTxnOperator(op client.TxnOperator) {
	proc.Base.CloneTxnOperator = op
}

func (proc *Process) GetCloneTxnOperator() client.TxnOperator {
	return proc.Base.CloneTxnOperator
}

func (proc *Process) GetTxnOperator() client.TxnOperator {
	return proc.Base.TxnOperator
}

// SetPlanSnapshotTS binds this process to the snapshot used to build its plan.
// Child pipeline processes inherit the immutable binding pointer.
func (proc *Process) SetPlanSnapshotTS(ts timestamp.Timestamp) {
	proc.planSnapshotTS = &ts
}

// ClearPlanSnapshotTS removes the plan binding. Lock callers without a plan
// then retain the legacy transaction-snapshot behavior.
func (proc *Process) ClearPlanSnapshotTS() {
	proc.planSnapshotTS = nil
}

// GetPlanSnapshotTS returns the immutable plan snapshot for this execution
// generation and whether one was bound.
func (proc *Process) GetPlanSnapshotTS() (timestamp.Timestamp, bool) {
	if proc.planSnapshotTS == nil {
		return timestamp.Timestamp{}, false
	}
	return *proc.planSnapshotTS, true
}

// CopyPlanSnapshotFrom propagates one execution generation's binding to a
// child or reused pipeline process without exposing the presence bit.
func (proc *Process) CopyPlanSnapshotFrom(parent *Process) {
	if parent == nil {
		proc.ClearPlanSnapshotTS()
		return
	}
	proc.planSnapshotTS = parent.planSnapshotTS
}

func (proc *Process) GetBaseProcessRunningStatus() bool {
	return proc.Base.atRuntime
}

func (proc *Process) SetBaseProcessRunningStatus(status bool) {
	proc.Base.atRuntime = status
}

func (proc *Process) GetPostDmlSqlList() *threadsafe.Slice[string] {
	return proc.Base.PostDmlSqlList
}

func (proc *Process) GetStageCache() *threadsafe.Map[string, stage.StageDef] {
	return proc.Base.StageCache
}

func (si *SessionInfo) GetUser() string {
	return si.User
}

func (si *SessionInfo) GetHost() string {
	return si.Host
}

func (si *SessionInfo) GetUserHost() string {
	//currently, the host_name is 'localhost'
	return si.User + "@localhost"
}

func (si *SessionInfo) GetRole() string {
	return si.Role
}

func (si *SessionInfo) GetCharset() string {
	return "utf8mb4"
}

func (si *SessionInfo) GetCollation() string {
	return "utf8mb4_general_ci"
}

func (si *SessionInfo) GetConnectionID() uint64 {
	return si.ConnectionID
}

// GetUserLevelLockIdentity returns the immutable user-level lock identity
// pinned to this top process. Child processes share BaseProcess and therefore
// observe the same session identity.
func (proc *Process) GetUserLevelLockIdentity() (string, uint64) {
	if proc == nil || proc.Base == nil {
		return "", 0
	}
	proc.Base.userLevelLockIdentityMu.Lock()
	defer proc.Base.userLevelLockIdentityMu.Unlock()
	return proc.Base.userLevelLockOwner, proc.Base.userLevelLockConnID
}

// PinUserLevelLockIdentity installs the user-level lock identity once for the
// lifetime of this top process. It is intentionally not reset after the last
// lock is released: a concurrent acquisition must not be assigned a different
// synthetic transaction owner, and SET CONNECTION ID must not mutate it.
func (proc *Process) PinUserLevelLockIdentity(owner string, connID uint64) (string, uint64) {
	if proc == nil || proc.Base == nil {
		return "", 0
	}
	proc.Base.userLevelLockIdentityMu.Lock()
	defer proc.Base.userLevelLockIdentityMu.Unlock()
	if proc.Base.userLevelLockOwner == "" {
		if proc.Base.userLevelLockGeneration == "" {
			proc.Base.userLevelLockGeneration = uuid.New().String()
		}
		if proc.Base.userLevelLockGeneration != "" && strings.Count(owner, ":") < 2 {
			owner = owner + ":" + proc.Base.userLevelLockGeneration
		}
		proc.Base.userLevelLockOwner = owner
		proc.Base.userLevelLockConnID = connID
	}
	return proc.Base.userLevelLockOwner, proc.Base.userLevelLockConnID
}

func (si *SessionInfo) GetDatabase() string {
	return si.Database
}

func (si *SessionInfo) GetVersion() string {
	return si.Version
}

func (proc *Process) DebugBreakDump(cond bool) {
	if proc.Base.SessionInfo.User == "dump" && cond {
		logutil.GetGlobalLogger().Info("debug break dump")
	}
}
