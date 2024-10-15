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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/hayageek/threadsafe"
	"go.uber.org/zap/zapcore"

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
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var (
	NormalEndRegisterMessage = NewRegMsg(nil)
)

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

// WaitRegister channel
type WaitRegister struct {
	// Ch2, data receiver's channel for receive-action-signal.
	Ch2 chan PipelineSignal

	// how many nil-batches this channel can receive, default 0 means every nil batch close channel
	NilBatchCnt int
}

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
	// MaxMessageSize max size for read messages from dn
	MaxMsgSize uint64
}

// SessionInfo session information
type SessionInfo struct {
	Account              string
	User                 string
	Host                 string
	Role                 string
	ConnectionID         uint64
	AccountId            uint32
	RoleId               uint32
	UserId               uint32
	LastInsertID         uint64
	Database             string
	Version              string
	TimeZone             *time.Location
	StorageEngine        engine.Engine
	QueryId              []string
	ResultColTypes       []types.Type
	SeqCurValues         map[uint64]string
	SeqDeleteKeys        []uint64
	SeqAddValues         map[uint64]string
	SeqLastValue         []string
	SqlHelper            sqlHelper
	Buf                  *buffer.Buffer
	SourceInMemScanBatch []*kafka.Message
	LogLevel             zapcore.Level
	SessionId            uuid.UUID
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

	StmtProfile *StmtProfile
	// Id, query id.
	Id              string
	Lim             Limitation
	mp              *mpool.MPool
	prepareBatch    *batch.Batch
	prepareExprList any
	valueScanBatch  map[[16]byte]*batch.Batch
	// unix timestamp
	UnixTime            int64
	TxnClient           client.TxnClient
	SessionInfo         SessionInfo
	FileService         fileservice.FileService
	LockService         lockservice.LockService
	IncrService         incrservice.AutoIncrementService
	LoadTag             bool
	LastInsertID        *uint64
	LoadLocalReader     *io.PipeReader
	Aicm                *defines.AutoIncrCacheManager
	resolveVariableFunc func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	prepareParams       *vector.Vector
	QueryClient         qclient.QueryClient
	Hakeeper            logservice.CNHAKeeperClient
	UdfService          udf.Service
	WaitPolicy          lock.WaitPolicy
	messageBoard        *message.MessageBoard
	logger              *log.MOLogger
	TxnOperator         client.TxnOperator
	CloneTxnOperator    client.TxnOperator

	// post dml sqls run right after all pipelines finished.
	PostDmlSqlList *threadsafe.Slice[string]
}

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	// BaseProcess is the common part of one process, and it's shared by all its children processes.
	Base *BaseProcess
	Reg  Register

	// Ctx and Cancel are pipeline's context and cancel function.
	// Every pipeline has its own context, and the lifecycle of the pipeline is controlled by the context.
	Ctx    context.Context
	Cancel context.CancelFunc
}

type sqlHelper interface {
	GetCompilerContext() any
	ExecSql(string) ([][]interface{}, error)
	GetSubscriptionMeta(string) (sub *plan.SubscriptionMeta, err error)
}

// WrapCs record information about pipeline's remote receiver.
type WrapCs struct {
	sync.RWMutex
	ReceiverDone bool
	MsgId        uint64
	Uid          uuid.UUID
	Cs           morpc.ClientSession
	Err          chan error
}

// RemotePipelineInformationChannel used to deliver remote receiver pipeline's information.
//
// remote run Server will use this channel to send information to dispatch operator.
type RemotePipelineInformationChannel chan *WrapCs

func (proc *Process) GetMessageBoard() *message.MessageBoard {
	return proc.Base.messageBoard
}

func (proc *Process) SetMessageBoard(mb *message.MessageBoard) {
	proc.Base.messageBoard = mb
}

func (proc *Process) SetStmtProfile(sp *StmtProfile) {
	proc.Base.StmtProfile = sp
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

func (proc *Process) SetValueScanBatch(key uuid.UUID, batch *batch.Batch) {
	proc.Base.valueScanBatch[key] = batch
}

func (proc *Process) GetValueScanBatch(key uuid.UUID) *batch.Batch {
	return proc.Base.valueScanBatch[key]
}

func (proc *Process) CleanValueScanBatchs() {
	mp := proc.Mp()
	for k, bat := range proc.Base.valueScanBatch {
		if bat != nil {
			bat.Clean(mp)
		}
		// todo: why not remake the map after all clean ?
		delete(proc.Base.valueScanBatch, k)
	}
}

func (proc *Process) GetPrepareParamsAt(i int) ([]byte, error) {
	if i < 0 || i >= proc.Base.prepareParams.Length() {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "get prepare params error, index %d not exists", i)
	}
	if proc.Base.prepareParams.IsNull(uint64(i)) {
		return nil, nil
	} else {
		val := proc.Base.prepareParams.GetRawBytesAt(i)
		return val, nil
	}
}

func (proc *Process) SetResolveVariableFunc(f func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)) {
	proc.Base.resolveVariableFunc = f
}

func (proc *Process) GetResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return proc.Base.resolveVariableFunc
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

func (proc *Process) GetBaseProcessRunningStatus() bool {
	return proc.Base.atRuntime
}

func (proc *Process) SetBaseProcessRunningStatus(status bool) {
	proc.Base.atRuntime = status
}

func (proc *Process) GetPostDmlSqlList() *threadsafe.Slice[string] {
	return proc.Base.PostDmlSqlList
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

func (si *SessionInfo) GetDatabase() string {
	return si.Database
}

func (si *SessionInfo) GetVersion() string {
	return si.Version
}
