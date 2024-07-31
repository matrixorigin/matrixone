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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
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

// Analyze analyzes information for operator
type Analyze interface {
	Stop()
	ChildrenCallStop(time.Time)
	Start()
	Alloc(int64)
	InputBlock()
	Input(*batch.Batch, bool)
	Output(*batch.Batch, bool)
	WaitStop(time.Time)
	DiskIO(*batch.Batch)
	S3IOByte(*batch.Batch)
	S3IOInputCount(int)
	S3IOOutputCount(int)
	Network(*batch.Batch)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
}

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
	Ctx context.Context
	Ch  chan *RegisterMessage
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

// AnalyzeInfo  operatorAnalyzer information for query
type AnalyzeInfo struct {
	// NodeId, index of query's node list
	NodeId int32
	// InputRows, number of rows accepted by node
	InputRows int64
	// OutputRows, number of rows output by node
	OutputRows int64
	// TimeConsumed, time taken by the node in milliseconds
	TimeConsumed int64
	// WaitTimeConsumed, time taken by the node waiting for channel in milliseconds
	WaitTimeConsumed int64
	// InputSize, data size accepted by node
	InputSize   int64
	InputBlocks int64
	// OutputSize, data size output by node
	OutputSize int64
	// MemorySize, memory alloc by node
	MemorySize int64
	// DiskIO, data size read from disk
	DiskIO int64
	// S3IOByte, data size read from s3
	S3IOByte int64
	// S3IOInputCount, count for PUT, COPY, POST and LIST
	S3IOInputCount int64
	// S3IOOutputCount, count for GET, SELECT and other
	S3IOOutputCount int64
	// NetworkIO, message size send between CN node
	NetworkIO int64
	// ScanTime, scan cost time in external scan
	ScanTime int64
	// InsertTime, insert cost time in load flow
	InsertTime int64

	// time consumed by every single parallel
	mu                     *sync.Mutex
	TimeConsumedArrayMajor []int64
	TimeConsumedArrayMinor []int64
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

// the common part of process.
// each query share only 1 instance of BaseProcess
type BaseProcess struct {
	StmtProfile *StmtProfile
	// Id, query id.
	Id              string
	Lim             Limitation
	vp              *cachedVectorPool
	mp              *mpool.MPool
	prepareBatch    *batch.Batch
	prepareExprList any
	valueScanBatch  map[[16]byte]*batch.Batch
	// unix timestamp
	UnixTime            int64
	TxnClient           client.TxnClient
	AnalInfos           []*AnalyzeInfo
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
	MessageBoard        *MessageBoard
	logger              *log.MOLogger
	TxnOperator         client.TxnOperator
	CloneTxnOperator    client.TxnOperator
}

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	Base             *BaseProcess
	Reg              Register
	Ctx              context.Context
	Cancel           context.CancelFunc
	DispatchNotifyCh chan *WrapCs
}

type sqlHelper interface {
	GetCompilerContext() any
	ExecSql(string) ([][]interface{}, error)
	GetSubscriptionMeta(string) (sub *plan.SubscriptionMeta, err error)
}

type WrapCs struct {
	sync.RWMutex
	ReceiverDone bool
	MsgId        uint64
	Uid          uuid.UUID
	Cs           morpc.ClientSession
	Err          chan error
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

func (proc *Process) SetValueScanBatch(key uuid.UUID, batch *batch.Batch) {
	proc.Base.valueScanBatch[key] = batch
}

func (proc *Process) GetValueScanBatch(key uuid.UUID) *batch.Batch {
	bat, ok := proc.Base.valueScanBatch[key]
	if ok {
		bat.SetCnt(1000) // make sure this batch wouldn't be cleaned
		return bat
		// delete(proc.valueScanBatch, key)
	}
	return bat
}

func (proc *Process) CleanValueScanBatchs() {
	for k, bat := range proc.Base.valueScanBatch {
		bat.SetCnt(1)
		bat.Clean(proc.Mp())
		delete(proc.Base.valueScanBatch, k)
	}
}

func (proc *Process) GetValueScanBatchs() []*batch.Batch {
	var bats []*batch.Batch

	for k, bat := range proc.Base.valueScanBatch {
		if bat != nil {
			bats = append(bats, bat)
		}
		delete(proc.Base.valueScanBatch, k)
	}
	return bats
}

func (proc *Process) GetPrepareParamsAt(i int) ([]byte, error) {
	if i < 0 || i >= proc.Base.prepareParams.Length() {
		return nil, moerr.NewInternalError(proc.Ctx, "get prepare params error, index %d not exists", i)
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

// Operator Resource Analzyer
type operatorAnalyzer struct {
	parallelMajor        bool
	parallelIdx          int
	start                time.Time
	wait                 time.Duration
	analInfo             *AnalyzeInfo
	childrenCallDuration time.Duration
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

func (a *AnalyzeInfo) AddNewParallel(major bool) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if major {
		a.TimeConsumedArrayMajor = append(a.TimeConsumedArrayMajor, 0)
		return len(a.TimeConsumedArrayMajor) - 1
	} else {
		a.TimeConsumedArrayMinor = append(a.TimeConsumedArrayMinor, 0)
		return len(a.TimeConsumedArrayMinor) - 1
	}
}

func (a *AnalyzeInfo) DeepCopyArray(pa *plan.AnalyzeInfo) {
	a.mu.Lock()
	defer a.mu.Unlock()
	pa.TimeConsumedArrayMajor = pa.TimeConsumedArrayMajor[:0]
	pa.TimeConsumedArrayMajor = append(pa.TimeConsumedArrayMajor, a.TimeConsumedArrayMajor...)
	pa.TimeConsumedArrayMinor = pa.TimeConsumedArrayMinor[:0]
	pa.TimeConsumedArrayMinor = append(pa.TimeConsumedArrayMinor, a.TimeConsumedArrayMinor...)
}

func (a *AnalyzeInfo) MergeArray(pa *plan.AnalyzeInfo) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.TimeConsumedArrayMajor = append(a.TimeConsumedArrayMajor, pa.TimeConsumedArrayMajor...)
	a.TimeConsumedArrayMinor = append(a.TimeConsumedArrayMinor, pa.TimeConsumedArrayMinor...)
}

func (a *AnalyzeInfo) AddSingleParallelTimeConsumed(major bool, parallelIdx int, t int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if major {
		if parallelIdx >= 0 && parallelIdx < len(a.TimeConsumedArrayMajor) {
			a.TimeConsumedArrayMajor[parallelIdx] += t
		}
	} else {
		if parallelIdx >= 0 && parallelIdx < len(a.TimeConsumedArrayMinor) {
			a.TimeConsumedArrayMinor[parallelIdx] += t
		}
	}
}
