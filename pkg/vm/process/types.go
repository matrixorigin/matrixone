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

	"github.com/google/uuid"
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
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Analyze analyzes information for operator
type Analyze interface {
	Stop()
	Start()
	Alloc(int64)
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

// WaitRegister channel
type WaitRegister struct {
	Ctx context.Context
	Ch  chan *batch.Batch
}

// Register used in execution pipeline and shared with all operators of the same pipeline.
type Register struct {
	// Ss, temporarily stores the row number list in the execution of operators,
	// and it can be reused in the future execution.
	Ss [][]int64
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.Batch
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
	Account        string
	User           string
	Host           string
	Role           string
	ConnectionID   uint64
	AccountId      uint32
	RoleId         uint32
	UserId         uint32
	LastInsertID   uint64
	Database       string
	Version        string
	TimeZone       *time.Location
	StorageEngine  engine.Engine
	QueryId        []string
	ResultColTypes []types.Type
	SeqCurValues   map[uint64]string
	SeqDeleteKeys  []uint64
	SeqAddValues   map[uint64]string
	SeqLastValue   []string
	SqlHelper      sqlHelper
}

// AnalyzeInfo  analyze information for query
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
	InputSize int64
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
}

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	// Id, query id.
	Id  string
	Reg Register
	Lim Limitation

	vp           *vectorPool
	mp           *mpool.MPool
	prepareBatch *batch.Batch

	// unix timestamp
	UnixTime int64

	TxnClient client.TxnClient

	TxnOperator client.TxnOperator

	AnalInfos []*AnalyzeInfo

	SessionInfo SessionInfo

	Ctx context.Context

	Cancel context.CancelFunc

	FileService fileservice.FileService
	LockService lockservice.LockService
	IncrService incrservice.AutoIncrementService

	LoadTag bool

	LastInsertID *uint64

	LoadLocalReader *io.PipeReader

	DispatchNotifyCh chan WrapCs

	Aicm *defines.AutoIncrCacheManager

	resolveVariableFunc func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	prepareParams       []any
}

type vectorPool struct {
	sync.Mutex
	vecs map[uint8][]*vector.Vector
}

type sqlHelper interface {
	GetCompilerContext() any
	ExecSql(string) ([]interface{}, error)
}

type WrapCs struct {
	MsgId  uint64
	Uid    uuid.UUID
	Cs     morpc.ClientSession
	DoneCh chan struct{}
}

func (proc *Process) InitSeq() {
	proc.SessionInfo.SeqCurValues = make(map[uint64]string)
	proc.SessionInfo.SeqLastValue = make([]string, 1)
	proc.SessionInfo.SeqLastValue[0] = ""
	proc.SessionInfo.SeqAddValues = make(map[uint64]string)
	proc.SessionInfo.SeqDeleteKeys = make([]uint64, 0)
}

func (proc *Process) SetPrepareParams(prepareParams []any) {
	proc.prepareParams = prepareParams
}

func (proc *Process) GetPrepareParamsAt(i int) (any, error) {
	if i < 0 || i >= len(proc.prepareParams) {
		return nil, moerr.NewInternalError(proc.Ctx, "get prepare params error, index %d not exists", i)
	}
	return proc.prepareParams[i], nil
}

func (proc *Process) SetResolveVariableFunc(f func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)) {
	proc.resolveVariableFunc = f
}

func (proc *Process) GetResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return proc.resolveVariableFunc
}

func (proc *Process) SetLastInsertID(num uint64) {
	if proc.LastInsertID != nil {
		atomic.StoreUint64(proc.LastInsertID, num)
	}
}

func (proc *Process) GetSessionInfo() *SessionInfo {
	return &proc.SessionInfo
}

func (proc *Process) GetLastInsertID() uint64 {
	if proc.LastInsertID != nil {
		num := atomic.LoadUint64(proc.LastInsertID)
		return num
	}
	return 0
}

func (proc *Process) SetCacheForAutoCol(name string) {
	aicm := proc.Aicm
	aicm.Mu.Lock()
	defer aicm.Mu.Unlock()
	aicm.AutoIncrCaches[name] = defines.AutoIncrCache{CurNum: 0, MaxNum: aicm.MaxSize, Step: 1}
}

type analyze struct {
	start    time.Time
	wait     time.Duration
	analInfo *AnalyzeInfo
}

func (si *SessionInfo) GetUser() string {
	return si.User
}

func (si *SessionInfo) GetHost() string {
	return si.Host
}

func (si *SessionInfo) GetUserHost() string {
	return si.User + "@" + si.Host
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
