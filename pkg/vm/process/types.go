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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	S3IOCount(int)
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
	Account           string
	User              string
	Host              string
	Role              string
	ConnectionID      uint64
	AccountId         uint32
	RoleId            uint32
	UserId            uint32
	LastInsertID      uint64
	Database          string
	Version           string
	TimeZone          *time.Location
	StorageEngine     engine.Engine
	QueryId           []string
	ResultColTypes    []types.Type
	AutoIncrCaches    defines.AutoIncrCaches
	AutoIncrCacheSize uint64
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
	// S3IOCount, query count that read from s3
	S3IOCount int64
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

	mp *mpool.MPool

	// unix timestamp
	UnixTime int64

	TxnClient client.TxnClient

	TxnOperator client.TxnOperator

	AnalInfos []*AnalyzeInfo

	SessionInfo SessionInfo

	Ctx context.Context

	Cancel context.CancelFunc

	FileService fileservice.FileService

	LoadTag bool

	LastInsertID *uint64

	LoadLocalReader *io.PipeReader

	DispatchNotifyCh chan WrapCs
}

type WrapCs struct {
	MsgId  uint64
	Uid    uuid.UUID
	Cs     morpc.ClientSession
	DoneCh chan struct{}
}

func (proc *Process) SetLastInsertID(num uint64) {
	if proc.LastInsertID != nil {
		*proc.LastInsertID = num
	}
}

func (proc *Process) GetLastInsertID() uint64 {
	if proc.LastInsertID != nil {
		return *proc.LastInsertID
	}
	return 0
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
