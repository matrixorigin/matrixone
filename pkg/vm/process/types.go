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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// Analyze analyze information for operator
type Analyze interface {
	Stop()
	Start()
	Alloc(int64)
	Input(*batch.Batch)
	Output(*batch.Batch)
}

// WaitRegister channel
type WaitRegister struct {
	Ctx context.Context
	Ch  chan *batch.Batch
}

// Register used in execution pipeline and shared with all operators of the same pipeline.
type Register struct {
	// Ss, temporarily stores the row number list in the execution of operators
	// and it can be reused in the future execution.
	Ss [][]int64
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.Batch
	// MergeReceivers, receives result of multi previous operators from other pipelines
	// e.g. merge operator.
	MergeReceivers []*WaitRegister
}

//Limitation specifies the maximum resources that can be used in one query.
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
}

// SessionInfo session information
type SessionInfo struct {
	User         string
	Host         string
	Role         string
	ConnectionID uint64
	Database     string
	Version      string
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
	// InputSize, data size accepted by node
	InputSize int64
	// OutputSize, data size output by node
	OutputSize int64
	// MemorySize, memory alloc by node
	MemorySize int64
}

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	// Id, query id.
	Id  string
	Reg Register
	Lim Limitation
	Mp  *mheap.Mheap

	// unix timestamp
	UnixTime int64

	// snapshot is transaction context
	Snapshot []byte

	AnalInfos []*AnalyzeInfo

	SessionInfo SessionInfo

	// snapshot is transaction context
	Cancel context.CancelFunc
}

type analyze struct {
	start    time.Time
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
