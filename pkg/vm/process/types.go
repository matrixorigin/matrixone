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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

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
	// Vecs, temporarily stores the column data in the execution of operators
	// and it can be reused in the future execution to avoid mem alloc and type casting overhead.
	Vecs []*vector.Vector
	// MergeReceivers, receives result of multi previous operators from other pipelines
	// e.g. merge operator.
	MergeReceivers []*WaitRegister
}

//Limitation specifies the maximum resources that can be used in one query.
type Limitation struct {
	// Size, memory threshold.
	Size int64
	// BatchRows, max rows for batch.
	BatchRows int64
	// BatchSize, max size for batch.
	BatchSize int64
	// PartitionRows, max rows for partition.
	PartitionRows int64
}

// session information
type SessionInfo struct {
	User         string
	Host         string
	Role         string
	ConnectionID uint64
	Database     string
	Version      string
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

	SessionInfo SessionInfo

	// snapshot is transaction context
	Cancel context.CancelFunc
}
