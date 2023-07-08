// Copyright 2021 - 2022 Matrix Origin
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

package db

import (
	"context"
	fmt "fmt"
	"time"

	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	OpPreCommit  = uint32(apipb.OpCode_OpPreCommit)
	OpGetLogTail = uint32(apipb.OpCode_OpGetLogTail)
)

type Request interface {
	CreateDatabaseReq |
		DropDatabaseReq |
		CreateRelationReq |
		DropOrTruncateRelationReq |
		UpdateConstraintReq |
		WriteReq |
		apipb.SyncLogTailReq
}

type Response interface {
	CreateDatabaseResp |
		DropDatabaseResp |
		CreateRelationResp |
		DropOrTruncateRelationResp |
		UpdateConstraintResp |
		WriteResp |
		apipb.SyncLogTailResp
}

type RelationType uint8

const (
	RelationTable RelationType = iota + 1
	RelationView
)

type AccessInfo struct {
	AccountID uint32
	UserID    uint32
	RoleID    uint32
}

type CreateDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
	CreateSql  string
	DatTyp     string
	//Global unique, allocated by CN .
	DatabaseId uint64
}

type FlushTable struct {
	AccessInfo AccessInfo
	DatabaseID uint64
	TableID    uint64
}

func (m *FlushTable) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *FlushTable) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type Checkpoint struct {
	FlushDuration time.Duration
}

func (m *Checkpoint) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *Checkpoint) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type InspectDN struct {
	AccessInfo AccessInfo
	Operation  string
}

func (m *InspectDN) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *InspectDN) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

const (
	EnableFaultInjection = "enable_fault_injection"
	DisableFaultInjection = "disable_fault_injection"
)

type FaultPoint struct {
	Name   string
	Freq   string
	Action string
	Iarg   int64
	Sarg   string
}

func (m *FaultPoint) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *FaultPoint) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type CreateDatabaseResp struct {
	ID uint64
}

type DropDatabaseReq struct {
	Name string
	ID   uint64
}

type DropDatabaseResp struct {
	ID uint64
}

type CreateRelationReq struct {
	AccessInfo   AccessInfo
	DatabaseID   uint64
	DatabaseName string
	Name         string
	RelationId   uint64
	Type         RelationType
	Defs         []engine.TableDef
}

func (req *CreateRelationReq) String() string {
	return fmt.Sprintf("%+v, %d-%s:%d-%s",
		req.AccessInfo, req.DatabaseID, req.DatabaseName, req.RelationId, req.Name)
}

type UpdateConstraintReq struct {
	TableId      uint64
	TableName    string
	DatabaseId   uint64
	DatabaseName string
	Constraint   []byte
}

type UpdateConstraintResp struct{}

type CreateRelationResp struct {
	ID uint64
}

type DropOrTruncateRelationReq struct {
	IsDrop       bool
	DatabaseID   uint64
	DatabaseName string
	Name         string
	ID           uint64
	NewId        uint64
}

type DropOrTruncateRelationResp struct {
}

type EntryType int32

const (
	EntryInsert EntryType = 0
	EntryDelete EntryType = 1
)

type PKCheckType int32

const (
	//IncrementalDedup do not check uniqueness of PK before txn's snapshot TS.
	IncrementalDedup PKCheckType = 0
	//FullSkipWorkspaceDedup do not check uniqueness of PK against txn's workspace.
	FullSkipWorkspaceDedup PKCheckType = 1
	FullDedup              PKCheckType = 2
)

type LocationKey struct{}

// writeReq responds to entry
type WriteReq struct {
	Type         EntryType
	DatabaseId   uint64
	TableID      uint64
	DatabaseName string
	TableName    string
	Schema       *catalog2.Schema
	Batch        *batch.Batch
	//[IncrementalDedup|FullSkipWorkspaceDedup|FullDedup], default is IncrementalDedup.
	//If incremental-dedup in dn.toml is false, IncrementalDedup will be treated as FullSkipWorkspaceDedup.
	//IncrementalDedup do not check uniqueness of PK before txn's snapshot TS.
	//FullSkipWorkspaceDedup do not check uniqueness of PK against txn's workspace.
	PkCheck PKCheckType
	//S3 object file name
	FileName string
	MetaLocs []string
	//for delete on S3
	DeltaLocs []string
	//tasks for loading primary keys or deleted row ids
	Jobs []*tasks.Job
	//loaded sorted primary keys or deleted row ids.
	JobRes []*tasks.JobResult
	//load context cancel function
	Cancel context.CancelFunc
}

type WriteResp struct {
}

type InspectResp struct {
	Typ     int    `json:"-"`
	Message string `json:"msg"`
	Payload []byte `json:"-"`
}

func (m *InspectResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *InspectResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

const (
	InspectNormal = 0
	InspectCata   = 1
)

func (m *InspectResp) GetResponse() any {
	switch m.Typ {
	case InspectCata:
		resp := new(CatalogResp)
		types.Decode(m.Payload, resp)
		return resp
	}
	return m
}

type CatalogResp struct {
	Item string         `json:"Item,omitempty"`
	Sub  []*CatalogResp `json:"Sub,omitempty"`
}

func (m *CatalogResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CatalogResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}
