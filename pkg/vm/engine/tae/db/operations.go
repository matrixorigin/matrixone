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
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	OpPreCommit  = uint32(apipb.OpCode_OpPreCommit)
	OpGetLogTail = uint32(apipb.OpCode_OpGetLogTail)
)

func init() {

	// register TableDef types
	gob.Register(new(engine.ViewDef))
	gob.Register(new(engine.CommentDef))
	gob.Register(new(engine.PartitionDef))
	gob.Register(new(engine.AttributeDef))
	gob.Register(new(engine.IndexTableDef))
	gob.Register(new(engine.PropertiesDef))
	gob.Register(new(engine.PrimaryIndexDef))
	gob.Register(new(engine.ComputeIndexDef))

	// register vector column types
	gob.Register([]bool{})
	gob.Register([]int8{})
	gob.Register([]int16{})
	gob.Register([]int32{})
	gob.Register([]int64{})
	gob.Register([]uint8{})
	gob.Register([]uint16{})
	gob.Register([]uint32{})
	gob.Register([]uint64{})
	gob.Register([]float32{})
	gob.Register([]float64{})
	gob.Register([]string{})
	gob.Register([][]any{})
	gob.Register([]types.Date{})
	gob.Register([]types.Datetime{})
	gob.Register([]types.Timestamp{})
	gob.Register([]types.Decimal64{})
	gob.Register([]types.Decimal128{})

	//plan types

}

type Request interface {
	CreateDatabaseReq |
		DropDatabaseReq |
		CreateRelationReq |
		DropOrTruncateRelationReq |
		WriteReq |
		apipb.SyncLogTailReq
}

type Response interface {
	CreateDatabaseResp |
		DropDatabaseResp |
		CreateRelationResp |
		DropOrTruncateRelationResp
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
	//Global unique, allocated by CN .
	DatabaseId uint64
}

type CreateDatabaseResp struct {
	ID uint64
}

type DropDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
	ID         uint64
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

type CreateRelationResp struct {
	ID uint64
}

type DropOrTruncateRelationReq struct {
	AccessInfo   AccessInfo
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

type WriteReq struct {
	AccessInfo   AccessInfo
	Type         EntryType
	TableID      uint64
	DatabaseName string
	TableName    string
	FileName     string
	BlockID      uint64
	Batch        *batch.Batch
}

type WriteResp struct {
}
