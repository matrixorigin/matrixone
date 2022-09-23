// Copyright 2022 Matrix Origin
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

package txnengine

import (
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

const (
	OpCreateDatabase = iota + 64
	OpOpenDatabase
	OpGetDatabases
	OpDeleteDatabase
	OpCreateRelation
	OpDeleteRelation
	OpOpenRelation
	OpGetRelations
	OpAddTableDef
	OpDelTableDef
	OpDelete
	OpGetPrimaryKeys
	OpGetTableDefs
	OpGetHiddenKeys
	OpTruncate
	OpUpdate
	OpWrite
	OpNewTableIter
	OpRead
	OpCloseTableIter
	OpTableStats
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

	// plan types
	gob.Register(&plan.Expr_C{})
	gob.Register(&plan.Expr_P{})
	gob.Register(&plan.Expr_V{})
	gob.Register(&plan.Expr_Col{})
	gob.Register(&plan.Expr_F{})
	gob.Register(&plan.Expr_Sub{})
	gob.Register(&plan.Expr_Corr{})
	gob.Register(&plan.Expr_T{})
	gob.Register(&plan.Expr_List{})
	gob.Register(&plan.Const_Ival{})
	gob.Register(&plan.Const_Dval{})
	gob.Register(&plan.Const_Sval{})
	gob.Register(&plan.Const_Bval{})
	gob.Register(&plan.Const_Uval{})
	gob.Register(&plan.Const_Fval{})
	gob.Register(&plan.Const_Dateval{})
	gob.Register(&plan.Const_Datetimeval{})
	gob.Register(&plan.Const_Decimal64Val{})
	gob.Register(&plan.Const_Decimal128Val{})
	gob.Register(&plan.Const_Timestampval{})
	gob.Register(&plan.Const_Jsonval{})
	gob.Register(&plan.Const_Defaultval{})

}

type Request interface {
	CreateDatabaseReq |
		OpenDatabaseReq |
		GetDatabasesReq |
		DeleteDatabaseReq |
		CreateRelationReq |
		DeleteRelationReq |
		OpenRelationReq |
		GetRelationsReq |
		AddTableDefReq |
		DelTableDefReq |
		DeleteReq |
		GetPrimaryKeysReq |
		GetTableDefsReq |
		GetHiddenKeysReq |
		TruncateReq |
		UpdateReq |
		WriteReq |
		NewTableIterReq |
		ReadReq |
		CloseTableIterReq |
		TableStatsReq |
		apipb.SyncLogTailReq
}

type Response interface {
	CreateDatabaseResp |
		OpenDatabaseResp |
		GetDatabasesResp |
		DeleteDatabaseResp |
		CreateRelationResp |
		DeleteRelationResp |
		OpenRelationResp |
		GetRelationsResp |
		AddTableDefResp |
		DelTableDefResp |
		DeleteResp |
		GetPrimaryKeysResp |
		GetTableDefsResp |
		GetHiddenKeysResp |
		TruncateResp |
		UpdateResp |
		WriteResp |
		NewTableIterResp |
		ReadResp |
		CloseTableIterResp |
		TableStatsResp |
		apipb.SyncLogTailResp
}

type CreateDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

type CreateDatabaseResp struct {
	ID ID
}

type OpenDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

type OpenDatabaseResp struct {
	ID   ID
	Name string
}

type GetDatabasesReq struct {
	AccessInfo AccessInfo
}

type GetDatabasesResp struct {
	Names []string
}

type DeleteDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

type DeleteDatabaseResp struct {
	ID ID
}

type CreateRelationReq struct {
	DatabaseID   ID
	DatabaseName string
	Name         string
	Type         RelationType
	Defs         []engine.TableDef
}

type CreateRelationResp struct {
	ID ID
}

type DeleteRelationReq struct {
	DatabaseID   ID
	DatabaseName string
	Name         string
}

type DeleteRelationResp struct {
	ID ID
}

type OpenRelationReq struct {
	DatabaseID   ID
	DatabaseName string
	Name         string
}

type OpenRelationResp struct {
	ID           ID
	Type         RelationType
	DatabaseName string
	RelationName string
}

type GetRelationsReq struct {
	DatabaseID ID
}

type GetRelationsResp struct {
	Names []string
}

type AddTableDefReq struct {
	TableID ID
	Def     engine.TableDef

	DatabaseName string
	TableName    string
}

type AddTableDefResp struct {
}

type DelTableDefReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Def          engine.TableDef
}

type DelTableDefResp struct {
}

type DeleteReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	ColumnName   string
	Vector       *vector.Vector
}

type DeleteResp struct {
}

type GetPrimaryKeysReq struct {
	TableID ID
}

type GetPrimaryKeysResp struct {
	Attrs []*engine.Attribute
}

type GetTableDefsReq struct {
	TableID ID
}

type GetTableDefsResp struct {
	Defs []engine.TableDef
}

type GetHiddenKeysReq struct {
	TableID ID
}

type GetHiddenKeysResp struct {
	Attrs []*engine.Attribute
}

type TruncateReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
}

type TruncateResp struct {
	AffectedRows int64
}

type UpdateReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Batch        *batch.Batch
}

type UpdateResp struct {
}

type WriteReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Batch        *batch.Batch
}

type WriteResp struct {
}

type NewTableIterReq struct {
	TableID ID
	Expr    *plan.Expr
}

type NewTableIterResp struct {
	IterID ID
}

type ReadReq struct {
	IterID   ID
	ColNames []string
}

type ReadResp struct {
	Batch *batch.Batch

	heap *mheap.Mheap
}

func (r *ReadResp) Close() error {
	if r.Batch != nil {
		r.Batch.Clean(r.heap)
	}
	return nil
}

func (r *ReadResp) SetHeap(heap *mheap.Mheap) {
	r.heap = heap
}

type CloseTableIterReq struct {
	IterID ID
}

type CloseTableIterResp struct {
}

type TableStatsReq struct {
	TableID ID
}

type TableStatsResp struct {
	Rows int
}
