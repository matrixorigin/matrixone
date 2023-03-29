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

package memoryengine

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	OpCreateDatabase = iota + 64
	OpOpenDatabase
	OpGetDatabases
	OpDeleteDatabase
	OpCreateRelation
	OpDeleteRelation
	OpTruncateRelation
	OpOpenRelation
	OpGetRelations
	OpAddTableDef
	OpDelTableDef
	OpDelete
	OpGetPrimaryKeys
	OpGetTableColumns
	OpGetTableDefs
	OpGetHiddenKeys
	OpUpdate
	OpWrite
	OpNewTableIter
	OpRead
	OpCloseTableIter
	OpTableStats
	OpPreCommit  = uint32(apipb.OpCode_OpPreCommit)
	OpGetLogTail = uint32(apipb.OpCode_OpGetLogTail)
)

type ReadRequest interface {
	OpenDatabaseReq |
		GetDatabasesReq |
		OpenRelationReq |
		GetRelationsReq |
		GetPrimaryKeysReq |
		GetTableColumnsReq |
		GetTableDefsReq |
		GetHiddenKeysReq |
		NewTableIterReq |
		ReadReq |
		CloseTableIterReq |
		TableStatsReq |
		apipb.SyncLogTailReq
}

type WriteReqeust interface {
	CreateDatabaseReq |
		DeleteDatabaseReq |
		CreateRelationReq |
		DeleteRelationReq |
		TruncateRelationReq |
		AddTableDefReq |
		DelTableDefReq |
		DeleteReq |
		UpdateReq |
		WriteReq
}

type Request interface {
	ReadRequest | WriteReqeust
}

type Response interface {
	CreateDatabaseResp |
		OpenDatabaseResp |
		GetDatabasesResp |
		DeleteDatabaseResp |
		CreateRelationResp |
		DeleteRelationResp |
		TruncateRelationResp |
		OpenRelationResp |
		GetRelationsResp |
		AddTableDefResp |
		DelTableDefResp |
		DeleteResp |
		GetPrimaryKeysResp |
		GetTableColumnsResp |
		GetTableDefsResp |
		GetHiddenKeysResp |
		UpdateResp |
		WriteResp |
		NewTableIterResp |
		ReadResp |
		CloseTableIterResp |
		TableStatsResp |
		apipb.SyncLogTailResp
}

type CreateDatabaseReq struct {
	ID         ID
	AccessInfo AccessInfo
	Name       string
	Typ        string
	CreateSql  string
}

type CreateDatabaseResp struct {
	ID ID
}

type OpenDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

type OpenDatabaseResp struct {
	ID        ID
	Name      string
	DatTyp    string
	CreateSql string
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
	ID           ID
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

type TruncateRelationReq struct {
	NewTableID   ID
	OldTableID   ID
	DatabaseID   ID
	DatabaseName string
	Name         string
}

type DeleteRelationResp struct {
	ID ID
}

type TruncateRelationResp struct {
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

type GetTableColumnsReq struct {
	TableID ID
}

type GetTableColumnsResp struct {
	Attrs []*engine.Attribute
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

//type TruncateReq struct {
//	TableID      ID
//	DatabaseName string
//	TableName    string
//}
//
//type TruncateResp struct {
//	AffectedRows int64
//}

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

	mp *mpool.MPool
}

func (r *ReadResp) Close() error {
	if r.Batch != nil {
		r.Batch.Clean(r.mp)
	}
	return nil
}

func (r *ReadResp) SetHeap(mp *mpool.MPool) {
	r.mp = mp
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
