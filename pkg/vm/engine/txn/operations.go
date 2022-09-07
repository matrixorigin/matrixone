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
	OpOpenRelation
	OpGetRelations
	OpAddTableDef
	OpDelTableDef
	OpDelete
	OpGetPrimaryKeys
	OpGetTableDefs
	OpTruncate
	OpUpdate
	OpWrite
	OpNewTableIter
	OpRead
	OpCloseTableIter
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

type CreateDatabaseReq struct {
	Name string
}

type CreateDatabaseResp struct {
	ErrReadOnly ErrReadOnly
	ErrExisted  ErrExisted
}

type OpenDatabaseReq struct {
	Name string
}

type OpenDatabaseResp struct {
	ID          string
	ErrNotFound ErrDatabaseNotFound
}

type GetDatabasesReq struct {
}

type GetDatabasesResp struct {
	Names []string
}

type DeleteDatabaseReq struct {
	Name string
}

type DeleteDatabaseResp struct {
	ErrReadOnly ErrReadOnly
	ErrNotFound ErrDatabaseNotFound
}

type CreateRelationReq struct {
	DatabaseID string
	Name       string
	Type       RelationType
	Defs       []engine.TableDef
}

type CreateRelationResp struct {
	ErrReadOnly         ErrReadOnly
	ErrDatabaseNotFound ErrDatabaseNotFound
	ErrExisted          ErrExisted
}

type DeleteRelationReq struct {
	DatabaseID string
	Name       string
}

type DeleteRelationResp struct {
	ErrReadOnly ErrReadOnly
	ErrNotFound ErrRelationNotFound
}

type OpenRelationReq struct {
	DatabaseID string
	Name       string
}

type OpenRelationResp struct {
	ID          string
	Type        RelationType
	ErrNotFound ErrRelationNotFound
}

type GetRelationsReq struct {
	DatabaseID string
}

type GetRelationsResp struct {
	Names []string
}

type AddTableDefReq struct {
	TableID string
	Def     engine.TableDef
}

type AddTableDefResp struct {
	ErrReadOnly       ErrReadOnly
	ErrTableNotFound  ErrRelationNotFound
	ErrExisted        ErrExisted
	ErrColumnNotFound ErrColumnNotFound
}

type DelTableDefReq struct {
	TableID string
	Def     engine.TableDef
}

type DelTableDefResp struct {
	ErrReadOnly      ErrReadOnly
	ErrTableNotFound ErrRelationNotFound
	ErrDefNotFound   ErrDefNotFound
}

type DeleteReq struct {
	TableID string
	Vector  *vector.Vector
}

type DeleteResp struct {
	ErrReadOnly      ErrReadOnly
	ErrTableNotFound ErrRelationNotFound
}

type GetPrimaryKeysReq struct {
	TableID string
}

type GetPrimaryKeysResp struct {
	Attrs            []*engine.Attribute
	ErrTableNotFound ErrRelationNotFound
}

type GetTableDefsReq struct {
	TableID string
}

type GetTableDefsResp struct {
	Defs             []engine.TableDef
	ErrTableNotFound ErrRelationNotFound
}

type TruncateReq struct {
	TableID string
}

type TruncateResp struct {
	ErrReadOnly      ErrReadOnly
	AffectedRows     int64
	ErrTableNotFound ErrRelationNotFound
}

type UpdateReq struct {
	TableID string
	Batch   *batch.Batch
}

type UpdateResp struct {
	ErrReadOnly      ErrReadOnly
	ErrTableNotFound ErrRelationNotFound
}

type WriteReq struct {
	TableID string
	Batch   *batch.Batch
}

type WriteResp struct {
	ErrReadOnly      ErrReadOnly
	ErrTableNotFound ErrRelationNotFound
}

type NewTableIterReq struct {
	TableID string
	Expr    *plan.Expr
	Shards  [][]byte
}

type NewTableIterResp struct {
	IterID           string
	ErrTableNotFound ErrRelationNotFound
}

type ReadReq struct {
	IterID   string
	ColNames []string
}

type ReadResp struct {
	Batch             *batch.Batch
	ErrIterNotFound   ErrIterNotFound
	ErrColumnNotFound ErrColumnNotFound
}

type CloseTableIterReq struct {
	IterID string
}

type CloseTableIterResp struct {
	ErrIterNotFound ErrIterNotFound
}
