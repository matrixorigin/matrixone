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
	"bytes"
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

func mustEncodePayload(o any) []byte {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(o); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

type CreateDatabaseReq struct {
	Name string
}

type CreateDatabaseResp struct {
	ErrExisted bool
}

type OpenDatabaseReq struct {
	Name string
}

type OpenDatabaseResp struct {
	ID          string
	ErrNotFound bool
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
	ErrNotFound bool
}

type CreateRelationReq struct {
	DatabaseID string
	Name       string
	Type       RelationType
	Defs       []engine.TableDef
}

func init() {
	// register all TableDef types
	gob.Register(new(engine.CommentDef))
	gob.Register(new(engine.AttributeDef))
	gob.Register(new(engine.IndexTableDef))
	gob.Register(new(engine.PropertiesDef))
	gob.Register(new(engine.PrimaryIndexDef))
}

type CreateRelationResp struct {
	ErrExisted bool
}

type DeleteRelationReq struct {
	DatabaseID string
	Name       string
}

type DeleteRelationResp struct {
	ErrNotFound bool
}

type OpenRelationReq struct {
	DatabaseID string
	Name       string
}

type OpenRelationResp struct {
	ID          string
	Type        RelationType
	ErrNotFound bool
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
	ErrTableNotFound  bool
	ErrExisted        bool
	ErrColumnNotFound string
}

type DelTableDefReq struct {
	TableID string
	Def     engine.TableDef
}

type DelTableDefResp struct {
	ErrTableNotFound bool
	ErrDefNotFound   bool
}

type DeleteReq struct {
	TableID string
	Vector  *vector.Vector
}

type DeleteResp struct {
	ErrTableNotFound bool
}

type GetPrimaryKeysReq struct {
	TableID string
}

type GetPrimaryKeysResp struct {
	Attrs            []*engine.Attribute
	ErrTableNotFound bool
}

type GetTableDefsReq struct {
	TableID string
}

type GetTableDefsResp struct {
	Defs             []engine.TableDef
	ErrTableNotFound bool
}

type TruncateReq struct {
	TableID string
}

type TruncateResp struct {
	AffectedRows     int64
	ErrTableNotFound bool
}

type UpdateReq struct {
	TableID string
	Batch   *batch.Batch
}

type UpdateResp struct {
	ErrTableNotFound bool
}

type WriteReq struct {
	TableID string
	Batch   *batch.Batch
}

type WriteResp struct {
	ErrTableNotFound bool
}

type NewTableIterReq struct {
	TableID string
	Expr    *plan.Expr
	Shards  [][]byte
}

type NewTableIterResp struct {
	IterID           string
	ErrTableNotFound bool
}

type ReadReq struct {
	IterID   string
	ColNames []string
}

type ReadResp struct {
	Batch            *batch.Batch
	ErrTableNotFound bool
}

type CloseTableIterReq struct {
	IterID string
}

type CloseTableIterResp struct {
	ErrIterNotFound bool
}
