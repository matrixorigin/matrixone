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

package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type DBMutationCtx struct {
	Id     uint64
	Offset int
	Size   int
	DB     string
}

type CreateDBCtx = DBMutationCtx
type DropDBCtx = DBMutationCtx

type PrepareSplitCtx struct {
	DB   string
	Size uint64
}

type ExecSplitCtx struct {
	DBMutationCtx
	NewNames    []string
	RenameTable db.RenameTableFactory
	SplitKeys   [][]byte
	SplitCtx    []byte
}

type CreateSnapshotCtx struct {
	DB   string
	Path string
	Sync bool
}

type ApplySnapshotCtx struct {
	DB   string
	Path string
}

type TableMutationCtx struct {
	DBMutationCtx
	Table string
}

type DropTableCtx = TableMutationCtx

type CreateTableCtx struct {
	DBMutationCtx
	Schema *db.TableSchema
	Indice *db.IndexSchema
}

type CreateIndexCtx struct {
	DBMutationCtx
	Table string
	Indices *db.IndexSchema
}

type DropIndexCtx struct {
	DBMutationCtx
	Table string
	IndexNames []string
}

type AppendCtx struct {
	TableMutationCtx
	Data *batch.Batch
}

func (ctx *DBMutationCtx) ToLogIndex(database *metadata.Database) *db.LogIndex {
	return &db.LogIndex{
		ShardId: database.GetShardId(),
		Id: db.IndexId{
			Id:     ctx.Id,
			Offset: uint32(ctx.Offset),
			Size:   uint32(ctx.Size),
		},
	}
}

func (ctx *AppendCtx) ToLogIndex(database *metadata.Database) *db.LogIndex {
	index := ctx.DBMutationCtx.ToLogIndex(database)
	index.Capacity = uint64(ctx.Data.Vecs[0].Length())
	return index
}
