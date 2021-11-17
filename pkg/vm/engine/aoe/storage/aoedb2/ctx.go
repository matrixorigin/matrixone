package aoedb2

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
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
