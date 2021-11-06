package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
)

type Impl = db.DB

type DB struct {
	Impl
}

func (d *DB) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (id uint64, err error) {
	info.Name = ctx.TableName
	schema := adaptor.TableInfoToSchema(d.Opts.Meta.Catalog, info)
	logutil.Debugf("Create table, schema.Primarykey is %d", schema.PrimaryKey)
	index := adaptor.GetLogIndexFromTableOpCtx(&ctx)

	dbName := ShardIdToName(ctx.ShardId)
	return d.Impl.CreateTable(dbName, schema, index)
}

func (d *DB) GetSegmentIds(ctx dbi.GetSegmentsCtx) (ids dbi.IDS) {
	dbName := ShardIdToName(ctx.ShardId)
	return d.Impl.GetSegmentIds(dbName, ctx.TableName)
}

func (d *DB) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	ctx.DBName = ShardIdToName(ctx.ShardId)
	return d.Impl.GetSnapshot(ctx)
}

func (d *DB) TableIDs(shardId uint64) (ids []uint64, err error) {
	return d.Impl.TableIDs(ShardIdToName(shardId))
}

func (d *DB) TableNames(shardId uint64) []string {
	return d.Impl.TableNames(ShardIdToName(shardId))
}

func (d *DB) DropTable(ctx dbi.DropTableCtx) (id uint64, err error) {
	ctx.DBName = ShardIdToName(ctx.ShardId)
	return d.DropTable(ctx)
}

func (d *DB) Append(ctx dbi.AppendCtx) (err error) {
	ctx.DBName = ShardIdToName(ctx.ShardId)
	return d.Append(ctx)
}

func (d *DB) CreateSnapshot(shardId uint64, path string) (uint64, error) {
	return d.Impl.CreateSnapshot(ShardIdToName(shardId), path)
}

func (d *DB) ApplySnapshot(shardId uint64, path string) error {
	return d.Impl.ApplySnapshot(ShardIdToName(shardId), path)
}
