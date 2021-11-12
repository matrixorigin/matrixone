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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type Relation = db.Relation
type Impl = db.DB

type DB struct {
	Impl
}

func (d *DB) Relation(shardId uint64, tableName string) (*Relation, error) {
	dbName := ShardIdToName(shardId)
	return d.Impl.Relation(dbName, tableName)
}

func (d *DB) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (id uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	info.Name = ctx.TableName
	schema := adaptor.TableInfoToSchema(d.Opts.Meta.Catalog, info)
	logutil.Debugf("Create table, schema.Primarykey is %d", schema.PrimaryKey)
	index := adaptor.GetLogIndexFromTableOpCtx(&ctx)
	if err = d.Wal.SyncLog(index); err != nil {
		return
	}
	defer d.Wal.Checkpoint(index)
	dbName := ShardIdToName(ctx.ShardId)

	txn := d.StartTxn(index)
	database, err := d.Store.Catalog.CreateDatabaseInTxn(txn, dbName)
	if err != nil {
		d.AbortTxn(txn)
		return
	}
	table, err := database.CreateTableInTxn(txn, schema)
	if err != nil {
		d.AbortTxn(txn)
		return
	}
	if err = d.CommitTxn(txn); err != nil {
		d.AbortTxn(txn)
		return
	}
	id = table.Id
	return
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
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	index := adaptor.GetLogIndexFromDropTableCtx(&ctx)
	logutil.Infof("DropTable %s", index.String())
	if err = d.Wal.SyncLog(index); err != nil {
		return
	}
	defer d.Wal.Checkpoint(index)
	ctx.DBName = ShardIdToName(ctx.ShardId)
	txn := d.StartTxn(index)
	database, err := d.Store.Catalog.GetDatabaseByNameInTxn(txn, ctx.DBName)
	if err != nil {
		d.AbortTxn(txn)
		return
	}
	if err = database.SoftDeleteInTxn(txn); err != nil {
		d.AbortTxn(txn)
		return
	}
	var table *metadata.Table
	if table, err = database.DropTableByNameInTxn(txn, ctx.TableName); err != nil {
		d.AbortTxn(txn)
		return
	} else {
		id = table.Id
	}
	if err = d.CommitTxn(txn); err != nil {
		d.AbortTxn(txn)
		return
	}
	gcTable := gcreqs.NewDropTblRequest(d.Opts, table, d.Store.DataTables, d.MemTableMgr, ctx.OnFinishCB)
	d.Opts.GC.Acceptor.Accept(gcTable)
	gcDB := gcreqs.NewDropDBRequest(d.Opts, database, d.Store.DataTables, d.MemTableMgr)
	d.Opts.GC.Acceptor.Accept(gcDB)
	return
}

func (d *DB) Append(ctx dbi.AppendCtx) (err error) {
	ctx.DBName = ShardIdToName(ctx.ShardId)
	return d.Impl.Append(ctx)
}

func (d *DB) CreateSnapshot(shardId uint64, path string) (uint64, error) {
	return d.Impl.CreateSnapshot(ShardIdToName(shardId), path)
}

func (d *DB) ApplySnapshot(shardId uint64, path string) error {
	return d.Impl.ApplySnapshot(ShardIdToName(shardId), path)
}
