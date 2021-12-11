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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"path/filepath"
)

type Impl = db.DB

type DB struct {
	Impl
}

func (d *DB) Relation(dbName, tableName string) (*Relation, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta, err := d.Store.Catalog.SimpleGetTableByName(dbName, tableName)
	if err != nil {
		return nil, err
	}
	data, err := d.GetTableData(meta)
	if err != nil {
		return nil, err
	}
	return NewRelation(d, data, meta), nil
}

// FIXME: Log index first. Since the shard is should be defined first, we
// have to prepare create first to get a shard id and then use the shard id
// to log wal
func (d *DB) CreateDatabase(ctx *CreateDBCtx) (*metadata.Database, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleCreateDatabase(ctx.DB, nil)
	if err != nil {
		return nil, err
	}

	index := ctx.ToLogIndex(database)
	if index.Id.Size == 0 {
		index.Id.Size = 1
	}
	d.Wal.SyncLog(index)
	d.Wal.Checkpoint(index)

	return database, err
}

func (d *DB) DropDatabase(ctx *DropDBCtx) (*metadata.Database, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.GetDatabaseByName(ctx.DB)
	if err != nil {
		return nil, err
	}
	if database.IsDeleted() {
		return nil, db.ErrResourceDeleted
	}

	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return database, err
	}
	defer d.Wal.Checkpoint(index)
	if err = database.SimpleSoftDelete(index); err != nil {
		return database, err
	}
	d.ScheduleGCDatabase(database)
	return database, nil
}

func (d *DB) CreateTable(ctx *CreateTableCtx) (*metadata.Table, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return nil, err
	}
	index := ctx.ToLogIndex(database)

	if err = d.Wal.SyncLog(index); err != nil {
		return nil, err
	}
	defer d.Wal.Checkpoint(index)

	if database.InReplaying(index) {
		if _, ok := database.ConsumeIdempotentIndex(index); !ok {
			err = db.ErrIdempotence
			return nil, err
		}
	}

	return database.SimpleCreateTable(ctx.Schema, ctx.Indice, index)
}

func (d *DB) DropTable(ctx *DropTableCtx) (*metadata.Table, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return nil, err
	}
	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return nil, err
	}
	defer d.Wal.Checkpoint(index)

	if database.InReplaying(index) {
		if idx, ok := database.ConsumeIdempotentIndex(index); !ok {
			err = db.ErrIdempotence
			return nil, err
		} else if idx != nil {
			if idx.IsApplied() {
				err = db.ErrIdempotence
				return nil, err
			}
		}
	}

	meta := database.SimpleGetTableByName(ctx.Table)
	if meta == nil {
		err = metadata.TableNotFoundErr
		return nil, err
	}

	if err = meta.SimpleSoftDelete(index); err != nil {
		return nil, err
	}
	d.ScheduleGCTable(meta)
	return meta, err
}

func (d *DB) CreateIndex(ctx *CreateIndexCtx) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return err
	}
	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return err
	}
	defer d.Wal.Checkpoint(index)

	if database.InReplaying(index) {
		if idx, ok := database.ConsumeIdempotentIndex(index); !ok {
			err = db.ErrIdempotence
			return err
		} else if idx != nil {
			if idx.IsApplied() {
				err = db.ErrIdempotence
				return err
			}
		}
	}
	meta := database.SimpleGetTableByName(ctx.Table)
	if meta == nil {
		err = metadata.TableNotFoundErr
		return err
	}

	if err = meta.SimpleAddIndice(ctx.Indices.Indice, index); err != nil {
		return err
	}

	tblData, err := d.GetTableData(meta)
	if err != nil {
		return err
	}
	for _, info := range ctx.Indices.Indice {
		cols := info.Columns
		// currently, only num/str bsi is supported for one column, if
		// more kinds of indices are loaded for one column at the same
		// time, need some refactor.
		for _, segId := range tblData.SegmentIds() {
			seg := tblData.StrongRefSegment(segId)
			segMeta := seg.GetMeta()
			segMeta.RLock()
			if !segMeta.IsSortedLocked() {
				for _, blkId := range seg.BlockIds() {
					blk := seg.WeakRefBlock(blkId)
					// TODO(zzl): thread safe?
					if blk != nil && blk.GetType() == base.PERSISTENT_BLK {
						e := sched.NewFlushBlockIndexEvent(&sched.Context{}, blk)
						e.Cols = cols
						d.Scheduler.Schedule(e)
					}
				}
				seg.Unref()
				segMeta.RUnlock()
				continue
			}
			segMeta.RUnlock()
			e := sched.NewFlushSegIndexEvent(&sched.Context{}, seg)
			e.Cols = cols
			d.Scheduler.Schedule(e)
			seg.Unref()
		}
	}
	return nil
}

func (d *DB) DropIndex(ctx *DropIndexCtx) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return err
	}
	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return err
	}
	defer d.Wal.Checkpoint(index)

	if database.InReplaying(index) {
		if idx, ok := database.ConsumeIdempotentIndex(index); !ok {
			err = db.ErrIdempotence
			return err
		} else if idx != nil {
			if idx.IsApplied() {
				err = db.ErrIdempotence
				return err
			}
		}
	}
	meta := database.SimpleGetTableByName(ctx.Table)
	if meta == nil {
		err = metadata.TableNotFoundErr
		return err
	}

	names := ctx.IndexNames
	tblId := meta.Id
	tblData, err := d.GetTableData(meta)
	if err != nil {
		return err
	}
	for _, segId := range tblData.SegmentIds() {
		seg := tblData.StrongRefSegment(segId)
		holder := seg.GetIndexHolder()
		for _, info := range meta.GetIndexSchema().Indice {
			if !util.ContainsString(names, info.Name) {
				continue
			}
			cols := info.Columns
			for _, col := range cols {
				if holder.HolderType() == base.SORTED_SEG {
					currVersion := holder.FetchCurrentVersion(col, 0)
					bn := common.MakeBitSlicedIndexFileName(currVersion, tblId, segId, col)
					fullname := filepath.Join(filepath.Join(d.Dir, "data"), bn)
					holder.DropIndex(fullname)
				} else if holder.HolderType() == base.UNSORTED_SEG {
					for _, blkId := range seg.BlockIds() {
						// TODO(zzl): thread safe?
						if seg.WeakRefBlock(blkId).GetType() != base.PERSISTENT_BLK {
							continue
						}
						currVersion := holder.FetchCurrentVersion(col, blkId)
						bn := common.MakeBlockBitSlicedIndexFileName(currVersion, tblId, segId, blkId, col)
						fullname := filepath.Join(filepath.Join(d.Dir, "data"), bn)
						holder.DropIndex(fullname)
					}
				} else {
					panic("unexpected error")
				}
			}
		}
		seg.Unref()
	}

	if err = meta.SimpleDropIndice(names, index); err != nil {
		return err
	}

	return nil
}

func (d *DB) Append(ctx *AppendCtx) (err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return err
	}
	index := ctx.ToLogIndex(database)
	var meta *metadata.Table
	if database.InReplaying(index) {
		meta, err = database.GetTableByNameAndLogIndex(ctx.Table, index)
		if err != nil {
			return
		}
		index, err = d.TableIdempotenceCheckAndIndexRewrite(meta, index)
		if err == metadata.IdempotenceErr {
			return
		}
		if meta.IsDeleted() {
			return metadata.TableNotFoundErr
		}
	} else {
		meta = database.SimpleGetTableByName(ctx.Table)
		if meta == nil {
			return metadata.TableNotFoundErr
		}
	}
	if err = d.Wal.SyncLog(index); err != nil {
		return
	}
	defer func() {
		if err != nil {
			index.Count = index.Capacity - index.Start
			d.Wal.Checkpoint(index)
		}
	}()
	err = d.DoAppend(meta, ctx.Data, index.AsSlice())
	return err
}

func (d *DB) CreateSnapshot(ctx *CreateSnapshotCtx) (uint64, error) {
	return d.Impl.CreateSnapshot(ctx.DB, ctx.Path, ctx.Sync)
}

func (d *DB) ApplySnapshot(ctx *ApplySnapshotCtx) error {
	return d.Impl.ApplySnapshot(ctx.DB, ctx.Path)
}

func (d *DB) PrepareSplitDatabase(ctx *PrepareSplitCtx) (uint64, uint64, [][]byte, []byte, error) {
	return d.Impl.SpliteDatabaseCheck(ctx.DB, ctx.Size)
}

func (d *DB) ExecSplitDatabase(ctx *ExecSplitCtx) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	// TODO: validate parameters
	var (
		err      error
		database *metadata.Database
	)
	database, err = d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return err
	}

	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return err
	}
	defer d.Wal.Checkpoint(index)

	splitter := db.NewSplitter(database, ctx.NewNames, ctx.RenameTable, ctx.SplitKeys, ctx.SplitCtx, index, &d.Impl)
	defer splitter.Close()
	if err = splitter.Prepare(); err != nil {
		return err
	}
	if err = splitter.Commit(); err != nil {
		return err
	}
	splitter.ScheduleEvents(&d.Impl)
	d.ScheduleGCDatabase(database)
	return err
}
