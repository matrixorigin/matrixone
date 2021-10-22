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

package db

import (
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/adaptor"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	tiface "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrClosed      = errors.New("aoe: closed")
	ErrUnsupported = errors.New("aoe: unsupported")
	ErrNotFound    = errors.New("aoe: notfound")
)

type DB struct {
	// Working directory of DB
	Dir string
	// Basic options of DB
	Opts *storage.Options
	// FsMgr manages all file related usages including virtual file.
	FsMgr base.IManager
	// MemTableMgr manages memtables.
	MemTableMgr mtif.IManager
	// IndexBufMgr manages all segment/block indices in memory.
	IndexBufMgr bmgrif.IBufferManager

	// Those two managers not used currently.
	MTBufMgr  bmgrif.IBufferManager
	SSTBufMgr bmgrif.IBufferManager

	// MutationBufMgr is a replacement for MTBufMgr
	MutationBufMgr bb.INodeManager

	Wal wal.Wal

	// Internal data storage of DB.
	Store struct {
		Mu         *sync.RWMutex
		Catalog    *metadata.Catalog
		DataTables *table.Tables
	}

	Cleaner struct {
		MetaFiles iw.IHeartbeater
	}

	DataDir  *os.File
	DBLocker io.Closer

	// Scheduler schedules all the events happening like flush segment, drop table, etc.
	Scheduler sched.Scheduler

	Closed  *atomic.Value
	ClosedC chan struct{}
}

func (d *DB) Flush(name string) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tbl := d.Store.Catalog.SimpleGetTableByName(name)
	if tbl == nil {
		return metadata.TableNotFoundErr
	}
	collection := d.MemTableMgr.StrongRefCollection(tbl.Id)
	if collection == nil {
		eCtx := &memdata.Context{
			Opts:        d.Opts,
			MTMgr:       d.MemTableMgr,
			TableMeta:   tbl,
			IndexBufMgr: d.IndexBufMgr,
			MTBufMgr:    d.MTBufMgr,
			SSTBufMgr:   d.SSTBufMgr,
			FsMgr:       d.FsMgr,
			Tables:      d.Store.DataTables,
			Waitable:    true,
		}
		e := memdata.NewCreateTableEvent(eCtx)
		if err := d.Scheduler.Schedule(e); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		if err := e.WaitDone(); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection = e.Collection
	}
	defer collection.Unref()
	return collection.Flush()
}

func (d *DB) Append(ctx dbi.AppendCtx) (err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	if ctx.OpOffset >= ctx.OpSize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", ctx.OpIndex, ctx.OpOffset, ctx.OpSize))
	}
	tbl := d.Store.Catalog.SimpleGetTableByName(ctx.TableName)
	if tbl == nil {
		return metadata.TableNotFoundErr
	}

	collection := d.MemTableMgr.StrongRefCollection(tbl.Id)
	if collection == nil {
		eCtx := &memdata.Context{
			Opts:        d.Opts,
			MTMgr:       d.MemTableMgr,
			TableMeta:   tbl,
			IndexBufMgr: d.IndexBufMgr,
			MTBufMgr:    d.MTBufMgr,
			SSTBufMgr:   d.SSTBufMgr,
			FsMgr:       d.FsMgr,
			Tables:      d.Store.DataTables,
			Waitable:    true,
		}
		e := memdata.NewCreateTableEvent(eCtx)
		if err = d.Scheduler.Schedule(e); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		if err = e.WaitDone(); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection = e.Collection
	}

	index := adaptor.GetLogIndexFromAppendCtx(&ctx)
	defer collection.Unref()
	if entry, err := d.Wal.Log(index); err != nil {
		return err
	} else {
		entry.WaitDone()
		entry.Free()
	}
	return collection.Append(ctx.Data, index)
}

func (d *DB) getTableData(meta *metadata.Table) (tiface.ITableData, error) {
	data, err := d.Store.DataTables.StrongRefTable(meta.Id)
	if err != nil {
		eCtx := &memdata.Context{
			Opts:        d.Opts,
			MTMgr:       d.MemTableMgr,
			TableMeta:   meta,
			IndexBufMgr: d.IndexBufMgr,
			MTBufMgr:    d.MTBufMgr,
			SSTBufMgr:   d.SSTBufMgr,
			FsMgr:       d.FsMgr,
			Tables:      d.Store.DataTables,
			Waitable:    true,
		}
		e := memdata.NewCreateTableEvent(eCtx)
		if err = d.Scheduler.Schedule(e); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		if err = e.WaitDone(); err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection := e.Collection
		if data, err = d.Store.DataTables.StrongRefTable(meta.Id); err != nil {
			collection.Unref()
			return nil, err
		}
		collection.Unref()
	}
	return data, nil
}

func (d *DB) Relation(name string) (*Relation, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta := d.Store.Catalog.SimpleGetTableByName(name)
	if meta == nil {
		return nil, metadata.TableNotFoundErr
	}
	data, err := d.getTableData(meta)
	if err != nil {
		return nil, err
	}
	return NewRelation(d, data, meta), nil
}

func (d *DB) HasTable(name string) bool {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta := d.Store.Catalog.SimpleGetTableByName(name)
	return meta != nil
}

func (d *DB) DropTable(ctx dbi.DropTableCtx) (id uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	eCtx := &dbsched.Context{
		Opts:     d.Opts,
		Waitable: true,
	}
	e := meta.NewDropTableEvent(eCtx, ctx, d.MemTableMgr, d.Store.DataTables)
	if err = d.Scheduler.Schedule(e); err != nil {
		return id, err
	}
	err = e.WaitDone()
	return e.Id, err
}

func (d *DB) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (id uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	info.Name = ctx.TableName
	schema := adaptor.TableInfoToSchema(d.Opts.Meta.Catalog, info)
	index := adaptor.GetLogIndexFromTableOpCtx(&ctx)
	entry, err := d.Wal.Log(index)
	if err != nil {
		return
	}
	defer entry.Free()
	entry.WaitDone()
	defer d.Wal.Checkpoint(index)

	logutil.Infof("CreateTable %s", index.String())
	tbl, err := d.Opts.Meta.Catalog.SimpleCreateTable(schema, index)
	if err != nil {
		return id, err
	}
	return tbl.Id, nil
}

func (d *DB) GetSegmentIds(ctx dbi.GetSegmentsCtx) (ids dbi.IDS) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta := d.Store.Catalog.SimpleGetTableByName(ctx.TableName)
	if meta == nil {
		return ids
	}
	data, err := d.getTableData(meta)
	if err != nil {
		return ids
	}
	ids.Ids = data.SegmentIds()
	data.Unref()
	return ids
}

func (d *DB) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tableMeta := d.Store.Catalog.SimpleGetTableByName(ctx.TableName)
	if tableMeta == nil {
		return nil, metadata.TableNotFoundErr
	}
	if tableMeta.SimpleGetSegmentCount() == 0 {
		return handle.NewEmptySnapshot(), nil
	}
	tableData, err := d.Store.DataTables.StrongRefTable(tableMeta.Id)
	if err != nil {
		return nil, err
	}
	var ss *handle.Snapshot
	if ctx.ScanAll {
		ss = handle.NewLinkAllSnapshot(ctx.Cols, tableData)
	} else {
		ss = handle.NewSnapshot(ctx.SegmentIds, ctx.Cols, tableData)
	}
	return ss, nil
}

func (d *DB) TableIDs() (ids []uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	ids = d.Store.Catalog.SimpleGetTableIds()
	return ids, err
}

func (d *DB) TableNames() []string {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Store.Catalog.SimpleGetTableNames()
}

func (d *DB) TableSegmentIDs(tableID uint64) (ids []common.ID, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tbl := d.Store.Catalog.SimpleGetTable(tableID)
	if tbl == nil {
		return ids, metadata.TableNotFoundErr
	}
	sids := tbl.SimpleGetSegmentIds()
	// TODO: Refactor metainfo to 1. keep order 2. use common.RelationName
	for _, sid := range sids {
		ids = append(ids, common.ID{TableID: tableID, SegmentID: sid})
	}
	return ids, err
}

func (d *DB) GetSegmentedId(ctx dbi.GetSegmentedIdCtx) (id uint64, err error) {
	id = ^uint64(0)
	for _, matcher := range ctx.Matchers {
		switch matcher.Type {
		case dbi.MTPrefix:
			tbls := d.Store.Catalog.SimpleGetTablesByPrefix(matcher.Pattern)
			for _, tbl := range tbls {
				data, err := d.getTableData(tbl)
				defer data.Unref()
				if err != nil {
					return id, err
				}
				tmpId, ok := data.GetSegmentedIndex()
				if !ok {
					return 0, nil
				}
				if tmpId < id {
					id = tmpId
				}
			}
		default:
			panic("not supported")
		}
	}
	if id == ^uint64(0) {
		return id, ErrNotFound
	}
	return id, err
}

func (d *DB) startWorkers() {
	d.Opts.GC.Acceptor.Start()
}

func (d *DB) EnsureNotClosed() {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
}

func (d *DB) IsClosed() bool {
	if err := d.Closed.Load(); err != nil {
		return true
	}
	return false
}

func (d *DB) stopWorkers() {
	d.Opts.GC.Acceptor.Stop()
}

func (d *DB) Close() error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}

	d.Closed.Store(ErrClosed)
	close(d.ClosedC)
	d.Wal.Close()
	d.Scheduler.Stop()
	d.stopWorkers()
	d.Opts.Meta.Catalog.Close()
	err := d.DBLocker.Close()
	return err
}
