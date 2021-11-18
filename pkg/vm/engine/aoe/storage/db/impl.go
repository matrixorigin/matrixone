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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	tiface "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

// There is a premise here, that is, all mutation requests of a database are
// single-threaded
func (d *DB) DoFlushDatabase(meta *metadata.Database) error {
	tables := make([]*metadata.Table, 0, 8)
	fn := func(t *metadata.Table) error {
		if t.IsDeleted() {
			return nil
		}
		tables = append(tables, t)
		return nil
	}
	meta.ForLoopTables(fn)
	var err error
	for _, t := range tables {
		if err = d.DoFlushTable(t); err != nil {
			break
		}
	}
	return err
}

// There is a premise here, that is, all change requests of a database are
// single-threaded
func (d *DB) DoFlushTable(meta *metadata.Table) error {
	collection := d.MemTableMgr.StrongRefCollection(meta.Id)
	if collection == nil {
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

func (d *DB) DoCreateSnapshot(database *metadata.Database, path string, forcesync bool) (uint64, error) {
	var err error
	if forcesync {
		endTime := time.Now().Add(time.Duration(1) * time.Second)
		if err = d.DoFlushDatabase(database); err != nil {
			return 0, err
		}
		interval := time.Duration(1) * time.Millisecond
		waitAllCheckpointed := func() bool {
			for time.Now().Before(endTime) {
				if database.UncheckpointedCnt() == 0 {
					return true
				}
				time.Sleep(interval)
			}
			return false
		}
		if !waitAllCheckpointed() {
			return 0, ErrTimeout
		}
	}
	writer := NewDBSSWriter(database, path, d.Store.DataTables)
	if err = writer.PrepareWrite(); err != nil {
		return 0, err
	}
	defer writer.Close()
	if forcesync {
		if database.GetCheckpointId() != writer.GetIndex() {
			return 0, ErrStaleErr
		}
	}
	if err = writer.CommitWrite(); err != nil {
		return 0, err
	}

	return writer.GetIndex(), nil
}

func (d *DB) TableIdempotenceCheckAndIndexRewrite(meta *metadata.Table, index *LogIndex) (*LogIndex, error) {
	idempotentIdx, ok := meta.ConsumeIdempotentIndex(index)
	if !ok || (idempotentIdx != nil && idempotentIdx.IsApplied()) {
		logutil.Infof("Table %s | %s | %s | Stale Index", meta.Repr(false), index.String(), idempotentIdx.String())
		return index, metadata.IdempotenceErr
	}
	if idempotentIdx == nil {
		return index, nil
	}
	logutil.Infof("Table %s | %s | %s | Rewrite Index", meta.Repr(false), index.String(), idempotentIdx.String())
	index.Start = idempotentIdx.Count + idempotentIdx.Start
	return index, nil
}

func (d *DB) DoAppend(meta *metadata.Table, data *batch.Batch, index *LogIndex) error {
	var err error
	collection := d.MemTableMgr.StrongRefCollection(meta.Id)
	if collection == nil {
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
		collection = e.Collection
	}
	defer collection.Unref()
	return collection.Append(data, index)
}

func (d *DB) GetTableData(meta *metadata.Table) (tiface.ITableData, error) {
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

func (d *DB) ScheduleGCDatabase(database *metadata.Database) {
	gcReq := gcreqs.NewDropDBRequest(d.Opts, database, d.Store.DataTables, d.MemTableMgr)
	d.Opts.GC.Acceptor.Accept(gcReq)
}

func (d *DB) ScheduleGCTable(meta *metadata.Table) {
	gcReq := gcreqs.NewDropTblRequest(d.Opts, meta, d.Store.DataTables, d.MemTableMgr, nil)
	d.Opts.GC.Acceptor.Accept(gcReq)
}

func (d *DB) ForceCompactCatalog() error {
	compactReq := gcreqs.NewCatalogCompactionRequest(d.Store.Catalog, d.Opts.MetaCleanerCfg.Interval)
	return compactReq.Execute()
}
