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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

// There is a premise here, that is, all mutation requests of a database are
// single-threaded
func (d *DB) doFlushDatabase(meta *metadata.Database) error {
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
		if err = d.doFlushTable(t); err != nil {
			break
		}
	}
	return err
}

// There is a premise here, that is, all change requests of a database are
// single-threaded
func (d *DB) doFlushTable(meta *metadata.Table) error {
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

func (d *DB) doCreateSnapshot(database *metadata.Database, path string, forcesync bool) (uint64, error) {
	var err error
	if forcesync {
		endTime := time.Now().Add(time.Duration(1) * time.Second)
		if err = d.doFlushDatabase(database); err != nil {
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

func (d *DB) tableIdempotenceCheckAndIndexRewrite(meta *metadata.Table, index *LogIndex) (*LogIndex, error) {
	idempotentIdx, ok := meta.ConsumeIdempotentIndex(index)
	if !ok {
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
