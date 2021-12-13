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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/flusher"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker/base"
)

type DB struct {
	// Working directory of DB
	Dir string
	// Basic options of DB
	Opts *storage.Options
	// FsMgr manages all file related usages including virtual file.
	FsMgr base.IManager
	// IndexBufMgr manages all segment/block indices in memory.
	IndexBufMgr bmgrif.IBufferManager

	// Those two managers not used currently.
	MTBufMgr  bmgrif.IBufferManager
	SSTBufMgr bmgrif.IBufferManager

	// MutationBufMgr is a replacement for MTBufMgr
	MutationBufMgr bb.INodeManager

	Wal wal.ShardAwareWal

	FlushDriver  flusher.Driver
	TimedFlusher wb.IHeartbeater

	// Internal data storage of DB.
	Store struct {
		Mu         *sync.RWMutex
		Catalog    *metadata.Catalog
		DataTables *table.Tables
	}

	DataDir  *os.File
	DBLocker io.Closer

	// Scheduler schedules all the events happening like flush segment, drop table, etc.
	Scheduler sched.Scheduler

	Closed  *atomic.Value
	ClosedC chan struct{}
}

func (d *DB) GetTempDir() string {
	return common.MakeTempDir(d.Dir)
}

// FIXME: start txn should not accept log index. For create database, the index
// is comfirmed until then end
func (d *DB) StartTxn(index *metadata.LogIndex) *TxnCtx {
	return d.Store.Catalog.StartTxn(index)
}

func (d *DB) CommitTxn(txn *TxnCtx) error {
	return txn.Commit()
}

func (d *DB) AbortTxn(txn *TxnCtx) error {
	return txn.Abort()
}

func (d *DB) CreateDatabaseInTxn(txn *TxnCtx, name string) (*metadata.Database, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Store.Catalog.CreateDatabaseInTxn(txn, name)
}

func (d *DB) CreateTableInTxn(txn *TxnCtx, dbName string, schema *TableSchema, indice *IndexSchema) (*metadata.Table, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.GetDatabaseByNameInTxn(txn, dbName)
	if err != nil {
		return nil, err
	}
	return database.CreateTableInTxn(txn, schema, indice)
}

func (d *DB) FlushDatabase(dbName string) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return err
	}
	return d.DoFlushDatabase(database)
}

func (d *DB) FlushTable(dbName, tableName string) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta, err := d.Store.Catalog.SimpleGetTableByName(dbName, tableName)
	if err != nil {
		return err
	}
	return d.DoFlushTable(meta)
}

func (d *DB) GetSegmentIds(dbName string, tableName string) (ids dbi.IDS) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	meta := database.SimpleGetTableByName(tableName)
	if meta == nil {
		return
	}
	data, err := d.GetTableData(meta)
	if err != nil {
		return
	}
	ids.Ids = data.SegmentIds()
	data.Unref()
	return
}

func (d *DB) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}

	tableMeta, err := d.Store.Catalog.SimpleGetTableByName(ctx.DBName, ctx.TableName)
	if err != nil {
		return nil, err
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

func (d *DB) DatabaseNames() []string {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Store.Catalog.SimpleGetDatabaseNames()
}

func (d *DB) TableIDs(dbName string) (ids []uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	ids = database.SimpleGetTableIds()
	return
}

func (d *DB) TableNames(dbName string) (names []string) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	names = database.SimpleGetTableNames()
	return
}

func (d *DB) GetShardCheckpointId(shardId uint64) uint64 {
	return d.Wal.GetShardCheckpointId(shardId)
}

func (d *DB) GetDBCheckpointId(dbName string) uint64 {
	database, err := d.Store.Catalog.GetDatabaseByName(dbName)
	if err != nil {
		return 0
	}
	return database.GetCheckpointId()
}

// There is a premise here, that is, all mutation requests of a database are
// single-threaded
func (d *DB) CreateSnapshot(dbName string, path string, forcesync bool) (uint64, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return 0, err
	}
	var index uint64
	now := time.Now()
	maxTillTime := now.Add(time.Duration(4) * time.Second)
	for time.Now().Before(maxTillTime) {
		index, err = d.DoCreateSnapshot(database, path, forcesync)
		if err != ErrStaleErr && err != ErrTimeout {
			break
		}
	}
	logutil.Infof("CreateSnapshot %s takes %s", database.Repr(), time.Since(now))
	return index, err
}

// ApplySnapshot applies a snapshot of the shard stored in `path` to engine atomically.
func (d *DB) ApplySnapshot(dbName string, path string) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	var err error
	database, _ := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	loader := NewDBSSLoader(d.Store.Catalog, database, d.Store.DataTables, path)
	if err = loader.PrepareLoad(); err != nil {
		return err
	}
	if err = loader.CommitLoad(); err != nil {
		return err
	}
	loader.ScheduleEvents(d)
	if database != nil {
		d.ScheduleGCDatabase(database)
	}
	return err
}

func (d *DB) SpliteDatabaseCheck(dbName string, size uint64) (coarseSize uint64, coarseCount uint64, keys [][]byte, ctx []byte, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	var database *metadata.Database
	database, err = d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	index := database.GetCheckpointId()
	return database.SplitCheck(size, index)
}

func (d *DB) startWorkers() {
	d.Opts.GC.Acceptor.Start()
	d.FlushDriver.Start()
	d.TimedFlusher.Start()
}

func (d *DB) IsClosed() bool {
	if err := d.Closed.Load(); err != nil {
		return true
	}
	return false
}

func (d *DB) stopWorkers() {
	d.TimedFlusher.Stop()
	d.FlushDriver.Stop()
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
