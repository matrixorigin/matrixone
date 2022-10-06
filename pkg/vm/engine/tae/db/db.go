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
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrClosed = moerr.NewInternalError("tae: closed")
)

type DB struct {
	Dir  string
	Opts *options.Options

	Catalog *catalog.Catalog

	IndexBufMgr base.INodeManager
	MTBufMgr    base.INodeManager
	TxnBufMgr   base.INodeManager

	TxnMgr     *txnbase.TxnManager
	LogtailMgr *logtail.LogtailMgr
	Wal        wal.Driver

	CKPDriver checkpoint.Driver

	Scheduler tasks.TaskScheduler

	TimedScanner wb.IHeartbeater

	FileFactory file.SegmentFactory

	DBLocker io.Closer

	Closed *atomic.Value
}

func (db *DB) StartTxn(info []byte) (txnif.AsyncTxn, error) {
	return db.TxnMgr.StartTxn(info)
}

func (db *DB) CommitTxn(txn txnif.AsyncTxn) (err error) {
	return txn.Commit()
}

func (db *DB) GetTxnByCtx(txnOperator client.TxnOperator) (txn txnif.AsyncTxn, err error) {
	txnID := txnOperator.Txn().ID
	txn = db.TxnMgr.GetTxnByCtx(txnID)
	if txn == nil {
		err = moerr.NewNotFound()
	}
	return
}

func (db *DB) GetOrCreateTxnWithMeta(
	info []byte,
	id []byte,
	ts types.TS) (txn txnif.AsyncTxn, err error) {
	return db.TxnMgr.GetOrCreateTxnWithMeta(info, id, ts)
}

func (db *DB) GetTxn(id string) (txn txnif.AsyncTxn, err error) {
	txn = db.TxnMgr.GetTxn(id)
	if txn == nil {
		err = moerr.NewNotFound()
	}
	return
}

func (db *DB) RollbackTxn(txn txnif.AsyncTxn) error {
	return txn.Rollback()
}

func (db *DB) Replay(dataFactory *tables.DataFactory) {
	maxTs := db.Catalog.GetCheckpointed().MaxTS
	replayer := newReplayer(dataFactory, db)
	replayer.OnTimeStamp(maxTs)
	replayer.Replay()

	err := db.TxnMgr.Init(replayer.GetMaxTS())
	if err != nil {
		panic(err)
	}
}

func (db *DB) PrintStats() {
	pc, _, _, _ := runtime.Caller(1)
	caller := runtime.FuncForPC(pc).Name()
	stats := db.CollectStats()
	logutil.Infof("[PrintStats][Caller=%s]:%s", caller, stats.ToString(""))
}

func (db *DB) CollectStats() *Stats {
	stats := NewStats(db)
	stats.Collect()
	return stats
}

func (db *DB) Close() error {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	defer db.PrintStats()
	db.Closed.Store(ErrClosed)
	db.TimedScanner.Stop()
	db.CKPDriver.Stop()
	db.Scheduler.Stop()
	db.TxnMgr.Stop()
	db.Wal.Close()
	db.Opts.Catalog.Close()
	return db.DBLocker.Close()
}
