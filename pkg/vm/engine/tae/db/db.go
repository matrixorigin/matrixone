package db

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/worker/base"
)

var (
	ErrClosed = errors.New("tae: closed")
)

type DB struct {
	Dir  string
	Opts *options.Options

	Catalog *catalog.Catalog

	IndexBufMgr base.INodeManager
	MTBufMgr    base.INodeManager
	TxnBufMgr   base.INodeManager

	TxnMgr *txnbase.TxnManager
	Wal    wal.Driver

	CKPDriver checkpoint.Driver

	Scheduler tasks.TaskScheduler

	TimedScanner wb.IHeartbeater

	DBLocker io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}
}

func (db *DB) StartTxn(info []byte) txnif.AsyncTxn {
	return db.TxnMgr.StartTxn(info)
}

func (db *DB) CommitTxn(txn txnif.AsyncTxn) (err error) {
	return txn.Commit()
}

func (db *DB) RollbackTxn(txn txnif.AsyncTxn) error {
	return txn.Rollback()
}

func (db *DB) startWorkers() (err error) {
	db.CKPDriver.Start()
	db.TimedScanner.Start()
	return
}

func (db *DB) stopWorkers() (err error) {
	db.TimedScanner.Stop()
	db.CKPDriver.Stop()
	return
}

func (db *DB) Close() error {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	db.stopWorkers()
	db.Closed.Store(ErrClosed)
	close(db.ClosedC)
	db.Scheduler.Stop()
	db.TxnMgr.Stop()
	db.Wal.Close()
	db.Opts.Catalog.Close()
	return db.DBLocker.Close()
}
