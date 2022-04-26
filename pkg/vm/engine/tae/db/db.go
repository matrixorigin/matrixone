package db

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/worker/base"
)

var (
	ErrClosed = errors.New("tae: closed")
)

type DB struct {
	Dir  string
	Opts *options.Options

	IndexBufMgr base.INodeManager
	MTBufMgr    base.INodeManager
	TxnBufMgr   base.INodeManager

	TxnMgr       *txnbase.TxnManager
	TxnLogDriver txnbase.NodeDriver

	CKPDriver checkpoint.Driver

	TaskScheduler tasks.TaskScheduler
	IOScheduler   tasks.Scheduler

	CalibrationTimer wb.IHeartbeater

	DBLocker io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}
}

func (db *DB) StartTxn(info []byte) (txn txnif.AsyncTxn, err error) {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	txn = db.TxnMgr.StartTxn(info)
	return
}

func (db *DB) CommitTxn(txn txnif.AsyncTxn) (err error) {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	err = txn.Commit()
	return
}

func (db *DB) RollbackTxn(txn txnif.AsyncTxn) (err error) {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	err = txn.Rollback()
	return
}

func (db *DB) startWorkers() (err error) {
	db.CKPDriver.Start()
	db.CalibrationTimer.Start()
	return
}

func (db *DB) stopWorkers() (err error) {
	db.CalibrationTimer.Stop()
	db.CKPDriver.Stop()
	return
}

func (db *DB) Close() error {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	db.Closed.Store(ErrClosed)
	close(db.ClosedC)
	db.TaskScheduler.Stop()
	db.IOScheduler.Stop()
	db.TxnMgr.Stop()
	db.TxnLogDriver.Close()
	db.Opts.Catalog.Close()
	return db.DBLocker.Close()
}
