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
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrClosed = moerr.NewInternalErrorNoCtx("tae: closed")
)

type DBTxnMode uint32

const (
	DBTxnMode_Write DBTxnMode = iota
	DBTxnMode_Replay
)

func (m DBTxnMode) String() string {
	switch m {
	case DBTxnMode_Write:
		return "TxnWriteMode"
	case DBTxnMode_Replay:
		return "TxnReplayMode"
	default:
		return fmt.Sprintf("UnknownTxnMode(%d)", m)
	}
}

func (m DBTxnMode) IsValid() bool {
	return m == DBTxnMode_Write || m == DBTxnMode_Replay
}

type DBOption func(*DB)

func WithTxnMode(mode DBTxnMode) DBOption {
	return func(db *DB) {
		db.TxnMode.Store(uint32(mode))
	}
}

type DB struct {
	Dir        string
	TxnMode    atomic.Uint32
	Controller *Controller

	Opts *options.Options

	usageMemo *logtail.TNUsageMemo
	Catalog   *catalog.Catalog

	TxnMgr *txnbase.TxnManager

	LogtailMgr *logtail.Manager
	Wal        wal.Driver

	CronJobs *tasks.CancelableJobs

	BGScanner          wb.IHeartbeater
	BGCheckpointRunner checkpoint.Runner

	MergeScheduler *merge.Scheduler

	DiskCleaner *gc2.DiskCleaner

	Runtime *dbutils.Runtime

	DBLocker io.Closer

	Closed *atomic.Value
}

func (db *DB) GetTxnMode() DBTxnMode {
	return DBTxnMode(db.TxnMode.Load())
}

func (db *DB) IsReplayMode() bool {
	return db.GetTxnMode() == DBTxnMode_Replay
}

func (db *DB) IsWriteMode() bool {
	return db.GetTxnMode() == DBTxnMode_Write
}

func (db *DB) SwitchTxnMode(
	ctx context.Context,
	iarg int,
	sarg string,
) error {
	return db.Controller.SwitchTxnMode(ctx, iarg, sarg)
}

func (db *DB) GetUsageMemo() *logtail.TNUsageMemo {
	return db.usageMemo
}

func (db *DB) FlushTable(
	ctx context.Context,
	tenantID uint32,
	dbId, tableId uint64,
	ts types.TS) (err error) {
	err = db.BGCheckpointRunner.FlushTable(ctx, dbId, tableId, ts)
	return
}

func (db *DB) ForceCheckpoint(
	ctx context.Context,
	ts types.TS,
	flushDuration time.Duration,
) (err error) {
	// FIXME: cannot disable with a running job
	db.BGCheckpointRunner.DisableCheckpoint()
	defer db.BGCheckpointRunner.EnableCheckpoint()
	db.BGCheckpointRunner.CleanPenddingCheckpoint()
	if flushDuration == 0 {
		flushDuration = time.Minute * 3 / 2
	}
	t0 := time.Now()
	err = db.BGCheckpointRunner.ForceFlush(ts, ctx, flushDuration)
	forceFlushCost := time.Since(t0)

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Control-Force-Checkpoint",
			zap.Error(err),
			zap.Duration("total-cost", time.Since(t0)),
			zap.String("ts", ts.ToString()),
			zap.Duration("flush-duration", flushDuration),
			zap.Duration("force-flush-cost", forceFlushCost),
		)
	}()

	if err != nil {
		return err
	}

	wait := flushDuration - time.Since(t0)
	if wait < time.Minute {
		wait = time.Minute
	}

	timeout := time.After(wait)
	for {
		select {
		case <-timeout:
			err = moerr.NewInternalError(ctx, "force checkpoint timeout")
			return
		default:
			err = db.BGCheckpointRunner.ForceIncrementalCheckpoint(ts, true)
			if dbutils.IsRetrieableCheckpoint(err) {
				db.BGCheckpointRunner.CleanPenddingCheckpoint()
				interval := flushDuration.Milliseconds() / 400
				time.Sleep(time.Duration(interval))
				break
			}
			return
		}
	}
}

func (db *DB) ForceGlobalCheckpoint(
	ctx context.Context,
	ts types.TS,
	flushDuration, versionInterval time.Duration,
) (err error) {
	// FIXME: cannot disable with a running job
	db.BGCheckpointRunner.DisableCheckpoint()
	defer db.BGCheckpointRunner.EnableCheckpoint()
	db.BGCheckpointRunner.CleanPenddingCheckpoint()
	t0 := time.Now()
	err = db.BGCheckpointRunner.ForceFlush(ts, ctx, flushDuration)
	forceFlushCost := time.Since(t0)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Control-ForceGlobalCheckpoint",
			zap.Duration("total-cost", time.Since(t0)),
			zap.Duration("force-flush-cost", forceFlushCost),
			zap.Duration("flush-duration", flushDuration),
			zap.Duration("version-interval", versionInterval),
			zap.Error(err),
		)
	}()

	if err != nil {
		return
	}

	err = db.BGCheckpointRunner.ForceGlobalCheckpointSynchronously(
		ctx, ts, versionInterval,
	)
	return err
}

func (db *DB) ForceCheckpointForBackup(
	ctx context.Context,
	ts types.TS,
	flushDuration time.Duration,
) (location string, err error) {
	// FIXME: cannot disable with a running job
	db.BGCheckpointRunner.DisableCheckpoint()
	defer db.BGCheckpointRunner.EnableCheckpoint()
	db.BGCheckpointRunner.CleanPenddingCheckpoint()
	t0 := time.Now()
	err = db.BGCheckpointRunner.ForceFlush(ts, ctx, flushDuration)
	forceFlushCost := time.Since(t0)

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Control-ForeCheckpointForBackup",
			zap.Duration("total-cost", time.Since(t0)),
			zap.Duration("force-flush-cost", forceFlushCost),
			zap.Duration("flush-duration", flushDuration),
			zap.String("location", location),
			zap.Error(err),
		)
	}()

	if err != nil {
		return
	}

	location, err = db.BGCheckpointRunner.ForceCheckpointForBackup(ts)

	return
}

func (db *DB) StartTxn(info []byte) (txnif.AsyncTxn, error) {
	return db.TxnMgr.StartTxn(info)
}

func (db *DB) CommitTxn(txn txnif.AsyncTxn) (err error) {
	return txn.Commit(context.Background())
}

func (db *DB) GetTxnByID(id []byte) (txn txnif.AsyncTxn, err error) {
	txn = db.TxnMgr.GetTxnByCtx(id)
	if txn == nil {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (db *DB) GetOrCreateTxnWithMeta(
	info []byte,
	id []byte,
	ts types.TS) (txn txnif.AsyncTxn, err error) {
	return db.TxnMgr.GetOrCreateTxnWithMeta(info, id, ts)
}

func (db *DB) StartTxnWithStartTSAndSnapshotTS(
	info []byte,
	ts types.TS) (txn txnif.AsyncTxn, err error) {
	return db.TxnMgr.StartTxnWithStartTSAndSnapshotTS(info, ts, ts)
}

func (db *DB) RollbackTxn(txn txnif.AsyncTxn) error {
	return txn.Rollback(context.Background())
}

func (db *DB) Replay(dataFactory *tables.DataFactory, maxTs types.TS, lsn uint64, valid bool) {
	if !valid {
		logutil.Infof("checkpoint version is too small, LSN check is disable")
	}
	replayer := newReplayer(dataFactory, db, maxTs, lsn, valid)
	replayer.OnTimeStamp(maxTs)
	replayer.Replay()

	err := db.TxnMgr.Init(replayer.GetMaxTS())

	db.usageMemo.EstablishFromCKPs(db.Catalog)
	if err != nil {
		panic(err)
	}
}

func (db *DB) AddFaultPoint(
	ctx context.Context, name string, freq string,
	action string, iarg int64, sarg string, constant bool,
) error {
	return fault.AddFaultPoint(ctx, name, freq, action, iarg, sarg, constant)
}

func (db *DB) ResetTxnHeartbeat() {
	db.TxnMgr.ResetHeartbeat()
}

func (db *DB) StopTxnHeartbeat() {
	db.TxnMgr.StopHeartbeat()
}

func (db *DB) Close() error {
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	db.Closed.Store(ErrClosed)
	db.Controller.Stop()
	db.CronJobs.Reset()
	db.BGScanner.Stop()
	db.BGCheckpointRunner.Stop()
	db.Runtime.Scheduler.Stop()
	db.TxnMgr.Stop()
	db.LogtailMgr.Stop()
	db.Catalog.Close()
	db.DiskCleaner.Stop()
	db.Wal.Close()
	db.Runtime.TransferTable.Close()
	db.usageMemo.Clear()
	return db.DBLocker.Close()
}
