// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync/atomic"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"

	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

var ErrCancelReplayAnyway = moerr.NewInternalErrorNoCtx("terminate")

var (
	skippedTbl = map[uint64]bool{
		pkgcatalog.MO_DATABASE_ID: true,
		pkgcatalog.MO_TABLES_ID:   true,
		pkgcatalog.MO_COLUMNS_ID:  true,
	}
)

type replayCtl struct {
	startTime   time.Time
	doneTime    time.Time
	doneCh      chan struct{}
	err         error
	causeCancel context.CancelCauseFunc
	mode        atomic.Int32
	onSuccess   func()
	replayer    *WalReplayer
}

func newReplayCtl(
	replayer *WalReplayer,
	mode driver.ReplayMode,
	onSuccess func(),
) *replayCtl {
	ctl := &replayCtl{
		doneCh:    make(chan struct{}),
		startTime: time.Now(),
		onSuccess: onSuccess,
		replayer:  replayer,
	}
	ctl.mode.Store(int32(mode))
	return ctl
}

func (ctl *replayCtl) MaxLSN() uint64 {
	return ctl.replayer.MaxLSN()
}

func (ctl *replayCtl) Wait() (err error) {
	<-ctl.doneCh
	return ctl.err
}

func (ctl *replayCtl) Done(err error) {
	ctl.err = err
	ctl.doneTime = time.Now()
	// PXU TODO: why onSuccess is called even if there is an error. since
	// any error before will be panic.(Fix it later)
	if ctl.onSuccess != nil {
		ctl.onSuccess()
	}
	close(ctl.doneCh)
}

func (ctl *replayCtl) Stop() (err error) {
	if ctl.causeCancel != nil {
		ctl.causeCancel(ErrCancelReplayAnyway)
		ctl.Wait()
		ctl.causeCancel = nil
	}

	err = ctl.err
	// ErrCancelReplayAnyway or nil means no error
	return
}

func (ctl *replayCtl) StopForWrite() (err error) {
	ctl.mode.Store(int32(driver.ReplayMode_ReplayForWrite))
	ctl.Wait()
	// here cancel has no effect, just close the channel
	if ctl.causeCancel != nil {
		ctl.causeCancel(nil)
		ctl.causeCancel = nil
	}

	// nil means no error
	err = ctl.err
	return
}

func (ctl *replayCtl) GetMode() driver.ReplayMode {
	return driver.ReplayMode(ctl.mode.Load())
}

// must be called after Wait
func (ctl *replayCtl) Err() error {
	return ctl.err
}

// must be called after Wait
func (ctl *replayCtl) Duration() time.Duration {
	return ctl.doneTime.Sub(ctl.startTime)
}

type WalReplayer struct {
	db            *DB
	maxTs         types.TS
	once          sync.Once
	fromTS        types.TS
	applyDuration time.Duration
	readCount     int
	applyCount    int
	maxLSN        atomic.Uint64

	lsn uint64
}

func newWalReplayer(
	db *DB,
	fromTS types.TS,
	lsn uint64,
) *WalReplayer {
	replayer := &WalReplayer{
		db:     db,
		fromTS: fromTS,
		lsn:    lsn,
	}
	replayer.OnTimeStamp(fromTS)
	return replayer
}

func (replayer *WalReplayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if entry.GetTable().IsVirtual() {
			return moerr.GetOkStopCurrRecur()
		}
		dropCommit, obj := entry.TreeMaxDropCommitEntry()
		if dropCommit != nil && dropCommit.DeleteBeforeLocked(replayer.fromTS) {
			return moerr.GetOkStopCurrRecur()
		}
		if obj != nil && obj.DeleteBefore(replayer.fromTS) {
			return moerr.GetOkStopCurrRecur()
		}
		entry.InitData(replayer.db.Catalog.DataFactory)
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		if !moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			panic(err)
		}
	}
}

func (replayer *WalReplayer) postReplayWal() error {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if skippedTbl[entry.GetTable().ID] {
			return nil
		}
		if entry.IsAppendable() && entry.HasDropCommitted() {
			err = entry.GetObjectData().TryUpgrade()
		}
		return
	}
	return replayer.db.Catalog.RecurLoop(processor)
}

func (replayer *WalReplayer) Schedule(
	ctx context.Context,
	mode driver.ReplayMode,
	onDone func(),
) (
	ctl *replayCtl,
	err error,
) {
	var (
		wg       sync.WaitGroup
		cmdQueue = make(chan *txnbase.TxnCmd, 100)
	)
	wg.Add(1)
	go replayer.applyReplayTxnLoop(cmdQueue, &wg)

	ctl = newReplayCtl(
		replayer,
		mode,
		func() {
			replayer.db.usageMemo.EstablishFromCKPs()
			replayer.db.Catalog.ReplayTableRows()
			if onDone != nil {
				onDone()
			}
		},
	)
	ctx, ctl.causeCancel = context.WithCancelCause(ctx)

	go func() {
		var err2 error
		defer func() {
			logger := logutil.Info
			if err2 != nil {
				logger = logutil.Error
			}
			logger(
				"Wal-Replay-Trace-End",
				zap.Duration("apply-cost", replayer.applyDuration),
				zap.Int("read-count", replayer.readCount),
				zap.Int("apply-count", replayer.applyCount),
				zap.Uint64("max-lsn", replayer.maxLSN.Load()),
				zap.Error(err2),
			)
			ctl.Done(err2)
		}()
		err2 = replayer.db.Wal.Replay(
			ctx,
			replayer.MakeReplayHandle(cmdQueue),
			ctl.GetMode,
			nil,
		)
		cmdQueue <- txnbase.NewEndCmd()
		close(cmdQueue)
		wg.Wait()
		if err2 != nil {
			return
		}
		err2 = replayer.postReplayWal()
	}()
	return
}

func (replayer *WalReplayer) MaxLSN() uint64 {
	return replayer.maxLSN.Load()
}

func (replayer *WalReplayer) MakeReplayHandle(
	sender chan<- *txnbase.TxnCmd,
) wal.ApplyHandle {
	return func(
		group uint32, lsn uint64, payload []byte, typ uint16, info any,
	) driver.ReplayEntryState {
		replayer.once.Do(replayer.PreReplayWal)
		if group != wal.GroupUserTxn && group != wal.GroupC {
			return driver.RE_Internal
		}
		if !replayer.checkLSN(lsn) {
			return driver.RE_Truncate
		}
		head := objectio.DecodeIOEntryHeader(payload)
		if head.Version < txnbase.IOET_WALTxnEntry_V4 {
			return driver.RE_Nomal
		}
		codec := objectio.GetIOEntryCodec(*head)
		entry, err := codec.Decode(payload[4:])
		if err != nil {
			panic(err)
		}
		txnCmd := entry.(*txnbase.TxnCmd)
		txnCmd.Lsn = lsn
		sender <- txnCmd
		return driver.RE_Nomal
	}
}

func (replayer *WalReplayer) applyReplayTxnLoop(
	receiver <-chan *txnbase.TxnCmd,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		txnCmd := <-receiver
		if txnCmd.IsEnd() {
			break
		}
		t0 := time.Now()
		replayer.OnReplayTxn(txnCmd, txnCmd.Lsn)
		if txnCmd.Lsn > replayer.maxLSN.Load() {
			replayer.maxLSN.Store(txnCmd.Lsn)
		}
		txnCmd.Close()
		replayer.applyDuration += time.Since(t0)
	}
}
func (replayer *WalReplayer) GetMaxTS() types.TS {
	return replayer.maxTs
}

func (replayer *WalReplayer) OnTimeStamp(ts types.TS) {
	if ts.GT(&replayer.maxTs) {
		replayer.maxTs = ts
	}
}
func (replayer *WalReplayer) checkLSN(lsn uint64) (needReplay bool) {
	if lsn <= replayer.lsn {
		return false
	}
	if lsn == replayer.lsn+1 {
		replayer.lsn++
		return true
	}
	panic(fmt.Sprintf("invalid lsn %d, current lsn %d", lsn, replayer.lsn))
}
func (replayer *WalReplayer) OnReplayTxn(cmd txnif.TxnCmd, lsn uint64) {
	var err error
	replayer.readCount++
	txnCmd := cmd.(*txnbase.TxnCmd)
	// If WAL entry splits, they share same prepareTS
	if txnCmd.PrepareTS.LT(&replayer.maxTs) {
		return
	}
	replayer.applyCount++
	txn := txnimpl.MakeReplayTxn(
		replayer.db.Runtime.Options.Ctx,
		replayer.db.TxnMgr,
		txnCmd.TxnCtx,
		lsn,
		txnCmd,
		replayer,
		replayer.db.Catalog,
	)
	if err = replayer.db.TxnMgr.OnReplayTxn(txn); err != nil {
		panic(err)
	}
	if txn.Is2PC() {
		if _, err = txn.Prepare(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	} else {
		if err = txn.Commit(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	}
}
