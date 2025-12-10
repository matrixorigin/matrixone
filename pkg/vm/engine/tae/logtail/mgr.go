// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

const (
	LogtailHeartbeatDuration = time.Millisecond * 2
)

func MockCallback(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error {
	defer closeCB()
	if len(tails) == 0 {
		return nil
	}
	s := fmt.Sprintf("get logtail\nfrom %v, to %v, tails cnt %d", from, to, len(tails))
	for _, tail := range tails {
		s = fmt.Sprintf("%s\nts %v, dbid %d, tid %d,entries cnt %d", s, tail.Ts, tail.Table.DbId, tail.Table.TbId, len(tail.Commands))
		for _, entry := range tail.Commands {
			s = fmt.Sprintf("%s\n    db name %s, table name %s, insert %v, batch length %d\n    %v",
				s, entry.DatabaseName, entry.TableName, entry.EntryType == api.Entry_Insert, entry.Bat.Vecs[0].Len, entry.Bat.Attrs)
			for i, vec := range entry.Bat.Vecs {
				s = fmt.Sprintf("%s\n        %v, type %v, len %d", s, entry.Bat.Attrs[i], vec.Type, vec.Len)
			}
		}
	}
	logutil.Info(s)
	return nil
}

type callback struct {
	cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error
}

func (cb *callback) call(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error {
	// for debug
	// MockCallback(from,to,tails...)
	return cb.cb(from, to, closeCB, tails...)
}

// Logtail manager holds sorted txn handles. Its main jobs:
//
// - Insert new txn handle
// - Efficiently iterate over arbitrary range of txn handles on a snapshot
// - Truncate unneceessary txn handles according to GC timestamp
type Manager struct {
	txnbase.NoopCommitListener
	table     *TxnTable
	rt        *dbutils.Runtime
	truncated types.TS
	nowClock  func() types.TS // nowClock is from TxnManager

	maxCommittedLSN atomic.Uint64

	previousSaveTS      types.TS
	logtailCallback     atomic.Pointer[callback]
	collectLogtailQueue sm.Queue
	waitCommitQueue     sm.Queue
	eventOnce           sync.Once
	nextCompactTS       types.TS
}

func NewManager(
	rt *dbutils.Runtime,
	blockSize int,
	nowClock func() types.TS,
) *Manager {

	mgr := &Manager{
		rt: rt,
		table: NewTxnTable(
			blockSize,
			nowClock,
		),
		nowClock: nowClock,
	}
	mgr.collectLogtailQueue = sm.NewSafeQueue(10000, 100, mgr.onCollectTxnLogtails)
	mgr.waitCommitQueue = sm.NewSafeQueue(10000, 100, mgr.onWaitTxnCommit)

	return mgr
}

type txnWithLogtails struct {
	txn     txnif.AsyncTxn
	tails   *[]logtail.TableLogtail
	closeCB func()
}

func (mgr *Manager) onCollectTxnLogtails(items ...any) {
	for _, item := range items {
		txn := item.(txnif.AsyncTxn)
		if txn.IsReplay() {
			lsn := txn.GetLSN()
			if lsn > mgr.maxCommittedLSN.Load() {
				mgr.maxCommittedLSN.Store(lsn)
			} else {
				logutil.Warn(
					"Logtail-Manager-Wrong-LSN",
					zap.Uint64("lsn", lsn),
					zap.Uint64("max-lsn", mgr.maxCommittedLSN.Load()),
				)
			}
			continue
		}
		builder := NewTxnLogtailRespBuilder(mgr.rt)
		entries, closeCB := builder.CollectLogtail(txn)
		txn.GetStore().DoneWaitEvent(1)
		txnWithLogtails := &txnWithLogtails{
			txn:     txn,
			tails:   entries,
			closeCB: closeCB,
		}
		mgr.waitCommitQueue.Enqueue(txnWithLogtails)
	}
}
func (mgr *Manager) onWaitTxnCommit(items ...any) {
	for _, item := range items {
		txn := item.(*txnWithLogtails)
		state := txn.txn.GetTxnState(true)
		if state != txnif.TxnStateCommitted {
			if state != txnif.TxnStateRollbacked {
				panic(fmt.Sprintf("wrong state %v", state))
			}
			continue
		}
		if !txn.txn.GetStore().IsHeartbeat() {
			lsn := txn.txn.GetLSN()
			if lsn > mgr.maxCommittedLSN.Load() {
				mgr.maxCommittedLSN.Store(lsn)
			} else {
				logutil.Warn(
					"Logtail-Manager-Wrong-LSN",
					zap.Uint64("lsn", lsn),
					zap.Uint64("max-lsn", mgr.maxCommittedLSN.Load()),
				)
			}
		}
		mgr.generateLogtailWithTxn(txn)
	}
}

func (mgr *Manager) Stop() {
	mgr.collectLogtailQueue.Stop()
	mgr.waitCommitQueue.Stop()
}

func (mgr *Manager) Start() {
	mgr.waitCommitQueue.Start()
	mgr.collectLogtailQueue.Start()
}

func (mgr *Manager) UpdateMaxCommittedLSN(lsn uint64) {
	if lsn > mgr.maxCommittedLSN.Load() {
		mgr.maxCommittedLSN.Store(lsn)
	}
}

func (mgr *Manager) GetMaxCommittedLSN() uint64 {
	return mgr.maxCommittedLSN.Load()
}

func (mgr *Manager) generateLogtailWithTxn(txn *txnWithLogtails) {
	callback := mgr.logtailCallback.Load()
	if callback != nil {
		to := txn.txn.GetPrepareTS()
		var from types.TS
		if mgr.previousSaveTS.IsEmpty() {
			from = to
		} else {
			from = mgr.previousSaveTS
		}
		mgr.previousSaveTS = to
		// Send ts in order to initialize waterline of logtail service
		mgr.eventOnce.Do(func() {
			logutil.Info("logtail.mgr.init.waterline", zap.String("ts", from.ToString()))
			callback.call(from.ToTimestamp(), from.ToTimestamp(), txn.closeCB)
		})
		callback.call(from.ToTimestamp(), to.ToTimestamp(), txn.closeCB, *txn.tails...)
	} else {
		txn.closeCB()
	}
}

// OnEndPrePrepare is a listener for TxnManager. When a txn completes PrePrepare,
// add it to the logtail manager
func (mgr *Manager) OnEndPrePrepare(txn txnif.AsyncTxn) {
	if txn.GetStore().IsHeartbeat() {
		return
	}
	mgr.table.AddTxn(txn)
}
func (mgr *Manager) OnEndPrepareWAL(txn txnif.AsyncTxn) {
	txn.GetStore().AddWaitEvent(1)
	mgr.collectLogtailQueue.Enqueue(txn)
}

// GetReader get a snapshot of all txn prepared between from and to.
func (mgr *Manager) GetReader(from, to types.TS) *Reader {
	return &Reader{
		from:  from,
		to:    to,
		table: mgr.table,
	}
}

func (mgr *Manager) GetTruncateTS() types.TS {
	return mgr.truncated
}

func (mgr *Manager) GCByTS(ctx context.Context, ts types.TS) (updated bool) {
	if ts.LE(&mgr.truncated) {
		return
	}
	updated = true
	mgr.truncated = ts
	cnt := mgr.table.TruncateByTimeStamp(ts)
	logutil.Info(
		"GC-Logtail-Table",
		zap.String("ts", ts.ToString()),
		zap.Int("deleted-blk", cnt),
		zap.Int("remaining-blk", mgr.table.BlockCount()),
	)
	return
}

func (mgr *Manager) TryCompactTable() {
	mgr.nextCompactTS = mgr.table.TryCompact(mgr.nextCompactTS, mgr.rt)
}

func (mgr *Manager) GetTableOperator(
	from, to types.TS,
	tableEntry *catalog.TableEntry,
	visitor *TableLogtailRespBuilder,
) *BoundTableOperator {
	return &BoundTableOperator{
		tbl:     tableEntry,
		visitor: visitor,
		from:    from,
		to:      to,
	}
}

func (mgr *Manager) RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error) error {
	callbackFn := &callback{
		cb: cb,
	}
	mgr.logtailCallback.Store(callbackFn)
	return nil
}
