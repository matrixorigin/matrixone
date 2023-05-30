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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

const (
	LogtailHeartbeatDuration = time.Millisecond * 2
)

func MockCallback(from, to timestamp.Timestamp, tails ...logtail.TableLogtail) error {
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
	logutil.Infof(s)
	return nil
}

type callback struct {
	cb func(from, to timestamp.Timestamp, tails ...logtail.TableLogtail) error
}

func (cb *callback) call(from, to timestamp.Timestamp, tails ...logtail.TableLogtail) error {
	// for debug
	// MockCallback(from,to,tails...)
	return cb.cb(from, to, tails...)
}

// Logtail manager holds sorted txn handles. Its main jobs:
//
// - Insert new txn handle
// - Efficiently iterate over arbitrary range of txn handles on a snapshot
// - Truncate unneceessary txn handles according to GC timestamp
type Manager struct {
	txnbase.NoopCommitListener
	table     *TxnTable
	truncated types.TS
	now       func() types.TS // now is from TxnManager

	previousSaveTS      types.TS
	logtailCallback     atomic.Pointer[callback]
	collectLogtailQueue sm.Queue
	waitCommitQueue     sm.Queue
	eventOnce           sync.Once
}

func NewManager(blockSize int, now func() types.TS) *Manager {
	mgr := &Manager{
		table: NewTxnTable(
			blockSize,
			now,
		),
		now: now,
	}
	mgr.previousSaveTS = now()
	mgr.collectLogtailQueue = sm.NewSafeQueue(10000, 100, mgr.onCollectTxnLogtails)
	mgr.waitCommitQueue = sm.NewSafeQueue(10000, 100, mgr.onWaitTxnCommit)

	return mgr
}

type txnWithLogtails struct {
	txn   txnif.AsyncTxn
	tails *[]logtail.TableLogtail
}

func (mgr *Manager) onCollectTxnLogtails(items ...any) {
	builder := NewTxnLogtailRespBuilder()
	for _, item := range items {
		txn := item.(txnif.AsyncTxn)
		entries := builder.CollectLogtail(txn)
		txn.GetStore().DoneWaitEvent(1)
		txnWithLogtails := &txnWithLogtails{
			txn:   txn,
			tails: entries,
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

func (mgr *Manager) generateLogtailWithTxn(txn *txnWithLogtails) {
	callback := mgr.logtailCallback.Load()
	if callback != nil {
		to := txn.txn.GetPrepareTS()
		from := mgr.previousSaveTS
		mgr.previousSaveTS = to
		// Send ts in order to initialize waterline of logtail service
		mgr.eventOnce.Do(func() {
			logutil.Infof("init waterline to %v", from.ToString())
			callback.call(from.ToTimestamp(), from.ToTimestamp())
		})
		callback.call(from.ToTimestamp(), to.ToTimestamp(), *txn.tails...)
	}
}

// OnEndPrePrepare is a listener for TxnManager. When a txn completes PrePrepare,
// add it to the logtail manager
func (mgr *Manager) OnEndPrePrepare(txn txnif.AsyncTxn) {
	if txn.GetStore().GetTransactionType() == txnif.TxnType_Heartbeat {
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

func (mgr *Manager) GCByTS(ctx context.Context, ts types.TS) {
	if ts.Equal(mgr.truncated) {
		return
	}
	mgr.truncated = ts
	cnt := mgr.table.TruncateByTimeStamp(ts)
	logutil.Info("[logtail] GC", zap.String("ts", ts.ToString()), zap.Int("deleted", cnt))
}

func (mgr *Manager) GetTableOperator(
	from, to types.TS,
	catalog *catalog.Catalog,
	dbID, tableID uint64,
	scope Scope,
	visitor catalog.Processor,
) *BoundTableOperator {
	reader := mgr.GetReader(from, to)
	return NewBoundTableOperator(
		catalog,
		reader,
		scope,
		dbID,
		tableID,
		visitor,
	)
}

func (mgr *Manager) RegisterCallback(cb func(from, to timestamp.Timestamp, tails ...logtail.TableLogtail) error) error {
	callbackFn := &callback{
		cb: cb,
	}
	mgr.logtailCallback.Store(callbackFn)
	return nil
}
