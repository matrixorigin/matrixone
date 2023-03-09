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
	pblogtail "github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

const (
	LogtailHeartbeatDuration = time.Millisecond * 2
)

type TxnMgr interface {
	Lock()
	Unlock()
}

type callback func(from, to timestamp.Timestamp, tails ...logtail.TableLogtail)

func MockCallback(from, to timestamp.Timestamp, tails ...pblogtail.TableLogtail) {
	if len(tails) == 0 {
		return
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
}

type Manager struct {
	txnbase.NoopCommitListener
	table     *TxnTable
	truncated types.TS

	committingTS        types.TS
	previousSaveTS      types.TS
	committedTS         atomic.Value
	logtailCallback     atomic.Value
	collectLogtailQueue sm.Queue
	waitCommitQueue     sm.Queue
	committedTxn        chan *txnWithLogtails
	txnMgr              TxnMgr
	tmpAlloctor         *types.TsAlloctor
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
}

func NewManager(blockSize int, clock clock.Clock, txnMgr TxnMgr) *Manager {
	tsAlloc := types.NewTsAlloctor(clock)
	mgr := &Manager{
		table: NewTxnTable(
			blockSize,
			tsAlloc,
		),
		tmpAlloctor: tsAlloc,
		wg:          sync.WaitGroup{},
		txnMgr:      txnMgr,
	}
	mgr.previousSaveTS = tsAlloc.Alloc()
	mgr.collectLogtailQueue = sm.NewSafeQueue(10000, 100, mgr.onCollectTxnLogtails)
	mgr.waitCommitQueue = sm.NewSafeQueue(10000, 100, mgr.onWaitTxnCommit)
	mgr.committedTxn = make(chan *txnWithLogtails)
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.Start()

	return mgr
}
func (mgr *Manager) OnCommittingTS(ts types.TS) {
	mgr.committingTS = ts
}

func (mgr *Manager) OnCommittedTS(ts types.TS) {
	mgr.committedTS.Store(ts)
}
func (mgr *Manager) generateHeartbeat() {
	icallback := mgr.logtailCallback.Load()
	if icallback != nil {
		callback := icallback.(callback)
		from := mgr.previousSaveTS
		to := mgr.getSaveTS()
		callback(from.ToTimestamp(), to.ToTimestamp())
	}
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
		if len(*entries) == 0 {
			continue
		}
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
		mgr.committedTxn <- txn
	}
}
func (mgr *Manager) Stop() {
	mgr.cancel()
	mgr.wg.Wait()
	mgr.collectLogtailQueue.Stop()
	mgr.waitCommitQueue.Stop()
}
func (mgr *Manager) Start() {
	mgr.wg.Add(1)
	mgr.collectLogtailQueue.Start()
	mgr.waitCommitQueue.Start()
	go mgr.generateLogtails()
}
func (mgr *Manager) generateLogtails() {
	defer mgr.wg.Done()
	ticker := time.NewTicker(LogtailHeartbeatDuration)
	for {
		select {
		case <-mgr.ctx.Done():
			return
		case txn := <-mgr.committedTxn:
			mgr.generateLogtailWithTxn(txn)
		case <-ticker.C:
			mgr.generateHeartbeat()
		}
	}
}
func (mgr *Manager) generateLogtailWithTxn(txn *txnWithLogtails) {
	icallback := mgr.logtailCallback.Load()
	if icallback != nil {
		callback := icallback.(callback)
		to := (*txn.tails)[0].Ts
		from := mgr.previousSaveTS
		mgr.previousSaveTS = types.TimestampToTS(*to)
		callback(from.ToTimestamp(), *to, *txn.tails...)
	}
}
func (mgr *Manager) getSaveTS() types.TS {
	mgr.txnMgr.Lock()
	defer mgr.txnMgr.Unlock()
	committingTS := mgr.committingTS
	if committingTS.IsEmpty() {
		ts := mgr.tmpAlloctor.Alloc()
		mgr.previousSaveTS = ts
		return ts
	}
	committedTS := mgr.committedTS.Load()
	if committedTS == nil {
		return mgr.previousSaveTS
	}
	ts := committedTS.(types.TS)
	mgr.previousSaveTS = ts
	return ts
}

func (mgr *Manager) OnEndPrePrepare(txn txnif.AsyncTxn) {
	mgr.table.AddTxn(txn)
}
func (mgr *Manager) OnEndPreApplyCommit(txn txnif.AsyncTxn) {
	mgr.collectLogtailQueue.Enqueue(txn)
}

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

func (mgr *Manager) RegisterCallback(cb callback) error {
	mgr.logtailCallback.Store(cb)
	return nil
}
