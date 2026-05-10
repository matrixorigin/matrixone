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
	"runtime"
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
	"github.com/panjf2000/ants/v2"
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

	previousSaveTS  types.TS
	logtailCallback atomic.Pointer[callback]
	logtailQueue    sm.Queue
	eventOnce       sync.Once
	nextCompactTS   types.TS

	orderedList []*txnWithLogtails
	collectWg   sync.WaitGroup
	collectPool *ants.Pool
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

	const batSize = 100
	mgr.orderedList = make([]*txnWithLogtails, batSize*2)
	// Re-panic from ants's internal recover so a panic inside a
	// collect goroutine crashes the process instead of being silently
	// swallowed. If we only logged and continued, a committed txn
	// whose collect panicked would apply to storage but its logtail
	// would never be published, breaking CN-side consistency. Fatal
	// crash matches what logservicedriver does for unrecoverable WAL
	// errors and is the safer failure mode here.
	mgr.collectPool, _ = ants.NewPool(
		runtime.NumCPU(),
		ants.WithPanicHandler(func(v any) { panic(v) }),
	)
	mgr.logtailQueue = sm.NewSafeQueue(batSize*batSize, batSize, mgr.onTxnLogTails)

	return mgr
}

type txnWithLogtails struct {
	txn     txnif.AsyncTxn
	tails   *[]logtail.TableLogtail
	closeCB func()
}

// orderedCollectAndPublish collects logtails for n items in parallel via submit,
// then publishes them strictly in index order (0, 1, 2, ...).
//
//   - skip(i) returning true means item i is excluded (no collect, no publish).
//   - collect(i) is invoked concurrently in a goroutine scheduled by submit.
//     Returning nil means the item was collected but should not be published
//     (e.g. the txn rolled back).
//   - publish(v) is invoked serially by the caller's goroutine, for each
//     collect result that is not nil, in ascending index order.
//
// The helper preserves PrepareTS ordering required by generateLogtailWithTxn
// (mgr.previousSaveTS invariant) while allowing later slots' collection to
// proceed in parallel with earlier slots' publish.
func orderedCollectAndPublish(
	n int,
	skip func(i int) bool,
	submit func(fn func()),
	collect func(i int) *txnWithLogtails,
	publish func(v *txnWithLogtails),
) {
	readyCh := make([]chan *txnWithLogtails, n)
	for i := 0; i < n; i++ {
		if skip(i) {
			readyCh[i] = nil
			continue
		}
		ch := make(chan *txnWithLogtails, 1)
		readyCh[i] = ch
		idx := i
		submit(func() {
			// The deferred send guarantees the publisher is never stuck
			// on <-ch, even if collect() panics: Go runs deferred funcs
			// during panic unwinding, so `ch <- v` executes with v still
			// at its zero value (nil), and the publisher's nil-skip path
			// drops that slot cleanly. The panic then continues to
			// propagate out of this goroutine into ants's top-level
			// recover, which logs the stack. Cleanup of per-txn state
			// (closeCB, DoneEvent) must be handled by collect itself via
			// its own defers — see onTxnLogTails below.
			var v *txnWithLogtails
			defer func() { ch <- v }()
			v = collect(idx)
		})
	}

	for _, ch := range readyCh {
		if ch == nil {
			continue
		}
		if v := <-ch; v != nil {
			publish(v)
		}
	}
}

// txnLogtailCollector is how onTxnLogTails invokes the real logtail
// builder. Exposed as a field only so tests can inject a stub without
// standing up a full TAE runtime.
type txnLogtailCollector func(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func())

// collectOneTxn runs the per-slot logic used by onTxnLogTails:
//   - WaitEvent(WalPreparing) — make sure WAL marshal is done first
//   - collect(txn) — materialize the logtail batches
//   - DoneEvent(TailCollecting) (deferred) — balance OnEndPrepareWAL's
//     matching AddEvent so WaitWalAndTail never hangs, even on panic
//   - closeCB (deferred unless we hand it to publish) — release
//     batches whenever we're not publishing
//   - GetTxnState — only committed txns get published; rollback
//     returns nil and the deferred closer releases batches
//
// On panic (e.g. GetTxnState on an unexpected state) the defers still
// fire: DoneEvent runs so apply doesn't hang locally, closeCB runs so
// batches don't leak. The panic then propagates out of this function
// into the collect goroutine, where the pool's PanicHandler re-panics
// to terminate the process so a committed-but-unpublished tail can
// never leak to subscribers.
func collectOneTxn(
	txn txnif.AsyncTxn,
	collect txnLogtailCollector,
) *txnWithLogtails {
	txn.GetStore().WaitEvent(txnif.WalPreparing)

	defer txn.GetStore().DoneEvent(txnif.TailCollecting)

	entries, closeCB := collect(txn)

	runCloseCB := true
	defer func() {
		if runCloseCB {
			closeCB()
		}
	}()

	// A rolled-back txn must not be published as logtail:
	// CollectLogtail walks the txn store without filtering by final
	// state, so the batches captured above reflect pre-cleanup
	// mutations that subscribers must never see. Release via the
	// deferred closer and skip publish by returning nil.
	state := txn.GetTxnState(true)
	if state != txnif.TxnStateCommitted {
		if state != txnif.TxnStateRollbacked {
			panic(fmt.Sprintf("wrong state %v", state))
		}
		return nil
	}

	// Committed: hand closeCB over to the publish path.
	runCloseCB = false
	return &txnWithLogtails{
		txn:     txn,
		tails:   entries,
		closeCB: closeCB,
	}
}

func (mgr *Manager) onTxnLogTails(items ...any) {
	// Collect logtails for all txns in parallel via collectPool.
	// A slow txn only blocks the publisher up to its slot, not the
	// collection of later slots nor the publishing of earlier already-ready
	// slots. generateLogtailWithTxn is still called in PrepareTS order.
	collect := func(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
		builder := NewTxnLogtailRespBuilder(mgr.rt)
		return builder.CollectLogtail(txn)
	}
	orderedCollectAndPublish(
		len(items),
		func(i int) bool {
			return items[i].(txnif.AsyncTxn).IsReplay()
		},
		func(fn func()) { _ = mgr.collectPool.Submit(fn) },
		func(i int) *txnWithLogtails {
			return collectOneTxn(items[i].(txnif.AsyncTxn), collect)
		},
		mgr.generateLogtailWithTxn,
	)
}

func (mgr *Manager) Stop() {
	mgr.logtailQueue.Stop()
	mgr.collectPool.Release()
}

func (mgr *Manager) Start() {
	mgr.logtailQueue.Start()
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
	txn.GetStore().AddEvent(txnif.TailCollecting)
	mgr.logtailQueue.Enqueue(txn)
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
