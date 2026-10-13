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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	logtailQueueBatchSize  = 100
	maxPendingReadBarriers = logtailQueueBatchSize
)

func MockCallback(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error {
	if closeCB != nil {
		defer closeCB()
	}
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
	table      *TxnTable
	rt         *dbutils.Runtime
	truncateMu sync.RWMutex
	truncated  types.TS
	nowClock   func() types.TS // nowClock is from TxnManager

	maxCommittedLSN atomic.Uint64

	previousSaveTS  types.TS
	logtailCallback atomic.Pointer[callback]
	logtailQueue    sm.ContextQueue
	// readBarrierSlots bounds control-plane work admitted to the same FIFO as
	// commit logtails. Its capacity is one queue batch, so any number of login
	// attempts can add at most one batch of markers ahead of commit producers.
	readBarrierSlots    chan struct{}
	pendingReadBarriers atomic.Int64
	eventOnce           sync.Once
	nextCompactTS       types.TS

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
	mgr.logtailQueue = sm.NewSafeQueue(
		logtailQueueBatchSize*logtailQueueBatchSize,
		logtailQueueBatchSize,
		mgr.onTxnLogTails,
	)
	mgr.readBarrierSlots = make(chan struct{}, maxPendingReadBarriers)

	return mgr
}

type txnWithLogtails struct {
	txn     txnif.AsyncTxn
	tails   *[]logtail.TableLogtail
	closeCB func()
}

// readBarrier is a marker in the same FIFO as committed transactions. Once
// the manager reaches it, every transaction queued before the marker has been
// collected and handed to the logtail publisher in PrepareTS order.
type readBarrier struct {
	done    chan timestamp.Timestamp
	release func()
	once    sync.Once
}

func newReadBarrier(release func()) *readBarrier {
	return &readBarrier{
		done:    make(chan timestamp.Timestamp, 1),
		release: release,
	}
}

func (b *readBarrier) complete(ts timestamp.Timestamp) {
	b.once.Do(func() {
		// The channel is buffered because the request may be canceled after the
		// marker was admitted. Queue progress must never depend on that caller.
		b.done <- ts
		b.release()
	})
}

func (b *readBarrier) abort() {
	b.once.Do(b.release)
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
//   - DoneEvent(TailCollecting) — balance OnEndPrepareWAL's matching
//     AddEvent so the owning txn's WaitWalAndTail can proceed. MUST
//     fire before GetTxnState(true): apply runs WaitWalAndTail then
//     DoneApply which flips the state, so if we wait on the state
//     first the commit goroutine is blocked behind our event and we
//     deadlock.
//   - GetTxnState — only committed txns get published; rollback
//     returns nil and the deferred closer releases batches
//   - closeCB (deferred unless we hand it to publish) — release
//     batches whenever we're not publishing
//
// A doneTail flag plus a deferred fallback guarantees DoneEvent still
// fires if collect(txn) panics before we reach the inline call. closeCB
// is only defined after collect succeeds, so its deferred release is
// registered afterwards. The panic then propagates out of this function
// into the collect goroutine, where the pool's PanicHandler re-panics
// to terminate the process so a committed-but-unpublished tail can
// never leak to subscribers.
func collectOneTxn(
	txn txnif.AsyncTxn,
	collect txnLogtailCollector,
) *txnWithLogtails {
	txn.GetStore().WaitEvent(txnif.WalPreparing)

	doneTail := false
	defer func() {
		if !doneTail {
			txn.GetStore().DoneEvent(txnif.TailCollecting)
		}
	}()

	entries, closeCB := collect(txn)

	// Unblock apply's WaitWalAndTail before waiting on the txn state.
	// Waiting first would deadlock: apply holds the commit state flip
	// behind WaitWalAndTail, which waits on TailCollecting.
	txn.GetStore().DoneEvent(txnif.TailCollecting)
	doneTail = true

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
	collect := func(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
		builder := NewTxnLogtailRespBuilder(mgr.rt)
		return builder.CollectLogtail(txn)
	}

	// This is the normal commit path. Do not scan and type-assert the batch a
	// second time when no barrier is outstanding. A marker acquired after this
	// load cannot be part of items, which the queue already removed atomically.
	if mgr.pendingReadBarriers.Load() == 0 {
		mgr.collectAndPublishTxnSegment(items, collect)
		return
	}

	// A barrier splits a queue batch into ordered transaction segments. Work
	// after the barrier is deliberately not scheduled before the marker: it
	// cannot contribute to the frontier and must not compete with the work the
	// caller is waiting for. Each segment retains parallel collection and
	// PrepareTS-ordered publication through orderedCollectAndPublish.
	segmentStart := 0
	for i := 0; i < len(items); {
		item := items[i]
		_, ok := item.(*readBarrier)
		if !ok {
			if _, ok := item.(txnif.AsyncTxn); !ok {
				panic(fmt.Sprintf("unknown logtail queue item %T", item))
			}
			i++
			continue
		}
		mgr.collectAndPublishTxnSegment(items[segmentStart:i], collect)
		frontier := mgr.previousSaveTS.ToTimestamp()
		// Adjacent barriers share exactly the same FIFO frontier. Complete them
		// together without creating empty transaction segments between markers.
		for i < len(items) {
			barrier, ok := items[i].(*readBarrier)
			if !ok {
				break
			}
			barrier.complete(frontier)
			i++
		}
		segmentStart = i
	}
	mgr.collectAndPublishTxnSegment(items[segmentStart:], collect)
}

func (mgr *Manager) collectAndPublishTxnSegment(
	segment []any,
	collect txnLogtailCollector,
) {
	if len(segment) == 0 {
		return
	}
	orderedCollectAndPublish(
		len(segment),
		func(i int) bool {
			txn, ok := segment[i].(txnif.AsyncTxn)
			if !ok {
				panic(fmt.Sprintf("unknown logtail queue item %T", segment[i]))
			}
			return txn.IsReplay()
		},
		func(fn func()) {
			if err := mgr.collectPool.Submit(fn); err != nil {
				panic(err)
			}
		},
		func(i int) *txnWithLogtails {
			return collectOneTxn(segment[i].(txnif.AsyncTxn), collect)
		},
		mgr.generateLogtailWithTxn,
	)
}

// ReadBarrier returns the latest logtail frontier after all transactions that
// entered the manager before this call have been published. The returned
// timestamp is an exact committed frontier, not a wall-clock estimate.
func (mgr *Manager) ReadBarrier(ctx context.Context) (timestamp.Timestamp, error) {
	if mgr.logtailCallback.Load() == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "logtail publisher is not registered")
	}
	if err := ctx.Err(); err != nil {
		return timestamp.Timestamp{}, context.Cause(ctx)
	}

	select {
	case mgr.readBarrierSlots <- struct{}{}:
		mgr.pendingReadBarriers.Add(1)
	case <-ctx.Done():
		return timestamp.Timestamp{}, context.Cause(ctx)
	}
	barrier := newReadBarrier(func() {
		mgr.pendingReadBarriers.Add(-1)
		<-mgr.readBarrierSlots
	})
	if _, err := mgr.logtailQueue.EnqueueWithContext(ctx, barrier); err != nil {
		barrier.abort()
		return timestamp.Timestamp{}, err
	}

	select {
	case ts := <-barrier.done:
		return ts, nil
	case <-ctx.Done():
		return timestamp.Timestamp{}, context.Cause(ctx)
	}
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
			// This event carries only a watermark. It never owns transaction
			// batches: the following real event is their sole owner, which lets
			// server shutdown drain callbacks exactly once.
			_ = callback.call(from.ToTimestamp(), from.ToTimestamp(), nil)
		})
		if err := callback.call(from.ToTimestamp(), to.ToTimestamp(), txn.closeCB, *txn.tails...); err != nil {
			// The callback did not accept ownership (for example the push
			// server is already closed), so release collected batches locally.
			txn.closeCB()
		}
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
	mgr.truncateMu.RLock()
	defer mgr.truncateMu.RUnlock()
	return mgr.truncated
}

func (mgr *Manager) GCByTS(ctx context.Context, ts types.TS) (updated bool) {
	mgr.truncateMu.Lock()
	if ts.LE(&mgr.truncated) {
		mgr.truncateMu.Unlock()
		return
	}
	mgr.truncated = ts
	mgr.truncateMu.Unlock()

	updated = true
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
