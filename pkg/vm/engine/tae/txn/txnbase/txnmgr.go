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

package txnbase

import (
	"context"
	"fmt"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type TxnCommitListener interface {
	OnBeginPrePrepare(txnif.AsyncTxn)
	OnEndPrePrepare(txnif.AsyncTxn)
	OnEndPrepareWAL(txnif.AsyncTxn)
}

type NoopCommitListener struct{}

func (bl *NoopCommitListener) OnBeginPrePrepare(txn txnif.AsyncTxn) {}
func (bl *NoopCommitListener) OnEndPrePrepare(txn txnif.AsyncTxn)   {}

type batchTxnCommitListener struct {
	listeners []TxnCommitListener
}

func newBatchCommitListener() *batchTxnCommitListener {
	return &batchTxnCommitListener{
		listeners: make([]TxnCommitListener, 0),
	}
}

func (bl *batchTxnCommitListener) AddTxnCommitListener(l TxnCommitListener) {
	bl.listeners = append(bl.listeners, l)
}

func (bl *batchTxnCommitListener) OnBeginPrePrepare(txn txnif.AsyncTxn) {
	for _, l := range bl.listeners {
		l.OnBeginPrePrepare(txn)
	}
}

func (bl *batchTxnCommitListener) OnEndPrePrepare(txn txnif.AsyncTxn) {
	for _, l := range bl.listeners {
		l.OnEndPrePrepare(txn)
	}
}
func (bl *batchTxnCommitListener) OnEndPrepareWAL(txn txnif.AsyncTxn) {
	for _, l := range bl.listeners {
		l.OnEndPrepareWAL(txn)
	}
}

type TxnStoreFactory = func() txnif.TxnStore
type TxnFactory = func(*TxnManager, txnif.TxnStore, []byte, types.TS, types.TS) txnif.AsyncTxn

type TxnManager struct {
	sync.RWMutex
	sm.ClosedState
	PreparingSM     sm.StateMachine
	FlushQueue      sm.Queue
	IDMap           map[string]txnif.AsyncTxn
	IdAlloc         *common.TxnIDAllocator
	TsAlloc         *types.TsAlloctor
	MaxCommittedTS  atomic.Pointer[types.TS]
	TxnStoreFactory TxnStoreFactory
	TxnFactory      TxnFactory
	Exception       *atomic.Value
	CommitListener  *batchTxnCommitListener
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup

	// for debug
	prevPrepareTS             types.TS
	prevPrepareTSInPreparing  types.TS
	prevPrepareTSInPrepareWAL types.TS
}

func NewTxnManager(txnStoreFactory TxnStoreFactory, txnFactory TxnFactory, clock clock.Clock) *TxnManager {
	if txnFactory == nil {
		txnFactory = DefaultTxnFactory
	}
	mgr := &TxnManager{
		IDMap:           make(map[string]txnif.AsyncTxn),
		IdAlloc:         common.NewTxnIDAllocator(),
		TsAlloc:         types.NewTsAlloctor(clock),
		TxnStoreFactory: txnStoreFactory,
		TxnFactory:      txnFactory,
		Exception:       new(atomic.Value),
		CommitListener:  newBatchCommitListener(),
		wg:              sync.WaitGroup{},
	}
	mgr.initMaxCommittedTS()
	pqueue := sm.NewSafeQueue(20000, 1000, mgr.dequeuePreparing)
	prepareWALQueue := sm.NewSafeQueue(20000, 1000, mgr.onPrepareWAL)
	mgr.FlushQueue = sm.NewSafeQueue(20000, 1000, mgr.dequeuePrepared)
	mgr.PreparingSM = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, prepareWALQueue)

	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	return mgr
}

func (mgr *TxnManager) initMaxCommittedTS() {
	now := mgr.Now()
	mgr.MaxCommittedTS.Store(&now)
}

// Now gets a timestamp under the protect from a inner lock. The lock makes
// all timestamps allocated before have been assigned to txn, which means those
// txn are visible for the returned timestamp.
func (mgr *TxnManager) Now() types.TS {
	mgr.Lock()
	defer mgr.Unlock()
	return mgr.TsAlloc.Alloc()
}

func (mgr *TxnManager) Init(prevTs types.TS) error {
	logutil.Infof("init ts to %v", prevTs.ToString())
	mgr.TsAlloc.SetStart(prevTs)
	logutil.Debug("[INIT]", TxnMgrField(mgr))
	return nil
}

func (mgr *TxnManager) StatMaxCommitTS() (ts types.TS) {
	mgr.RLock()
	ts = mgr.TsAlloc.Alloc()
	mgr.RUnlock()
	return
}

// Note: Replay should always runs in a single thread
func (mgr *TxnManager) OnReplayTxn(txn txnif.AsyncTxn) (err error) {
	mgr.Lock()
	defer mgr.Unlock()
	// TODO: idempotent check
	mgr.IDMap[txn.GetID()] = txn
	return
}

// StartTxn starts a local transaction initiated by DN
func (mgr *TxnManager) StartTxn(info []byte) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := *mgr.MaxCommittedTS.Load()

	store := mgr.TxnStoreFactory()
	txn = mgr.TxnFactory(mgr, store, txnId, startTs, types.TS{})
	store.BindTxn(txn)
	mgr.IDMap[string(txnId)] = txn
	return
}

// StartTxn starts a local transaction initiated by DN
func (mgr *TxnManager) StartTxnWithLatestTS(info []byte) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := mgr.TsAlloc.Alloc()

	store := mgr.TxnStoreFactory()
	txn = mgr.TxnFactory(mgr, store, txnId, startTs, types.TS{})
	store.BindTxn(txn)
	mgr.IDMap[string(txnId)] = txn
	return
}

func (mgr *TxnManager) StartTxnWithStartTSAndSnapshotTS(
	info []byte,
	startTS, snapshotTS types.TS,
) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	store := mgr.TxnStoreFactory()
	txnId := mgr.IdAlloc.Alloc()
	txn = mgr.TxnFactory(mgr, store, txnId, startTS, snapshotTS)
	store.BindTxn(txn)
	mgr.IDMap[string(txnId)] = txn
	return
}

// GetOrCreateTxnWithMeta Get or create a txn initiated by CN
func (mgr *TxnManager) GetOrCreateTxnWithMeta(
	info []byte,
	id []byte,
	ts types.TS) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	txn, ok := mgr.IDMap[string(id)]
	if !ok {
		store := mgr.TxnStoreFactory()
		txn = mgr.TxnFactory(mgr, store, id, ts, ts)
		store.BindTxn(txn)
		mgr.IDMap[string(id)] = txn
	}
	return
}

func (mgr *TxnManager) DeleteTxn(id string) (err error) {
	mgr.Lock()
	defer mgr.Unlock()
	txn := mgr.IDMap[id]
	if txn == nil {
		err = moerr.NewTxnNotFoundNoCtx()
		logutil.Warnf("Txn %s not found", id)
		return
	}
	delete(mgr.IDMap, id)
	return
}

func (mgr *TxnManager) GetTxnByCtx(ctx []byte) txnif.AsyncTxn {
	return mgr.GetTxn(IDCtxToID(ctx))
}

func (mgr *TxnManager) GetTxn(id string) txnif.AsyncTxn {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.IDMap[id]
}

func (mgr *TxnManager) EnqueueFlushing(op any) (err error) {
	_, err = mgr.PreparingSM.EnqueueCheckpoint(op)
	return
}

func (mgr *TxnManager) heartbeat(ctx context.Context) {
	defer mgr.wg.Done()
	heartbeatTicker := time.NewTicker(time.Millisecond * 2)
	for {
		select {
		case <-mgr.ctx.Done():
			return
		case <-heartbeatTicker.C:
			op := mgr.newHeartbeatOpTxn(ctx)
			op.Txn.(*Txn).Add(1)
			_, err := mgr.PreparingSM.EnqueueRecevied(op)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (mgr *TxnManager) newHeartbeatOpTxn(ctx context.Context) *OpTxn {
	if exp := mgr.Exception.Load(); exp != nil {
		err := exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return nil
	}
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := mgr.TsAlloc.Alloc()

	store := &heartbeatStore{}
	txn := DefaultTxnFactory(mgr, store, txnId, startTs, types.TS{})
	store.BindTxn(txn)
	return &OpTxn{
		ctx: ctx,
		Txn: txn,
		Op:  OpCommit,
	}
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) (err error) {
	_, err = mgr.PreparingSM.EnqueueRecevied(op)
	return
}

func (mgr *TxnManager) onPrePrepare(op *OpTxn) {
	// If txn is not trying committing, do nothing
	if !op.IsTryCommitting() {
		return
	}

	mgr.CommitListener.OnBeginPrePrepare(op.Txn)
	defer mgr.CommitListener.OnEndPrePrepare(op.Txn)
	// If txn is trying committing, call txn.PrePrepare()
	now := time.Now()
	op.Txn.SetError(op.Txn.PrePrepare(op.ctx))
	common.DoIfDebugEnabled(func() {
		logutil.Debug("[PrePrepare]", TxnField(op.Txn), common.DurationField(time.Since(now)))
	})
}

func (mgr *TxnManager) onPreparCommit(txn txnif.AsyncTxn) {
	txn.SetError(txn.PrepareCommit())
}

func (mgr *TxnManager) onPreApplyCommit(txn txnif.AsyncTxn) {
	if err := txn.PreApplyCommit(); err != nil {
		txn.SetError(err)
		mgr.OnException(err)
	}
}

func (mgr *TxnManager) onPreparRollback(txn txnif.AsyncTxn) {
	_ = txn.PrepareRollback()
}

func (mgr *TxnManager) onBindPrepareTimeStamp(op *OpTxn) (ts types.TS) {
	// Replay txn is always prepared
	if op.IsReplay() {
		ts = op.Txn.GetPrepareTS()
		if err := op.Txn.ToPreparingLocked(ts); err != nil {
			panic(err)
		}
		return
	}

	mgr.Lock()
	defer mgr.Unlock()

	ts = mgr.TsAlloc.Alloc()
	if !mgr.prevPrepareTS.IsEmpty() {
		if ts.Less(mgr.prevPrepareTS) {
			panic(fmt.Sprintf("timestamp rollback current %v, previous %v", ts.ToString(), mgr.prevPrepareTS.ToString()))
		}
	}
	mgr.prevPrepareTS = ts

	op.Txn.Lock()
	defer op.Txn.Unlock()

	if op.Txn.GetError() != nil {
		op.Op = OpRollback
	}

	if op.Op == OpRollback {
		// Should not fail here
		_ = op.Txn.ToRollbackingLocked(ts)
	} else {
		// Should not fail here
		_ = op.Txn.ToPreparingLocked(ts)
	}
	return
}

func (mgr *TxnManager) onPrepare(op *OpTxn, ts types.TS) {
	//assign txn's prepare timestamp to TxnMvccNode.
	mgr.onPreparCommit(op.Txn)
	if op.Txn.GetError() != nil {
		op.Op = OpRollback
		op.Txn.Lock()
		// Should not fail here
		_ = op.Txn.ToRollbackingLocked(ts)
		op.Txn.Unlock()
		mgr.onPreparRollback(op.Txn)
	} else {
		// 1. Appending the data into appendableNode of block
		// 2. Collect redo log,append into WalDriver
		// TODO::need to handle the error,instead of panic for simplicity
		mgr.onPreApplyCommit(op.Txn)
		if op.Txn.GetError() != nil {
			panic(op.Txn.GetID())
		}
	}
}

func (mgr *TxnManager) onPrepare1PC(op *OpTxn, ts types.TS) {
	// If Op is not OpCommit, prepare rollback
	if op.Op != OpCommit {
		mgr.onPreparRollback(op.Txn)
		return
	}
	mgr.onPrepare(op, ts)
}

func (mgr *TxnManager) onPrepare2PC(op *OpTxn, ts types.TS) {
	// If Op is not OpPrepare, prepare rollback
	if op.Op != OpPrepare {
		mgr.onPreparRollback(op.Txn)
		return
	}

	mgr.onPrepare(op, ts)
}

func (mgr *TxnManager) on1PCPrepared(op *OpTxn) {
	var err error
	var isAbort bool
	switch op.Op {
	case OpCommit:
		isAbort = false
		if err = op.Txn.ApplyCommit(); err != nil {
			panic(err)
		}
	case OpRollback:
		isAbort = true
		if err = op.Txn.ApplyRollback(); err != nil {
			mgr.OnException(err)
			logutil.Warn("[ApplyRollback]", TxnField(op.Txn), common.ErrorField(err))
		}
	}
	mgr.OnCommitTxn(op.Txn)
	// Here to change the txn state and
	// broadcast the rollback or commit event to all waiting threads
	_ = op.Txn.WaitDone(err, isAbort)
}
func (mgr *TxnManager) OnCommitTxn(txn txnif.AsyncTxn) {
	commitTS := txn.GetCommitTS()
	mgr.MaxCommittedTS.Store(&commitTS)
}
func (mgr *TxnManager) on2PCPrepared(op *OpTxn) {
	var err error
	var isAbort bool
	switch op.Op {
	// case OpPrepare:
	// 	if err = op.Txn.ToPrepared(); err != nil {
	// 		panic(err)
	// 	}
	case OpRollback:
		isAbort = true
		if err = op.Txn.ApplyRollback(); err != nil {
			mgr.OnException(err)
			logutil.Warn("[ApplyRollback]", TxnField(op.Txn), common.ErrorField(err))
		}
	}
	// Here to change the txn state and
	// broadcast the rollback event to all waiting threads
	_ = op.Txn.WaitDone(err, isAbort)
}

// 1PC and 2PC
// dequeuePreparing the commit of 1PC txn and prepare of 2PC txn
// must both enter into this queue for conflict check.
// OpCommit : the commit of 1PC txn
// OpPrepare: the prepare of 2PC txn
// OPRollback:the rollback of 2PC or 1PC
func (mgr *TxnManager) dequeuePreparing(items ...any) {
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		t0 := time.Now()
		// Idempotent check
		if state := op.Txn.GetTxnState(false); state != txnif.TxnStateActive {
			op.Txn.WaitDone(moerr.NewTxnNotActiveNoCtx(txnif.TxnStrState(state)), false)
			continue
		}

		// Mainly do : 1. conflict check for 1PC Commit or 2PC Prepare;
		//   		   2. push the AppendNode into the MVCCHandle of block
		mgr.onPrePrepare(op)

		//Before this moment, all mvcc nodes of a txn has been pushed into the MVCCHandle.
		//1. Allocate a timestamp , set it to txn's prepare timestamp and commit timestamp,
		//   which would be changed in the future if txn is 2PC.
		//2. Set transaction's state to Preparing or Rollbacking if op.Op is OpRollback.
		ts := mgr.onBindPrepareTimeStamp(op)

		if op.Txn.Is2PC() {
			mgr.onPrepare2PC(op, ts)
		} else {
			mgr.onPrepare1PC(op, ts)
		}
		if !op.Txn.IsReplay() {
			if !mgr.prevPrepareTSInPreparing.IsEmpty() {
				if op.Txn.GetPrepareTS().Less(mgr.prevPrepareTSInPreparing) {
					panic(fmt.Sprintf("timestamp rollback current %v, previous %v", op.Txn.GetPrepareTS().ToString(), mgr.prevPrepareTSInPreparing.ToString()))
				}
			}
			mgr.prevPrepareTSInPreparing = op.Txn.GetPrepareTS()
		}

		dequeuePreparingDuration := time.Since(t0)
		_, enable, threshold := trace.IsMOCtledSpan(trace.SpanKindTNRPCHandle)
		if enable && dequeuePreparingDuration > threshold && op.Txn.GetContext() != nil {
			op.Txn.GetStore().SetContext(context.WithValue(op.Txn.GetContext(), common.DequeuePreparing, &common.DurationRecords{Duration: dequeuePreparingDuration}))
		}

		if err := mgr.EnqueueFlushing(op); err != nil {
			panic(err)
		}

		v2.TxnDequeuePreparingDurationHistogram.Observe(time.Since(t0).Seconds())
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debug("[dequeuePreparing]",
			common.NameSpaceField("txns"),
			common.DurationField(time.Since(now)),
			common.CountField(len(items)))
	})
}

func (mgr *TxnManager) onPrepareWAL(items ...any) {
	now := time.Now()

	var t1 time.Time
	var debugT1, debugT2, debugT3 int64

	for _, item := range items {
		t0 := time.Now()
		op := item.(*OpTxn)
		if op.Txn.GetError() == nil && op.Op == OpCommit || op.Op == OpPrepare {
			t1 = time.Now()
			if err := op.Txn.PrepareWAL(); err != nil {
				panic(err)
			}

			dur := time.Since(t1)
			v2.TxnOnPrepareWALPrepareWALDurationHistogram.Observe(dur.Seconds())
			debugT1 = dur.Milliseconds()

			if !op.Txn.IsReplay() {
				if !mgr.prevPrepareTSInPrepareWAL.IsEmpty() {
					if op.Txn.GetPrepareTS().Less(mgr.prevPrepareTSInPrepareWAL) {
						panic(fmt.Sprintf("timestamp rollback current %v, previous %v", op.Txn.GetPrepareTS().ToString(), mgr.prevPrepareTSInPrepareWAL.ToString()))
					}
				}
				mgr.prevPrepareTSInPrepareWAL = op.Txn.GetPrepareTS()
			}
			prepareWALDuration := time.Since(t0)
			_, enable, threshold := trace.IsMOCtledSpan(trace.SpanKindTNRPCHandle)
			if enable && prepareWALDuration > threshold && op.Txn.GetContext() != nil {
				op.Txn.GetStore().SetContext(context.WithValue(op.Txn.GetContext(), common.PrepareWAL, &common.DurationRecords{Duration: prepareWALDuration}))
			}

			t1 = time.Now()
			mgr.CommitListener.OnEndPrepareWAL(op.Txn)

			dur = time.Since(t1)
			v2.TxnOnPrepareWALEndPrepareDurationHistogram.Observe(dur.Seconds())
			debugT2 = dur.Milliseconds()
		}

		t1 = time.Now()
		if _, err := mgr.FlushQueue.Enqueue(op); err != nil {
			panic(err)
		}

		dur := time.Since(t1)
		v2.TxnOnPrepareWALFlushQueueDurationHistogram.Observe(dur.Seconds())
		debugT3 = dur.Milliseconds()

		if total := debugT1 + debugT2 + debugT3; total > time.Second.Milliseconds() {
			logutil.Info(fmt.Sprintf(
				"[onPrepareWAL durations]: total = %d ms; prepare wal = %d; end prepare wal = %d; enqueue flush = %d",
				total, debugT1, debugT2, debugT3))
		}

		v2.TxnOnPrepareWALTotalDurationHistogram.Observe(time.Since(t0).Seconds())
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debug("[prepareWAL]",
			common.NameSpaceField("txns"),
			common.DurationField(time.Since(now)),
			common.CountField(len(items)))
	})
}

// 1PC and 2PC
func (mgr *TxnManager) dequeuePrepared(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		//Notice that WaitPrepared do nothing when op is OpRollback
		t0 := time.Now()
		if err = op.Txn.WaitPrepared(op.ctx); err != nil {
			// v0.6 TODO: Error handling
			panic(err)
		}

		if op.Is2PC() {
			mgr.on2PCPrepared(op)
		} else {
			mgr.on1PCPrepared(op)
		}
		dequeuePreparedDuration := time.Since(t0)
		_, enable, threshold := trace.IsMOCtledSpan(trace.SpanKindTNRPCHandle)
		if enable && dequeuePreparedDuration > threshold && op.Txn.GetContext() != nil {
			op.Txn.GetStore().SetContext(context.WithValue(op.Txn.GetContext(), common.PrepareWAL, &common.DurationRecords{Duration: dequeuePreparedDuration}))
		}
		v2.TxnDequeuePreparedDurationHistogram.Observe(dequeuePreparedDuration.Seconds())
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debug("[dequeuePrepared]",
			common.NameSpaceField("txns"),
			common.CountField(len(items)),
			common.DurationField(time.Since(now)))
	})
}

func (mgr *TxnManager) OnException(new error) {
	old := mgr.Exception.Load()
	for old == nil {
		if mgr.Exception.CompareAndSwap(old, new) {
			break
		}
		old = mgr.Exception.Load()
	}
}

// MinTSForTest is only be used in ut to ensure that
// files that have been gc will not be used.
func (mgr *TxnManager) MinTSForTest() types.TS {
	mgr.RLock()
	defer mgr.RUnlock()
	minTS := types.MaxTs()
	for _, txn := range mgr.IDMap {
		startTS := txn.GetStartTS()
		if startTS.Less(minTS) {
			minTS = startTS
		}
	}
	return minTS
}

func (mgr *TxnManager) Start(ctx context.Context) {
	mgr.FlushQueue.Start()
	mgr.PreparingSM.Start()
	mgr.wg.Add(1)
	go mgr.heartbeat(ctx)
}

func (mgr *TxnManager) Stop() {
	mgr.cancel()
	mgr.wg.Wait()
	mgr.PreparingSM.Stop()
	mgr.FlushQueue.Stop()
	mgr.OnException(sm.ErrClose)
	logutil.Info("[Stop]", TxnMgrField(mgr))
}
