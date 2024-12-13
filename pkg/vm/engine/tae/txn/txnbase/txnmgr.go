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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type TxnManagerOption func(*TxnManager)

// WithTxnSkipFlag set the TxnSkipFlag
// 0 or TxnSkipFlag_None: skip nothing
// TxnFlag_Normal: skip normal txn
// TxnFlag_Replay|TxnFlag_Normal: skip normal and replay txn
// TxnFlag_Heartbeat|TxnFlag_Normal|TxnFlag_Replay or TxnSkipFlag_All: skip all txn
func WithTxnSkipFlag(flag TxnFlag) TxnManagerOption {
	return func(m *TxnManager) {
		prevFlag := TxnFlag(m.txns.skipFlags.Load())
		m.txns.skipFlags.Store(uint64(flag))
		logutil.Info(
			"TxnManager-TxnSkipFlag-Change",
			zap.String("prev", prevFlag.String()),
			zap.String("current", flag.String()),
		)
	}
}

// Here define the write mode:
// TxnSkipFlag_None: skip nothing
func WithWriteMode(mgr *TxnManager) {
	WithTxnSkipFlag(TxnSkipFlag_None)(mgr)
}

// Here define the replay mode:
// TxnFlag_Normal|TxnFlag_Heartbeat: skip normal and heartbeat txn
func WithReplayMode(mgr *TxnManager) {
	WithTxnSkipFlag(TxnFlag_Normal | TxnFlag_Heartbeat)(mgr)
}

// Here define the readonly mode:
// TxnFlag_Normal|TxnFlag_Heartbeat|TxnFlag_Replay: skip all txn
func WithReadonlyMode(mgr *TxnManager) {
	WithTxnSkipFlag(TxnFlag_Normal | TxnFlag_Heartbeat | TxnFlag_Replay)(mgr)
}

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
	sm.ClosedState
	PreparingSM     sm.StateMachine
	FlushQueue      sm.Queue
	IdAlloc         *common.TxnIDAllocator
	MaxCommittedTS  atomic.Pointer[types.TS]
	TxnStoreFactory TxnStoreFactory
	TxnFactory      TxnFactory
	Exception       *atomic.Value
	CommitListener  *batchTxnCommitListener
	workers         *ants.Pool

	heartbeatJob atomic.Pointer[tasks.CancelableJob]

	txns struct {
		// store all txns
		store *sync.Map

		// wg is used to wait all txns to be done
		wg sync.WaitGroup

		// TxnSkipFlag to skip some txn type
		// 0: skip nothing
		// TxnFlag_Normal: skip normal txn
		// TxnFlag_Replay: skip replay txn
		// TxnFlag_Heartbeat: skip heartbeat txn
		// TxnFlag_Normal | TxnFlag_Replay: skip normal and replay txn
		skipFlags atomic.Uint64
	}

	ts struct {
		mu        sync.Mutex
		allocator *types.TsAlloctor
	}

	// for debug
	prevPrepareTS             types.TS
	prevPrepareTSInPreparing  types.TS
	prevPrepareTSInPrepareWAL types.TS
}

func NewTxnManager(
	txnStoreFactory TxnStoreFactory,
	txnFactory TxnFactory,
	clock clock.Clock,
	opts ...TxnManagerOption,
) *TxnManager {
	if txnFactory == nil {
		txnFactory = DefaultTxnFactory
	}
	mgr := &TxnManager{
		IdAlloc:         common.NewTxnIDAllocator(),
		TxnStoreFactory: txnStoreFactory,
		TxnFactory:      txnFactory,
		Exception:       new(atomic.Value),
		CommitListener:  newBatchCommitListener(),
	}
	mgr.txns.store = new(sync.Map)
	mgr.txns.wg = sync.WaitGroup{}
	for _, opt := range opts {
		opt(mgr)
	}
	mgr.ts.allocator = types.NewTsAlloctor(clock)
	mgr.initMaxCommittedTS()
	pqueue := sm.NewSafeQueue(20000, 1000, mgr.dequeuePreparing)
	prepareWALQueue := sm.NewSafeQueue(20000, 1000, mgr.onPrepareWAL)
	mgr.FlushQueue = sm.NewSafeQueue(20000, 1000, mgr.dequeuePrepared)
	mgr.PreparingSM = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, prepareWALQueue)

	mgr.workers, _ = ants.NewPool(runtime.GOMAXPROCS(0))
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
	mgr.ts.mu.Lock()
	defer mgr.ts.mu.Unlock()
	return mgr.ts.allocator.Alloc()
}

func (mgr *TxnManager) ToWriteMode() {
	WithWriteMode(mgr)
	mgr.ResetHeartbeat()
}

func (mgr *TxnManager) IsReplayMode() bool {
	skipFlags := mgr.GetTxnSkipFlags()
	if skipFlags&TxnFlag_Replay == 0 && skipFlags&TxnFlag_Normal != 0 && skipFlags&TxnFlag_Heartbeat != 0 {
		return true
	}
	return false
}

func (mgr *TxnManager) IsWriteMode() bool {
	skipFlags := mgr.GetTxnSkipFlags()
	return skipFlags == TxnSkipFlag_None
}

// it is only safe to call this function in the readonly mode
func (mgr *TxnManager) ToReplayMode() {
	WithReplayMode(mgr)
}

func (mgr *TxnManager) SwitchToReadonly(ctx context.Context) (err error) {
	now := time.Now()
	defer func() {
		logutil.Info(
			"Wait-TxnManager-To-ReplayMode",
			zap.Duration("duration", time.Since(now)),
		)
	}()

	// 1. do not accept new txn
	WithReadonlyMode(mgr)

	// 2. try to abort slow txn: big-tombstone-txn and merge-txn
	mgr.txns.store.Range(func(key, value any) bool {
		// TODO
		return true
	})

	// 3. wait all txn to be done.
	// Note:
	// the heartbeats may be still running. The controller
	// should wait all logtail to be flushed
	err = mgr.WaitEmpty(ctx)
	return
}

func (mgr *TxnManager) GetTxnSkipFlags() TxnSkipFlag {
	return TxnSkipFlag(mgr.txns.skipFlags.Load())
}

func (mgr *TxnManager) Init(prevTs types.TS) error {
	logutil.Info(
		"TxnManager-Init",
		zap.String("prev-ts", prevTs.ToString()),
	)
	mgr.ts.allocator.SetStart(prevTs)
	return nil
}

// Note: Replay should always runs in a single thread
func (mgr *TxnManager) OnReplayTxn(txn txnif.AsyncTxn) (err error) {
	mgr.storeTxn(txn, TxnFlag_Replay)
	return
}

// StartTxn starts a local transaction initiated by DN
func (mgr *TxnManager) StartTxn(info []byte) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	txnId := mgr.IdAlloc.Alloc()
	startTs := *mgr.MaxCommittedTS.Load()

	store := mgr.TxnStoreFactory()
	txn = mgr.TxnFactory(mgr, store, txnId, startTs, types.TS{})
	store.BindTxn(txn)
	mgr.storeTxn(txn, TxnFlag_Normal)
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
	store := mgr.TxnStoreFactory()
	txnId := mgr.IdAlloc.Alloc()
	txn = mgr.TxnFactory(mgr, store, txnId, startTS, snapshotTS)
	store.BindTxn(txn)
	err = mgr.storeTxn(txn, TxnFlag_Normal)
	return
}

func (mgr *TxnManager) WaitEmpty(ctx context.Context) (err error) {
	c := make(chan struct{})
	go func() {
		mgr.txns.wg.Wait()
		close(c)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c:
		return
	}
}

func (mgr *TxnManager) loadTxn(
	id string,
) (txnif.AsyncTxn, bool) {
	if res, ok := mgr.txns.store.Load(id); ok {
		return res.(txnif.AsyncTxn), true
	}
	return nil, false
}

func (mgr *TxnManager) loadAndDeleteTxn(
	id string,
) (txnif.AsyncTxn, bool) {
	if res, ok := mgr.txns.store.LoadAndDelete(id); ok {
		mgr.txns.wg.Done()
		return res.(txnif.AsyncTxn), true
	}
	return nil, false
}

// flag: specify the txn type. only one bit is set
func (mgr *TxnManager) storeTxn(
	newTxn txnif.AsyncTxn, flag TxnFlag,
) (err error) {
	mgr.txns.wg.Add(1)

	skipFlags := TxnSkipFlag(mgr.txns.skipFlags.Load())
	if skipFlags.Skip(flag) {
		mgr.txns.wg.Done()
		return moerr.NewTxnControlErrorNoCtxf(
			"%s Skip %s",
			skipFlags.String(),
			flag.String(),
		)
	}

	mgr.txns.store.Store(newTxn.GetID(), newTxn)
	return
}

// flag: specify the txn type. only one bit is set
func (mgr *TxnManager) loadOrStoreTxn(
	newTxn txnif.AsyncTxn, flag TxnFlag,
) (txnif.AsyncTxn, bool, error) {
	mgr.txns.wg.Add(1)

	skipFlags := TxnSkipFlag(mgr.txns.skipFlags.Load())
	if skipFlags.Skip(flag) {
		mgr.txns.wg.Done()
		return nil, false, moerr.NewTxnControlErrorNoCtxf(
			"%s Skip %s",
			skipFlags.String(),
			flag.String(),
		)
	}

	if actual, loaded := mgr.txns.store.LoadOrStore(
		newTxn.GetID(), newTxn,
	); loaded {
		mgr.txns.wg.Done()
		return actual.(txnif.AsyncTxn), true, nil
	}
	return newTxn, false, nil
}

// GetOrCreateTxnWithMeta Get or create a txn initiated by CN
func (mgr *TxnManager) GetOrCreateTxnWithMeta(
	info []byte, id []byte, ts types.TS,
) (txn txnif.AsyncTxn, err error) {
	if exp := mgr.Exception.Load(); exp != nil {
		err = exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return
	}
	var ok bool
	if txn, ok = mgr.loadTxn(util.UnsafeBytesToString(id)); ok {
		return
	}

	store := mgr.TxnStoreFactory()
	txn = mgr.TxnFactory(mgr, store, id, ts, ts)
	store.BindTxn(txn)
	txn, _, err = mgr.loadOrStoreTxn(txn, TxnFlag_Normal)
	return
}

func (mgr *TxnManager) DeleteTxn(id string) (err error) {
	if _, ok := mgr.loadAndDeleteTxn(id); !ok {
		err = moerr.NewTxnNotFoundNoCtx()
	}
	if err != nil {
		logutil.Warn(
			"DeleteTxnError",
			zap.String("txn", id),
			zap.Error(err),
		)
	}
	return
}

func (mgr *TxnManager) GetTxnByCtx(ctx []byte) txnif.AsyncTxn {
	return mgr.GetTxn(IDCtxToID(ctx))
}

func (mgr *TxnManager) GetTxn(id string) txnif.AsyncTxn {
	res, ok := mgr.loadTxn(id)
	if !ok || res == nil {
		return nil
	}
	return res
}

func (mgr *TxnManager) EnqueueFlushing(op any) (err error) {
	_, err = mgr.PreparingSM.EnqueueCheckpoint(op)
	return
}

func (mgr *TxnManager) newHeartbeatOpTxn(ctx context.Context) *OpTxn {
	if exp := mgr.Exception.Load(); exp != nil {
		err := exp.(error)
		logutil.Warnf("StartTxn: %v", err)
		return nil
	}
	startTs := mgr.Now()
	txnId := mgr.IdAlloc.Alloc()
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
	_, err = mgr.PreparingSM.EnqueueReceived(op)
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

	mgr.ts.mu.Lock()
	defer mgr.ts.mu.Unlock()

	ts = mgr.ts.allocator.Alloc()
	if !mgr.prevPrepareTS.IsEmpty() {
		if ts.LT(&mgr.prevPrepareTS) {
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
	new := txn.GetCommitTS()
	for old := mgr.MaxCommittedTS.Load(); new.GT(old); old = mgr.MaxCommittedTS.Load() {
		if mgr.MaxCommittedTS.CompareAndSwap(old, &new) {
			return
		}
	}
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
		store := op.Txn.GetStore()
		store.TriggerTrace(txnif.TracePreparing)

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
				prepareTS := op.Txn.GetPrepareTS()
				if prepareTS.LT(&mgr.prevPrepareTSInPreparing) {
					panic(fmt.Sprintf("timestamp rollback current %v, previous %v", op.Txn.GetPrepareTS().ToString(), mgr.prevPrepareTSInPreparing.ToString()))
				}
			}
			mgr.prevPrepareTSInPreparing = op.Txn.GetPrepareTS()
		}

		store.TriggerTrace(txnif.TracePrepareWalWait)
		if err := mgr.EnqueueFlushing(op); err != nil {
			panic(err)
		}
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

	for _, item := range items {
		op := item.(*OpTxn)
		store := op.Txn.GetStore()
		store.TriggerTrace(txnif.TracePrepareWal)
		var t1, t2, t3, t4, t5 time.Time
		t1 = time.Now()
		if op.Txn.GetError() == nil && op.Op == OpCommit || op.Op == OpPrepare {
			if err := op.Txn.PrepareWAL(); err != nil {
				panic(err)
			}

			t2 = time.Now()

			if !op.Txn.IsReplay() {
				if !mgr.prevPrepareTSInPrepareWAL.IsEmpty() {
					prepareTS := op.Txn.GetPrepareTS()
					if prepareTS.LT(&mgr.prevPrepareTSInPrepareWAL) {
						panic(fmt.Sprintf("timestamp rollback current %v, previous %v", op.Txn.GetPrepareTS().ToString(), mgr.prevPrepareTSInPrepareWAL.ToString()))
					}
				}
				mgr.prevPrepareTSInPrepareWAL = op.Txn.GetPrepareTS()
			}

			mgr.CommitListener.OnEndPrepareWAL(op.Txn)
			t3 = time.Now()
		}

		t4 = time.Now()
		store.TriggerTrace(txnif.TracePreapredWait)
		if _, err := mgr.FlushQueue.Enqueue(op); err != nil {
			panic(err)
		}
		t5 = time.Now()

		if t5.Sub(t1) > time.Second {
			logutil.Warn(
				"SLOW-LOG",
				zap.String("txn", op.Txn.String()),
				zap.Duration("prepare-wal-duration", t2.Sub(t1)),
				zap.Duration("end-prepare-duration", t3.Sub(t2)),
				zap.Duration("enqueue-flush-duration", t5.Sub(t4)),
			)
		}
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
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		store := op.Txn.GetStore()
		store.TriggerTrace(txnif.TracePrepared)
		mgr.workers.Submit(func() {
			//Notice that WaitPrepared do nothing when op is OpRollback
			if err := op.Txn.WaitPrepared(op.ctx); err != nil {
				// v0.6 TODO: Error handling
				panic(err)
			}

			if op.Is2PC() {
				mgr.on2PCPrepared(op)
			} else {
				mgr.on1PCPrepared(op)
			}
		})
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
	minTS := types.MaxTs()
	mgr.txns.store.Range(func(key, value any) bool {
		txn := value.(txnif.AsyncTxn)
		startTS := txn.GetStartTS()
		if startTS.LT(&minTS) {
			minTS = startTS
		}
		return true
	})
	return minTS
}

func (mgr *TxnManager) StopHeartbeat() {
	old := mgr.heartbeatJob.Load()
	if old == nil {
		return
	}
	old.Stop()
	for swapped := mgr.heartbeatJob.CompareAndSwap(old, nil); !swapped; {
		if old = mgr.heartbeatJob.Load(); old != nil {
			old.Stop()
		}
	}
}

func (mgr *TxnManager) ResetHeartbeat() {
	old := mgr.heartbeatJob.Load()
	if old != nil {
		old.Stop()
	}
	newJob := tasks.NewCancelableCronJob(
		"TxnManager-HB",
		time.Millisecond*2,
		func(ctx context.Context) {
			op := mgr.newHeartbeatOpTxn(ctx)
			op.Txn.(*Txn).Add(1)
			if err := mgr.OnOpTxn(op); err != nil {
				logutil.Error(
					"TxnManager-HB-Error",
					zap.Error(err),
				)
			}
		},
		true,
		1,
	)
	for swapped := mgr.heartbeatJob.CompareAndSwap(old, newJob); !swapped; {
		if old = mgr.heartbeatJob.Load(); old != nil {
			old.Stop()
		}
	}
	newJob.Start()
}

func (mgr *TxnManager) Start(ctx context.Context) {
	isReplayMode := mgr.IsReplayMode()
	isWriteMode := mgr.IsWriteMode()
	mgr.FlushQueue.Start()
	mgr.PreparingSM.Start()
	mgr.ResetHeartbeat()
	logutil.Info(
		"TxnManager-Started",
		zap.Bool("is-replay-mode", isReplayMode),
		zap.Bool("is-write-mode", isWriteMode),
	)
}

func (mgr *TxnManager) Stop() {
	isReplayMode := mgr.IsReplayMode()
	isWriteMode := mgr.IsWriteMode()
	mgr.StopHeartbeat()
	mgr.PreparingSM.Stop()
	mgr.FlushQueue.Stop()
	mgr.OnException(sm.ErrClose)
	mgr.workers.Release()
	logutil.Info(
		"TxnManager-Stopped",
		zap.Bool("is-replay-mode", isReplayMode),
		zap.Bool("is-write-mode", isWriteMode),
	)
}
