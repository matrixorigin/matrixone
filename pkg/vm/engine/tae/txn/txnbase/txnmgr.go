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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type TxnCommitListener interface {
	OnBeginPrePrepare(*OpTxn)
	OnEndPrePrepare(*OpTxn)
}

type NoopCommitListener struct{}

func (bl *NoopCommitListener) OnBeginPrePrepare(op *OpTxn) {}
func (bl *NoopCommitListener) OnEndPrePrepare(op *OpTxn)   {}

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

func (bl *batchTxnCommitListener) OnBeginPrePrepare(op *OpTxn) {
	for _, l := range bl.listeners {
		l.OnBeginPrePrepare(op)
	}
}

func (bl *batchTxnCommitListener) OnEndPrePrepare(op *OpTxn) {
	for _, l := range bl.listeners {
		l.OnEndPrePrepare(op)
	}
}

type TxnStoreFactory = func() txnif.TxnStore
type TxnFactory = func(*TxnManager, txnif.TxnStore, uint64, types.TS, []byte) txnif.AsyncTxn

type TxnManager struct {
	sync.RWMutex
	common.ClosedState
	PreparingSM     sm.StateMachine
	IDMap           map[uint64]txnif.AsyncTxn
	IdAlloc         *common.IdAlloctor
	TsAlloc         *types.TsAlloctor
	TxnStoreFactory TxnStoreFactory
	TxnFactory      TxnFactory
	Active          *btree.Generic[types.TS]
	Exception       *atomic.Value
	CommitListener  *batchTxnCommitListener
}

func NewTxnManager(txnStoreFactory TxnStoreFactory, txnFactory TxnFactory, clock clock.Clock) *TxnManager {
	if txnFactory == nil {
		txnFactory = DefaultTxnFactory
	}
	mgr := &TxnManager{
		IDMap:           make(map[uint64]txnif.AsyncTxn),
		IdAlloc:         common.NewIdAlloctor(1),
		TsAlloc:         types.NewTsAlloctor(clock),
		TxnStoreFactory: txnStoreFactory,
		TxnFactory:      txnFactory,
		Active: btree.NewGeneric(func(a, b types.TS) bool {
			return a.Less(b)
		}),
		Exception:      new(atomic.Value),
		CommitListener: newBatchCommitListener(),
	}
	pqueue := sm.NewSafeQueue(20000, 1000, mgr.dequeuePreparing)
	fqueue := sm.NewSafeQueue(20000, 1000, mgr.dequeuePrepared)
	mgr.PreparingSM = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, fqueue)

	return mgr
}

func (mgr *TxnManager) Init(prevTxnId uint64, prevTs types.TS) error {
	mgr.IdAlloc.SetStart(prevTxnId)
	mgr.TsAlloc.SetStart(prevTs)
	logutil.Info("[INIT]", TxnMgrField(mgr))
	return nil
}

func (mgr *TxnManager) StatActiveTxnCnt() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.Active.Len()
}

func (mgr *TxnManager) StatSafeTS() (ts types.TS) {
	mgr.RLock()
	if len(mgr.IDMap) > 0 {
		ts, _ = mgr.Active.Min()
		ts = ts.Prev()
	} else {
		//ts = mgr.TsAlloc.Get()
		ts = mgr.TsAlloc.Alloc()
	}
	mgr.RUnlock()
	return
}

func (mgr *TxnManager) StatMaxCommitTS() (ts types.TS) {
	mgr.RLock()
	ts = mgr.TsAlloc.Alloc()
	mgr.RUnlock()
	return
}

func (mgr *TxnManager) StartTxn(info []byte) (txn txnif.AsyncTxn, err error) {
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
	txn = mgr.TxnFactory(mgr, store, txnId, startTs, info)
	store.BindTxn(txn)
	mgr.IDMap[txnId] = txn
	//mgr.ActiveMask.Add(startTs)
	mgr.Active.Set(startTs)
	return
}

func (mgr *TxnManager) DeleteTxn(id uint64) (err error) {
	mgr.Lock()
	defer mgr.Unlock()
	txn := mgr.IDMap[id]
	if txn == nil {
		err = moerr.NewTxnNotFound()
		logutil.Warnf("Txn %d not found", id)
		return
	}
	delete(mgr.IDMap, id)
	mgr.Active.Delete(txn.GetStartTS())
	return
}

func (mgr *TxnManager) GetTxnByCtx(ctx []byte) txnif.AsyncTxn {
	return mgr.GetTxn(IDCtxToID(ctx))
}

func (mgr *TxnManager) GetTxn(id uint64) txnif.AsyncTxn {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.IDMap[id]
}

func (mgr *TxnManager) EnqueueFlushing(op any) (err error) {
	_, err = mgr.PreparingSM.EnqueueCheckpoint(op)
	return
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

	mgr.CommitListener.OnBeginPrePrepare(op)
	defer mgr.CommitListener.OnEndPrePrepare(op)
	// If txn is trying committing, call txn.PrePrepare()
	now := time.Now()
	op.Txn.SetError(op.Txn.PrePrepare())
	logutil.Debug("[PrePrepare]", TxnField(op.Txn), common.DurationField(time.Since(now)))
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
	mgr.Lock()
	defer mgr.Unlock()

	ts = mgr.TsAlloc.Alloc()

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
	mgr.onPreparCommit(op.Txn)
	if op.Txn.GetError() != nil {
		op.Op = OpRollback
		op.Txn.Lock()
		// Should not fail here
		_ = op.Txn.ToRollbackingLocked(ts)
		op.Txn.Unlock()
		mgr.onPreparRollback(op.Txn)
	} else {
		//1.  Appending the data into appendableNode of block
		// 2. Collect redo log,append into WalDriver
		//TODO::need to handle the error,instead of panic for simplicity
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

	// Here only notify the user txn have been done with err.
	// The err returned can be access via op.Txn.GetError()
	_ = op.Txn.WaitDone(err, isAbort)
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

		// Idempotent check
		if state := op.Txn.GetTxnState(false); state != txnif.TxnStateActive {
			op.Txn.WaitDone(moerr.NewTxnNotActive(txnif.TxnStrState(state)), false)
			continue
		}

		// Mainly do conflict check for 1PC Commit or 2PC Prepare
		mgr.onPrePrepare(op)

		//Before this moment, all mvcc nodes of a txn has been pushed into the MVCCHandle.
		ts := mgr.onBindPrepareTimeStamp(op)

		if op.Txn.Is2PC() {
			mgr.onPrepare2PC(op, ts)
		} else {
			mgr.onPrepare1PC(op, ts)
		}

		if err := mgr.EnqueueFlushing(op); err != nil {
			panic(err)
		}
	}
	logutil.Debug("[dequeuePreparing]",
		common.NameSpaceField("txns"),
		common.DurationField(time.Since(now)),
		common.CountField(len(items)))
}

// 1PC and 2PC
func (mgr *TxnManager) dequeuePrepared(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)

		if err = op.Txn.WaitPrepared(); err != nil {
			// v0.6 TODO: Error handling
			panic(err)
		}

		if op.Is2PC() {
			mgr.on2PCPrepared(op)
		} else {
			mgr.on1PCPrepared(op)
		}
	}
	logutil.Debug("[dequeuePrepared]",
		common.NameSpaceField("txns"),
		common.CountField(len(items)),
		common.DurationField(time.Since(now)))
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

func (mgr *TxnManager) Start() {
	mgr.PreparingSM.Start()
}

func (mgr *TxnManager) Stop() {
	mgr.PreparingSM.Stop()
	mgr.OnException(common.ErrClose)
	logutil.Info("[Stop]", TxnMgrField(mgr))
}
