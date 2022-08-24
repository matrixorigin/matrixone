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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/tidwall/btree"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type TxnStoreFactory = func() txnif.TxnStore
type TxnFactory = func(*TxnManager, txnif.TxnStore, uint64, types.TS, []byte) txnif.AsyncTxn

type TxnManager struct {
	sync.RWMutex
	common.ClosedState
	PreparingSM sm.StateMachine
	//Notice that prepared transactions would be enqueued into
	//the receiveQueue of CommittingSM at run time or replay time.
	CommittingSM    sm.StateMachine
	IDMap           map[uint64]txnif.AsyncTxn
	IdAlloc         *common.IdAlloctor
	TsAlloc         *types.TsAlloctor
	TxnStoreFactory TxnStoreFactory
	TxnFactory      TxnFactory
	Active          *btree.Generic[types.TS]
	Exception       *atomic.Value
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
		Active: btree.NewGeneric[types.TS](func(a, b types.TS) bool {
			return a.Less(b)
		}),
		Exception: new(atomic.Value)}
	pqueue := sm.NewSafeQueue(20000, 1000, mgr.onPreparing)
	fqueue := sm.NewSafeQueue(20000, 1000, mgr.onFlushing)
	mgr.PreparingSM = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, fqueue)

	cqueue := sm.NewSafeQueue(20000, 1000, mgr.onCommitting)
	cfqueue := sm.NewSafeQueue(20000, 1000, mgr.onCFlushing)
	mgr.CommittingSM = sm.NewStateMachine(new(sync.WaitGroup), mgr, cqueue, cfqueue)
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

func (mgr *TxnManager) DeleteTxn(id uint64) {
	mgr.Lock()
	defer mgr.Unlock()
	txn := mgr.IDMap[id]
	delete(mgr.IDMap, id)
	mgr.Active.Delete(txn.GetStartTS())
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

func (mgr *TxnManager) EnqueueCommitting(op any) (err error) {
	_, err = mgr.CommittingSM.EnqueueRecevied(op)
	return
}

func (mgr *TxnManager) EnqueueCFlushing(op any) (err error) {
	_, err = mgr.CommittingSM.EnqueueCheckpoint(op)
	return
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) (err error) {
	_, err = mgr.PreparingSM.EnqueueRecevied(op)
	return
}

func (mgr *TxnManager) onPreCommitOr2PCPrepare(txn txnif.AsyncTxn) {
	now := time.Now()
	txn.SetError(txn.PreCommitOr2PCPrepare())
	logutil.Debug("[PreCommit]", TxnField(txn), common.DurationField(time.Since(now)))
}

func (mgr *TxnManager) onPreparCommit(txn txnif.AsyncTxn) {
	txn.SetError(txn.PrepareCommit())
}

func (mgr *TxnManager) onPrepar2PCPrepare(txn txnif.AsyncTxn) {
	txn.SetError(txn.Prepare2PCPrepare())
}

func (mgr *TxnManager) onPreApplyCommit(txn txnif.AsyncTxn) {
	if err := txn.PreApplyCommit(); err != nil {
		txn.SetError(err)
		mgr.OnException(err)
	}
}

func (mgr *TxnManager) onPreApply2PCPrepare(txn txnif.AsyncTxn) {
	if err := txn.PreApplyCommit(); err != nil {
		txn.SetError(err)
		mgr.OnException(err)
	}
}

func (mgr *TxnManager) onPreparRollback(txn txnif.AsyncTxn) {
	_ = txn.PrepareRollback()
}

func (mgr *TxnManager) onPreparing(items ...any) {
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		if op.Op == OpCommit || op.Op == OpPrepare {
			//conflict check for 1PC Commit or 2PC Prepare
			mgr.onPreCommitOr2PCPrepare(op.Txn)
		}
		mgr.Lock()
		ts := mgr.TsAlloc.Alloc()
		op.Txn.Lock()
		if op.Txn.GetError() != nil {
			op.Op = OpRollback
		}
		if op.Op == OpCommit {
			// Should not fail here
			_ = op.Txn.ToCommittingLocked(ts)
		} else if op.Op == OpRollback {
			// Should not fail here
			_ = op.Txn.ToRollbackingLocked(ts)
		} else if op.Op == OpPrepare {
			_ = op.Txn.ToPreparingLocked(ts)
		}
		op.Txn.Unlock()
		mgr.Unlock()
		//for 1PC Commit
		if op.Op == OpCommit {
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
			//for 2PC Prepare
		} else if op.Op == OpPrepare {
			mgr.onPrepar2PCPrepare(op.Txn)
			if op.Txn.GetError() != nil {
				op.Op = OpRollback
				op.Txn.Lock()
				// Should not fail here
				_ = op.Txn.ToRollbackingLocked(ts)
				op.Txn.Unlock()
				mgr.onPreparRollback(op.Txn)
			} else {
				//1.Appending the data into appendableNode of block
				// 2. Collect redo log,append into WalDriver
				//TODO::need to handle the error,instead of panic for simplicity
				mgr.onPreApply2PCPrepare(op.Txn)
				if op.Txn.GetError() != nil {
					panic(op.Txn.GetID())
				}
			}
		} else {
			//for 1PC or 2PC Rollback
			mgr.onPreparRollback(op.Txn)
		}
		if err := mgr.EnqueueFlushing(op); err != nil {
			panic(err)
		}
	}
	logutil.Debug("[onPreparing]",
		common.NameSpaceField("txns"),
		common.DurationField(time.Since(now)),
		common.CountField(len(items)))
}

func (mgr *TxnManager) onFlushing(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		switch op.Op {
		//for 1PC Commit
		case OpCommit:
			if err = op.Txn.ApplyCommit(); err != nil {
				panic(err)
			}
		//for 2PC Prepare
		case OpPrepare:
			//wait for redo log synced.
			if err = op.Txn.Apply2PCPrepare(); err != nil {
				panic(err)
			}
			err = mgr.EnqueueCommitting(&OpTxn{
				Txn: op.Txn,
				Op:  OpInvalid,
			})
			if err != nil {
				panic(err)
			}
		//for 1PC or 2PC Rollback
		case OpRollback:
			if err = op.Txn.ApplyRollback(); err != nil {
				mgr.OnException(err)
				logutil.Warn("[ApplyRollback]", TxnField(op.Txn), common.ErrorField(err))
			}
		}
		// Here only notify the user txn have been done with err.
		// The err returned can be access via op.Txn.GetError()
		_ = op.Txn.WaitDone(err)
	}
	logutil.Debug("[onFlushing]",
		common.NameSpaceField("txns"),
		common.CountField(len(items)),
		common.DurationField(time.Since(now)))
}

// wait for committing, commit, rollback events of 2PC distributed transactions
func (mgr *TxnManager) onCommitting(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(OpTxn)
		ev := op.Txn.Event()
		switch ev {
		//TODO:1. Set the commit ts passed by the Coordinator.
		//     2. Apply Commit and append a committing log.
		case EventCommitting:
			//txn.LogTxnEntry()

			op.Op = OpCommitting
		//TODO:1. Set the commit ts passed by the Coordinator.
		//     2. Apply Commit and append a commit log
		case EventCommit:
			//txn.LogTxnEntry
			op.Op = OpCommit
		case EventRollback:
			pts := op.Txn.GetPrepareTS()
			op.Txn.Lock()
			//FIXME::set commit ts of a rollbacking transaction to its prepare ts?
			_ = op.Txn.ToRollbackingLocked(pts)
			op.Txn.Unlock()
			op.Op = OpRollback
			//_ = op.Txn.ApplyRollback()
			//_ = op.Txn.WaitDone(err)

		}
		err = mgr.EnqueueCFlushing(op)
		if err != nil {
			panic(err)
		}
	}
	logutil.Debug("[onCommitting]",
		common.NameSpaceField("txns"),
		common.CountField(len(items)),
		common.DurationField(time.Since(now)))
}

func (mgr *TxnManager) onCFlushing(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(OpTxn)
		switch op.Op {
		//TODO::wait for commit log synced
		case OpCommit:

		//TODO:wait for committing log synced.
		case OpCommitting:

		case OpRollback:
			//Notice that can't call op.Txn.PrepareRollback here,
			//since data had been appended into the appendableNode of block
			//_ = op.Txn.PrepareRollback()
			_ = op.Txn.ApplyRollback()
		}
		_ = op.Txn.WaitDone(err)
	}
	logutil.Debug("[onCFlushing]",
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
	mgr.CommittingSM.Start()
	mgr.PreparingSM.Start()
}

func (mgr *TxnManager) Stop() {
	mgr.PreparingSM.Stop()
	mgr.CommittingSM.Stop()
	mgr.OnException(common.ErrClose)
	logutil.Info("[Stop]", TxnMgrField(mgr))
}
