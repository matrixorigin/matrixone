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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type TxnStoreFactory = func() txnif.TxnStore
type TxnFactory = func(*TxnManager, txnif.TxnStore, uint64, uint64, []byte) txnif.AsyncTxn

type TxnManager struct {
	sync.RWMutex
	common.ClosedState
	sm.StateMachine
	Active           map[uint64]txnif.AsyncTxn
	IdAlloc, TsAlloc *common.IdAlloctor
	TxnStoreFactory  TxnStoreFactory
	TxnFactory       TxnFactory
	ActiveMask       *roaring64.Bitmap
	Exception        *atomic.Value
}

func NewTxnManager(txnStoreFactory TxnStoreFactory, txnFactory TxnFactory) *TxnManager {
	if txnFactory == nil {
		txnFactory = DefaultTxnFactory
	}
	mgr := &TxnManager{
		Active:          make(map[uint64]txnif.AsyncTxn),
		IdAlloc:         common.NewIdAlloctor(1),
		TsAlloc:         common.NewIdAlloctor(1),
		TxnStoreFactory: txnStoreFactory,
		TxnFactory:      txnFactory,
		ActiveMask:      roaring64.New(),
		Exception:       new(atomic.Value),
	}
	pqueue := sm.NewSafeQueue(20000, 1000, mgr.onPreparing)
	cqueue := sm.NewSafeQueue(20000, 1000, mgr.onCommit)
	mgr.StateMachine = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, cqueue)
	return mgr
}

func (mgr *TxnManager) Init(prevTxnId uint64, prevTs uint64) error {
	mgr.IdAlloc.SetStart(prevTxnId)
	mgr.TsAlloc.SetStart(prevTs)
	logutil.Info("[INIT]", TxnMgrField(mgr))
	return nil
}

func (mgr *TxnManager) StatActiveTxnCnt() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return int(mgr.ActiveMask.GetCardinality())
}

func (mgr *TxnManager) StatSafeTS() (ts uint64) {
	mgr.RLock()
	if len(mgr.Active) > 0 {
		ts = mgr.ActiveMask.Minimum() - 1
	} else {
		ts = mgr.TsAlloc.Get()
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
	mgr.Active[txnId] = txn
	mgr.ActiveMask.Add(startTs)
	return
}

func (mgr *TxnManager) DeleteTxn(id uint64) {
	mgr.Lock()
	defer mgr.Unlock()
	txn := mgr.Active[id]
	delete(mgr.Active, id)
	mgr.ActiveMask.Remove(txn.GetStartTS())
}

func (mgr *TxnManager) GetTxnByCtx(ctx []byte) txnif.AsyncTxn {
	return mgr.GetTxn(IDCtxToID(ctx))
}

func (mgr *TxnManager) GetTxn(id uint64) txnif.AsyncTxn {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.Active[id]
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) (err error) {
	_, err = mgr.EnqueueRecevied(op)
	return
}

func (mgr *TxnManager) onPreCommit(txn txnif.AsyncTxn) {
	now := time.Now()
	txn.SetError(txn.PreCommit())
	logutil.Debug("[PreCommit]", TxnField(txn), common.DurationField(time.Since(now)))
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

// TODO
func (mgr *TxnManager) onPreparing(items ...any) {
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		if op.Op == OpCommit {
			mgr.onPreCommit(op.Txn)
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
		}
		op.Txn.Unlock()
		mgr.Unlock()
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
				mgr.onPreApplyCommit(op.Txn)
			}
		} else {
			mgr.onPreparRollback(op.Txn)
		}
		if _, err := mgr.EnqueueCheckpoint(op); err != nil {
			panic(err)
		}
	}
	logutil.Debug("[PrepareCommit]",
		common.NameSpaceField("txns"),
		common.DurationField(time.Since(now)),
		common.CountField(len(items)))
}

// TODO
func (mgr *TxnManager) onCommit(items ...any) {
	var err error
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		switch op.Op {
		case OpCommit:
			if err = op.Txn.ApplyCommit(); err != nil {
				mgr.OnException(err)
				logutil.Warn("[ApplyCommit]", TxnField(op.Txn), common.ErrorField(err))
			}
		case OpRollback:
			if err = op.Txn.ApplyRollback(); err != nil {
				mgr.OnException(err)
				logutil.Warn("[ApplyRollback]", TxnField(op.Txn), common.ErrorField(err))
			}
		}
		// Here only wait the txn to be done. The err returned can be access via op.Txn.GetError()
		_ = op.Txn.WaitDone(err)
	}
	logutil.Debug("[Commit]",
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

func (mgr *TxnManager) Stop() {
	mgr.StateMachine.Stop()
	mgr.OnException(common.ClosedErr)
	logutil.Info("[Stop]", TxnMgrField(mgr))
}
