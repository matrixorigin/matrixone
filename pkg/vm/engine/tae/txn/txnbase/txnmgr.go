package txnbase

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/sirupsen/logrus"
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
	}
	pqueue := sm.NewSafeQueue(10000, 200, mgr.onPreparing)
	cqueue := sm.NewSafeQueue(10000, 200, mgr.onCommit)
	mgr.StateMachine = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, cqueue)
	return mgr
}

func (mgr *TxnManager) Init(prevTxnId uint64, prevTs uint64) error {
	mgr.IdAlloc.SetStart(prevTxnId)
	mgr.TsAlloc.SetStart(prevTs)
	return nil
}

func (mgr *TxnManager) StartTxn(info []byte) txnif.AsyncTxn {
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := mgr.TsAlloc.Alloc()

	store := mgr.TxnStoreFactory()
	txn := mgr.TxnFactory(mgr, store, txnId, startTs, info)
	store.BindTxn(txn)
	mgr.Active[txnId] = txn
	return txn
}

func (mgr *TxnManager) DeleteTxn(id uint64) {
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.Active, id)
}

func (mgr *TxnManager) GetTxn(id uint64) txnif.AsyncTxn {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.Active[id]
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) {
	mgr.EnqueueRecevied(op)
}

func (mgr *TxnManager) onPreCommit(txn txnif.AsyncTxn) {
	now := time.Now()
	txn.SetError(txn.PreCommit())
	logrus.Debugf("%s PreCommit Takes: %s", txn.String(), time.Since(now))
}

func (mgr *TxnManager) onPreparCommit(txn txnif.AsyncTxn) {
	txn.SetError(txn.PrepareCommit())
}

func (mgr *TxnManager) onPreparRollback(txn txnif.AsyncTxn) {
	txn.PrepareRollback()
}

// TODO
func (mgr *TxnManager) onPreparing(items ...interface{}) {
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
			op.Txn.ToCommittingLocked(ts)
		} else if op.Op == OpRollback {
			op.Txn.ToRollbackingLocked(ts)
		}
		op.Txn.Unlock()
		mgr.Unlock()
		if op.Op == OpCommit {
			mgr.onPreparCommit(op.Txn)
			if op.Txn.GetError() != nil {
				op.Op = OpRollback
				op.Txn.SetError(txnif.TxnRollbacked)
				op.Txn.Lock()
				op.Txn.ToRollbackingLocked(ts)
				op.Txn.Unlock()
				mgr.onPreparRollback(op.Txn)
			}
		} else {
			mgr.onPreparRollback(op.Txn)
		}
		mgr.EnqueueCheckpoint(op)
	}
	logrus.Infof("PrepareCommit %d Txns Takes: %s", len(items), time.Since(now))
}

// TODO
func (mgr *TxnManager) onCommit(items ...interface{}) {
	now := time.Now()
	for _, item := range items {
		op := item.(*OpTxn)
		switch op.Op {
		case OpCommit:
			if err := op.Txn.ApplyCommit(); err != nil {
				panic(err)
			}
		case OpRollback:
			if err := op.Txn.ApplyRollback(); err != nil {
				panic(err)
			}
		}
		op.Txn.WaitDone()
		logrus.Debugf("%s Done", op.Repr())
	}
	logrus.Infof("Commit %d Txns Takes: %s", len(items), time.Since(now))
}
