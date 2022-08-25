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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type OpType int8

const (
	OpCommit = iota
	OpRollback
	OpPrepare
	OpCommitting
	OpInvalid
)

const (
	EventRollback = iota + 1
	EventCommitting
	EventCommit
)

type OpTxn struct {
	Txn txnif.AsyncTxn
	Op  OpType
}

func (txn *OpTxn) Repr() string {
	if txn.Op == OpCommit {
		return fmt.Sprintf("[Commit][Txn-%d]", txn.Txn.GetID())
	} else {
		return fmt.Sprintf("[Rollback][Txn-%d]", txn.Txn.GetID())
	}
}

var DefaultTxnFactory = func(mgr *TxnManager, store txnif.TxnStore, id uint64, startTS types.TS, info []byte) txnif.AsyncTxn {
	return NewTxn(mgr, store, id, startTS, info)
}

type Txn struct {
	sync.WaitGroup
	*TxnCtx
	Ch                       chan int
	Mgr                      *TxnManager
	Store                    txnif.TxnStore
	Err                      error
	LSN                      uint64
	TenantID, UserID, RoleID uint32

	PrepareCommitFn     func(txnif.AsyncTxn) error
	Prepare2PCPrepareFn func(txnif.AsyncTxn) error
	PrepareRollbackFn   func(txnif.AsyncTxn) error
	ApplyPrepareFn      func(txnif.AsyncTxn) error
	ApplyCommitFn       func(txnif.AsyncTxn) error
	ApplyRollbackFn     func(txnif.AsyncTxn) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId uint64, start types.TS, info []byte) *Txn {
	txn := &Txn{
		Mgr:   mgr,
		Store: store,
		Ch:    make(chan int, 1),
	}
	txn.TxnCtx = NewTxnCtx(txnId, start, info)
	return txn
}

func (txn *Txn) MockIncWriteCnt() int { return txn.Store.IncreateWriteCnt() }

func (txn *Txn) SetError(err error) { txn.Err = err }
func (txn *Txn) GetError() error    { return txn.Err }

func (txn *Txn) SetPrepareCommitFn(fn func(txnif.AsyncTxn) error)   { txn.PrepareCommitFn = fn }
func (txn *Txn) SetPrepareRollbackFn(fn func(txnif.AsyncTxn) error) { txn.PrepareRollbackFn = fn }
func (txn *Txn) SetApplyCommitFn(fn func(txnif.AsyncTxn) error)     { txn.ApplyCommitFn = fn }
func (txn *Txn) SetApplyRollbackFn(fn func(txnif.AsyncTxn) error)   { txn.ApplyRollbackFn = fn }

//The state transition of transaction is as follows:
// 1PC: TxnStateActive--->TxnStateCommitting--->TxnStateCommitted/TxnStateRollbacked
//         TxnStateActive--->TxnStateRollbacking--->TxnStateRollbacked
// 2PC running on Coordinator: TxnStateActive--->TxnStatePreparing-->TxnStatePrepared
//								-->TxnStateCommittingFinished--->TxnStateCommitted or
//								TxnStateActive--->TxnStatePreparing-->TxnStatePrepared-->TxnStateRollbacked or
//                             TxnStateActive--->TxnStateRollbacking--->TxnStateRollbacked.
// 2PC running on Participant: TxnStateActive--->TxnStatePrepared-->TxnStateCommitted or
//                             TxnStateActive--->TxnStatePrepared-->TxnStateRollbacked or
//                             TxnStateActive--->TxnStateRollbacking-->TxnStateRollbacked.

// Prepare is used to pre-commit a 2PC distributed transaction.
// Notice that once any error happened, we should rollback the txn.
// TODO: 1. How to handle the case in which log service timed out?
//  2. For a 2pc transaction, Rollback message may arrive before Prepare message,
//     should handle this case by TxnStorage?
func (txn *Txn) Prepare() (err error) {
	//TODO::should handle this by TxnStorage?
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		logutil.Warn("tae : txn is not found in TxnManager")
		//txn.Err = ErrTxnNotFound
		return ErrTxnNotFound
	}
	state := (txnif.TxnState)(atomic.LoadInt32((*int32)(&txn.State)))
	if state != txnif.TxnStateActive {
		logutil.Warnf("unexpected txn status : %s", txnif.TxnStrState(txn.State))
		txn.Err = ErrTxnStateNotActive
		return txn.Err
	}
	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpPrepare,
	})
	// TxnManager is closed
	if err != nil {
		txn.SetError(err)
		txn.Lock()
		_ = txn.ToRollbackingLocked(txn.GetStartTS().Next())
		txn.Unlock()
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err)
	}
	txn.Wait()
	if txn.Err == nil {
		//txn.State = txnif.TxnStatePrepared
		atomic.StoreInt32((*int32)(&txn.State), (int32)(txnif.TxnStatePrepared))
	} else {
		//txn.Status = txnif.TxnStatusRollbacked
		txn.Mgr.DeleteTxn(txn.GetID())
	}
	return txn.GetError()
}

// Rollback is used to roll back a 1PC or 2PC transaction.
// rollback's idempotent is handled here, Although Prepare/Commit/Committing message's idempotent
// is handled by the transaction framework.
// Notice that there may be a such scenario in which a 2PC distributed transaction in ACTIVE will be rollbacked,
// since Rollback message may arrive before the Prepare message. Should handle this case by TxnStorage?
func (txn *Txn) Rollback() (err error) {
	//TODO:idempotent for rollback should be guaranteed by TxnStoage?
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		logutil.Warn("tae : txn is not found in TxnManager")
		return
	}

	state := (txnif.TxnState)(atomic.LoadInt32((*int32)(&txn.State)))
	if (!txn.Is2PC && state != txnif.TxnStateActive) ||
		txn.Is2PC && state != txnif.TxnStateActive &&
			state != txnif.TxnStatePrepared {
		logutil.Warnf("unexpected txn status : %s", txnif.TxnStrState(txn.State))
		return ErrTxnStateCannotRollback
	}
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return
	}
	//2PC
	if txn.Is2PC {
		if state == txnif.TxnStateActive {
			txn.Add(1)
			err = txn.Mgr.OnOpTxn(&OpTxn{
				Txn: txn,
				Op:  OpRollback,
			})
			if err != nil {
				_ = txn.PrepareRollback()
				_ = txn.ApplyRollback()
				txn.DoneWithErr(err)
			}
			txn.Wait()
			//txn.Status = txnif.TxnStatusRollbacked
			//atomic.StoreInt32((*int32)(&txn.State), (int32)(txnif.TxnStateRollbacked))
			txn.Mgr.DeleteTxn(txn.GetID())
		}
		if state == txnif.TxnStatePrepared {
			txn.Add(1)
			txn.Ch <- EventRollback
			//Wait txn rollbacked
			txn.Wait()
			//txn.Status = txnif.TxnStatusRollbacked
			txn.Mgr.DeleteTxn(txn.GetID())
		}
		return txn.GetError()
	}
	//1PC
	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpRollback,
	})
	if err != nil {
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err)
	}
	txn.Wait()
	//txn.Status = txnif.TxnStatusRollbacked
	txn.Mgr.DeleteTxn(txn.GetID())
	return txn.Err
}

// Committing is used to record a log for 2PC distributed transaction running in Coordinator.
// Notice that transaction must be committed once committing message arrives, since Preparing
// had already succeeded.
func (txn *Txn) Committing() (err error) {
	state := (txnif.TxnState)(atomic.LoadInt32((*int32)(&txn.State)))
	if state != txnif.TxnStatePrepared {
		logutil.Warnf("unexpected txn status : %s", txnif.TxnStrState(txn.State))
		//txn.Err = ErrTxnStatusNotPrepared
		return ErrTxnStateNotPrepared
	}
	txn.Add(1)
	txn.Ch <- EventCommitting
	txn.Wait()
	//txn.Status = txnif.TxnStatusCommittingFinished
	atomic.StoreInt32((*int32)(&txn.State), (int32)(txnif.TxnStateCommittingFinished))
	return txn.Err
}

// Commit is used to commit a 1PC or 2PC transaction running in Coordinator or running in Participant.
// Notice that the Commit of a 2PC transaction must be success once the commit message arrives,
// since Preparing had already succeeded.
func (txn *Txn) Commit() (err error) {
	state := (txnif.TxnState)(atomic.LoadInt32((*int32)(&txn.State)))
	if (!txn.Is2PC && state != txnif.TxnStateActive) ||
		txn.Is2PC && state != txnif.TxnStateCommittingFinished &&
			state != txnif.TxnStatePrepared {
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(txn.State))
		//txn.Err = ErrTxnStatusCannotCommit
		return ErrTxnStateCannotCommit
	}
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return nil
	}
	if txn.Is2PC {
		//It's a 2PC transaction running in Coordinator
		if state == txnif.TxnStateCommittingFinished {
			//TODO:Append committed log entry into log service asynchronously
			//     for checkpointing the committing log entry
			//txn.SetError(txn.LogTxnEntry())
			if txn.Err == nil {
				//txn.State = txnif.TxnStateCommitted
				atomic.StoreInt32((*int32)(&txn.State), (int32)(txnif.TxnStateCommitted))
			}
			txn.Mgr.DeleteTxn(txn.GetID())
		}
		//It's a 2PC transaction running in Participant.
		//Notice that Commit must be success once the commit message arrives,
		//since Preparing had already succeeded.
		if state == txnif.TxnStatePrepared {
			txn.Add(1)
			txn.Ch <- EventCommit
			txn.Wait()
			//txn.Status = txnif.TxnStatusCommitted
			atomic.StoreInt32((*int32)(&txn.State), (int32)(txnif.TxnStateCommitted))
			txn.Mgr.DeleteTxn(txn.GetID())
		}
		return txn.GetError()
	}
	//It's a 1PC transaction
	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpCommit,
	})
	// TxnManager is closed
	if err != nil {
		txn.SetError(err)
		txn.Lock()
		_ = txn.ToRollbackingLocked(txn.GetStartTS().Next())
		txn.Unlock()
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err)
	}
	txn.Wait()
	//if txn.Err == nil {
	//txn.Status = txnif.TxnStatusCommitted
	//}
	txn.Mgr.DeleteTxn(txn.GetID())
	return txn.GetError()
}

func (txn *Txn) GetStore() txnif.TxnStore {
	return txn.Store
}

func (txn *Txn) GetLSN() uint64 { return txn.LSN }

func (txn *Txn) Event() (e int) {
	e = <-txn.Ch
	return
}

// TODO::need to take 2PC txn account into.
func (txn *Txn) DoneWithErr(err error) {
	txn.DoneCond.L.Lock()
	if err != nil {
		txn.ToUnknownLocked()
		txn.SetError(err)
	} else {
		if txn.State == txnif.TxnStateCommitting {
			if err := txn.ToCommittedLocked(); err != nil {
				txn.SetError(err)
			}
		} else {
			if err := txn.ToRollbackedLocked(); err != nil {
				txn.SetError(err)
			}
		}
	}
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
	txn.DoneCond.L.Unlock()
}

func (txn *Txn) IsTerminated(waitIfcommitting bool) bool {
	state := txn.GetTxnState(waitIfcommitting)
	return state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked
}

func (txn *Txn) PrepareCommit() (err error) {
	logutil.Debugf("Prepare Committing %d", txn.ID)
	if txn.PrepareCommitFn != nil {
		if err = txn.PrepareCommitFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.PrepareCommit()
	return err
}

func (txn *Txn) Prepare2PCPrepare() (err error) {
	logutil.Debugf("Prepare Committing %d", txn.ID)
	if txn.Prepare2PCPrepareFn != nil {
		if err = txn.Prepare2PCPrepareFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.Prepare2PCPrepare()
	return err
}

func (txn *Txn) PreApplyCommit() (err error) {
	err = txn.Store.PreApplyCommit()
	return
}

func (txn *Txn) PreApply2PCPrepare() (err error) {
	err = txn.Store.PreApply2PCPrepare()
	return
}

// Apply2PCPrepare apply preparing for a 2PC distributed transaction
func (txn *Txn) Apply2PCPrepare() (err error) {
	defer func() {
		//Get the lsn of ETTxnRecord entry in GroupC
		txn.LSN = txn.Store.GetLSN()
		if err != nil {
			txn.Store.Close()
		}
	}()
	if txn.ApplyPrepareFn != nil {
		if err = txn.ApplyPrepareFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.Apply2PCPrepare()
	return
}

func (txn *Txn) ApplyCommit() (err error) {
	defer func() {
		//Get the lsn of ETTxnRecord entry in GroupC.
		txn.LSN = txn.Store.GetLSN()
		if err == nil {
			err = txn.Store.Close()
		} else {
			txn.Store.Close()
		}
	}()
	if txn.ApplyCommitFn != nil {
		if err = txn.ApplyCommitFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.ApplyCommit()
	return
}

func (txn *Txn) ApplyRollback() (err error) {
	defer func() {
		txn.LSN = txn.Store.GetLSN()
		if err == nil {
			err = txn.Store.Close()
		} else {
			txn.Store.Close()
		}
	}()
	if txn.ApplyRollbackFn != nil {
		if err = txn.ApplyRollbackFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.ApplyRollback()
	return
}

func (txn *Txn) PreCommitOr2PCPrepare() error {
	return txn.Store.PreCommitOr2PCPrepare()
}

func (txn *Txn) PrepareRollback() (err error) {
	logutil.Debugf("Prepare Rollbacking %d", txn.ID)
	if txn.PrepareRollbackFn != nil {
		if err = txn.PrepareRollbackFn(txn); err != nil {
			return
		}
	}
	err = txn.Store.PrepareRollback()
	return
}

func (txn *Txn) String() string {
	str := txn.TxnCtx.String()
	return fmt.Sprintf("%s: %v", str, txn.GetError())
}

func (txn *Txn) WaitDone(err error) error {
	// logutil.Infof("Wait %s Done", txn.String())
	txn.DoneWithErr(err)
	return txn.Err
}

func (txn *Txn) BindAccessInfo(tenantID, userID, roleID uint32) {
	atomic.StoreUint32(&txn.TenantID, tenantID)
	atomic.StoreUint32(&txn.UserID, userID)
	atomic.StoreUint32(&txn.RoleID, roleID)
}

func (txn *Txn) GetTenantID() uint32 {
	return atomic.LoadUint32(&txn.TenantID)
}

func (txn *Txn) GetUserAndRoleID() (uint32, uint32) {
	return atomic.LoadUint32(&txn.UserID), atomic.LoadUint32(&txn.RoleID)
}

func (txn *Txn) CreateDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) DropDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) GetDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) UseDatabase(name string) (err error) {
	return
}

func (txn *Txn) CurrentDatabase() (db handle.Database) {
	return
}

func (txn *Txn) DatabaseNames() (names []string) {
	return
}

func (txn *Txn) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return
}
