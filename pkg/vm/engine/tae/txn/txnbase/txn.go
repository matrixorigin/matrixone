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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type OpType int8

const (
	OpCommit = iota
	OpRollback
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
	sync.RWMutex
	sync.WaitGroup
	*TxnCtx
	Mgr      *TxnManager
	Store    txnif.TxnStore
	Err      error
	DoneCond sync.Cond
	LSN      uint64

	PrepareCommitFn   func(txnif.AsyncTxn) error
	PrepareRollbackFn func(txnif.AsyncTxn) error
	ApplyCommitFn     func(txnif.AsyncTxn) error
	ApplyRollbackFn   func(txnif.AsyncTxn) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId uint64, start types.TS, info []byte) *Txn {
	txn := &Txn{
		Mgr:   mgr,
		Store: store,
	}
	txn.TxnCtx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	txn.DoneCond = *sync.NewCond(txn)
	return txn
}

func (txn *Txn) MockIncWriteCnt() int { return txn.Store.IncreateWriteCnt() }

func (txn *Txn) SetError(err error) { txn.Err = err }
func (txn *Txn) GetError() error    { return txn.Err }

func (txn *Txn) SetPrepareCommitFn(fn func(txnif.AsyncTxn) error)   { txn.PrepareCommitFn = fn }
func (txn *Txn) SetPrepareRollbackFn(fn func(txnif.AsyncTxn) error) { txn.PrepareRollbackFn = fn }
func (txn *Txn) SetApplyCommitFn(fn func(txnif.AsyncTxn) error)     { txn.ApplyCommitFn = fn }
func (txn *Txn) SetApplyRollbackFn(fn func(txnif.AsyncTxn) error)   { txn.ApplyRollbackFn = fn }

func (txn *Txn) Commit() (err error) {
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return nil
	}
	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpCommit,
	})
	// TxnManager is closed
	if err != nil {
		txn.SetError(err)
		txn.Lock()
		//_ = txn.ToRollbackingLocked(txn.GetStartTS() + 1)
		_ = txn.ToRollbackingLocked(txn.GetStartTS().Next())
		txn.Unlock()
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err)
	}
	txn.Wait()
	txn.Mgr.DeleteTxn(txn.GetID())
	return txn.GetError()
}

func (txn *Txn) GetStore() txnif.TxnStore {
	return txn.Store
}

func (txn *Txn) GetLSN() uint64 { return txn.LSN }

func (txn *Txn) Rollback() (err error) {
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return
	}
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
	txn.Mgr.DeleteTxn(txn.GetID())
	err = txn.Err
	return
}

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

func (txn *Txn) GetTxnState(waitIfcommitting bool) txnif.TxnState {
	txn.RLock()
	state := txn.State
	if !waitIfcommitting {
		txn.RUnlock()
		return state
	}
	if state != txnif.TxnStateCommitting {
		txn.RUnlock()
		return state
	}
	txn.RUnlock()
	txn.DoneCond.L.Lock()
	state = txn.State
	if state != txnif.TxnStateCommitting {
		txn.DoneCond.L.Unlock()
		return state
	}
	txn.DoneCond.Wait()
	state = txn.State
	txn.DoneCond.L.Unlock()
	return state
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

func (txn *Txn) PreApplyCommit() (err error) {
	err = txn.Store.PreApplyCommit()
	return
}

func (txn *Txn) ApplyCommit() (err error) {
	defer func() {
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

func (txn *Txn) PreCommit() error {
	return txn.Store.PreCommit()
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
