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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func (txn *OpTxn) Is2PC() bool { return txn.Txn.Is2PC() }
func (txn *OpTxn) IsTryCommitting() bool {
	return txn.Op == OpCommit || txn.Op == OpPrepare
}

func (txn *OpTxn) Repr() string {
	if txn.Op == OpCommit {
		return fmt.Sprintf("[Commit][Txn-%s]", txn.Txn.GetID())
	} else {
		return fmt.Sprintf("[Rollback][Txn-%s]", txn.Txn.GetID())
	}
}

var DefaultTxnFactory = func(mgr *TxnManager, store txnif.TxnStore, id []byte, startTS types.TS, info []byte) txnif.AsyncTxn {
	return NewTxn(mgr, store, id, startTS, info)
}

type Txn struct {
	sync.WaitGroup
	*TxnCtx
	Mgr                      *TxnManager
	Store                    txnif.TxnStore
	Err                      error
	LSN                      uint64
	TenantID, UserID, RoleID uint32

	PrepareCommitFn   func(txnif.AsyncTxn) error
	PrepareRollbackFn func(txnif.AsyncTxn) error
	ApplyCommitFn     func(txnif.AsyncTxn) error
	ApplyRollbackFn   func(txnif.AsyncTxn) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId []byte, start types.TS, info []byte) *Txn {
	txn := &Txn{
		Mgr:   mgr,
		Store: store,
	}
	txn.TxnCtx = NewTxnCtx(txnId, start, info)
	return txn
}

func (txn *Txn) HandleCmd(entry *api.Entry) (err error) {
	return
}

func (txn *Txn) MockIncWriteCnt() int { return txn.Store.IncreateWriteCnt() }

func (txn *Txn) SetError(err error) { txn.Err = err }
func (txn *Txn) GetError() error    { return txn.Err }

func (txn *Txn) SetPrepareCommitFn(fn func(txnif.AsyncTxn) error)   { txn.PrepareCommitFn = fn }
func (txn *Txn) SetPrepareRollbackFn(fn func(txnif.AsyncTxn) error) { txn.PrepareRollbackFn = fn }
func (txn *Txn) SetApplyCommitFn(fn func(txnif.AsyncTxn) error)     { txn.ApplyCommitFn = fn }
func (txn *Txn) SetApplyRollbackFn(fn func(txnif.AsyncTxn) error)   { txn.ApplyRollbackFn = fn }

//The state transition of transaction is as follows:
// 1PC: TxnStateActive--->TxnStatePreparing--->TxnStateCommitted/TxnStateRollbacked
//		TxnStateActive--->TxnStatePreparing--->TxnStateRollbacking--->TxnStateRollbacked
//      TxnStateActive--->TxnStateRollbacking--->TxnStateRollbacked
// 2PC running on Coordinator: TxnStateActive--->TxnStatePreparing-->TxnStatePrepared
//								-->TxnStateCommittingFinished--->TxnStateCommitted or
//								TxnStateActive--->TxnStatePreparing-->TxnStatePrepared-->TxnStateRollbacked or
//                             TxnStateActive--->TxnStateRollbacking--->TxnStateRollbacked.
// 2PC running on Participant: TxnStateActive--->TxnStatePreparing-->TxnStatePrepared-->TxnStateCommitted or
//                             TxnStateActive--->TxnStatePreparing-->TxnStatePrepared-->
//                             TxnStateRollbacking-->TxnStateRollbacked or
//                             TxnStateActive--->TxnStateRollbacking-->TxnStateRollbacked.

// Prepare is used to pre-commit a 2PC distributed transaction.
// Notice that once any error happened, we should rollback the txn.
// TODO: 1. How to handle the case in which log service timed out?
//  2. For a 2pc transaction, Rollback message may arrive before Prepare message,
//     should handle this case by TxnStorage?
func (txn *Txn) Prepare() (pts types.TS, err error) {
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		logutil.Warn("tae : txn is not found in TxnManager")
		//txn.Err = ErrTxnNotFound
		return types.TS{}, moerr.NewTxnNotFound()
	}
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		logutil.Warnf("unexpected txn status : %s", txnif.TxnStrState(state))
		txn.Err = moerr.NewTxnNotActive(txnif.TxnStrState(state))
		return types.TS{}, txn.Err
	}
	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpPrepare,
	})
	// TxnManager is closed
	if err != nil {
		txn.SetError(err)
		txn.ToRollbacking(txn.GetStartTS())
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err, true)
	}
	txn.Wait()

	if txn.Err != nil {
		txn.Mgr.DeleteTxn(txn.GetID())
	}
	return txn.GetPrepareTS(), txn.GetError()
}

// Rollback is used to roll back a 1PC or 2PC transaction.
// Notice that there may be a such scenario in which a 2PC distributed transaction in ACTIVE
// will be rollbacked, since Rollback message may arrive before the Prepare message.
func (txn *Txn) Rollback() (err error) {
	//idempotent check
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		logutil.Warnf("tae : txn %s is not found in TxnManager", txn.GetID())
		err = moerr.NewTxnNotFound()
		return
	}
	if txn.Store.IsReadonly() {
		err = txn.Mgr.DeleteTxn(txn.GetID())
		return
	}

	if txn.Is2PC() {
		return txn.rollback2PC()
	}

	return txn.rollback1PC()
}

// Committing is used to record a "committing" status for coordinator.
// Notice that txn must commit successfully once committing message arrives, since Preparing
// had already succeeded.
func (txn *Txn) Committing() (err error) {
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		err = moerr.NewTxnNotFound()
		return
	}
	state := txn.GetTxnState(false)
	if state != txnif.TxnStatePrepared {
		return moerr.NewInternalError(
			"stat not prepared, unexpected txn status : %s",
			txnif.TxnStrState(state),
		)
	}
	if err = txn.ToCommittingFinished(); err != nil {
		panic(err)
	}
	//Make a committing log entry, flush and wait it synced.
	//A log entry's payload contains txn id , commit timestamp and txn's state.
	_, err = txn.LogTxnState(true)
	if err != nil {
		panic(err)
	}
	err = txn.Err
	return
}

// Commit is used to commit a 1PC or 2PC transaction running on Coordinator or running on Participant.
// Notice that the Commit of a 2PC transaction must be success once the Commit message arrives,
// since Preparing had already succeeded.
func (txn *Txn) Commit() (err error) {
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		err = moerr.NewTxnNotFound()
		return
	}
	// Skip readonly txn
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return nil
	}

	if txn.Is2PC() {
		return txn.commit2PC()
	}

	return txn.commit1PC()
}

func (txn *Txn) GetStore() txnif.TxnStore {
	return txn.Store
}

func (txn *Txn) GetLSN() uint64 { return txn.LSN }

func (txn *Txn) DoneWithErr(err error, isAbort bool) {
	// Idempotent check
	if moerr.IsMoErrCode(err, moerr.ErrTxnNotActive) {
		txn.WaitGroup.Done()
		return
	}

	if txn.Is2PC() {
		txn.done2PCWithErr(err, isAbort)
		return
	}
	txn.done1PCWithErr(err)
}

func (txn *Txn) PrepareCommit() (err error) {
	logutil.Debugf("Prepare Commite %s", txn.ID)
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

func (txn *Txn) PrePrepare() error {
	return txn.Store.PrePrepare()
}

func (txn *Txn) PrepareRollback() (err error) {
	logutil.Debugf("Prepare Rollbacking %s", txn.ID)
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

func (txn *Txn) WaitPrepared() error {
	return txn.Store.WaitPrepared()
}

func (txn *Txn) WaitDone(err error, isAbort bool) error {
	// logutil.Infof("Wait %s Done", txn.String())
	txn.DoneWithErr(err, isAbort)
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

func (txn *Txn) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	return
}
