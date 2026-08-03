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
	"runtime/trace"
	"sync/atomic"
	"time"

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
	OpInvalid
)

const (
	EventRollback = iota + 1
	EventCommitting
	EventCommit
)

type OpTxn struct {
	ctx context.Context
	Txn txnif.AsyncTxn
	Op  OpType
}

func (txn *OpTxn) IsReplay() bool { return txn.Txn.IsReplay() }
func (txn *OpTxn) IsTryCommitting() bool {
	return txn.Op == OpCommit
}

func (txn *OpTxn) Repr() string {
	if txn.Op == OpCommit {
		return fmt.Sprintf("[Commit][Txn-%X]", txn.Txn.GetID())
	} else {
		return fmt.Sprintf("[Rollback][Txn-%X]", txn.Txn.GetID())
	}
}

var DefaultTxnFactory = func(
	mgr *TxnManager,
	store txnif.TxnStore,
	id []byte,
	startTS types.TS,
	snapshotTS types.TS) txnif.AsyncTxn {
	return NewTxn(mgr, store, id, startTS, snapshotTS)
}

type Txn struct {
	*TxnCtx
	Mgr                      *TxnManager
	Store                    txnif.TxnStore
	Err                      error
	LSN                      uint64
	TenantID, UserID, RoleID atomic.Uint32
	isReplay                 bool
	DedupType                txnif.DedupPolicy

	syncProtectionJobID string // Job ID for CCPR sync protection validation

	FreezeFn          func(txnif.AsyncTxn) error
	PrepareCommitFn   func(txnif.AsyncTxn) error
	PrepareRollbackFn func(txnif.AsyncTxn) error
	ApplyCommitFn     func(txnif.AsyncTxn) error
	ApplyRollbackFn   func(txnif.AsyncTxn) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId []byte, start, snapshot types.TS) *Txn {
	txn := &Txn{
		Mgr:   mgr,
		Store: store,
	}
	txn.TxnCtx = NewTxnCtx(txnId, start, snapshot)
	return txn
}

func MockTxnReaderWithStartTS(startTS types.TS) *Txn {
	return &Txn{
		TxnCtx: &TxnCtx{
			StartTS: startTS,
		},
	}
}

func MockTxnReaderWithNow() *Txn {
	return MockTxnReaderWithStartTS(types.BuildTS(time.Now().UTC().UnixNano(), 0))
}

func NewPersistedTxn(
	mgr *TxnManager,
	ctx *TxnCtx,
	store txnif.TxnStore,
	lsn uint64,
	prepareCommitFn func(txnif.AsyncTxn) error,
	prepareRollbackFn func(txnif.AsyncTxn) error,
	applyCommitFn func(txnif.AsyncTxn) error,
	applyRollbackFn func(txnif.AsyncTxn) error) *Txn {
	return &Txn{
		Mgr:               mgr,
		TxnCtx:            ctx,
		Store:             store,
		isReplay:          true,
		LSN:               lsn,
		PrepareRollbackFn: prepareRollbackFn,
		PrepareCommitFn:   prepareCommitFn,
		ApplyRollbackFn:   applyRollbackFn,
		ApplyCommitFn:     applyCommitFn,
	}
}
func (txn *Txn) GetBase() txnif.BaseTxn {
	return txn
}
func (txn *Txn) GetLsn() uint64              { return txn.LSN }
func (txn *Txn) IsReplay() bool              { return txn.isReplay }
func (txn *Txn) GetContext() context.Context { return txn.Store.GetContext() }
func (txn *Txn) MockIncWriteCnt() error      { return txn.Store.IncreateWriteCnt("mock") }

func (txn *Txn) SetError(err error) { txn.Err = err }
func (txn *Txn) GetError() error    { return txn.Err }

func (txn *Txn) SetFreezeFn(fn func(txnif.AsyncTxn) error)          { txn.FreezeFn = fn }
func (txn *Txn) SetPrepareCommitFn(fn func(txnif.AsyncTxn) error)   { txn.PrepareCommitFn = fn }
func (txn *Txn) SetPrepareRollbackFn(fn func(txnif.AsyncTxn) error) { txn.PrepareRollbackFn = fn }
func (txn *Txn) SetApplyCommitFn(fn func(txnif.AsyncTxn) error)     { txn.ApplyCommitFn = fn }
func (txn *Txn) SetApplyRollbackFn(fn func(txnif.AsyncTxn) error)   { txn.ApplyRollbackFn = fn }
func (txn *Txn) SetDedupType(dedupType txnif.DedupPolicy)           { txn.DedupType = dedupType }
func (txn *Txn) GetDedupType() txnif.DedupPolicy                    { return txn.DedupType }

func (txn *Txn) SetSyncProtectionJobID(jobID string) { txn.syncProtectionJobID = jobID }
func (txn *Txn) GetSyncProtectionJobID() string      { return txn.syncProtectionJobID }

// The state transition of a transaction is:
// TxnStateActive--->TxnStatePreparing--->TxnStateCommitted/TxnStateRollbacked
//
//			TxnStateActive--->TxnStatePreparing--->TxnStateRollbacking--->TxnStateRollbacked
//	     TxnStateActive--->TxnStateRollbacking--->TxnStateRollbacked
//
// Rollback rolls back a transaction.
func (txn *Txn) Rollback(ctx context.Context) (err error) {
	if txn.GetStore().IsOffline() {
		return
	}
	//idempotent check
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		logutil.Warnf("tae : txn %s is not found in TxnManager", txn.GetID())
		err = moerr.NewTxnNotFoundNoCtx()
		return
	}
	if txn.Store.IsReadonly() {
		err = txn.Mgr.DeleteTxn(txn.GetID())
		return
	}

	return txn.rollback1PC(ctx)
}

// Commit commits a transaction.
func (txn *Txn) Commit(ctx context.Context) (err error) {
	probe := trace.StartRegion(context.Background(), "Commit")
	defer probe.End()

	err = txn.doCommit(ctx)
	return
}

func (txn *Txn) doCommit(ctx context.Context) (err error) {
	if txn.GetStore().IsOffline() {
		return
	}
	if txn.Mgr.GetTxn(txn.GetID()) == nil {
		err = moerr.NewTxnNotFoundNoCtx()
		return
	}
	// Skip readonly txn
	if txn.Store.IsReadonly() {
		txn.Mgr.DeleteTxn(txn.GetID())
		return nil
	}

	return txn.commit1PC(ctx)
}

func (txn *Txn) GetStore() txnif.TxnStore {
	return txn.Store
}

func (txn *Txn) GetLSN() uint64 {
	txn.GetTxnState(true)
	return txn.LSN
}

func (txn *Txn) DoneWithErr(err error, _ bool) {
	// Idempotent check
	if moerr.IsMoErrCode(err, moerr.ErrTxnNotActive) {
		// FIXME::??
		txn.WaitGroup.Done()
		return
	}

	txn.done1PCWithErr(err)
	txn.GetStore().EndTrace()
}

func (txn *Txn) PrepareCommit() (err error) {
	logutil.Debugf("Prepare Commite %X", txn.ID)
	if txn.PrepareCommitFn != nil {
		err = txn.PrepareCommitFn(txn)
		return
	}
	err = txn.Store.PrepareCommit()
	return err
}

func (txn *Txn) PreApplyCommit() (err error) {
	err = txn.Store.PreApplyCommit()
	return
}

func (txn *Txn) PrepareWAL() (err error) {
	err = txn.Store.PrepareWAL()
	return
}

func (txn *Txn) ApplyCommit() (err error) {
	if txn.ApplyCommitFn != nil {
		err = txn.ApplyCommitFn(txn)
		return
	}
	defer func() {
		//Get the lsn of ETTxnRecord entry in GroupC.
		txn.LSN = txn.Store.GetLSN()
		if err == nil {
			err = txn.Store.Close()
		} else {
			txn.Store.Close()
		}
	}()
	err = txn.Store.ApplyCommit()
	return
}

func (txn *Txn) ApplyRollback() (err error) {
	if txn.ApplyRollbackFn != nil {
		err = txn.ApplyRollbackFn(txn)
		return
	}
	defer func() {
		txn.LSN = txn.Store.GetLSN()
		if err == nil {
			err = txn.Store.Close()
		} else {
			txn.Store.Close()
		}
	}()
	err = txn.Store.ApplyRollback()
	return
}

func (txn *Txn) PrePrepare(ctx context.Context) error {
	return txn.Store.PrePrepare(ctx)
}

func (txn *Txn) Freeze(ctx context.Context) error {
	if txn.FreezeFn != nil {
		err := txn.FreezeFn(txn)
		return err
	}
	return txn.Store.Freeze(ctx)
}

func (txn *Txn) PrepareRollback() (err error) {
	logutil.Debugf("Prepare Rollbacking %X", txn.ID)
	if txn.PrepareRollbackFn != nil {
		err = txn.PrepareRollbackFn(txn)
		return
	}
	err = txn.Store.PrepareRollback()
	return
}

func (txn *Txn) String() string {
	str := txn.TxnCtx.String()
	return fmt.Sprintf("%s: %v", str, txn.GetError())
}

func (txn *Txn) WaitWalAndTail(ctx context.Context) error {
	return txn.Store.WaitWalAndTail(ctx)
}

func (txn *Txn) DoneApply(err error, isAbort bool) error {
	// logutil.Infof("Wait %s Done", txn.String())
	txn.DoneWithErr(err, isAbort)
	return txn.Err
}

func (txn *Txn) BindAccessInfo(tenantID, userID, roleID uint32) {
	txn.TenantID.Store(tenantID)
	txn.UserID.Store(userID)
	txn.RoleID.Store(roleID)
}

func (txn *Txn) GetTenantID() uint32 {
	return txn.TenantID.Load()
}

func (txn *Txn) GetUserAndRoleID() (uint32, uint32) {
	return txn.UserID.Load(), txn.RoleID.Load()
}

func (txn *Txn) CreateDatabase(name, createSql, datTyp string) (db handle.Database, err error) {
	return
}

func (txn *Txn) CreateDatabaseWithCtx(ctx context.Context,
	name, createSql, datTyp string, id uint64) (db handle.Database, err error) {
	return
}

func (txn *Txn) DropDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) DropDatabaseByID(id uint64) (db handle.Database, err error) {
	return
}

func (txn *Txn) UnsafeGetDatabase(id uint64) (db handle.Database, err error) {
	return
}

func (txn *Txn) UnsafeGetRelation(dbId, id uint64) (db handle.Relation, err error) {
	return
}

func (txn *Txn) GetDatabase(name string) (db handle.Database, err error) {
	return
}
func (txn *Txn) GetDatabaseWithCtx(_ context.Context, _ string) (db handle.Database, err error) {
	return
}

func (txn *Txn) GetDatabaseByID(id uint64) (db handle.Database, err error) {
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

func (txn *Txn) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	return
}
