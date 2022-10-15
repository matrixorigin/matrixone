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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type ReplayTxn struct {
	*TxnCtx
	LSN                      uint64
	TenantID, UserID, RoleID atomic.Uint32
	Cmd                      *TxnCmd
}

func NewReplayTxn(txnCtx *TxnCtx, cmd *TxnCmd) *ReplayTxn {
	txn := &ReplayTxn{
		TxnCtx: txnCtx,
		Cmd:    cmd,
	}
	return txn
}

func (txn *ReplayTxn) HandleCmd(entry *api.Entry) (err error) {
	return
}

func (txn *ReplayTxn) MockIncWriteCnt() int {
	panic("not support")
}

func (txn *ReplayTxn) SetError(err error) {
	panic("not support")
}
func (txn *ReplayTxn) GetError() error {
	panic("not support")
}

func (txn *ReplayTxn) SetPrepareCommitFn(fn func(txnif.AsyncTxn) error) {
	panic("not support")
}
func (txn *ReplayTxn) SetPrepareRollbackFn(fn func(txnif.AsyncTxn) error) {
	panic("not support")
}
func (txn *ReplayTxn) SetApplyCommitFn(fn func(txnif.AsyncTxn) error)   { panic("not support") }
func (txn *ReplayTxn) SetApplyRollbackFn(fn func(txnif.AsyncTxn) error) { panic("not support") }

func (txn *ReplayTxn) Prepare() (pts types.TS, err error) { panic("not support") }

func (txn *ReplayTxn) Rollback() (err error) {
	defer func() {
		txn.Cmd.Close()
	}()
	txn.Cmd.ApplyRollback()
	txn.ToRollbackedLocked()
	return
}

func (txn *ReplayTxn) Committing() (err error) {
	panic("not support")
}
func (txn *ReplayTxn) UnsafeGetDatabase(id uint64) (h handle.Database, err error) {
	panic("not support")
}
func (txn *ReplayTxn) UnsafeGetRelation(dbid, tblid uint64) (h handle.Relation, err error) {
	panic("not support")
}
func (txn *ReplayTxn) Commit() (err error) {
	defer func() {
		txn.Cmd.Close()
	}()
	txn.Cmd.ApplyCommit()
	txn.ToCommittedLocked()
	return nil
}

func (txn *ReplayTxn) GetStore() txnif.TxnStore {
	panic("not support")
}

func (txn *ReplayTxn) GetLSN() uint64 { return txn.LSN }

func (txn *ReplayTxn) DoneWithErr(err error, isAbort bool) {
	panic("not support")
}

func (txn *ReplayTxn) PrepareCommit() (err error) {
	panic("not support")
}

func (txn *ReplayTxn) PreApplyCommit() (err error) {
	panic("not support")
}

func (txn *ReplayTxn) ApplyCommit() (err error) {
	panic("not support")
}

func (txn *ReplayTxn) ApplyRollback() (err error) {
	panic("not support")
}

func (txn *ReplayTxn) PrePrepare() error {
	panic("not support")
}

func (txn *ReplayTxn) PrepareRollback() (err error) {
	panic("not support")
}

func (txn *ReplayTxn) String() string {
	return fmt.Sprintf("%s: %v", txn.TxnCtx.String(), txn.GetError())
}

func (txn *ReplayTxn) WaitPrepared() error {
	panic("not support")
}

func (txn *ReplayTxn) WaitDone(err error, isAbort bool) error {
	panic("not support")
}

func (txn *ReplayTxn) BindAccessInfo(tenantID, userID, roleID uint32) {
	txn.TenantID.Store(tenantID)
	txn.UserID.Store(userID)
	txn.RoleID.Store(roleID)
}

func (txn *ReplayTxn) GetTenantID() uint32 {
	return txn.TenantID.Load()
}

func (txn *ReplayTxn) GetUserAndRoleID() (uint32, uint32) {
	return txn.UserID.Load(), txn.RoleID.Load()
}

func (txn *ReplayTxn) CreateDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *ReplayTxn) DropDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *ReplayTxn) GetDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *ReplayTxn) UseDatabase(name string) (err error) {
	return
}

func (txn *ReplayTxn) CurrentDatabase() (db handle.Database) {
	return
}

func (txn *ReplayTxn) DatabaseNames() (names []string) {
	return
}

func (txn *ReplayTxn) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return
}

func (txn *ReplayTxn) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	return
}
