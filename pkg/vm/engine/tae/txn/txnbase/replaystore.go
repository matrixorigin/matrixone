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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type replayTxnStore struct {
	NoopTxnStore
	Cmd *TxnCmd
}

func MakeReplayTxn(
	mgr *TxnManager,
	ctx *TxnCtx,
	lsn uint64,
	cmd *TxnCmd) *Txn {
	store := &replayTxnStore{
		Cmd: cmd,
	}
	txn := NewPersistedTxn(
		mgr,
		ctx,
		store,
		lsn,
		store.prepareCommit,
		store.prepareRollback,
		store.applyCommit,
		store.applyRollback)
	return txn
}

func (store *replayTxnStore) IsReadonly() bool { return false }

func (store *replayTxnStore) prepareCommit(txn txnif.AsyncTxn) (err error) {
	logutil.Infof("TODO PreparCommit %s", txn.String())
	// TODO
	// PrepareCommit all commands
	// Check idempotent of each command
	// Record all idempotent error commands and skip apply|rollback later
	return
}

func (store *replayTxnStore) applyCommit(txn txnif.AsyncTxn) (err error) {
	logutil.Infof("TODO ApplyCommit %s", txn.String())
	// TODO
	// ApplyCommit all commands
	// Release all commands
	return
}

func (store *replayTxnStore) applyRollback(txn txnif.AsyncTxn) (err error) {
	logutil.Infof("TODO ApplyRollback %s", txn.String())
	if !txn.Is2PC() {
		panic(moerr.NewInternalError("cannot apply rollback 1PC replay txn: %s",
			txn.String()))
	}
	// TODO
	// ApplyRollback all commands
	// Release all commands
	return
}

func (store *replayTxnStore) prepareRollback(txn txnif.AsyncTxn) (err error) {
	panic(moerr.NewInternalError("cannot prepareRollback rollback replay txn: %s",
		txn.String()))
}
