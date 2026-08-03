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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (txn *Txn) rollback1PC(ctx context.Context) (err error) {
	if txn.IsReplay() {
		panic(moerr.NewTAERollbackNoCtxf("replayed txn %s should not be rolled back", txn.String()))
	}
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		return moerr.NewTAERollbackNoCtxf("unexpected txn status : %s", txnif.TxnStrState(state))
	}

	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		ctx: ctx,
		Txn: txn,
		Op:  OpRollback,
	})
	if err != nil {
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err, true)
	}
	txn.Wait()
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	return txn.Err
}

func (txn *Txn) commit1PC(ctx context.Context) (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommitNoCtxf("invalid txn state %s", txnif.TxnStrState(state))
	}
	txn.Add(1)
	if err = txn.Freeze(ctx); err == nil {
		txn.GetStore().StartTrace()
		err = txn.Mgr.OnOpTxn(&OpTxn{
			ctx: ctx,
			Txn: txn,
			Op:  OpCommit,
		})
	}

	if err != nil {
		txn.SetError(err)
		txn.Lock()
		ts := txn.GetStartTS()
		_ = txn.ToRollbackingLocked(ts.Next())
		txn.Unlock()
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err, true)
	}
	txn.Wait()
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	return txn.GetError()
}

func (txn *Txn) done1PCWithErr(err error) {
	txn.DoneCond.L.Lock()
	defer txn.DoneCond.L.Unlock()

	if err != nil {
		txn.ToUnknownLocked()
		txn.SetError(err)
	} else {
		if txn.State == txnif.TxnStatePreparing {
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
}
