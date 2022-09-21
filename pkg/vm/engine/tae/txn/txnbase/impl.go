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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (txn *Txn) rollback1PC() (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		return moerr.NewTAERollback("unexpected txn status : %s", txnif.TxnStrState(state))
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
	//txn.Status = txnif.TxnStatusRollbacked
	txn.Mgr.DeleteTxn(txn.GetID())
	return txn.Err
}

func (txn *Txn) commit1PC() (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommit("invalid txn state %s", txnif.TxnStrState(state))
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

func (txn *Txn) rollback2PC() (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive && state != txnif.TxnStatePrepared {
		return moerr.NewInternalError("cannot rollback, unexpected txn state : %s", txnif.TxnStrState(state))
	}

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

func (txn *Txn) commit2PC() (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateCommittingFinished && state != txnif.TxnStatePrepared {
		return moerr.NewInternalError("cannot commit, unexpected txn state : %s", txnif.TxnStrState(state))
	}

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
