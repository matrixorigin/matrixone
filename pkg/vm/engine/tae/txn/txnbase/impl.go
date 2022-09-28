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
		txn.DoneWithErr(err, true)
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
		txn.DoneWithErr(err, true)
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

	switch state {
	case txnif.TxnStateActive:
		txn.Add(1)
		err = txn.Mgr.OnOpTxn(&OpTxn{
			Txn: txn,
			Op:  OpRollback,
		})
		if err != nil {
			_ = txn.PrepareRollback()
			_ = txn.ApplyRollback()
			_ = txn.ToRollbacking(txn.GetStartTS())
			txn.DoneWithErr(err, true)
		}
		txn.Wait()

	case txnif.TxnStatePrepared:
		_ = txn.ApplyRollback()
		txn.DoneWithErr(nil, true)

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAERollback("unexpected txn status : %s", txnif.TxnStrState(state))
	}

	txn.Mgr.DeleteTxn(txn.GetID())

	return txn.GetError()
}

func (txn *Txn) commit2PC() (err error) {
	state := txn.GetTxnState(false)

	switch state {
	case txnif.TxnStateCommittingFinished:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		//TODO:Append committed log entry into log service asynchronously
		//     for checkpointing the committing log entry
		//txn.SetError(txn.LogTxnEntry())
		txn.DoneWithErr(nil, false)

	//It's a 2PC transaction running in Participant.
	//Notice that Commit must be success once the commit message arrives,
	//since Preparing had already succeeded.
	case txnif.TxnStatePrepared:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		// TODO: Append committed log entry
		txn.DoneWithErr(nil, false)

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommit("invalid txn state %s", txnif.TxnStrState(state))
	}
	txn.Mgr.DeleteTxn(txn.GetID())

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

func (txn *Txn) done2PCWithErr(err error, isAbort bool) {
	txn.DoneCond.L.Lock()
	defer txn.DoneCond.L.Unlock()

	endOfTxn := true

	if err != nil {
		txn.ToUnknownLocked()
		txn.SetError(err)
	} else {
		switch txn.State {
		case txnif.TxnStateRollbacking:
			if err = txn.ToRollbackedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStatePreparing:
			endOfTxn = false
			if err = txn.ToPreparedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStateCommittingFinished:
			if err = txn.ToCommittedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStatePrepared:
			if isAbort {
				if err = txn.ToRollbackedLocked(); err != nil {
					panic(err)
				}
			} else {
				if err = txn.ToCommittedLocked(); err != nil {
					panic(err)
				}
			}
		}
	}

	txn.WaitGroup.Done()

	if endOfTxn {
		txn.DoneCond.Broadcast()
	}
}
