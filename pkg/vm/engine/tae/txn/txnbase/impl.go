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
		panic(moerr.NewTAERollbackNoCtxf("1pc txn %s should not be called here", txn.String()))
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
	//txn.Status = txnif.TxnStatusRollbacked
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	return txn.Err
}

func (txn *Txn) commit1PC(ctx context.Context, _ bool) (err error) {
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

	// TxnManager is closed
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
	//if txn.Err == nil {
	//txn.Status = txnif.TxnStatusCommitted
	//}
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	return txn.GetError()
}

func (txn *Txn) rollback2PC(ctx context.Context) (err error) {
	state := txn.GetTxnState(false)

	switch state {
	case txnif.TxnStateActive:
		txn.Add(1)
		err = txn.Mgr.OnOpTxn(&OpTxn{
			ctx: ctx,
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
		//Notice that at this moment, txn had already appended data into state machine, so
		// we can not just delete the AppendNode from the MVCCHandle, instead ,we should
		// set the state of the AppendNode to Abort to make reader perceive it .
		_ = txn.ApplyRollback()
		txn.DoneWithErr(nil, true)

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAERollbackNoCtxf("unexpected txn status : %s", txnif.TxnStrState(state))
	}

	txn.Mgr.DeleteTxn(txn.GetID())

	return txn.GetError()
}

func (txn *Txn) commit2PC(inRecovery bool) (err error) {
	state := txn.GetTxnState(false)
	txn.Mgr.OnCommitTxn(txn)

	switch state {
	//It's a 2PC transaction running on Coordinator
	case txnif.TxnStateCommittingFinished:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		txn.DoneWithErr(nil, false)

		// Skip logging if in recovery
		if !inRecovery {
			//Append a committed log entry into log service asynchronously
			//     for checkpointing the committing log entry
			_, err = txn.LogTxnState(false)
			if err != nil {
				panic(err)
			}
		}

	//It's a 2PC transaction running on Participant.
	//Notice that Commit must be successful once the commit message arrives,
	//since Committing had succeed.
	case txnif.TxnStatePrepared:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		txn.DoneWithErr(nil, false)

		// Skip logging if in recovery
		if !inRecovery {
			//Append committed log entry ,and wait it synced.
			_, err = txn.LogTxnState(true)
			if err != nil {
				panic(err)
			}
		}

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommitNoCtxf("invalid txn state %s", txnif.TxnStrState(state))
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
	done := true

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
			done = false
			if err = txn.ToCommittedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStatePrepared:
			done = false
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
	if done {
		txn.WaitGroup.Done()
	}

	if endOfTxn {
		txn.DoneCond.Broadcast()
	}
}
