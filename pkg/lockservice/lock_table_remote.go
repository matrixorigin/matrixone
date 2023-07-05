// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// remoteLockTable the lock corresponding to the Table is managed by a remote LockTable.
// And the remoteLockTable acts as a proxy for this LockTable locally.
type remoteLockTable struct {
	logger             *log.MOLogger
	serviceID          string
	bind               pb.LockTable
	client             Client
	bindChangedHandler func(pb.LockTable)
}

func newRemoteLockTable(
	serviceID string,
	binding pb.LockTable,
	client Client,
	bindChangedHandler func(pb.LockTable)) *remoteLockTable {
	tag := "remote-lock-table"
	logger := runtime.ProcessLevelRuntime().Logger().
		Named(tag).
		With(zap.String("binding", binding.DebugString()))
	l := &remoteLockTable{
		logger:             logger,
		serviceID:          serviceID,
		client:             client,
		bind:               binding,
		bindChangedHandler: bindChangedHandler,
	}
	return l
}

func (l *remoteLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error)) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.remote")
	defer span.End()

	logRemoteLock(l.serviceID, txn, rows, opts, l.bind)

	req := acquireRequest()
	defer releaseRequest(req)

	req.LockTable = l.bind
	req.Method = pb.Method_Lock
	req.Lock.Options = opts.LockOptions
	req.Lock.TxnID = txn.txnID
	req.Lock.ServiceID = l.serviceID
	req.Lock.Rows = rows

	// rpc maybe wait too long, to avoid deadlock, we need unlock txn, and lock again
	// after rpc completed
	txn.Unlock()
	resp, err := l.client.Send(ctx, req)
	txn.Lock()

	// txn closed
	if !bytes.Equal(req.Lock.TxnID, txn.txnID) {
		cb(pb.Result{}, ErrTxnNotFound)
		return
	}

	if err == nil {
		defer releaseResponse(resp)
		if err := l.maybeHandleBindChanged(resp); err != nil {
			logRemoteLockFailed(l.serviceID, txn, rows, opts, l.bind, err)
			cb(pb.Result{}, err)
			return
		}

		txn.lockAdded(l.serviceID, l.bind.Table, rows, nil)
		logRemoteLockAdded(l.serviceID, txn, rows, opts, l.bind)
		cb(resp.Lock.Result, nil)
		return
	}

	logRemoteLockFailed(l.serviceID, txn, rows, opts, l.bind, err)
	// encounter any error, we need try to check bind is valid.
	// And use origin error to return, because once handlerError
	// swallows the error, the transaction will not be abort.
	_ = l.handleError(txn.txnID, err)
	cb(pb.Result{}, err)
}

func (l *remoteLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	logUnlockTableOnRemote(
		l.serviceID,
		txn,
		l.bind)
	for {
		err := l.doUnlock(txn, commitTS)
		if err == nil {
			return
		}

		logUnlockTableOnRemoteFailed(
			l.serviceID,
			txn,
			l.bind,
			err)
		// unlock cannot fail and must ensure that all locks have been
		// released.
		//
		// handleError returns nil meaning bind changed, then all locks
		// will be released. If handleError returns any error, it means
		// that the current bind is valid, retry unlock.
		if err := l.handleError(txn.txnID, err); err == nil {
			return
		}
	}
}

func (l *remoteLockTable) getLock(txnID, key []byte, fn func(Lock)) {
	for {
		lock, ok, err := l.doGetLock(txnID, key)
		if err == nil {
			if ok {
				fn(lock)
				w := lock.waiter
				for {
					w = w.close(l.serviceID, notifyValue{})
					if w == nil {
						break
					}
					w.clearAllNotify(l.serviceID, "remove temp notify")
				}
			}
			return
		}

		// why use loop is similar to unlock
		if err = l.handleError(txnID, err); err == nil {
			return
		}
	}
}

func (l *remoteLockTable) doUnlock(
	txn *activeTxn,
	commitTS timestamp.Timestamp) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_Unlock
	req.LockTable = l.bind
	req.Unlock.TxnID = txn.txnID
	req.Unlock.CommitTS = commitTS

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		defer releaseResponse(resp)
		return l.maybeHandleBindChanged(resp)
	}
	return err
}

func (l *remoteLockTable) doGetLock(txnID, key []byte) (Lock, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_GetTxnLock
	req.LockTable = l.bind
	req.GetTxnLock.Row = key
	req.GetTxnLock.TxnID = txnID

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		defer releaseResponse(resp)
		if err := l.maybeHandleBindChanged(resp); err != nil {
			return Lock{}, false, err
		}

		lock := Lock{
			txnID:  txnID,
			value:  byte(resp.GetTxnLock.Value),
			waiter: acquireWaiter(l.serviceID, txnID),
		}
		for _, v := range resp.GetTxnLock.WaitingList {
			w := acquireWaiter(l.serviceID, v.TxnID)
			w.waitTxn = v
			lock.waiter.add(l.serviceID, w)
		}
		return lock, true, nil
	}
	return Lock{}, false, err
}

func (l *remoteLockTable) getBind() pb.LockTable {
	return l.bind
}

func (l *remoteLockTable) close() {
	logLockTableClosed(l.serviceID, l.bind, true)
}

func (l *remoteLockTable) handleError(txnID []byte, err error) error {
	oldError := err
	// ErrLockTableBindChanged error must already handled. Skip
	if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
		return nil
	}

	// any other errors, retry.
	// Note. Since the cn where the remote lock table is located may
	// be permanently gone, we need to go to the allocator to check if
	// the bind is valid.
	new, err := getLockTableBind(
		l.client,
		l.bind.Table,
		l.serviceID)
	if err != nil {
		logGetRemoteBindFailed(l.serviceID, l.bind.Table, err)
		return oldError
	}
	if new.Changed(l.bind) {
		l.bindChangedHandler(new)
		return nil
	}
	return oldError
}

func (l *remoteLockTable) maybeHandleBindChanged(resp *pb.Response) error {
	if resp.NewBind == nil {
		return nil
	}
	newBind := resp.NewBind
	l.bindChangedHandler(*newBind)
	return ErrLockTableBindChanged
}
