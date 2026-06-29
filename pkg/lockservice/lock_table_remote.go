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
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	remoteRetryInitialBackoff = 100 * time.Millisecond
	remoteRetryMaxBackoff     = 5 * time.Second
)

const (
	// lockRpcSlack is the extra budget added to the RPC deadline beyond
	// LockWaitTimeout.  The lock-table owner starts its own wait budget only
	// after receiving the RPC, so the client-side RPC deadline must outlive the
	// server-side wait timer for the owner to observe and return ErrLockTimeout.
	// Without this slack, the client deadline can fire before the owner returns
	// ErrLockTimeout, causing the client to see a retryable connectivity error
	// instead of a lock-timeout result.
	lockRpcSlack = 30 * time.Second
)

// remoteLockTable the lock corresponding to the Table is managed by a remote LockTable.
// And the remoteLockTable acts as a proxy for this LockTable locally.
type remoteLockTable struct {
	logger                      *log.MOLogger
	removeLockTimeout           time.Duration
	serviceID                   string
	bind                        pb.LockTable
	client                      Client
	bindChangedHandler          func(pb.LockTable)
	allocatorStateProvider      func() allocatorState
	allocatorBindChangedHandler func(string, pb.LockTable, pb.LockTable, allocatorState, allocatorState) error
}

func newRemoteLockTable(
	serviceID string,
	removeLockTimeout time.Duration,
	binding pb.LockTable,
	client Client,
	bindChangedHandler func(pb.LockTable),
	logger *log.MOLogger,
) *remoteLockTable {
	logger = logger.With(zap.String("binding", binding.DebugString()))
	l := &remoteLockTable{
		logger:             logger,
		removeLockTimeout:  removeLockTimeout,
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
	cb func(pb.Result, error),
) {
	v2.TxnRemoteLockTotalCounter.Inc()

	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.remote")
	defer span.End()

	logRemoteLock(l.logger, txn, rows, opts, l.bind)

	req := acquireRequest()
	defer releaseRequest(req)

	req.LockTable = l.bind
	req.Method = pb.Method_Lock
	req.Lock.Options = opts.LockOptions
	req.Lock.TxnID = txn.txnID
	req.Lock.ServiceID = l.serviceID
	req.Lock.Rows = rows

	if err := ctx.Err(); err != nil {
		logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
		cb(pb.Result{}, err)
		return
	}

	// rpc maybe wait too long, to avoid deadlock, we need unlock txn, and lock again
	// after rpc completed
	txn.Unlock()

	// When session-level lock_wait_timeout is set, bound the RPC by that
	// timeout plus slack so the lock-table owner has enough time to observe
	// and return ErrLockTimeout before the client-side RPC deadline fires.
	// Without a session timeout, use the caller context as-is.
	var rpcCtx context.Context
	var rpcCancel context.CancelFunc
	if d := time.Duration(opts.LockWaitTimeout) * time.Second; d > 0 {
		lockRpcTimeout := d + lockRpcSlack
		rpcCtx, rpcCancel = context.WithTimeout(ctx, lockRpcTimeout)
	} else {
		rpcCtx = ctx
	}
	defer func() {
		if rpcCancel != nil {
			rpcCancel()
		}
	}()
	resp, err := l.client.Send(rpcCtx, req)

	txn.Lock()

	// txn closed
	if !bytes.Equal(req.Lock.TxnID, txn.txnID) {
		cb(pb.Result{}, ErrTxnNotFound)
		return
	}
	if txn.bindChanged {
		cb(pb.Result{}, ErrLockTableBindChanged)
		return
	}

	if err == nil {
		defer releaseResponse(resp)
		if resp.NewBind != nil {
			txn.Unlock()
			err = l.maybeHandleBindChanged(resp)
			txn.Lock()
			if !bytes.Equal(req.Lock.TxnID, txn.txnID) {
				cb(pb.Result{}, ErrTxnNotFound)
				return
			}
			if txn.bindChanged {
				cb(pb.Result{}, ErrLockTableBindChanged)
				return
			}
			logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
			cb(pb.Result{}, err)
			return
		}

		err = txn.lockAdded(l.bind.Group, l.bind, rows, l.logger)
		logRemoteLockAdded(l.logger, txn, rows, opts, l.bind)
		cb(resp.Lock.Result, err)
		return
	}

	// The request may have reached the remote owner and acquired locks even if
	// the response was lost or the client-side context timed out. Keep local
	// bookkeeping so normal transaction close can send the remote unlock.
	_ = txn.lockAdded(l.bind.Group, l.bind, rows, l.logger)
	logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
	// encounter any error, we need try to check bind is valid.
	// And use origin error to return, because once handlerError
	// swallows the error, the transaction will not be abort.
	originalErr := err
	txn.Unlock()
	e := l.handleError(err, true)
	txn.Lock()
	if !bytes.Equal(req.Lock.TxnID, txn.txnID) {
		cb(pb.Result{}, ErrTxnNotFound)
		return
	}
	if txn.bindChanged {
		cb(pb.Result{}, ErrLockTableBindChanged)
		return
	}
	if e != nil {
		err = e
	} else {
		// handleError returned nil, meaning bind changed and error was swallowed
		// This is a critical issue: lock failed but error was swallowed, transaction may continue incorrectly
		// Return ErrLockTableBindChanged to trigger retry in lockWithRetry
		l.logger.Error("CRITICAL: lock failed but error swallowed due to bind change",
			zap.String("txn-id", hex.EncodeToString(txn.txnID)),
			zap.Uint64("table-id", l.bind.Table),
			zap.String("original-error", originalErr.Error()),
			zap.String("bind", l.bind.DebugString()),
		)
		// Return ErrLockTableBindChanged to trigger retry, preventing transaction from continuing without lock
		err = moerr.NewLockTableBindChangedNoCtx()
	}
	cb(pb.Result{}, err)
}

func (l *remoteLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) {
	logUnlockTableOnRemote(
		l.logger,
		txn,
		l.bind,
	)
	retryCount := 0
	backoff := remoteRetryInitialBackoff
	for {
		err := l.doUnlock(txn, commitTS, mutations...)
		if err == nil {
			return
		}

		retryCount++
		// Rate limit unlock error logs: log first 3, then every 100th
		if retryCount <= 3 || retryCount%100 == 0 {
			logUnlockTableOnRemoteFailedWithCount(
				l.logger,
				txn,
				l.bind,
				err,
				retryCount,
			)
		}
		// unlock cannot fail and must ensure that all locks have been
		// released.
		//
		// handleError returns nil meaning bind changed, then all locks
		// will be released. If handleError returns any error, it means
		// that the current bind is valid, retry unlock.
		if err := l.handleError(err, false); err == nil {
			return
		}
		waitRemoteRetryBackoff(backoff)
		backoff = nextRemoteRetryBackoff(backoff)
	}
}

func (l *remoteLockTable) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	backoff := remoteRetryInitialBackoff
	for {
		lock, ok, err := l.doGetLock(key, txn)
		if err == nil {
			if ok {
				fn(lock)
				lock.close(notifyValue{})
			}
			return
		}

		// why use loop is similar to unlock
		if err = l.handleError(err, false); err == nil {
			return
		}
		waitRemoteRetryBackoff(backoff)
		backoff = nextRemoteRetryBackoff(backoff)
	}
}

func (l *remoteLockTable) getLockHolder(ctx context.Context, key []byte) (pb.WaitTxn, bool, error) {
	backoff := remoteRetryInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return pb.WaitTxn{}, false, err
		}
		holder, ok, err := l.doGetLockHolder(ctx, key)
		if err == nil {
			return holder, ok, nil
		}
		if err := ctx.Err(); err != nil {
			return pb.WaitTxn{}, false, err
		}
		if err = l.handleError(err, false); err == nil {
			// The bind-change handler replaces the lock-table object in service.tableGroups.
			// This in-flight remote table still carries the stale bind, so let the service
			// reacquire the current table before retrying the holder lookup.
			return pb.WaitTxn{}, false, ErrLockTableBindChanged
		}
		if err := waitRemoteRetryBackoffWithContext(ctx, backoff); err != nil {
			return pb.WaitTxn{}, false, err
		}
		backoff = nextRemoteRetryBackoff(backoff)
	}
}

func waitRemoteRetryBackoff(backoff time.Duration) {
	if backoff > 0 {
		time.Sleep(backoff)
	}
}

func waitRemoteRetryBackoffWithContext(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRemoteRetryBackoff(backoff time.Duration) time.Duration {
	if backoff <= 0 {
		return remoteRetryInitialBackoff
	}
	backoff *= 2
	if backoff > remoteRetryMaxBackoff {
		return remoteRetryMaxBackoff
	}
	return backoff
}

func (l *remoteLockTable) doUnlock(
	txn *activeTxn,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), defaultRPCTimeout, moerr.CauseDoUnlock)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_Unlock
	req.LockTable = l.bind
	req.Unlock.TxnID = txn.txnID
	req.Unlock.CommitTS = commitTS
	req.Unlock.Mutations = mutations

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		defer releaseResponse(resp)
		return l.maybeHandleBindChanged(resp)
	}
	return moerr.AttachCause(ctx, err)
}

func (l *remoteLockTable) doGetLock(key []byte, txn pb.WaitTxn) (Lock, bool, error) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), defaultRPCTimeout, moerr.CauseDoGetLock)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_GetTxnLock
	req.LockTable = l.bind
	req.GetTxnLock.Row = key
	req.GetTxnLock.TxnID = txn.TxnID

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		defer releaseResponse(resp)
		if err := l.maybeHandleBindChanged(resp); err != nil {
			return Lock{}, false, err
		}

		wq := newWaiterQueue()
		wq.init(l.logger)
		lock := Lock{
			holders: newHolders(),
			waiters: wq,
			value:   byte(resp.GetTxnLock.Value),
		}
		lock.holders.add(txn)
		for _, v := range resp.GetTxnLock.WaitingList {
			w := acquireWaiter(v, "doGetLock", l.logger)
			lock.addWaiter(l.logger, w)
			w.close("doGetLock", l.logger)
		}
		return lock, true, nil
	}
	return Lock{}, false, moerr.AttachCause(ctx, err)
}

func (l *remoteLockTable) doGetLockHolder(ctx context.Context, key []byte) (pb.WaitTxn, bool, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, defaultRPCTimeout, moerr.CauseDoGetLock)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_GetLockHolder
	req.LockTable = l.bind
	req.GetLockHolder.Row = key
	req.GetLockHolder.Sharding = l.bind.Sharding

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		defer releaseResponse(resp)
		if err := l.maybeHandleBindChanged(resp); err != nil {
			return pb.WaitTxn{}, false, err
		}
		if len(resp.GetLockHolder.Holder.TxnID) == 0 {
			return pb.WaitTxn{}, false, nil
		}
		return resp.GetLockHolder.Holder, true, nil
	}
	return pb.WaitTxn{}, false, moerr.AttachCause(ctx, err)
}

func (l *remoteLockTable) getBind() pb.LockTable {
	return l.bind
}

func (l *remoteLockTable) close(reason closeReason) {
	logLockTableClosed(l.logger, l.bind, true, reason)
}

func (l *remoteLockTable) handleError(
	err error,
	mustHandleLockBindChangedErr bool,
) error {
	if retryRemoteLockError(err) {
		err = moerr.NewBackendCannotConnectNoCtx(err.Error())
	}
	oldError := err
	// ErrLockTableBindChanged error must already handled. Skip
	if !mustHandleLockBindChangedErr && moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
		return nil
	}

	// any other errors, retry.
	// Note. Since the cn where the remote lock table is located may
	// be permanently gone, we need to go to the allocator to check if
	// the bind is valid.
	requestAllocator := allocatorState{}
	if l.allocatorStateProvider != nil {
		requestAllocator = l.allocatorStateProvider()
	}
	new, allocator, err := getLockTableBind(
		l.client,
		l.bind.Group,
		l.bind.Table,
		l.bind.OriginTable,
		l.serviceID,
		l.bind.Sharding,
	)
	if err != nil {
		logGetRemoteBindFailed(l.logger, l.bind.Table, err)
		return oldError
	}
	if new.Changed(l.bind) {
		if l.allocatorBindChangedHandler != nil {
			return l.allocatorBindChangedHandler(
				"remote-bind-refresh",
				l.bind,
				new,
				allocator,
				requestAllocator)
		}
		l.bindChangedHandler(new)
		return nil
	}
	return oldError
}

func retryRemoteLockError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded) ||
		moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF) {
		return true
	}
	return false
}

func (l *remoteLockTable) maybeHandleBindChanged(resp *pb.Response) error {
	if resp.NewBind == nil {
		return nil
	}
	newBind := resp.NewBind
	if l.allocatorBindChangedHandler != nil &&
		l.allocatorStateProvider != nil &&
		l.bind.AllocatorID != "" &&
		newBind.AllocatorID != "" &&
		l.bind.AllocatorID != newBind.AllocatorID {
		requestAllocator := l.allocatorStateProvider()
		if err := l.allocatorBindChangedHandler(
			"remote-new-bind",
			l.bind,
			*newBind,
			allocatorState{id: newBind.AllocatorID, version: newBind.Version},
			requestAllocator); err != nil {
			return err
		}
		return ErrLockTableBindChanged
	}
	l.bindChangedHandler(*newBind)
	return ErrLockTableBindChanged
}

func isRetryError(err error) bool {
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return false
	}
	return true
}
