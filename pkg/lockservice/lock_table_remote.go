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
	// the effective lock-wait deadline. The client-side RPC context must outlive
	// the owner-side wait timer long enough to carry ErrLockTimeout back.
	// Without this slack, the client deadline can fire before the owner returns
	// ErrLockTimeout, causing the client to see a retryable connectivity error
	// instead of a lock-timeout result.
	lockRpcSlack = 30 * time.Second
)

// newLockRPCContext bounds the transport by the effective lock deadline while
// preserving an earlier caller deadline. The slack applies only to RPC
// delivery: the owner-side waiter still enforces LockWaitDeadline exactly, and
// the extra time lets its ErrLockTimeout response reach the caller instead of
// being replaced by a retryable transport timeout.
func newLockRPCContext(ctx context.Context, opts pb.LockOptions) (context.Context, context.CancelFunc) {
	if opts.LockWaitDeadline > 0 {
		return context.WithDeadline(ctx, time.Unix(0, opts.LockWaitDeadline).Add(lockRpcSlack))
	}
	if d := time.Duration(opts.LockWaitTimeout) * time.Second; d > 0 {
		return context.WithTimeout(ctx, d+lockRpcSlack)
	}
	return ctx, nil
}

// carryEarlierContextDeadline copies an earlier caller deadline into the lock
// request itself. MORPC bounds the origin-side Future with ctx, but the owner
// handler is not guaranteed to observe that exact context deadline on every
// transport/lifecycle path. The absolute option is therefore the durable
// cross-CN budget; the relative seconds field remains only a compatibility
// fallback for peers that do not consume LockWaitDeadline.
func carryEarlierContextDeadline(ctx context.Context, opts pb.LockOptions) pb.LockOptions {
	deadline, ok := ctx.Deadline()
	if !ok {
		return opts
	}
	if opts.LockWaitDeadline == 0 || deadline.Before(time.Unix(0, opts.LockWaitDeadline)) {
		opts.LockWaitDeadline = deadline.UnixNano()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// New peers reject the expired absolute deadline immediately. Keep a
			// one-second relative fallback so an older peer that ignores the
			// deadline cannot turn this race into an unbounded wait.
			opts.LockWaitTimeout = 1
		} else {
			opts.LockWaitTimeout = int64(remaining / time.Second)
			if remaining%time.Second != 0 {
				opts.LockWaitTimeout++
			}
		}
	}
	return opts
}

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

	effectiveOptions := carryEarlierContextDeadline(ctx, opts.LockOptions)
	lockBudgetCtx, cancelLockBudget := newLockWaitContext(ctx, effectiveOptions)
	if cancelLockBudget != nil {
		defer cancelLockBudget()
	}

	req.LockTable = l.bind
	req.Method = pb.Method_Lock
	req.Lock.Options = effectiveOptions
	req.Lock.TxnID = txn.txnID
	req.Lock.ServiceID = l.serviceID
	req.Lock.Rows = rows

	if err := lockBudgetCtx.Err(); err != nil {
		err = lockWaitContextError(lockBudgetCtx, err)
		logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
		cb(pb.Result{}, err)
		return
	}
	if lockWaitDeadlineExpired(effectiveOptions, time.Now()) {
		cb(pb.Result{}, ErrLockTimeout)
		return
	}

	// rpc maybe wait too long, to avoid deadlock, we need unlock txn, and lock again
	// after rpc completed
	txn.Unlock()

	// Bound the RPC by the absolute lock deadline plus transport slack so the
	// lock-table owner has enough time to return ErrLockTimeout before the
	// client-side RPC deadline fires.
	// Service entry points also use this field for the safety ceiling. A zero
	// value is possible only for direct lock-table callers and tests, where the
	// caller context remains the fallback.
	rpcCtx, rpcCancel := newLockRPCContext(ctx, effectiveOptions)
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
			err = l.maybeHandleBindChanged(lockBudgetCtx, resp)
			if ctx.Err() == nil {
				if ctxErr := lockBudgetCtx.Err(); ctxErr != nil {
					err = lockWaitContextError(lockBudgetCtx, ctxErr)
				}
			}
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
	// Both owner-cap and caller lock-budget timeouts are terminal. Bind recovery
	// uses an independent allocator RPC and must not extend an exhausted lock
	// budget by another defaultRPCTimeout.
	if moerr.IsMoErrCode(err, moerr.ErrRemoteLockWaitTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout) {
		cb(pb.Result{}, err)
		return
	}
	if ctx.Err() != nil {
		// Preserve the historical transport error while still skipping bind
		// recovery. The failed RPC may have acquired remotely, so the transaction
		// bookkeeping above remains necessary for compensating Unlock.
		if retryRemoteLockError(err) {
			err = moerr.NewBackendCannotConnectNoCtx(err.Error())
		}
		cb(pb.Result{}, err)
		return
	}
	if lockWaitDeadlineExpired(opts.LockOptions, time.Now()) {
		cb(pb.Result{}, ErrLockTimeout)
		return
	}
	if ctxErr := lockBudgetCtx.Err(); ctxErr != nil {
		cb(pb.Result{}, lockWaitContextError(lockBudgetCtx, ctxErr))
		return
	}
	// encounter any error, we need try to check bind is valid.
	// And use origin error to return, because once handlerError
	// swallows the error, the transaction will not be abort.
	originalErr := err
	txn.Unlock()
	e := l.handleErrorWithContext(lockBudgetCtx, err, true)
	if ctx.Err() != nil {
		e = originalErr
		if retryRemoteLockError(e) {
			e = moerr.NewBackendCannotConnectNoCtx(e.Error())
		}
	} else if ctxErr := lockBudgetCtx.Err(); ctxErr != nil {
		e = lockWaitContextError(lockBudgetCtx, ctxErr)
	}
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
	_ = l.unlockWithContext(context.Background(), txn, ls, commitTS, mutations...)
}

func (l *remoteLockTable) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	logUnlockTableOnRemote(
		l.logger,
		txn,
		l.bind,
	)
	retryCount := 0
	backoff := remoteRetryInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := l.doUnlock(ctx, txn, commitTS, mutations...)
		if err == nil {
			return nil
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
		if err := l.handleErrorWithContext(ctx, err, false); err == nil {
			return nil
		}
		if err := waitRemoteRetryBackoffWithContext(ctx, backoff); err != nil {
			return err
		}
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
	parent context.Context,
	txn *activeTxn,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	ctx, cancel := context.WithTimeoutCause(parent, defaultRPCTimeout, moerr.CauseDoUnlock)
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
		return l.maybeHandleBindChanged(ctx, resp)
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
		if err := l.maybeHandleBindChanged(ctx, resp); err != nil {
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
			// WaitingList is filtered by the remote owner to active wait-for
			// edges. Keep that snapshot semantics separate from the normal
			// waiter status machine: remoteLockTable.getLock closes this
			// synthetic lock immediately after the callback.
			w.isRemoteSnapshot = true
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
		if err := l.maybeHandleBindChanged(ctx, resp); err != nil {
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
	return l.handleErrorWithContext(
		context.Background(),
		err,
		mustHandleLockBindChangedErr,
	)
}

func (l *remoteLockTable) handleErrorWithContext(
	ctx context.Context,
	err error,
	mustHandleLockBindChangedErr bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	new, allocator, err := getLockTableBindWithContext(
		ctx,
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

func (l *remoteLockTable) maybeHandleBindChanged(
	ctx context.Context,
	resp *pb.Response,
) error {
	if resp.NewBind == nil {
		return nil
	}
	newBind := *resp.NewBind
	if l.allocatorBindChangedHandler != nil &&
		l.allocatorStateProvider != nil &&
		l.client != nil {
		// NewBind belongs to the owner response's point in time. The local
		// service may have observed a newer allocator while the RPC was in
		// flight, so only publish a bind refreshed from the current allocator.
		requestAllocator := l.allocatorStateProvider()
		refreshedBind, allocator, err := getLockTableBindWithContext(
			ctx,
			l.client,
			l.bind.Group,
			l.bind.Table,
			l.bind.OriginTable,
			l.serviceID,
			l.bind.Sharding,
		)
		if err != nil {
			logGetRemoteBindFailed(l.logger, l.bind.Table, err)
			return ErrLockTableBindChanged
		}
		if !refreshedBind.Changed(l.bind) {
			return ErrLockTableBindChanged
		}
		if err := l.allocatorBindChangedHandler(
			"remote-new-bind",
			l.bind,
			refreshedBind,
			allocator,
			requestAllocator); err != nil {
			return err
		}
		return ErrLockTableBindChanged
	}
	l.bindChangedHandler(newBind)
	return ErrLockTableBindChanged
}

func isRetryError(err error) bool {
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return false
	}
	return true
}
