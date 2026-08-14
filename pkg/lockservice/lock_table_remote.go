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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	remoteRetryInitialBackoff = 100 * time.Millisecond
	remoteRetryMaxBackoff     = 5 * time.Second
	// Bound one remote wait-for lookup without limiting the number of lock keys
	// or transactions that a deadlock check can traverse.
	remoteLockSnapshotTimeout = 3*defaultRPCTimeout + remoteRetryMaxBackoff
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
		return context.WithDeadlineCause(
			ctx,
			time.Unix(0, opts.LockWaitDeadline).Add(lockRpcSlack),
			context.DeadlineExceeded,
		)
	}
	if d := time.Duration(opts.LockWaitTimeout) * time.Second; d > 0 {
		return context.WithTimeoutCause(ctx, d+lockRpcSlack, context.DeadlineExceeded)
	}
	return ctx, nil
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

	req.LockTable = l.bind
	req.Method = pb.Method_Lock
	req.Lock.Options = opts.LockOptions
	req.Lock.TxnID = txn.txnID
	req.Lock.ServiceID = l.serviceID
	req.Lock.Rows = rows
	if opts.replaceTxnLocks && len(opts.originalRows) > 0 {
		// Coarsening is an owner-side physical representation decision. Send the
		// logical request so the authoritative owner can re-plan from its current
		// ledger and can retry it exactly if a wait invalidates eligibility.
		req.Lock.Rows = opts.originalRows
		req.Lock.Options = opts.originalOptions
	}

	if err := ctx.Err(); err != nil {
		logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
		cb(pb.Result{}, err)
		return
	}
	if lockWaitDeadlineExpired(opts.LockOptions, time.Now()) {
		cb(pb.Result{}, ErrLockTimeout)
		return
	}
	txnGeneration := txn.generation

	// rpc maybe wait too long, to avoid deadlock, we need unlock txn, and lock again
	// after rpc completed
	txn.Unlock()

	// Bound the RPC by the absolute lock deadline plus transport slack so the
	// lock-table owner has enough time to return ErrLockTimeout before the
	// client-side RPC deadline fires.
	// Service entry points also use this field for the safety ceiling. A zero
	// value is possible only for direct lock-table callers and tests, where the
	// caller context remains the fallback.
	rpcCtx, rpcCancel := newLockRPCContext(ctx, opts.LockOptions)
	defer func() {
		if rpcCancel != nil {
			rpcCancel()
		}
	}()
	resp, err := l.client.Send(rpcCtx, req)

	txn.Lock()

	// txn closed
	if txnGeneration != txn.generation ||
		!bytes.Equal(req.Lock.TxnID, txn.txnID) {
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
			err = l.maybeHandleBindChanged(ctx, resp)
			txn.Lock()
			if txnGeneration != txn.generation ||
				!bytes.Equal(req.Lock.TxnID, txn.txnID) {
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

		txn.markRemoteUnlockRequiredLocked(l.bind.Group, l.bind.Table)
		ownerLocalSnapshot := resp.Lock.TxnWaitingListOnLockTableSupported
		recordRows := rows
		recordOptions := opts.LockOptions
		if opts.replaceTxnLocks && len(opts.originalRows) > 0 {
			// A concurrent Shared/sharded acquisition can invalidate the same
			// origin-side plan while the RPC is in flight. In that case the owner
			// retries the logical request exactly, so those are also the keys whose
			// deadlock probes must be retained.
			recordRows = opts.originalRows
			recordOptions = opts.originalOptions
		}
		if opts.requireOwnerLocalWaitSnapshot && !ownerLocalSnapshot {
			// This request is the physical holder generation for a local Shared
			// proxy. Without the v20 owner-local snapshot/table-scoped-unlock
			// contract the proxy must not publish cache-only holders. The owner has
			// already granted the lock, so retain a confirmed cleanup route before
			// fencing the transaction.
			trackingErr := txn.ensureRemoteLockTableTracked(
				l.bind.Group,
				l.bind,
				recordRows,
				recordOptions,
				true,
				l.logger,
			)
			txn.markBindChangedLocked(l.logger)
			if trackingErr != nil {
				cb(pb.Result{}, errors.Join(ErrLockTableBindChanged, trackingErr))
			} else {
				cb(pb.Result{}, ErrLockTableBindChanged)
			}
			return
		}
		if !ownerLocalSnapshot &&
			txn.hasOwnerLocalWaitSnapshotLocked(l.bind.Group, l.bind.Table) {
			// The origin may already have compacted this table's probe ledger.
			// Falling back to per-key traversal after a peer downgrade would miss
			// edges, so fence the transaction and retain its table-level cleanup
			// route instead of changing semantics in place.
			txn.markBindChangedLocked(l.logger)
			cb(pb.Result{}, ErrLockTableBindChanged)
			return
		}
		if ownerLocalSnapshot {
			txn.markOwnerLocalWaitSnapshotLocked(l.bind.Group, l.bind.Table)
		}
		if !ownerLocalSnapshot {
			// A legacy owner has no authoritative transaction-level snapshot.
			// Reconcile the complete exact probe ledger on every acknowledged
			// success, including NewLockAdd=false after a lost successful response.
			// Only missing keys are appended, keeping re-entry bounded.
			err = txn.reconcileLegacyRemoteLocks(
				l.bind.Group,
				l.bind,
				recordRows,
				recordOptions,
				l.logger,
			)
		} else if !opts.requireOwnerLocalWaitSnapshot {
			// With v20 the physical owner is authoritative for wait-for traversal,
			// and remote Unlock releases the whole table by transaction ID. Record
			// that bounded route directly instead of first trying to mirror an
			// arbitrarily large owner ledger and turning physical success into a
			// smaller origin's capacity error. A local Shared proxy is the one
			// exception: it needs each cached key for local holder cleanup below.
			err = txn.ensureRemoteLockTableTracked(
				l.bind.Group,
				l.bind,
				recordRows,
				recordOptions,
				true,
				l.logger,
			)
		} else if resp.Lock.Result.NewLockAdd {
			err = txn.remoteLockAdded(
				l.bind.Group,
				l.bind,
				recordRows,
				recordOptions,
				l.logger,
			)
		} else {
			// The owner reused physical ownership already held by this transaction.
			// The proxy must not publish that generation into its Shared cache: the
			// existing owner lock may be Exclusive or a covering range. Only its
			// bounded table-level cleanup route is needed here.
			err = txn.ensureRemoteLockTableTracked(
				l.bind.Group,
				l.bind,
				recordRows,
				recordOptions,
				true,
				l.logger,
			)
		}
		if err != nil {
			// The owner has already committed. This origin may have a smaller
			// fixed-slice pool during a rolling configuration change, so failure
			// to retain the detailed probe ledger must still leave one bounded
			// table route for the eventual transaction-ID unlock.
			trackingErr := txn.ensureRemoteLockTableTracked(
				l.bind.Group,
				l.bind,
				req.Lock.Rows,
				req.Lock.Options,
				true,
				l.logger,
			)
			if trackingErr != nil {
				err = errors.Join(err, trackingErr)
			}
			if !ownerLocalSnapshot {
				// A legacy table cannot fall back to an owner snapshot. Once the
				// acknowledged physical ownership exceeds this origin's exact probe
				// capacity, fence the transaction and retain only its cleanup route.
				txn.markBindChangedLocked(l.logger)
			} else if opts.requireOwnerLocalWaitSnapshot && trackingErr == nil &&
				moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
				// The physical owner and its authoritative wait snapshot succeeded,
				// but this origin cannot retain another exact proxy-cache key. Tell
				// the proxy to complete this transaction through the bounded cleanup
				// route without publishing a local Shared representative. This
				// sentinel is consumed inside localLockTableProxy and never reaches
				// the Lock API.
				err = errRetryUncachedProxyLock
			}
		}
		logRemoteLockAdded(l.logger, txn, rows, opts, l.bind)
		cb(resp.Lock.Result, err)
		return
	}

	// Transport failures are indeterminate. Keep one unconfirmed witness for
	// cleanup; remote unlock releases the owner's complete table ownership by
	// transaction ID. Unconfirmed rows are excluded from wait-for traversal, so
	// an ambiguous failed Lock cannot invent holder edges.
	// ErrNotSupported is an application response proving that the owner published
	// no ownership.
	if !moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		txn.markRemoteUnlockRequiredLocked(l.bind.Group, l.bind.Table)
		if trackingErr := txn.ensureRemoteLockTableTracked(
			l.bind.Group,
			l.bind,
			req.Lock.Rows,
			req.Lock.Options,
			false,
			l.logger,
		); trackingErr != nil {
			err = errors.Join(err, trackingErr)
		}
	}
	logRemoteLockFailed(l.logger, txn, rows, opts, l.bind, err)
	if moerr.IsMoErrCode(err, moerr.ErrRemoteLockWaitTimeout) {
		cb(pb.Result{}, err)
		return
	}
	// encounter any error, we need try to check bind is valid.
	// And use origin error to return, because once handlerError
	// swallows the error, the transaction will not be abort.
	originalErr := err
	txn.Unlock()
	e := l.handleErrorWithContext(ctx, err, true)
	txn.Lock()
	if txnGeneration != txn.generation ||
		!bytes.Equal(req.Lock.TxnID, txn.txnID) {
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
		err = ErrLockTableBindChanged
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
		// Rate limit unlock error logs: log first 3, then every 100th.
		// Deterministic owner rejections return after this first observation.
		if retryCount <= 3 || retryCount%100 == 0 {
			logUnlockTableOnRemoteFailedWithCount(
				l.logger,
				txn,
				l.bind,
				err,
				retryCount,
			)
		}
		if !retryRemoteUnlockError(err) {
			if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
				// The owner has authoritatively rejected this old generation. Its
				// physical table is already gone, so cleanup is complete.
				return nil
			}
			if moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) {
				// A missing table can race allocator reassignment. Resolve that one
				// ambiguity, but do not loop if the allocator still reports this bind.
				if handledErr := l.handleErrorWithContext(ctx, err, false); handledErr == nil {
					return nil
				} else {
					return handledErr
				}
			}
			// The owner rejected the ownership transition itself (for example,
			// replacement bookkeeping could not be prepared). Replaying the same
			// request cannot repair that state and used to spin forever. Let the
			// retained closing transaction surface the error and retry explicitly.
			return err
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

func retryRemoteUnlockError(err error) bool {
	return retryRemoteLockError(err) ||
		errors.Is(err, morpc.ErrBackendCreateTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendClosed)
}

func (l *remoteLockTable) getLock(
	ctx context.Context,
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) error {
	ctx, cancel := context.WithTimeoutCause(
		ctx,
		remoteLockSnapshotTimeout,
		context.DeadlineExceeded,
	)
	defer cancel()

	if err := ctx.Err(); err != nil {
		return err
	}
	backoff := remoteRetryInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		lock, ok, err := l.doGetLock(ctx, key, txn)
		if err == nil {
			if ok {
				defer lock.close(notifyValue{})
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if ok {
				fn(lock)
			}
			return nil
		}

		// why use loop is similar to unlock
		if err = l.handleErrorWithContext(ctx, err, false); err == nil {
			// The bind-change handler replaces this table in service.tableGroups.
			// Let the caller reacquire it instead of treating the stale snapshot as
			// an empty waiting list.
			return ErrLockTableBindChanged
		}
		if err := waitRemoteRetryBackoffWithContext(ctx, backoff); err != nil {
			return err
		}
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
		if err = l.handleErrorWithContext(ctx, err, false); err == nil {
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

func (l *remoteLockTable) getTxnWaitingList(
	ctx context.Context,
	txnID []byte,
) ([]pb.WaitTxn, error) {
	ctx, cancel := context.WithTimeoutCause(
		ctx,
		remoteLockSnapshotTimeout,
		context.DeadlineExceeded,
	)
	defer cancel()

	backoff := remoteRetryInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values, err := l.doGetTxnWaitingList(ctx, txnID)
		if err == nil {
			return values, nil
		}
		// Capability was negotiated by a successful Lock response. A later
		// protocol rejection or bind-generation error is terminal for this
		// snapshot, not a transport failure that can become safe by retrying.
		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) ||
			moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) ||
			moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := waitRemoteRetryBackoffWithContext(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = nextRemoteRetryBackoff(backoff)
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

func (l *remoteLockTable) doGetLock(parent context.Context, key []byte, txn pb.WaitTxn) (Lock, bool, error) {
	ctx, cancel := context.WithTimeoutCause(parent, defaultRPCTimeout, moerr.CauseDoGetLock)
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

func (l *remoteLockTable) doGetTxnWaitingList(
	parent context.Context,
	txnID []byte,
) ([]pb.WaitTxn, error) {
	ctx, cancel := context.WithTimeoutCause(
		parent,
		defaultRPCTimeout,
		moerr.CauseDoGetLock,
	)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)
	req.Method = pb.Method_GetTxnWaitingListOnLockTable
	req.GetWaitingList.Txn.TxnID = txnID
	// Route the dedicated owner-local snapshot RPC to the physical lock owner.
	// Its handler is forbidden from recursively issuing this method.
	req.GetWaitingList.Txn.CreatedOn = l.bind.ServiceID

	resp, err := l.client.Send(ctx, req)
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}
	defer releaseResponse(resp)
	return resp.GetWaitingList.WaitingList, nil
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
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
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
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
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
	// A backend-create timeout and BackendClosed are observations about this
	// local transport generation. They can also be produced by a concurrent
	// targeted reset, so neither is proof that the discovered service is dead.
	if errors.Is(err, morpc.ErrBackendCreateTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendClosed) {
		return true
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return false
	}
	return true
}
