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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func (s *service) forwardLock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	opts pb.LockOptions) (pb.Result, error) {
	bindCtx, cancelBind := newLockWaitContext(ctx, opts)
	if cancelBind != nil {
		defer cancelBind()
	}
	l, err := s.getLockTableWithCreateContext(
		bindCtx,
		opts.Group,
		tableID,
		rows,
		opts.Sharding)
	if err != nil {
		return pb.Result{}, err
	}
	if err := bindCtx.Err(); err != nil {
		return pb.Result{}, lockWaitContextError(bindCtx, err)
	}
	if lockWaitDeadlineExpired(opts, time.Now()) {
		return pb.Result{}, ErrLockTimeout
	}

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_ForwardLock
	if opts.NoWait {
		req.Method = pb.Method_ForwardTryLock
	}
	req.LockTable = l.getBind()
	req.Lock.Options = opts
	req.Lock.TxnID = txnID
	req.Lock.ServiceID = s.serviceID
	req.Lock.Rows = rows

	// ForwardTo is an RPC just like a regular remote-table lock. In particular,
	// morpc requires a deadline and a background caller may not have one, so use
	// the effective lock deadline injected at service entry.
	rpcCtx, cancel := newLockRPCContext(ctx, opts)
	if cancel != nil {
		defer cancel()
	}
	resp, err := s.remote.client.Send(rpcCtx, req)
	if err != nil {
		if opts.NoWait && moerr.IsMoErrCode(err, moerr.ErrLockConflict) {
			// A forwarded FastFail conflict is recoverable only when its typed
			// response proves the physical owner and cleanup capabilities. A raw
			// application error from an older or malformed peer is ambiguous.
			return pb.Result{LockedOn: l.getBind()}, ErrLockTableBindChanged
		}
		if opts.NoWait {
			// Once the forwarding RPC was sent, its owner may have acquired a
			// prefix before the response was lost. Return the known physical route
			// so the transaction client can retain an unlock binding.
			return pb.Result{LockedOn: l.getBind()}, err
		}
		return pb.Result{}, err
	}
	defer releaseResponse(resp)
	if resp.NewBind != nil {
		return pb.Result{}, ErrLockTableBindChanged
	}
	if resp.Lock.NoWaitConflict {
		result := resp.Lock.Result
		if !opts.NoWait ||
			!validNoWaitLockOptions(opts) ||
			!supportsNonblockingLockProtocol(s.cfg.ServiceID) ||
			!resp.Lock.TxnWaitingListOnLockTableSupported ||
			!resp.Lock.BatchUnlockSupported ||
			!result.LockedOn.Valid ||
			result.LockedOn.Changed(l.getBind()) {
			return result, ErrLockTableBindChanged
		}
		return result, ErrLockConflict
	}
	return resp.Lock.Result, nil
}
