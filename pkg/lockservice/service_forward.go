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
		return pb.Result{}, err
	}
	defer releaseResponse(resp)
	return resp.Lock.Result, nil
}
