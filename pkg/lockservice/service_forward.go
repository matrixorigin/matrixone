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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"

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
	l, err := s.getForwardBinding(bindCtx, tableID, rows, txnID, opts)
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
	// A cold route lookup can advance the allocator epoch before this RPC.
	requestAllocator := s.allocatorStateSnapshot()
	resp, err := s.remote.client.Send(rpcCtx, req)
	if err != nil {
		return pb.Result{}, err
	}
	defer releaseResponse(resp)
	if bind := resp.NewBind; bind != nil {
		// The coordinator already queried the allocator with the transaction
		// fence in scope. Publish that result without another network wait.
		allocator := allocatorState{id: resp.GetBind.AllocatorID, version: resp.GetBind.AllocatorVersion}
		if allocator == (allocatorState{}) ||
			!validForwardBinding(*bind, req.LockTable.Group, req.LockTable.Table, tableID, req.LockTable.Sharding) {
			return pb.Result{}, ErrLockTableBindChanged
		}
		if err := s.handleBindChangedFromAllocator("forward-lock", req.LockTable, *bind, allocator, requestAllocator); err != nil {
			return pb.Result{}, err
		}
		return pb.Result{}, ErrLockTableBindChanged
	}
	if !resp.Lock.Result.LockedOn.Valid {
		return pb.Result{}, ErrLockTableBindChanged
	}
	return resp.Lock.Result, nil
}

func (s *service) getForwardBinding(
	ctx context.Context, origin uint64, rows [][]byte, txnID []byte, opts pb.LockOptions,
) (lockTable, error) {
	table := origin
	if opts.Sharding == pb.Sharding_ByRow {
		table = ShardingByRow(rows[0])
	}
	if cached := s.tableGroups.get(opts.Group, table); cached != nil {
		return cached, nil
	}

	// Only a full service ID identifies the coordinator's incarnation.
	if opts.ForwardTo == getUUIDFromServiceIdentifier(opts.ForwardTo) {
		return s.getLockTableWithCreateContext(ctx, opts.Group, origin, rows, opts.Sharding, nil, false)
	}

	// A mirror table-wide allocation can belong to an unrelated transaction.
	// Resolve a cold route through this transaction's coordinator, before
	// waiting on any mirror-local allocation or admitting a lock.
	req := acquireRequest()
	defer releaseRequest(req)
	req.Method = pb.Method_ResolveForwardBind
	req.Lock.Options.ForwardTo = opts.ForwardTo
	req.Lock.TxnID = txnID
	req.GetBind = pb.GetBindRequest{
		ServiceID:   s.serviceID,
		Table:       table,
		OriginTable: origin,
		Sharding:    opts.Sharding,
		Group:       opts.Group,
	}
	requestAllocator := s.allocatorStateSnapshot()
	resp, err := sendBindLookupRequest(ctx, s.remote.client, req)
	if err != nil {
		if moerr.IsLockTableBindChangedForFencedTxn(err) {
			return nil, err
		}
		return nil, lockWaitContextError(ctx, err)
	}
	defer releaseResponse(resp)
	bind := resp.GetBind.LockTable
	if !validForwardBinding(bind, opts.Group, table, origin, opts.Sharding) {
		return nil, ErrLockTableBindChanged
	}
	return s.publishLockTableBindFromAllocator(ctx, "forward-lock", opts.Group, table, bind,
		allocatorState{id: resp.GetBind.AllocatorID, version: resp.GetBind.AllocatorVersion}, requestAllocator)
}

func validForwardBinding(bind pb.LockTable, group uint32, table, origin uint64, sharding pb.Sharding) bool {
	return bind.Valid && bind.ServiceID != "" && bind.Group == group &&
		bind.Table == table && bind.OriginTable == origin && bind.Sharding == sharding
}

// Preserve coordinator workers for Unlock and heartbeat when TN is slow.
const maxForwardBindResolvers = 3

func (s *service) acquireForwardBindSlot(ctx context.Context) error {
	for {
		active := s.forwardBindResolvers.Load()
		if active >= maxForwardBindResolvers {
			return moerr.NewForwardBindBusy(ctx)
		}
		if s.forwardBindResolvers.CompareAndSwap(active, active+1) {
			return nil
		}
	}
}

func (s *service) handleResolveForwardBind(
	ctx context.Context, cancel context.CancelFunc, req *pb.Request, resp *pb.Response, cs morpc.ClientSession,
) {
	if req.Lock.Options.ForwardTo != s.serviceID {
		writeResponse(s.logger, cancel, resp, ErrLockTableBindChanged, cs)
		return
	}
	if err := s.checkFencedTxn(req.Lock.TxnID); err != nil {
		writeResponse(s.logger, cancel, resp, err, cs)
		return
	}
	serviceCtx, admitted := s.beginServiceOperation()
	if !admitted {
		writeResponse(s.logger, cancel, resp, ErrLockTableBindChanged, cs)
		return
	}
	defer s.endServiceOperation()
	ctx, cancelClose := contextWithServiceClose(ctx, serviceCtx)
	defer cancelClose()

	bind, allocator, err := s.resolveForwardBind(ctx, req)
	if err == nil {
		resp.GetBind = pb.GetBindResponse{
			LockTable: bind, AllocatorID: allocator.id, AllocatorVersion: allocator.version,
		}
	}
	writeResponse(s.logger, cancel, resp, err, cs)
}

func (s *service) resolveForwardBind(
	ctx context.Context, req *pb.Request,
) (bind pb.LockTable, allocator allocatorState, err error) {
	ctx, finish, err := s.bindLookupContext(ctx, req.Lock.TxnID)
	if err != nil {
		return bind, allocator, err
	}
	defer finish(&err)
	if err := s.acquireForwardBindSlot(ctx); err != nil {
		return bind, allocator, err
	}
	defer s.forwardBindResolvers.Add(-1)

	lookup := req.GetBind
	return getLockTableBindWithContext(ctx, s.remote.client, lookup.Group, lookup.Table,
		lookup.OriginTable, lookup.ServiceID, lookup.Sharding)
}

func (s *service) refreshForwardBinding(
	ctx context.Context, previous pb.LockTable, origin uint64, txnID []byte,
) (bind pb.LockTable, allocator allocatorState, err error) {
	ctx, finish, err := s.bindLookupContext(ctx, txnID)
	if err != nil {
		return bind, allocator, err
	}
	defer finish(&err)
	if err := s.acquireForwardBindSlot(ctx); err != nil {
		return bind, allocator, err
	}
	defer s.forwardBindResolvers.Add(-1)

	requestAllocator := s.allocatorStateSnapshot()
	bind, allocator, err = getLockTableBindWithContext(
		ctx, s.remote.client, previous.Group, previous.Table,
		origin, s.serviceID, previous.Sharding,
	)
	if err != nil {
		if ctx.Err() != nil {
			return bind, allocator, lockWaitContextError(ctx, ctx.Err())
		}
		return bind, allocator, ErrLockTableBindChanged
	}
	err = s.handleBindChangedFromAllocator("forward-lock", previous, bind, allocator, requestAllocator)
	return
}
