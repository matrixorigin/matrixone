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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func (s *service) initRemote() {
	rpcClient, err := NewClient(s.cfg.RPC)
	if err != nil {
		panic(err)
	}
	rpcServer, err := NewServer(
		s.cfg.ListenAddress,
		s.cfg.RPC)
	if err != nil {
		panic(err)
	}

	s.remote.client = rpcClient
	s.remote.server = rpcServer
	s.remote.keeper = NewLockTableKeeper(
		s.cfg.ServiceID,
		rpcClient,
		s.cfg.KeepBindDuration.Duration,
		s.cfg.KeepRemoteLockDuration.Duration,
		&s.tables)
	s.initRemoteHandler()
	if err := s.remote.server.Start(); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.unlockTimeoutRemoteTxn); err != nil {
		panic(err)
	}
}

func (s *service) initRemoteHandler() {
	s.remote.server.RegisterMethodHandler(pb.Method_Lock,
		s.handleRemoteLock)
	s.remote.server.RegisterMethodHandler(pb.Method_ForwardLock,
		s.handleForwardLock)
	s.remote.server.RegisterMethodHandler(pb.Method_Unlock,
		s.handleRemoteUnlock)
	s.remote.server.RegisterMethodHandler(pb.Method_GetTxnLock,
		s.handleRemoteGetLock)
	s.remote.server.RegisterMethodHandler(pb.Method_GetWaitingList,
		s.handleRemoteGetWaitingList)
	s.remote.server.RegisterMethodHandler(pb.Method_KeepRemoteLock,
		s.handleKeepRemoteLock)
}

func (s *service) handleRemoteLock(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l, err := s.getLocalLockTable(req, resp)
	if err != nil ||
		l == nil {
		// means that the lockservice sending the lock request holds a stale
		// lock table binding.
		writeResponse(ctx, cancel, resp, err, cs)
		return
	}

	txn := s.activeTxnHolder.getActiveTxn(req.Lock.TxnID, true, req.Lock.ServiceID)
	txn.Lock()
	defer txn.Unlock()
	if !bytes.Equal(txn.txnID, req.Lock.TxnID) {
		writeResponse(ctx, cancel, resp, ErrTxnNotFound, cs)
		return
	}
	if txn.deadlockFound {
		writeResponse(ctx, cancel, resp, ErrDeadLockDetected, cs)
		return
	}

	l.lock(
		ctx,
		txn,
		req.Lock.Rows,
		LockOptions{LockOptions: req.Lock.Options, async: true},
		func(result pb.Result, err error) {
			resp.Lock.Result = result
			writeResponse(ctx, cancel, resp, err, cs)
		})
}

func (s *service) handleForwardLock(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l, err := s.getLockTable(req.LockTable.Table)
	if err != nil ||
		l == nil {
		// means that the lockservice sending the lock request holds a stale
		// lock table binding.
		writeResponse(ctx, cancel, resp, err, cs)
		return
	}

	txn := s.activeTxnHolder.getActiveTxn(req.Lock.TxnID, true, "")
	txn.Lock()
	if !bytes.Equal(txn.txnID, req.Lock.TxnID) {
		txn.Unlock()
		writeResponse(ctx, cancel, resp, ErrTxnNotFound, cs)
		return
	}
	if txn.deadlockFound {
		txn.Unlock()
		writeResponse(ctx, cancel, resp, ErrDeadLockDetected, cs)
		return
	}

	l.lock(
		ctx,
		txn,
		req.Lock.Rows,
		LockOptions{LockOptions: req.Lock.Options, async: true},
		func(result pb.Result, err error) {
			txn.Unlock()
			resp.Lock.Result = result
			writeResponse(ctx, cancel, resp, err, cs)
		})
}

func (s *service) handleRemoteUnlock(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l, err := s.getLocalLockTable(req, resp)
	if err != nil ||
		l == nil {
		// means that the lockservice sending the lock request holds a stale lock
		// table binding.
		writeResponse(ctx, cancel, resp, err, cs)
		return
	}
	err = s.Unlock(ctx, req.Unlock.TxnID, req.Unlock.CommitTS)
	writeResponse(ctx, cancel, resp, err, cs)
}

func (s *service) handleRemoteGetLock(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l, err := s.getLocalLockTable(req, resp)
	if err != nil ||
		l == nil {
		// means that the lockservice sending the lock request holds a stale lock
		// table binding.
		writeResponse(ctx, cancel, resp, err, cs)
		return
	}

	l.getLock(
		req.GetTxnLock.TxnID,
		req.GetTxnLock.Row,
		func(lock Lock) {
			n := lock.waiter.waiters.len()
			if n > 0 {
				resp.GetTxnLock.Value = int32(lock.value)
				values := make([]pb.WaitTxn, 0, n)
				lock.waiter.waiters.iter(func(w *waiter) bool {
					values = append(values, w.belongTo)
					return true
				})
				resp.GetTxnLock.WaitingList = values
			}
		})
	writeResponse(ctx, cancel, resp, err, cs)
}

func (s *service) handleRemoteGetWaitingList(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	select {
	case s.fetchWhoWaitingListC <- who{ctx: ctx, cancel: cancel, cs: cs, resp: resp, txnID: req.GetWaitingList.Txn.TxnID}:
		return
	default:
		writeResponse(ctx, cancel, resp, ErrDeadLockDetected, cs)
	}
}

func (s *service) handleKeepRemoteLock(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l, err := s.getLocalLockTable(req, resp)
	if err != nil ||
		l == nil {
		writeResponse(ctx, cancel, resp, err, cs)
		return
	}

	s.activeTxnHolder.keepRemoteActiveTxn(req.KeepRemoteLock.ServiceID)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (s *service) getLocalLockTable(
	req *pb.Request,
	resp *pb.Response) (lockTable, error) {
	l, err := s.getLockTableWithCreate(req.LockTable.Table, false)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return nil, ErrLockTableNotFound
	}
	bind := l.getBind()
	if bind.Changed(req.LockTable) {
		resp.NewBind = &bind
		return nil, nil
	}
	return l, nil
}

func (s *service) getTxnWaitingListOnRemote(
	txnID []byte,
	createdOn string) ([]pb.WaitTxn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_GetWaitingList
	req.GetWaitingList.Txn.TxnID = txnID
	req.GetWaitingList.Txn.CreatedOn = createdOn

	resp, err := s.remote.client.Send(ctx, req)
	if err != nil {
		return nil, err
	}
	defer releaseResponse(resp)
	v := resp.GetWaitingList.WaitingList
	return v, nil
}

func (s *service) unlockTimeoutRemoteTxn(ctx context.Context) {
	wait := s.cfg.RemoteLockTimeout.Duration
	timer := time.NewTimer(wait)
	defer timer.Stop()

	var timeoutTxns [][]byte
	timeoutServices := make(map[string]struct{})
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timeoutTxns, wait = s.activeTxnHolder.getTimeoutRemoveTxn(
				timeoutServices,
				timeoutTxns,
				s.cfg.RemoteLockTimeout.Duration)
			if len(timeoutTxns) > 0 {
				for _, txnID := range timeoutTxns {
					s.Unlock(ctx, txnID, timestamp.Timestamp{})
				}
			}

			if wait == 0 {
				wait = s.cfg.RemoteLockTimeout.Duration
			}
			timer.Reset(wait)
		}
	}
}

func getLockTableBind(
	c Client,
	tableID uint64,
	serviceID string) (pb.LockTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_GetBind
	req.GetBind.ServiceID = serviceID
	req.GetBind.Table = tableID

	resp, err := c.Send(ctx, req)
	if err != nil {
		return pb.LockTable{}, err
	}
	defer releaseResponse(resp)
	v := resp.GetBind.LockTable
	return v, nil
}

type who struct {
	ctx    context.Context
	cancel context.CancelFunc
	resp   *pb.Response
	cs     morpc.ClientSession
	txnID  []byte
}

func (s *service) handleFetchWhoWaitingMe(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case w := <-s.fetchWhoWaitingListC:
			txn := s.activeTxnHolder.getActiveTxn(
				w.txnID,
				false,
				"")
			if txn == nil {
				writeResponse(w.ctx, w.cancel, w.resp, nil, w.cs)
				return
			}
			txn.fetchWhoWaitingMe(
				s.cfg.ServiceID,
				w.txnID,
				s.activeTxnHolder,
				func(wt pb.WaitTxn) bool {
					w.resp.GetWaitingList.WaitingList = append(w.resp.GetWaitingList.WaitingList, wt)
					return true
				},
				s.getLockTable)
			writeResponse(w.ctx, w.cancel, w.resp, nil, w.cs)
		}
	}
}
