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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type localLockTableProxy struct {
	remote    lockTable
	serviceID string

	mu struct {
		sync.RWMutex
		holders map[string]*sharedOps // key: row
	}
}

func newLockTableProxy(
	serviceID string,
	remote lockTable) lockTable {
	lp := &localLockTableProxy{
		remote:    remote,
		serviceID: serviceID,
	}
	lp.mu.holders = make(map[string]*sharedOps)
	return lp
}

func (lp *localLockTableProxy) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions,
	cb func(pb.Result, error)) {
	if options.Mode != pb.LockMode_Shared {
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}

	if len(rows) != 1 {
		panic("local lock table proxy can only support on single row")
	}

	lp.mu.Lock()
	key := util.UnsafeBytesToString(rows[0])
	v, ok := lp.mu.holders[key]
	if !ok {
		v = &sharedOps{
			rows: rows,
			bind: lp.getBind(),
		}
		lp.mu.holders[key] = v
	}

	first := v.isEmpty()
	w := v.add(lp.serviceID, txn, cb)
	defer w.close()

	if !first {
		w.setStatus(blocking)
	}
	lp.mu.Unlock()

	if first {
		var result pb.Result
		var err error
		lp.remote.lock(
			ctx,
			txn,
			rows,
			options,
			func(r pb.Result, e error) {
				result = r
				err = e
			})

		lp.mu.Lock()
		defer lp.mu.Unlock()
		if err == nil && len(v.sharedTxns) > 1 {
			err = lp.appendSharedLocks(ctx, txn.txnID, rows[0], v.sharedTxns)
		}
		v.done(result, err)
		return
	}

	// wait first done
	w.wait(ctx)
}

func (lp *localLockTableProxy) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	lp.remote.unlock(txn, ls, commitTS)
}

func (lp *localLockTableProxy) appendSharedLocks(
	ctx context.Context,
	holdTxnID []byte,
	row []byte,
	sharedTxns []pb.WaitTxn) error {
	return lp.remote.appendSharedLocks(ctx, holdTxnID, row, sharedTxns)
}

func (lp *localLockTableProxy) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	lp.remote.getLock(key, txn, fn)
}

func (lp *localLockTableProxy) getBind() pb.LockTable {
	return lp.remote.getBind()
}

func (lp *localLockTableProxy) close() {
	lp.remote.close()
}

type sharedOps struct {
	bind       pb.LockTable
	rows       [][]byte
	txns       []*activeTxn
	waiters    []*waiter
	cbs        []func(pb.Result, error)
	sharedTxns []pb.WaitTxn
}

func (s *sharedOps) done(
	r pb.Result,
	err error) {
	for idx, cb := range s.cbs {
		s.txns[idx].lockAdded(s.bind.Group, s.bind.Table, s.rows)
		cb(r, err)
		s.waiters[idx].notify(notifyValue{})
		s.txns[idx] = nil
		s.cbs[idx] = nil
		s.waiters[idx] = nil
	}
	s.txns = s.txns[:0]
	s.cbs = s.cbs[:0]
	s.waiters = s.waiters[:0]
	s.sharedTxns = s.sharedTxns[:0]
}

func (s *sharedOps) isEmpty() bool {
	return len(s.txns) == 0
}

func (s *sharedOps) add(
	serviceID string,
	txn *activeTxn,
	cb func(pb.Result, error)) *waiter {
	v := txn.toWaitTxn(serviceID, true)
	w := acquireWaiter(v)
	s.txns = append(s.txns, txn)
	s.cbs = append(s.cbs, cb)
	s.waiters = append(s.waiters, w)
	s.sharedTxns = append(s.sharedTxns, v)
	return w
}
