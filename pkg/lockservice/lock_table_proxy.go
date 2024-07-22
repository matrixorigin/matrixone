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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type localLockTableProxy struct {
	remote    lockTable
	serviceID string
	logger    *log.MOLogger

	mu struct {
		sync.RWMutex
		holders       map[string]*sharedOps // key: row
		currentHolder map[string][]byte
	}
}

func newLockTableProxy(
	serviceID string,
	remote lockTable,
	logger *log.MOLogger,
) lockTable {
	lp := &localLockTableProxy{
		remote:    remote,
		serviceID: serviceID,
		logger:    logger,
	}
	lp.mu.holders = make(map[string]*sharedOps)
	lp.mu.currentHolder = make(map[string][]byte)
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
	r := v.result
	w := v.add(
		lp.serviceID,
		txn,
		cb,
		lp.hasRemoteHolderLocked(key))
	if w != nil {
		defer w.close()
	}
	lp.mu.Unlock()

	if first {
		lp.remote.lock(
			ctx,
			txn,
			rows,
			options,
			func(r pb.Result, e error) {
				lp.mu.Lock()
				defer lp.mu.Unlock()
				if e == nil {
					lp.mu.currentHolder[key] = v.txns[0].txnID
				}
				v.done(r, e, lp.logger)
			})
		return
	}

	defer func() {
		bind := lp.getBind()
		txn.lockAdded(bind.Group, bind, rows, lp.logger)
	}()

	// wait first done
	if w != nil {
		w.wait(ctx, lp.logger)
		return
	}

	cb(r, nil)
}

func (lp *localLockTableProxy) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	_ ...pb.ExtraMutation) {
	rows := ls.slice()
	defer rows.unref()

	skipped := 0
	n := rows.len()
	var mutations []pb.ExtraMutation
	lp.mu.Lock()
	defer lp.mu.Unlock()
	rows.iter(func(key []byte) bool {
		row := util.UnsafeBytesToString(key)
		if v, ok := lp.mu.holders[row]; ok {
			isHolder := lp.isRemoteHolderLocked(row, txn.txnID)
			if !v.remove(txn) {
				return true
			}

			// not the holder, no need to unlock
			if !isHolder {
				skipped++
				if n > 1 {
					mutations = append(mutations,
						pb.ExtraMutation{
							Key:  key,
							Skip: true,
						})
				}
				return true
			}

			// update holder to last txn
			if !v.isEmpty() {
				lp.mu.currentHolder[row] = v.last()
				mutations = append(mutations,
					pb.ExtraMutation{
						Key:       key,
						Skip:      false,
						ReplaceTo: v.last(),
					})
			} else {
				delete(lp.mu.currentHolder, row)
			}
		}
		return true
	})

	// all skipped
	if skipped == rows.len() {
		return
	}

	lp.remote.unlock(txn, ls, commitTS, mutations...)
}

func (lp *localLockTableProxy) isRemoteHolderLocked(
	row string,
	txnID []byte) bool {
	v, ok := lp.mu.currentHolder[row]
	if !ok {
		return false
	}
	return bytes.Equal(txnID, v)
}

func (lp *localLockTableProxy) hasRemoteHolderLocked(row string) bool {
	_, ok := lp.mu.currentHolder[row]
	return ok
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
	bind    pb.LockTable
	result  pb.Result
	rows    [][]byte
	txns    []*activeTxn
	waiters []*waiter
	cbs     []func(pb.Result, error)
}

func (s *sharedOps) done(
	r pb.Result,
	err error,
	logger *log.MOLogger,
) {
	for idx, cb := range s.cbs {
		cb(r, err)
		if idx > 0 {
			s.waiters[idx].notify(notifyValue{}, logger)
		}
		s.cbs[idx] = nil
		s.waiters[idx] = nil
	}
	if err != nil {
		s.txns = s.txns[:0]
		s.cbs = s.cbs[:0]
		s.waiters = s.waiters[:0]
	} else {
		s.result = r
	}
}

func (s *sharedOps) isEmpty() bool {
	return len(s.txns) == 0
}

func (s *sharedOps) last() []byte {
	return s.txns[len(s.txns)-1].txnID
}

func (s *sharedOps) add(
	serviceID string,
	txn *activeTxn,
	cb func(pb.Result, error),
	hasHolder bool) *waiter {
	var w *waiter
	if !hasHolder && !s.isEmpty() {
		v := txn.toWaitTxn(serviceID, true)
		w = acquireWaiter(v)
		w.setStatus(blocking)
	}
	if hasHolder {
		cb = nil
	}

	s.txns = append(s.txns, txn)
	s.cbs = append(s.cbs, cb)
	s.waiters = append(s.waiters, w)
	return w
}

func (s *sharedOps) remove(txn *activeTxn) bool {
	found := false
	newTxns := s.txns[:0]
	newCbs := s.cbs[:0]
	newWaiters := s.waiters[:0]
	for idx, v := range s.txns {
		if v != txn {
			if bytes.Equal(v.txnID, txn.txnID) {
				panic("fatal")
			}
			newTxns = append(newTxns, v)
			newWaiters = append(newWaiters, s.waiters[idx])
			newCbs = append(newCbs, s.cbs[idx])
		} else {
			found = true
		}
	}
	s.txns = newTxns
	s.waiters = newWaiters
	s.cbs = newCbs
	return found
}
