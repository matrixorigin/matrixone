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

type lockTableProxy struct {
	target lockTable

	mu struct {
		sync.RWMutex
		holders map[string]*sharedOps
	}
}

func newLockTableProxy(target lockTable) lockTable {
	lp := &lockTableProxy{
		target: target,
	}
	lp.mu.holders = make(map[string]*sharedOps)
	return lp
}

func (lp *lockTableProxy) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions,
	cb func(pb.Result, error)) {
	if !options.AggregationShared {
		lp.lock(ctx, txn, rows, options, cb)
		return
	}

	if options.Sharding != pb.Sharding_ByRow {
		panic("sharding must be by row")
	}
	if len(rows) != 1 {
		panic("aggregation shared lock must be on one row")
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
	v.add(txn, cb)
	lp.mu.Unlock()

	if first {
		lp.lock(
			ctx,
			txn,
			rows,
			options,
			func(r pb.Result, err error) {
				lp.mu.Lock()
				defer lp.mu.Unlock()
				v.done(r, err)
			})
		return
	}
}

func (lp *lockTableProxy) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	lp.unlock(txn, ls, commitTS)
}

func (lp *lockTableProxy) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	lp.target.getLock(key, txn, fn)
}

func (lp *lockTableProxy) getBind() pb.LockTable {
	return lp.target.getBind()
}

func (lp *lockTableProxy) close() {
	lp.target.close()
}

type sharedOps struct {
	bind pb.LockTable
	rows [][]byte
	txns []*activeTxn
	cbs  []func(pb.Result, error)
}

func (s *sharedOps) done(
	r pb.Result,
	err error) {
	for idx, cb := range s.cbs {
		s.txns[idx].lockAdded(s.bind.Group, s.bind.Table, s.rows)
		cb(r, err)
		if err != nil {
			s.txns[idx] = nil
			s.cbs[idx] = nil
		}
	}
	if err != nil {
		s.txns = s.txns[:0]
		s.cbs = s.cbs[:0]
	}
}

func (s *sharedOps) isEmpty() bool {
	return len(s.txns) == 0
}

func (s *sharedOps) add(
	txn *activeTxn,
	cb func(pb.Result, error)) {
	s.txns = append(s.txns, txn)
	s.cbs = append(s.cbs, cb)
}
