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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func (s *service) GetWaitingList(
	ctx context.Context,
	txnID []byte) (bool, []pb.WaitTxn, error) {
	txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
	if txn == nil {
		return false, nil, nil
	}
	v := txn.toWaitTxn(s.serviceID, false)
	if v.CreatedOn == s.serviceID {
		values := make([]pb.WaitTxn, 0, 1)
		txn.fetchWhoWaitingMe(
			s.serviceID,
			txnID,
			s.activeTxnHolder,
			func(w pb.WaitTxn) bool {
				values = append(values, w)
				return true
			},
			s.getLockTable)
		return true, values, nil
	}

	waitingList, err := s.getTxnWaitingListOnRemote(txnID, v.CreatedOn)
	if err != nil {
		return false, nil, nil
	}
	return true, waitingList, nil
}

func (s *service) ForceRefreshLockTableBinds(
	targets []uint64,
	matcher func(bind pb.LockTable) bool) {
	contains := func(id uint64, l lockTable) bool {
		if len(targets) == 0 {
			return true
		}

		for _, v := range targets {
			if v == id && (matcher == nil || matcher(l.getBind())) {
				return true
			}
		}
		return false
	}

	s.tableGroups.removeWithFilter(contains)
}

func (s *service) GetLockTableBind(
	group uint32,
	tableID uint64) (pb.LockTable, error) {
	l, err := s.getLockTable(group, tableID)
	if err != nil {
		return pb.LockTable{}, err
	}
	if l == nil {
		return pb.LockTable{}, nil
	}
	return l.getBind(), nil
}

func (s *service) IterLocks(fn func(tableID uint64, keys [][]byte, lock Lock) bool) {
	var lockTables []*localLockTable
	s.tableGroups.iter(func(_ uint64, v lockTable) bool {
		l, ok := v.(*localLockTable)
		if !ok {
			return true
		}
		lockTables = append(lockTables, l)
		return true
	})

	for _, l := range lockTables {
		keys := make([][]byte, 0, 2)
		ok := func() bool {
			stop := false
			l.mu.RLock()
			defer l.mu.RUnlock()
			l.mu.store.Iter(func(key []byte, lock Lock) bool {
				keys = append(keys, key)
				if lock.isLockRangeStart() {
					return true
				}
				stop = !fn(l.bind.OriginTable, keys, lock)
				keys = keys[:0]
				return !stop
			})
			return !stop
		}()
		if !ok {
			return
		}
	}
}

func (s *service) CloseRemoteLockTable(
	group uint32,
	tableID uint64,
	version uint64) (bool, error) {
	removed := false
	s.tableGroups.removeWithFilter(func(id uint64, lt lockTable) bool {
		ok := id == tableID &&
			lt.getBind().Version == version &&
			lt.getBind().Group == group
		if ok {
			removed = ok
		}
		return ok
	})
	return removed, nil
}
