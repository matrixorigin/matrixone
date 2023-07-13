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
	v := txn.toWaitTxn(s.cfg.ServiceID, false)
	if v.CreatedOn == s.cfg.ServiceID {
		values := make([]pb.WaitTxn, 0, 1)
		txn.fetchWhoWaitingMe(
			s.cfg.ServiceID,
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

func (s *service) ForceRefreshLockTableBinds() {
	s.tables.Range(func(key, value any) bool {
		value.(lockTable).close()
		s.tables.Delete(key)
		return true
	})
}

func (s *service) GetLockTableBind(tableID uint64) (pb.LockTable, error) {
	l, err := s.getLockTable(tableID)
	if err != nil {
		return pb.LockTable{}, err
	}
	return l.getBind(), nil
}
