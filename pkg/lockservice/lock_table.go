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
	"fmt"
	"sync"
)

// a locktable instance manages the locks on a table
type lockTable struct {
	sync.RWMutex
	store LockStorage
}

func newLockTable() *lockTable {
	return &lockTable{
		store: newBtreeBasedStorage(),
	}
}

func (l *lockTable) acquireLock(ctx context.Context, txnID []byte, rows [][]byte, options LockOptions) error {
	waiter := acquireWaiter(txnID)
	for {
		ok, err := l.doAcquireLock(waiter, rows, txnID, options)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		if err := waiter.wait(ctx); err != nil {
			return err
		}
		waiter.resetWait()
	}
}

func (l *lockTable) doAcquireLock(waiter *waiter, txnID []byte, rows [][]byte, opts LockOptions) (bool, error) {
	l.Lock()
	defer l.Unlock()

	switch opts.granularity {
	case Row:
		return l.acquireRowLock(waiter, rows[0], txnID, opts.mode)
	case Range:
		return l.acquireRangeLock(waiter, rows[0], rows[1], txnID, opts.mode)
	default:
		panic(fmt.Sprintf("not support lock granularity %d", opts))
	}
}

func (l *lockTable) acquireRowLock(w *waiter, row []byte, txnID []byte, mode LockMode) (bool, error) {
	txnKey := unsafeByteSliceToString(txnID)
	key, lock, ok := l.store.Seek(row)
	if ok && (bytes.Equal(key, row) || lock.isLockRangeEnd()) {
		if err := lock.waiter.add(w); err != nil {
			return false, err
		}

		s.waiterAdded(txnID, txnKey, w)
		return false, nil
	} else {
		addRowLock(storage, txnID, row, w, mode)
	}
	s.locks[txnKey][tableID] = append(s.locks[txnKey][tableID], row)
	return true, nil
}
