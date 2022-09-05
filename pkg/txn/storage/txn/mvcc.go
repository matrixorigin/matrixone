// Copyright 2022 Matrix Origin
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

package txnstorage

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
)

type MVCC[T any] struct {
	//TODO use lock-free linked list
	sync.RWMutex
	Values []*MVCCValue[T]
}

type MVCCValue[T any] struct {
	BornTx   *Transaction
	BornTime Time
	LockTx   *Transaction
	LockTime Time
	Value    *T
}

// Read reads the visible value from Values
// readTime's logical time should be monotonically increasing in one transaction to reflect commands order
func (m *MVCC[T]) Read(tx *Transaction, readTime Time) (*T, error) {
	if tx.State.Load() != Active {
		panic("should not call Read")
	}

	m.RLock()
	defer m.RUnlock()
	for i := len(m.Values) - 1; i >= 0; i-- {
		value := m.Values[i]
		if value.Visible(tx.ID, readTime) {
			switch tx.IsolationPolicy.Read {
			case ReadCommitted:
			case ReadSnapshot:
				if value.BornTx != tx && value.BornTime.After(tx.BeginTime) {
					continue
				}
			case ReadNoStale:
				if value.BornTx != tx && value.BornTime.After(tx.BeginTime) {
					return value.Value, &ErrReadConflict{
						ReadingTx: tx,
						Stale:     value.BornTx,
					}
				}
			}
			return value.Value, nil
		}
	}

	return nil, sql.ErrNoRows
}

func (m *MVCCValue[T]) Visible(txID string, readTime Time) bool {

	// the following algorithm is from https://momjian.us/main/writings/pgsql/mvcc.pdf
	// "[Mike Olson] says 17 march 1993: the tests in this routine are correct; if you think they’re not, you’re wrongand you should think about it again. i know, it happened to me."

	// inserted by current tx
	if m.BornTx.ID == txID {
		// inserted before the read time
		if m.BornTime.Before(readTime) {
			// not been deleted
			if m.LockTx == nil {
				return true
			}
			// deleted by current tx after the read time
			if m.LockTx.ID == txID && m.LockTime.After(readTime) {
				return true
			}
		}
	}

	// inserted by a committed tx
	if m.BornTx.State.Load() == Committed {
		// not been deleted
		if m.LockTx == nil {
			// for isolation levels stricter than read-committed, instead of checking timestamps here, let the caller do it.
			return true
		}
		// being deleted by current tx after the read time
		if m.LockTx.ID == txID && m.LockTime.After(readTime) {
			return true
		}
		// deleted by another tx but not committed
		if m.LockTx.ID != txID && m.LockTx.State.Load() != Committed {
			return true
		}
	}

	return false
}

func (m *MVCC[T]) Insert(tx *Transaction, writeTime Time, value T) error {
	if tx.State.Load() != Active {
		panic("should not call Insert")
	}

	m.Lock()
	defer m.Unlock()

	for i := len(m.Values) - 1; i >= 0; i-- {
		value := m.Values[i]
		if value.Visible(tx.ID, writeTime) {
			if value.LockTx != nil {
				return &ErrWriteConflict{
					WritingTx: tx,
					Locked:    value.LockTx,
				}
			}
			if value.BornTx != tx && value.BornTime.After(tx.BeginTime) {
				return &ErrWriteConflict{
					WritingTx: tx,
					Stale:     value.BornTx,
				}
			}
		}
	}

	m.Values = append(m.Values, &MVCCValue[T]{
		BornTx:   tx,
		BornTime: writeTime,
		Value:    &value,
	})

	return nil
}

func (m *MVCC[T]) Delete(tx *Transaction, writeTime Time) error {
	if tx.State.Load() != Active {
		panic("should not call Delete")
	}

	m.Lock()
	defer m.Unlock()

	for i := len(m.Values) - 1; i >= 0; i-- {
		value := m.Values[i]
		if value.Visible(tx.ID, writeTime) {
			if value.LockTx != nil {
				return &ErrWriteConflict{
					WritingTx: tx,
					Locked:    value.LockTx,
				}
			}
			if value.BornTx != tx && value.BornTime.After(tx.BeginTime) {
				return &ErrWriteConflict{
					WritingTx: tx,
					Stale:     value.BornTx,
				}
			}
			value.LockTx = tx
			value.LockTime = writeTime
			return nil
		}
	}

	return sql.ErrNoRows
}

func (m *MVCC[T]) Update(tx *Transaction, writeTime Time, newValue T) error {
	if tx.State.Load() != Active {
		panic("should not call Update")
	}

	m.Lock()
	defer m.Unlock()

	for i := len(m.Values) - 1; i >= 0; i-- {
		value := m.Values[i]
		if value.Visible(tx.ID, writeTime) {
			if value.LockTx != nil {
				return &ErrWriteConflict{
					WritingTx: tx,
					Locked:    value.LockTx,
				}
			}
			if value.BornTx != tx && value.BornTime.After(tx.BeginTime) {
				return &ErrWriteConflict{
					WritingTx: tx,
					Stale:     value.BornTx,
				}
			}
			value.LockTx = tx
			value.LockTime = writeTime
			m.Values = append(m.Values, &MVCCValue[T]{
				BornTx:   tx,
				BornTime: writeTime,
				Value:    &newValue,
			})
			return nil
		}
	}

	return sql.ErrNoRows
}

func (m *MVCC[T]) dump(w io.Writer) {
	for _, value := range m.Values {
		fmt.Fprintf(w, "born tx %s, born time %s, value %v",
			value.BornTx.ID,
			value.BornTime.String(),
			*value.Value,
		)
		if value.LockTx != nil {
			fmt.Fprintf(w, " lock tx %s, lock time %s",
				value.LockTx.ID,
				value.LockTime.String(),
			)
		}
		fmt.Fprintf(w, "\n")
	}
}
