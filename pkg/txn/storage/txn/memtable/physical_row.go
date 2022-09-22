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

package memtable

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type PhysicalRow[
	K Ordered[K],
	V any,
] struct {
	Key        K
	LastUpdate *Atomic[time.Time]
	Versions   struct {
		sync.RWMutex
		List []*Version[V]
		//TODO version GC
	}
}

type Version[T any] struct {
	ID       ID
	BornTx   *Transaction
	BornTime Time
	LockTx   *Transaction
	LockTime Time
	Value    T
}

// Read reads the visible value from Values
// readTime's logical time should be monotonically increasing in one transaction to reflect commands order
func (p *PhysicalRow[K, V]) Read(now Time, tx *Transaction) (value V, err error) {
	version, err := p.readVersion(now, tx)
	if version != nil {
		value = version.Value
	}
	return
}

func (p *PhysicalRow[K, V]) readVersion(now Time, tx *Transaction) (*Version[V], error) {
	if tx.State.Load() != Active {
		panic("should not call Read")
	}

	p.Versions.RLock()
	defer p.Versions.RUnlock()
	for i := len(p.Versions.List) - 1; i >= 0; i-- {
		value := p.Versions.List[i]
		if value.Visible(now, tx.ID) {
			switch tx.IsolationPolicy.Read {
			case ReadCommitted:
			case ReadSnapshot:
				// BornTx must be committed to be visible here
				if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
					continue
				}
			case ReadNoStale:
				// BornTx must be committed to be visible here
				if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
					return value, moerr.NewTxnReadConflict("%s %s", tx.ID, value.BornTx.ID)
				}
			}
			return value, nil
		}
	}

	return nil, sql.ErrNoRows
}

// ReadVisible reads a committed value despite the tx's isolation policy
func (p *PhysicalRow[K, V]) ReadVisible(now Time, tx *Transaction) (*Version[V], error) {
	if tx.State.Load() != Active {
		panic("should not call Read")
	}

	p.Versions.RLock()
	defer p.Versions.RUnlock()
	for i := len(p.Versions.List) - 1; i >= 0; i-- {
		value := p.Versions.List[i]
		if value.Visible(now, tx.ID) {
			return value, nil
		}
	}

	return nil, sql.ErrNoRows
}

func (v *Version[T]) Visible(now Time, txID string) bool {

	// the following algorithm is from https://momjian.us/main/writings/pgsql/mvcc.pdf
	// "[Mike Olson] says 17 march 1993: the tests in this routine are correct; if you think they’re not, you’re wrongand you should think about it again. i know, it happened to me."

	// inserted by current tx
	if v.BornTx.ID == txID {
		// inserted before the read time
		if v.BornTime.Before(now) {
			// not been deleted
			if v.LockTx == nil {
				return true
			}
			// deleted by current tx after the read time
			if v.LockTx.ID == txID && v.LockTime.After(now) {
				return true
			}
		}
	}

	// inserted by a committed tx
	if v.BornTx.State.Load() == Committed {
		// not been deleted
		if v.LockTx == nil {
			// for isolation levels stricter than read-committed, instead of checking timestamps here, let the caller do it.
			return true
		}
		// being deleted by current tx after the read time
		if v.LockTx.ID == txID && v.LockTime.After(now) {
			return true
		}
		// deleted by another tx but not committed
		if v.LockTx.ID != txID && v.LockTx.State.Load() != Committed {
			return true
		}
	}

	return false
}

func (p *PhysicalRow[K, V]) Insert(now Time, tx *Transaction, value V, callbacks ...any) error {
	if tx.State.Load() != Active {
		panic("should not call Insert")
	}

	p.Versions.Lock()
	defer p.Versions.Unlock()

	for i := len(p.Versions.List) - 1; i >= 0; i-- {
		value := p.Versions.List[i]
		if value.Visible(now, tx.ID) {
			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				// locked by active or committed tx
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}
			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}
		}
	}

	id := txnengine.NewID()
	p.Versions.List = append(p.Versions.List, &Version[V]{
		ID:       id,
		BornTx:   tx,
		BornTime: now,
		Value:    value,
	})

	for _, callback := range callbacks {
		switch callback := callback.(type) {
		case func(ID): // version id
			callback(id)
		default:
			panic(fmt.Sprintf("unknown type: %T", callback))
		}
	}

	return nil
}

func (p *PhysicalRow[K, V]) Delete(now Time, tx *Transaction) error {
	if tx.State.Load() != Active {
		panic("should not call Delete")
	}

	p.Versions.Lock()
	defer p.Versions.Unlock()

	for i := len(p.Versions.List) - 1; i >= 0; i-- {
		value := p.Versions.List[i]
		if value.Visible(now, tx.ID) {
			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}
			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}
			value.LockTx = tx
			value.LockTime = now
			return nil
		}
	}

	return sql.ErrNoRows
}

func (p *PhysicalRow[K, V]) Update(now Time, tx *Transaction, newValue V, callbacks ...any) error {
	if tx.State.Load() != Active {
		panic("should not call Update")
	}

	p.Versions.Lock()
	defer p.Versions.Unlock()

	for i := len(p.Versions.List) - 1; i >= 0; i-- {
		value := p.Versions.List[i]
		if value.Visible(now, tx.ID) {

			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}

			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}

			value.LockTx = tx
			value.LockTime = now
			id := txnengine.NewID()
			p.Versions.List = append(p.Versions.List, &Version[V]{
				ID:       id,
				BornTx:   tx,
				BornTime: now,
				Value:    newValue,
			})

			for _, callback := range callbacks {
				switch callback := callback.(type) {
				case func(ID): // version id
					callback(id)
				default:
					panic(fmt.Sprintf("unknown type: %T", callback))
				}
			}

			return nil
		}
	}

	return sql.ErrNoRows
}

func (p *PhysicalRow[K, V]) dump(w io.Writer) {
	p.Versions.RLock()
	defer p.Versions.RUnlock()
	for _, value := range p.Versions.List {
		fmt.Fprintf(w, "born tx %s, born time %s, value %v",
			value.BornTx.ID,
			value.BornTime.String(),
			value.Value,
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
