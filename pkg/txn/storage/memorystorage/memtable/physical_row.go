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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type PhysicalRow[
	K Ordered[K],
	V any,
] struct {
	Key        K
	LastUpdate time.Time
	Versions   []Version[V]
	//TODO version GC
}

type Version[T any] struct {
	ID       int64
	BornTx   *Transaction
	BornTime Time
	LockTx   *Transaction
	LockTime Time
	Value    T
}

var nextVersionID int64

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

	for i := len(p.Versions) - 1; i >= 0; i-- {
		value := p.Versions[i]
		if value.Visible(now, tx.ID, tx.IsolationPolicy) {
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
					return &value, moerr.NewTxnReadConflict("%s %s", tx.ID, value.BornTx.ID)
				}
			}
			return &value, nil
		}
	}

	return nil, sql.ErrNoRows
}

func (v *Version[T]) Visible(now Time, txID string, policy IsolationPolicy) bool {

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
		// deleted by another committed tx after the read time
		if v.LockTx.ID != txID && v.LockTx.State.Load() == Committed && v.LockTime.After(now) {
			if policy == SnapshotIsolation {
				return true
			}
		}
	}

	return false
}

func (p *PhysicalRow[K, V]) Insert(
	now Time,
	tx *Transaction,
	value V,
) (
	newRow *PhysicalRow[K, V],
	version *Version[V],
	err error,
) {

	if tx.State.Load() != Active {
		panic("should not call Insert")
	}

	for i := len(p.Versions) - 1; i >= 0; i-- {
		value := p.Versions[i]
		if value.Visible(now, tx.ID, tx.IsolationPolicy) {
			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				// locked by active or committed tx
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}
			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}
		}
	}

	p = p.clone()
	p.LastUpdate = time.Now()

	version = &Version[V]{
		ID:       atomic.AddInt64(&nextVersionID, 1),
		BornTx:   tx,
		BornTime: now,
		Value:    value,
	}
	p.Versions = append(p.Versions, *version)

	return p, version, nil
}

func (p *PhysicalRow[K, V]) Delete(
	now Time,
	tx *Transaction,
) (
	newRow *PhysicalRow[K, V],
	version *Version[V],
	err error,
) {
	if tx.State.Load() != Active {
		panic("should not call Delete")
	}

	for i := len(p.Versions) - 1; i >= 0; i-- {
		value := p.Versions[i]
		if value.Visible(now, tx.ID, tx.IsolationPolicy) {
			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}
			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}

			p = p.clone()
			p.LastUpdate = time.Now()
			value.LockTx = tx
			value.LockTime = now
			p.Versions[i] = value

			return p, &value, nil
		}
	}

	return nil, nil, sql.ErrNoRows
}

func (p *PhysicalRow[K, V]) Update(
	now Time,
	tx *Transaction,
	newValue V,
) (
	newRow *PhysicalRow[K, V],
	version *Version[V],
	err error,
) {

	if tx.State.Load() != Active {
		panic("should not call Update")
	}

	for i := len(p.Versions) - 1; i >= 0; i-- {
		value := p.Versions[i]
		if value.Visible(now, tx.ID, tx.IsolationPolicy) {

			if value.LockTx != nil && value.LockTx.State.Load() != Aborted {
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.LockTx.ID)
			}

			if value.BornTx.ID != tx.ID && value.BornTime.After(tx.BeginTime) {
				return nil, nil, moerr.NewTxnWriteConflict("%s %s", tx.ID, value.BornTx.ID)
			}

			p = p.clone()
			p.LastUpdate = time.Now()

			value.LockTx = tx
			value.LockTime = now
			p.Versions[i] = value

			version = &Version[V]{
				ID:       atomic.AddInt64(&nextVersionID, 1),
				BornTx:   tx,
				BornTime: now,
				Value:    newValue,
			}
			p.Versions = append(p.Versions, *version)

			return p, version, nil
		}
	}

	return nil, nil, sql.ErrNoRows
}

func (p *PhysicalRow[K, V]) clone() *PhysicalRow[K, V] {
	newRow := *p
	newRow.Versions = make([]Version[V], len(p.Versions))
	copy(newRow.Versions, p.Versions)
	return &newRow
}

func (p *PhysicalRow[K, V]) dump(w io.Writer) {
	for _, value := range p.Versions {
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
