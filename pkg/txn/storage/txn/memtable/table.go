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
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/btree"
)

type Table[
	K Ordered[K],
	V any,
	R Row[K, V],
] struct {
	sync.Mutex
	rows      *btree.BTreeG[*PhysicalRow[K, V]]
	index     *btree.BTreeG[*IndexEntry[K, V]]
	writeSets map[*Transaction]map[*PhysicalRow[K, V]]struct{}
}

type Row[K any, V any] interface {
	Key() K
	Value() V
	Indexes() []Tuple
}

type Ordered[To any] interface {
	Less(to To) bool
}

type IndexEntry[
	K Ordered[K],
	V any,
] struct {
	Index     Tuple
	Key       K
	VersionID ID
	Value     V
}

func NewTable[
	K Ordered[K],
	V any,
	R Row[K, V],
]() *Table[K, V, R] {
	return &Table[K, V, R]{
		rows: btree.NewBTreeG(func(a, b *PhysicalRow[K, V]) bool {
			return a.Key.Less(b.Key)
		}),
		index: btree.NewBTreeG(func(a, b *IndexEntry[K, V]) bool {
			if a.Index.Less(b.Index) {
				return true
			}
			if b.Index.Less(a.Index) {
				return false
			}
			if a.Key.Less(b.Key) {
				return true
			}
			if b.Key.Less(a.Key) {
				return false
			}
			return a.VersionID.Less(b.VersionID)
		}),
		writeSets: make(map[*Transaction]map[*PhysicalRow[K, V]]struct{}),
	}
}

func (t *Table[K, V, R]) Insert(
	tx *Transaction,
	row R,
) error {
	key := row.Key()
	physicalRow := t.getOrSetRowByKey(key)

	existed, err := physicalRow.ReadVisible(tx.Time, tx)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return err
	}
	if existed != nil {
		//return moerr.NewPrimaryKeyDuplicated(key)
		return moerr.NewDuplicate()
	}

	value := row.Value()
	if err := physicalRow.Insert(
		tx.Time, tx, value,
		func(versionID ID) {
			for _, index := range row.Indexes() {
				t.index.Set(&IndexEntry[K, V]{
					Index:     index,
					Key:       key,
					VersionID: versionID,
					Value:     value,
				})
			}
		},
	); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())

	t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, V, R]) Update(
	tx *Transaction,
	row R,
) error {
	key := row.Key()
	physicalRow := t.getOrSetRowByKey(key)

	value := row.Value()
	if err := physicalRow.Update(
		tx.Time, tx, value,
		func(versionID ID) {
			for _, index := range row.Indexes() {
				t.index.Set(&IndexEntry[K, V]{
					Index:     index,
					Key:       key,
					VersionID: versionID,
					Value:     value,
				})
			}
		},
	); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())

	t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, V, R]) Delete(
	tx *Transaction,
	key K,
) error {
	physicalRow := t.getRowByKey(key)

	if physicalRow == nil {
		return nil
	}
	if err := physicalRow.Delete(tx.Time, tx); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())

	t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, V, R]) Get(
	tx *Transaction,
	key K,
) (
	value V,
	err error,
) {
	physicalRow := t.getRowByKey(key)
	if physicalRow == nil {
		err = sql.ErrNoRows
		return
	}
	value, err = physicalRow.Read(tx.Time, tx)
	if err != nil {
		return
	}
	return
}

func (t *Table[K, V, R]) getRowByKey(key K) *PhysicalRow[K, V] {
	pivot := &PhysicalRow[K, V]{
		Key: key,
	}
	row, _ := t.rows.Get(pivot)
	return row
}

func (t *Table[K, V, R]) getOrSetRowByKey(key K) *PhysicalRow[K, V] {
	pivot := &PhysicalRow[K, V]{
		Key: key,
	}
	if row, _ := t.rows.Get(pivot); row != nil {
		return row
	}
	return t.getOrSetRowByKeySlow(pivot)
}

func (t *Table[K, V, R]) getOrSetRowByKeySlow(pivot *PhysicalRow[K, V]) *PhysicalRow[K, V] {
	t.Lock()
	defer t.Unlock()
	if row, _ := t.rows.Get(pivot); row != nil {
		return row
	}
	pivot.LastUpdate = NewAtomic(time.Now())
	t.rows.Set(pivot)
	return pivot
}

func (t *Table[K, V, R]) Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error) {
	pivot := &IndexEntry[K, V]{
		Index: index,
	}
	iter := t.index.Copy().Iter()
	defer iter.Release()
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		item := iter.Item()
		if index.Less(item.Index) {
			break
		}
		if item.Index.Less(index) {
			break
		}

		physicalRow := t.getRowByKey(item.Key)
		if physicalRow == nil {
			continue
		}
		currentVersion, err := physicalRow.readVersion(tx.Time, tx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if currentVersion.ID == item.VersionID {
			entries = append(entries, item)
		}
	}
	return
}

func (t *Table[K, V, R]) setCommitter(tx *Transaction, row *PhysicalRow[K, V]) {
	tx.committers[t] = struct{}{}
	t.Lock()
	defer t.Unlock()
	set, ok := t.writeSets[tx]
	if !ok {
		set = make(map[*PhysicalRow[K, V]]struct{})
		t.writeSets[tx] = set
	}
	set[row] = struct{}{}
}

func (t *Table[K, V, R]) CommitTx(tx *Transaction) error {
	t.Lock()
	set := t.writeSets[tx]
	t.Unlock()
	defer func() {
		t.Lock()
		delete(t.writeSets, tx)
		t.Unlock()
	}()

	for physicalRow := range set {

		// verify the latest committed operation is done by the tx
		var err error
		physicalRow.Versions.RLock()
		for i := len(physicalRow.Versions.List) - 1; i >= 0; i-- {
			version := physicalRow.Versions.List[i]

			// locked by another committed tx after tx begin
			if version.LockTx != nil &&
				version.LockTx.State.Load() == Committed &&
				version.LockTx.ID != tx.ID &&
				version.LockTime.After(tx.BeginTime) {
				//err = moerr.NewPrimaryKeyDuplicated(physicalRow.Key)
				err = moerr.NewDuplicate()
				break
			}

			// born in another committed tx after tx begin
			if version.BornTx.State.Load() == Committed &&
				version.BornTx.ID != tx.ID &&
				version.BornTime.After(tx.BeginTime) {
				//err = moerr.NewPrimaryKeyDuplicated(physicalRow.Key)
				err = moerr.NewDuplicate()
				break
			}

		}
		physicalRow.Versions.RUnlock()

		if err != nil {
			return err
		}

	}

	return nil
}

func (t *Table[K, V, R]) AbortTx(tx *Transaction) {
	t.Lock()
	delete(t.writeSets, tx)
	t.Unlock()
}
