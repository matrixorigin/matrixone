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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/tidwall/btree"
)

type Table[
	K memorytable.Ordered[K],
	V any,
	R Row[K, V],
] struct {
	sync.Mutex
	state atomic.Pointer[tableState[K, V]]
}

type tableState[
	K memorytable.Ordered[K],
	V any,
] struct {
	rows                 *btree.BTreeG[*PhysicalRow[K, V]]
	indexes              *btree.BTreeG[*IndexEntry[K, V]]
	reverseIndexes       *btree.BTreeG[*ReverseIndexEntry[K, V]]
	writes               *btree.BTreeG[*WriteEntry[K, V]]
	uniqueIndexes        *btree.BTreeG[*IndexEntry[K, V]]
	reverseUniqueIndexes *btree.BTreeG[*ReverseIndexEntry[K, V]]
}

func (t *tableState[K, V]) Copy() *tableState[K, V] {
	return &tableState[K, V]{
		rows:                 t.rows.Copy(),
		indexes:              t.indexes.Copy(),
		reverseIndexes:       t.reverseIndexes.Copy(),
		writes:               t.writes.Copy(),
		uniqueIndexes:        t.uniqueIndexes.Copy(),
		reverseUniqueIndexes: t.reverseUniqueIndexes.Copy(),
	}
}

type Row[K any, V any] interface {
	Key() K
	Value() V
	Indexes() []Tuple
	UniqueIndexes() []Tuple
}

type IndexEntry[
	K memorytable.Ordered[K],
	V any,
] struct {
	Index     Tuple
	Key       K
	VersionID int64
}

type ReverseIndexEntry[
	K memorytable.Ordered[K],
	V any,
] struct {
	Key       K
	VersionID int64
	Index     Tuple
}

type WriteEntry[
	K memorytable.Ordered[K],
	V any,
] struct {
	Transaction *Transaction
	Key         *K

	VersionID int64
}

func NewTable[
	K memorytable.Ordered[K],
	V any,
	R Row[K, V],
]() *Table[K, V, R] {
	ret := &Table[K, V, R]{}
	state := &tableState[K, V]{
		rows:                 btree.NewBTreeG(comparePhysicalRow[K, V]),
		indexes:              btree.NewBTreeG(compareIndexEntry[K, V]),
		reverseIndexes:       btree.NewBTreeG(compareReverseIndexEntry[K, V]),
		writes:               btree.NewBTreeG(compareWriteEntry[K, V]),
		uniqueIndexes:        btree.NewBTreeG(compareIndexEntry[K, V]),
		reverseUniqueIndexes: btree.NewBTreeG(compareReverseIndexEntry[K, V]),
	}
	ret.state.Store(state)
	return ret
}

func comparePhysicalRow[
	K memorytable.Ordered[K],
	V any,
](a, b *PhysicalRow[K, V]) bool {
	return a.Key.Less(b.Key)
}

func compareIndexEntry[
	K memorytable.Ordered[K],
	V any,
](a, b *IndexEntry[K, V]) bool {
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
	return a.VersionID < b.VersionID
}

func compareReverseIndexEntry[
	K memorytable.Ordered[K],
	V any,
](a, b *ReverseIndexEntry[K, V]) bool {
	if a.Key.Less(b.Key) {
		return true
	}
	if b.Key.Less(a.Key) {
		return false
	}
	if a.VersionID < b.VersionID {
		return true
	}
	if b.VersionID < a.VersionID {
		return false
	}
	return a.Index.Less(b.Index)
}

func compareWriteEntry[
	K memorytable.Ordered[K],
	V any,
](a, b *WriteEntry[K, V]) bool {
	if a.Transaction.ID < b.Transaction.ID {
		return true
	}
	if a.Transaction.ID > b.Transaction.ID {
		return false
	}
	if a.Key != nil && b.Key != nil {
		if (*a.Key).Less(*b.Key) {
			return true
		}
		if (*b.Key).Less(*a.Key) {
			return false
		}
	}
	return a.Key == nil && b.Key != nil
}

func (t *Table[K, V, R]) Insert(
	tx *Transaction,
	row R,
) error {
	key := row.Key()

	return t.update(func(state *tableState[K, V]) error {
		physicalRow := getOrSetRowByKey(state.rows, key)

		if err := validate(physicalRow, tx); err != nil {
			return err
		}

		for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
			version := physicalRow.Versions[i]
			if version.Visible(tx.Time, tx.ID, tx.IsolationPolicy.Read) {
				return moerr.NewDuplicate()
			}
		}

		value := row.Value()
		physicalRow, version, err := physicalRow.Insert(
			tx.Time, tx, value,
		)
		if err != nil {
			return err
		}

		// index entry
		if err := setIndexes(tx, t, state, key, version, row); err != nil {
			return err
		}

		// write entry
		tx.committers[t] = struct{}{}
		state.writes.Set(&WriteEntry[K, V]{
			Transaction: tx,
			Key:         &key,
			VersionID:   version.ID,
		})

		// row entry
		state.rows.Set(physicalRow)

		tx.Time.Tick()
		return nil
	})

}

func (t *Table[K, V, R]) Update(
	tx *Transaction,
	row R,
) error {
	key := row.Key()

	return t.update(func(state *tableState[K, V]) error {
		physicalRow := getOrSetRowByKey(state.rows, key)

		value := row.Value()
		physicalRow, version, err := physicalRow.Update(
			tx.Time, tx, value,
		)
		if err != nil {
			return err
		}

		// index entry
		if err := setIndexes(tx, t, state, key, version, row); err != nil {
			return err
		}

		// write entry
		tx.committers[t] = struct{}{}
		state.writes.Set(&WriteEntry[K, V]{
			Transaction: tx,
			Key:         &key,
			VersionID:   version.ID,
		})

		// row entry
		state.rows.Set(physicalRow)

		tx.Time.Tick()
		return nil
	})
}

func (t *Table[K, V, R]) Delete(
	tx *Transaction,
	key K,
) error {

	return t.update(func(state *tableState[K, V]) error {
		physicalRow := getRowByKey(state.rows, key)
		if physicalRow == nil {
			return nil
		}

		physicalRow, version, err := physicalRow.Delete(tx.Time, tx)
		if err != nil {
			return err
		}

		// write entry
		tx.committers[t] = struct{}{}
		state.writes.Set(&WriteEntry[K, V]{
			Transaction: tx,
			Key:         &key,
			VersionID:   version.ID,
		})

		// row entry
		state.rows.Set(physicalRow)

		tx.Time.Tick()
		return nil
	})

}

func (t *Table[K, V, R]) Upsert(
	tx *Transaction,
	row R,
) error {
	key := row.Key()

	return t.update(func(state *tableState[K, V]) error {
		physicalRow := getOrSetRowByKey(state.rows, key)

		value := row.Value()
		updatedPhysicalRow, version, err := physicalRow.Update(
			tx.Time, tx, value,
		)
		if err != nil {

			if errors.Is(err, sql.ErrNoRows) {
				// insert
				if err := validate(physicalRow, tx); err != nil {
					return err
				}

				for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
					version := physicalRow.Versions[i]
					if version.Visible(tx.Time, tx.ID, tx.IsolationPolicy.Read) {
						return moerr.NewDuplicate()
					}
				}

				value := row.Value()
				physicalRow, version, err = physicalRow.Insert(
					tx.Time, tx, value,
				)
				if err != nil {
					return err
				}

			} else {
				return err
			}
		} else {
			physicalRow = updatedPhysicalRow
		}

		// index entry
		if err := setIndexes(tx, t, state, key, version, row); err != nil {
			return err
		}

		// write entry
		tx.committers[t] = struct{}{}
		state.writes.Set(&WriteEntry[K, V]{
			Transaction: tx,
			Key:         &key,
			VersionID:   version.ID,
		})

		// row entry
		state.rows.Set(physicalRow)

		tx.Time.Tick()
		return nil
	})
}

func setIndexes[
	K memorytable.Ordered[K],
	V any,
	R Row[K, V],
](
	tx *Transaction,
	table *Table[K, V, R],
	state *tableState[K, V],
	key K,
	version *Version[V],
	row R,
) error {

	// index entries
	for _, index := range row.Indexes() {
		state.indexes.Set(&IndexEntry[K, V]{
			Index:     index,
			Key:       key,
			VersionID: version.ID,
		})
		state.reverseIndexes.Set(&ReverseIndexEntry[K, V]{
			Key:       key,
			VersionID: version.ID,
			Index:     index,
		})
	}

	// unique index entries
	uniqueIndexes := row.UniqueIndexes()
	for _, index := range uniqueIndexes {
		iter := table.newIndexIter(
			state.uniqueIndexes.Copy().Iter(),
			state.rows,
			tx,
			index,
			append(index, Min),
		)
		for ok := iter.First(); ok; ok = iter.Next() {
			return moerr.NewDuplicate()
		}
		state.uniqueIndexes.Set(&IndexEntry[K, V]{
			Index:     index,
			Key:       key,
			VersionID: version.ID,
		})
		state.reverseUniqueIndexes.Set(&ReverseIndexEntry[K, V]{
			Key:       key,
			VersionID: version.ID,
			Index:     index,
		})
	}

	return nil
}

func (t *Table[K, V, R]) Get(
	tx *Transaction,
	key K,
) (
	value V,
	err error,
) {
	state := t.state.Load()
	physicalRow := getRowByKey(state.rows, key)
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

func getRowByKey[
	K memorytable.Ordered[K],
	V any,
](
	tree *btree.BTreeG[*PhysicalRow[K, V]],
	key K,
) *PhysicalRow[K, V] {
	pivot := &PhysicalRow[K, V]{
		Key: key,
	}
	row, _ := tree.Get(pivot)
	if row == nil {
		return nil
	}
	return row
}

func getOrSetRowByKey[
	K memorytable.Ordered[K],
	V any,
](
	tree *btree.BTreeG[*PhysicalRow[K, V]],
	key K,
) *PhysicalRow[K, V] {
	pivot := &PhysicalRow[K, V]{
		Key: key,
	}
	if row, _ := tree.Get(pivot); row != nil {
		return row
	}
	pivot.LastUpdate = time.Now()
	tree.Set(pivot)
	return pivot
}

func (t *Table[K, V, R]) Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error) {
	iter := t.NewIndexIter(
		tx,
		index,
		index,
	)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry := iter.Item()
		entries = append(entries, entry)
	}
	return
}

func (t *Table[K, V, R]) CommitTx(tx *Transaction) error {
	return t.update(func(state *tableState[K, V]) error {
		iter := state.writes.Copy().Iter()
		defer iter.Release()
		pivot := &WriteEntry[K, V]{
			Transaction: tx,
		}
		for ok := iter.Seek(pivot); ok; ok = iter.Next() {
			entry := iter.Item()
			if entry.Transaction != tx {
				break
			}

			key := *entry.Key
			physicalRow := getRowByKey(state.rows, key)
			if err := validate(physicalRow, tx); err != nil {
				return err
			}

			physicalRow = physicalRow.clone()
			for i, version := range physicalRow.Versions {
				if version.ID != entry.VersionID {
					continue
				}

				// set born time and lock time to commit time
				if version.LockTx == tx {
					version.LockTime = tx.CommitTime
				}
				if version.BornTx == tx {
					version.BornTime = tx.CommitTime
				}

				// check unique index
				reverseIter := state.reverseUniqueIndexes.Copy().Iter()
				pivot := &ReverseIndexEntry[K, V]{
					Key:       key,
					VersionID: version.ID,
					Index:     Tuple{Min},
				}
				for ok := reverseIter.Seek(pivot); ok; ok = reverseIter.Next() {
					entry := reverseIter.Item()
					if key.Less(entry.Key) {
						break
					}
					if entry.VersionID != version.ID {
						break
					}
					iter := t.newIndexIter(
						state.uniqueIndexes.Copy().Iter(),
						state.rows,
						tx,
						entry.Index,
						append(entry.Index, Min),
					)
					for ok := iter.First(); ok; ok = iter.Next() {
						index := iter.Item()
						if index.Key.Less(entry.Key) ||
							entry.Key.Less(index.Key) ||
							index.VersionID != entry.VersionID {
							return moerr.NewDuplicate()
						}
					}
				}

				physicalRow.Versions[i] = version
			}
			state.rows.Set(physicalRow)

			// delete write entry
			state.writes.Delete(entry)

		}
		return nil
	})

}

func (t *Table[K, V, R]) FilterVersions(filterFunc func(K, []Version[V]) ([]Version[V], error)) error {
	return t.update(func(state *tableState[K, V]) error {
		rowsIter := state.rows.Copy().Iter()
		defer rowsIter.Release()
		for ok := rowsIter.First(); ok; ok = rowsIter.Next() {
			physicalRow := rowsIter.Item()
			key := physicalRow.Key

			newVersions, err := filterFunc(key, physicalRow.Versions)
			if err != nil {
				return err
			}

			if len(newVersions) == 0 {
				// delete
				state.rows.Delete(physicalRow)
			} else {
				// update
				physicalRow = physicalRow.clone()
				physicalRow.Versions = newVersions
				state.rows.Set(physicalRow)
			}

			newVersionIDSet := make(map[int64]bool)
			for _, v := range newVersions {
				newVersionIDSet[v.ID] = true
			}

			// remove indexes
			iter := state.reverseIndexes.Copy().Iter()
			for ok := iter.Seek(&ReverseIndexEntry[K, V]{
				Key:       key,
				VersionID: 0,
			}); ok; ok = iter.Next() {
				entry := iter.Item()
				if key.Less(entry.Key) {
					break
				}
				if newVersionIDSet[entry.VersionID] {
					continue
				}
				state.indexes.Delete(&IndexEntry[K, V]{
					Index:     entry.Index,
					Key:       entry.Key,
					VersionID: entry.VersionID,
				})
				state.reverseIndexes.Delete(entry)
			}

			// remove unique indexes
			iter = state.reverseUniqueIndexes.Copy().Iter()
			for ok := iter.Seek(&ReverseIndexEntry[K, V]{
				Key:       key,
				VersionID: 0,
			}); ok; ok = iter.Next() {
				entry := iter.Item()
				if key.Less(entry.Key) {
					break
				}
				if newVersionIDSet[entry.VersionID] {
					continue
				}
				state.uniqueIndexes.Delete(&IndexEntry[K, V]{
					Index:     entry.Index,
					Key:       entry.Key,
					VersionID: entry.VersionID,
				})
				state.reverseUniqueIndexes.Delete(entry)
			}
		}

		return nil
	})
}

func (t *Table[K, V, R]) AbortTx(tx *Transaction) error {
	return t.update(func(state *tableState[K, V]) error {
		iter := state.writes.Copy().Iter()
		defer iter.Release()
		pivot := &WriteEntry[K, V]{
			Transaction: tx,
		}
		for ok := iter.Seek(pivot); ok; ok = iter.Next() {
			entry := iter.Item()
			if entry.Transaction != tx {
				break
			}
			state.writes.Delete(entry)
		}
		return nil
	})
}

func (t *Table[K, V, R]) update(
	fn func(state *tableState[K, V]) error,
) error {
	t.Lock()
	defer t.Unlock()
	state := t.state.Load()
	newState := state.Copy()
	if err := fn(newState); err != nil {
		return err
	}
	t.state.Store(newState)
	return nil
}

func validate[
	K memorytable.Ordered[K],
	V any,
](
	physicalRow *PhysicalRow[K, V],
	tx *Transaction,
) error {

	for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
		version := physicalRow.Versions[i]

		// locked by another committed tx after tx begin
		if version.LockTx != nil &&
			version.LockTx.State.Load() == Committed &&
			version.LockTx.ID != tx.ID &&
			version.LockTime.After(tx.BeginTime) {
			//err = moerr.NewPrimaryKeyDuplicated(physicalRow.Key)
			return moerr.NewDuplicate()
		}

		// born in another committed tx after tx begin
		if version.BornTx.State.Load() == Committed &&
			version.BornTx.ID != tx.ID &&
			version.BornTime.After(tx.BeginTime) {
			//err = moerr.NewPrimaryKeyDuplicated(physicalRow.Key)
			return moerr.NewDuplicate()
		}

	}

	return nil
}

func (t *Table[K, V, R]) Dump(out io.Writer) {
	iter := t.state.Load().rows.Copy().Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		item := iter.Item()
		fmt.Fprintf(out, "key: %+v\n", item.Key)
		for _, version := range item.Versions {
			fmt.Fprintf(out, "\tversion: %+v\n", version)
		}
	}
}
