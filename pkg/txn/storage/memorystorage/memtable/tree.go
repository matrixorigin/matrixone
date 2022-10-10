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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/btree"
)

type Tree[
	K Ordered[K],
	V any,
	R Row[K, V],
] struct {
	rows    *btree.BTreeG[*PhysicalRow[K, V]]
	indexes *btree.BTreeG[*IndexEntry[K, V]]
	writes  *btree.BTreeG[*WriteEntry[K, V]]
}

var btreeOptions = btree.Options{
	NoLocks: true,
}

func NewTree[
	K Ordered[K],
	V any,
	R Row[K, V],
]() *Tree[K, V, R] {
	return &Tree[K, V, R]{
		rows:    btree.NewBTreeGOptions(comparePhysicalRow[K, V], btreeOptions),
		indexes: btree.NewBTreeGOptions(compareIndexEntry[K, V], btreeOptions),
		writes:  btree.NewBTreeGOptions(compareWriteEntry[K, V], btreeOptions),
	}
}

type IndexEntry[
	K Ordered[K],
	V any,
] struct {
	Index     Tuple
	Key       K
	VersionID int64
	Value     V
}

type WriteEntry[
	K Ordered[K],
	V any,
] struct {
	Transaction *Transaction
	Row         *PhysicalRow[K, V]
	VersionID   int64
}

func comparePhysicalRow[
	K Ordered[K],
	V any,
](a, b *PhysicalRow[K, V]) bool {
	return a.Key.Less(b.Key)
}

func compareIndexEntry[
	K Ordered[K],
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

func compareWriteEntry[
	K Ordered[K],
	V any,
](a, b *WriteEntry[K, V]) bool {
	if a.Transaction.ID < b.Transaction.ID {
		return true
	}
	if a.Transaction.ID > b.Transaction.ID {
		return false
	}
	if a.Row != nil && b.Row != nil {
		if a.Row.Key.Less(b.Row.Key) {
			return true
		}
		if b.Row.Key.Less(a.Row.Key) {
			return false
		}
	}
	return a.Row == nil && b.Row != nil
}

func getRowByKey[
	K Ordered[K],
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
	K Ordered[K],
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

var _ Piece[dumbRow, dumbRow] = new(Tree[dumbRow, dumbRow, dumbRow])

func (t *Tree[K, V, R]) Copy() Piece[K, V] {
	newTree := &Tree[K, V, R]{
		rows:    t.rows.Copy(),
		indexes: t.indexes.Copy(),
		writes:  t.writes.Copy(),
	}
	return newTree
}

func (t *Tree[K, V, R]) Get(
	tx *Transaction,
	key K,
) (
	value V,
	err error,
) {
	physicalRow := getRowByKey(t.rows, key)
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

var _ IndexedPiece[dumbRow, dumbRow] = new(Tree[dumbRow, dumbRow, dumbRow])

func (t *Tree[K, V, R]) Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error) {
	iter := t.indexes.Copy().Iter()
	defer iter.Release()
	pivot := &IndexEntry[K, V]{
		Index: index,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		entry := iter.Item()
		if index.Less(entry.Index) {
			break
		}
		if entry.Index.Less(index) {
			break
		}

		physicalRow := getRowByKey(t.rows, entry.Key)
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

		if currentVersion.ID == entry.VersionID {
			entries = append(entries, entry)
		}
	}
	return
}

var _ MutablePiece[dumbRow, dumbRow, dumbRow] = new(Tree[dumbRow, dumbRow, dumbRow])

func (t *Tree[K, V, R]) Insert(tx *Transaction, row R) error {
	key := row.Key()
	physicalRow := getOrSetRowByKey(t.rows, key)

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
	for _, index := range row.Indexes() {
		t.indexes.Set(&IndexEntry[K, V]{
			Index:     index,
			Key:       key,
			VersionID: version.ID,
			Value:     value,
		})
	}

	// write entry
	tx.committers[t] = struct{}{}
	t.writes.Set(&WriteEntry[K, V]{
		Transaction: tx,
		Row:         physicalRow,
		VersionID:   version.ID,
	})

	// row entry
	t.rows.Set(physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Tree[K, V, R]) Update(
	tx *Transaction,
	row R,
) error {
	key := row.Key()

	physicalRow := getOrSetRowByKey(t.rows, key)

	value := row.Value()
	physicalRow, version, err := physicalRow.Update(
		tx.Time, tx, value,
	)
	if err != nil {
		return err
	}

	// index entry
	for _, index := range row.Indexes() {
		t.indexes.Set(&IndexEntry[K, V]{
			Index:     index,
			Key:       key,
			VersionID: version.ID,
			Value:     value,
		})
	}

	// write entry
	tx.committers[t] = struct{}{}
	t.writes.Set(&WriteEntry[K, V]{
		Transaction: tx,
		Row:         physicalRow,
		VersionID:   version.ID,
	})

	// row entry
	t.rows.Set(physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Tree[K, V, R]) Delete(
	tx *Transaction,
	key K,
) error {

	physicalRow := getRowByKey(t.rows, key)
	if physicalRow == nil {
		return nil
	}

	physicalRow, version, err := physicalRow.Delete(tx.Time, tx)
	if err != nil {
		return err
	}

	// write entry
	tx.committers[t] = struct{}{}
	t.writes.Set(&WriteEntry[K, V]{
		Transaction: tx,
		Row:         physicalRow,
		VersionID:   version.ID,
	})

	// row entry
	t.rows.Set(physicalRow)

	tx.Time.Tick()
	return nil

}

func (t *Tree[K, V, R]) CommitTx(tx *Transaction) error {
	iter := t.writes.Copy().Iter()
	defer iter.Release()
	pivot := &WriteEntry[K, V]{
		Transaction: tx,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.Transaction != tx {
			break
		}

		if err := validate(entry.Row, tx); err != nil {
			return err
		}

		// set born time or lock time to commit time
		physicalRow := entry.Row.clone()
		for i, version := range physicalRow.Versions {
			if version.ID == entry.VersionID {
				if version.LockTx == tx {
					version.LockTime = tx.CommitTime
				}
				if version.BornTx == tx {
					version.BornTime = tx.CommitTime
				}
				physicalRow.Versions[i] = version
			}
		}
		t.rows.Set(physicalRow)

		// delete write entry
		t.writes.Delete(entry)
	}
	return nil
}

func (t *Tree[K, V, R]) AbortTx(tx *Transaction) {
	iter := t.writes.Copy().Iter()
	defer iter.Release()
	pivot := &WriteEntry[K, V]{
		Transaction: tx,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.Transaction != tx {
			break
		}
		t.writes.Delete(entry)
	}
}

func validate[
	K Ordered[K],
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
