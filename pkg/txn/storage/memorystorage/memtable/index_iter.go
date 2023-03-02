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

	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/tidwall/btree"
)

type IndexIter[
	K memorytable.Ordered[K],
	V any,
] struct {
	iter           btree.GenericIter[*IndexEntry[K, V]]
	rows           *btree.BTreeG[*PhysicalRow[K, V]]
	readTime       Time
	tx             *Transaction
	min            Tuple
	max            Tuple
	currentVersion *Version[V]
}

func (t *Table[K, V, R]) NewIndexIter(tx *Transaction, min Tuple, max Tuple) *IndexIter[K, V] {
	state := t.state.Load()
	return t.newIndexIter(
		state.indexes.Copy().Iter(),
		state.rows,
		tx,
		min,
		max,
	)
}

func (t *Table[K, V, R]) newIndexIter(
	iter btree.GenericIter[*IndexEntry[K, V]],
	rows *btree.BTreeG[*PhysicalRow[K, V]],
	tx *Transaction,
	min Tuple,
	max Tuple,
) *IndexIter[K, V] {
	return &IndexIter[K, V]{
		iter:     iter,
		rows:     rows,
		readTime: tx.Time,
		tx:       tx,
		min:      min,
		max:      max,
	}
}

func (i *IndexIter[K, V]) First() bool {
	if !i.iter.First() {
		return false
	}
	if !i.iter.Seek(&IndexEntry[K, V]{
		Index: i.min,
	}) {
		return false
	}
	return i.seekToValid()
}

func (i *IndexIter[K, V]) seekToValid() bool {
	for {
		entry := i.iter.Item()
		// check range
		if entry.Index.Less(i.min) {
			return false
		}
		if i.max.Less(entry.Index) {
			return false
		}
		// check physical row
		physicalRow := getRowByKey(i.rows, entry.Key)
		if physicalRow == nil {
			if !i.iter.Next() {
				return false
			}
			continue
		}
		// check current version
		currentVersion, err := physicalRow.readVersion(i.readTime, i.tx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if !i.iter.Next() {
					return false
				}
				continue
			}
			panic(err)
		}
		if currentVersion.ID != entry.VersionID {
			if !i.iter.Next() {
				return false
			}
			continue
		}
		i.currentVersion = currentVersion
		return true
	}
}

func (i *IndexIter[K, V]) Next() bool {
	if !i.iter.Next() {
		return false
	}
	return i.seekToValid()
}

func (i *IndexIter[K, V]) Close() error {
	i.iter.Release()
	return nil
}

func (i *IndexIter[K, V]) Item() *IndexEntry[K, V] {
	return i.iter.Item()
}

func (i *IndexIter[K, V]) Read() (key K, value V, err error) {
	item := i.iter.Item()
	key = item.Key
	value = i.currentVersion.Value
	return
}
