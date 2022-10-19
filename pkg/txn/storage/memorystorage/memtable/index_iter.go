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

	"github.com/tidwall/btree"
)

type IndexIter[
	K Ordered[K],
	V any,
] struct {
	iter     btree.GenericIter[*IndexEntry[K, V]]
	rows     *btree.BTreeG[*PhysicalRow[K, V]]
	readTime Time
	tx       *Transaction
	min      Tuple
	max      Tuple
}

func (t *Table[K, V, R]) NewIndexIter(tx *Transaction, min Tuple, max Tuple) *IndexIter[K, V] {
	state := t.state.Load()
	return &IndexIter[K, V]{
		iter:     state.indexes.Copy().Iter(),
		rows:     state.rows,
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
		if !entry.Index.Less(i.max) {
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
