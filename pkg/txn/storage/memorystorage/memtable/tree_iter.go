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

type TreeIter[
	K Ordered[K],
	V any,
] struct {
	tx       *Transaction
	iter     btree.GenericIter[*PhysicalRow[K, V]]
	readTime Time
}

func (t *Tree[K, V, R]) NewIter(tx *Transaction) Iter[K, V] {
	iter := t.rows.Copy().Iter()
	ret := &TreeIter[K, V]{
		tx:       tx,
		iter:     iter,
		readTime: tx.Time,
	}
	return ret
}

func (t *TreeIter[K, V]) Read() (key K, value V, err error) {
	physicalRow := t.iter.Item()
	if physicalRow == nil {
		panic("impossible")
	}
	key = physicalRow.Key
	value, err = physicalRow.Read(t.readTime, t.tx)
	if err != nil {
		return
	}
	return
}

func (t *TreeIter[K, V]) Item() any {
	return t.iter.Item()
}

func (t *TreeIter[K, V]) Next() bool {
	for {
		if ok := t.iter.Next(); !ok {
			return false
		}
		// skip unreadable values
		item := t.iter.Item()
		_, err := item.Read(t.readTime, t.tx)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		return true
	}
}

func (t *TreeIter[K, V]) First() bool {
	if ok := t.iter.First(); !ok {
		return false
	}
	for {
		// skip unreadable values
		item := t.iter.Item()
		_, err := item.Read(t.readTime, t.tx)
		if errors.Is(err, sql.ErrNoRows) {
			if ok := t.iter.Next(); !ok {
				return false
			}
			continue
		}
		return true
	}
}

func (t *TreeIter[K, V]) Seek(key K) bool {
	pivot := &PhysicalRow[K, V]{
		Key: key,
	}
	if ok := t.iter.Seek(pivot); !ok {
		return false
	}
	for {
		// skip unreadable values
		item := t.iter.Item()
		_, err := item.Read(t.readTime, t.tx)
		if errors.Is(err, sql.ErrNoRows) {
			if ok := t.iter.Next(); !ok {
				return false
			}
			continue
		}
		return true
	}
}

func (t *TreeIter[K, V]) Close() error {
	t.iter.Release()
	return nil
}
