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

import "github.com/tidwall/btree"

type TablePhysicalIter[
	K Ordered[K],
	V any,
] struct {
	iter btree.GenericIter[*TreeItem[K, V]]
}

func (t *Table[K, V, R]) NewPhysicalIter() *TablePhysicalIter[K, V] {
	ret := &TablePhysicalIter[K, V]{
		iter: t.tree.Load().Iter(),
	}
	ret.seekFirst()
	return ret
}

func (t *TablePhysicalIter[K, V]) seekFirst() {
	var zero K
	t.iter.Seek(&TreeItem[K, V]{
		Row: &PhysicalRow[K, V]{
			Key: zero,
		},
	})
}

func (t *TablePhysicalIter[K, V]) Close() error {
	t.iter.Release()
	return nil
}

func (t *TablePhysicalIter[K, V]) Seek(pivot *PhysicalRow[K, V]) bool {
	t.seekFirst()
	return t.iter.Seek(&TreeItem[K, V]{
		Row: pivot,
	})
}

func (t *TablePhysicalIter[K, V]) First() bool {
	if !t.iter.First() {
		return false
	}
	t.seekFirst()
	item := t.iter.Item()
	return item.Row != nil
}

func (t *TablePhysicalIter[K, V]) Next() bool {
	if !t.iter.Next() {
		return false
	}
	item := t.iter.Item()
	return item.Row != nil
}

func (t *TablePhysicalIter[K, V]) Item() *PhysicalRow[K, V] {
	item := t.iter.Item()
	if item == nil {
		panic("impossible")
	}
	return item.Row
}
