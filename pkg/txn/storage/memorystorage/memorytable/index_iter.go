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

package memorytable

import (
	"fmt"

	"github.com/tidwall/btree"
)

type IndexIter[
	K Ordered[K],
	V any,
] struct {
	tx   *Transaction
	iter btree.GenericIter[*IndexEntry[K, V]]
	min  Tuple
	max  Tuple
}

func (t *Table[K, V, R]) NewIndexIter(tx *Transaction, min Tuple, max Tuple) (*IndexIter[K, V], error) {
	if max.Less(min) {
		panic(fmt.Sprintf("%v is less than %v", max, min))
	}
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return nil, err
	}
	state := txTable.state.Load().(*tableState[K, V])
	iter := state.indexes.Copy().Iter()
	return &IndexIter[K, V]{
		tx:   tx,
		iter: iter,
		min:  min,
		max:  max,
	}, nil
}

var _ Iter[*IndexEntry[Int, int]] = new(IndexIter[Int, int])

func (i *IndexIter[K, V]) First() bool {
	if !i.iter.First() {
		return false
	}
	if !i.iter.Seek(&IndexEntry[K, V]{
		Index: i.min,
	}) {
		return false
	}
	entry := i.iter.Item()
	if entry.Index.Less(i.min) {
		return false
	}
	if i.max.Less(entry.Index) {
		return false
	}
	return true
}

func (i *IndexIter[K, V]) Next() bool {
	if !i.iter.Next() {
		return false
	}
	entry := i.iter.Item()
	if entry.Index.Less(i.min) {
		return false
	}
	if i.max.Less(entry.Index) {
		return false
	}
	return true
}

func (i *IndexIter[K, V]) Close() error {
	i.iter.Release()
	return nil
}

func (i *IndexIter[K, V]) Read() (*IndexEntry[K, V], error) {
	return i.iter.Item(), nil
}
