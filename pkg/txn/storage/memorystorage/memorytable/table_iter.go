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

import "github.com/tidwall/btree"

type TableIter[
	K Ordered[K],
	V any,
] struct {
	tx   *Transaction
	iter btree.GenericIter[*KVPair[K, V]]
}

func (t *Table[K, V, R]) NewIter(tx *Transaction) (*TableIter[K, V], error) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return nil, err
	}
	state := txTable.state.Load().(*tableState[K, V])
	ret := &TableIter[K, V]{
		tx:   tx,
		iter: state.rows.Copy().Iter(),
	}
	return ret, nil
}

var _ KVIter[Int, int] = new(TableIter[Int, int])

func (t *TableIter[K, V]) Read() (key K, value V, err error) {
	pair := t.iter.Item()
	key = pair.Key
	value = pair.Value
	return
}

func (t *TableIter[K, V]) Next() bool {
	return t.iter.Next()
}

func (t *TableIter[K, V]) First() bool {
	return t.iter.First()
}

func (t *TableIter[K, V]) Seek(key K) bool {
	return t.iter.Seek(&KVPair[K, V]{
		Key: key,
	})
}

func (t *TableIter[K, V]) Close() error {
	t.iter.Release()
	return nil
}
