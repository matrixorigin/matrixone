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

// TableIter represents an iter over a table
type TableIter[
	K Ordered[K],
	V any,
] struct {
	tx   *Transaction
	iter RowsIter[K, V]
}

// NewIter creates a new iter
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

// Read reads current key and value
func (t *TableIter[K, V]) Read() (key K, value V, err error) {
	var pair *KVPair[K, V]
	pair, err = t.iter.Read()
	if err != nil {
		return
	}
	key = pair.Key
	value = pair.Value
	return
}

// Next moves the cursor to next entry
func (t *TableIter[K, V]) Next() bool {
	return t.iter.Next()
}

// First moves the cursor to first entry
func (t *TableIter[K, V]) First() bool {
	return t.iter.First()
}

// Seek moves the cursor to or before the key
func (t *TableIter[K, V]) Seek(key K) bool {
	return t.iter.Seek(&KVPair[K, V]{
		Key: key,
	})
}

// Close closes the iter
func (t *TableIter[K, V]) Close() error {
	return t.iter.Close()
}
