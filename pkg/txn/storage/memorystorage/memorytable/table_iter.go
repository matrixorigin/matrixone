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
	iter TreeIter[K, V]
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
		iter: state.tree.Copy().Iter(),
	}
	return ret, nil
}

// Read reads current key and value
func (t *TableIter[K, V]) Read() (key K, value V, err error) {
	var node TreeNode[K, V]
	node, err = t.iter.Read()
	if err != nil {
		return
	}
	if node.KVPair == nil {
		panic("should not call Read on nil KVPair node")
	}
	key = node.KVPair.Key
	value = node.KVPair.Value
	return
}

// Next moves the cursor to next entry
func (t *TableIter[K, V]) Next() bool {
	if !t.iter.Next() {
		return false
	}
	node, err := t.iter.Read()
	if err != nil {
		panic(err)
	}
	return node.KVPair != nil
}

// First moves the cursor to first entry
func (t *TableIter[K, V]) First() bool {
	if !t.iter.First() {
		return false
	}
	node, err := t.iter.Read()
	if err != nil {
		panic(err)
	}
	return node.KVPair != nil
}

// Seek moves the cursor to or before the key
func (t *TableIter[K, V]) Seek(key K) bool {
	if !t.iter.Seek(TreeNode[K, V]{
		KVPair: &KVPair[K, V]{
			Key: key,
		},
	}) {
		return false
	}
	node, err := t.iter.Read()
	if err != nil {
		panic(err)
	}
	return node.KVPair != nil
}

// Close closes the iter
func (t *TableIter[K, V]) Close() error {
	return t.iter.Close()
}
