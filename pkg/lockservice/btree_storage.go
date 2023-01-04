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

package lockservice

import (
	"bytes"

	"github.com/google/btree"
)

type treeItem struct {
	key   []byte
	value Lock
}

func (item treeItem) isEmpty() bool {
	return len(item.key) == 0 &&
		len(item.value.txnID) == 0
}

// Less returns true if the item key is less than the other.
func (item treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(treeItem).key
	return bytes.Compare(right, left) > 0
}

type btreeBasedStorage struct {
	tree *btree.BTree
}

func newBtreeBasedStorage() LockStorage {
	return &btreeBasedStorage{
		tree: btree.New(32),
	}
}

func (k *btreeBasedStorage) Add(key []byte, value Lock) {
	k.tree.ReplaceOrInsert(treeItem{
		key:   key,
		value: value,
	})
}

func (k *btreeBasedStorage) Get(key []byte) (Lock, bool) {
	item := k.tree.Get(treeItem{key: key})
	if item == nil {
		return Lock{}, false
	}
	return item.(treeItem).value, true
}

func (k *btreeBasedStorage) Len() int {
	return k.tree.Len()
}

func (k *btreeBasedStorage) Delete(key []byte) {
	k.tree.Delete(treeItem{key: key})
}

func (k *btreeBasedStorage) Seek(key []byte) ([]byte, Lock, bool) {
	var result treeItem
	k.tree.AscendGreaterOrEqual(treeItem{key: key}, func(i btree.Item) bool {
		result = i.(treeItem)
		return false
	})
	return result.key, result.value, !result.isEmpty()
}
