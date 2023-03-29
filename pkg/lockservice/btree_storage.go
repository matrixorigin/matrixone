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
	tree *btree.BTreeG[treeItem]
}

func newBtreeBasedStorage() LockStorage {
	return &btreeBasedStorage{
		tree: btree.NewG(32, func(a, b treeItem) bool {
			return bytes.Compare(a.key, b.key) < 0
		}),
	}
}

func (k *btreeBasedStorage) Add(key []byte, value Lock) {
	k.tree.ReplaceOrInsert(treeItem{
		key:   key,
		value: value,
	})
}

func (k *btreeBasedStorage) Get(key []byte) (Lock, bool) {
	item, ok := k.tree.Get(treeItem{key: key})
	return item.value, ok
}

func (k *btreeBasedStorage) Len() int {
	return k.tree.Len()
}

func (k *btreeBasedStorage) Delete(key []byte) (Lock, bool) {
	item, ok := k.tree.Delete(treeItem{key: key})
	return item.value, ok
}

func (k *btreeBasedStorage) Seek(key []byte) ([]byte, Lock, bool) {
	var result treeItem
	k.tree.AscendGreaterOrEqual(treeItem{key: key}, func(item treeItem) bool {
		result = item
		return false
	})
	return result.key, result.value, !result.isEmpty()
}

func (k *btreeBasedStorage) Prev(key []byte) ([]byte, Lock, bool) {
	var result treeItem
	k.tree.AscendLessThan(treeItem{key: key}, func(item treeItem) bool {
		result = item
		return false
	})
	return result.key, result.value, !result.isEmpty()
}

func (k *btreeBasedStorage) Range(
	start, end []byte,
	fn func([]byte, Lock) bool) {
	k.tree.AscendRange(
		treeItem{key: start},
		treeItem{key: end},
		func(item treeItem) bool {
			return fn(item.key, item.value)
		})
}

func (k *btreeBasedStorage) Iter(fn func([]byte, Lock) bool) {
	k.tree.Ascend(func(item treeItem) bool {
		return fn(item.key, item.value)
	})
}

func (k *btreeBasedStorage) Clear() {
	k.tree.Clear(false)
}
