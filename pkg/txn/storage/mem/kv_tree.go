// Copyright 2021 - 2022 Matrix Origin
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

package mem

import (
	"bytes"

	"github.com/google/btree"
)

type treeItem struct {
	key   []byte
	value []byte
}

func (item treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(treeItem).key
	return bytes.Compare(right, left) > 0
}

// KV mem kv based on btree. Just used to test transaction.
type KV struct {
	tree *btree.BTree
	tmp  treeItem
}

// NewKV returns a Mem KV
func NewKV() *KV {
	return &KV{
		tree: btree.New(32),
	}
}

// Set set key value to KV
func (kv *KV) Set(key, value []byte) {
	kv.tree.ReplaceOrInsert(treeItem{
		key:   key,
		value: value,
	})
}

// Delete delete key from KV
func (kv *KV) Delete(key []byte) bool {
	item := treeItem{key: key}
	return nil != kv.tree.Delete(item)
}

// Get returns the value of the key
func (kv *KV) Get(key []byte) ([]byte, bool) {
	kv.tmp.key = key
	var result treeItem
	kv.tree.AscendGreaterOrEqual(kv.tmp, func(i btree.Item) bool {
		result = i.(treeItem)
		return false
	})
	if bytes.Equal(result.key, key) {
		return result.value, true
	}
	return nil, false
}

// Len return the count of keys
func (kv *KV) Len() int {
	return kv.tree.Len()
}

// AscendRange iter in [start, end)
func (kv *KV) AscendRange(start, end []byte, fn func(key, value []byte) bool) {
	kv.tmp.key = start
	kv.tree.AscendGreaterOrEqual(kv.tmp, func(i btree.Item) bool {
		target := i.(treeItem)
		if bytes.Compare(target.key, end) < 0 {
			return fn(target.key, target.value)
		}
		return false
	})
}

func (kv *KV) DescendRange(
	key []byte,
	fn func(key, value []byte) bool,
) {
	kv.tmp.key = key
	kv.tree.DescendLessOrEqual(kv.tmp, func(i btree.Item) bool {
		target := i.(treeItem)
		if bytes.Compare(target.key, key) <= 0 {
			return fn(target.key, target.value)
		}
		return false
	})
}
