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
	"github.com/tidwall/btree"
)

type BTree[
	K Ordered[K],
	V any,
] struct {
	rows *btree.BTreeG[TreeNode[K, V]]
}

func NewBTree[
	K Ordered[K],
	V any,
]() *BTree[K, V] {
	return &BTree[K, V]{
		rows: btree.NewBTreeG(compareTreeNode[K, V]),
	}
}

var _ Tree[Int, int] = new(BTree[Int, int])

func (b *BTree[K, V]) Copy() Tree[K, V] {
	return &BTree[K, V]{
		rows: b.rows.Copy(),
	}
}

func (b *BTree[K, V]) Get(pivot TreeNode[K, V]) (TreeNode[K, V], bool) {
	return b.rows.Get(pivot)
}

func (b *BTree[K, V]) Set(pair TreeNode[K, V]) (TreeNode[K, V], bool) {
	return b.rows.Set(pair)
}

func (b *BTree[K, V]) Delete(pivot TreeNode[K, V]) {
	b.rows.Delete(pivot)
}

type btreeKVIter[
	K Ordered[K],
	V any,
] struct {
	iter btree.IterG[TreeNode[K, V]]
}

func (b *BTree[K, V]) Iter() TreeIter[K, V] {
	iter := b.rows.Iter()
	return &btreeKVIter[K, V]{
		iter: iter,
	}
}

func (b *btreeKVIter[K, V]) Close() error {
	b.iter.Release()
	return nil
}

func (b *btreeKVIter[K, V]) First() bool {
	return b.iter.First()
}

func (b *btreeKVIter[K, V]) Next() bool {
	return b.iter.Next()
}

func (b *btreeKVIter[K, V]) Read() (TreeNode[K, V], error) {
	return b.iter.Item(), nil
}

func (b *btreeKVIter[K, V]) Seek(pivot TreeNode[K, V]) bool {
	return b.iter.Seek(pivot)
}
