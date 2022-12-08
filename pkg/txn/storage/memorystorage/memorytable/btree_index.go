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

type BTreeIndex[
	K Ordered[K],
	V any,
] struct {
	index *btree.BTreeG[*IndexEntry[K, V]]
}

func NewBTreeIndex[
	K Ordered[K],
	V any,
]() *BTreeIndex[K, V] {
	return &BTreeIndex[K, V]{
		index: btree.NewBTreeG(compareIndexEntry[K, V]),
	}
}

var _ Index[Int, int] = new(BTreeIndex[Int, int])

func (b *BTreeIndex[K, V]) Copy() Index[K, V] {
	return &BTreeIndex[K, V]{
		index: b.index.Copy(),
	}
}

func (b *BTreeIndex[K, V]) Delete(entry *IndexEntry[K, V]) {
	b.index.Delete(entry)
}

func (b *BTreeIndex[K, V]) Set(entry *IndexEntry[K, V]) {
	b.index.Set(entry)
}

type btreeIndexIter[
	K Ordered[K],
	V any,
] struct {
	iter btree.GenericIter[*IndexEntry[K, V]]
}

func (b *BTreeIndex[K, V]) Iter() IndexIter[K, V] {
	iter := b.index.Iter()
	return &btreeIndexIter[K, V]{
		iter: iter,
	}
}

func (b *btreeIndexIter[K, V]) Close() error {
	b.iter.Release()
	return nil
}

func (b *btreeIndexIter[K, V]) First() bool {
	return b.iter.First()
}

func (b *btreeIndexIter[K, V]) Next() bool {
	return b.iter.Next()
}

func (b *btreeIndexIter[K, V]) Read() (*IndexEntry[K, V], error) {
	return b.iter.Item(), nil
}

func (b *btreeIndexIter[K, V]) Seek(pivot *IndexEntry[K, V]) bool {
	return b.iter.Seek(pivot)
}
