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

type BTreeRows[
	K Ordered[K],
	V any,
] struct {
	rows *btree.BTreeG[*KVPair[K, V]]
}

func NewBTreeRows[
	K Ordered[K],
	V any,
]() *BTreeRows[K, V] {
	return &BTreeRows[K, V]{
		rows: btree.NewBTreeG(compareKVPair[K, V]),
	}
}

var _ Rows[Int, int] = new(BTreeRows[Int, int])

func (b *BTreeRows[K, V]) Copy() Rows[K, V] {
	return &BTreeRows[K, V]{
		rows: b.rows.Copy(),
	}
}

func (b *BTreeRows[K, V]) Get(pivot *KVPair[K, V]) (*KVPair[K, V], bool) {
	return b.rows.Get(pivot)
}

func (b *BTreeRows[K, V]) Set(pair *KVPair[K, V]) (*KVPair[K, V], bool) {
	return b.rows.Set(pair)
}

func (b *BTreeRows[K, V]) Delete(pivot *KVPair[K, V]) {
	b.rows.Delete(pivot)
}

type btreeRowsIter[
	K Ordered[K],
	V any,
] struct {
	iter btree.GenericIter[*KVPair[K, V]]
}

func (b *BTreeRows[K, V]) Iter() RowsIter[K, V] {
	iter := b.rows.Iter()
	return &btreeRowsIter[K, V]{
		iter: iter,
	}
}

func (b *btreeRowsIter[K, V]) Close() error {
	b.iter.Release()
	return nil
}

func (b *btreeRowsIter[K, V]) First() bool {
	return b.iter.First()
}

func (b *btreeRowsIter[K, V]) Next() bool {
	return b.iter.Next()
}

func (b *btreeRowsIter[K, V]) Read() (*KVPair[K, V], error) {
	return b.iter.Item(), nil
}

func (b *btreeRowsIter[K, V]) Seek(pivot *KVPair[K, V]) bool {
	return b.iter.Seek(pivot)
}
