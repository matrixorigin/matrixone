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

type BTreeLog[
	K Ordered[K],
	V any,
] struct {
	log *btree.BTreeG[*logEntry[K, V]]
}

func NewBTreeLog[
	K Ordered[K],
	V any,
]() *BTreeLog[K, V] {
	return &BTreeLog[K, V]{
		log: btree.NewBTreeG(compareLogEntry[K, V]),
	}
}

var _ Log[Int, int] = new(BTreeLog[Int, int])

func (b *BTreeLog[K, V]) Set(entry *logEntry[K, V]) {
	b.log.Set(entry)
}

type btreeLogIter[
	K Ordered[K],
	V any,
] struct {
	iter btree.IterG[*logEntry[K, V]]
}

func (b *BTreeLog[K, V]) Iter() LogIter[K, V] {
	iter := b.log.Iter()
	return &btreeLogIter[K, V]{
		iter: iter,
	}
}

func (b *btreeLogIter[K, V]) Close() error {
	b.iter.Release()
	return nil
}

func (b *btreeLogIter[K, V]) First() bool {
	return b.iter.First()
}

func (b *btreeLogIter[K, V]) Next() bool {
	return b.iter.Next()
}

func (b *btreeLogIter[K, V]) Read() (*logEntry[K, V], error) {
	return b.iter.Item(), nil
}
