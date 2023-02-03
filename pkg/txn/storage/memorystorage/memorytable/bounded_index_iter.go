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

import "fmt"

// BoundedIndexIter wraps another index iter within bounds
type BoundedIndexIter[
	K Ordered[K],
	V any,
] struct {
	iter TreeIter[K, V]
	min  Tuple
	max  Tuple
}

func NewBoundedIndexIter[
	K Ordered[K],
	V any,
](iter TreeIter[K, V], min Tuple, max Tuple) *BoundedIndexIter[K, V] {
	if max.Less(min) {
		panic(fmt.Sprintf("%v is less than %v", max, min))
	}
	return &BoundedIndexIter[K, V]{
		iter: iter,
		min:  min,
		max:  max,
	}
}

var _ Iter[*IndexEntry[Int, int]] = new(BoundedIndexIter[Int, int])

// First sets the cursor to the first index entry
func (i *BoundedIndexIter[K, V]) First() bool {
	if !i.iter.First() {
		return false
	}
	if !i.iter.Seek(TreeNode[K, V]{
		IndexEntry: &IndexEntry[K, V]{
			Index: i.min,
		},
	}) {
		return false
	}
	node, err := i.iter.Read()
	if err != nil {
		panic(err)
	}
	if node.IndexEntry == nil {
		return false
	}
	if node.IndexEntry.Index.Less(i.min) {
		return false
	}
	if i.max.Less(node.IndexEntry.Index) {
		return false
	}
	return true
}

// Next reports whether next entry is valid
func (i *BoundedIndexIter[K, V]) Next() bool {
	if !i.iter.Next() {
		return false
	}
	node, err := i.iter.Read()
	if err != nil {
		panic(err)
	}
	if node.IndexEntry == nil {
		return false
	}
	if node.IndexEntry.Index.Less(i.min) {
		return false
	}
	if i.max.Less(node.IndexEntry.Index) {
		return false
	}
	return true
}

// Close closes the iter
func (i *BoundedIndexIter[K, V]) Close() error {
	return i.iter.Close()
}

// Read returns the current entry
func (i *BoundedIndexIter[K, V]) Read() (*IndexEntry[K, V], error) {
	node, err := i.iter.Read()
	if err != nil {
		return nil, err
	}
	return node.IndexEntry, nil
}

// Seek seeks to the pivot
func (i *BoundedIndexIter[K, V]) Seek(pivot *IndexEntry[K, V]) bool {
	return i.iter.Seek(TreeNode[K, V]{
		IndexEntry: pivot,
	})
}
