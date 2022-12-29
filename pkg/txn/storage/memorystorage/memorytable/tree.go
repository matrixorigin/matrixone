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

type Tree[
	K Ordered[K],
	V any,
] interface {
	Copy() Tree[K, V]
	Get(TreeNode[K, V]) (TreeNode[K, V], bool)
	Set(TreeNode[K, V]) (TreeNode[K, V], bool)
	Delete(TreeNode[K, V])
	Iter() TreeIter[K, V]
}

type TreeNode[
	K Ordered[K],
	V any,
] struct {
	IndexEntry *IndexEntry[K, V]
	KVPair     *KVPair[K, V]
}

type TreeIter[
	K Ordered[K],
	V any,
] interface {
	SeekIter[TreeNode[K, V]]
}

func compareTreeNode[
	K Ordered[K],
	V any,
](a, b TreeNode[K, V]) bool {
	if a.KVPair != nil && b.KVPair != nil {
		return compareKVPair(*a.KVPair, *b.KVPair)
	}
	if a.IndexEntry != nil && b.IndexEntry != nil {
		return compareIndexEntry(a.IndexEntry, b.IndexEntry)
	}
	if a.KVPair != nil && b.IndexEntry != nil {
		return true
	}
	if a.IndexEntry != nil && b.KVPair != nil {
		return false
	}
	panic("impossible")
}
