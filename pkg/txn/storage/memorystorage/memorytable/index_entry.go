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

// IndexEntry represents an index entry
type IndexEntry[
	K Ordered[K],
	V any,
] struct {
	Index Tuple
	Key   K
}

func compareIndexEntry[
	K Ordered[K],
	V any,
](a, b *IndexEntry[K, V]) bool {
	if a.Index.Less(b.Index) {
		return true
	}
	if b.Index.Less(a.Index) {
		return false
	}
	return a.Key.Less(b.Key)
}
