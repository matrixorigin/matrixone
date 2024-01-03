// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"container/heap"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

// min heap item
type mItem[T any] struct {
	row   int
	entry T
}

type itemSet[T any] []*mItem[T]

func (is itemSet[T]) Len() int { return len(is) }

func (is itemSet[T]) Less(i, j int) bool {
	// max heap
	return is[i].row > is[j].row
}

func (is itemSet[T]) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is *itemSet[T]) Push(x any) {
	item := x.(*mItem[T])
	*is = append(*is, item)
}

func (is *itemSet[T]) Pop() any {
	old := *is
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*is = old[0 : n-1]
	return item
}

func (is *itemSet[T]) Clear() {
	old := *is
	*is = old[:0]
}

// heapBuilder founds out blocks to be merged via maintaining a min heap
type heapBuilder[T any] struct {
	items itemSet[T]
}

func (h *heapBuilder[T]) reset() {
	h.items.Clear()
}

func (h *heapBuilder[T]) pushWithCap(item *mItem[T], cap int) {
	heap.Push(&h.items, item)
	for h.items.Len() > cap {
		heap.Pop(&h.items)
	}
}

// copy out the items in the heap
func (h *heapBuilder[T]) finish() []T {
	ret := make([]T, h.items.Len())
	for i, item := range h.items {
		ret[i] = item.entry
	}
	return ret
}

func estimateMergeConsume(mobjs []*catalog.ObjectEntry) (origSize, estSize, compSize int) {
	if len(mobjs) == 0 {
		return
	}
	rows, merged := 0, 0
	for _, m := range mobjs {
		rows += m.GetRows()
		merged += m.GetRemainingRows()
		origSize += m.GetOriginSize()
		compSize += m.GetCompSize()
	}
	// by test exprience, full 8192 rows batch will expand to (6~8)x memory comsupation.
	// the ExpansionRate will be moderated by the actual row number after applying deletes
	factor := float64(merged) / float64(rows)
	rate := float64(constMergeExpansionRate) * factor
	if rate < 2 {
		rate = 2
	}
	estSize = int(float64(origSize) * rate)

	// compSize is estimated after applying deletes
	compSize = int(float64(compSize) * factor)
	return
}
