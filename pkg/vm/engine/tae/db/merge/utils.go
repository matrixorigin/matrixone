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
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/objectio"
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

// heapBuilder founds out blocks to be merged via maintaining a min heap holding
// up to default 300 items.
type heapBuilder[T any] struct {
	items itemSet[T]
	cap   int
}

func (h *heapBuilder[T]) reset() {
	h.items.Clear()
}

func (h *heapBuilder[T]) push(item *mItem[T]) {
	heap.Push(&h.items, item)
	if h.items.Len() > h.cap {
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

func humanReadableBytes(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2fKB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2fMB", float64(bytes)/const1MBytes)
	}
	return fmt.Sprintf("%.2fGB", float64(bytes)/const1GBytes)
}

func shortSegId(x objectio.Segmentid) string {
	var shortuuid [8]byte
	hex.Encode(shortuuid[:], x[:4])
	return string(shortuuid[:])
}
