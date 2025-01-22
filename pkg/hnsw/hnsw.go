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

package hnsw

import (
	"container/heap"
	"strings"
	"sync"

	usearch "github.com/unum-cloud/usearch/golang"
)

/*
quantization := map[string]usearch.Quantization{"BF16": usearch.BF16, "F16": usearch.F16,

	"F32": usearch.F32, "F64": usearch.F64, "I8": usearch.I8, "B1": usearch.B1}
*/
func QuantizationValid(a string) (usearch.Quantization, bool) {
	q := strings.ToUpper(a)
	// we can only support below quantization
	quantization := map[string]usearch.Quantization{"F16": usearch.F16,
		"F32": usearch.F32, "I8": usearch.I8}
	r, ok := quantization[q]
	return r, ok
}

type SearchResult struct {
	id       uint64
	distance float32
}

type SearchResultHeap []*SearchResult

func (h SearchResultHeap) Len() int { return len(h) }

func (h SearchResultHeap) Less(i, j int) bool {
	return h[i].distance > h[j].distance
}

func (h SearchResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SearchResultHeap) Push(x any) {
	item := x.(*SearchResult)
	*h = append(*h, item)
}

func (h *SearchResultHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

type SearchResultSafeHeap struct {
	mutex   sync.Mutex
	resheap SearchResultHeap
}

func NewSearchResultSafeHeap(size int) *SearchResultSafeHeap {
	h := &SearchResultSafeHeap{}
	h.resheap = make(SearchResultHeap, 0, size)
	heap.Init(&h.resheap)
	return h
}

func (h *SearchResultSafeHeap) Len() int {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.resheap.Len()
}

func (h *SearchResultSafeHeap) Push(id usearch.Key, distance float32) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	heap.Push(&h.resheap, &SearchResult{id, distance})
}

func (h *SearchResultSafeHeap) Pop() (usearch.Key, float32) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	x := heap.Pop(&h.resheap).(*SearchResult)
	return x.id, x.distance
}
