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

package vectorindex

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"container/heap"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
)

// get the checksum of the file
func CheckSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	chksum := hex.EncodeToString(h.Sum(nil))

	return chksum, nil
}

func CheckSumFromBuffer(b []byte) string {
	chksum := fmt.Sprintf("%x", md5.Sum(b))
	return chksum
}

// Priority Queue/Heap structure for getting N-Best results from multiple mini-indexes
type SearchResultIf interface {
	GetDistance() float64
}
type SearchResult struct {
	Id       int64
	Distance float64
}

func (s *SearchResult) GetDistance() float64 {
	return s.Distance
}

type SearchResultAnyKey struct {
	Id       any
	Distance float64
}

func (s *SearchResultAnyKey) GetDistance() float64 {
	return s.Distance
}

// Non thread-safe heap struct
type SearchResultHeap []SearchResultIf

func (h SearchResultHeap) Len() int { return len(h) }

func (h SearchResultHeap) Less(i, j int) bool {
	return h[i].GetDistance() < h[j].GetDistance()
}

func (h SearchResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SearchResultHeap) Push(x any) {
	item := x.(SearchResultIf)
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

type SearchResultMaxHeap []SearchResultIf

func (h SearchResultMaxHeap) Len() int { return len(h) }

func (h SearchResultMaxHeap) Less(i, j int) bool {
	return h[i].GetDistance() > h[j].GetDistance()
}

func (h SearchResultMaxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SearchResultMaxHeap) Push(x any) {
	item := x.(SearchResultIf)
	*h = append(*h, item)
}

func (h *SearchResultMaxHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

// Thread-safe Heap struct
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

func (h *SearchResultSafeHeap) Push(srif SearchResultIf) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	heap.Push(&h.resheap, srif)
}

func (h *SearchResultSafeHeap) Pop() SearchResultIf {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	x := heap.Pop(&h.resheap).(SearchResultIf)
	return x
}

// FastMaxHeap is a highly optimized, generic bounded max-heap designed specifically for 
// vector search Top-K operations. 
//
// Benefits over standard container/heap:
// 1. Zero Interface Boxing: By using generics and specific array layouts, it completely avoids 
//    the heap-escape "boxing" allocations caused by passing interface{} around.
// 2. Struct of Arrays (SoA): Uses independent slices for keys and distances rather than an 
//    Array of Structs (AoS). This dramatically improves CPU cache locality during distance 
//    comparisons.
// 3. Inline Array Reuse: Requires passing pre-allocated backing buffers to ensure zero 
//    allocations inside tight loops.
// 4. Bounded Logic: Natively handles "Limit/K" bounded sizing directly during the push step, 
//    reducing structural overhead.
type FastMaxHeap[T types.RealNumbers] struct {
	keys      []int64
	distances []T
	size      int
	limit     int
}

// NewFastMaxHeap initializes the FastMaxHeap using caller-provided buffer slices 
// to guarantee zero-allocation operations during tight query loops.
func NewFastMaxHeap[T types.RealNumbers](limit int, keysBuf []int64, distsBuf []T) *FastMaxHeap[T] {
	return &FastMaxHeap[T]{
		keys:      keysBuf,
		distances: distsBuf,
		size:      0,
		limit:     limit,
	}
}

func (h *FastMaxHeap[T]) siftUp(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || h.distances[j] <= h.distances[i] {
			break
		}
		h.distances[i], h.distances[j] = h.distances[j], h.distances[i]
		h.keys[i], h.keys[j] = h.keys[j], h.keys[i]
		j = i
	}
}

func (h *FastMaxHeap[T]) siftDown(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.distances[j2] > h.distances[j1] {
			j = j2 // right child
		}
		if h.distances[j] <= h.distances[i] {
			break
		}
		h.distances[i], h.distances[j] = h.distances[j], h.distances[i]
		h.keys[i], h.keys[j] = h.keys[j], h.keys[i]
		i = j
	}
}

// Push inserts a new element into the max-heap. If the heap is at its limit,
// it replaces the maximum (root) element if the new distance is smaller.
func (h *FastMaxHeap[T]) Push(key int64, dist T) {
	if h.size < h.limit {
		h.distances[h.size] = dist
		h.keys[h.size] = key
		h.siftUp(h.size)
		h.size++
	} else if dist < h.distances[0] {
		h.distances[0] = dist
		h.keys[0] = key
		h.siftDown(0, h.limit)
	}
}

// Pop extracts the element with the largest distance from the max-heap.
func (h *FastMaxHeap[T]) Pop() (int64, T, bool) {
	if h.size == 0 {
		return -1, 0, false
	}
	h.size--
	key := h.keys[0]
	dist := h.distances[0]

	h.keys[0] = h.keys[h.size]
	h.distances[0] = h.distances[h.size]
	h.siftDown(0, h.size)

	return key, dist, true
}

// Thread-safe wrapper for FastMaxHeap
type FastMaxHeapSafe[T types.RealNumbers] struct {
	mutex sync.Mutex
	heap  *FastMaxHeap[T]
}

// NewFastMaxHeapSafe creates a thread-safe FastMaxHeap
func NewFastMaxHeapSafe[T types.RealNumbers](limit int, keysBuf []int64, distsBuf []T) *FastMaxHeapSafe[T] {
	return &FastMaxHeapSafe[T]{
		heap: NewFastMaxHeap(limit, keysBuf, distsBuf),
	}
}

func (s *FastMaxHeapSafe[T]) Push(key int64, dist T) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.heap.Push(key, dist)
}

func (s *FastMaxHeapSafe[T]) Pop() (int64, T, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.heap.Pop()
}
