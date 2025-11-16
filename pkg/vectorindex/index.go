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
	"container/heap"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	// defensive: avoid panic if nil or typed-nil slips in
	li := h[i]
	lj := h[j]
	if isNilSearchResultIf(li) && isNilSearchResultIf(lj) {
		return false
	}
	if isNilSearchResultIf(li) {
		// nil considered greater so it sinks
		return false
	}
	if isNilSearchResultIf(lj) {
		return true
	}
	return li.GetDistance() < lj.GetDistance()
}

func (h SearchResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SearchResultHeap) Push(x any) {
	// defensive: skip nil pushes
	if x == nil {
		logutil.Warnf("SearchResultHeap.Push received nil")
		return
	}
	item, ok := x.(SearchResultIf)
	if !ok || item == nil {
		if !ok {
			logutil.Warnf("SearchResultHeap.Push unexpected type: %T", x)
		} else {
			logutil.Warnf("SearchResultHeap.Push underlying item is nil")
		}
		return
	}
	// skip typed-nil values (interface with nil concrete pointer)
	if isNilSearchResultIf(item) {
		logutil.Warnf("SearchResultHeap.Push typed-nil: %T", item)
		return
	}
	*h = append(*h, item)
}

// isNilSearchResultIf detects typed-nil concrete pointers stored in the interface
func isNilSearchResultIf(x SearchResultIf) bool {
	switch v := x.(type) {
	case *SearchResultAnyKey:
		return v == nil
	case *SearchResult:
		return v == nil
	default:
		return x == nil
	}
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
