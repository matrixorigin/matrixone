// Copyright 2021 Matrix Origin
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

package mergesort

import "bytes"

type numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type sortSlice interface {
	Len() int
	Less(int, int) bool
	Swap(int, int)
}

type heapSlice[T any] interface {
	sortSlice
	Push(T)
	Pop() T
}

type sortElem[T any] struct {
	data T
	idx  uint32
}

type numericSortSlice[T numeric] []sortElem[T]

func (s numericSortSlice[T]) Len() int           { return len(s) }
func (s numericSortSlice[T]) Less(i, j int) bool { return s[i].data < s[j].data }
func (s numericSortSlice[T]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type stringSortSlice []sortElem[[]byte]

func (s stringSortSlice) Len() int           { return len(s) }
func (s stringSortSlice) Less(i, j int) bool { return bytes.Compare(s[i].data, s[j].data) < 0 }
func (s stringSortSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type heapElem[T any] struct {
	data T
	src  uint16
	next uint32
}

type numericHeapSlice[T numeric] []heapElem[T]

func (h numericHeapSlice[T]) Len() int           { return len(h) }
func (h numericHeapSlice[T]) Less(i, j int) bool { return h[i].data < h[j].data }
func (h numericHeapSlice[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *numericHeapSlice[T]) Push(x heapElem[T]) {
	*h = append(*h, x)
}

func (h *numericHeapSlice[T]) Pop() heapElem[T] {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type stringHeapSlice []heapElem[[]byte]

func (h stringHeapSlice) Len() int           { return len(h) }
func (h stringHeapSlice) Less(i, j int) bool { return bytes.Compare(h[i].data, h[j].data) < 0 }
func (h stringHeapSlice) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *stringHeapSlice) Push(x heapElem[[]byte]) {
	*h = append(*h, x)
}

func (h *stringHeapSlice) Pop() heapElem[[]byte] {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
