// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package heap provides heap operations for any type that implements
// heap.Interface. A heap is a tree with the property that each node is the
// minimum-valued node in its subtree.
//
// The minimum element in the tree is the root, at index 0.
//
// A heap is a common way to implement a priority queue. To build a priority
// queue, implement the Heap interface with the (negative) priority as the
// ordering for the Less method, so Push adds items while Pop removes the
// highest-priority item from the queue. The Examples include such an
// implementation; the file example_pq_test.go has the complete source.
//
package varchar

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is Operator(n) where n = len(h).
func heapInit(h heapSlice) {
	// heapify
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// Push pushes the element x onto the heap.
// The complexity is Operator(log n) where n = len(h).
func heapPush(h *heapSlice, x heapElem) {
	*h = append(*h, x)
	up(*h, len(*h)-1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is Operator(log n) where n = len(h).
// Pop is equivalent to Remove(h, 0).
func heapPop(h *heapSlice) heapElem {
	n := len(*h) - 1
	(*h)[0], (*h)[n] = (*h)[n], (*h)[0]
	down(*h, 0, n)
	res := (*h)[n]
	*h = (*h)[:n]
	return res
}

// Remove removes and returns the element at index i from the heap.
// The complexity is Operator(log n) where n = len(h).
func heapRemove(h *heapSlice, i int) heapElem {
	n := len(*h) - 1
	if n != i {
		h.Swap(i, n)
		if !down(*h, i, n) {
			up(*h, i)
		}
	}
	res := (*h)[n]
	*h = (*h)[:n]
	return res
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Fix is equivalent to,
// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
// The complexity is Operator(log n) where n = len(h).
func heapFix(h heapSlice, i int) {
	if !down(h, i, len(h)) {
		up(h, i)
	}
}

func up(h heapSlice, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h heapSlice, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
