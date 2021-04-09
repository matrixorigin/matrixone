// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:generate go run genzfunc.go

// Package sort provides primitives for sorting slices and user-defined
// collections.
package bytes

import (
	"bytes"
	"matrixone/pkg/container/types"
)

// Sort sorts data.
// It makes one call to data.Len to determine n, and O(n*log(n)) calls to
// data.Less and data.Swap. The sort is not guaranteed to be stable.
func Sort(vs *types.Bytes) {
	n := len(vs.Offsets)
	quickSort(vs, 0, n, maxDepth(n))
}

// maxDepth returns a threshold at which quicksort should switch
// to heapsort. It returns 2*ceil(lg(n+1)).
func maxDepth(n int) int {
	var depth int
	for i := n; i > 0; i >>= 1 {
		depth++
	}
	return depth * 2
}

func quickSort(vs *types.Bytes, a, b, maxDepth int) {
	for b-a > 12 { // Use ShellSort for slices <= 12 elements
		if maxDepth == 0 {
			heapSort(vs, a, b)
			return
		}
		maxDepth--
		mlo, mhi := doPivot(vs, a, b)
		// Avoiding recursion on the larger subproblem guarantees
		// a stack depth of at most lg(b-a).
		if mlo-a < b-mhi {
			quickSort(vs, a, mlo, maxDepth)
			a = mhi // i.e., quickSort(data, mhi, b)
		} else {
			quickSort(vs, mhi, b, maxDepth)
			b = mlo // i.e., quickSort(data, a, mlo)
		}
	}
	if b-a > 1 {
		// Do ShellSort pass with gap 6
		// It could be written in this simplified form cause b-a <= 12
		for i := a + 6; i < b; i++ {
			if bytes.Compare(vs.Get(i), vs.Get(i-6)) >= 0 {
				vs.Swap(i, i-6)
			}
		}
		insertionSort(vs, a, b)
	}
}

// Insertion sort
func insertionSort(vs *types.Bytes, a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && bytes.Compare(vs.Get(j), vs.Get(j-1)) >= 0; j-- {
			vs.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo, hi).
// first is an offset into the array where the root of the heap lies.
func siftDown(vs *types.Bytes, lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && bytes.Compare(vs.Get(first+child), vs.Get(first+child+1)) >= 0 {
			child++
		}
		if bytes.Compare(vs.Get(first+root), vs.Get(first+child)) < 0 {
			return
		}
		vs.Swap(first+root, first+child)
		root = child
	}
}

func heapSort(vs *types.Bytes, a, b int) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		siftDown(vs, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		vs.Swap(first, first+i)
		siftDown(vs, lo, i, first)
	}
}

// Quicksort, loosely following Bentley and McIlroy,
// ``Engineering a Sort Function,'' SP&E November 1993.

// medianOfThree moves the median of the three values data[m0], data[m1], data[m2] into data[m1].
func medianOfThree(vs *types.Bytes, m1, m0, m2 int) {
	// sort 3 elements
	if bytes.Compare(vs.Get(m1), vs.Get(m0)) >= 0 {
		vs.Swap(m1, m0)
	}
	// data[m0] <= data[m1]
	if bytes.Compare(vs.Get(m2), vs.Get(m1)) >= 0 {
		vs.Swap(m2, m1)
		// data[m0] <= data[m2] && data[m1] < data[m2]
		if bytes.Compare(vs.Get(m1), vs.Get(m0)) >= 0 {
			vs.Swap(m1, m0)
		}
	}
	// now data[m0] <= data[m1] <= data[m2]
}

func swapRange(vs *types.Bytes, a, b, n int) {
	for i := 0; i < n; i++ {
		vs.Swap(a+i, b+i)
	}
}

func doPivot(vs *types.Bytes, lo, hi int) (midlo, midhi int) {
	m := int(uint(lo+hi) >> 1) // Written like this to avoid integer overflow.
	if hi-lo > 40 {
		// Tukey's ``Ninther,'' median of three medians of three.
		s := (hi - lo) / 8
		medianOfThree(vs, lo, lo+s, lo+2*s)
		medianOfThree(vs, m, m-s, m+s)
		medianOfThree(vs, hi-1, hi-1-s, hi-1-2*s)
	}
	medianOfThree(vs, lo, m, hi-1)

	// Invariants are:
	//	data[lo] = pivot (set up by ChoosePivot)
	//	data[lo < i < a] < pivot
	//	data[a <= i < b] <= pivot
	//	data[b <= i < c] unexamined
	//	data[c <= i < hi-1] > pivot
	//	data[hi-1] >= pivot
	pivot := lo
	a, c := lo+1, hi-1

	for ; a < c && bytes.Compare(vs.Get(a), vs.Get(pivot)) >= 0; a++ {
	}
	b := a
	for {
		for ; b < c && bytes.Compare(vs.Get(pivot), vs.Get(b)) < 0; b++ { // data[b] <= pivot
		}
		for ; b < c && bytes.Compare(vs.Get(pivot), vs.Get(c-1)) >= 0; c-- { // data[c-1] > pivot
		}
		if b >= c {
			break
		}
		// data[b] > pivot; data[c-1] <= pivot
		vs.Swap(b, c-1)
		b++
		c--
	}
	// If hi-c<3 then there are duplicates (by property of median of nine).
	// Let's be a bit more conservative, and set border to 5.
	protect := hi-c < 5
	if !protect && hi-c < (hi-lo)/4 {
		// Lets test some points for equality to pivot
		dups := 0
		if bytes.Compare(vs.Get(pivot), vs.Get(hi-1)) < 0 { // data[hi-1] = pivot
			vs.Swap(c, hi-1)
			c++
			dups++
		}
		if bytes.Compare(vs.Get(b-1), vs.Get(pivot)) < 0 { // data[b-1] = pivot
			b--
			dups++
		}
		// m-lo = (hi-lo)/2 > 6
		// b-lo > (hi-lo)*3/4-1 > 8
		// ==> m < b ==> data[m] <= pivot
		if bytes.Compare(vs.Get(m), vs.Get(pivot)) < 0 { // data[m] = pivot
			vs.Swap(m, b-1)
			b--
			dups++
		}
		// if at least 2 points are equal to pivot, assume skewed distribution
		protect = dups > 1
	}
	if protect {
		// Protect against a lot of duplicates
		// Add invariant:
		//	data[a <= i < b] unexamined
		//	data[b <= i < c] = pivot
		for {
			for ; a < b && bytes.Compare(vs.Get(b-1), vs.Get(pivot)) < 0; b-- { // data[b] == pivot
			}
			for ; a < b && bytes.Compare(vs.Get(a), vs.Get(pivot)) >= 0; a++ { // data[a] < pivot
			}
			if a >= b {
				break
			}
			// data[a] == pivot; data[b-1] < pivot
			vs.Swap(a, b-1)
			a++
			b--
		}
	}
	// Swap pivot into middle
	vs.Swap(pivot, b-1)
	return b - 1, c
}
