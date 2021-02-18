// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:generate go run genzfunc.go

// Package sort provides primitives for sorting slices and user-defined
// collections.
package bytes

import (
	"bytes"
	"matrixbase/pkg/container/vector"
)

// Sort sorts data.
// It makes one call to data.Len to determine n, and O(n*log(n)) calls to
// data.Less and data.Swap. The sort is not guaranteed to be stable.
func Sort(vs *vector.Bytes) {
	n := len(vs.Os)
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

func quickSort(vs *vector.Bytes, a, b, maxDepth int) {
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
			if bytes.Compare(vs.Data[vs.Os[i]:vs.Os[i]+vs.Ns[i]], vs.Data[vs.Os[i-6]:vs.Os[i-6]+vs.Ns[i-6]]) < 0 {
				vs.Os[i], vs.Os[i-6] = vs.Os[i-6], vs.Os[i]
				vs.Ns[i], vs.Ns[i-6] = vs.Ns[i-6], vs.Ns[i]
			}
		}
		insertionSort(vs, a, b)
	}
}

// Insertion sort
func insertionSort(vs *vector.Bytes, a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && bytes.Compare(vs.Data[vs.Os[j]:vs.Os[j]+vs.Ns[j]],
			vs.Data[vs.Os[j-1]:vs.Os[j-1]+vs.Ns[j-1]]) < 0; j-- {
			vs.Os[j], vs.Os[j-1] = vs.Os[j-1], vs.Os[j]
			vs.Ns[j], vs.Ns[j-1] = vs.Ns[j-1], vs.Ns[j]
		}
	}
}

// siftDown implements the heap property on data[lo, hi).
// first is an offset into the array where the root of the heap lies.
func siftDown(vs *vector.Bytes, lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && bytes.Compare(vs.Data[vs.Os[first+child]:vs.Os[first+child]+vs.Ns[first+child]],
			vs.Data[vs.Os[first+child+1]:vs.Os[first+child+1]+vs.Ns[first+child+1]]) < 0 {
			child++
		}
		if bytes.Compare(vs.Data[vs.Os[first+root]:vs.Os[first+root]+vs.Ns[first+root]],
			vs.Data[vs.Os[first+child]:vs.Os[first+child]+vs.Ns[first+child]]) >= 0 {
			return
		}
		vs.Os[first+root], vs.Os[first+child] = vs.Os[first+child], vs.Os[first+root]
		vs.Ns[first+root], vs.Ns[first+child] = vs.Ns[first+child], vs.Ns[first+root]
		root = child
	}
}

func heapSort(vs *vector.Bytes, a, b int) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		siftDown(vs, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		vs.Os[first], vs.Os[first+i] = vs.Os[first+i], vs.Os[first]
		vs.Ns[first], vs.Ns[first+i] = vs.Ns[first+i], vs.Ns[first]
		siftDown(vs, lo, i, first)
	}
}

// Quicksort, loosely following Bentley and McIlroy,
// ``Engineering a Sort Function,'' SP&E November 1993.

// medianOfThree moves the median of the three values data[m0], data[m1], data[m2] into data[m1].
func medianOfThree(vs *vector.Bytes, m1, m0, m2 int) {
	// sort 3 elements
	if bytes.Compare(vs.Data[vs.Os[m1]:vs.Os[m1]+vs.Ns[m1]], vs.Data[vs.Os[m0]:vs.Os[m0]+vs.Ns[m0]]) < 0 {
		vs.Os[m1], vs.Os[m0] = vs.Os[m0], vs.Os[m1]
		vs.Ns[m1], vs.Ns[m0] = vs.Ns[m0], vs.Ns[m1]
	}
	// data[m0] <= data[m1]
	if bytes.Compare(vs.Data[vs.Os[m2]:vs.Os[m2]+vs.Ns[m2]], vs.Data[vs.Os[m1]:vs.Os[m1]+vs.Ns[m1]]) < 0 {
		vs.Os[m2], vs.Os[m1] = vs.Os[m1], vs.Os[m2]
		vs.Ns[m2], vs.Ns[m1] = vs.Ns[m1], vs.Ns[m2]
		// data[m0] <= data[m2] && data[m1] < data[m2]
		if bytes.Compare(vs.Data[vs.Os[m1]:vs.Os[m1]+vs.Ns[m1]], vs.Data[vs.Os[m0]:vs.Os[m0]+vs.Ns[m0]]) < 0 {
			vs.Os[m1], vs.Os[m0] = vs.Os[m0], vs.Os[m1]
			vs.Ns[m1], vs.Ns[m0] = vs.Ns[m0], vs.Ns[m1]
		}
	}
	// now data[m0] <= data[m1] <= data[m2]
}

func swapRange(vs *vector.Bytes, a, b, n int) {
	for i := 0; i < n; i++ {
		vs.Os[a+i], vs.Os[b+i] = vs.Os[b+i], vs.Os[a+i]
		vs.Ns[a+i], vs.Ns[b+i] = vs.Ns[b+i], vs.Ns[a+i]
	}
}

func doPivot(vs *vector.Bytes, lo, hi int) (midlo, midhi int) {
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

	for ; a < c && bytes.Compare(vs.Data[vs.Os[a]:vs.Os[a]+vs.Ns[a]], vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]]) < 0; a++ {
	}
	b := a
	for {
		for ; b < c && bytes.Compare(vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]],
			vs.Data[vs.Os[b]:vs.Os[b]+vs.Ns[b]]) >= 0; b++ { // data[b] <= pivot
		}
		for ; b < c && bytes.Compare(vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]],
			vs.Data[vs.Os[c-1]:vs.Os[c-1]+vs.Ns[c-1]]) < 0; c-- { // data[c-1] > pivot
		}
		if b >= c {
			break
		}
		// data[b] > pivot; data[c-1] <= pivot
		vs.Os[b], vs.Os[c-1] = vs.Os[c-1], vs.Os[b]
		vs.Ns[b], vs.Ns[c-1] = vs.Ns[c-1], vs.Ns[b]
		b++
		c--
	}
	// If hi-c<3 then there are duplicates (by property of median of nine).
	// Let's be a bit more conservative, and set border to 5.
	protect := hi-c < 5
	if !protect && hi-c < (hi-lo)/4 {
		// Lets test some points for equality to pivot
		dups := 0
		if bytes.Compare(vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]],
			vs.Data[vs.Os[hi-1]:vs.Os[hi-1]+vs.Ns[hi-1]]) >= 0 { // data[hi-1] = pivot
			vs.Os[c], vs.Os[hi-1] = vs.Os[hi-1], vs.Os[c]
			vs.Ns[c], vs.Ns[hi-1] = vs.Ns[hi-1], vs.Ns[c]
			c++
			dups++
		}
		if bytes.Compare(vs.Data[vs.Os[b-1]:vs.Os[b-1]+vs.Ns[b-1]],
			vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]]) >= 0 { // data[b-1] = pivot
			b--
			dups++
		}
		// m-lo = (hi-lo)/2 > 6
		// b-lo > (hi-lo)*3/4-1 > 8
		// ==> m < b ==> data[m] <= pivot
		if bytes.Compare(vs.Data[vs.Os[m]:vs.Os[m]+vs.Ns[m]],
			vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]]) >= 0 { // data[m] = pivot
			vs.Os[m], vs.Os[b-1] = vs.Os[b-1], vs.Os[m]
			vs.Ns[m], vs.Ns[b-1] = vs.Ns[b-1], vs.Ns[m]
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
			for ; a < b && bytes.Compare(vs.Data[vs.Os[b-1]:vs.Os[b-1]+vs.Ns[b-1]],
				vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]]) >= 0; b-- { // data[b] == pivot
			}
			for ; a < b && bytes.Compare(vs.Data[vs.Os[a]:vs.Os[a]+vs.Ns[a]],
				vs.Data[vs.Os[pivot]:vs.Os[pivot]+vs.Ns[pivot]]) < 0; a++ { // data[a] < pivot
			}
			if a >= b {
				break
			}
			// data[a] == pivot; data[b-1] < pivot
			vs.Os[a], vs.Os[b-1] = vs.Os[b-1], vs.Os[a]
			vs.Ns[a], vs.Ns[b-1] = vs.Ns[b-1], vs.Ns[a]
			a++
			b--
		}
	}
	// Swap pivot into middle
	vs.Os[pivot], vs.Os[b-1] = vs.Os[b-1], vs.Os[pivot]
	vs.Ns[pivot], vs.Ns[b-1] = vs.Ns[b-1], vs.Ns[pivot]
	return b - 1, c
}
