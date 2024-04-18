// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:generate go run genzfunc.go

// Package sort provides primitives for sorting slices and user-defined collections.
package mergesort

// insertionSort sorts data[a:b] using insertion sort.
func insertionSort[T any](data SortSlice[T], a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && data.Less(j, j-1); j-- {
			data.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo:hi].
// first is an offset into the array where the root of the heap lies.
func siftDown[T any](data SortSlice[T], lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && data.Less(first+child, first+child+1) {
			child++
		}
		if !data.Less(first+root, first+child) {
			return
		}
		data.Swap(first+root, first+child)
		root = child
	}
}

func heapSort[T any](data SortSlice[T], a, b int) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		siftDown(data, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data.Swap(first, first+i)
		siftDown(data, lo, i, first)
	}
}

// Quicksort, loosely following Bentley and McIlroy,
// ``Engineering a Sort Function,'' SP&E November 1993.

// medianOfThree moves the median of the three values data[m0], data[m1], data[m2] into data[m1].
func medianOfThree[T any](data SortSlice[T], m1, m0, m2 int) {
	// sort 3 elements
	if data.Less(m1, m0) {
		data.Swap(m1, m0)
	}
	// data[m0] <= data[m1]
	if data.Less(m2, m1) {
		data.Swap(m2, m1)
		// data[m0] <= data[m2] && data[m1] < data[m2]
		if data.Less(m1, m0) {
			data.Swap(m1, m0)
		}
	}
	// now data[m0] <= data[m1] <= data[m2]
}

// func swapRange(data sortSlice, a, b, n int) {
// 	for i := 0; i < n; i++ {
// 		data.Swap(a+i, b+i)
// 	}
// }

func doPivot[T any](data SortSlice[T], lo, hi int) (midlo, midhi int) {
	m := int(uint(lo+hi) >> 1) // Written like this to avoid integer overflow.
	if hi-lo > 40 {
		// Tukey's ``Ninther,'' median of three medians of three.
		s := (hi - lo) / 8
		medianOfThree(data, lo, lo+s, lo+2*s)
		medianOfThree(data, m, m-s, m+s)
		medianOfThree(data, hi-1, hi-1-s, hi-1-2*s)
	}
	medianOfThree(data, lo, m, hi-1)

	// Invariants are:
	//	data[lo] = pivot (set up by ChoosePivot)
	//	data[lo < i < a] < pivot
	//	data[a <= i < b] <= pivot
	//	data[b <= i < c] unexamined
	//	data[c <= i < hi-1] > pivot
	//	data[hi-1] >= pivot
	pivot := lo
	a, c := lo+1, hi-1

	for ; a < c && data.Less(a, pivot); a++ {
	}
	b := a
	for {
		for ; b < c && !data.Less(pivot, b); b++ { // data[b] <= pivot
		}
		for ; b < c && data.Less(pivot, c-1); c-- { // data[c-1] > pivot
		}
		if b >= c {
			break
		}
		// data[b] > pivot; data[c-1] <= pivot
		data.Swap(b, c-1)
		b++
		c--
	}
	// If hi-c<3 then there are duplicates (by property of median of nine).
	// Let's be a bit more conservative, and set border to 5.
	protect := hi-c < 5
	if !protect && hi-c < (hi-lo)/4 {
		// Lets test some points for equality to pivot
		dups := 0
		if !data.Less(pivot, hi-1) { // data[hi-1] = pivot
			data.Swap(c, hi-1)
			c++
			dups++
		}
		if !data.Less(b-1, pivot) { // data[b-1] = pivot
			b--
			dups++
		}
		// m-lo = (hi-lo)/2 > 6
		// b-lo > (hi-lo)*3/4-1 > 8
		// ==> m < b ==> data[m] <= pivot
		if !data.Less(m, pivot) { // data[m] = pivot
			data.Swap(m, b-1)
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
			for ; a < b && !data.Less(b-1, pivot); b-- { // data[b] == pivot
			}
			for ; a < b && data.Less(a, pivot); a++ { // data[a] < pivot
			}
			if a >= b {
				break
			}
			// data[a] == pivot; data[b-1] < pivot
			data.Swap(a, b-1)
			a++
			b--
		}
	}
	// Swap pivot into middle
	data.Swap(pivot, b-1)
	return b - 1, c
}

func quickSort[T any](data SortSlice[T], a, b, maxDepth int) {
	for b-a > 12 { // Use ShellSort for slices <= 12 elements
		if maxDepth == 0 {
			heapSort(data, a, b)
			return
		}
		maxDepth--
		mlo, mhi := doPivot(data, a, b)
		// Avoiding recursion on the larger subproblem guarantees
		// a stack depth of at most lg(b-a).
		if mlo-a < b-mhi {
			quickSort(data, a, mlo, maxDepth)
			a = mhi // i.e., quickSort(data, mhi, b)
		} else {
			quickSort(data, mhi, b, maxDepth)
			b = mlo // i.e., quickSort(data, a, mlo)
		}
	}
	if b-a > 1 {
		// Do ShellSort pass with gap 6
		// It could be written in this simplified form cause b-a <= 12
		for i := a + 6; i < b; i++ {
			if data.Less(i, i-6) {
				data.Swap(i, i-6)
			}
		}
		insertionSort(data, a, b)
	}
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

// symMerge merges the two sorted subsequences data[a:m] and data[m:b] using
// the SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum
// Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz
// Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in
// Computer Science, pages 714-723. Springer, 2004.
//
// Let M = m-a and NodeInfo = b-n. Wolog M < NodeInfo.
// The recursion depth is bound by ceil(log(NodeInfo+M)).
// The algorithm needs Operator(M*log(NodeInfo/M + 1)) calls to data.Less.
// The algorithm needs Operator((M+NodeInfo)*log(M)) calls to data.Swap.
//
// The paper gives Operator((M+NodeInfo)*log(M)) as the number of assignments assuming a
// rotation algorithm which uses Operator(M+NodeInfo+gcd(M+NodeInfo)) assignments. The argumentation
// in the paper carries through for Swap operations, especially as the block
// swapping rotate uses only Operator(M+NodeInfo) Swaps.
//
// symMerge assumes non-degenerate arguments: a < m && m < b.
// Having the caller check this condition eliminates many leaf recursion calls,
// which improves performance.
// func symMerge(data sortSlice, a, m, b int) {
// 	// Avoid unnecessary recursions of symMerge
// 	// by direct insertion of data[a] into data[m:b]
// 	// if data[a:m] only contains one element.
// 	if m-a == 1 {
// 		// Use binary search to find the lowest index i
// 		// such that data[i] >= data[a] for m <= i < b.
// 		// Exit the search loop with i == b in case no such index exists.
// 		i := m
// 		j := b
// 		for i < j {
// 			h := int(uint(i+j) >> 1)
// 			if data.Less(h, a) {
// 				i = h + 1
// 			} else {
// 				j = h
// 			}
// 		}
// 		// Swap values until data[a] reaches the position before i.
// 		for k := a; k < i-1; k++ {
// 			data.Swap(k, k+1)
// 		}
// 		return
// 	}

// 	// Avoid unnecessary recursions of symMerge
// 	// by direct insertion of data[m] into data[a:m]
// 	// if data[m:b] only contains one element.
// 	if b-m == 1 {
// 		// Use binary search to find the lowest index i
// 		// such that data[i] > data[m] for a <= i < m.
// 		// Exit the search loop with i == m in case no such index exists.
// 		i := a
// 		j := m
// 		for i < j {
// 			h := int(uint(i+j) >> 1)
// 			if !data.Less(m, h) {
// 				i = h + 1
// 			} else {
// 				j = h
// 			}
// 		}
// 		// Swap values until data[m] reaches the position i.
// 		for k := m; k > i; k-- {
// 			data.Swap(k, k-1)
// 		}
// 		return
// 	}

// 	mid := int(uint(a+b) >> 1)
// 	n := mid + m
// 	var start, r int
// 	if m > mid {
// 		start = n - b
// 		r = mid
// 	} else {
// 		start = a
// 		r = m
// 	}
// 	p := n - 1

// 	for start < r {
// 		c := int(uint(start+r) >> 1)
// 		if !data.Less(p-c, c) {
// 			start = c + 1
// 		} else {
// 			r = c
// 		}
// 	}

// 	end := n - start
// 	if start < m && m < end {
// 		rotate(data, start, m, end)
// 	}
// 	if a < start && start < mid {
// 		symMerge(data, a, start, mid)
// 	}
// 	if mid < end && end < b {
// 		symMerge(data, mid, end, b)
// 	}
// }

// rotate rotates two consecutive blocks u = data[a:m] and v = data[m:b] in data:
// DataSource of the form 'x u v y' is changed to 'x v u y'.
// rotate performs at most b-a many calls to data.Swap,
// and it assumes non-degenerate arguments: a < m && m < b.
// func rotate(data sortSlice, a, m, b int) {
// 	i := m - a
// 	j := b - m

// 	for i != j {
// 		if i > j {
// 			swapRange(data, m-i, m, j)
// 			i -= j
// 		} else {
// 			swapRange(data, m-i, m+j-i, i)
// 			j -= i
// 		}
// 	}
// 	// i == j
// 	swapRange(data, m-i, m, i)
// }

/*
Complexity of Stable Sorting


Complexity of block swapping rotation

Each Swap puts one new element into its correct, final position.
Elements which reach their final position are no longer moved.
Thus block swapping rotation needs |u|+|v| calls to Swaps.
This is best possible as each element might need a move.

Pay attention when comparing to other optimal algorithms which
typically count the number of assignments instead of swaps:
E.g. the optimal algorithm of Dudzinski and Dydek for in-place
rotations uses Operator(u + v + gcd(u,v)) assignments which is
better than our Operator(3 * (u+v)) as gcd(u,v) <= u.


Stable sorting by SymMerge and BlockSwap rotations

SymMerg complexity for same size input M = NodeInfo:
Calls to Less:  Operator(M*log(NodeInfo/M+1)) = Operator(NodeInfo*log(2)) = Operator(NodeInfo)
Calls to Swap:  Operator((M+NodeInfo)*log(M)) = Operator(2*NodeInfo*log(NodeInfo)) = Operator(NodeInfo*log(NodeInfo))

(The following argument does not fuzz over a missing -1 or
other stuff which does not impact the final result).

Let n = data.Len(). Assume n = 2^k.

Plain merge sort performs log(n) = k iterations.
On iteration i the algorithm merges 2^(k-i) blocks, each of size 2^i.

Thus iteration i of merge sort performs:
Calls to Less  Operator(2^(k-i) * 2^i) = Operator(2^k) = Operator(2^log(n)) = Operator(n)
Calls to Swap  Operator(2^(k-i) * 2^i * log(2^i)) = Operator(2^k * i) = Operator(n*i)

In total k = log(n) iterations are performed; so in total:
Calls to Less Operator(log(n) * n)
Calls to Swap Operator(n + 2*n + 3*n + ... + (k-1)*n + k*n)
   = Operator((k/2) * k * n) = Operator(n * k^2) = Operator(n * log^2(n))


Above results should generalize to arbitrary n = 2^k + p
and should not be influenced by the initial insertion sort phase:
Insertion sort is Operator(n^2) on Swap and Less, thus Operator(bs^2) per block of
size bs at n/bs blocks:  Operator(bs*n) Swaps and Less during insertion sort.
Merge sort iterations start at i = log(bs). With t = log(bs) constant:
Calls to Less Operator((log(n)-t) * n + bs*n) = Operator(log(n)*n + (bs-t)*n)
   = Operator(n * log(n))
Calls to Swap Operator(n * log^2(n) - (t^2+t)/2*n) = Operator(n * log^2(n))

*/

// Sort sorts data.
// It makes one call to data.Len to determine n and Operator(n*log(n)) calls to
// data.Less and data.Swap. The sort is not guaranteed to be stable.
func sortUnstable[T any](data SortSlice[T]) {
	n := len(data.AsSlice())
	quickSort(data, 0, n, maxDepth(n))
}
