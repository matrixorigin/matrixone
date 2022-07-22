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

package sort

import (
	"bytes"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	unknownHint sortedHint = iota
	increasingHint
	decreasingHint
)

type xorshift uint64
type sortedHint int // hint for pdqsort when choosing the pivot

func Sort(desc bool, os []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_bool:
		col := vector.GenericVectorValues[bool](vec)
		if !desc {
			genericSort(col, os, boolLess[bool])
		} else {
			genericSort(col, os, boolGreater[bool])
		}
	case types.T_int8:
		col := vector.GenericVectorValues[int8](vec)
		if !desc {
			genericSort(col, os, genericLess[int8])
		} else {
			genericSort(col, os, genericGreater[int8])
		}
	case types.T_int16:
		col := vector.GenericVectorValues[int16](vec)
		if !desc {
			genericSort(col, os, genericLess[int16])
		} else {
			genericSort(col, os, genericGreater[int16])
		}
	case types.T_int32:
		col := vector.GenericVectorValues[int32](vec)
		if !desc {
			genericSort(col, os, genericLess[int32])
		} else {
			genericSort(col, os, genericGreater[int32])
		}
	case types.T_int64:
		col := vector.GenericVectorValues[int64](vec)
		if !desc {
			genericSort(col, os, genericLess[int64])
		} else {
			genericSort(col, os, genericGreater[int64])
		}
	case types.T_uint8:
		col := vector.GenericVectorValues[uint8](vec)
		if !desc {
			genericSort(col, os, genericLess[uint8])
		} else {
			genericSort(col, os, genericGreater[uint8])
		}
	case types.T_uint16:
		col := vector.GenericVectorValues[uint16](vec)
		if !desc {
			genericSort(col, os, genericLess[uint16])
		} else {
			genericSort(col, os, genericGreater[uint16])
		}
	case types.T_uint32:
		col := vector.GenericVectorValues[uint32](vec)
		if !desc {
			genericSort(col, os, genericLess[uint32])
		} else {
			genericSort(col, os, genericGreater[uint32])
		}
	case types.T_uint64:
		col := vector.GenericVectorValues[uint64](vec)
		if !desc {
			genericSort(col, os, genericLess[uint64])
		} else {
			genericSort(col, os, genericGreater[uint64])
		}
	case types.T_float32:
		col := vector.GenericVectorValues[float32](vec)
		if !desc {
			genericSort(col, os, genericLess[float32])
		} else {
			genericSort(col, os, genericGreater[float32])
		}
	case types.T_float64:
		col := vector.GenericVectorValues[float64](vec)
		if !desc {
			genericSort(col, os, genericLess[float64])
		} else {
			genericSort(col, os, genericGreater[float64])
		}
	case types.T_date:
		col := vector.GenericVectorValues[types.Date](vec)
		if !desc {
			genericSort(col, os, genericLess[types.Date])
		} else {
			genericSort(col, os, genericGreater[types.Date])
		}
	case types.T_datetime:
		col := vector.GenericVectorValues[types.Datetime](vec)
		if !desc {
			genericSort(col, os, genericLess[types.Datetime])
		} else {
			genericSort(col, os, genericGreater[types.Datetime])
		}
	case types.T_timestamp:
		col := vector.GenericVectorValues[types.Timestamp](vec)
		if !desc {
			genericSort(col, os, genericLess[types.Timestamp])
		} else {
			genericSort(col, os, genericGreater[types.Timestamp])
		}
	case types.T_decimal64:
		col := vector.GenericVectorValues[types.Decimal64](vec)
		if !desc {
			genericSort(col, os, decimal64Less)
		} else {
			genericSort(col, os, decimal64Greater)
		}
	case types.T_decimal128:
		col := vector.GenericVectorValues[types.Decimal128](vec)
		if !desc {
			genericSort(col, os, decimal128Less)
		} else {
			genericSort(col, os, decimal128Greater)
		}
	case types.T_char, types.T_varchar:
		col := vec.Col.(*types.Bytes)
		if !desc {
			genericSort([]types.String{col}, os, stringLess[types.String])
		} else {
			genericSort([]types.String{col}, os, stringGreater[types.String])
		}
	}
}

func boolLess[T bool](data []T, i, j int64) bool {
	return bool(!data[i] && data[j])
}

func boolGreater[T bool](data []T, i, j int64) bool {
	return bool(data[i] && !data[j])
}

func stringLess[T types.String](data []T, i, j int64) bool {
	return bytes.Compare(data[0].Get(i), data[0].Get(j)) < 0
}

func stringGreater[T types.String](data []T, i, j int64) bool {
	return bytes.Compare(data[0].Get(i), data[0].Get(j)) > 0
}

func decimal64Less(data []types.Decimal64, i, j int64) bool {
	return data[i].Compare(data[j]) < 0
}

func decimal64Greater(data []types.Decimal64, i, j int64) bool {
	return data[i].Compare(data[j]) > 0
}

func decimal128Less(data []types.Decimal128, i, j int64) bool {
	return data[i].Compare(data[j]) < 0
}

func decimal128Greater(data []types.Decimal128, i, j int64) bool {
	return data[i].Compare(data[j]) > 0
}

func genericLess[T types.Generic](data []T, i, j int64) bool {
	return data[i] < data[j]
}

func genericGreater[T types.Generic](data []T, i, j int64) bool {
	return data[i] > data[j]
}

func (r *xorshift) Next() uint64 {
	*r ^= *r << 13
	*r ^= *r >> 17
	*r ^= *r << 5
	return uint64(*r)
}

func nextPowerOfTwo(length int) uint {
	shift := uint(bits.Len(uint(length)))
	return uint(1 << shift)
}

// Sort sorts data in ascending order as determined by the Less method.
// It makes one call to data.Len to determine n and O(n*log(n)) calls to
// data.Less and data.Swap. The sort is not guaranteed to be stable.
func genericSort[T any](data []T, os []int64, fn func([]T, int64, int64) bool) {
	n := len(os)
	if n <= 1 {
		return
	}
	limit := bits.Len(uint(n))
	pdqsort(data, 0, n, limit, os, fn)
}

// pdqsort sorts data[a:b].
// The algorithm based on pattern-defeating quicksort(pdqsort), but without the optimizations from BlockQuicksort.
// pdqsort paper: https://arxiv.org/pdf/2106.05123.pdf
// C++ implementation: https://github.com/orlp/pdqsort
// Rust implementation: https://docs.rs/pdqsort/latest/pdqsort/
// limit is the number of allowed bad (very unbalanced) pivots before falling back to heapsort.
func pdqsort[T any](data []T, a, b, limit int, os []int64, fn func([]T, int64, int64) bool) {
	const maxInsertion = 12

	var (
		wasBalanced    = true // whether the last partitioning was reasonably balanced
		wasPartitioned = true // whether the slice was already partitioned
	)

	for {
		length := b - a

		if length <= maxInsertion {
			insertionSort(data, a, b, os, fn)
			return
		}

		// Fall back to heapsort if too many bad choices were made.
		if limit == 0 {
			heapSort(data, a, b, os, fn)
			return
		}

		// If the last partitioning was imbalanced, we need to breaking patterns.
		if !wasBalanced {
			breakPatterns(data, a, b, os)
			limit--
		}

		pivot, hint := choosePivot(data, a, b, os, fn)
		if hint == decreasingHint {
			reverseRange(data, a, b, os, fn)
			// The chosen pivot was pivot-a elements after the start of the array.
			// After reversing it is pivot-a elements before the end of the array.
			// The idea came from Rust's implementation.
			pivot = (b - 1) - (pivot - a)
			hint = increasingHint
		}

		// The slice is likely already sorted.
		if wasBalanced && wasPartitioned && hint == increasingHint {
			if partialInsertionSort(data, a, b, os, fn) {
				return
			}
		}

		// Probably the slice contains many duplicate elements, partition the slice into
		// elements equal to and elements greater than the pivot.
		if a > 0 && !fn(data, os[a-1], os[pivot]) {
			mid := partitionEqual(data, a, b, pivot, os, fn)
			a = mid
			continue
		}

		mid, alreadyPartitioned := partition(data, a, b, pivot, os, fn)
		wasPartitioned = alreadyPartitioned

		leftLen, rightLen := mid-a, b-mid
		balanceThreshold := length / 8
		if leftLen < rightLen {
			wasBalanced = leftLen >= balanceThreshold
			pdqsort(data, a, mid, limit, os, fn)
			a = mid + 1
		} else {
			wasBalanced = rightLen >= balanceThreshold
			pdqsort(data, mid+1, b, limit, os, fn)
			b = mid
		}
	}
}

// insertionSort sorts data[a:b] using insertion sort.
func insertionSort[T any](data []T, a, b int, os []int64, fn func([]T, int64, int64) bool) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && fn(data, os[j], os[j-1]); j-- {
			os[j], os[j-1] = os[j-1], os[j]
		}
	}
}

// siftDown implements the heap property on data[lo:hi].
// first is an offset into the array where the root of the heap lies.
func siftDown[T any](data []T, lo, hi, first int, os []int64, fn func([]T, int64, int64) bool) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && fn(data, os[first+child], os[first+child+1]) {
			child++
		}
		if !fn(data, os[first+root], os[first+child]) {
			return
		}
		os[first+root], os[first+child] = os[first+child], os[first+root]
		root = child
	}
}

func heapSort[T any](data []T, a, b int, os []int64, fn func([]T, int64, int64) bool) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		siftDown(data, i, hi, first, os, fn)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		os[first], os[first+i] = os[first+i], os[first]
		siftDown(data, lo, i, first, os, fn)
	}
}

// partition does one quicksort partition.
// Let p = data[pivot]
// Moves elements in data[a:b] around, so that data[i]<p and data[j]>=p for i<newpivot and j>newpivot.
// On return, data[newpivot] = p
func partition[T any](data []T, a, b, pivot int, os []int64, fn func([]T, int64, int64) bool) (newpivot int, alreadyPartitioned bool) {
	os[a], os[pivot] = os[pivot], os[a]
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for i <= j && fn(data, os[i], os[a]) {
		i++
	}
	for i <= j && !fn(data, os[j], os[a]) {
		j--
	}
	if i > j {
		os[j], os[a] = os[a], os[j]
		return j, true
	}
	os[i], os[j] = os[j], os[i]
	i++
	j--

	for {
		for i <= j && fn(data, os[i], os[a]) {
			i++
		}
		for i <= j && !fn(data, os[j], os[a]) {
			j--
		}
		if i > j {
			break
		}
		os[i], os[j] = os[j], os[i]
		i++
		j--
	}
	os[j], os[a] = os[a], os[j]
	return j, false
}

// partitionEqual partitions data[a:b] into elements equal to data[pivot] followed by elements greater than data[pivot].
// It assumed that data[a:b] does not contain elements smaller than the data[pivot].
func partitionEqual[T any](data []T, a, b, pivot int, os []int64, fn func([]T, int64, int64) bool) (newpivot int) {
	os[a], os[pivot] = os[pivot], os[a]
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for {
		for i <= j && !fn(data, os[a], os[i]) {
			i++
		}
		for i <= j && fn(data, os[a], os[j]) {
			j--
		}
		if i > j {
			break
		}
		os[i], os[j] = os[j], os[i]
		i++
		j--
	}
	return i
}

// partialInsertionSort partially sorts a slice, returns true if the slice is sorted at the end.
func partialInsertionSort[T any](data []T, a, b int, os []int64, fn func([]T, int64, int64) bool) bool {
	const (
		maxSteps         = 5  // maximum number of adjacent out-of-order pairs that will get shifted
		shortestShifting = 50 // don't shift any elements on short arrays
	)
	i := a + 1
	for j := 0; j < maxSteps; j++ {
		for i < b && !fn(data, os[i], os[i-1]) {
			i++
		}

		if i == b {
			return true
		}

		if b-a < shortestShifting {
			return false
		}

		os[i], os[i-1] = os[i-1], os[i]

		// Shift the smaller one to the left.
		if i-a >= 2 {
			for j := i - 1; j >= 1; j-- {
				if !fn(data, os[j], os[j-1]) {
					break
				}
				os[j], os[j-1] = os[j-1], os[j]
			}
		}
		// Shift the greater one to the right.
		if b-i >= 2 {
			for j := i + 1; j < b; j++ {
				if !fn(data, os[j], os[j-1]) {
					break
				}
				os[j], os[j-1] = os[j-1], os[j]
			}
		}
	}
	return false
}

// breakPatterns scatters some elements around in an attempt to break some patterns
// that might cause imbalanced partitions in quicksort.
func breakPatterns[T any](data []T, a, b int, os []int64) {
	length := b - a
	if length >= 8 {
		random := xorshift(length)
		modulus := nextPowerOfTwo(length)

		for idx := a + (length/4)*2 - 1; idx <= a+(length/4)*2+1; idx++ {
			other := int(uint(random.Next()) & (modulus - 1))
			if other >= length {
				other -= length
			}
			os[idx], os[a+other] = os[a+other], os[idx]
		}
	}
}

// choosePivot chooses a pivot in data[a:b].
//
// [0,8): chooses a static pivot.
// [8,shortestNinther): uses the simple median-of-three method.
// [shortestNinther,∞): uses the Tukey ninther method.
func choosePivot[T any](data []T, a, b int, os []int64, fn func([]T, int64, int64) bool) (pivot int, hint sortedHint) {
	const (
		shortestNinther = 50
		maxSwaps        = 4 * 3
	)

	l := b - a

	var (
		swaps int
		i     = a + l/4*1
		j     = a + l/4*2
		k     = a + l/4*3
	)

	if l >= 8 {
		if l >= shortestNinther {
			// Tukey ninther method, the idea came from Rust's implementation.
			i = medianAdjacent(data, i, &swaps, os, fn)
			j = medianAdjacent(data, j, &swaps, os, fn)
			k = medianAdjacent(data, k, &swaps, os, fn)
		}
		// Find the median among i, j, k and stores it into j.
		j = median(data, i, j, k, &swaps, os, fn)
	}

	switch swaps {
	case 0:
		return j, increasingHint
	case maxSwaps:
		return j, decreasingHint
	default:
		return j, unknownHint
	}
}

// order2 returns x,y where data[x] <= data[y], where x,y=a,b or x,y=b,a.
func order2[T any](data []T, a, b int, swaps *int, os []int64, fn func([]T, int64, int64) bool) (int, int) {
	if fn(data, os[b], os[a]) {
		*swaps++
		return b, a
	}
	return a, b
}

// median returns x where data[x] is the median of data[a],data[b],data[c], where x is a, b, or c.
func median[T any](data []T, a, b, c int, swaps *int, os []int64, fn func([]T, int64, int64) bool) int {
	a, b = order2(data, a, b, swaps, os, fn)
	b, _ = order2(data, b, c, swaps, os, fn)
	_, b = order2(data, a, b, swaps, os, fn)
	return b
}

// medianAdjacent finds the median of data[a - 1], data[a], data[a + 1] and stores the index into a.
func medianAdjacent[T any](data []T, a int, swaps *int, os []int64, fn func([]T, int64, int64) bool) int {
	return median(data, a-1, a, a+1, swaps, os, fn)
}

func reverseRange[T any](data []T, a, b int, os []int64, fn func([]T, int64, int64) bool) {
	i := a
	j := b - 1
	for i < j {
		os[i], os[j] = os[j], os[i]
		i++
		j--
	}
}
