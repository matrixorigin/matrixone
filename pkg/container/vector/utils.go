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

package vector

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// FindFirstIndexInSortedSlice finds the first index of v in a sorted slice s
// If v is not found, return -1
func OrderedFindFirstIndexInSortedSlice[T types.OrderedT](v T, s []T) int {
	if len(s) == 0 {
		return -1
	}
	if len(s) == 1 {
		if s[0] == v {
			return 0
		}
		return -1
	}
	if s[0] == v {
		return 0
	}
	l, r := 0, len(s)-1
	for l < r {
		mid := (l + r) / 2
		if s[mid] >= v {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if s[l] == v {
		return l
	}
	return -1
}

// FindFirstIndexInSortedSlice finds the first index of v in a sorted slice s
// If v is not found, return -1
// compare is a function to compare two elements in s
func FixedSizeFindFirstIndexInSortedSliceWithCompare[T types.FixedSizeTExceptStrType](
	v T, s []T, compare func(T, T) int,
) int {
	if len(s) == 0 {
		return -1
	}
	if len(s) == 1 {
		if s[0] == v {
			return 0
		}
		return -1
	}
	if s[0] == v {
		return 0
	}
	l, r := 0, len(s)-1
	for l < r {
		mid := (l + r) / 2
		if compare(s[mid], v) >= 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if s[l] == v {
		return l
	}
	return -1
}

// FindFirstIndexInSortedSlice finds the first index of v in a sorted varlen vector
func FindFirstIndexInSortedVarlenVector(vec *Vector, v []byte) int {
	length := vec.Length()
	if length == 0 {
		return -1
	}
	if bytes.Equal(vec.GetBytesAt(0), v) {
		return 0
	}
	if length == 1 {
		return -1
	}
	l, r := 0, length-1
	for l < r {
		mid := (l + r) / 2
		if bytes.Compare(vec.GetBytesAt(mid), v) >= 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if bytes.Equal(vec.GetBytesAt(l), v) {
		return l
	}
	return -1
}

// OrderedGetMinAndMax returns the min and max value of a vector of ordered type
// If the vector has null, the null value will be ignored
func OrderedGetMinAndMax[T types.OrderedT](vec *Vector) (minv, maxv T) {
	col := MustFixedCol[T](vec)
	if vec.HasNull() {
		first := true
		for i, j := 0, vec.Length(); i < j; i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			if first {
				minv, maxv = col[i], col[i]
				first = false
			} else {
				if minv > col[i] {
					minv = col[i]
				}
				if maxv < col[i] {
					maxv = col[i]
				}
			}
		}
	} else {
		minv, maxv = col[0], col[0]
		for i, j := 1, vec.Length(); i < j; i++ {
			if minv > col[i] {
				minv = col[i]
			}
			if maxv < col[i] {
				maxv = col[i]
			}
		}
	}
	return
}

func FixedSizeGetMinMax[T types.OrderedT](
	vec *Vector, comp func(T, T) int64,
) (minv, maxv T) {
	col := MustFixedCol[T](vec)
	if vec.HasNull() {
		first := true
		for i, j := 0, vec.Length(); i < j; i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			if first {
				minv, maxv = col[i], col[i]
				first = false
			} else {
				if comp(minv, col[i]) > 0 {
					minv = col[i]
				}
				if comp(maxv, col[i]) < 0 {
					maxv = col[i]
				}
			}
		}
	} else {
		minv, maxv = col[0], col[0]
		for i, j := 1, vec.Length(); i < j; i++ {
			if comp(minv, col[i]) > 0 {
				minv = col[i]
			}
			if comp(maxv, col[i]) < 0 {
				maxv = col[i]
			}
		}
	}
	return
}

func VarlenGetMinMax(vec *Vector) (minv, maxv []byte) {
	col, area := MustVarlenaRawData(vec)
	if vec.HasNull() {
		first := true
		for i, j := 0, vec.Length(); i < j; i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			val := col[i].GetByteSlice(area)
			if first {
				minv, maxv = val, val
				first = false
			} else {
				if bytes.Compare(minv, val) > 0 {
					minv = val
				}
				if bytes.Compare(maxv, val) < 0 {
					maxv = val
				}
			}
		}
	} else {
		val := col[0].GetByteSlice(area)
		minv, maxv = val, val
		for i, j := 1, vec.Length(); i < j; i++ {
			val := col[i].GetByteSlice(area)
			if bytes.Compare(minv, val) > 0 {
				minv = val
			}
			if bytes.Compare(maxv, val) < 0 {
				maxv = val
			}
		}
	}
	return
}
