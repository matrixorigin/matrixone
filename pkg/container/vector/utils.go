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
	v T, s []T, compare func(T, T) int64,
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
