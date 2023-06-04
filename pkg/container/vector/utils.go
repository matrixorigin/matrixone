package vector

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

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
