// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const kMinLenForSubVector = 4
const kMaxLenForBinarySearch = 64

func OrderedSearchOffsetsByLess[T types.OrderedT](ub T, closed bool, quick bool) func(vector *Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vector)
		if len(rows) == 0 {
			return sels
		}

		for x := range rows {
			if closed && rows[x] <= ub {
				sels = append(sels, int64(x))
			} else if !closed && rows[x] < ub {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}
		return sels
	}
}

func OrderedSearchOffsetsByGreat[T types.OrderedT](lb T, closed bool, quick bool) func(vector *Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vector)
		if len(rows) == 0 {
			return sels
		}
		ll := len(rows)
		for x := ll - 1; x >= 0; x-- {
			if closed && rows[x] >= lb {
				sels = append(sels, int64(x))
			} else if !closed && rows[x] > lb {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}
		return sels
	}
}

func FixedSizeSearchOffsetsByLessTypeChecked[
	T types.Decimal128 | types.Decimal64 | types.Uuid](
	ub T, closed bool, quick bool, cmp func(a, b T) int) func(vector *Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vector)
		if len(rows) == 0 {
			return sels
		}

		for x := range rows {
			if closed && cmp(rows[x], ub) <= 0 {
				sels = append(sels, int64(x))
			} else if !closed && cmp(rows[x], ub) < 0 {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}
		return sels
	}
}

func FixedSizeSearchOffsetsByGTTypeChecked[T types.Decimal128 | types.Decimal64 | types.Uuid](
	lb T, closed bool, quick bool, cmp func(a, b T) int,
) func(vector *Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vector)
		if len(rows) == 0 {
			return sels
		}

		for x := len(rows) - 1; x >= 0; x-- {
			if closed && cmp(rows[x], lb) >= 0 {
				sels = append(sels, int64(x))
			} else if !closed && cmp(rows[x], lb) > 0 {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}
		return sels
	}
}

func VarlenSearchOffsetByLess(ub []byte, closed bool, quick bool) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}

		for x := 0; x < vecLen; x++ {
			if closed && bytes.Compare(vector.GetBytesAt(x), ub) <= 0 {
				sels = append(sels, int64(x))
			} else if !closed && bytes.Compare(vector.GetBytesAt(x), ub) < 0 {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}

		return sels
	}
}

func VarlenSearchOffsetByGreat(lb []byte, closed bool, quick bool) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}

		for x := vecLen - 1; x >= 0; x-- {
			if closed && bytes.Compare(vector.GetBytesAt(x), lb) >= 0 {
				sels = append(sels, int64(x))
			} else if !closed && bytes.Compare(vector.GetBytesAt(x), lb) > 0 {
				sels = append(sels, int64(x))
			} else if quick {
				break
			}
		}

		return sels
	}
}

func OrderedLinearSearchOffsetByValFactory[T types.OrderedT | types.Decimal128 | types.Decimal64 | types.Uuid](
	vals []T, cmp func(T, T) int) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vector)
		if len(rows) == 0 {
			return sels
		}
		// sort the rows first maybe better if there has many vals
		for x := range rows {
			for y := range vals {
				if (cmp != nil && cmp(rows[x], vals[y]) == 0) || (cmp == nil && rows[x] == vals[y]) {
					sels = append(sels, int64(x))
					break
				}
			}
		}
		return sels
	}
}

func FixedSizeLinearSearchOffsetByValFactory[T types.Decimal128 | types.Decimal64 | types.Uuid](
	vals []T, cmp func(T, T) int) func(*Vector) []int64 {
	return OrderedLinearSearchOffsetByValFactory(vals, cmp)
}

func VarlenLinearSearchOffsetByValFactory(vals [][]byte) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		for x := 0; x < vecLen; x++ {
			for y := range vals {
				if bytes.Equal(vals[y], vector.GetBytesAt(x)) {
					sels = append(sels, int64(x))
					break
				}
			}
		}
		return sels
	}
}

func LinearCollectOffsetsByPrefixEqFactory(val []byte) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		col, area := MustVarlenaRawData(vector)
		for x := 0; x < vecLen; x++ {
			if bytes.HasPrefix(col[x].GetByteSlice(area), val) {
				sels = append(sels, int64(x))
			}
		}
		return sels
	}
}

func LinearCollectOffsetsByPrefixBetweenFactory(lb, ub []byte) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		col, area := MustVarlenaRawData(vector)
		for x := 0; x < vecLen; x++ {
			bb := col[x].GetByteSlice(area)
			if types.PrefixCompare(bb, lb) >= 0 && types.PrefixCompare(bb, ub) <= 0 {
				sels = append(sels, int64(x))
			}
		}
		return sels
	}
}

func LinearCollectOffsetsByBetweenString(lb, ub string, hint uint8) func(*Vector) []int64 {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	var check func(oth string) bool
	switch hint {
	case 0:
		check = func(oth string) bool { return oth >= lb && oth <= ub }
	case 1:
		check = func(oth string) bool { return oth > lb && oth <= ub }
	case 2:
		check = func(oth string) bool { return oth >= lb && oth < ub }
	case 3:
		check = func(oth string) bool { return oth > lb && oth < ub }
	default:
		panic(hint)
	}
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		col, area := MustVarlenaRawData(vector)
		for x := 0; x < vecLen; x++ {
			if check(col[x].UnsafeGetString(area)) {
				sels = append(sels, int64(x))
			}
			//if cols[x] >= lb && cols[x] <= ub {
			//
			//}
		}
		return sels
	}
}

func LinearCollectOffsetsByBetweenFactory[T types.BuiltinNumber | types.Times | types.Enum](lb, ub T, hint uint8) func(*Vector) []int64 {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	var check func(oth T) bool
	switch hint {
	case 0:
		check = func(oth T) bool { return oth >= lb && oth <= ub }
	case 1:
		check = func(oth T) bool { return oth > lb && oth <= ub }
	case 2:
		check = func(oth T) bool { return oth >= lb && oth < ub }
	case 3:
		check = func(oth T) bool { return oth > lb && oth < ub }
	default:
		panic(hint)
	}
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		cols := MustFixedColNoTypeCheck[T](vector)
		for x := 0; x < vecLen; x++ {
			if check(cols[x]) {
				sels = append(sels, int64(x))
			}
			//if cols[x] >= lb && cols[x] <= ub {
			//
			//}
		}
		return sels
	}
}

func FixedSizedLinearCollectOffsetsByBetweenFactory[
	T types.Decimal128 | types.Decimal64 | types.Uuid](lb, ub T, cmp func(T, T) int) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		cols := MustFixedColNoTypeCheck[T](vector)
		for x := 0; x < vecLen; x++ {
			if cmp(cols[x], lb) >= 0 && cmp(cols[x], ub) <= 0 {
				sels = append(sels, int64(x))
			}
		}
		return sels
	}
}

func LinearCollectOffsetsByPrefixInFactory(rvec *Vector) func(*Vector) []int64 {
	return func(lvec *Vector) []int64 {
		var sels []int64
		lvecLen := lvec.Length()
		rvecLen := rvec.Length()
		if lvecLen == 0 || rvecLen == 0 {
			return sels
		}
		lcol, larea := MustVarlenaRawData(lvec)
		rcol, rarea := MustVarlenaRawData(rvec)

		for x := 0; x < lvecLen; x++ {
			bb := lcol[x].GetByteSlice(larea)
			for y := 0; y < rvecLen; y++ {
				if types.PrefixCompare(bb, rcol[y].GetByteSlice(rarea)) == 0 {
					sels = append(sels, int64(x))
					break
				}
			}
		}
		return sels
	}
}

func OrderedBinarySearchOffsetByValFactory[T types.OrderedT](vals []T) func(*Vector) []int64 {
	return func(vec *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vec)
		if len(rows) == 0 || len(vals) == 0 {
			return sels
		}
		subVals := vals
		if len(vals) >= kMinLenForSubVector {
			minVal := rows[0]
			maxVal := rows[len(rows)-1]
			lowerBound := sort.Search(len(vals), func(i int) bool {
				return minVal <= vals[i]
			})
			upperBound := sort.Search(len(vals), func(i int) bool {
				return maxVal < vals[i]
			})
			subVals = vals[lowerBound:upperBound]
			if len(subVals) == 0 {
				return sels
			}
		}

		if len(subVals) <= kMaxLenForBinarySearch {
			start := 0
			n1 := len(rows)
			for i := 0; i < len(subVals); i++ {
				if i > 0 && subVals[i] == subVals[i-1] {
					continue
				}
				idx := sort.Search(n1-start, func(idx int) bool {
					return rows[start+idx] >= subVals[i]
				})
				pos := start + idx
				if pos >= n1 {
					break
				}
				if rows[pos] == subVals[i] {
					runEnd := pos + 1
					for runEnd < n1 && rows[runEnd] == subVals[i] {
						runEnd++
					}
					for j := pos; j < runEnd; j++ {
						sels = append(sels, int64(j))
					}
					start = runEnd
					continue
				}
				start = pos
			}
		} else {
			n1, n2 := len(rows), len(subVals)
			i1, i2 := 0, 0
			for i1 < n1 && i2 < n2 {
				if rows[i1] == subVals[i2] {
					val := subVals[i2]
					runStart := i1
					for i1 < n1 && rows[i1] == val {
						i1++
					}
					for j := runStart; j < i1; j++ {
						sels = append(sels, int64(j))
					}
					for i2 < n2 && subVals[i2] == val {
						i2++
					}
				} else if rows[i1] < subVals[i2] {
					i1++
				} else {
					i2++
				}
			}
		}

		return sels
	}
}

func VarlenBinarySearchOffsetByValFactory(vals [][]byte) func(*Vector) []int64 {
	return func(vec *Vector) []int64 {
		var sels []int64
		n1 := vec.Length()
		if n1 == 0 || len(vals) == 0 {
			return sels
		}
		varlenas := MustFixedColNoTypeCheck[types.Varlena](vec)
		area := vec.GetArea()

		subVals := vals
		if len(vals) >= kMinLenForSubVector {
			lowerBound := sort.Search(len(vals), func(i int) bool {
				return bytes.Compare(varlenas[0].GetByteSlice(area), vals[i]) <= 0
			})
			upperBound := sort.Search(len(vals), func(i int) bool {
				return bytes.Compare(varlenas[n1-1].GetByteSlice(area), vals[i]) < 0
			})
			subVals = vals[lowerBound:upperBound]
			if len(subVals) == 0 {
				return sels
			}
		}

		if len(subVals) <= kMaxLenForBinarySearch {
			start := 0
			for i := 0; i < len(subVals); i++ {
				if i > 0 && bytes.Equal(subVals[i], subVals[i-1]) {
					continue
				}
				idx := sort.Search(n1-start, func(idx int) bool {
					return bytes.Compare(varlenas[start+idx].GetByteSlice(area), subVals[i]) >= 0
				})
				pos := start + idx
				if pos >= n1 {
					break
				}
				if bytes.Equal(varlenas[pos].GetByteSlice(area), subVals[i]) {
					runEnd := pos + 1
					for runEnd < n1 && bytes.Equal(varlenas[runEnd].GetByteSlice(area), subVals[i]) {
						runEnd++
					}
					for j := pos; j < runEnd; j++ {
						sels = append(sels, int64(j))
					}
					start = runEnd
					continue
				}
				start = pos
			}
		} else {
			n2 := len(subVals)
			i1, i2 := 0, 0
			for i1 < n1 && i2 < n2 {
				ord := bytes.Compare(varlenas[i1].GetByteSlice(area), subVals[i2])
				if ord == 0 {
					val := subVals[i2]
					runStart := i1
					for i1 < n1 && bytes.Equal(varlenas[i1].GetByteSlice(area), val) {
						i1++
					}
					for j := runStart; j < i1; j++ {
						sels = append(sels, int64(j))
					}
					for i2 < n2 && bytes.Equal(subVals[i2], val) {
						i2++
					}
				} else if ord < 0 {
					i1++
				} else {
					i2++
				}
			}
		}

		return sels
	}
}

func FixedSizedBinarySearchOffsetByValFactory[T any](vals []T, cmp func(T, T) int) func(*Vector) []int64 {
	return func(vec *Vector) []int64 {
		var sels []int64
		rows := MustFixedColNoTypeCheck[T](vec)
		if len(rows) == 0 || len(vals) == 0 {
			return sels
		}

		subVals := vals
		if len(vals) >= kMinLenForSubVector {
			minVal := rows[0]
			maxVal := rows[len(rows)-1]
			lowerBound := sort.Search(len(vals), func(i int) bool {
				return cmp(minVal, vals[i]) <= 0
			})
			upperBound := sort.Search(len(vals), func(i int) bool {
				return cmp(maxVal, vals[i]) < 0
			})
			subVals = vals[lowerBound:upperBound]
			if len(subVals) == 0 {
				return sels
			}
		}

		if len(subVals) <= kMaxLenForBinarySearch {
			start := 0
			n1 := len(rows)
			for i := 0; i < len(subVals); i++ {
				if i > 0 && cmp(subVals[i], subVals[i-1]) == 0 {
					continue
				}
				idx := sort.Search(n1-start, func(idx int) bool {
					return cmp(rows[start+idx], subVals[i]) >= 0
				})
				pos := start + idx
				if pos >= n1 {
					break
				}
				if cmp(rows[pos], subVals[i]) == 0 {
					runEnd := pos + 1
					for runEnd < n1 && cmp(rows[runEnd], subVals[i]) == 0 {
						runEnd++
					}
					for j := pos; j < runEnd; j++ {
						sels = append(sels, int64(j))
					}
					start = runEnd
					continue
				}
				start = pos
			}
		} else {
			n1, n2 := len(rows), len(subVals)
			i1, i2 := 0, 0
			for i1 < n1 && i2 < n2 {
				ord := cmp(rows[i1], subVals[i2])
				if ord == 0 {
					val := subVals[i2]
					runStart := i1
					for i1 < n1 && cmp(rows[i1], val) == 0 {
						i1++
					}
					for j := runStart; j < i1; j++ {
						sels = append(sels, int64(j))
					}
					for i2 < n2 && cmp(subVals[i2], val) == 0 {
						i2++
					}
				} else if ord < 0 {
					i1++
				} else {
					i2++
				}
			}
		}

		return sels
	}
}

func CollectOffsetsByPrefixEqFactory(val []byte) func(*Vector) []int64 {
	return func(lvec *Vector) []int64 {
		lvlen := lvec.Length()
		if lvlen == 0 {
			return nil
		}
		lcol, larea := MustVarlenaRawData(lvec)
		start, _ := sort.Find(lvlen, func(i int) int {
			return bytes.Compare(val, lcol[i].GetByteSlice(larea))
		})
		end := start
		for end < lvlen && bytes.HasPrefix(lcol[end].GetByteSlice(larea), val) {
			end++
		}
		if start == end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

func CollectOffsetsByPrefixBetweenFactory(lval, rval []byte) func(*Vector) []int64 {
	return func(lvec *Vector) []int64 {
		lvlen := lvec.Length()
		if lvlen == 0 {
			return nil
		}
		lcol, larea := MustVarlenaRawData(lvec)
		start := sort.Search(lvlen, func(i int) bool {
			return bytes.Compare(lcol[i].GetByteSlice(larea), lval) >= 0
		})
		if start == lvlen {
			return nil
		}
		end := sort.Search(lvlen, func(i int) bool {
			return types.PrefixCompare(lcol[i].GetByteSlice(larea), rval) > 0
		})
		if start == end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

func CollectOffsetsByPrefixInRangeFactory(lb, ub []byte, hint uint8) func(*Vector) []int64 {
	return func(lvec *Vector) []int64 {
		lvlen := lvec.Length()
		if lvlen == 0 {
			return nil
		}
		lcol, larea := MustVarlenaRawData(lvec)
		var start, end int
		switch hint {
		case 0: // [lb, ub]
			start = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), lb) >= 0
			})
			end = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), ub) > 0
			})
		case 1: // (lb, ub]
			start = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), lb) > 0
			})
			end = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), ub) > 0
			})
		case 2: // [lb, ub)
			start = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), lb) >= 0
			})
			end = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), ub) >= 0
			})
		case 3: // (lb, ub)
			start = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), lb) > 0
			})
			end = sort.Search(lvlen, func(i int) bool {
				return types.PrefixCompare(lcol[i].GetByteSlice(larea), ub) >= 0
			})
		}
		if start >= end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

func LinearCollectOffsetsByPrefixInRangeFactory(lb, ub []byte, hint uint8) func(*Vector) []int64 {
	return func(vector *Vector) []int64 {
		var sels []int64
		vecLen := vector.Length()
		if vecLen == 0 {
			return sels
		}
		col, area := MustVarlenaRawData(vector)
		for x := 0; x < vecLen; x++ {
			bb := col[x].GetByteSlice(area)
			cmpLB := types.PrefixCompare(bb, lb)
			cmpUB := types.PrefixCompare(bb, ub)
			var match bool
			switch hint {
			case 0:
				match = cmpLB >= 0 && cmpUB <= 0
			case 1:
				match = cmpLB > 0 && cmpUB <= 0
			case 2:
				match = cmpLB >= 0 && cmpUB < 0
			case 3:
				match = cmpLB > 0 && cmpUB < 0
			}
			if match {
				sels = append(sels, int64(x))
			}
		}
		return sels
	}
}

func CollectOffsetsByBetweenWithCompareFactory[T types.Decimal | types.Uuid](lval, rval T, cmp func(T, T) int) func(*Vector) []int64 {
	return func(vec *Vector) []int64 {
		vecLen := vec.Length()
		if vecLen == 0 {
			return nil
		}
		cols := MustFixedColNoTypeCheck[T](vec)
		start := sort.Search(vecLen, func(i int) bool {
			return cmp(cols[i], lval) >= 0
		})
		if start == vecLen {
			return nil
		}
		end := sort.Search(vecLen, func(i int) bool {
			return cmp(cols[i], rval) > 0
		})
		if start == end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

func CollectOffsetsByBetweenFactory[T types.BuiltinNumber | types.Times | types.Enum](lval, rval T, hint uint8) func(*Vector) []int64 {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	var cmpLeft, cmpRight func(oth, val T) bool
	switch hint {
	case 0:
		cmpLeft = func(oth, val T) bool { return oth >= val }
		cmpRight = func(oth, val T) bool { return oth > val }
	case 1:
		cmpLeft = func(oth, val T) bool { return oth > val }
		cmpRight = func(oth, val T) bool { return oth > val }
	case 2:
		cmpLeft = func(oth, val T) bool { return oth >= val }
		cmpRight = func(oth, val T) bool { return oth >= val }
	case 3:
		cmpLeft = func(oth, val T) bool { return oth > val }
		cmpRight = func(oth, val T) bool { return oth >= val }
	default:
		panic(hint)
	}

	return func(vec *Vector) []int64 {
		vecLen := vec.Length()
		if vecLen == 0 {
			return nil
		}
		cols := MustFixedColNoTypeCheck[T](vec)
		start := sort.Search(vecLen, func(i int) bool {
			//return cols[i] >= lval
			return cmpLeft(cols[i], lval)
		})
		if start == vecLen {
			return nil
		}
		end := sort.Search(vecLen, func(i int) bool {
			//return cols[i] > rval
			return cmpRight(cols[i], rval)
		})
		if start >= end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

func CollectOffsetsByBetweenString(lval, rval string, hint uint8) func(*Vector) []int64 {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	var cmpLeft, cmpRight func(oth, val string) bool
	switch hint {
	case 0:
		cmpLeft = func(oth, val string) bool { return oth >= val }
		cmpRight = func(oth, val string) bool { return oth > val }
	case 1:
		cmpLeft = func(oth, val string) bool { return oth > val }
		cmpRight = func(oth, val string) bool { return oth > val }
	case 2:
		cmpLeft = func(oth, val string) bool { return oth >= val }
		cmpRight = func(oth, val string) bool { return oth >= val }
	case 3:
		cmpLeft = func(oth, val string) bool { return oth > val }
		cmpRight = func(oth, val string) bool { return oth >= val }
	default:
		panic(hint)
	}

	return func(vec *Vector) []int64 {
		vecLen := vec.Length()
		if vecLen == 0 {
			return nil
		}
		col, area := MustVarlenaRawData(vec)
		start := sort.Search(vecLen, func(i int) bool {
			//return cols[i] >= lval
			return cmpLeft(col[i].UnsafeGetString(area), lval)
		})
		if start == vecLen {
			return nil
		}
		end := sort.Search(vecLen, func(i int) bool {
			//return cols[i] > rval
			return cmpRight(col[i].UnsafeGetString(area), rval)
		})
		if start >= end {
			return nil
		}
		sels := make([]int64, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int64(i)
		}
		return sels
	}
}

// CollectOffsetsByPrefixInFactory builds a "prefix IN" probe: for each row of
// lvec it emits that row's offset when some entry of the captured rvec is a
// prefix of it (types.PrefixCompare == 0). Each matching row is emitted once,
// and offsets come back in ascending order.
//
// PRECONDITION: both lvec (the data column) and rvec (the needles) MUST be sorted
// ascending. The routine is a single forward merge with galloping over runs of
// non-matching rows, which is correct ONLY on sorted input. This contract is
// enforced by the caller, not checked here, on both sides:
//   - lvec (the block column) is sorted because this is wired exclusively as a
//     SortedSearchFunc, reached only via BlockReadFilter.DecideSearchFunc when the
//     block metadata reports IsSorted(); unsorted (or fake-PK) blocks are routed to
//     the brute-force LinearCollectOffsetsByPrefixInFactory, which needs no ordering.
//   - rvec (the needles) is the planner-built IN-list, normalized to ascending
//     order at construction (InplaceSortAndCompact, tracked by the vector's
//     GetSorted flag) — the same guarantee the binary-search IN filter and the
//     zone-map PrefixIn rely on.
//
// Do not call this directly on data that is not known to be sorted.
func CollectOffsetsByPrefixInFactory(rvec *Vector) func(*Vector) []int64 {
	return func(lvec *Vector) []int64 {
		lvlen := lvec.Length()
		if lvlen == 0 {
			return nil
		}

		rvlen := rvec.Length()
		if rvlen == 0 {
			return nil
		}

		lcol, larea := MustVarlenaRawData(lvec)
		rcol, rarea := MustVarlenaRawData(rvec)

		sels := make([]int64, 0, rvlen)

		rpos := 0
		rval := rcol[0].GetByteSlice(rarea)
		i := 0
		for i < lvlen {
			lval := lcol[i].GetByteSlice(larea)
			cmp := types.PrefixCompare(lval, rval)
			if cmp > 0 {
				// lval sorts past the current needle: advance to the next needle.
				rpos++
				if rpos == rvlen {
					break
				}
				rval = rcol[rpos].GetByteSlice(rarea)
				continue
			}
			if cmp == 0 {
				// lval has rval as a prefix (PrefixCompare == 0 iff HasPrefix).
				sels = append(sels, int64(i))
				i++
				continue
			}
			// cmp < 0: a run of rows here all sort below the current needle, so
			// none can match it. PrefixCompare(lcol[k], rval) is monotonic in k
			// (lcol is sorted; prefix truncation preserves lexicographic order),
			// so gallop past the run to the first row >= rval instead of stepping
			// one at a time. Output is identical to the linear merge (each row is
			// tested once, offsets stay ascending); the win is turning the
			// O(block-rows) walk into O(needles·log gap) when the needle set is
			// sparse relative to the block — the IVF top-k -> base-row case.
			i = gallopPrefixGE(lcol, larea, rval, i+1, lvlen)
		}

		return sels
	}
}

// gallopPrefixGE returns the smallest index idx in [lo, hi) for which
// PrefixCompare(lcol[idx].GetByteSlice(area), rval) >= 0, or hi if every row in
// the range sorts below rval. It assumes that comparison is monotonic
// non-decreasing over [lo, hi) (true when lcol is sorted). Exponential probing
// makes a unit gap cost O(1) and a gap of g cost O(log g), so it never does
// worse than the linear scan it replaces.
func gallopPrefixGE(lcol []types.Varlena, area, rval []byte, lo, hi int) int {
	prev, cur, step := lo, lo, 1
	for cur < hi && types.PrefixCompare(lcol[cur].GetByteSlice(area), rval) < 0 {
		prev = cur + 1
		cur += step
		step <<= 1
	}
	if cur > hi {
		cur = hi
	}
	for prev < cur {
		mid := int(uint(prev+cur) >> 1)
		if types.PrefixCompare(lcol[mid].GetByteSlice(area), rval) < 0 {
			prev = mid + 1
		} else {
			cur = mid
		}
	}
	return prev
}
