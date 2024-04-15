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

const kMaxLenForBinarySearch = 64

func OrderedBinarySearchOffsetByValFactory[T types.OrderedT](vals []T) func(*Vector) []int32 {
	return func(vec *Vector) []int32 {
		var sels []int32
		rows := MustFixedCol[T](vec)

		if len(vals) <= kMaxLenForBinarySearch {
			offset := 0
			for i := range vals {
				idx := sort.Search(len(rows), func(idx int) bool {
					return rows[idx] >= vals[i]
				})
				if idx < len(rows) {
					if rows[idx] == vals[i] {
						sels = append(sels, int32(offset+idx))
					}
					offset += idx
					rows = rows[idx:]
				} else {
					break
				}
			}
		} else {
			n1, n2 := len(rows), len(vals)
			i1, i2 := 0, 0
			for i1 < n1 && i2 < n2 {
				if rows[i1] == vals[i2] {
					sels = append(sels, int32(i1))
					i1++
					i2++
				} else if rows[i1] < vals[i2] {
					i1++
				} else {
					i2++
				}
			}
		}

		return sels
	}
}

func VarlenBinarySearchOffsetByValFactory(vals [][]byte) func(*Vector) []int32 {
	return func(vec *Vector) []int32 {
		var sels []int32
		n1 := vec.Length()
		if n1 == 0 {
			return sels
		}

		if len(vals) <= kMaxLenForBinarySearch {
			offset := 0
			for i := range vals {
				idx, found := sort.Find(n1, func(idx int) int {
					return bytes.Compare(vals[i], vec.GetBytesAt(offset+idx))
				})
				if idx < n1 {
					if found {
						sels = append(sels, int32(offset+idx))
					}
					offset += idx
					n1 -= idx
				} else {
					break
				}
			}
		} else {
			n2 := len(vals)
			i1, i2 := 0, 0
			varlenas := MustFixedCol[types.Varlena](vec)
			s1 := varlenas[0].GetByteSlice(vec.GetArea())
			for i2 < n2 {
				ord := bytes.Compare(s1, vals[i2])
				if ord == 0 {
					sels = append(sels, int32(i1))
					i1++
					if i1 == n1 {
						break
					}
					i2++
					s1 = varlenas[i1].GetByteSlice(vec.GetArea())
				} else if ord < 0 {
					i1++
					if i1 == n1 {
						break
					}
					s1 = varlenas[i1].GetByteSlice(vec.GetArea())
				} else {
					i2++
				}
			}
		}

		return sels
	}
}

func FixedSizedBinarySearchOffsetByValFactory[T any](vals []T, cmp func(T, T) int) func(*Vector) []int32 {
	return func(vec *Vector) []int32 {
		var sels []int32
		rows := MustFixedCol[T](vec)

		if len(vals) <= kMaxLenForBinarySearch {
			offset := 0
			for i := range vals {
				idx, found := sort.Find(len(rows), func(idx int) int {
					return cmp(vals[i], rows[i])
				})
				if idx < len(rows) {
					if found {
						sels = append(sels, int32(offset+idx))
					}
					offset += idx
					rows = rows[idx:]
				} else {
					break
				}
			}
		} else {
			n1, n2 := len(rows), len(vals)
			i1, i2 := 0, 0
			for i1 < n1 && i2 < n2 {
				ord := cmp(rows[i1], vals[i2])
				if ord == 0 {
					sels = append(sels, int32(i1))
					i1++
					i2++
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

func CollectOffsetsByPrefixEqFactory(val []byte) func(*Vector) []int32 {
	return func(lvec *Vector) []int32 {
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
		sels := make([]int32, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int32(i)
		}
		return sels
	}
}

func CollectOffsetsByPrefixBetweenFactory(lval, rval []byte) func(*Vector) []int32 {
	return func(lvec *Vector) []int32 {
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
		sels := make([]int32, end-start)
		for i := start; i < end; i++ {
			sels[i-start] = int32(i)
		}
		return sels
	}
}

func CollectOffsetsByPrefixInFactory(rvec *Vector) func(*Vector) []int32 {
	return func(lvec *Vector) []int32 {
		lvlen := lvec.Length()
		if lvlen == 0 {
			return nil
		}

		lcol, larea := MustVarlenaRawData(lvec)
		rcol, rarea := MustVarlenaRawData(rvec)

		rval := rcol[0].GetByteSlice(rarea)
		rpos := 0
		rvlen := rvec.Length()

		sels := make([]int32, 0, rvlen)
		for i := 0; i < lvlen; i++ {
			lval := lcol[i].GetByteSlice(larea)
			for types.PrefixCompare(lval, rval) > 0 {
				rpos++
				if rpos == rvlen {
					return sels
				}

				rval = rcol[rpos].GetByteSlice(rarea)
			}

			if bytes.HasPrefix(lval, rval) {
				sels = append(sels, int32(i))
			}
		}

		return sels
	}
}
