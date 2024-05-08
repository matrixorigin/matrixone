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
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
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

// IntegerGetSum get the sum the vector if the vector type is integer.
func IntegerGetSum[T types.Ints | types.UInts, U int64 | uint64](vec *Vector) (sum U) {
	var ok bool
	col := MustFixedCol[T](vec)
	if vec.HasNull() {
		for i, v := range col {
			if vec.IsNull(uint64(i)) {
				continue
			}
			sum, ok = addInt(sum, U(v))
			if !ok {
				return 0
			}
		}
	} else {
		for _, v := range col {
			sum, ok = addInt(sum, U(v))
			if !ok {
				return 0
			}
		}
	}
	return
}

func addInt[T int64 | uint64](a, b T) (T, bool) {
	c := a + b
	if (c > a) == (b > 0) {
		return c, true
	}
	return 0, false
}

// FloatGetSum get the sum the vector if the vector type is float.
func FloatGetSum[T types.Floats](vec *Vector) (sum float64) {
	col := MustFixedCol[T](vec)
	if vec.HasNull() {
		for i, v := range col {
			if vec.IsNull(uint64(i)) {
				continue
			}
			sum += float64(v)
		}
	} else {
		for _, v := range col {
			sum += float64(v)
		}
	}
	return
}

func Decimal64GetSum(vec *Vector) (sum types.Decimal64) {
	var err error
	col := MustFixedCol[types.Decimal64](vec)
	if vec.HasNull() {
		for i, v := range col {
			if vec.IsNull(uint64(i)) {
				continue
			}
			sum, err = sum.Add64(v)
			if err != nil {
				return 0
			}
		}
	} else {
		for _, dec := range col {
			sum, err = sum.Add64(dec)
			if err != nil {
				return 0
			}
		}
	}
	return
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

func ArrayGetMinMax[T types.RealNumbers](vec *Vector) (minv, maxv []T) {
	col, area := MustVarlenaRawData(vec)
	if vec.HasNull() {
		first := true
		for i, j := 0, vec.Length(); i < j; i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			val := types.GetArray[T](&col[i], area)
			if first {
				minv, maxv = val, val
				first = false
			} else {
				if moarray.Compare[T](minv, val) > 0 {
					minv = val
				}
				if moarray.Compare[T](maxv, val) < 0 {
					maxv = val
				}
			}
		}
	} else {
		val := types.GetArray[T](&col[0], area)
		minv, maxv = val, val
		for i, j := 1, vec.Length(); i < j; i++ {
			val := types.GetArray[T](&col[i], area)
			if moarray.Compare[T](minv, val) > 0 {
				minv = val
			}
			if moarray.Compare[T](maxv, val) < 0 {
				maxv = val
			}
		}
	}
	return
}

func typeCompatible[T any](typ types.Type) bool {
	var t T
	switch (any)(t).(type) {
	case types.Varlena:
		return typ.IsVarlen()
	case bool:
		return typ.IsBoolean()
	case int8:
		return typ.Oid == types.T_int8
	case int16:
		return typ.Oid == types.T_int16
	case int32:
		return typ.Oid == types.T_int32
	case int64:
		return typ.Oid == types.T_int64
	case uint8:
		return typ.Oid == types.T_uint8
	case uint16:
		return typ.Oid == types.T_uint16
	case uint32:
		return typ.Oid == types.T_uint32
	case uint64:
		return typ.Oid == types.T_uint64 || typ.Oid == types.T_bit
	case float32:
		return typ.Oid == types.T_float32
	case float64:
		return typ.Oid == types.T_float64
	case types.Decimal64:
		return typ.Oid == types.T_decimal64
	case types.Decimal128:
		return typ.Oid == types.T_decimal128
	case types.Uuid:
		return typ.Oid == types.T_uuid
	case types.Date:
		return typ.Oid == types.T_date
	case types.Time:
		return typ.Oid == types.T_time
	case types.Datetime:
		return typ.Oid == types.T_datetime
	case types.Timestamp:
		return typ.Oid == types.T_timestamp
	case types.TS:
		return typ.Oid == types.T_TS
	case types.Rowid:
		return typ.Oid == types.T_Rowid
	case types.Blockid:
		return typ.Oid == types.T_Blockid
	case types.Enum:
		return typ.Oid == types.T_enum
	}
	return false
}
