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

package partition

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func genericPartition[T types.FixedSizeT](sels []int64, diffs []bool, partitions []int64, vec *vector.Vector) []int64 {
	partitions = partitions[:0]
	if len(sels) == 0 {
		return partitions
	}
	diffs[0] = true
	diffs = diffs[:len(sels)]

	if vec.IsConst() {
		if vec.IsConstNull() {
			for i := range sels {
				diffs[i] = false
			}
		}
		partitions = append(partitions, 0)

	} else {
		var n bool
		var v T

		vs := vector.MustFixedCol[T](vec)
		nsp := vec.GetNulls()
		if nsp.Any() {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(nsp, uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else if n && isNull {
					diffs[i] = false
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				n = isNull
				v = w
			}
		} else {
			for i, sel := range sels {
				w := vs[sel]
				diffs[i] = diffs[i] || (v != w)
				v = w
			}
		}

		for i, j := int64(0), int64(len(diffs)); i < j; i++ {
			if diffs[i] {
				partitions = append(partitions, i)
			}
		}
	}

	return partitions
}

func bytesPartition(sels []int64, diffs []bool, partitions []int64, vec *vector.Vector) []int64 {
	partitions = partitions[:0]
	if len(sels) == 0 {
		return partitions
	}
	diffs[0] = true
	diffs = diffs[:len(sels)]

	if vec.IsConst() {
		if vec.IsConstNull() {
			for i := range sels {
				diffs[i] = false
			}
		}
		partitions = append(partitions, 0)

	} else {
		var n bool
		var v []byte

		vs, area := vector.MustVarlenaRawData(vec)
		nsp := vec.GetNulls()
		if nsp.Any() {
			for i, sel := range sels {
				w := vs[sel].GetByteSlice(area)
				isNull := nulls.Contains(nsp, uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else if n && isNull {
					diffs[i] = false
				} else {
					diffs[i] = diffs[i] || !(bytes.Equal(v, w))
				}
				n = isNull
				v = w
			}
		} else {
			for i, sel := range sels {
				w := vs[sel].GetByteSlice(area)
				diffs[i] = diffs[i] || !(bytes.Equal(v, w))
				v = w
			}
		}
		for i, j := int64(0), int64(len(diffs)); i < j; i++ {
			if diffs[i] {
				partitions = append(partitions, i)
			}
		}
	}

	return partitions
}

// Partitions will return the rowSels; vs[rowSel] != vs[last_rowSel].
// by default, the 0th row is always not equal to the one before it
// (though it doesn't exist)
func Partition(sels []int64, diffs []bool, partitions []int64, vec *vector.Vector) []int64 {
	switch vec.GetType().Oid {
	case types.T_bool:
		return genericPartition[bool](sels, diffs, partitions, vec)
	case types.T_bit:
		return genericPartition[uint64](sels, diffs, partitions, vec)
	case types.T_int8:
		return genericPartition[int8](sels, diffs, partitions, vec)
	case types.T_int16:
		return genericPartition[int16](sels, diffs, partitions, vec)
	case types.T_int32:
		return genericPartition[int32](sels, diffs, partitions, vec)
	case types.T_int64:
		return genericPartition[int64](sels, diffs, partitions, vec)
	case types.T_uint8:
		return genericPartition[uint8](sels, diffs, partitions, vec)
	case types.T_uint16:
		return genericPartition[uint16](sels, diffs, partitions, vec)
	case types.T_uint32:
		return genericPartition[uint32](sels, diffs, partitions, vec)
	case types.T_uint64:
		return genericPartition[uint64](sels, diffs, partitions, vec)
	case types.T_float32:
		return genericPartition[float32](sels, diffs, partitions, vec)
	case types.T_float64:
		return genericPartition[float64](sels, diffs, partitions, vec)
	case types.T_date:
		return genericPartition[types.Date](sels, diffs, partitions, vec)
	case types.T_datetime:
		return genericPartition[types.Datetime](sels, diffs, partitions, vec)
	case types.T_time:
		return genericPartition[types.Time](sels, diffs, partitions, vec)
	case types.T_timestamp:
		return genericPartition[types.Timestamp](sels, diffs, partitions, vec)
	case types.T_enum:
		return genericPartition[types.Enum](sels, diffs, partitions, vec)
	case types.T_decimal64:
		return genericPartition[types.Decimal64](sels, diffs, partitions, vec)
	case types.T_decimal128:
		return genericPartition[types.Decimal128](sels, diffs, partitions, vec)
	case types.T_char, types.T_varchar, types.T_json, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return bytesPartition(sels, diffs, partitions, vec)
		//Used by ORDER_BY SQL clause.
		//Byte partition logic doesn't use byte.Compare or Str.
		//Hence, we can use bytesPartition here.
	default:
		panic(moerr.NewNotSupportedNoCtx(vec.GetType().Oid.String()))
	}
}
