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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Partitions will return the rowSels; vs[rowSel] != vs[last_rowSel].
// by default, the 0th row is always not equal to the one before it
// (though it doesn't exist)
func Partition(sels []int64, diffs []bool, partitions []int64, vec *vector.Vector) []int64 {
	diffs[0] = true
	diffs = diffs[:len(sels)]
	switch vec.GetType().Oid {
	case types.T_int8:
		var n bool
		var v int8

		vs := vector.MustFixedCol[int8](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_int16:
		var n bool
		var v int16

		vs := vector.MustFixedCol[int16](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_int32:
		var n bool
		var v int32

		vs := vector.MustFixedCol[int32](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_date:
		var n bool
		var v types.Date

		vs := vector.MustFixedCol[types.Date](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_int64:
		var n bool
		var v int64

		vs := vector.MustFixedCol[int64](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_datetime:
		var n bool
		var v types.Datetime

		vs := vector.MustFixedCol[types.Datetime](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_time:
		var n bool
		var v types.Time

		vs := vector.MustFixedCol[types.Time](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_decimal64:
		var n bool
		var v types.Decimal64

		vs := vector.MustFixedCol[types.Decimal64](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_decimal128:
		var n bool
		var v types.Decimal128

		vs := vector.MustFixedCol[types.Decimal128](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_uint8:
		var n bool
		var v uint8

		vs := vector.MustFixedCol[uint8](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_uint16:
		var n bool
		var v uint16

		vs := vector.MustFixedCol[uint16](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_uint32:
		var n bool
		var v uint32

		vs := vector.MustFixedCol[uint32](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_uint64:
		var n bool
		var v uint64

		vs := vector.MustFixedCol[uint64](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_float32:
		var n bool
		var v float32

		vs := vector.MustFixedCol[float32](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_float64:
		var n bool
		var v float64

		vs := vector.MustFixedCol[float64](vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != vs[sel])
				}
				v = w
				n = isNull
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	case types.T_char, types.T_varchar, types.T_json:
		var n bool
		var v string
		vs := vector.MustStrCol(vec)
		if nulls.Any(vec.GetNulls()) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.GetNulls(), uint64(sel))
				if n != isNull {
					diffs[i] = true
				} else {
					diffs[i] = diffs[i] || (v != w)
				}
				n = isNull
				v = w
			}
			break
		}
		for i, sel := range sels {
			w := vs[sel]
			diffs[i] = diffs[i] || (v != w)
			v = w
		}
	}
	partitions = partitions[:0]
	for i, j := int64(0), int64(len(diffs)); i < j; i++ {
		if diffs[i] {
			partitions = append(partitions, i)
		}
	}
	return partitions
}
