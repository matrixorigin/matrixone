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
	switch vec.Typ.Oid {
	case types.T_int8:
		var n bool
		var v int8

		vs := vec.Col.([]int8)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]int16)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]int32)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]types.Date)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]int64)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]types.Datetime)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]types.Time)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]uint8)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]uint16)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]uint32)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]uint64)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]float32)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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

		vs := vec.Col.([]float64)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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
		vs := vector.GetStrVectorValues(vec)
		if nulls.Any(vec.Nsp) {
			for i, sel := range sels {
				w := vs[sel]
				isNull := nulls.Contains(vec.Nsp, uint64(sel))
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
