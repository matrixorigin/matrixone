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

package compute

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func SortAndDedup[T any](
	vals []T,
	lessFn func(*T, *T) bool,
	eqFn func(*T, *T) bool,
) []T {
	if len(vals) < 1 {
		return vals
	}
	sort.Slice(vals, func(i, j int) bool {
		return lessFn(&vals[i], &vals[j])
	})
	pos := 1
	for start, end := 1, len(vals); start < end; start++ {
		if !eqFn(&vals[start], &vals[start-1]) {
			vals[pos] = vals[start]
			pos++
		}
	}
	return vals[:pos]
}

func ShuffleByDeletes(inputDeletes, deletes *nulls.Bitmap) (outDeletes *nulls.Bitmap) {
	if deletes.IsEmpty() || inputDeletes.IsEmpty() {
		return inputDeletes
	}
	delIt := inputDeletes.GetBitmap().Iterator()
	outDeletes = nulls.NewWithSize(1)
	deleteIt := deletes.GetBitmap().Iterator()
	deleteCnt := uint64(0)
	for deleteIt.HasNext() {
		del := deleteIt.Next()
		for delIt.HasNext() {
			row := delIt.PeekNext()
			if row < del {
				outDeletes.Add(row - deleteCnt)
				delIt.Next()
			} else if row == del {
				delIt.Next()
			} else {
				break
			}
		}
		deleteCnt++
	}
	for delIt.HasNext() {
		row := delIt.Next()
		outDeletes.Add(row - deleteCnt)
	}

	return outDeletes
}

func GetOffsetOfBytes(
	data *vector.Vector,
	val []byte,
	skipmask *nulls.Bitmap,
) (offset int, exist bool) {
	start, end := 0, data.Length()-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		res := bytes.Compare(data.GetBytesAt(mid), val)
		if res > 0 {
			end = mid - 1
		} else if res < 0 {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint64(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return

}

func GetOffsetWithFunc[T any](
	vals []T,
	val T,
	compare func(a, b T) int,
	skipmask *nulls.Bitmap,
) (offset int, exist bool) {
	start, end := 0, len(vals)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		res := compare(vals[mid], val)
		if res > 0 {
			end = mid - 1
		} else if res < 0 {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint64(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func GetOffsetOfOrdered[T types.OrderedT](column []T, val T, skipmask *nulls.Bitmap) (offset int, exist bool) {
	start, end := 0, len(column)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if column[mid] > val {
			end = mid - 1
		} else if column[mid] < val {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint64(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func GetOffsetByVal(data containers.Vector, v any, skipmask *nulls.Bitmap) (offset int, exist bool) {
	vec := data.GetDownstreamVector()
	switch data.GetType().Oid {
	case types.T_bool:
		vs := vector.MustFixedColNoTypeCheck[bool](vec)
		return GetOffsetWithFunc(vs, v.(bool), CompareBool, skipmask)
	case types.T_bit:
		vs := vector.MustFixedColNoTypeCheck[uint64](vec)
		return GetOffsetOfOrdered(vs, v.(uint64), skipmask)
	case types.T_int8:
		vs := vector.MustFixedColNoTypeCheck[int8](vec)
		return GetOffsetOfOrdered(vs, v.(int8), skipmask)
	case types.T_int16:
		vs := vector.MustFixedColNoTypeCheck[int16](vec)
		return GetOffsetOfOrdered(vs, v.(int16), skipmask)
	case types.T_int32:
		vs := vector.MustFixedColNoTypeCheck[int32](vec)
		return GetOffsetOfOrdered(vs, v.(int32), skipmask)
	case types.T_int64:
		vs := vector.MustFixedColNoTypeCheck[int64](vec)
		return GetOffsetOfOrdered(vs, v.(int64), skipmask)
	case types.T_uint8:
		vs := vector.MustFixedColNoTypeCheck[uint8](vec)
		return GetOffsetOfOrdered(vs, v.(uint8), skipmask)
	case types.T_uint16:
		vs := vector.MustFixedColNoTypeCheck[uint16](vec)
		return GetOffsetOfOrdered(vs, v.(uint16), skipmask)
	case types.T_uint32:
		vs := vector.MustFixedColNoTypeCheck[uint32](vec)
		return GetOffsetOfOrdered(vs, v.(uint32), skipmask)
	case types.T_uint64:
		vs := vector.MustFixedColNoTypeCheck[uint64](vec)
		return GetOffsetOfOrdered(vs, v.(uint64), skipmask)
	case types.T_float32:
		vs := vector.MustFixedColNoTypeCheck[float32](vec)
		return GetOffsetOfOrdered(vs, v.(float32), skipmask)
	case types.T_float64:
		vs := vector.MustFixedColNoTypeCheck[float64](vec)
		return GetOffsetOfOrdered(vs, v.(float64), skipmask)
	case types.T_date:
		vs := vector.MustFixedColNoTypeCheck[types.Date](vec)
		return GetOffsetOfOrdered(vs, v.(types.Date), skipmask)
	case types.T_time:
		vs := vector.MustFixedColNoTypeCheck[types.Time](vec)
		return GetOffsetOfOrdered(vs, v.(types.Time), skipmask)
	case types.T_datetime:
		vs := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		return GetOffsetOfOrdered(vs, v.(types.Datetime), skipmask)
	case types.T_timestamp:
		vs := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		return GetOffsetOfOrdered(vs, v.(types.Timestamp), skipmask)
	case types.T_enum:
		vs := vector.MustFixedColNoTypeCheck[types.Enum](vec)
		return GetOffsetOfOrdered(vs, v.(types.Enum), skipmask)
	case types.T_decimal64:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		return GetOffsetWithFunc(
			vs,
			v.(types.Decimal64),
			types.CompareDecimal64,
			skipmask)
	case types.T_decimal128:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		return GetOffsetWithFunc(
			vs,
			v.(types.Decimal128),
			types.CompareDecimal128,
			skipmask)
	case types.T_TS:
		return GetOffsetWithFunc(
			vector.MustFixedColNoTypeCheck[types.TS](vec),
			v.(types.TS),
			types.CompareTSTSAligned,
			skipmask)
	case types.T_Rowid:
		return GetOffsetWithFunc(
			vector.MustFixedColNoTypeCheck[types.Rowid](vec),
			v.(types.Rowid),
			types.CompareRowidRowidAligned,
			skipmask)
	case types.T_Blockid:
		return GetOffsetWithFunc(
			vector.MustFixedColNoTypeCheck[types.Blockid](vec),
			v.(types.Blockid),
			types.CompareBlockidBlockidAligned,
			skipmask)
	case types.T_uuid:
		return GetOffsetWithFunc(
			vector.MustFixedColNoTypeCheck[types.Uuid](vec),
			v.(types.Uuid),
			types.CompareUuid,
			skipmask)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_json, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		// data is retrieved from DN vector, hence T_array can be handled here.
		val := v.([]byte)
		start, end := 0, data.Length()-1
		var mid int
		for start <= end {
			mid = (start + end) / 2
			res := bytes.Compare(data.ShallowGet(mid).([]byte), val)
			if res > 0 {
				end = mid - 1
			} else if res < 0 {
				start = mid + 1
			} else {
				if skipmask != nil && skipmask.Contains(uint64(mid)) {
					return
				}
				offset = mid
				exist = true
				return
			}
		}
		return
	default:
		panic("unsupported type")
	}
}

func GetOrderedMinAndMax[T types.OrderedT](vs ...T) (minv, maxv T) {
	minv = vs[0]
	maxv = vs[0]
	for _, v := range vs[1:] {
		if v < minv {
			minv = v
		}
		if v > maxv {
			maxv = v
		}
	}
	return
}

func GetDecimal64MinAndMax(vs []types.Decimal64) (minv, maxv types.Decimal64) {
	minv = vs[0]
	maxv = vs[0]
	for _, v := range vs[1:] {
		if types.CompareDecimal64(v, minv) < 0 {
			minv = v
		}
		if types.CompareDecimal64(v, maxv) > 0 {
			maxv = v
		}
	}
	return
}

func GetDecimal128MinAndMax(vs []types.Decimal128) (minv, maxv types.Decimal128) {
	minv = vs[0]
	maxv = vs[0]
	for _, v := range vs[1:] {
		if types.CompareDecimal128(v, minv) < 0 {
			minv = v
		}
		if types.CompareDecimal128(v, maxv) > 0 {
			maxv = v
		}
	}
	return
}
