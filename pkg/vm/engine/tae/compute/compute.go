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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type deleteRange struct {
	pos     uint32
	deleted uint32
}

func findDeleteRange(pos uint32, ranges []*deleteRange) *deleteRange {
	left, right := 0, len(ranges)-1
	var mid int
	for left <= right {
		mid = (left + right) / 2
		if ranges[mid].pos < pos {
			left = mid + 1
		} else if ranges[mid].pos > pos {
			right = mid - 1
		} else {
			break
		}
	}
	if mid == 0 && ranges[mid].pos < pos {
		mid = mid + 1
	}
	// logutil.Infof("pos=%d, mid=%d, range.pos=%d,range.deleted=%d", pos, mid, ranges[mid].pos, ranges[mid].deleted)
	return ranges[mid]
}

func ShuffleByDeletes(origMask *roaring.Bitmap, origVals map[uint32]any, deleteMask, deletes *roaring.Bitmap) (destMask *roaring.Bitmap, destVals map[uint32]any, destDelets *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return origMask, origVals, deleteMask
	}
	if origMask != nil && !origMask.IsEmpty() {
		valIt := origMask.Iterator()
		destMask = roaring.New()
		destVals = make(map[uint32]any)
		deleteIt := deletes.Iterator()
		deleteCnt := uint32(0)
		for deleteIt.HasNext() {
			del := deleteIt.Next()
			for valIt.HasNext() {
				row := valIt.PeekNext()
				if row < del {
					destMask.Add(row - deleteCnt)
					destVals[row-deleteCnt] = origVals[row]
					valIt.Next()
				} else if row == del {
					valIt.Next()
				} else {
					break
				}
			}
			deleteCnt++
		}
		for valIt.HasNext() {
			row := valIt.Next()
			destMask.Add(row - deleteCnt)
			destVals[row-deleteCnt] = origVals[row]
		}
	}
	if deleteMask != nil && !deleteMask.IsEmpty() {
		delIt := deleteMask.Iterator()
		destDelets = roaring.New()
		deleteIt := deletes.Iterator()
		deleteCnt := uint32(0)
		for deleteIt.HasNext() {
			del := deleteIt.Next()
			for delIt.HasNext() {
				row := delIt.PeekNext()
				if row < del {
					destDelets.Add(row - deleteCnt)
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
			destDelets.Add(row - deleteCnt)
		}
	}
	return destMask, destVals, destDelets
}

func GetOffsetWithFunc[T any](
	vals []T,
	val T,
	compare func(a, b T) int64,
	skipmask *roaring.Bitmap,
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
			if skipmask != nil && skipmask.Contains(uint32(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func GetOffsetOfOrdered[T types.OrderedT](vs, v any, skipmask *roaring.Bitmap) (offset int, exist bool) {
	column := vs.([]T)
	val := v.(T)
	start, end := 0, len(column)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if column[mid] > val {
			end = mid - 1
		} else if column[mid] < val {
			start = mid + 1
		} else {
			if skipmask != nil && skipmask.Contains(uint32(mid)) {
				return
			}
			offset = mid
			exist = true
			return
		}
	}
	return
}

func EstimateSize(bat *containers.Batch, offset, length uint32) uint64 {
	size := uint64(0)
	for _, vec := range bat.Vecs {
		colSize := length * uint32(vec.GetType().Size)
		size += uint64(colSize)
	}
	return size
}

func GetOffsetByVal(data containers.Vector, v any, skipmask *roaring.Bitmap) (offset int, exist bool) {
	switch data.GetType().Oid {
	case types.Type_BOOL:
		return GetOffsetWithFunc[bool](data.Slice().([]bool), v.(bool), CompareBool, skipmask)
	case types.Type_INT8:
		return GetOffsetOfOrdered[int8](data.Slice(), v, skipmask)
	case types.Type_INT16:
		return GetOffsetOfOrdered[int16](data.Slice(), v, skipmask)
	case types.Type_INT32:
		return GetOffsetOfOrdered[int32](data.Slice(), v, skipmask)
	case types.Type_INT64:
		return GetOffsetOfOrdered[int64](data.Slice(), v, skipmask)
	case types.Type_UINT8:
		return GetOffsetOfOrdered[uint8](data.Slice(), v, skipmask)
	case types.Type_UINT16:
		return GetOffsetOfOrdered[uint16](data.Slice(), v, skipmask)
	case types.Type_UINT32:
		return GetOffsetOfOrdered[uint32](data.Slice(), v, skipmask)
	case types.Type_UINT64:
		return GetOffsetOfOrdered[uint64](data.Slice(), v, skipmask)
	case types.Type_FLOAT32:
		return GetOffsetOfOrdered[float32](data.Slice(), v, skipmask)
	case types.Type_FLOAT64:
		return GetOffsetOfOrdered[float64](data.Slice(), v, skipmask)
	case types.Type_DATE:
		return GetOffsetOfOrdered[types.Date](data.Slice(), v, skipmask)
	case types.Type_DATETIME:
		return GetOffsetOfOrdered[types.Datetime](data.Slice(), v, skipmask)
	case types.Type_TIMESTAMP:
		return GetOffsetOfOrdered[types.Timestamp](data.Slice(), v, skipmask)
	case types.Type_DECIMAL64:
		return GetOffsetOfOrdered[types.Decimal64](data.Slice(), v, skipmask)
	case types.Type_DECIMAL128:
		return GetOffsetWithFunc[types.Decimal128](
			data.Slice().([]types.Decimal128),
			v.(types.Decimal128),
			types.CompareDecimal128Decimal128Aligned,
			skipmask)
	case types.Type_CHAR, types.Type_VARCHAR:
		// column := data.Slice().(*containers.Bytes)
		val := v.([]byte)
		start, end := 0, data.Length()-1
		var mid int
		for start <= end {
			mid = (start + end) / 2
			res := bytes.Compare(data.Get(mid).([]byte), val)
			if res > 0 {
				end = mid - 1
			} else if res < 0 {
				start = mid + 1
			} else {
				if skipmask != nil && skipmask.Contains(uint32(mid)) {
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
