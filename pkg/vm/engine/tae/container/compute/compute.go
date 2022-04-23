package compute

import (
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

func AppendValue(vec *gvec.Vector, v interface{}) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vvals := vec.Col.([]int8)
		vec.Col = append(vvals, v.(int8))
	case types.T_int16:
		vvals := vec.Col.([]int16)
		vec.Col = append(vvals, v.(int16))
	case types.T_int32:
		vvals := vec.Col.([]int32)
		vec.Col = append(vvals, v.(int32))
	case types.T_int64:
		vvals := vec.Col.([]int64)
		vec.Col = append(vvals, v.(int64))
	case types.T_uint8:
		vvals := vec.Col.([]uint8)
		vec.Col = append(vvals, v.(uint8))
	case types.T_uint16:
		vvals := vec.Col.([]uint16)
		vec.Col = append(vvals, v.(uint16))
	case types.T_uint32:
		vvals := vec.Col.([]uint32)
		vec.Col = append(vvals, v.(uint32))
	case types.T_uint64:
		vvals := vec.Col.([]uint64)
		vec.Col = append(vvals, v.(uint64))
	case types.T_decimal:
		vvals := vec.Col.([]types.Decimal)
		vec.Col = append(vvals, v.(types.Decimal))
	case types.T_float32:
		vvals := vec.Col.([]float32)
		vec.Col = append(vvals, v.(float32))
	case types.T_float64:
		vvals := vec.Col.([]float64)
		vec.Col = append(vvals, v.(float64))
	case types.T_date:
		vvals := vec.Col.([]types.Date)
		vec.Col = append(vvals, v.(types.Date))
	case types.T_datetime:
		vvals := vec.Col.([]types.Datetime)
		vec.Col = append(vvals, v.(types.Datetime))
	case types.T_char, types.T_varchar, types.T_json:
		vvals := vec.Col.(*types.Bytes)
		offset := len(vvals.Data)
		length := len(v.([]byte))
		vvals.Data = append(vvals.Data, v.([]byte)...)
		vvals.Offsets = append(vvals.Offsets, uint32(offset))
		vvals.Lengths = append(vvals.Lengths, uint32(length))
	default:
		panic("not expected")
	}
}

func GetValue(col *gvec.Vector, row uint32) interface{} {
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		return data[row]
	case types.T_int16:
		data := vals.([]int16)
		return data[row]
	case types.T_int32:
		data := vals.([]int32)
		return data[row]
	case types.T_int64:
		data := vals.([]int64)
		return data[row]
	case types.T_uint8:
		data := vals.([]uint8)
		return data[row]
	case types.T_uint16:
		data := vals.([]uint16)
		return data[row]
	case types.T_uint32:
		data := vals.([]uint32)
		return data[row]
	case types.T_uint64:
		data := vals.([]uint64)
		return data[row]
	case types.T_decimal:
		data := vals.([]types.Decimal)
		return data[row]
	case types.T_float32:
		data := vals.([]float32)
		return data[row]
	case types.T_float64:
		data := vals.([]float64)
		return data[row]
	case types.T_date:
		data := vals.([]types.Date)
		return data[row]
	case types.T_datetime:
		data := vals.([]types.Datetime)
		return data[row]
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		s := data.Offsets[row]
		e := data.Lengths[row]
		return string(data.Data[s : e+s])
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}
func SetFixSizeTypeValue(col *gvec.Vector, row uint32, val interface{}) error {
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		data[row] = val.(int8)
		col.Col = data
	case types.T_int16:
		data := vals.([]int16)
		data[row] = val.(int16)
		col.Col = data
	case types.T_int32:
		data := vals.([]int32)
		data[row] = val.(int32)
		col.Col = data
	case types.T_int64:
		data := vals.([]int64)
		data[row] = val.(int64)
		col.Col = data
	case types.T_uint8:
		data := vals.([]uint8)
		data[row] = val.(uint8)
		col.Col = data
	case types.T_uint16:
		data := vals.([]uint16)
		data[row] = val.(uint16)
		col.Col = data
	case types.T_uint32:
		data := vals.([]uint32)
		data[row] = val.(uint32)
		col.Col = data
	case types.T_uint64:
		data := vals.([]uint64)
		data[row] = val.(uint64)
		col.Col = data
	case types.T_decimal:
		data := vals.([]types.Decimal)
		data[row] = val.(types.Decimal)
		col.Col = data
	case types.T_float32:
		data := vals.([]float32)
		data[row] = val.(float32)
		col.Col = data
	case types.T_float64:
		data := vals.([]float64)
		data[row] = val.(float64)
		col.Col = data
	case types.T_date:
		data := vals.([]types.Date)
		data[row] = val.(types.Date)
		col.Col = data
	case types.T_datetime:
		data := vals.([]types.Datetime)
		data[row] = val.(types.Datetime)
		col.Col = data
	case types.T_char, types.T_varchar, types.T_json:
		// data := vals.(*types.Bytes)
		// s := data.Offsets[row]
		// e := data.Lengths[row]
		// return string(data.Data[s:e])
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}

func DeleteFixSizeTypeValue(col *gvec.Vector, row uint32) error {
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_int16:
		data := vals.([]int16)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_int32:
		data := vals.([]int32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_int64:
		data := vals.([]int64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_uint8:
		data := vals.([]uint8)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_uint16:
		data := vals.([]uint16)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_uint32:
		data := vals.([]uint32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_uint64:
		data := vals.([]uint64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_decimal:
		data := vals.([]types.Decimal)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_float32:
		data := vals.([]float32)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_float64:
		data := vals.([]float64)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_date:
		data := vals.([]types.Date)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_datetime:
		data := vals.([]types.Datetime)
		data = append(data[:row], data[row+1:]...)
		col.Col = data
	case types.T_char, types.T_varchar, types.T_json:
		// data := vals.(*types.Bytes)
		// s := data.Offsets[row]
		// e := data.Lengths[row]
		// return string(data.Data[s:e])
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}

func UpdateOffsets(data *types.Bytes, start, end int) {
	if start == -1 {
		data.Offsets[0] = 0
		start++
	}
	for i := start; i < end; i++ {
		data.Offsets[i+1] = data.Offsets[i] + data.Lengths[i]
	}
}
func SplitBatch(bat *gbat.Batch, cnt int) []*gbat.Batch {
	if cnt == 1 {
		return []*gbat.Batch{bat}
	}
	length := gvec.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*gbat.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := gbat.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := gvec.New(bat.Vecs[j].Typ)
				gvec.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
				newBat.Vecs[j] = window
			}
			bats = append(bats, newBat)
		}
		return bats
	}
	rowArray := make([]int, 0)
	if length/cnt == 0 {
		for i := 0; i < length; i++ {
			rowArray = append(rowArray, 1)
		}
	} else {
		left := length
		for i := 0; i < cnt; i++ {
			if left >= rows {
				rowArray = append(rowArray, rows)
			} else {
				rowArray = append(rowArray, left)
			}
			left -= rows
		}
	}
	start := 0
	bats := make([]*gbat.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := gbat.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := gvec.New(bat.Vecs[j].Typ)
			gvec.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func EstimateSize(bat *gbat.Batch, offset, length uint32) uint64 {
	size := uint64(0)
	for _, vec := range bat.Vecs {
		colSize := length * uint32(vec.Typ.Size)
		size += uint64(colSize)
	}
	return size
}

func CopyToIBatch(data *gbat.Batch, capacity uint64) (bat batch.IBatch, err error) {
	vecs := make([]vector.IVector, len(data.Vecs))
	attrs := make([]int, len(data.Vecs))
	for i, vec := range data.Vecs {
		attrs[i] = i
		vecs[i] = vector.NewVector(vec.Typ, capacity)
		_, err = vecs[i].AppendVector(vec, 0)
		if err != nil {
			return
		}
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}

func ApplyDeleteToVector(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	if deletes == nil || deletes.GetCardinality() == 0 {
		return vec
	}
	col := vec.Col
	deletesIterator := deletes.Iterator()
	nsp := &nulls.Nulls{}
	nsp.Np = &roaring64.Bitmap{}
	var nspIterator roaring64.IntPeekable64
	if vec.Nsp != nil && vec.Nsp.Np != nil {
		nspIterator = vec.Nsp.Np.Iterator()
	}
	deleted := 0
	switch vec.Typ.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
		vec.Col = common.InplaceDeleteRows(vec.Col, deletesIterator)
		deletesIterator = deletes.Iterator()
		for deletesIterator.HasNext() {
			row := deletesIterator.Next()
			if nspIterator != nil {
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						nsp.Np.Add(n - uint64(deleted))
					}
				}
			}
			deleted++
		}
		if nspIterator != nil {
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				nsp.Np.Add(n - uint64(deleted))
			}
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := col.(*types.Bytes)
		pre := -1
		for deletesIterator.HasNext() {
			row := deletesIterator.Next()
			currRow := row - uint32(deleted)
			if pre != -1 {
				if int(currRow) == len(data.Lengths)-1 {
					UpdateOffsets(data, pre-1, int(currRow))
				} else {
					UpdateOffsets(data, pre-1, int(currRow)+1)
				}
			}
			if int(currRow) == len(data.Lengths)-1 {
				data.Data = data.Data[:data.Offsets[currRow]]
				data.Lengths = data.Lengths[:currRow]
				data.Offsets = data.Offsets[:currRow]
			} else {
				data.Data = append(data.Data[:data.Offsets[currRow]], data.Data[data.Offsets[currRow+1]:]...)
				data.Lengths = append(data.Lengths[:currRow], data.Lengths[currRow+1:]...)
				data.Offsets = append(data.Offsets[:currRow], data.Offsets[currRow+1:]...)
			}
			if nspIterator != nil {
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						nsp.Np.Add(n - uint64(deleted))
					}
				}
			}
			deleted++
			pre = int(currRow)
		}
		if nspIterator != nil {
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				nsp.Np.Add(n - uint64(deleted))
			}
		}
		if pre != -1 {
			UpdateOffsets(data, pre-1, len(data.Lengths)-1)
		}
	}
	vec.Nsp = nsp
	return vec
}

func ApplyUpdateToVector(vec *gvec.Vector, mask *roaring.Bitmap, vals map[uint32]interface{}) *gvec.Vector {
	if mask == nil || mask.GetCardinality() == 0 {
		return vec
	}
	iterator := mask.Iterator()
	col := vec.Col
	switch vec.Typ.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
		for iterator.HasNext() {
			row := iterator.Next()
			SetFixSizeTypeValue(vec, row, vals[row])
			if vec.Nsp != nil && vec.Nsp.Np != nil {
				if vec.Nsp.Np.Contains(uint64(row)) {
					vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
				}
			}
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := col.(*types.Bytes)
		pre := -1
		for iterator.HasNext() {
			row := iterator.Next()
			if pre != -1 {
				UpdateOffsets(data, pre, int(row))
			}
			val := vals[row].([]byte)
			suffix := data.Data[data.Offsets[row]+data.Lengths[row]:]
			data.Lengths[row] = uint32(len(val))
			val = append(val, suffix...)
			data.Data = append(data.Data[:data.Offsets[row]], val...)
			pre = int(row)
			if vec.Nsp != nil && vec.Nsp.Np != nil {
				if vec.Nsp.Np.Contains(uint64(row)) {
					vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
				}
			}
		}
		if pre != -1 {
			UpdateOffsets(data, pre, len(data.Lengths)-1)
		}
	}
	return vec
}

func ApplyUpdateToIVector(vec vector.IVector, mask *roaring.Bitmap, vals map[uint32]interface{}) vector.IVector {
	if mask == nil || mask.GetCardinality() == 0 {
		return vec
	}
	updateIterator := mask.Iterator()
	switch vec.GetType() {
	case container.StdVec:
		if vec.IsReadonly() {
			vec = vec.(*vector.StdVector).Clone()
			vec.ResetReadonly()
		}
		for updateIterator.HasNext() {
			rowIdx := updateIterator.Next()
			err := vec.SetValue(int(rowIdx), vals[rowIdx])
			if err != nil {
				panic(err)
			}
			if vec.(*vector.StdVector).VMask != nil &&
				vec.(*vector.StdVector).VMask.Np != nil &&
				vec.(*vector.StdVector).VMask.Np.Contains(uint64(rowIdx)) {
				vec.(*vector.StdVector).VMask.Np.Flip(uint64(rowIdx), uint64(rowIdx))
			}
		}
	case container.StrVec:
		strVec := vec.(*vector.StrVector)
		data := strVec.Data
		pre := -1
		if vec.IsReadonly() {
			strVec2 := vector.NewEmptyStrVector()
			if strVec.VMask != nil && strVec.VMask.Np != nil {
				strVec2.VMask.Np = strVec.VMask.Np.Clone()
			}
			pos2 := 0
			strVec2.Data.Lengths = make([]uint32, len(data.Lengths))
			strVec2.Data.Offsets = make([]uint32, len(data.Offsets))
			for updateIterator.HasNext() {
				row := updateIterator.Next()
				val := vals[row].([]byte)

				preOffset := int(data.Offsets[pre+1])
				length := int(data.Offsets[row] - uint32(preOffset))
				strVec2.Data.Data = append(strVec2.Data.Data, make([]byte, length+len(val))...)
				copy(strVec2.Data.Data[pos2:pos2+length], data.Data[preOffset:preOffset+length])
				pos2 += length

				for i := pre + 1; i < int(row); i++ {
					strVec2.Data.Lengths[i] = data.Lengths[i]
					strVec2.Data.Offsets[i+1] = strVec2.Data.Offsets[i] + strVec2.Data.Lengths[i]
				}
				copy(strVec2.Data.Data[pos2:pos2+len(val)], val)
				pos2 += len(val)

				if strVec.VMask != nil && strVec.VMask.Np != nil && strVec.VMask.Np.Contains(uint64(row)) {
					strVec.VMask.Np.Flip(uint64(row), uint64(row))
				}

				strVec2.Data.Lengths[row] = uint32(len(val))
				if int(row) != len(data.Offsets)-1 {
					strVec2.Data.Offsets[row+1] = strVec2.Data.Offsets[row] + strVec2.Data.Lengths[row]
				}
				pre = int(row)
			}
			preOffset := int(data.Offsets[pre] + data.Lengths[pre])
			row := len(data.Offsets)
			length := int(len(data.Data) - preOffset)
			strVec2.Data.Data = append(strVec2.Data.Data, make([]byte, length)...)
			copy(strVec2.Data.Data[pos2:pos2+length], data.Data[preOffset:preOffset+length])

			for i := pre + 1; i < int(row); i++ {
				strVec2.Data.Lengths[i] = data.Lengths[i]
				if i != len(data.Offsets)-1 {
					strVec2.Data.Offsets[i+1] = strVec2.Data.Offsets[i] + strVec2.Data.Lengths[i]
				}
			}
			strVec2.StatMask = strVec.StatMask
			strVec2.Type = types.Type{
				Oid:   strVec.Type.Oid,
				Size:  strVec.Type.Size,
				Width: strVec.Type.Width,
			}
			vec = strVec2
			vec.ResetReadonly()
		} else {
			for updateIterator.HasNext() {
				row := updateIterator.Next()
				if pre != -1 {
					UpdateOffsets(data, pre, int(row))
				}
				val := vals[row].([]byte)
				suffix := data.Data[data.Offsets[row]+data.Lengths[row]:]
				data.Lengths[row] = uint32(len(val))
				val = append(val, suffix...)
				data.Data = append(data.Data[:data.Offsets[row]], val...)
				pre = int(row)
				if strVec.VMask != nil && strVec.VMask.Np != nil && strVec.VMask.Np.Contains(uint64(row)) {
					strVec.VMask.Np.Flip(uint64(row), uint64(row))
				}
			}
			if pre != -1 {
				UpdateOffsets(data, pre, len(data.Offsets)-1)
			}
		}
	default:
		panic("not support")
	}
	return vec
}

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

func ShuffleByDeletes(origMask *roaring.Bitmap, origVals map[uint32]interface{}, deletes *roaring.Bitmap) (*roaring.Bitmap, map[uint32]interface{}, *roaring.Bitmap) {
	if deletes == nil {
		return origMask, origVals, deletes
	}
	destDelets := roaring.New()
	ranges := make([]*deleteRange, 0, 10)
	deletesIt := deletes.Iterator()
	deletedCnt := uint32(0)
	for deletesIt.HasNext() {
		pos := deletesIt.Next()
		destDelets.Add(pos - deletedCnt)
		ranges = append(ranges, &deleteRange{pos: pos, deleted: deletedCnt})
		deletedCnt++
	}
	if origMask == nil || origMask.GetCardinality() == 0 {
		return origMask, origVals, destDelets
	}

	ranges = append(ranges, &deleteRange{pos: math.MaxUint32, deleted: deletedCnt})
	destMask := roaring.New()
	destVals := make(map[uint32]interface{})
	origIt := origMask.Iterator()
	for origIt.HasNext() {
		pos := origIt.Next()
		drange := findDeleteRange(pos, ranges)
		destMask.Add(pos - drange.deleted)
		destVals[pos-drange.deleted] = origVals[pos]
	}
	// for i, r := range ranges {
	// 	logutil.Infof("%d range.pos=%d,range.deleted=%d", i, r.pos, r.deleted)
	// }
	return destMask, destVals, destDelets
}
