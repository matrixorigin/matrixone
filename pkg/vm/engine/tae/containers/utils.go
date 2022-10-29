// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
)

func ApplyUpdates(vec Vector, mask *roaring.Bitmap, vals map[uint32]any) {
	it := mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		vec.Update(int(row), vals[row])
	}
}

func FillBufferWithBytes(bs *Bytes, buffer *bytes.Buffer) *Bytes {
	buffer.Reset()
	size := bs.Size()
	if buffer.Cap() < size {
		buffer.Grow(size)
	}
	nbs := stl.NewBytesWithTypeSize(bs.TypeSize)
	buf := buffer.Bytes()[:size]
	copy(buf, bs.StorageBuf())
	copy(buf[bs.StorageSize():], bs.HeaderBuf())

	nbs.SetStorageBuf(buf[:bs.StorageSize()])
	nbs.SetHeaderBuf(buf[bs.StorageSize():bs.Size()])
	return nbs
}

func CloneWithBuffer(src Vector, buffer *bytes.Buffer, allocator ...*mpool.MPool) (cloned Vector) {
	opts := new(Options)
	// XXX what does the following test mean?
	if len(allocator) > 0 {
		opts.Allocator = common.DefaultAllocator
	} else {
		opts.Allocator = src.GetAllocator()
	}
	cloned = MakeVector(src.GetType(), src.Nullable(), opts)
	bs := src.Bytes()
	var nulls *roaring64.Bitmap
	if src.HasNull() {
		nulls = src.NullMask().Clone()
	}
	nbs := FillBufferWithBytes(bs, buffer)
	cloned.ResetWithData(nbs, nulls)
	return
}

func UnmarshalToMoVec(vec Vector) (mov *movec.Vector) {
	bs := vec.Bytes()

	if vec.GetType().IsVarlen() {
		mov, _ = movec.BuildVarlenaVector(vec.GetType(), bs.Header, bs.Storage)
	} else {
		mov = movec.NewOriginalWithData(vec.GetType(), bs.StorageBuf(), &nulls.Nulls{})
	}
	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
	}
	mov.SetOriginal(true)

	return
}

func CopyToMoVec(vec Vector) (mov *movec.Vector) {
	bs := vec.Bytes()
	typ := vec.GetType()

	if vec.GetType().IsVarlen() {
		header := make([]types.Varlena, len(bs.Header))
		copy(header, bs.Header)
		storage := make([]byte, len(bs.Storage))
		if len(storage) > 0 {
			copy(storage, bs.Storage)
		}
		mov, _ = movec.BuildVarlenaVector(typ, header, storage)
	} else if vec.GetType().IsTuple() {
		mov = movec.NewOriginal(vec.GetType())
		cnt := types.DecodeInt32(bs.Storage)
		if cnt != 0 {
			if err := types.Decode(bs.Storage, &mov.Col); err != nil {
				panic(any(err))
			}
		}
	} else {
		data := make([]byte, len(bs.Storage))
		if len(data) > 0 {
			copy(data, bs.Storage)
		}
		mov = movec.NewOriginalWithData(vec.GetType(), data, new(nulls.Nulls))
	}

	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
		//mov.Nsp.Np = vec.NullMask()
	}

	return mov
}

// No copy
func UnmarshalToMoVecs(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = UnmarshalToMoVec(vecs[i])
	}
	return movecs
}

// Deep copy
func CopyToMoVecs(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVec(vecs[i])
	}
	return movecs
}

func CopyToMoBatch(bat *Batch) *batch.Batch {
	ret := batch.New(true, bat.Attrs)
	for i := range bat.Vecs {
		ret.Vecs[i] = CopyToMoVec(bat.Vecs[i])
	}
	return ret
}

func movecToBytes[T types.FixedSizeT](v *movec.Vector) *Bytes {
	bs := stl.NewFixedTypeBytes[T]()
	if v.Col == nil || len(movec.MustTCols[T](v)) == 0 {
		bs.Storage = make([]byte, v.Length()*v.GetType().TypeSize())
		logutil.Warn("[Moengine]", common.OperationField("movecToBytes"),
			common.OperandField("Col length is 0"))
	} else {
		bs.Storage = types.EncodeSlice(movec.MustTCols[T](v))
	}
	return bs
}

func NewVectorWithSharedMemory(v *movec.Vector, nullable bool) Vector {
	vec := MakeVector(v.Typ, nullable)
	var bs *Bytes

	switch v.Typ.Oid {
	case types.T_bool:
		bs = movecToBytes[bool](v)
	case types.T_int8:
		bs = movecToBytes[int8](v)
	case types.T_int16:
		bs = movecToBytes[int16](v)
	case types.T_int32:
		bs = movecToBytes[int32](v)
	case types.T_int64:
		bs = movecToBytes[int64](v)
	case types.T_uint8:
		bs = movecToBytes[uint8](v)
	case types.T_uint16:
		bs = movecToBytes[uint16](v)
	case types.T_uint32:
		bs = movecToBytes[uint32](v)
	case types.T_uint64:
		bs = movecToBytes[uint64](v)
	case types.T_float32:
		bs = movecToBytes[float32](v)
	case types.T_float64:
		bs = movecToBytes[float64](v)
	case types.T_date:
		bs = movecToBytes[types.Date](v)
	case types.T_time:
		bs = movecToBytes[types.Time](v)
	case types.T_datetime:
		bs = movecToBytes[types.Datetime](v)
	case types.T_timestamp:
		bs = movecToBytes[types.Timestamp](v)
	case types.T_decimal64:
		bs = movecToBytes[types.Decimal64](v)
	case types.T_decimal128:
		bs = movecToBytes[types.Decimal128](v)
	case types.T_uuid:
		bs = movecToBytes[types.Uuid](v)
	case types.T_TS:
		bs = movecToBytes[types.TS](v)
	case types.T_Rowid:
		bs = movecToBytes[types.Rowid](v)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		bs = stl.NewBytesWithTypeSize(-types.VarlenaSize)
		if v.Col != nil {
			bs.Header, bs.Storage = movec.MustVarlenaRawData(v)
		}
	default:
		panic(any(moerr.NewInternalError("%s not supported", v.Typ.String())))
	}
	var np *roaring64.Bitmap
	if v.Nsp.Np != nil {
		np = roaring64.New()
		np.AddMany(v.Nsp.Np.ToArray())
		logutil.Debugf("sie : %d", np.GetCardinality())
	}
	vec.ResetWithData(bs, np)
	return vec
}

func SplitBatch(bat *batch.Batch, cnt int) []*batch.Batch {
	if cnt == 1 {
		return []*batch.Batch{bat}
	}
	length := movec.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*batch.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := batch.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := movec.New(bat.Vecs[j].Typ)
				movec.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
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
			if left >= rows && i < cnt-1 {
				rowArray = append(rowArray, rows)
			} else {
				rowArray = append(rowArray, left)
			}
			left -= rows
		}
	}
	start := 0
	bats := make([]*batch.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := batch.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := movec.New(bat.Vecs[j].Typ)
			movec.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

var mockMp = common.DefaultAllocator

func MockVec(typ types.Type, rows int, offset int) *movec.Vector {
	vec := movec.New(typ)
	switch typ.Oid {
	case types.T_bool:
		data := make([]bool, 0)
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				data = append(data, true)
			} else {
				data = append(data, false)
			}
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_int8:
		data := make([]int8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int8(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_int16:
		data := make([]int16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int16(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_int32:
		data := make([]int32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int32(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_int64:
		data := make([]int64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int64(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_uint8:
		data := make([]uint8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint8(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_uint16:
		data := make([]uint16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint16(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_uint32:
		data := make([]uint32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint32(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_uint64:
		data := make([]uint64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint64(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_float32:
		data := make([]float32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float32(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_float64:
		data := make([]float64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float64(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_decimal64:
		data := make([]types.Decimal64, 0)
		for i := 0; i < rows; i++ {
			d, _ := types.InitDecimal64(int64(i+offset), 64, 0)
			data = append(data, d)
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_decimal128:
		data := make([]types.Decimal128, 0)
		for i := 0; i < rows; i++ {
			d, _ := types.InitDecimal128(int64(i+offset), 64, 0)
			data = append(data, d)
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_timestamp:
		data := make([]types.Timestamp, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Timestamp(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_date:
		data := make([]types.Date, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Date(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_time:
		data := make([]types.Time, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Time(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_datetime:
		data := make([]types.Datetime, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Datetime(i+offset))
		}
		_ = movec.AppendFixed(vec, data, mockMp)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		data := make([][]byte, 0)
		for i := 0; i < rows; i++ {
			data = append(data, []byte(strconv.Itoa(i+offset)))
		}
		_ = movec.AppendBytes(vec, data, mockMp)
	case types.T_TS:
		data := make([]types.TS, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.BuildTS(int64(i+1), uint32(i%16)))
		}
		_ = movec.AppendFixed(vec, data, mockMp)

	case types.T_Rowid:
		data := make([]types.Rowid, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.BuildRowid(int64(i+1), int64(i%16)))
		}
		_ = movec.AppendFixed(vec, data, mockMp)

	default:
		panic("not support")
	}
	return vec
}

func GenericUpdateFixedValue[T types.FixedSizeT](vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		nulls.Add(vec.Nsp, uint64(row))
	} else {
		movec.SetTAt(vec, int(row), v.(T))
		if vec.Nsp.Np != nil && vec.Nsp.Np.Contains(uint64(row)) {
			vec.Nsp.Np.Remove(uint64(row))
		}
	}
}

func GenericUpdateBytes(vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		nulls.Add(vec.Nsp, uint64(row))
	} else {
		movec.SetBytesAt(vec, int(row), v.([]byte), mockMp)
		if vec.Nsp.Np != nil && vec.Nsp.Np.Contains(uint64(row)) {
			vec.Nsp.Np.Remove(uint64(row))
		}
	}
}

func AppendFixedValue[T types.FixedSizeT](vec *movec.Vector, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		zt := types.DefaultVal[T]()
		vec.Append(zt, isNull, mockMp)
	} else {
		vec.Append(v.(T), false, mockMp)
	}
}

func AppendBytes(vec *movec.Vector, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		vec.Append(nil, true, mockMp)
	} else {
		vec.Append(v.([]byte), false, mockMp)
	}
}

func AppendValue(vec *movec.Vector, v any) {
	switch vec.Typ.Oid {
	case types.T_bool:
		AppendFixedValue[bool](vec, v)
	case types.T_int8:
		AppendFixedValue[int8](vec, v)
	case types.T_int16:
		AppendFixedValue[int16](vec, v)
	case types.T_int32:
		AppendFixedValue[int32](vec, v)
	case types.T_int64:
		AppendFixedValue[int64](vec, v)
	case types.T_uint8:
		AppendFixedValue[uint8](vec, v)
	case types.T_uint16:
		AppendFixedValue[uint16](vec, v)
	case types.T_uint32:
		AppendFixedValue[uint32](vec, v)
	case types.T_uint64:
		AppendFixedValue[uint64](vec, v)
	case types.T_decimal64:
		AppendFixedValue[types.Decimal64](vec, v)
	case types.T_decimal128:
		AppendFixedValue[types.Decimal128](vec, v)
	case types.T_float32:
		AppendFixedValue[float32](vec, v)
	case types.T_float64:
		AppendFixedValue[float64](vec, v)
	case types.T_date:
		AppendFixedValue[types.Date](vec, v)
	case types.T_time:
		AppendFixedValue[types.Time](vec, v)
	case types.T_timestamp:
		AppendFixedValue[types.Timestamp](vec, v)
	case types.T_datetime:
		AppendFixedValue[types.Datetime](vec, v)
	case types.T_uuid:
		AppendFixedValue[types.Uuid](vec, v)
	case types.T_TS:
		AppendFixedValue[types.TS](vec, v)
	case types.T_Rowid:
		AppendFixedValue[types.Rowid](vec, v)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		AppendBytes(vec, v)
	default:
		panic(any("not expected"))
	}
}

func GetValue(col *movec.Vector, row uint32) any {
	if col.Nsp.Np != nil && col.Nsp.Np.Contains(uint64(row)) {
		return types.Null{}
	}
	switch col.Typ.Oid {
	case types.T_bool:
		return movec.GetValueAt[bool](col, int64(row))
	case types.T_int8:
		return movec.GetValueAt[int8](col, int64(row))
	case types.T_int16:
		return movec.GetValueAt[int16](col, int64(row))
	case types.T_int32:
		return movec.GetValueAt[int32](col, int64(row))
	case types.T_int64:
		return movec.GetValueAt[int64](col, int64(row))
	case types.T_uint8:
		return movec.GetValueAt[uint8](col, int64(row))
	case types.T_uint16:
		return movec.GetValueAt[uint16](col, int64(row))
	case types.T_uint32:
		return movec.GetValueAt[uint32](col, int64(row))
	case types.T_uint64:
		return movec.GetValueAt[uint64](col, int64(row))
	case types.T_decimal64:
		return movec.GetValueAt[types.Decimal64](col, int64(row))
	case types.T_decimal128:
		return movec.GetValueAt[types.Decimal128](col, int64(row))
	case types.T_uuid:
		return movec.GetValueAt[types.Uuid](col, int64(row))
	case types.T_float32:
		return movec.GetValueAt[float32](col, int64(row))
	case types.T_float64:
		return movec.GetValueAt[float64](col, int64(row))
	case types.T_date:
		return movec.GetValueAt[types.Date](col, int64(row))
	case types.T_time:
		return movec.GetValueAt[types.Time](col, int64(row))
	case types.T_datetime:
		return movec.GetValueAt[types.Datetime](col, int64(row))
	case types.T_timestamp:
		return movec.GetValueAt[types.Timestamp](col, int64(row))
	case types.T_TS:
		return movec.GetValueAt[types.TS](col, int64(row))
	case types.T_Rowid:
		return movec.GetValueAt[types.Rowid](col, int64(row))
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		return col.GetBytes(int64(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

func UpdateValue(col *movec.Vector, row uint32, val any) {
	switch col.Typ.Oid {
	case types.T_bool:
		GenericUpdateFixedValue[bool](col, row, val)
	case types.T_int8:
		GenericUpdateFixedValue[int8](col, row, val)
	case types.T_int16:
		GenericUpdateFixedValue[int16](col, row, val)
	case types.T_int32:
		GenericUpdateFixedValue[int32](col, row, val)
	case types.T_int64:
		GenericUpdateFixedValue[int64](col, row, val)
	case types.T_uint8:
		GenericUpdateFixedValue[uint8](col, row, val)
	case types.T_uint16:
		GenericUpdateFixedValue[uint16](col, row, val)
	case types.T_uint32:
		GenericUpdateFixedValue[uint32](col, row, val)
	case types.T_uint64:
		GenericUpdateFixedValue[uint64](col, row, val)
	case types.T_decimal64:
		GenericUpdateFixedValue[types.Decimal64](col, row, val)
	case types.T_decimal128:
		GenericUpdateFixedValue[types.Decimal128](col, row, val)
	case types.T_float32:
		GenericUpdateFixedValue[float32](col, row, val)
	case types.T_float64:
		GenericUpdateFixedValue[float64](col, row, val)
	case types.T_date:
		GenericUpdateFixedValue[types.Date](col, row, val)
	case types.T_time:
		GenericUpdateFixedValue[types.Time](col, row, val)
	case types.T_datetime:
		GenericUpdateFixedValue[types.Datetime](col, row, val)
	case types.T_timestamp:
		GenericUpdateFixedValue[types.Timestamp](col, row, val)
	case types.T_uuid:
		GenericUpdateFixedValue[types.Uuid](col, row, val)
	case types.T_TS:
		GenericUpdateFixedValue[types.TS](col, row, val)
	case types.T_Rowid:
		GenericUpdateFixedValue[types.Rowid](col, row, val)

	case types.T_varchar, types.T_char, types.T_json, types.T_blob, types.T_text:
		GenericUpdateBytes(col, row, val)
	default:
		panic(moerr.NewInternalError("%v not supported", col.Typ))
	}
}

func ForEachValue(col *movec.Vector, reversed bool, op func(v any, row uint32) error) (err error) {
	if reversed {
		for i := movec.Length(col) - 1; i >= 0; i-- {
			v := GetValue(col, uint32(i))
			if err = op(v, uint32(i)); err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < movec.Length(col); i++ {
		v := GetValue(col, uint32(i))
		if err = op(v, uint32(i)); err != nil {
			return
		}
	}
	return
}

func BatchWindow(bat *batch.Batch, start, end int) *batch.Batch {
	window := batch.New(true, bat.Attrs)
	window.Vecs = make([]*movec.Vector, len(bat.Vecs))
	for i := range window.Vecs {
		vec := movec.New(bat.Vecs[i].Typ)
		movec.Window(bat.Vecs[i], start, end, vec)
		window.Vecs[i] = vec
	}
	return window
}

/*func ApplyDeleteToVector(vec *movec.Vector, deletes *roaring.Bitmap) *movec.Vector {
	if deletes == nil || deletes.IsEmpty() {
		return vec
	}
	deletesIterator := deletes.Iterator()

	np := bitmap.New(0)
	var nspIterator bitmap.Iterator
	if vec.Nsp != nil && vec.Nsp.Np != nil {
		nspIterator = vec.Nsp.Np.Iterator()
	}
	deleted := 0
	vec.Col = compute.InplaceDeleteRows(vec.Col, deletesIterator)
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
					np.Add(n - uint64(deleted))
				}
			}
		}
		deleted++
	}
	if nspIterator != nil {
		for nspIterator.HasNext() {
			n := nspIterator.Next()
			np.Add(n - uint64(deleted))
		}
	}
	vec.Nsp.Np = np
	return vec
}*/

func ApplyUpdateToVector(vec *movec.Vector, mask *roaring.Bitmap, vals map[uint32]any) *movec.Vector {
	if mask == nil || mask.IsEmpty() {
		return vec
	}
	iterator := mask.Iterator()
	for iterator.HasNext() {
		row := iterator.Next()
		UpdateValue(vec, row, vals[row])
	}
	return vec
}

func CopyToProtoBatch(bat *Batch) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		apiVec, err := CopyToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs = append(rbat.Vecs, apiVec)
	}
	return rbat, nil
}

func CopyToProtoVector(vec Vector) (*api.Vector, error) {
	vecNsp := new(nulls.Nulls)
	if vec.HasNull() {
		vecNsp.Np = bitmap.New(vec.Length())
		vecNsp.Np.AddMany(vec.NullMask().ToArray())
	}
	nsp, err := vecNsp.Show()
	if err != nil {
		return nil, err
	}
	bs := vec.Bytes()
	data := make([]byte, bs.StorageSize())
	copy(data, bs.StorageBuf())
	area := make([]byte, bs.HeaderSize())
	copy(area, bs.HeaderBuf())
	return &api.Vector{
		Nsp:      nsp,
		Nullable: true,
		Area:     area,
		Data:     data,
		IsConst:  false,
		Len:      uint32(vec.Length()),
		Type:     movec.TypeToProtoType(vec.GetType()),
	}, nil
}
