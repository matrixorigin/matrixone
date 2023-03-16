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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

// ### Shallow copy Functions

func CloneWithBuffer(src Vector, buffer *bytes.Buffer, allocator ...*mpool.MPool) (cloned Vector) {
	opts := Options{}
	// XXX what does the following test mean?
	if len(allocator) > 0 {
		opts.Allocator = common.DefaultAllocator
	} else {
		opts.Allocator = src.GetAllocator()
	}
	cloned = MakeVector(src.GetType(), src.Nullable(), opts)
	bs := src.Bytes()
	var nulls *cnNulls.Nulls
	if src.HasNull() {
		nulls = src.NullMask().Clone()
	}
	nbs := FillBufferWithBytes(bs, buffer)
	cloned.ResetWithData(nbs, nulls)
	return
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

func UnmarshalToMoVec(vec Vector) *movec.Vector {
	bs := vec.Bytes()

	mov, _ := movec.FromDNVector(vec.GetType(), bs.Header, bs.Storage, true)
	cnNulls.Add(mov.GetNulls(), vec.NullMask().ToArray()...)

	//mov.SetOriginal(true)

	return mov
}

func UnmarshalToMoVecs(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = UnmarshalToMoVec(vecs[i])
	}
	return movecs
}

func NewVectorWithSharedMemory(v *movec.Vector, nullable bool) Vector {
	vec := MakeVector(*v.GetType(), nullable)
	vec.SetDownstreamVector(v)
	return vec
}

func NewNonNullBatchWithSharedMemory(b *batch.Batch) *Batch {
	bat := NewBatch()
	for i, attr := range b.Attrs {
		v := NewVectorWithSharedMemory(b.Vecs[i], false)
		bat.AddVector(attr, v)
	}
	return bat
}

// ### Deep copy Functions

func CopyToMoVec(vec Vector) (mov *movec.Vector) {
	bs := vec.Bytes()
	typ := vec.GetType()

	if vec.GetType().IsVarlen() {
		var header []types.Varlena
		if bs.AsWindow {
			header = make([]types.Varlena, bs.WinLength)
			copy(header, bs.Header[bs.WinOffset:bs.WinOffset+bs.WinLength])
		} else {
			header = make([]types.Varlena, len(bs.Header))
			copy(header, bs.Header)
		}
		storage := make([]byte, len(bs.Storage))
		if len(storage) > 0 {
			copy(storage, bs.Storage)
		}
		mov, _ = movec.FromDNVector(typ, header, storage, true)
		//} else if vec.GetType().IsTuple() {
		//	mov = movec.NewVector(vec.GetType())
		//	cnt := types.DecodeInt32(bs.Storage)
		//	if cnt != 0 {
		//		if err := types.Decode(bs.Storage, &mov.Col); err != nil {
		//			panic(any(err))
		//		}
		//	}
	} else {
		data := make([]byte, len(bs.Storage))
		if len(data) > 0 {
			copy(data, bs.Storage)
		}
		mov, _ = movec.FromDNVector(typ, nil, data, true)
	}

	if vec.HasNull() {
		cnNulls.Add(mov.GetNulls(), vec.NullMask().ToArray()...)
		//mov.GetNulls().Np = vec.NullMask()
	}

	return mov
}

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

// ### Bytes Functions

func movecToBytes[T types.FixedSizeT](v *movec.Vector) *Bytes {
	bs := stl.NewFixedTypeBytes[T]()
	if v.Length() == 0 {
		bs.Storage = make([]byte, v.Length()*v.GetType().TypeSize())
	} else {
		bs.Storage = types.EncodeSlice(movec.MustFixedCol[T](v))
	}
	return bs
}

func MoVecToBytes(v *movec.Vector) *Bytes {
	var bs *Bytes

	switch v.GetType().Oid {
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
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		bs = stl.NewBytesWithTypeSize(-types.VarlenaSize)
		if v.Length() > 0 {
			bs.Header, bs.Storage = movec.MustVarlenaRawData(v)
		}
	default:
		panic(any(moerr.NewInternalErrorNoCtx("%s not supported", v.GetType().String())))
	}
	return bs
}

// ### Get Functions

func GetValue(col *movec.Vector, row uint32) any {
	if col.GetNulls().Np != nil && col.GetNulls().Np.Contains(uint64(row)) {
		return types.Null{}
	}
	return getNonNullValue(col, row)
}

// getNonNullValue Please don't merge it with GetValue(). Used in Vector for getting NonNullValue.
func getNonNullValue(col *movec.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return movec.GetFixedAt[bool](col, int(row))
	case types.T_int8:
		return movec.GetFixedAt[int8](col, int(row))
	case types.T_int16:
		return movec.GetFixedAt[int16](col, int(row))
	case types.T_int32:
		return movec.GetFixedAt[int32](col, int(row))
	case types.T_int64:
		return movec.GetFixedAt[int64](col, int(row))
	case types.T_uint8:
		return movec.GetFixedAt[uint8](col, int(row))
	case types.T_uint16:
		return movec.GetFixedAt[uint16](col, int(row))
	case types.T_uint32:
		return movec.GetFixedAt[uint32](col, int(row))
	case types.T_uint64:
		return movec.GetFixedAt[uint64](col, int(row))
	case types.T_decimal64:
		return movec.GetFixedAt[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return movec.GetFixedAt[types.Decimal128](col, int(row))
	case types.T_uuid:
		return movec.GetFixedAt[types.Uuid](col, int(row))
	case types.T_float32:
		return movec.GetFixedAt[float32](col, int(row))
	case types.T_float64:
		return movec.GetFixedAt[float64](col, int(row))
	case types.T_date:
		return movec.GetFixedAt[types.Date](col, int(row))
	case types.T_time:
		return movec.GetFixedAt[types.Time](col, int(row))
	case types.T_datetime:
		return movec.GetFixedAt[types.Datetime](col, int(row))
	case types.T_timestamp:
		return movec.GetFixedAt[types.Timestamp](col, int(row))
	case types.T_TS:
		return movec.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return movec.GetFixedAt[types.Rowid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

// ### Update Function

var mockMp = common.DefaultAllocator

func GenericUpdateFixedValue[T types.FixedSizeT](vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		cnNulls.Add(vec.GetNulls(), uint64(row))
	} else {
		movec.SetFixedAt(vec, int(row), v.(T))
		if vec.GetNulls().Np != nil && vec.GetNulls().Np.Contains(uint64(row)) {
			vec.GetNulls().Np.Remove(uint64(row))
		}
	}
}

func GenericUpdateBytes(vec *movec.Vector, row uint32, v any) {
	_, isNull := v.(types.Null)
	if isNull {
		cnNulls.Add(vec.GetNulls(), uint64(row))
	} else {
		movec.SetBytesAt(vec, int(row), v.([]byte), mockMp)
		if vec.GetNulls().Np != nil && vec.GetNulls().Np.Contains(uint64(row)) {
			vec.GetNulls().Np.Remove(uint64(row))
		}
	}
}

func UpdateValue(col *movec.Vector, row uint32, val any) {
	switch col.GetType().Oid {
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

	case types.T_varchar, types.T_char, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		GenericUpdateBytes(col, row, val)
	default:
		panic(moerr.NewInternalErrorNoCtx("%v not supported", col.GetType()))
	}
}

// ### Misc Functions

func SplitBatch(bat *batch.Batch, cnt int) []*batch.Batch {
	if cnt == 1 {
		return []*batch.Batch{bat}
	}
	length := bat.Vecs[0].Length()
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*batch.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := batch.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := movec.NewVec(*bat.Vecs[j].GetType())
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
			window := movec.NewVec(*bat.Vecs[j].GetType())
			movec.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}
