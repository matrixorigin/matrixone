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
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	offBuf := bs.OffsetBuf()
	lenBuf := bs.LengthBuf()
	dataBuf := bs.Data
	size := len(offBuf) + len(lenBuf) + len(dataBuf)
	if buffer.Cap() < size {
		buffer.Grow(size)
	}
	nbs := NewBytes()
	buf := buffer.Bytes()[:size]
	copy(buf, dataBuf)
	nbs.Data = buf[:len(dataBuf)]
	if len(offBuf) == 0 {
		return nbs
	}
	copy(buf[len(dataBuf):], offBuf)
	copy(buf[len(dataBuf)+len(offBuf):], lenBuf)
	nbs.SetOffsetBuf(buf[len(dataBuf) : len(dataBuf)+len(offBuf)])
	nbs.SetLengthBuf(buf[len(dataBuf)+len(offBuf) : size])
	return nbs
}

func CloneWithBuffer(src Vector, buffer *bytes.Buffer, allocator ...MemAllocator) (cloned Vector) {
	opts := new(Options)
	if len(allocator) > 0 {
		opts.Allocator = DefaultAllocator
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

func CopyToMoVector(vec Vector) *movec.Vector {
	return VectorsToMO(vec)
}

// XXX VectorsToMo and CopyToMoVector.   The old impl. will move
// vec.Data to movec.Data and keeps on sharing.   This is way too
// fragile and error prone.
//
// Not just copy it.   Until profiler says I need to work harder.
func VectorsToMO(vec Vector) *movec.Vector {
	mov := movec.NewOriginal(vec.GetType())
	data := vec.Data()
	typ := vec.GetType()
	mov.Typ = typ
	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
		//mov.Nsp.Np = vec.NullMask()
	}

	if vec.GetType().IsVarlen() {
		bs := vec.Bytes()
		nbs := len(bs.Offset)
		bsv := make([][]byte, nbs)
		for i := 0; i < nbs; i++ {
			bsv[i] = bs.Data[bs.Offset[i] : bs.Offset[i]+bs.Length[i]]
		}
		movec.AppendBytes(mov, bsv, nil)
	} else if vec.GetType().IsTuple() {
		cnt := types.DecodeInt32(data)
		if cnt != 0 {
			if err := types.Decode(data, &mov.Col); err != nil {
				panic(any(err))
			}
		}
	} else {
		movec.AppendFixedRaw(mov, data)
	}

	return mov
}

func CopyToMoVectors(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}

func MOToTAEVector(v *movec.Vector, nullable bool) Vector {
	vec := MakeVector(v.Typ, nullable)
	bs := NewBytes()
	switch v.Typ.Oid {
	case types.T_bool:
		if v.Col == nil || len(v.Col.([]bool)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]bool), 1)
		}
	case types.T_int8:
		if v.Col == nil || len(v.Col.([]int8)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int8), 1)
		}
	case types.T_int16:
		if v.Col == nil || len(v.Col.([]int16)) == 0 {
			bs.Data = make([]byte, v.Length()*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int16), 2)
		}
	case types.T_int32:
		if v.Col == nil || len(v.Col.([]int32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int32), 4)
		}
	case types.T_int64:
		if v.Col == nil || len(v.Col.([]int64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int64), 8)
		}
	case types.T_uint8:
		if v.Col == nil || len(v.Col.([]uint8)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint8), 1)
		}
	case types.T_uint16:
		if v.Col == nil || len(v.Col.([]uint16)) == 0 {
			bs.Data = make([]byte, v.Length()*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint16), 2)
		}
	case types.T_uint32:
		if v.Col == nil || len(v.Col.([]uint32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint32), 4)
		}
	case types.T_uint64:
		if v.Col == nil || len(v.Col.([]uint64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint64), 8)
		}
	case types.T_float32:
		if v.Col == nil || len(v.Col.([]float32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]float32), 4)
		}
	case types.T_float64:
		if v.Col == nil || len(v.Col.([]float64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]float64), 8)
		}
	case types.T_date:
		if v.Col == nil || len(v.Col.([]types.Date)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Date), 4)
		}
	case types.T_datetime:
		if v.Col == nil || len(v.Col.([]types.Datetime)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
		}
	case types.T_timestamp:
		if v.Col == nil || len(v.Col.([]types.Timestamp)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
		}
	case types.T_decimal64:
		if v.Col == nil || len(v.Col.([]types.Decimal64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
		}
	case types.T_decimal128:
		if v.Col == nil || len(v.Col.([]types.Decimal128)) == 0 {
			bs.Data = make([]byte, v.Length()*16)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
		}
	case types.T_uuid:
		if v.Col == nil || len(v.Col.([]types.Uuid)) == 0 {
			bs.Data = make([]byte, v.Length()*16)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Uuid), 16)
		}
	case types.T_TS:
		if v.Col == nil || len(v.Col.([]types.TS)) == 0 {
			bs.Data = make([]byte, v.Length()*types.TxnTsSize)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.TS), types.TxnTsSize)
		}
	case types.T_Rowid:
		if v.Col == nil || len(v.Col.([]types.Rowid)) == 0 {
			bs.Data = make([]byte, v.Length()*types.RowidSize)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Rowid), types.RowidSize)
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		if v.Col == nil {
			bs.Data = make([]byte, 0)
		} else {
			vbs := movec.GetBytesVectorValues(v)
			for _, v := range vbs {
				bs.Append(v)
			}
		}
	default:
		panic(any(fmt.Errorf("%s not supported", v.Typ.String())))
	}
	if v.Nsp.Np != nil {
		np := &roaring64.Bitmap{}
		np.AddMany(v.Nsp.Np.ToArray())
		logutil.Infof("sie : %d", np.GetCardinality())
		vec.ResetWithData(bs, np)
		return vec
	}
	vec.ResetWithData(bs, nil)
	return vec
}

func MOToTAEBatch(bat *batch.Batch, allNullables []bool) *Batch {
	//allNullables := schema.AllNullables()
	taeBatch := NewEmptyBatch()
	defer taeBatch.Close()
	for i, vec := range bat.Vecs {
		v := MOToTAEVector(vec, allNullables[i])
		taeBatch.AddVector(bat.Attrs[i], v)
	}
	return taeBatch
}
