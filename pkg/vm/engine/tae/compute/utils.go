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

package compute

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func ForeachApply[T types.FixedSizeT](vs any, offset, length uint32, sels []uint32, op func(any, uint32) error) (err error) {
	vals := vs.([]T)
	vals = vals[offset : offset+length]
	if len(sels) == 0 {
		for i, v := range vals {
			if err = op(v, uint32(i)); err != nil {
				return
			}
		}
	} else {
		for _, idx := range sels {
			v := vals[idx]
			if err = op(v, idx); err != nil {
				return
			}
		}
	}
	return
}

func ApplyOpToColumn(vec *vector.Vector, op func(v any, pos uint32) error, selmask *roaring.Bitmap) error {
	return ApplyOpToColumnWithOffset(vec, 0, uint32(vector.Length(vec)), op, selmask)
}

func ApplyOpToColumnWithOffset(vec *vector.Vector, offset, length uint32, op func(v any, pos uint32) error, selmask *roaring.Bitmap) error {
	var sels []uint32
	if selmask != nil {
		sels = selmask.ToArray()
	}
	switch vec.Typ.Oid {
	case types.Type_BOOL:
		return ForeachApply[bool](vec.Col, offset, length, sels, op)
	case types.Type_INT8:
		return ForeachApply[int8](vec.Col, offset, length, sels, op)
	case types.Type_INT16:
		return ForeachApply[int16](vec.Col, offset, length, sels, op)
	case types.Type_INT32:
		return ForeachApply[int32](vec.Col, offset, length, sels, op)
	case types.Type_INT64:
		return ForeachApply[int64](vec.Col, offset, length, sels, op)
	case types.Type_UINT8:
		return ForeachApply[uint8](vec.Col, offset, length, sels, op)
	case types.Type_UINT16:
		return ForeachApply[uint16](vec.Col, offset, length, sels, op)
	case types.Type_UINT32:
		return ForeachApply[uint32](vec.Col, offset, length, sels, op)
	case types.Type_UINT64:
		return ForeachApply[uint64](vec.Col, offset, length, sels, op)
	case types.Type_DECIMAL64:
		return ForeachApply[types.Decimal64](vec.Col, offset, length, sels, op)
	case types.Type_DECIMAL128:
		return ForeachApply[types.Decimal128](vec.Col, offset, length, sels, op)
	case types.Type_FLOAT32:
		return ForeachApply[float32](vec.Col, offset, length, sels, op)
	case types.Type_FLOAT64:
		return ForeachApply[float64](vec.Col, offset, length, sels, op)
	case types.Type_TIMESTAMP:
		return ForeachApply[types.Timestamp](vec.Col, offset, length, sels, op)
	case types.Type_DATE:
		return ForeachApply[types.Date](vec.Col, offset, length, sels, op)
	case types.Type_DATETIME:
		return ForeachApply[types.Datetime](vec.Col, offset, length, sels, op)
	case types.Type_CHAR, types.Type_VARCHAR:
		vs := vec.Col.(*types.Bytes)
		if selmask == nil {
			for i := range vs.Offsets[offset:] {
				v := vs.Get(int64(i))
				if err := op(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range sels[offset:] {
				v := vs.Get(int64(idx))
				if err := op(v, idx); err != nil {
					return err
				}
			}
		}
	default:
		panic("unsupported type")
	}
	return nil
}

func MOToVector(v *gvec.Vector, nullable bool) containers.Vector {
	vec := containers.MakeVector(v.Typ, nullable)
	bs := containers.NewBytes()
	switch v.Typ.Oid {
	case types.Type_BOOL:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]bool), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT8:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int8), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT16:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int16), 2)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_INT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]int64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT8:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint8), 1)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT16:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint16), 2)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_UINT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]uint64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_FLOAT32:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]float32), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_FLOAT64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]float64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DATE:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Date), 4)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DATETIME:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_TIMESTAMP:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DECIMAL64:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_DECIMAL128:
		bs.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
		vec.ResetWithData(bs, v.Nsp.Np)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vbs := v.Col.(*types.Bytes)
		bs.Data = vbs.Data
		bs.Offset = vbs.Offsets
		bs.Length = vbs.Lengths
	default:
		panic(fmt.Errorf("%s not supported", v.Typ.String()))
	}
	return vec
}

func CopyToMoVector(vec containers.Vector) *gvec.Vector {
	mov := gvec.New(vec.GetType())
	w := new(bytes.Buffer)
	_, _ = w.Write(types.EncodeType(vec.GetType()))
	if vec.HasNull() {
		var nullBuf []byte
		nullBuf, _ = vec.NullMask().ToBytes()
		_, _ = w.Write(types.EncodeFixed(uint32(len(nullBuf))))
		_, _ = w.Write(nullBuf)
	} else {
		_, _ = w.Write(types.EncodeFixed(uint32(0)))
	}
	switch vec.GetType().Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		_, _ = w.Write(types.EncodeFixed(uint32(vec.Length())))
		if vec.Length() > 0 {
			bs := vec.Bytes()
			_, _ = w.Write(bs.LengthBuf())
			_, _ = w.Write(bs.DataBuf())
		}
	default:
		bs := vec.Data()
		_, _ = w.Write(bs)
	}
	if err := mov.Read(w.Bytes()); err != nil {
		panic(err)
	}
	return mov
}

func CopyToMoVectors(vecs []containers.Vector) []*gvec.Vector {
	movecs := make([]*gvec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}

func MOToVectors(movecs []*gvec.Vector, nullables []bool) []containers.Vector {
	vecs := make([]containers.Vector, len(movecs))
	for i := range movecs {
		vecs[i] = MOToVector(movecs[i], nullables[i])
	}
	return vecs
}
