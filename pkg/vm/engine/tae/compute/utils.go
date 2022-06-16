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
	"github.com/RoaringBitmap/roaring"
	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func Hash(v any, typ types.Type) (uint64, error) {
	data := EncodeKey(v, typ)
	//murmur := murmur3.Sum64(data)
	xx := xxhash.Sum64(data)
	return xx, nil
}

func DecodeKey(key []byte, typ types.Type) any {
	switch typ.Oid {
	case types.Type_BOOL:
		return encoding.DecodeBool(key)
	case types.Type_INT8:
		return encoding.DecodeInt8(key)
	case types.Type_INT16:
		return encoding.DecodeInt16(key)
	case types.Type_INT32:
		return encoding.DecodeInt32(key)
	case types.Type_INT64:
		return encoding.DecodeInt64(key)
	case types.Type_UINT8:
		return encoding.DecodeUint8(key)
	case types.Type_UINT16:
		return encoding.DecodeUint16(key)
	case types.Type_UINT32:
		return encoding.DecodeUint32(key)
	case types.Type_UINT64:
		return encoding.DecodeUint64(key)
	case types.Type_FLOAT32:
		return encoding.DecodeFloat32(key)
	case types.Type_FLOAT64:
		return encoding.DecodeFloat64(key)
	case types.Type_DATE:
		return encoding.DecodeDate(key)
	case types.Type_DATETIME:
		return encoding.DecodeDatetime(key)
	case types.Type_TIMESTAMP:
		return encoding.DecodeTimestamp(key)
	case types.Type_DECIMAL64:
		return encoding.DecodeDecimal64(key)
	case types.Type_DECIMAL128:
		return encoding.DecodeDecimal128(key)
	case types.Type_CHAR, types.Type_VARCHAR:
		return key
	default:
		panic("unsupported type")
	}
}

func EncodeKey(key any, typ types.Type) []byte {
	switch typ.Oid {
	case types.Type_BOOL:
		return encoding.EncodeBool(key.(bool))
	case types.Type_INT8:
		return encoding.EncodeInt8(key.(int8))
	case types.Type_INT16:
		return encoding.EncodeInt16(key.(int16))
	case types.Type_INT32:
		return encoding.EncodeInt32(key.(int32))
	case types.Type_INT64:
		return encoding.EncodeInt64(key.(int64))
	case types.Type_UINT8:
		return encoding.EncodeUint8(key.(uint8))
	case types.Type_UINT16:
		return encoding.EncodeUint16(key.(uint16))
	case types.Type_UINT32:
		return encoding.EncodeUint32(key.(uint32))
	case types.Type_UINT64:
		return encoding.EncodeUint64(key.(uint64))
	case types.Type_DECIMAL64:
		return encoding.EncodeDecimal64(key.(types.Decimal64))
	case types.Type_DECIMAL128:
		return encoding.EncodeDecimal128(key.(types.Decimal128))
	case types.Type_FLOAT32:
		return encoding.EncodeFloat32(key.(float32))
	case types.Type_FLOAT64:
		return encoding.EncodeFloat64(key.(float64))
	case types.Type_DATE:
		return encoding.EncodeDate(key.(types.Date))
	case types.Type_TIMESTAMP:
		return encoding.EncodeTimestamp(key.(types.Timestamp))
	case types.Type_DATETIME:
		return encoding.EncodeDatetime(key.(types.Datetime))
	case types.Type_CHAR, types.Type_VARCHAR:
		return key.([]byte)
	default:
		panic("unsupported type")
	}
}

func ForeachApply[T types.FixedSizeT](vs any, offset, length uint32, sels []uint32, op func(any, uint32) error) (err error) {
	vals := vs.([]T)
	vals = vals[offset : offset+length]
	if sels == nil || len(sels) == 0 {
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

func ProcessVector(vec *vector.Vector, offset, length uint32, op func(v any, pos uint32) error, keyselects *roaring.Bitmap) error {
	var sels []uint32
	if keyselects != nil {
		sels = keyselects.ToArray()
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
		if keyselects == nil {
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
