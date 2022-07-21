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

package types

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

var TypeSize = encoding.TypeSize
var DateSize = encoding.DateSize
var DatetimeSize = encoding.DatetimeSize
var TimestampSize = encoding.TimestampSize
var Decimal64Size = encoding.Decimal64Size
var Decimal128Size = encoding.Decimal128Size

var EncodeType = encoding.EncodeType
var DecodeType = encoding.DecodeType

var EncodeBoolSlice = encoding.EncodeBoolSlice
var EncodeInt8Slice = encoding.EncodeInt8Slice
var EncodeInt16Slice = encoding.EncodeInt16Slice
var EncodeInt32Slice = encoding.EncodeInt32Slice
var EncodeInt64Slice = encoding.EncodeInt64Slice
var EncodeUint8Slice = encoding.EncodeUint8Slice
var EncodeUint16Slice = encoding.EncodeUint16Slice
var EncodeUint32Slice = encoding.EncodeUint32Slice
var EncodeUint64Slice = encoding.EncodeUint64Slice
var EncodeFloat32Slice = encoding.EncodeFloat32Slice
var EncodeFloat64Slice = encoding.EncodeFloat64Slice

var EncodeDateSlice = encoding.EncodeDateSlice
var EncodeDatetimeSlice = encoding.EncodeDatetimeSlice
var EncodeTimestampSlice = encoding.EncodeTimestampSlice
var EncodeDecimal64Slice = encoding.EncodeDecimal64Slice
var EncodeDecimal128Slice = encoding.EncodeDecimal128Slice

var DecodeBoolSlice = encoding.DecodeBoolSlice
var DecodeInt8Slice = encoding.DecodeInt8Slice
var DecodeInt16Slice = encoding.DecodeInt16Slice
var DecodeInt32Slice = encoding.DecodeInt32Slice
var DecodeInt64Slice = encoding.DecodeInt64Slice
var DecodeUint8Slice = encoding.DecodeUint8Slice
var DecodeUint16Slice = encoding.DecodeUint16Slice
var DecodeUint32Slice = encoding.DecodeUint32Slice
var DecodeUint64Slice = encoding.DecodeUint64Slice
var DecodeFloat32Slice = encoding.DecodeFloat32Slice
var DecodeFloat64Slice = encoding.DecodeFloat64Slice

var DecodeDateSlice = encoding.DecodeDateSlice
var DecodeDatetimeSlice = encoding.DecodeDatetimeSlice
var DecodeTimestampSlice = encoding.DecodeTimestampSlice
var DecodeDecimal64Slice = encoding.DecodeDecimal64Slice
var DecodeDecimal128Slice = encoding.DecodeDecimal128Slice

func EncodeFixed[T any](v T) []byte {
	sz := unsafe.Sizeof(v)
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sz)
}
func DecodeFixed[T any](v []byte) T {
	return *(*T)(unsafe.Pointer(&v[0]))
}
func EncodeFixedSlice[T any](v []T, sz int) (ret []byte) {
	if len(v) > 0 {
		ret = unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*sz)[:len(v)*sz]
	}
	return
}

func DecodeFixedSlice[T any](v []byte, sz int) (ret []T) {
	if len(v) > 0 {
		ret = unsafe.Slice((*T)(unsafe.Pointer(&v[0])), cap(v)/sz)[:len(v)/sz]
	}
	return
}

func Hash(v any, typ Type) (uint64, error) {
	data := EncodeValue(v, typ)
	//murmur := murmur3.Sum64(data)
	xx := xxhash.Sum64(data)
	return xx, nil
}

func DecodeValue(val []byte, typ Type) any {
	switch typ.Oid {
	case Type_BOOL:
		return DecodeFixed[bool](val)
	case Type_INT8:
		return DecodeFixed[int8](val)
	case Type_INT16:
		return DecodeFixed[int16](val)
	case Type_INT32:
		return DecodeFixed[int32](val)
	case Type_INT64:
		return DecodeFixed[int64](val)
	case Type_UINT8:
		return DecodeFixed[uint8](val)
	case Type_UINT16:
		return DecodeFixed[uint16](val)
	case Type_UINT32:
		return DecodeFixed[uint32](val)
	case Type_UINT64:
		return DecodeFixed[uint64](val)
	case Type_FLOAT32:
		return DecodeFixed[float32](val)
	case Type_FLOAT64:
		return DecodeFixed[float64](val)
	case Type_DATE:
		return DecodeFixed[Date](val)
	case Type_DATETIME:
		return DecodeFixed[Datetime](val)
	case Type_TIMESTAMP:
		return DecodeFixed[Timestamp](val)
	case Type_DECIMAL64:
		return DecodeFixed[Decimal64](val)
	case Type_DECIMAL128:
		return DecodeFixed[Decimal128](val)
	case Type_CHAR, Type_VARCHAR:
		return val
	default:
		panic("unsupported type")
	}
}

func EncodeValue(val any, typ Type) []byte {
	switch typ.Oid {
	case Type_BOOL:
		return EncodeFixed(val.(bool))
	case Type_INT8:
		return EncodeFixed(val.(int8))
	case Type_INT16:
		return EncodeFixed(val.(int16))
	case Type_INT32:
		return EncodeFixed(val.(int32))
	case Type_INT64:
		return EncodeFixed(val.(int64))
	case Type_UINT8:
		return EncodeFixed(val.(uint8))
	case Type_UINT16:
		return EncodeFixed(val.(uint16))
	case Type_UINT32:
		return EncodeFixed(val.(uint32))
	case Type_UINT64:
		return EncodeFixed(val.(uint64))
	case Type_DECIMAL64:
		return EncodeFixed(val.(Decimal64))
	case Type_DECIMAL128:
		return EncodeFixed(val.(Decimal128))
	case Type_FLOAT32:
		return EncodeFixed(val.(float32))
	case Type_FLOAT64:
		return EncodeFixed(val.(float64))
	case Type_DATE:
		return EncodeFixed(val.(Date))
	case Type_TIMESTAMP:
		return EncodeFixed(val.(Timestamp))
	case Type_DATETIME:
		return EncodeFixed(val.(Datetime))
	case Type_CHAR, Type_VARCHAR:
		return val.([]byte)
	default:
		panic("unsupported type")
	}
}

func WriteFixedValue[T any](w io.Writer, v T) (err error) {
	_, err = w.Write(EncodeFixed(v))
	return
}

func WriteValues(w io.Writer, vals ...any) (n int64, err error) {
	var nr int
	for _, val := range vals {
		switch v := val.(type) {
		case []byte:
			if nr, err = w.Write(v); err != nil {
				return
			}
			n += int64(nr)
		case bool:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case int8:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case int16:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case int32:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case int64:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case uint8:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case uint16:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case uint32:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case uint64:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case float32:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case float64:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case types.Date:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case types.Datetime:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case types.Timestamp:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case types.Decimal64:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case types.Decimal128:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		default:
			panic(fmt.Errorf("%T:%v not supported", v, v))
		}
	}
	return
}

func CopyFixValueToBuf[T any](dest []byte, val T) {
	vbuf := EncodeFixed(val)
	copy(dest[:unsafe.Sizeof(val)], vbuf)
}
