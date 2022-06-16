package types

import (
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

var TypeSize = encoding.TypeSize

var EncodeType = encoding.EncodeType
var DecodeType = encoding.DecodeType

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
		return EncodeFixed[bool](val.(bool))
	case Type_INT8:
		return EncodeFixed[int8](val.(int8))
	case Type_INT16:
		return EncodeFixed[int16](val.(int16))
	case Type_INT32:
		return EncodeFixed[int32](val.(int32))
	case Type_INT64:
		return EncodeFixed[int64](val.(int64))
	case Type_UINT8:
		return EncodeFixed[uint8](val.(uint8))
	case Type_UINT16:
		return EncodeFixed[uint16](val.(uint16))
	case Type_UINT32:
		return EncodeFixed[uint32](val.(uint32))
	case Type_UINT64:
		return EncodeFixed[uint64](val.(uint64))
	case Type_DECIMAL64:
		return EncodeFixed[Decimal64](val.(Decimal64))
	case Type_DECIMAL128:
		return EncodeFixed[Decimal128](val.(Decimal128))
	case Type_FLOAT32:
		return EncodeFixed[float32](val.(float32))
	case Type_FLOAT64:
		return EncodeFixed[float64](val.(float64))
	case Type_DATE:
		return EncodeFixed[Date](val.(Date))
	case Type_TIMESTAMP:
		return EncodeFixed[Timestamp](val.(Timestamp))
	case Type_DATETIME:
		return EncodeFixed[Datetime](val.(Datetime))
	case Type_CHAR, Type_VARCHAR:
		return val.([]byte)
	default:
		panic("unsupported type")
	}
}
