package encoding

import (
	"bytes"
	"encoding/gob"
	"matrixone/pkg/container/types"
	"reflect"
	"unsafe"
)

var TypeSize int
var DateSize int
var DatetimeSize int
var DecimalSize int

func init() {
	TypeSize = int(unsafe.Sizeof(types.Type{}))
	DateSize = int(unsafe.Sizeof(types.Date{}))
	DatetimeSize = int(unsafe.Sizeof(types.Datetime{}))
	DecimalSize = int(unsafe.Sizeof(types.Decimal{}))
}

func Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func EncodeType(v types.Type) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: TypeSize, Cap: TypeSize}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeType(v []byte) types.Type {
	return *(*types.Type)(unsafe.Pointer(&v[0]))
}

func EncodeInt8(v int8) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 1, Cap: 1}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt8(v []byte) int8 {
	return *(*int8)(unsafe.Pointer(&v[0]))
}

func EncodeUint8(v uint8) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 1, Cap: 1}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint8(v []byte) uint8 {
	return *(*uint8)(unsafe.Pointer(&v[0]))
}

func EncodeInt16(v int16) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 2, Cap: 2}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt16(v []byte) int16 {
	return *(*int16)(unsafe.Pointer(&v[0]))
}

func EncodeUint16(v uint16) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 2, Cap: 2}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint16(v []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&v[0]))
}

func EncodeInt32(v int32) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func EncodeUint32(v uint32) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}

func EncodeInt64(v int64) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 8, Cap: 8}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt64(v []byte) int64 {
	return *(*int64)(unsafe.Pointer(&v[0]))
}

func EncodeUint64(v uint64) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 8, Cap: 8}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint64(v []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&v[0]))
}

func EncodeFloat32(v float32) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeFloat32(v []byte) float32 {
	return *(*float32)(unsafe.Pointer(&v[0]))
}

func EncodeFloat64(v float64) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 8, Cap: 8}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeFloat64(v []byte) float64 {
	return *(*float64)(unsafe.Pointer(&v[0]))
}

func EncodeInt8Slice(v []int8) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt8Slice(v []byte) []int8 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]int8)(unsafe.Pointer(&hp))
}

func EncodeUint8Slice(v []uint8) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint8Slice(v []byte) []uint8 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]uint8)(unsafe.Pointer(&hp))
}

func EncodeInt16Slice(v []int16) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 2
	hp.Cap *= 2
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt16Slice(v []byte) []int16 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 2
	hp.Cap /= 2
	return *(*[]int16)(unsafe.Pointer(&hp))
}

func EncodeUint16Slice(v []uint16) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 2
	hp.Cap *= 2
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint16Slice(v []byte) []uint16 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 2
	hp.Cap /= 2
	return *(*[]uint16)(unsafe.Pointer(&hp))
}

func EncodeInt32Slice(v []int32) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 4
	hp.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt32Slice(v []byte) []int32 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 4
	hp.Cap /= 4
	return *(*[]int32)(unsafe.Pointer(&hp))
}

func EncodeUint32Slice(v []uint32) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 4
	hp.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint32Slice(v []byte) []uint32 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 4
	hp.Cap /= 4
	return *(*[]uint32)(unsafe.Pointer(&hp))
}

func EncodeInt64Slice(v []int64) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 8
	hp.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt64Slice(v []byte) []int64 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 8
	hp.Cap /= 8
	return *(*[]int64)(unsafe.Pointer(&hp))
}

func EncodeUint64Slice(v []uint64) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 8
	hp.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint64Slice(v []byte) []uint64 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 8
	hp.Cap /= 8
	return *(*[]uint64)(unsafe.Pointer(&hp))
}

func EncodeFloat32Slice(v []float32) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 4
	hp.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeFloat32Slice(v []byte) []float32 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 4
	hp.Cap /= 4
	return *(*[]float32)(unsafe.Pointer(&hp))
}

func EncodeFloat64Slice(v []float64) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 8
	hp.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeFloat64Slice(v []byte) []float64 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 8
	hp.Cap /= 8
	return *(*[]float64)(unsafe.Pointer(&hp))
}

func EncodeDateSlice(v []types.Date) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= DateSize
	hp.Cap *= DateSize
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeDateSlice(v []byte) []types.Date {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= DateSize
	hp.Cap /= DateSize
	return *(*[]types.Date)(unsafe.Pointer(&hp))
}

func EncodeDatetimeSlice(v []types.Datetime) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= DatetimeSize
	hp.Cap *= DatetimeSize
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeDatetimeSlice(v []byte) []types.Datetime {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= DatetimeSize
	hp.Cap /= DatetimeSize
	return *(*[]types.Datetime)(unsafe.Pointer(&hp))
}

func EncodeDecimalSlice(v []types.Decimal) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= DecimalSize
	hp.Cap *= DecimalSize
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeDecimalSlice(v []byte) []types.Decimal {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= DecimalSize
	hp.Cap /= DecimalSize
	return *(*[]types.Decimal)(unsafe.Pointer(&hp))
}

func EncodeStringSlice(vs []string) []byte {
	var o int32
	var buf bytes.Buffer

	cnt := int32(len(vs))
	buf.Write(EncodeInt32(cnt))
	if cnt == 0 {
		return buf.Bytes()
	}
	os := make([]int32, cnt)
	for i, v := range vs {
		os[i] = o
		o += int32(len(v))
	}
	buf.Write(EncodeInt32Slice(os))
	for _, v := range vs {
		buf.WriteString(v)
	}
	return buf.Bytes()
}

func DecodeStringSlice(data []byte) []string {
	var tm []byte

	cnt := DecodeInt32(data)
	if cnt == 0 {
		return nil
	}
	data = data[4:]
	vs := make([]string, cnt)
	os := DecodeInt32Slice(data)
	data = data[4*cnt:]
	for i := int32(0); i < cnt; i++ {
		if i == cnt-1 {
			tm = data[os[i]:]
			vs[i] = *(*string)(unsafe.Pointer(&tm))
		} else {
			tm = data[os[i]:os[i+1]]
			vs[i] = *(*string)(unsafe.Pointer(&tm))
		}
	}
	return vs
}
