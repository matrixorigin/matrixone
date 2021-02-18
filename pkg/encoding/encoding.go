package encoding

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"unsafe"
)

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

func EncodeUint64(v uint64) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 8, Cap: 8}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint64(v []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&v[0]))
}

func EncodeBoolSlice(v []bool) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeBoolSlice(v []byte) []bool {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	return *(*[]bool)(unsafe.Pointer(&hp))
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
