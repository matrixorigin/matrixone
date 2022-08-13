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
// limitations under the License.

//go:build !debug
// +build !debug

package types

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

var TSize int
var DateSize int
var DatetimeSize int
var TimestampSize int
var Decimal64Size int
var Decimal128Size int

func init() {
	TSize = int(unsafe.Sizeof(Type{}))
	DateSize = int(unsafe.Sizeof(Date(0)))
	DatetimeSize = int(unsafe.Sizeof(Datetime(0)))
	TimestampSize = int(unsafe.Sizeof(Timestamp(0)))
	Decimal64Size = int(unsafe.Sizeof(Decimal64{}))
	Decimal128Size = int(unsafe.Sizeof(Decimal128{}))
}

func EncodeSlice[T any](v []T, sz int) (ret []byte) {
	if len(v) > 0 {
		ret = unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*sz)[:len(v)*sz]
	}
	return
}

func DecodeSlice[T any](v []byte, sz int) (ret []T) {
	if len(v) > 0 {
		ret = unsafe.Slice((*T)(unsafe.Pointer(&v[0])), cap(v)/sz)[:len(v)/sz]
	}
	return
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

func EncodeJson(v bytejson.ByteJson) ([]byte, error) {
	//TODO handle error
	buf, _ := v.Marshal()
	return buf, nil
}

func DecodeJson(buf []byte) bytejson.ByteJson {
	bj := bytejson.ByteJson{}
	//TODO handle error
	_ = bj.Unmarshal(buf)
	return bj
}

func EncodeType(v Type) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), TSize)
}

func DecodeType(v []byte) Type {
	return *(*Type)(unsafe.Pointer(&v[0]))
}

func EncodeFixed[T any](v T) []byte {
	sz := unsafe.Sizeof(v)
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sz)
}
func DecodeFixed[T any](v []byte) T {
	return *(*T)(unsafe.Pointer(&v[0]))
}

func DecodeBool(v []byte) bool {
	return *(*bool)(unsafe.Pointer(&v[0]))
}

func EncodeBool(v bool) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 1)
}

func EncodeInt8(v int8) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 1)
}

func DecodeInt8(v []byte) int8 {
	return *(*int8)(unsafe.Pointer(&v[0]))
}

func EncodeUint8(v uint8) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 1)
}

func DecodeUint8(v []byte) uint8 {
	return v[0]
}

func EncodeInt16(v int16) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 2)
}

func DecodeInt16(v []byte) int16 {
	return *(*int16)(unsafe.Pointer(&v[0]))
}

func EncodeUint16(v uint16) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 2)
}

func DecodeUint16(v []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&v[0]))
}

func EncodeInt32(v int32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 4)
}

func DecodeInt32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func EncodeUint32(v uint32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 4)
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}

func EncodeInt64(v int64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeInt64(v []byte) int64 {
	return *(*int64)(unsafe.Pointer(&v[0]))
}

func EncodeUint64(v uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeUint64(v []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&v[0]))
}

func EncodeFloat32(v float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 4)
}

func DecodeFloat32(v []byte) float32 {
	return *(*float32)(unsafe.Pointer(&v[0]))
}

func EncodeFloat64(v float64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeFloat64(v []byte) float64 {
	return *(*float64)(unsafe.Pointer(&v[0]))
}

func EncodeDate(v Date) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 4)
}

func DecodeDate(v []byte) Date {
	return *(*Date)(unsafe.Pointer(&v[0]))
}

func EncodeDatetime(v Datetime) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeDatetime(v []byte) Datetime {
	return *(*Datetime)(unsafe.Pointer(&v[0]))
}

func EncodeTimestamp(v Timestamp) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeTimestamp(v []byte) Timestamp {
	return *(*Timestamp)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal64(v Decimal64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), Decimal64Size)
}

func DecodeDecimal64(v []byte) Decimal64 {
	return *(*Decimal64)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal128(v Decimal128) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), Decimal128Size)
}

func DecodeDecimal128(v []byte) Decimal128 {
	return *(*Decimal128)(unsafe.Pointer(&v[0]))
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

func EncodeBoolSlice(v []bool) []byte {
	return *(*[]byte)(unsafe.Pointer(&v))
}

func EncodeInt8Slice(v []int8) []byte {
	return *(*[]byte)(unsafe.Pointer(&v))
}

func DecodeBoolSlice(v []byte) []bool {
	return *(*[]bool)(unsafe.Pointer(&v))
}

func DecodeInt8Slice(v []byte) []int8 {
	return *(*[]int8)(unsafe.Pointer(&v))
}

func EncodeUint8Slice(v []uint8) []byte {
	return v
}

func DecodeUint8Slice(v []byte) []uint8 {
	return v
}

func EncodeInt16Slice(v []int16) []byte {
	return EncodeFixedSlice(v, 2)
}

func DecodeInt16Slice(v []byte) []int16 {
	return DecodeFixedSlice[int16](v, 2)
}

func EncodeUint16Slice(v []uint16) []byte {
	return EncodeFixedSlice(v, 2)
}

func DecodeUint16Slice(v []byte) []uint16 {
	return DecodeFixedSlice[uint16](v, 2)
}

func EncodeInt32Slice(v []int32) []byte {
	return EncodeFixedSlice(v, 4)
}

func DecodeInt32Slice(v []byte) []int32 {
	return DecodeFixedSlice[int32](v, 4)
}

func EncodeUint32Slice(v []uint32) []byte {
	return EncodeFixedSlice(v, 4)
}

func DecodeUint32Slice(v []byte) []uint32 {
	return DecodeFixedSlice[uint32](v, 4)
}

func EncodeInt64Slice(v []int64) []byte {
	return EncodeFixedSlice(v, 8)
}

func DecodeInt64Slice(v []byte) []int64 {
	return DecodeFixedSlice[int64](v, 8)
}

func EncodeUint64Slice(v []uint64) []byte {
	return EncodeFixedSlice(v, 8)
}

func DecodeUint64Slice(v []byte) []uint64 {
	return DecodeFixedSlice[uint64](v, 8)
}

func EncodeFloat32Slice(v []float32) []byte {
	return EncodeFixedSlice(v, 4)
}

func DecodeFloat32Slice(v []byte) []float32 {
	return DecodeFixedSlice[float32](v, 4)
}

func EncodeFloat64Slice(v []float64) []byte {
	return EncodeFixedSlice(v, 8)
}

func DecodeFloat64Slice(v []byte) []float64 {
	return DecodeFixedSlice[float64](v, 8)
}

func EncodeFloat64SliceForBenchmark(v []float64) (ret []byte) {
	if len(v) > 0 {
		ret = unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*8)[:len(v)*8]
	}
	return
}

func DecodeFloat64SliceForBenchmark(v []byte) (ret []float64) {
	if len(v) > 0 {
		ret = unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), cap(v)/8)[:len(v)/8]
	}
	return
}

func EncodeDateSlice(v []Date) []byte {
	return EncodeFixedSlice(v, DateSize)
}

func DecodeDateSlice(v []byte) []Date {
	return DecodeFixedSlice[Date](v, DateSize)
}

func EncodeDatetimeSlice(v []Datetime) []byte {
	return EncodeFixedSlice(v, DatetimeSize)
}

func DecodeDatetimeSlice(v []byte) (ret []Datetime) {
	return DecodeFixedSlice[Datetime](v, DatetimeSize)
}

func EncodeTimestampSlice(v []Timestamp) []byte {
	return EncodeFixedSlice(v, TimestampSize)
}

func DecodeTimestampSlice(v []byte) (ret []Timestamp) {
	return DecodeFixedSlice[Timestamp](v, TimestampSize)
}

func EncodeDecimal64Slice(v []Decimal64) []byte {
	return EncodeFixedSlice(v, Decimal64Size)
}

func DecodeDecimal64Slice(v []byte) (ret []Decimal64) {
	return DecodeFixedSlice[Decimal64](v, Decimal64Size)
}

func EncodeDecimal128Slice(v []Decimal128) []byte {
	return EncodeFixedSlice(v, Decimal128Size)
}

func DecodeDecimal128Slice(v []byte) (ret []Decimal128) {
	return DecodeFixedSlice[Decimal128](v, Decimal128Size)
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

func DecodeValue(val []byte, typ Type) any {
	switch typ.Oid {
	case T_bool:
		return DecodeFixed[bool](val)
	case T_int8:
		return DecodeFixed[int8](val)
	case T_int16:
		return DecodeFixed[int16](val)
	case T_int32:
		return DecodeFixed[int32](val)
	case T_int64:
		return DecodeFixed[int64](val)
	case T_uint8:
		return DecodeFixed[uint8](val)
	case T_uint16:
		return DecodeFixed[uint16](val)
	case T_uint32:
		return DecodeFixed[uint32](val)
	case T_uint64:
		return DecodeFixed[uint64](val)
	case T_float32:
		return DecodeFixed[float32](val)
	case T_float64:
		return DecodeFixed[float64](val)
	case T_date:
		return DecodeFixed[Date](val)
	case T_datetime:
		return DecodeFixed[Datetime](val)
	case T_timestamp:
		return DecodeFixed[Timestamp](val)
	case T_decimal64:
		return DecodeFixed[Decimal64](val)
	case T_decimal128:
		return DecodeFixed[Decimal128](val)
	case T_char, T_varchar:
		return val
	default:
		panic("unsupported type")
	}
}

func EncodeValue(val any, typ Type) []byte {
	switch typ.Oid {
	case T_bool:
		return EncodeFixed(val.(bool))
	case T_int8:
		return EncodeFixed(val.(int8))
	case T_int16:
		return EncodeFixed(val.(int16))
	case T_int32:
		return EncodeFixed(val.(int32))
	case T_int64:
		return EncodeFixed(val.(int64))
	case T_uint8:
		return EncodeFixed(val.(uint8))
	case T_uint16:
		return EncodeFixed(val.(uint16))
	case T_uint32:
		return EncodeFixed(val.(uint32))
	case T_uint64:
		return EncodeFixed(val.(uint64))
	case T_decimal64:
		return EncodeFixed(val.(Decimal64))
	case T_decimal128:
		return EncodeFixed(val.(Decimal128))
	case T_float32:
		return EncodeFixed(val.(float32))
	case T_float64:
		return EncodeFixed(val.(float64))
	case T_date:
		return EncodeFixed(val.(Date))
	case T_timestamp:
		return EncodeFixed(val.(Timestamp))
	case T_datetime:
		return EncodeFixed(val.(Datetime))
	case T_char, T_varchar:
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
		case Date:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case Datetime:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case Timestamp:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case Decimal64:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case Decimal128:
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
