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
	"encoding"
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

const (
	TSize          int = int(unsafe.Sizeof(Type{}))
	DateSize       int = 4
	TimeSize       int = 8
	DatetimeSize   int = 8
	TimestampSize  int = 8
	Decimal64Size  int = 8
	Decimal128Size int = 16
	UuidSize       int = 16
)

func EncodeSliceWithCap[T any](v []T) []byte {
	var t T
	sz := int(unsafe.Sizeof(t))
	if cap(v) > 0 {
		return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*sz)[:len(v)*sz]
	}
	return nil
}

func EncodeSlice[T any](v []T) []byte {
	var t T
	sz := int(unsafe.Sizeof(t))
	if len(v) > 0 {
		return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*sz)[:len(v)*sz]
	}
	return nil
}

func DecodeSlice[T any](v []byte) []T {
	var t T
	sz := int(unsafe.Sizeof(t))

	if len(v)%sz != 0 {
		panic(moerr.NewInternalErrorNoCtx("decode slice that is not a multiple of element size"))
	}

	if len(v) > 0 {
		return unsafe.Slice((*T)(unsafe.Pointer(&v[0])), len(v)/sz)[:len(v)/sz]
	}
	return nil
}

func Encode(v encoding.BinaryMarshaler) ([]byte, error) {
	return v.MarshalBinary()
}

func Decode(data []byte, v encoding.BinaryUnmarshaler) error {
	return v.UnmarshalBinary(data)
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

func EncodeType(v *Type) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), TSize)
}

func DecodeType(v []byte) Type {
	return *(*Type)(unsafe.Pointer(&v[0]))
}

func EncodeFixed[T FixedSizeT](v T) []byte {
	sz := unsafe.Sizeof(v)
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sz)
}
func DecodeFixed[T FixedSizeT](v []byte) T {
	return *(*T)(unsafe.Pointer(&v[0]))
}

func DecodeBool(v []byte) bool {
	return *(*bool)(unsafe.Pointer(&v[0]))
}

func EncodeBool(v *bool) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 1)
}

func EncodeInt8(v *int8) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 1)
}

func DecodeInt8(v []byte) int8 {
	return *(*int8)(unsafe.Pointer(&v[0]))
}

func EncodeUint8(v *uint8) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 1)
}

func DecodeUint8(v []byte) uint8 {
	return v[0]
}

func EncodeInt16(v *int16) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 2)
}

func DecodeInt16(v []byte) int16 {
	return *(*int16)(unsafe.Pointer(&v[0]))
}

func EncodeUint16(v *uint16) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 2)
}

func DecodeUint16(v []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&v[0]))
}

func EncodeInt32(v *int32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeInt32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func EncodeUint32(v *uint32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}

func EncodeInt64(v *int64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeInt64(v []byte) int64 {
	return *(*int64)(unsafe.Pointer(&v[0]))
}

func EncodeUint64(v *uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeUint64(v []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&v[0]))
}

func EncodeFloat32(v *float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeFloat32(v []byte) float32 {
	return *(*float32)(unsafe.Pointer(&v[0]))
}

func EncodeFloat64(v *float64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeFloat64(v []byte) float64 {
	return *(*float64)(unsafe.Pointer(&v[0]))
}

func EncodeDate(v *Date) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeDate(v []byte) Date {
	return *(*Date)(unsafe.Pointer(&v[0]))
}

func EncodeTime(v *Time) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeTime(v []byte) Time {
	return *(*Time)(unsafe.Pointer(&v[0]))
}

func EncodeDatetime(v *Datetime) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeDatetime(v []byte) Datetime {
	return *(*Datetime)(unsafe.Pointer(&v[0]))
}

func EncodeTimestamp(v *Timestamp) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeTimestamp(v []byte) Timestamp {
	return *(*Timestamp)(unsafe.Pointer(&v[0]))
}

func EncodeEnum(v *Enum) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 2)
}

func DecodeEnum(v []byte) Enum {
	return *(*Enum)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal64(v *Decimal64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), Decimal64Size)
}

func DecodeDecimal64(v []byte) Decimal64 {
	return *(*Decimal64)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal128(v *Decimal128) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), Decimal128Size)
}

func DecodeDecimal128(v []byte) Decimal128 {
	return *(*Decimal128)(unsafe.Pointer(&v[0]))
}

func EncodeUuid(v *Uuid) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), UuidSize)
}

func DecodeUuid(v []byte) Uuid {
	return *(*Uuid)(unsafe.Pointer(&v[0]))
}

func EncodeStringSlice(vs []string) []byte {
	var o int32
	var buf bytes.Buffer

	cnt := int32(len(vs))
	buf.Write(EncodeInt32(&cnt))
	if cnt == 0 {
		return buf.Bytes()
	}
	os := make([]int32, cnt)
	for i, v := range vs {
		os[i] = o
		o += int32(len(v))
	}
	buf.Write(EncodeSlice(os))
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
	os := DecodeSlice[int32](data[:4*cnt])
	data = data[4*cnt:]
	for i := int32(0); i < cnt; i++ {
		if i == cnt-1 {
			tm = data[os[i]:]
			vs[i] = util.UnsafeBytesToString(tm)
		} else {
			tm = data[os[i]:os[i+1]]
			vs[i] = util.UnsafeBytesToString(tm)
		}
	}
	return vs
}

func DecodeValue(val []byte, t T) any {
	switch t {
	case T_bool:
		return DecodeFixed[bool](val)
	case T_bit:
		return DecodeFixed[uint64](val)
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
	case T_time:
		return DecodeFixed[Time](val)
	case T_datetime:
		return DecodeFixed[Datetime](val)
	case T_timestamp:
		return DecodeFixed[Timestamp](val)
	case T_decimal64:
		return DecodeFixed[Decimal64](val)
	case T_decimal128:
		return DecodeFixed[Decimal128](val)
	case T_uuid:
		return DecodeFixed[Uuid](val)
	case T_TS:
		return DecodeFixed[TS](val)
	case T_Rowid:
		return DecodeFixed[Rowid](val)
	case T_char, T_varchar, T_blob, T_json, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_datalink:
		return val
	case T_enum:
		return DecodeFixed[Enum](val)
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func EncodeValue(val any, t T) []byte {
	switch t {
	case T_bool:
		return EncodeFixed(val.(bool))
	case T_bit:
		return EncodeFixed(val.(uint64))
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
	case T_float32:
		return EncodeFixed(val.(float32))
	case T_float64:
		return EncodeFixed(val.(float64))
	case T_decimal64:
		return EncodeFixed(val.(Decimal64))
	case T_decimal128:
		return EncodeFixed(val.(Decimal128))
	case T_date:
		return EncodeFixed(val.(Date))
	case T_time:
		return EncodeFixed(val.(Time))
	case T_timestamp:
		return EncodeFixed(val.(Timestamp))
	case T_datetime:
		return EncodeFixed(val.(Datetime))
	case T_uuid:
		return EncodeFixed(val.(Uuid))
	case T_TS:
		return EncodeFixed(val.(TS))
	case T_Rowid:
		return EncodeFixed(val.(Rowid))
	case T_char, T_varchar, T_blob, T_json, T_text, T_binary, T_varbinary,
		T_array_float32, T_array_float64, T_datalink:
		// Mainly used by Zonemap, which receives val input from DN batch/vector.
		// This val is mostly []bytes and not []float32 or []float64
		return val.([]byte)
	case T_enum:
		return EncodeFixed(val.(Enum))
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
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
		case Time:
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
		case Uuid:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case TS:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		case Rowid:
			if nr, err = w.Write(EncodeFixed(v)); err != nil {
				return
			}
			n += int64(nr)
		default:
			panic(moerr.NewInternalErrorNoCtx("%T:%v not supported", v, v))
		}
	}
	return
}

func Int32ToUint32(x int32) uint32 {
	ux := uint32(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return ux
}

func Uint32ToInt32(ux uint32) int32 {
	x := int32(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}
