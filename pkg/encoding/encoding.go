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

package encoding

import (
	"bytes"
	"encoding/gob"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var TypeSize int
var DateSize int
var DatetimeSize int
var TimestampSize int
var Decimal64Size int
var Decimal128Size int

func init() {
	TypeSize = int(unsafe.Sizeof(types.Type{}))
	DateSize = int(unsafe.Sizeof(types.Date(0)))
	DatetimeSize = int(unsafe.Sizeof(types.Datetime(0)))
	TimestampSize = int(unsafe.Sizeof(types.Timestamp(0)))
	Decimal64Size = int(unsafe.Sizeof(types.Decimal64{}))
	Decimal128Size = int(unsafe.Sizeof(types.Decimal128{}))
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

func EncodeType(v types.Type) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), TypeSize)
}

func DecodeType(v []byte) types.Type {
	return *(*types.Type)(unsafe.Pointer(&v[0]))
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

func EncodeDate(v types.Date) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 4)
}

func DecodeDate(v []byte) types.Date {
	return *(*types.Date)(unsafe.Pointer(&v[0]))
}

func EncodeDatetime(v types.Datetime) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeDatetime(v []byte) types.Datetime {
	return *(*types.Datetime)(unsafe.Pointer(&v[0]))
}

func EncodeTimestamp(v types.Timestamp) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
}

func DecodeTimestamp(v []byte) types.Timestamp {
	return *(*types.Timestamp)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal64(v types.Decimal64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), Decimal64Size)
}

func DecodeDecimal64(v []byte) types.Decimal64 {
	return *(*types.Decimal64)(unsafe.Pointer(&v[0]))
}

func EncodeDecimal128(v types.Decimal128) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&v)), Decimal128Size)
}

func DecodeDecimal128(v []byte) types.Decimal128 {
	return *(*types.Decimal128)(unsafe.Pointer(&v[0]))
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

func EncodeDateSlice(v []types.Date) []byte {
	return EncodeFixedSlice(v, DateSize)
}

func DecodeDateSlice(v []byte) []types.Date {
	return DecodeFixedSlice[types.Date](v, DateSize)
}

func EncodeDatetimeSlice(v []types.Datetime) []byte {
	return EncodeFixedSlice(v, DatetimeSize)
}

func DecodeDatetimeSlice(v []byte) (ret []types.Datetime) {
	return DecodeFixedSlice[types.Datetime](v, DatetimeSize)
}

func EncodeTimestampSlice(v []types.Timestamp) []byte {
	return EncodeFixedSlice(v, TimestampSize)
}

func DecodeTimestampSlice(v []byte) (ret []types.Timestamp) {
	return DecodeFixedSlice[types.Timestamp](v, TimestampSize)
}

func EncodeDecimal64Slice(v []types.Decimal64) []byte {
	return EncodeFixedSlice(v, Decimal64Size)
}

func DecodeDecimal64Slice(v []byte) (ret []types.Decimal64) {
	return DecodeFixedSlice[types.Decimal64](v, Decimal64Size)
}

func EncodeDecimal128Slice(v []types.Decimal128) []byte {
	return EncodeFixedSlice(v, Decimal128Size)
}

func DecodeDecimal128Slice(v []byte) (ret []types.Decimal128) {
	return DecodeFixedSlice[types.Decimal128](v, Decimal128Size)
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
