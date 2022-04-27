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

package typecast

import (
	"strconv"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int16ToInt8   = numericToNumeric[int16, int8]
	Int32ToInt8   = numericToNumeric[int32, int8]
	Int64ToInt8   = numericToNumeric[int64, int8]
	Uint8ToInt8   = numericToNumeric[uint8, int8]
	Uint16ToInt8  = numericToNumeric[uint16, int8]
	Uint32ToInt8  = numericToNumeric[uint32, int8]
	Uint64ToInt8  = numericToNumeric[uint64, int8]
	Float32ToInt8 = numericToNumeric[float32, int8]
	Float64ToInt8 = numericToNumeric[float64, int8]

	Int8ToInt16    = numericToNumeric[int8, int16]
	Int32ToInt16   = numericToNumeric[int32, int16]
	Int64ToInt16   = numericToNumeric[int64, int16]
	Uint8ToInt16   = numericToNumeric[uint8, int16]
	Uint16ToInt16  = numericToNumeric[uint16, int16]
	Uint32ToInt16  = numericToNumeric[uint32, int16]
	Uint64ToInt16  = numericToNumeric[uint64, int16]
	Float32ToInt16 = numericToNumeric[float32, int16]
	Float64ToInt16 = numericToNumeric[float64, int16]

	Int8ToInt32    = numericToNumeric[int8, int32]
	Int16ToInt32   = numericToNumeric[int16, int32]
	Int64ToInt32   = numericToNumeric[int64, int32]
	Uint8ToInt32   = numericToNumeric[uint8, int32]
	Uint16ToInt32  = numericToNumeric[uint16, int32]
	Uint32ToInt32  = numericToNumeric[uint32, int32]
	Uint64ToInt32  = numericToNumeric[uint64, int32]
	Float32ToInt32 = numericToNumeric[float32, int32]
	Float64ToInt32 = numericToNumeric[float64, int32]

	Int8ToInt64    = numericToNumeric[int8, int64]
	Int16ToInt64   = numericToNumeric[int16, int64]
	Int32ToInt64   = numericToNumeric[int32, int64]
	Uint8ToInt64   = numericToNumeric[uint8, int64]
	Uint16ToInt64  = numericToNumeric[uint16, int64]
	Uint32ToInt64  = numericToNumeric[uint32, int64]
	Uint64ToInt64  = numericToNumeric[uint64, int64]
	Float32ToInt64 = numericToNumeric[float32, int64]
	Float64ToInt64 = numericToNumeric[float64, int64]

	Int8ToUint8    = numericToNumeric[int8, uint8]
	Int16ToUint8   = numericToNumeric[int16, uint8]
	Int32ToUint8   = numericToNumeric[int32, uint8]
	Int64ToUint8   = numericToNumeric[int64, uint8]
	Uint16ToUint8  = numericToNumeric[uint16, uint8]
	Uint32ToUint8  = numericToNumeric[uint32, uint8]
	Uint64ToUint8  = numericToNumeric[uint64, uint8]
	Float32ToUint8 = numericToNumeric[float32, uint8]
	Float64ToUint8 = numericToNumeric[float64, uint8]

	Int8ToUint16    = numericToNumeric[int8, uint16]
	Int16ToUint16   = numericToNumeric[int16, uint16]
	Int32ToUint16   = numericToNumeric[int32, uint16]
	Int64ToUint16   = numericToNumeric[int64, uint16]
	Uint8ToUint16   = numericToNumeric[uint8, uint16]
	Uint32ToUint16  = numericToNumeric[uint32, uint16]
	Uint64ToUint16  = numericToNumeric[uint64, uint16]
	Float32ToUint16 = numericToNumeric[float32, uint16]
	Float64ToUint16 = numericToNumeric[float64, uint16]

	Int8ToUint32    = numericToNumeric[int8, uint32]
	Int16ToUint32   = numericToNumeric[int16, uint32]
	Int32ToUint32   = numericToNumeric[int32, uint32]
	Int64ToUint32   = numericToNumeric[int64, uint32]
	Uint8ToUint32   = numericToNumeric[uint8, uint32]
	Uint16ToUint32  = numericToNumeric[uint16, uint32]
	Uint64ToUint32  = numericToNumeric[uint64, uint32]
	Float32ToUint32 = numericToNumeric[float32, uint32]
	Float64ToUint32 = numericToNumeric[float64, uint32]

	Int8ToUint64    = numericToNumeric[int8, uint64]
	Int16ToUint64   = numericToNumeric[int16, uint64]
	Int32ToUint64   = numericToNumeric[int32, uint64]
	Int64ToUint64   = numericToNumeric[int64, uint64]
	Uint8ToUint64   = numericToNumeric[uint8, uint64]
	Uint16ToUint64  = numericToNumeric[uint16, uint64]
	Uint32ToUint64  = numericToNumeric[uint32, uint64]
	Float32ToUint64 = numericToNumeric[float32, uint64]
	Float64ToUint64 = numericToNumeric[float64, uint64]

	Int8ToFloat32    = numericToNumeric[int8, float32]
	Int16ToFloat32   = numericToNumeric[int16, float32]
	Int32ToFloat32   = numericToNumeric[int32, float32]
	Int64ToFloat32   = numericToNumeric[int64, float32]
	Uint8ToFloat32   = numericToNumeric[uint8, float32]
	Uint16ToFloat32  = numericToNumeric[uint16, float32]
	Uint32ToFloat32  = numericToNumeric[uint32, float32]
	Uint64ToFloat32  = numericToNumeric[uint64, float32]
	Float64ToFloat32 = numericToNumeric[float64, float32]

	Int8ToFloat64    = numericToNumeric[int8, float64]
	Int16ToFloat64   = numericToNumeric[int16, float64]
	Int32ToFloat64   = numericToNumeric[int32, float64]
	Int64ToFloat64   = numericToNumeric[int64, float64]
	Uint8ToFloat64   = numericToNumeric[uint8, float64]
	Uint16ToFloat64  = numericToNumeric[uint16, float64]
	Uint32ToFloat64  = numericToNumeric[uint32, float64]
	Uint64ToFloat64  = numericToNumeric[uint64, float64]
	Float32ToFloat64 = numericToNumeric[float32, float64]

	BytesToInt8    = bytesToInt[int8]
	Int8ToBytes    = intToBytes[int8]
	BytesToInt16   = bytesToInt[int16]
	Int16ToBytes   = intToBytes[int16]
	BytesToInt32   = bytesToInt[int32]
	Int32ToBytes   = intToBytes[int32]
	BytesToInt64   = bytesToInt[int64]
	Int64ToBytes   = intToBytes[int64]
	BytesToUint8   = bytesToInt[uint8]
	Uint8ToBytes   = intToBytes[uint8]
	BytesToUint16  = bytesToInt[uint16]
	Uint16ToBytes  = intToBytes[uint16]
	BytesToUint32  = bytesToInt[uint32]
	Uint32ToBytes  = intToBytes[uint32]
	BytesToUint64  = bytesToInt[uint64]
	Uint64ToBytes  = intToBytes[uint64]
	BytesToFloat32 = bytesToFloat[float32]
	Float32ToBytes = floatToBytes[float32]
	BytesToFloat64 = bytesToFloat[float64]
	Float64ToBytes = floatToBytes[float64]

	Decimal64ToDecimal128 = decimal64ToDecimal128Pure

	Int8ToDecimal128  = intToDecimal128[int8]
	Int16ToDecimal128 = intToDecimal128[int16]
	Int32ToDecimal128 = intToDecimal128[int32]
	Int64ToDecimal128 = intToDecimal128[int64]
)

func numericToNumeric[T1, T2 constraints.Integer | constraints.Float](xs []T1, rs []T2) ([]T2, error) {
	for i, x := range xs {
		rs[i] = T2(x)
	}
	return rs, nil
}

func bytesToInt[T constraints.Integer](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8

	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, bitSize)
		if err != nil {
			return nil, err
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func intToBytes[T constraints.Integer](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func bytesToFloat[T constraints.Float](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8

	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseFloat(s, bitSize)
		if err != nil {
			return nil, err
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func floatToBytes[T constraints.Float](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8

	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendFloat(rs.Data, float64(x), 'G', -1, bitSize)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func decimal64ToDecimal128Pure(xs []types.Decimal64, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i, x := range xs {
		rs[i] = types.Decimal64ToDecimal128(x)
	}
	return rs, nil
}

func intToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i, x := range xs {
		rs[i] = types.InitDecimal128(int64(x))
	}
	return rs, nil
}
