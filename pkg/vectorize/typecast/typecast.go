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
	Int16ToInt8   = NumericToNumeric[int16, int8]
	Int32ToInt8   = NumericToNumeric[int32, int8]
	Int64ToInt8   = NumericToNumeric[int64, int8]
	Uint8ToInt8   = NumericToNumeric[uint8, int8]
	Uint16ToInt8  = NumericToNumeric[uint16, int8]
	Uint32ToInt8  = NumericToNumeric[uint32, int8]
	Uint64ToInt8  = NumericToNumeric[uint64, int8]
	Float32ToInt8 = NumericToNumeric[float32, int8]
	Float64ToInt8 = NumericToNumeric[float64, int8]

	Int8ToInt16    = NumericToNumeric[int8, int16]
	Int32ToInt16   = NumericToNumeric[int32, int16]
	Int64ToInt16   = NumericToNumeric[int64, int16]
	Uint8ToInt16   = NumericToNumeric[uint8, int16]
	Uint16ToInt16  = NumericToNumeric[uint16, int16]
	Uint32ToInt16  = NumericToNumeric[uint32, int16]
	Uint64ToInt16  = NumericToNumeric[uint64, int16]
	Float32ToInt16 = NumericToNumeric[float32, int16]
	Float64ToInt16 = NumericToNumeric[float64, int16]

	Int8ToInt32    = NumericToNumeric[int8, int32]
	Int16ToInt32   = NumericToNumeric[int16, int32]
	Int64ToInt32   = NumericToNumeric[int64, int32]
	Uint8ToInt32   = NumericToNumeric[uint8, int32]
	Uint16ToInt32  = NumericToNumeric[uint16, int32]
	Uint32ToInt32  = NumericToNumeric[uint32, int32]
	Uint64ToInt32  = NumericToNumeric[uint64, int32]
	Float32ToInt32 = NumericToNumeric[float32, int32]
	Float64ToInt32 = NumericToNumeric[float64, int32]

	Int8ToInt64    = NumericToNumeric[int8, int64]
	Int16ToInt64   = NumericToNumeric[int16, int64]
	Int32ToInt64   = NumericToNumeric[int32, int64]
	Uint8ToInt64   = NumericToNumeric[uint8, int64]
	Uint16ToInt64  = NumericToNumeric[uint16, int64]
	Uint32ToInt64  = NumericToNumeric[uint32, int64]
	Uint64ToInt64  = NumericToNumeric[uint64, int64]
	Float32ToInt64 = NumericToNumeric[float32, int64]
	Float64ToInt64 = NumericToNumeric[float64, int64]

	Int8ToUint8    = NumericToNumeric[int8, uint8]
	Int16ToUint8   = NumericToNumeric[int16, uint8]
	Int32ToUint8   = NumericToNumeric[int32, uint8]
	Int64ToUint8   = NumericToNumeric[int64, uint8]
	Uint16ToUint8  = NumericToNumeric[uint16, uint8]
	Uint32ToUint8  = NumericToNumeric[uint32, uint8]
	Uint64ToUint8  = NumericToNumeric[uint64, uint8]
	Float32ToUint8 = NumericToNumeric[float32, uint8]
	Float64ToUint8 = NumericToNumeric[float64, uint8]

	Int8ToUint16    = NumericToNumeric[int8, uint16]
	Int16ToUint16   = NumericToNumeric[int16, uint16]
	Int32ToUint16   = NumericToNumeric[int32, uint16]
	Int64ToUint16   = NumericToNumeric[int64, uint16]
	Uint8ToUint16   = NumericToNumeric[uint8, uint16]
	Uint32ToUint16  = NumericToNumeric[uint32, uint16]
	Uint64ToUint16  = NumericToNumeric[uint64, uint16]
	Float32ToUint16 = NumericToNumeric[float32, uint16]
	Float64ToUint16 = NumericToNumeric[float64, uint16]

	Int8ToUint32    = NumericToNumeric[int8, uint32]
	Int16ToUint32   = NumericToNumeric[int16, uint32]
	Int32ToUint32   = NumericToNumeric[int32, uint32]
	Int64ToUint32   = NumericToNumeric[int64, uint32]
	Uint8ToUint32   = NumericToNumeric[uint8, uint32]
	Uint16ToUint32  = NumericToNumeric[uint16, uint32]
	Uint64ToUint32  = NumericToNumeric[uint64, uint32]
	Float32ToUint32 = NumericToNumeric[float32, uint32]
	Float64ToUint32 = NumericToNumeric[float64, uint32]

	Int8ToUint64    = NumericToNumeric[int8, uint64]
	Int16ToUint64   = NumericToNumeric[int16, uint64]
	Int32ToUint64   = NumericToNumeric[int32, uint64]
	Int64ToUint64   = NumericToNumeric[int64, uint64]
	Uint8ToUint64   = NumericToNumeric[uint8, uint64]
	Uint16ToUint64  = NumericToNumeric[uint16, uint64]
	Uint32ToUint64  = NumericToNumeric[uint32, uint64]
	Float32ToUint64 = NumericToNumeric[float32, uint64]
	Float64ToUint64 = NumericToNumeric[float64, uint64]

	Int8ToFloat32    = NumericToNumeric[int8, float32]
	Int16ToFloat32   = NumericToNumeric[int16, float32]
	Int32ToFloat32   = NumericToNumeric[int32, float32]
	Int64ToFloat32   = NumericToNumeric[int64, float32]
	Uint8ToFloat32   = NumericToNumeric[uint8, float32]
	Uint16ToFloat32  = NumericToNumeric[uint16, float32]
	Uint32ToFloat32  = NumericToNumeric[uint32, float32]
	Uint64ToFloat32  = NumericToNumeric[uint64, float32]
	Float64ToFloat32 = NumericToNumeric[float64, float32]

	Int8ToFloat64    = NumericToNumeric[int8, float64]
	Int16ToFloat64   = NumericToNumeric[int16, float64]
	Int32ToFloat64   = NumericToNumeric[int32, float64]
	Int64ToFloat64   = NumericToNumeric[int64, float64]
	Uint8ToFloat64   = NumericToNumeric[uint8, float64]
	Uint16ToFloat64  = NumericToNumeric[uint16, float64]
	Uint32ToFloat64  = NumericToNumeric[uint32, float64]
	Uint64ToFloat64  = NumericToNumeric[uint64, float64]
	Float32ToFloat64 = NumericToNumeric[float32, float64]

	BytesToInt8    = BytesToInt[int8]
	Int8ToBytes    = IntToBytes[int8]
	BytesToInt16   = BytesToInt[int16]
	Int16ToBytes   = IntToBytes[int16]
	BytesToInt32   = BytesToInt[int32]
	Int32ToBytes   = IntToBytes[int32]
	BytesToInt64   = BytesToInt[int64]
	Int64ToBytes   = IntToBytes[int64]
	BytesToUint8   = BytesToInt[uint8]
	Uint8ToBytes   = IntToBytes[uint8]
	BytesToUint16  = BytesToInt[uint16]
	Uint16ToBytes  = IntToBytes[uint16]
	BytesToUint32  = BytesToInt[uint32]
	Uint32ToBytes  = IntToBytes[uint32]
	BytesToUint64  = BytesToInt[uint64]
	Uint64ToBytes  = IntToBytes[uint64]
	BytesToFloat32 = BytesToFloat[float32]
	Float32ToBytes = FloatToBytes[float32]
	BytesToFloat64 = BytesToFloat[float64]
	Float64ToBytes = FloatToBytes[float64]

	Decimal64ToDecimal128 = decimal64ToDecimal128Pure

	Int8ToDecimal128   = IntToDecimal128[int8]
	Int16ToDecimal128  = IntToDecimal128[int16]
	Int32ToDecimal128  = IntToDecimal128[int32]
	Int64ToDecimal128  = IntToDecimal128[int64]
	Uint8ToDecimal128  = UintToDecimal128[uint8]
	Uint16ToDecimal128 = UintToDecimal128[uint16]
	Uint32ToDecimal128 = UintToDecimal128[uint32]
	Uint64ToDecimal128 = UintToDecimal128[uint64]

	TimestampToDatetime = timestampToDatetime
)

func NumericToNumeric[T1, T2 constraints.Integer | constraints.Float](xs []T1, rs []T2) ([]T2, error) {
	for i, x := range xs {
		rs[i] = T2(x)
	}
	return rs, nil
}

func BytesToInt[T constraints.Integer](xs *types.Bytes, rs []T) ([]T, error) {
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

func IntToBytes[T constraints.Integer](xs []T, rs *types.Bytes) (*types.Bytes, error) {
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

func BytesToFloat[T constraints.Float](xs *types.Bytes, rs []T) ([]T, error) {
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

func FloatToBytes[T constraints.Float](xs []T, rs *types.Bytes) (*types.Bytes, error) {
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

func IntToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i, x := range xs {
		rs[i] = types.InitDecimal128(int64(x))
	}
	return rs, nil
}

func UintToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i, x := range xs {
		rs[i] = types.InitDecimal128UsingUint(uint64(x))
	}
	return rs, nil
}

func timestampToDatetime(xs []types.Timestamp, rs []types.Datetime) ([]types.Datetime, error) {
	return types.TimestampToDatetime(xs, rs)
}
