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

package binary

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

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
	Float32ToInt8 = FloatToIntWithoutError[float32, int8]
	Float64ToInt8 = FloatToIntWithoutError[float64, int8]

	Int8ToInt16    = NumericToNumeric[int8, int16]
	Int32ToInt16   = NumericToNumeric[int32, int16]
	Int64ToInt16   = NumericToNumeric[int64, int16]
	Uint8ToInt16   = NumericToNumeric[uint8, int16]
	Uint16ToInt16  = NumericToNumeric[uint16, int16]
	Uint32ToInt16  = NumericToNumeric[uint32, int16]
	Uint64ToInt16  = NumericToNumeric[uint64, int16]
	Float32ToInt16 = FloatToIntWithoutError[float32, int16]
	Float64ToInt16 = FloatToIntWithoutError[float64, int16]

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
	Uint64ToInt64  = uint64ToInt64 // we handle overflow error in this function
	Float32ToInt64 = FloatToIntWithoutError[float32, int64]
	Float64ToInt64 = float64ToInt64

	Int8ToUint8    = NumericToNumeric[int8, uint8]
	Int16ToUint8   = NumericToNumeric[int16, uint8]
	Int32ToUint8   = NumericToNumeric[int32, uint8]
	Int64ToUint8   = NumericToNumeric[int64, uint8]
	Uint16ToUint8  = NumericToNumeric[uint16, uint8]
	Uint32ToUint8  = NumericToNumeric[uint32, uint8]
	Uint64ToUint8  = NumericToNumeric[uint64, uint8]
	Float32ToUint8 = FloatToIntWithoutError[float32, uint8]
	Float64ToUint8 = FloatToIntWithoutError[float64, uint8]

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
	Float32ToUint32 = FloatToIntWithoutError[float32, uint32]
	Float64ToUint32 = FloatToIntWithoutError[float64, uint32]

	Int8ToUint64    = NumericToNumeric[int8, uint64]
	Int16ToUint64   = NumericToNumeric[int16, uint64]
	Int32ToUint64   = NumericToNumeric[int32, uint64]
	Int64ToUint64   = int64ToUint64
	Uint8ToUint64   = NumericToNumeric[uint8, uint64]
	Uint16ToUint64  = NumericToNumeric[uint16, uint64]
	Uint32ToUint64  = NumericToNumeric[uint32, uint64]
	Float32ToUint64 = FloatToIntWithoutError[float32, uint64]
	Float64ToUint64 = FloatToIntWithoutError[float64, uint64]

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
	BytesToUint8   = BytesToUint[uint8]
	Uint8ToBytes   = IntToBytes[uint8]
	BytesToUint16  = BytesToUint[uint16]
	Uint16ToBytes  = IntToBytes[uint16]
	BytesToUint32  = BytesToUint[uint32]
	Uint32ToBytes  = IntToBytes[uint32]
	BytesToUint64  = BytesToUint[uint64]
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
	DatetimeToTimestamp = datetimeToTimestamp
	DateToTimestamp     = dateToTimestamp
	TimestampToVarchar  = timestampToVarchar
	BoolToBytes         = boolToBytes
	DateToBytes         = dateToBytes
	DateToDatetime      = dateToDateTime
	DateTimeToBytes     = datetimeToBytes
	DateTimeToDate      = datetimeToDate
)

func NumericToNumeric[T1, T2 constraints.Integer | constraints.Float](xs []T1, rs []T2) ([]T2, error) {
	for i, x := range xs {
		rs[i] = T2(x)
	}
	return rs, nil
}

func FloatToIntWithoutError[T1 constraints.Float, T2 constraints.Integer](xs []T1, rs []T2) ([]T2, error) {
	for i, x := range xs {
		rs[i] = T2(math.Round(float64(x)))
	}
	return rs, nil
}

func float64ToInt64(xs []float64, rs []int64, isEmptyStringOrNull ...[]int) ([]int64, error) {
	usedEmptyStringOrNull := len(isEmptyStringOrNull) > 0
	for i, x := range xs {
		if x > math.MaxInt64 || x < math.MinInt64 {
			if usedEmptyStringOrNull {
				isEmptyStringOrNull[0][i] = 1
			} else {
				return nil, moerr.NewError(moerr.OUT_OF_RANGE, "overflow from double to bigint")
			}
		}
		rs[i] = int64(math.Round((x)))
	}
	return rs, nil
}

func uint64ToInt64(xs []uint64, rs []int64) ([]int64, error) {
	overflowFlag := uint64(0)
	for i, x := range xs {
		rs[i] = int64(x)
		overflowFlag |= x >> 63
	}
	if overflowFlag != 0 {
		return nil, moerr.NewError(moerr.OUT_OF_RANGE, "overflow from bigint unsigned to bigint")
	}
	return rs, nil
}

func int64ToUint64(xs []int64, rs []uint64) ([]uint64, error) {
	overflowFlag := int64(0)
	for i, x := range xs {
		rs[i] = uint64(x)
		overflowFlag |= x >> 63
	}
	if overflowFlag != 0 {
		return nil, moerr.NewError(moerr.OUT_OF_RANGE, "overflow from bigint to bigint unsigned")
	}
	return rs, nil
}

func BytesToInt[T constraints.Signed](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8

	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(strings.TrimSpace(s), 10, bitSize)
		if err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				return nil, fmt.Errorf("overflow when cast '%s' as type of integer", s)
			}
			return nil, fmt.Errorf("can't cast '%s' as type of integer", s)
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func BytesToUint[T constraints.Unsigned](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8

	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseUint(strings.TrimSpace(s), 10, bitSize)
		if err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				return nil, fmt.Errorf("overflow when cast '%s' as type of unsigned integer", s)
			}
			return nil, fmt.Errorf("can't cast '%s' as type of unsigned integer", s)
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

func UintToBytes[T constraints.Integer](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendUint(rs.Data, uint64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func Decimal64ToBytes(xs []types.Decimal64, rs *types.Bytes, scale int32) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = append(rs.Data, x.ToStringWithScale(scale)...)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func Decimal128ToBytes(xs []types.Decimal128, rs *types.Bytes, scale int32) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = append(rs.Data, x.ToStringWithScale(scale)...)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToFloat[T constraints.Float](xs *types.Bytes, rs []T, isEmptyStringOrNull ...[]int) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8
	usedEmptyStringOrNull := len(isEmptyStringOrNull) > 0
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseFloat(s, bitSize)
		if err != nil {
			if usedEmptyStringOrNull {
				if !strings.Contains(err.Error(), "value out of range") {
					isEmptyStringOrNull[0][i] = 2
				} else {
					isEmptyStringOrNull[0][i] = 1
				}
			} else {
				return nil, err
			}
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
		rs[i] = types.Decimal128_FromDecimal64(x)
	}
	return rs, nil
}

func IntToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i, x := range xs {
		rs[i] = types.InitDecimal128(int64(x))
	}
	return rs, nil
}

func IntToDecimal64[T constraints.Integer](xs []T, rs []types.Decimal64) ([]types.Decimal64, error) {
	for i, x := range xs {
		rs[i] = types.InitDecimal64(int64(x))
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

func datetimeToTimestamp(xs []types.Datetime, rs []types.Timestamp) ([]types.Timestamp, error) {
	return types.DatetimeToTimestamp(xs, rs)
}

func dateToTimestamp(xs []types.Date, rs []types.Timestamp) ([]types.Timestamp, error) {
	return types.DateToTimestamp(xs, rs)
}

func timestampToVarchar(xs []types.Timestamp, rs *types.Bytes, precision int32) (*types.Bytes, error) {
	oldLen := uint32(0)
	for i, x := range xs {
		rs.Data = append(rs.Data, []byte(x.String2(precision))...)
		newLen := uint32(len(rs.Data))
		rs.Offsets[i] = oldLen
		rs.Lengths[i] = newLen - oldLen
		oldLen = newLen
	}
	return rs, nil
}

func boolToBytes(xs []bool, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = types.AppendBoolToByteArray(x, rs.Data)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func dateToDateTime(xs []types.Date, rs []types.Datetime) ([]types.Datetime, error) {
	for i := range xs {
		rs[i] = xs[i].ToTime()
	}
	return rs, nil
}

func dateToBytes(xs []types.Date, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = append(rs.Data, []byte(x.String())...)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func datetimeToDate(xs []types.Datetime, rs []types.Date) ([]types.Date, error) {
	for i := range xs {
		rs[i] = xs[i].ToDate()
	}
	return rs, nil
}

func datetimeToBytes(xs []types.Datetime, rs *types.Bytes, precision int32) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = append(rs.Data, []byte(x.String2(precision))...)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func NumericToTimestamp[T constraints.Integer](xs []T, rs []types.Timestamp) ([]types.Timestamp, error) {
	for i, x := range xs {
		rs[i] = types.UnixToTimestamp(int64(x))
	}
	return rs, nil
}

func Decimal64ToTimestamp(xs []types.Decimal64, precision int32, scale int32, rs []types.Timestamp) ([]types.Timestamp, error) {
	for i, x := range xs {
		ts := x.ToInt64()
		rs[i] = types.Timestamp(ts)
	}
	return rs, nil
}

func Decimal128ToTimestamp(xs []types.Decimal128, precision int32, scale int32, rs []types.Timestamp) ([]types.Timestamp, error) {
	for i, x := range xs {
		rs[i] = types.Timestamp(x.ToInt64())
	}
	return rs, nil
}

func Decimal64ToFloat32(xs []types.Decimal64, scale int32, rs []float32) ([]float32, error) {
	for i, x := range xs {
		xStr := string(x.ToStringWithScale(scale))
		result, err := strconv.ParseFloat(xStr, 32)
		if err != nil {
			return []float32{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to float correctly")
		}
		rs[i] = float32(result)
	}
	return rs, nil
}

func Decimal128ToFloat32(xs []types.Decimal128, scale int32, rs []float32) ([]float32, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		result, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []float32{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to float correctly")
		}
		rs[i] = float32(result)
	}
	return rs, nil
}

func Decimal64ToFloat64(xs []types.Decimal64, scale int32, rs []float64) ([]float64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		result, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []float64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to float correctly")
		}
		rs[i] = result
	}
	return rs, nil
}

func Decimal128ToFloat64(xs []types.Decimal128, scale int32, rs []float64) ([]float64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		result, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []float64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to float correctly")
		}
		rs[i] = result
	}
	return rs, nil
}

func Decimal64ToInt64(xs []types.Decimal64, scale int32, rs []int64) ([]int64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		floatRepresentation, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []int64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT correctly")
		}

		if floatRepresentation > math.MaxInt64 || floatRepresentation < math.MinInt64 {
			return []int64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT correctly")
		}

		result := int64(math.Round(floatRepresentation))
		rs[i] = result
	}
	return rs, nil
}

func Decimal128ToInt64(xs []types.Decimal128, scale int32, rs []int64) ([]int64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		floatRepresentation, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []int64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT correctly")
		}

		if floatRepresentation > math.MaxInt64 || floatRepresentation < math.MinInt64 {
			return []int64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT correctly")
		}

		result := int64(math.Round(floatRepresentation))
		rs[i] = result
	}
	return rs, nil
}

func Decimal128ToInt32(xs []types.Decimal128, scale int32, rs []int32) ([]int32, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		floatRepresentation, err := strconv.ParseFloat(xStr, 64)
		if err != nil {
			return []int32{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to INT correctly")
		}

		if floatRepresentation > math.MaxInt32 || floatRepresentation < math.MinInt32 {
			return []int32{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to INT correctly")
		}

		result := int32(math.Round(floatRepresentation))
		rs[i] = result
	}
	return rs, nil
}

func Decimal64ToUint64(xs []types.Decimal64, scale int32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		xStr = strings.Split(xStr, ".")[0]
		xVal, err := strconv.ParseUint(xStr, 10, 64)
		if err != nil {
			return []uint64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT UNSIGNED correctly")
		}
		rs[i] = xVal
	}
	return rs, nil
}

func Decimal128ToUint64(xs []types.Decimal128, scale int32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		xStr = strings.Split(xStr, ".")[0]
		xVal, err := strconv.ParseUint(xStr, 10, 64)
		if err != nil {
			return []uint64{}, moerr.NewError(moerr.OUT_OF_RANGE, "cannot convert decimal to BIGINT UNSIGNED correctly")
		}
		rs[i] = xVal
	}
	return rs, nil
}

// the scale of decimal128 is guaranteed to be less than 18
// this cast function is too slow, and therefore only temporary, rewrite needed
func Decimal128ToDecimal64(xs []types.Decimal128, xsScale int32, ysPrecision, ysScale int32, rs []types.Decimal64) ([]types.Decimal64, error) {
	var err error
	for i, x := range xs {
		rs[i], _ = x.ToDecimal64()
		if err != nil {
			return []types.Decimal64{}, moerr.NewError(moerr.OUT_OF_RANGE, fmt.Sprintf("cannot convert to Decimal(%d, %d) correctly", ysPrecision, ysScale))
		}
	}
	return rs, nil
}

func Decimal128ToDecimal128(xs []types.Decimal128, scale int32, rs []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		rs[i], err = types.Decimal128_FromString(xStr)
		if err != nil {
			return []types.Decimal128{}, moerr.NewError(moerr.OUT_OF_RANGE, fmt.Sprintf("cannot convert to Decimal(34, %d) correctly", scale))
		}
	}
	return rs, nil
}

func NumericToBool[T constraints.Integer | constraints.Float](xs []T, rs []bool) ([]bool, error) {
	for i, x := range xs {
		if x == 0 {
			rs[i] = false
		} else if x == 1 {
			rs[i] = true
		} else {
			return nil, moerr.NewError(moerr.INVALID_ARGUMENT, fmt.Sprintf("Can't cast '%v' as boolean type.", x))
		}
	}
	return rs, nil
}
