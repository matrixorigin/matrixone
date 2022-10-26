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
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"
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
	Uint8ToBytes   = UintToBytes[uint8]
	BytesToUint16  = BytesToUint[uint16]
	Uint16ToBytes  = UintToBytes[uint16]
	BytesToUint32  = BytesToUint[uint32]
	Uint32ToBytes  = UintToBytes[uint32]
	BytesToUint64  = BytesToUint[uint64]
	Uint64ToBytes  = UintToBytes[uint64]
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
	TimestampToVarchar  = timestampToVarchar
	BoolToBytes         = boolToBytes
	DateToBytes         = dateToBytes
	DateToDatetime      = dateToDateTime
	DateToTime          = dateToTime
	DatetimeToBytes     = datetimeToBytes
	DatetimeToTime      = datetimeToTime
	TimeToBytes         = timeToBytes
	TimeToDate          = timeToDate
	TimeToDatetime      = timeToDatetime
	DatetimeToDate      = datetimeToDate
	UuidToBytes         = uuidToBytes
)

func NumericToNumeric[T1, T2 constraints.Integer | constraints.Float](xs []T1, rs []T2) ([]T2, error) {
	if err := NumericToNumericOverflow(xs, rs); err != nil {
		return nil, err
	}
	for i, x := range xs {
		rs[i] = T2(x)
	}
	return rs, nil
}

func NumericToNumericOverflow[T1, T2 constraints.Integer | constraints.Float](xs []T1, rs []T2) error {
	var t1 T1
	var t2 T2
	var li interface{} = &t1
	var ri interface{} = &t2
	switch li.(type) {
	case *int8:
		switch ri.(type) {
		case *uint8, *uint16, *uint32, *uint64:
			for _, x := range xs {
				if x < 0 {
					return moerr.NewOutOfRange("uint", "value '%v'", x)
				}
			}
		}
	case *int16:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if int16(x) < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16, *uint32, *uint64:
			for _, x := range xs {
				if x < 0 {
					return moerr.NewOutOfRange("uint", "value '%v'", x)
				}
			}
		}
	case *int32:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if int32(x) < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if int32(x) < math.MinInt16 || int32(x) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		case *uint32, *uint64:
			for _, x := range xs {
				if x < 0 {
					return moerr.NewOutOfRange("uint", "value '%v'", x)
				}
			}
		}
	case *int64:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if int64(x) < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if int64(x) < math.MinInt16 || int64(x) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range xs {
				if int64(x) < math.MinInt32 || int64(x) > math.MaxInt32 {
					return moerr.NewOutOfRange("int32", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range xs {
				if x < 0 || int64(x) > math.MaxUint32 {
					return moerr.NewOutOfRange("uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range xs {
				if x < 0 {
					return moerr.NewOutOfRange("uint64", "value '%v'", x)
				}
			}
		}
	case *uint8:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		}
	case *uint16:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if uint16(x) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if uint16(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		}
	case *uint32:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if uint32(x) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range xs {
				if uint32(x) > math.MaxInt32 {
					return moerr.NewOutOfRange("int32", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if uint32(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if uint32(x) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		}
	case *uint64:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if uint64(x) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range xs {
				if uint64(x) > math.MaxInt32 {
					return moerr.NewOutOfRange("int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range xs {
				if uint64(x) > math.MaxInt64 {
					return moerr.NewOutOfRange("int64", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if uint64(x) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if uint64(x) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range xs {
				if uint64(x) > math.MaxUint32 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		}
	case *float32:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt32 {
					return moerr.NewOutOfRange("int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt64 {
					return moerr.NewOutOfRange("int64", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint32 {
					return moerr.NewOutOfRange("uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint64 {
					return moerr.NewOutOfRange("uint64", "value '%v'", x)
				}
			}
		}
	case *float64:
		switch ri.(type) {
		case *int8:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt8 {
					return moerr.NewOutOfRange("int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt16 {
					return moerr.NewOutOfRange("int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt32 {
					return moerr.NewOutOfRange("int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxInt64 || math.Round(float64(x)) < math.MinInt64 {
					return moerr.NewOutOfRange("int64", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint8 {
					return moerr.NewOutOfRange("uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint16 {
					return moerr.NewOutOfRange("uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint32 {
					return moerr.NewOutOfRange("uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range xs {
				if math.Round(float64(x)) > math.MaxUint64 {
					return moerr.NewOutOfRange("uint64", "value '%v'", x)
				}
			}
		case *float32:
			for _, x := range xs {
				if float64(x) > math.MaxFloat32 {
					return moerr.NewOutOfRange("float32", "value '%v'", x)
				}
			}
		}
	}
	return nil
}

func FloatToIntWithoutError[T1 constraints.Float, T2 constraints.Integer](xs []T1, rs []T2) ([]T2, error) {
	if err := NumericToNumericOverflow(xs, rs); err != nil {
		return nil, err
	}
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
				return nil, moerr.NewOutOfRange("int64", "value '%v'", x)
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
		return nil, moerr.NewOutOfRange("int64", "")
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
		return nil, moerr.NewOutOfRange("uint64", "")
	}
	return rs, nil
}

func BytesToInt[T constraints.Signed](xs []string, rs []T, isBin ...bool) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8
	var err error
	var val int64
	for i, s := range xs {
		if len(isBin) > 0 && isBin[0] {
			val, err = strconv.ParseInt(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s))), 16, 64)
			if err != nil {
				if strings.Contains(err.Error(), "value out of range") {
					return nil, moerr.NewOutOfRange("int", "") // the string maybe non-visibile,don't print it
				}
				return nil, moerr.NewInvalidArg("cast to int", s)
			}
		} else {
			val, err = strconv.ParseInt(strings.TrimSpace(s), 10, bitSize)
			if err != nil {
				if strings.Contains(err.Error(), "value out of range") {
					return nil, moerr.NewOutOfRange("int", "value '%v'", s)
				}
				return nil, moerr.NewInvalidArg("cast to int", s)
			}
		}
		if err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				return nil, moerr.NewOutOfRange("int", "value '%v'", s)
			}
			return nil, moerr.NewInvalidArg("cast to int", s)
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func BytesToUint[T constraints.Unsigned](xs []string, rs []T, isBin ...bool) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8
	var err error
	var val uint64
	for i, s := range xs {
		if len(isBin) > 0 && isBin[0] {
			val, err = strconv.ParseUint(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s))), 16, 64)
		} else {
			val, err = strconv.ParseUint(strings.TrimSpace(s), 10, bitSize)
		}
		if err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				return nil, moerr.NewOutOfRange("uint", "value '%v'", s)
			}
			return nil, moerr.NewInvalidArg("cast to uint", s)
		}
		rs[i] = T(val)
	}
	return rs, nil
}

// func UtilIntToBytes[T constraints.Integer](n T) []byte {
// 	x := uint64(n)
// 	bytesBuffer := bytes.NewBuffer([]byte{})
// 	binary.Write(bytesBuffer, binary.BigEndian, x)
// 	bytes := bytesBuffer.Bytes()
// 	var res []byte
// 	for i := range bytes {
// 		if bytes[i] != 0 {
// 			res = append(res, bytes[i])
// 		}
// 	}
// 	if len(res) == 0 {
// 		res = append(res, 0)
// 	}
// 	return res
// }

// XXX Potentially we can do much better with types.Varlena
func IntToBytes[T constraints.Integer](xs []T, rs []string, ZeroAndBin ...int64) ([]string, error) {
	for i, x := range xs {
		rs[i] = strconv.FormatInt(int64(x), 10)
	}
	return rs, nil
}

// XXX Potentially we can do much better with types.Varlena
func UintToBytes[T constraints.Integer](xs []T, rs []string) ([]string, error) {
	for i, x := range xs {
		rs[i] = strconv.FormatUint(uint64(x), 10)
	}
	return rs, nil
}

func Decimal64ToBytes(xs []types.Decimal64, rs []string, scale int32) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.ToStringWithScale(scale)
	}
	return rs, nil
}

func Decimal128ToBytes(xs []types.Decimal128, rs []string, scale int32) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.ToStringWithScale(scale)
	}
	return rs, nil
}

func BytesToFloat[T constraints.Float](xs []string, rs []T, isBin bool, isEmptyStringOrNull ...[]int) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8
	usedEmptyStringOrNull := len(isEmptyStringOrNull) > 0
	for i, s := range xs {
		var err error
		var val float64
		if isBin {
			// is there a better way to cast a 0xXXX to a float? let me do like below first
			Uval, err := strconv.ParseUint(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s))), 16, 64)
			if err != nil {
				if strings.Contains(err.Error(), "value out of range") {
					return nil, moerr.NewOutOfRange("float", "value '%v'", s)
				}
				return nil, moerr.NewInvalidArg("cast to float", s)
			}
			val, _ = strconv.ParseFloat(strconv.FormatUint(Uval, 10), bitSize)
		} else {
			val, err = strconv.ParseFloat(s, bitSize)
		}
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

func FloatToBytes[T constraints.Float](xs []T, rs []string) ([]string, error) {
	var bitSize = int(unsafe.Sizeof(T(0))) * 8
	for i, x := range xs {
		rs[i] = strconv.FormatFloat(float64(x), 'G', -1, bitSize)
	}
	return rs, nil
}

func decimal64ToDecimal128Pure(xs []types.Decimal64, rs []types.Decimal128, width, scale int32) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		if rs[i], err = types.Decimal128_FromDecimal64WithScale(x, width, scale); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func IntToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128, width, scale int32) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		if rs[i], err = types.InitDecimal128(int64(x), width, scale); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func IntToDecimal64[T constraints.Integer](xs []T, rs []types.Decimal64, width, scale int32) ([]types.Decimal64, error) {
	var err error
	for i, x := range xs {
		if rs[i], err = types.InitDecimal64(int64(x), width, scale); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func UintToDecimal128[T constraints.Integer](xs []T, rs []types.Decimal128, width, scale int32) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		if rs[i], err = types.InitDecimal128UsingUint(uint64(x), width, scale); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func timestampToDatetime(loc *time.Location, xs []types.Timestamp, rs []types.Datetime) ([]types.Datetime, error) {
	return types.TimestampToDatetime(loc, xs, rs)
}

func timestampToVarchar(loc *time.Location, xs []types.Timestamp, rs []string, precision int32) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.String2(loc, precision)
	}
	return rs, nil
}

func boolToBytes(xs []bool, rs []string) ([]string, error) {
	for i, x := range xs {
		rs[i] = types.BoolToIntString(x)
	}
	return rs, nil
}

func dateToDateTime(xs []types.Date, rs []types.Datetime) ([]types.Datetime, error) {
	for i := range xs {
		rs[i] = xs[i].ToDatetime()
	}
	return rs, nil
}

func dateToTime(xs []types.Date, rs []types.Time) ([]types.Time, error) {
	for i := range xs {
		rs[i] = xs[i].ToTime()
	}
	return rs, nil
}

func dateToBytes(xs []types.Date, rs []string) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.String()
	}
	return rs, nil
}

func datetimeToDate(xs []types.Datetime, rs []types.Date) ([]types.Date, error) {
	for i := range xs {
		rs[i] = xs[i].ToDate()
	}
	return rs, nil
}

func datetimeToBytes(xs []types.Datetime, rs []string, precision int32) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.String2(precision)
	}
	return rs, nil
}
func datetimeToTime(xs []types.Datetime, rs []types.Time, precision int32) ([]types.Time, error) {
	for i, x := range xs {
		rs[i] = x.ToTime(precision)
	}
	return rs, nil
}

func timeToBytes(xs []types.Time, rs []string, precision int32) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.String2(precision)
	}
	return rs, nil
}

func timeToDatetime(xs []types.Time, rs []types.Datetime, precision int32) ([]types.Datetime, error) {
	for i, x := range xs {
		rs[i] = x.ToDatetime(precision)
	}
	return rs, nil
}

func timeToDate(xs []types.Time, rs []types.Date) ([]types.Date, error) {
	for i, x := range xs {
		rs[i] = x.ToDate()
	}
	return rs, nil
}

func uuidToBytes(xs []types.Uuid, rs []string) ([]string, error) {
	for i, x := range xs {
		rs[i] = x.ToString()
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
			return []float32{}, moerr.NewOutOfRange("float32", "value '%v'", xStr)
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
			return []float32{}, moerr.NewOutOfRange("float32", "value '%v'", xStr)
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
			return []float64{}, moerr.NewOutOfRange("float64", "value '%v'", xStr)
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
			return []float64{}, moerr.NewOutOfRange("float64", "value '%v'", xStr)
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
			return []int64{}, moerr.NewOutOfRange("int64", "value '%v'", xStr)
		}

		if floatRepresentation > math.MaxInt64 || floatRepresentation < math.MinInt64 {
			return []int64{}, moerr.NewOutOfRange("int64", "value '%v'", xStr)
		}

		result := int64(math.Round(floatRepresentation))
		rs[i] = result
	}
	return rs, nil
}

func Decimal128ToInt64(xs []types.Decimal128, scale int32, rs []int64) ([]int64, error) {
	var err error
	for i, x := range xs {
		xStr := x.ToStringWithScale(0)
		rs[i], err = strconv.ParseInt(xStr, 10, 64)
		if err != nil {
			return []int64{}, moerr.NewOutOfRange("int64", "value '%v'", xStr)
		}
	}
	return rs, nil
}

func Decimal128ToInt32(xs []types.Decimal128, scale int32, rs []int32) ([]int32, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(0)
		ret, err := strconv.ParseInt(xStr, 10, 32)
		if err != nil {
			return []int32{}, moerr.NewOutOfRange("int32", "value '%v'", xStr)
		}
		rs[i] = int32(ret)
	}
	return rs, nil
}

func Decimal64ToUint64(xs []types.Decimal64, scale int32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		xStr = strings.Split(xStr, ".")[0]
		xVal, err := strconv.ParseUint(xStr, 10, 64)
		if err != nil {
			return []uint64{}, moerr.NewOutOfRange("uint64", "value '%v'", xStr)
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
			return []uint64{}, moerr.NewOutOfRange("uint64", "value '%v'", xStr)
		}
		rs[i] = xVal
	}
	return rs, nil
}

// the scale of decimal128 is guaranteed to be less than 18
// this cast function is too slow, and therefore only temporary, rewrite needed
func Decimal128ToDecimal64(xs []types.Decimal128, width, scale int32, rs []types.Decimal64) ([]types.Decimal64, error) {
	var err error
	for i, x := range xs {
		rs[i], err = x.ToDecimal64(width, scale)
		if err != nil {
			return []types.Decimal64{}, moerr.NewOutOfRange("dec64", "value '%v'", x)
		}
	}
	return rs, nil
}

func Decimal128ToDecimal128(xs []types.Decimal128, width, scale int32, rs []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		xStr := x.ToStringWithScale(scale)
		rs[i], err = types.Decimal128_FromString(xStr)
		if err != nil {
			return []types.Decimal128{}, moerr.NewOutOfRange("dec128", "value '%v'", x)
		}
	}
	return rs, nil
}

func BinaryByteToDecimal128(xs []string, rs []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i, x := range xs {
		rs[i], err = types.Decimal128_FromString(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&x))))
		if err != nil {
			return []types.Decimal128{}, moerr.NewOutOfRange("dec128", "value '%v'", x)
		}
	}
	return rs, nil
}

func NumericToBool[T constraints.Integer | constraints.Float](xs []T, rs []bool) ([]bool, error) {
	for i, x := range xs {
		if x == 0 {
			rs[i] = false
		} else {
			rs[i] = true
		}
	}
	return rs, nil
}

func BoolToNumeric[T constraints.Integer | constraints.Float](xs []bool, rs []T) ([]T, error) {
	for i, x := range xs {
		if x {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs, nil
}
