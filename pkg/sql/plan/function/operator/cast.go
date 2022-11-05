// Copyright 2022 Matrix Origin
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

package operator

import (
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vectorize/timestamp"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Cast(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec, err := doCast(vs, proc)
	return vec, err
}

// shorten the string to the one with no more than 101 characters.
func shortenValueString(valueStr string) string {
	utf8Str := []rune(valueStr)
	l := len(utf8Str)
	if l > 100 {
		return string(utf8Str[:100]) + "..."
	}
	return valueStr
}

func formatCastError(vec *vector.Vector, typ types.Type, extraInfo string) error {
	var errStr string
	if vec.IsScalar() {
		if vec.ConstVectorIsNull() {
			errStr = fmt.Sprintf("Can't cast 'NULL' as %v type.", typ)
		} else {
			valueStr := strings.TrimRight(strings.TrimLeft(fmt.Sprintf("%v", vec), "["), "]")
			shortenValueStr := shortenValueString(valueStr)
			errStr = fmt.Sprintf("Can't cast '%s' from %v type to %v type.", shortenValueStr, vec.Typ, typ)
		}
	} else {
		errStr = fmt.Sprintf("Can't cast column from %v type to %v type because of one or more values in that column.", vec.Typ, typ)
	}
	return moerr.NewInternalError(errStr + extraInfo)
}

func doCast(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vs[0]
	rv := vs[1]
	if rv.IsScalarNull() {
		return nil, formatCastError(lv, rv.Typ, "the target type of cast function cannot be null")
	}
	if lv.IsScalarNull() {
		return proc.AllocScalarNullVector(rv.Typ), nil
	}

	if lv.Typ.Oid == rv.Typ.Oid && lv.Typ.IsFixedLen() {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastSameType[int8](lv, rv, proc)
		case types.T_int16:
			return CastSameType[int16](lv, rv, proc)
		case types.T_int32:
			return CastSameType[int32](lv, rv, proc)
		case types.T_int64:
			return CastSameType[int64](lv, rv, proc)
		case types.T_uint8:
			return CastSameType[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastSameType[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastSameType[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastSameType[uint64](lv, rv, proc)
		case types.T_float32:
			return CastSameType[float32](lv, rv, proc)
		case types.T_float64:
			return CastSameType[float64](lv, rv, proc)
		case types.T_date:
			return CastSameType[types.Date](lv, rv, proc)
		case types.T_time:
			return CastSameType[types.Time](lv, rv, proc)
		case types.T_datetime:
			return CastSameType[types.Datetime](lv, rv, proc)
		case types.T_timestamp:
			return CastSameType[types.Timestamp](lv, rv, proc)
		case types.T_decimal64:
			return CastSameType[types.Decimal64](lv, rv, proc)
		case types.T_decimal128:
			return CastSameType[types.Decimal128](lv, rv, proc)
		case types.T_TS:
			return CastSameType[types.TS](lv, rv, proc)
		case types.T_Rowid:
			return CastSameType[types.Rowid](lv, rv, proc)
		default:
			panic("unknow type in case same type of fixed size.")
		}
	}

	if lv.Typ.Oid != rv.Typ.Oid && IsNumeric(lv.Typ.Oid) && IsNumeric(rv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_int8:
			switch rv.Typ.Oid {
			case types.T_int16:
				return CastLeftToRight[int8, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[int8, int32](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[int8, int64](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[int8, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[int8, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[int8, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[int8, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[int8, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[int8, float64](lv, rv, proc)
			}
		case types.T_int16:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[int16, int8](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[int16, int32](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[int16, int64](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[int16, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[int16, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[int16, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[int16, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[int16, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[int16, float64](lv, rv, proc)
			}
		case types.T_int32:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[int32, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[int32, int16](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[int32, int64](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[int32, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[int32, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[int32, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[int32, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[int32, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[int32, float64](lv, rv, proc)
			}
		case types.T_int64:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[int64, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[int64, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[int64, int32](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[int64, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[int64, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[int64, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastInt64ToUint64(lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[int64, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[int64, float64](lv, rv, proc)
			}
		case types.T_uint8:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[uint8, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[uint8, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[uint8, int32](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[uint8, int64](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[uint8, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[uint8, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[uint8, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[uint8, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[uint8, float64](lv, rv, proc)
			}
		case types.T_uint16:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[uint16, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[uint16, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[uint16, int32](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[uint16, int64](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[uint16, uint8](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[uint16, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[uint16, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[uint16, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[uint16, float64](lv, rv, proc)
			}
		case types.T_uint32:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[uint32, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[uint32, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[uint32, int32](lv, rv, proc)
			case types.T_int64:
				return CastLeftToRight[uint32, int64](lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[uint32, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[uint32, uint16](lv, rv, proc)
			case types.T_uint64:
				return CastLeftToRight[uint32, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[uint32, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[uint32, float64](lv, rv, proc)
			}
		case types.T_uint64:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastLeftToRight[uint64, int8](lv, rv, proc)
			case types.T_int16:
				return CastLeftToRight[uint64, int16](lv, rv, proc)
			case types.T_int32:
				return CastLeftToRight[uint64, int32](lv, rv, proc)
			case types.T_int64:
				return CastUint64ToInt64(lv, rv, proc)
			case types.T_uint8:
				return CastLeftToRight[uint64, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastLeftToRight[uint64, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastLeftToRight[uint64, uint32](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[uint64, float32](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[uint64, float64](lv, rv, proc)
			}
		case types.T_float32:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastFloatToInt[float32, int8](lv, rv, proc)
			case types.T_int16:
				return CastFloatToInt[float32, int16](lv, rv, proc)
			case types.T_int32:
				return CastFloatToInt[float32, int32](lv, rv, proc)
			case types.T_int64:
				return CastFloatToInt[float32, int64](lv, rv, proc)
			case types.T_uint8:
				return CastFloatToInt[float32, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastFloatToInt[float32, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastFloatToInt[float32, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastFloatToInt[float32, uint64](lv, rv, proc)
			case types.T_float64:
				return CastLeftToRight[float32, float64](lv, rv, proc)
			}
		case types.T_float64:
			switch rv.Typ.Oid {
			case types.T_int8:
				return CastFloatToInt[float64, int8](lv, rv, proc)
			case types.T_int16:
				return CastFloatToInt[float64, int16](lv, rv, proc)
			case types.T_int32:
				return CastFloatToInt[float64, int32](lv, rv, proc)
			case types.T_int64:
				return CastFloat64ToInt64(lv, rv, proc)
			case types.T_uint8:
				return CastFloatToInt[float64, uint8](lv, rv, proc)
			case types.T_uint16:
				return CastFloatToInt[float64, uint16](lv, rv, proc)
			case types.T_uint32:
				return CastFloatToInt[float64, uint32](lv, rv, proc)
			case types.T_uint64:
				return CastFloatToInt[float64, uint64](lv, rv, proc)
			case types.T_float32:
				return CastLeftToRight[float64, float32](lv, rv, proc)
			}
		}
	}

	if isString(lv.Typ.Oid) && IsInteger(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_int8:
			return CastSpecials1Int[int8](lv, rv, proc)
		case types.T_int16:
			return CastSpecials1Int[int16](lv, rv, proc)
		case types.T_int32:
			return CastSpecials1Int[int32](lv, rv, proc)
		case types.T_int64:
			return CastSpecials1Int[int64](lv, rv, proc)
		case types.T_uint8:
			return CastSpecials1Uint[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastSpecials1Uint[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastSpecials1Uint[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastSpecials1Uint[uint64](lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && IsFloat(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_float32:
			return CastSpecials1Float[float32](lv, rv, proc)
		case types.T_float64:
			return CastSpecials1Float[float64](lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && IsDecimal(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_decimal64:
			return CastStringAsDecimal64(lv, rv, proc)
		case types.T_decimal128:
			return CastStringAsDecimal128(lv, rv, proc)
		}
	}

	if IsInteger(lv.Typ.Oid) && isString(rv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastSpecials2Int[int8](lv, rv, proc)
		case types.T_int16:
			return CastSpecials2Int[int16](lv, rv, proc)
		case types.T_int32:
			return CastSpecials2Int[int32](lv, rv, proc)
		case types.T_int64:
			return CastSpecials2Int[int64](lv, rv, proc)
		case types.T_uint8:
			return CastSpecials2Uint[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastSpecials2Uint[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastSpecials2Uint[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastSpecials2Uint[uint64](lv, rv, proc)
		}
	}

	if IsFloat(lv.Typ.Oid) && isString(rv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastSpecials2Float[float32](lv, rv, proc)
		case types.T_float64:
			return CastSpecials2Float[float64](lv, rv, proc)
		}
	}
	if IsDecimal(lv.Typ.Oid) && isString(rv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_decimal64:
			return CastDecimal64ToString(lv, rv, proc)
		case types.T_decimal128:
			return CastDecimal128ToString(lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && isString(rv.Typ.Oid) {
		return CastSpecials3(lv, rv, proc)
	}

	if isSignedInteger(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal128 {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastSpecials4[int8](lv, rv, proc)
		case types.T_int16:
			return CastSpecials4[int16](lv, rv, proc)
		case types.T_int32:
			return CastSpecials4[int32](lv, rv, proc)
		case types.T_int64:
			return CastSpecials4[int64](lv, rv, proc)
		}
	}

	//The Big Number will be processed by string, it's ok
	if isSignedInteger(lv.Typ.Oid) && (rv.Typ.Oid == types.T_decimal64) {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastSpecials4_64[int8](lv, rv, proc)
		case types.T_int16:
			return CastSpecials4_64[int16](lv, rv, proc)
		case types.T_int32:
			return CastSpecials4_64[int32](lv, rv, proc)
		case types.T_int64:
			return CastSpecials4_64[int64](lv, rv, proc)
		}
	}

	if isUnsignedInteger(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal128 {
		switch lv.Typ.Oid {
		case types.T_uint8:
			return CastSpecialu4[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastSpecialu4[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastSpecialu4[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastSpecialu4[uint64](lv, rv, proc)
		}
	}

	if IsFloat(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal128 {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastFloatAsDecimal128[float32](lv, rv, proc)
		case types.T_float64:
			return CastFloatAsDecimal128[float64](lv, rv, proc)
		}
	}

	if IsFloat(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal64 {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastFloatAsDecimal64[float32](lv, rv, proc)
		case types.T_float64:
			return CastFloatAsDecimal64[float64](lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_date {
		return CastVarcharAsDate(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_time {
		return CastVarcharAsTime(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_datetime {
		return CastVarcharAsDatetime(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_timestamp {
		return CastVarcharAsTimestamp(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_decimal128 {
		return CastDecimal64AsDecimal128(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_timestamp && rv.Typ.Oid == types.T_datetime {
		return castTimeStampAsDatetime(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_datetime && rv.Typ.Oid == types.T_timestamp {
		return CastDatetimeAsTimeStamp(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_date && rv.Typ.Oid == types.T_timestamp {
		return CastDateAsTimeStamp(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_date && rv.Typ.Oid == types.T_time {
		return CastDateAsTime(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_time && rv.Typ.Oid == types.T_date {
		return CastTimeAsDate(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_timestamp && isString(rv.Typ.Oid) {
		return castTimestampAsVarchar(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_bool && isString(rv.Typ.Oid) {
		return CastBoolToString(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_date && rv.Typ.Oid == types.T_datetime {
		return CastDateAsDatetime(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_datetime && rv.Typ.Oid == types.T_date {
		return CastDatetimeAsDate(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_datetime && rv.Typ.Oid == types.T_time {
		return CastDatetimeAsTime(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_time && rv.Typ.Oid == types.T_datetime {
		return CastTimeAsDatetime(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_date && isString(rv.Typ.Oid) {
		return CastDateAsString(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_time && isString(rv.Typ.Oid) {
		return CastTimeAsString(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_datetime && isString(rv.Typ.Oid) {
		return CastDatetimeAsString(lv, rv, proc)
	}

	if IsInteger(lv.Typ.Oid) && rv.Typ.Oid == types.T_timestamp {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastIntAsTimestamp[int8](lv, rv, proc)
		case types.T_int16:
			return CastIntAsTimestamp[int16](lv, rv, proc)
		case types.T_int32:
			return CastIntAsTimestamp[int32](lv, rv, proc)
		case types.T_int64:
			return CastIntAsTimestamp[int64](lv, rv, proc)
		case types.T_uint8:
			return CastUIntAsTimestamp[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastUIntAsTimestamp[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastUIntAsTimestamp[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastUIntAsTimestamp[uint64](lv, rv, proc)
		}
	}

	if IsDecimal(lv.Typ.Oid) && rv.Typ.Oid == types.T_timestamp {
		switch lv.Typ.Oid {
		case types.T_decimal64:
			return CastDecimal64AsTimestamp(lv, rv, proc)
		case types.T_decimal128:
			return CastDecimal128AsTimestamp(lv, rv, proc)
		}
	}

	if lv.Typ.Oid == types.T_timestamp && rv.Typ.Oid == types.T_date {
		return CastTimestampAsDate(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_float32 {
		return CastDecimal64ToFloat32(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_float32 {
		return CastDecimal128ToFloat32(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_float64 {
		return CastDecimal64ToFloat64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_float64 {
		return CastDecimal128ToFloat64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_int64 {
		return CastDecimal64ToInt64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_int32 {
		return CastDecimal128ToInt32(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_int64 {
		return CastDecimal128ToInt64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_uint64 {
		return CastDecimal64ToUint64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_uint64 {
		return CastDecimal128ToUint64(lv, rv, proc)
	}
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_decimal64 {
		return CastDecimal128ToDecimal64(lv, rv, proc)
	}
	// if lv.Typ.Oid == types.T_timestamp && rv.Typ.Oid == types.T_time {
	// 	return CastTimestampAsTime(lv, rv, proc)
	// }

	if IsNumeric(lv.Typ.Oid) && rv.Typ.Oid == types.T_bool {
		switch lv.Typ.Oid {
		case types.T_int8:
			return CastNumValToBool[int8](lv, rv, proc)
		case types.T_int16:
			return CastNumValToBool[int16](lv, rv, proc)
		case types.T_int32:
			return CastNumValToBool[int32](lv, rv, proc)
		case types.T_int64:
			return CastNumValToBool[int64](lv, rv, proc)
		case types.T_uint8:
			return CastNumValToBool[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastNumValToBool[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastNumValToBool[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastNumValToBool[uint64](lv, rv, proc)
		case types.T_float32:
			return CastNumValToBool[float32](lv, rv, proc)
		case types.T_float64:
			return CastNumValToBool[float64](lv, rv, proc)
		}
	}

	if lv.Typ.Oid == types.T_bool && IsNumeric(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_int8:
			return CastBoolToNumeric[int8](lv, rv, proc)
		case types.T_int16:
			return CastBoolToNumeric[int16](lv, rv, proc)
		case types.T_int32:
			return CastBoolToNumeric[int32](lv, rv, proc)
		case types.T_int64:
			return CastBoolToNumeric[int64](lv, rv, proc)
		case types.T_uint8:
			return CastBoolToNumeric[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastBoolToNumeric[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastBoolToNumeric[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastBoolToNumeric[uint64](lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_bool {
		return CastStringToBool(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_json {
		return CastStringToJson(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_uuid {
		return CastStringToUuid(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_uuid && isString(rv.Typ.Oid) {
		return CastUuidToString(lv, rv, proc)
	}

	return nil, formatCastError(lv, rv.Typ, "")
}

func CastTimestampAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	lvs := vector.MustTCols[types.Timestamp](lv)
	if lv.IsScalar() {
		rs := make([]types.Datetime, 1)
		if _, err := binary.TimestampToDatetime(t, lvs, rs); err != nil {
			return nil, err
		}
		rs2 := make([]types.Date, 1)
		rs2[0] = rs[0].ToDate()
		vec := vector.NewConstFixed(rv.Typ, 1, rs2[0], proc.Mp())
		nulls.Set(vec.Nsp, lv.Nsp)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Date](vec)
	rs2 := make([]types.Datetime, len(lvs), cap(lvs))
	if _, err := binary.TimestampToDatetime(t, lvs, rs2); err != nil {
		return nil, err
	}
	for i := 0; i < len(rs2); i++ {
		rs[i] = rs2[i].ToDate()
	}
	return vec, nil
}

func CastDecimal64ToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		} else {
			lvs := vector.MustTCols[types.Decimal64](lv)
			col := make([]string, 1)
			if col, err = binary.Decimal64ToBytes(lvs, col, lv.Typ.Scale); err != nil {
				return nil, err
			}
			return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
		}
	} else {
		lvs := vector.MustTCols[types.Decimal64](lv)
		col := make([]string, len(lvs))
		if col, err = binary.Decimal64ToBytes(lvs, col, lv.Typ.Scale); err != nil {
			return nil, err
		}
		return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
	}
}

func CastDecimal128ToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		} else {
			lvs := vector.MustTCols[types.Decimal128](lv)
			col := make([]string, 1)
			if col, err = binary.Decimal128ToBytes(lvs, col, lv.Typ.Scale); err != nil {
				return nil, err
			}
			return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
		}
	} else {
		lvs := vector.MustTCols[types.Decimal128](lv)
		col := make([]string, len(lvs))
		if col, err = binary.Decimal128ToBytes(lvs, col, lv.Typ.Scale); err != nil {
			return nil, err
		}
		return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
	}
}

// CastSameType : Cast handles the same data type and is numeric , Contains the following:
// int8    -> int8,
// int16   -> int16,
// int32   -> int32,
// int64   -> int64,
// uint8   -> uint8,
// uint16  -> uint16,
// uint32  -> uint32,
// uint64  -> uint64,
// float32 -> float32,
// float64 -> float64,
func CastSameType[T types.FixedSizeT](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T, 1)
		copy(rs, lvs)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[T](vec)
	copy(rs, lvs)
	return vec, nil
}

// CastLeftToRight : Cast handles conversions in the form of cast (left as right), where left and right are different types,
//
//	and both left and right are numeric types, Contains the following:
//
// int8 -> (int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
// int16 -> (int8/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
// int32 -> (int8/int16/int64/uint8/uint16/uint32/uint64/float32/float64)
// int64 -> (int8/int16/int32/uint8/uint16/uint32/uint64/float32/float64)
// uint8 -> (int8/int16/int32/int64/uint16/uint32/uint64/float32/float64)
// uint16 -> (int8/int16/int32/int64/uint8/uint32/uint64/float32/float64)
// uint32 -> (int8/int16/int32/int64/uint8/uint16/uint64/float32/float64)
// uint64 -> (int8/int16/int32/int64/uint8/uint16/uint32/float32/float64)
func CastLeftToRight[T1, T2 constraints.Integer | constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T1](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T2, 1)
		if _, err := binary.NumericToNumeric(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[T2](vec)
	if _, err := binary.NumericToNumeric(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastFloatToInt[T1 constraints.Float, T2 constraints.Integer](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T1](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T2, 1)
		if _, err := binary.FloatToIntWithoutError(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[T2](vec)
	if _, err := binary.FloatToIntWithoutError(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// XXX can someone document why this one does not use the templated code?
// CastFloat64ToInt64 : cast float64 to int64
func CastFloat64ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[float64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := binary.Float64ToInt64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	if _, err := binary.Float64ToInt64(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// CastUint64ToInt64 : cast uint64 to int64
func CastUint64ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[uint64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := binary.Uint64ToInt64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	if _, err := binary.Uint64ToInt64(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// CastInt64ToUint64 : cast int64 to uint64
func CastInt64ToUint64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[int64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]uint64, 1)
		if _, err := binary.Int64ToUint64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[uint64](vec)
	if _, err := binary.Int64ToUint64(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// CastSpecials1Int : Cast converts string to integer,Contains the following:
// (char / varhcar / text) -> (int8 / int16 / int32/ int64 / uint8 / uint16 / uint32 / uint64)
func CastSpecials1Int[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	col := vector.MustStrCols(lv)
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	} else if lv.IsScalar() {
		rs := make([]T, 1)
		if _, err = binary.BytesToInt(col, rs, lv.GetIsBin()); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	} else {
		vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(col)), lv.Nsp)
		if err != nil {
			return nil, err
		}
		rs := vector.MustTCols[T](vec)
		if _, err = binary.BytesToInt(col, rs); err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rv.Typ, rs, lv.Nsp, proc.Mp()), nil
	}
}

func CastSpecials1Uint[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	col := vector.MustStrCols(lv)
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	} else if lv.IsScalar() {
		rs := make([]T, 1)
		if _, err = binary.BytesToUint(col, rs, lv.GetIsBin()); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	} else {
		vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(col)), lv.Nsp)
		if err != nil {
			return nil, err
		}
		rs := vector.MustTCols[T](vec)
		if _, err = binary.BytesToUint(col, rs); err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rv.Typ, rs, lv.Nsp, proc.Mp()), nil
	}
}

// CastSpecials1Float : Cast converts string to floating point number,Contains the following:
// (char / varhcar / text) -> (float32 / float64)
func CastSpecials1Float[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	col := vector.MustStrCols(lv)
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	} else if lv.IsScalar() {
		rs := make([]T, 1)
		if _, err = binary.BytesToFloat(col, rs, lv.GetIsBin()); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	} else {
		vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(col)), lv.Nsp)
		if err != nil {
			return nil, err
		}
		rs := vector.MustTCols[T](vec)
		if _, err = binary.BytesToFloat(col, rs, lv.GetIsBin()); err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rv.Typ, rs, lv.Nsp, proc.Mp()), nil
	}
}

// CastSpecials2Int : Cast converts integer to string,Contains the following:
// (int8 /int16/int32/int64) -> (char / varhcar / text)
func CastSpecials2Int[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	}
	lvs := vector.MustTCols[T](lv)
	col := make([]string, len(lvs))
	if col, err = binary.IntToBytes(lvs, col); err != nil {
		return nil, err
	}

	if lv.IsScalar() {
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

func CastSpecials2Uint[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	}
	lvs := vector.MustTCols[T](lv)
	col := make([]string, len(lvs))
	if col, err = binary.UintToBytes(lvs, col); err != nil {
		return nil, err
	}

	if lv.IsScalar() {
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

// CastSpecials2Float : Cast converts floating point number to string ,Contains the following:
// (float32/float64) -> (char / varhcar)
func CastSpecials2Float[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	}
	lvs := vector.MustTCols[T](lv)
	col := make([]string, len(lvs))
	if col, err = binary.FloatToBytes(lvs, col); err != nil {
		return nil, err
	}

	if lv.IsScalar() {
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

// func CastSpecials2Decimal[T constraints.decimal](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
// 	var err error
// 	lvs := lv.Col.([]T)
// 	col := &types.Bytes{
// 		Data:    make([]byte, 0, len(lvs)),
// 		Offsets: make([]uint32, 0, len(lvs)),
// 		Lengths: make([]uint32, 0, len(lvs)),
// 	}
// 	if col, err = typecast.FloatToBytes(lvs, col); err != nil {
// 		return nil, err
// 	}
// 	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
// 		return nil, err
// 	}
// 	vec := vector.New(rv.Typ)
// 	if lv.IsScalar() {
// 		vec.IsConst = true
// 	}
// 	vec.Data = col.Data
// 	nulls.Set(vec.Nsp, lv.Nsp)
// 	vector.SetCol(vec, col)
// 	return vec, nil
// }

// CastSpecials3 :  Cast converts string to string ,Contains the following:
// char -> char
// char -> varhcar
// char -> blob
// varchar -> char
// varchar -> varhcar
// varchar -> blob
// blob -> char
// blob -> varchar
// blob -> blob
// we need to consider the visiblity of 0xXXXX, the rule is a little complex,
// please do that in the future
// the rule is, if src string len is larger than the dest string len, report an error
// for example: select cast('aaaaaaa' as char(1)); will report an error here.
// insert into col(varchar(1) values 'aaaaa', report an error
// for other cases, where col(varchar(1))='aaaaa', do not report error, just return empty result. maybe we can optimize this to false?
// sometimes, the dest len is 0, then do not report error here. maybe a bug and need to fix in the future?
func CastSpecials3(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	source := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		if rv.Typ.Oid != types.T_text && int(rv.Typ.Width) != 0 && len(source[0]) > int(rv.Typ.Width) {
			errInfo := fmt.Sprintf(" Src length %v is larger than Dest length %v", len(source[0]), rv.Typ.Width)
			return nil, formatCastError(lv, rv.Typ, errInfo)
		}
		return vector.NewConstString(rv.Typ, lv.Length(), source[0], proc.Mp()), nil
	}
	destLen := int(rv.Typ.Width)
	if rv.Typ.Oid != types.T_text && destLen != 0 {
		for i, str := range source {
			if nulls.Contains(lv.Nsp, uint64(i)) {
				continue
			}
			if len(str) > destLen {
				errInfo := fmt.Sprintf(" Src length %v is larger than Dest length %v", len(str), destLen)
				return nil, formatCastError(lv, rv.Typ, errInfo)
			}
		}
	}
	return vector.NewWithStrings(rv.Typ, source, lv.Nsp, proc.Mp()), nil
}

func CastSpecialIntToDecimal[T constraints.Integer](
	lv, rv *vector.Vector,
	i2d func(xs []T, rs []types.Decimal128, width, scale int32) ([]types.Decimal128, error),
	proc *process.Process) (*vector.Vector, error) {
	resultScale := int32(0)
	resultTyp := types.T_decimal128.ToType()
	resultTyp.Scale = resultScale
	lvs := vector.MustTCols[T](lv)

	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(resultTyp, lv.Length()), nil
	} else if lv.IsScalar() {
		rs := make([]types.Decimal128, 1)
		if _, err := i2d(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(resultTyp, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(resultTyp, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal128](vec)
	if _, err := i2d(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastSpecialIntToDecimal64[T constraints.Integer](
	lv, rv *vector.Vector,
	i2d func(xs []T, rs []types.Decimal64, width, scale int32) ([]types.Decimal64, error),
	proc *process.Process) (*vector.Vector, error) {

	resultScale := int32(0)
	resultTyp := types.T_decimal64.ToType()
	resultTyp.Scale = resultScale
	lvs := vector.MustTCols[T](lv)

	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(resultTyp, lv.Length()), nil
	} else if lv.IsScalar() {
		rs := make([]types.Decimal64, 1)
		if _, err := i2d(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(resultTyp, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(resultTyp, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal64](vec)
	if _, err := i2d(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
		return nil, err
	}
	return vec, nil
}

// CastSpecials4 : Cast converts signed integer to decimal128 ,Contains the following:
// (int8/int16/int32/int64) to decimal128
func CastSpecials4[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal(lv, rv, binary.IntToDecimal128[T], proc)
}

func CastSpecials4_64[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal64(lv, rv, binary.IntToDecimal64[T], proc)
}

// CastSpecialu4 : Cast converts unsigned integer to decimal128 ,Contains the following:
// (uint8/uint16/uint32/uint64) to decimal128
func CastSpecialu4[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal(lv, rv, binary.UintToDecimal128[T], proc)
}

// XXX This is a super slow function that we need to vectorwise.
func CastFloatAsDecimal128[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	vs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		srcStr := fmt.Sprintf("%f", vs[0])
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal128, 1)
		decimal128, err := types.ParseStringToDecimal128(srcStr, resultType.Width, resultType.Scale, lv.GetIsBin())
		if err != nil {
			return nil, err
		}
		rs[0] = decimal128
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(resultType, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal128](vec)
	for i := 0; i < len(vs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		strValue := fmt.Sprintf("%f", vs[i])
		decimal128, err2 := types.ParseStringToDecimal128(strValue, resultType.Width, resultType.Scale, lv.GetIsBin())
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal128
	}
	return vec, nil
}

// XXX Super slow.
func CastFloatAsDecimal64[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	vs := vector.MustTCols[T](lv)
	var err error
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal64, 1)
		rs[0], err = types.Decimal64_FromFloat64(float64(vs[0]), resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(resultType, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal64](vec)
	for i := 0; i < len(vs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		rs[i], err = types.Decimal64_FromFloat64(float64(vs[i]), resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
	}
	return vec, nil
}

// CastVarcharAsDate : Cast converts varchar to date type
func CastVarcharAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)

	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		data, err2 := types.ParseDateCast(vs[0])
		if err2 != nil {
			return nil, err2
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), data, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Date](vec)
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		data, err2 := types.ParseDateCast(str)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = data
	}
	return vec, nil
}

func CastVarcharAsTime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		data, err2 := types.ParseTime(vs[0], rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), data, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Time](vec)
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		data, err2 := types.ParseTime(str, rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = data
	}
	return vec, nil
}

// CastVarcharAsDatetime : Cast converts varchar to datetime type
func CastVarcharAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)

	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		data, err2 := types.ParseDatetime(vs[0], rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), data, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Datetime](vec)
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		data, err2 := types.ParseDatetime(str, rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = data
	}
	return vec, nil
}

// CastVarcharAsTimestamp : Cast converts varchar to timestamp type
func CastVarcharAsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	vs := vector.MustStrCols(lv)

	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		data, err := types.ParseTimestamp(t, vs[0], rv.Typ.Precision)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), data, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	for i, str := range vs {
		data, err := types.ParseTimestamp(t, str, rv.Typ.Precision)
		if err != nil {
			return nil, err
		}
		rs[i] = data
	}
	return vec, nil
}

// CastDecimal64AsDecimal128 : Cast converts decimal64 to decimal128
func CastDecimal64AsDecimal128(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvScale := lv.Typ.Scale
	resultTyp := types.T_decimal128.ToType()
	resultTyp.Scale = lvScale
	lvs := vector.MustTCols[types.Decimal64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		if _, err := binary.Decimal64ToDecimal128(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(resultTyp, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal128](vec)
	if _, err := binary.Decimal64ToDecimal128(lvs, rs, rv.Typ.Width, rv.Typ.Scale); err != nil {
		return nil, err
	}
	return vec, nil
}

// castTimeStampAsDatetime : Cast converts timestamp to datetime decimal128
func castTimeStampAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	lvs := vector.MustTCols[types.Timestamp](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Datetime, 1)
		if _, err := binary.TimestampToDatetime(t, lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Datetime](vec)
	if _, err := binary.TimestampToDatetime(t, lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// castTimestampAsVarchar : Cast converts timestamp to varchar
func castTimestampAsVarchar(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	lvs := vector.MustTCols[types.Timestamp](lv)
	resultType := rv.Typ
	precision := lv.Typ.Precision
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(resultType, lv.Length()), nil
		}
		rs := make([]string, 1)
		if _, err := binary.TimestampToVarchar(t, lvs, rs, precision); err != nil {
			return nil, err
		}
		return vector.NewConstString(resultType, lv.Length(), rs[0], proc.Mp()), nil
	}

	rs := make([]string, len(lvs))
	if _, err := binary.TimestampToVarchar(t, lvs, rs, precision); err != nil {
		return nil, err
	}
	return vector.NewWithStrings(resultType, rs, lv.Nsp, proc.Mp()), nil
}

// CastStringAsDecimal64 : onverts char/varchar/text as decimal64
func CastStringAsDecimal64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		decimal64, err := types.ParseStringToDecimal64(vs[0], rv.Typ.Width, rv.Typ.Scale, lv.GetIsBin())
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), decimal64, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal64](vec)
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		decimal64, err2 := types.ParseStringToDecimal64(str, rv.Typ.Width, rv.Typ.Scale, lv.GetIsBin())
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal64
	}
	return vec, nil
}

func CastBoolToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	}

	lvs := vector.MustTCols[bool](lv)
	col := make([]string, len(lvs))
	if lv.IsScalar() {
		binary.BoolToBytes(lvs, col)
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}

	if _, err = binary.BoolToBytes(lvs, col); err != nil {
		return nil, err
	}

	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

func CastDateAsString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error

	if lv.IsScalarNull() {
		return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
	}

	lvs := vector.MustTCols[types.Date](lv)
	col := make([]string, len(lvs))
	if lv.IsScalar() {
		binary.DateToBytes(lvs, col)
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}

	// XXX All these binary functions should take null.Nulls as input
	if col, err = binary.DateToBytes(lvs, col); err != nil {
		return nil, err
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

func CastDateAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Date](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Datetime, 1)
		if _, err := binary.DateToDatetime(lvs, rs); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Datetime](vec)
	if _, err := binary.DateToDatetime(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// XXX I felt I have written this ten times, what is going on?
// CastStringAsDecimal128 : onverts char/varchar as decimal128
func CastStringAsDecimal128(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		decimal128, err := types.ParseStringToDecimal128(vs[0], rv.Typ.Width, rv.Typ.Scale, lv.GetIsBin())
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), decimal128, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal128](vec)
	for i := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		decimal128, err2 := types.ParseStringToDecimal128(vs[i], rv.Typ.Width, rv.Typ.Scale, lv.GetIsBin())
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal128
	}
	return vec, nil
}

// CastDatetimeAsTimeStamp : Cast converts datetime to timestamp
func CastDatetimeAsTimeStamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	lvs := vector.MustTCols[types.Datetime](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Timestamp, 1)
		timestamp.DatetimeToTimestamp(t, lvs, lv.Nsp, rs)
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	timestamp.DatetimeToTimestamp(t, lvs, lv.Nsp, rs)
	return vec, nil
}

// CastDateAsTimeStamp : Cast converts date to timestamp
func CastDateAsTimeStamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var t *time.Location
	if proc == nil {
		t = time.Local
	} else {
		t = proc.SessionInfo.TimeZone
	}
	lvs := vector.MustTCols[types.Date](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Timestamp, 1)
		timestamp.DateToTimestamp(t, lvs, lv.Nsp, rs)
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	timestamp.DateToTimestamp(t, lvs, lv.Nsp, rs)
	return vec, nil
}

func CastDatetimeAsString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error

	lvs := vector.MustTCols[types.Datetime](lv)
	col := make([]string, len(lvs))
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		if col, err = binary.DatetimeToBytes(lvs, col, lv.Typ.Precision); err != nil {
			return nil, err
		}
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}

	if col, err = binary.DatetimeToBytes(lvs, col, lv.Typ.Precision); err != nil {
		return nil, err
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

// CastDatetimeAsDate : convert datetime to date
// DateTime : high 44 bits stands for the seconds passed by, low 20 bits stands for the microseconds passed by
func CastDatetimeAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Datetime](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Date, 1)
		if _, err := binary.DatetimeToDate(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Date](vec)
	if _, err := binary.DatetimeToDate(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastIntAsTimestamp[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if lvs[0] < 0 || int64(lvs[0]) > 32536771199 {
			nulls.Add(lv.Nsp, 0)
		}
		if _, err := binary.NumericToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	// XXX This is simply WRONG.
	for i := 0; i < len(lvs); i++ {
		if lvs[i] < 0 || int64(lvs[i]) > 32536771199 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	if _, err := binary.NumericToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastUIntAsTimestamp[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if lvs[0] < 0 || uint64(lvs[0]) > 32536771199 {
			nulls.Add(lv.Nsp, 0)
		}
		if _, err := binary.NumericToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	// XXX Again, simply WRONG.
	for i := 0; i < len(lvs); i++ {
		if lvs[i] < 0 || uint64(lvs[i]) > 32536771199 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	if _, err := binary.NumericToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal64AsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Decimal64](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := binary.Decimal64ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	if _, err := binary.Decimal64ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128AsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Decimal128](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := binary.Decimal128ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Timestamp](vec)
	if _, err := binary.Decimal128ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastTimeAsString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error

	lvs := vector.MustTCols[types.Time](lv)
	col := make([]string, len(lvs))
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		if col, err = binary.TimeToBytes(lvs, col, lv.Typ.Precision); err != nil {
			return nil, err
		}
		return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
	}

	if col, err = binary.TimeToBytes(lvs, col, lv.Typ.Precision); err != nil {
		return nil, err
	}
	return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

func CastTimeAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Time](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Date, 1)
		if _, err := binary.TimeToDate(lvs, rs); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Date](vec)
	if _, err := binary.TimeToDate(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDateAsTime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Date](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Time, 1)
		if _, err := binary.DateToTime(lvs, rs); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Time](vec)
	if _, err := binary.DateToTime(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastTimeAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Time](lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Datetime, 1)
		if _, err := binary.TimeToDatetime(lvs, rs, rv.Typ.Precision); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Datetime](vec)
	if _, err := binary.TimeToDatetime(lvs, rs, rv.Typ.Precision); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDatetimeAsTime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Datetime](lv)
	precision := rv.Typ.Precision
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		rs := make([]types.Time, 1)
		if _, err := binary.DatetimeToTime(lvs, rs, precision); err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), rs[0], proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Time](vec)
	if _, err := binary.DatetimeToTime(lvs, rs, precision); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal64ToFloat32(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float32, 1)
		if _, err := binary.Decimal64ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[float32](vec)
	if _, err := binary.Decimal64ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128ToFloat32(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float32, 1)
		if _, err := binary.Decimal128ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[float32](vec)
	if _, err := binary.Decimal128ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal64ToFloat64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float64, 1)
		if _, err := binary.Decimal64ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[float64](vec)
	if _, err := binary.Decimal64ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128ToFloat64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float64, 1)
		if _, err := binary.Decimal128ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[float64](vec)
	if _, err := binary.Decimal128ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal64ToUint64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]uint64, 1)
		if _, err := binary.Decimal64ToUint64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[uint64](vec)
	if _, err := binary.Decimal64ToUint64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128ToUint64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]uint64, 1)
		if _, err := binary.Decimal128ToUint64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[uint64](vec)
	if _, err := binary.Decimal128ToUint64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

// this cast function is too slow, and therefore only temporary, rewrite needed
func CastDecimal128ToDecimal64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if lv.Typ.Scale > 18 {
		return nil, formatCastError(lv, rv.Typ, "")
	}
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Decimal64, 1)
		if _, err := binary.Decimal128ToDecimal64(lvs, rv.Typ.Width, rv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal64](vec)
	if _, err := binary.Decimal128ToDecimal64(lvs, rv.Typ.Width, rv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal64ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := binary.Decimal64ToInt64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	if _, err := binary.Decimal64ToInt64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := binary.Decimal128ToInt64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	if _, err := binary.Decimal128ToInt64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastDecimal128ToInt32(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int32, 1)
		if _, err := binary.Decimal128ToInt32(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int32](vec)
	if _, err := binary.Decimal128ToInt32(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastNumValToBool[T constraints.Integer | constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]bool, 1)
		if _, err := binary.NumericToBool(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[bool](vec)
	if _, err := binary.NumericToBool(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastBoolToNumeric[T constraints.Integer](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[bool](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T, 1)
		if _, err := binary.BoolToNumeric(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(lvs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[T](vec)
	if _, err := binary.BoolToNumeric(lvs, rs); err != nil {
		return nil, err
	}
	return vec, nil
}

func CastStringToJson(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}

		json, err := types.ParseStringToByteJson(vs[0])
		if err != nil {
			return nil, err
		}
		val, err := types.EncodeJson(json)
		if err != nil {
			return nil, err
		}
		return vector.NewConstBytes(rv.Typ, lv.Length(), val, proc.Mp()), nil
	}

	col := make([][]byte, len(vs))
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}

		json, err := types.ParseStringToByteJson(str)
		if err != nil {
			return nil, err
		}
		val, err := types.EncodeJson(json)
		if err != nil {
			return nil, err
		}
		col[i] = val
	}
	return vector.NewWithBytes(rv.Typ, col, lv.Nsp, proc.Mp()), nil
}

func CastStringToBool(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}

		val, err := types.ParseBool(vs[0])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), val, proc.Mp()), nil
	}

	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[bool](vec)
	for i, str := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		val, err := types.ParseBool(str)
		if err != nil {
			return nil, err
		}
		rs[i] = val
	}
	return vec, nil
}

// ---------------------------------------------uuid cast---------------------------------------------------------------
func CastStringToUuid(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustStrCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		}
		val, err := types.ParseUuid(vs[0])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rv.Typ, lv.Length(), val, proc.Mp()), nil
	}
	vec, err := proc.AllocVectorOfRows(rv.Typ, int64(len(vs)), lv.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Uuid](vec)
	for i := range vs {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		val, err2 := types.ParseUuid(vs[i])
		if err2 != nil {
			return nil, err2
		}
		rs[i] = val
	}
	return vec, nil
}

func CastUuidToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocConstNullVector(rv.Typ, lv.Length()), nil
		} else {
			lvs := vector.MustTCols[types.Uuid](lv)
			col := make([]string, 1)
			if col, err = binary.UuidToBytes(lvs, col); err != nil {
				return nil, err
			}
			return vector.NewConstString(rv.Typ, lv.Length(), col[0], proc.Mp()), nil
		}
	} else {
		lvs := vector.MustTCols[types.Uuid](lv)
		col := make([]string, len(lvs))
		if col, err = binary.UuidToBytes(lvs, col); err != nil {
			return nil, err
		}
		return vector.NewWithStrings(rv.Typ, col, lv.Nsp, proc.Mp()), nil
	}
}

// ----------------------------------------------------------------------------------------------------------------------
// IsInteger return true if the types.T is integer type
func IsInteger(t types.T) bool {
	if t == types.T_int8 || t == types.T_int16 || t == types.T_int32 || t == types.T_int64 ||
		t == types.T_uint8 || t == types.T_uint16 || t == types.T_uint32 || t == types.T_uint64 {
		return true
	}
	return false
}

// isSignedInteger: return true if the types.T is Signed integer type
func isSignedInteger(t types.T) bool {
	if t == types.T_int8 || t == types.T_int16 || t == types.T_int32 || t == types.T_int64 {
		return true
	}
	return false
}

// isUnsignedInteger: return true if the types.T is UnSigned integer type
func isUnsignedInteger(t types.T) bool {
	if t == types.T_uint8 || t == types.T_uint16 || t == types.T_uint32 || t == types.T_uint64 {
		return true
	}
	return false
}

// IsFloat: return true if the types.T is floating Point Types
func IsFloat(t types.T) bool {
	if t == types.T_float32 || t == types.T_float64 {
		return true
	}
	return false
}

// IsNumeric: return true if the types.T is numbric type
func IsNumeric(t types.T) bool {
	if IsInteger(t) || IsFloat(t) {
		return true
	}
	return false
}

// isString: return true if the types.T is string type
func isString(t types.T) bool {
	if t == types.T_char || t == types.T_varchar || t == types.T_blob || t == types.T_text {
		return true
	}
	return false
}

// IsDecimal: return true if the types.T is decimal64 or decimal128
func IsDecimal(t types.T) bool {
	if t == types.T_decimal64 || t == types.T_decimal128 {
		return true
	}
	return false
}
