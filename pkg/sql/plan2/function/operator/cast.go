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
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/typecast"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Cast(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec, err := doCast(vs, proc)
	if err != nil {
		extraError := ""
		if sqlErr, ok := err.(*errors.SqlError); ok {
			if sqlErr.Code() == errno.SyntaxErrororAccessRuleViolation {
				return nil, err
			}
		}
		if moErr, ok := err.(*moerr.Error); ok {
			if moErr.Code == moerr.OUT_OF_RANGE {
				extraError = " Reason: overflow"
			}
		}
		return nil, formatCastError(vs[0], vs[1].Typ, extraError)
	}
	return vec, err
}

// shorten the string to the one with no more than 100 characters.
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
			valueStr := strings.TrimRight(strings.TrimLeft(fmt.Sprintf("%v", vec.Col), "["), "]")
			shortenValueStr := shortenValueString(valueStr)
			errStr = fmt.Sprintf("Can't cast '%s' from %v type to %v type.", shortenValueStr, vec.Typ, typ)
		}
	} else {
		errStr = fmt.Sprintf("Can't cast column from %v type to %v type because of one or more values in that column.", vec.Typ, typ)
	}
	return errors.New(errno.InternalError, errStr+extraInfo)
}

func doCast(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vs[0]
	rv := vs[1]
	if rv.IsScalarNull() {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "the target type of cast function cannot be null")
	}
	if lv.IsScalarNull() {
		return proc.AllocScalarNullVector(rv.Typ), nil
	}

	if lv.Typ.Oid == rv.Typ.Oid && isNumeric(lv.Typ.Oid) {
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
		}
	}

	if lv.Typ.Oid == rv.Typ.Oid && isDateSeries(lv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_date:
			return CastSameType2[types.Date](lv, rv, proc)
		case types.T_datetime:
			return CastSameType2[types.Datetime](lv, rv, proc)
		case types.T_timestamp:
			return CastSameType2[types.Timestamp](lv, rv, proc)
		}
	}

	if lv.Typ.Oid != rv.Typ.Oid && isNumeric(lv.Typ.Oid) && isNumeric(rv.Typ.Oid) {
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

	if isString(lv.Typ.Oid) && isInteger(rv.Typ.Oid) {
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

	if isString(lv.Typ.Oid) && isFloat(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_float32:
			return CastSpecials1Float[float32](lv, rv, proc)
		case types.T_float64:
			return CastSpecials1Float[float64](lv, rv, proc)
		}
	}

	if isString(lv.Typ.Oid) && isDecimal(rv.Typ.Oid) {
		switch rv.Typ.Oid {
		case types.T_decimal64:
			return CastStringAsDecimal64(lv, rv, proc)
		case types.T_decimal128:
			return CastStringAsDecimal128(lv, rv, proc)
		}
	}

	if isInteger(lv.Typ.Oid) && isString(rv.Typ.Oid) {
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
			return CastSpecials2Int[uint8](lv, rv, proc)
		case types.T_uint16:
			return CastSpecials2Int[uint16](lv, rv, proc)
		case types.T_uint32:
			return CastSpecials2Int[uint32](lv, rv, proc)
		case types.T_uint64:
			return CastSpecials2Int[uint64](lv, rv, proc)
		}
	}

	if isFloat(lv.Typ.Oid) && isString(rv.Typ.Oid) {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastSpecials2Float[float32](lv, rv, proc)
		case types.T_float64:
			return CastSpecials2Float[float64](lv, rv, proc)
		}
	}
	if isDecimal(lv.Typ.Oid) && isString(rv.Typ.Oid) {
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

	if isFloat(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal128 {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastFloatAsDecimal128[float32](lv, rv, proc)
		case types.T_float64:
			return CastFloatAsDecimal128[float64](lv, rv, proc)
		}
	}

	if isFloat(lv.Typ.Oid) && rv.Typ.Oid == types.T_decimal64 {
		switch lv.Typ.Oid {
		case types.T_float32:
			return CastFloatAsDecimal64[float32](lv, rv, proc)
		case types.T_float64:
			return CastFloatAsDecimal64[float64](lv, rv, proc)
		}
	}
	// sametype
	if lv.Typ.Oid == types.T_decimal64 && rv.Typ.Oid == types.T_decimal64 {
		return CastDecimal64AsDecimal64(lv, rv, proc)
	}

	// sametype
	if lv.Typ.Oid == types.T_decimal128 && rv.Typ.Oid == types.T_decimal128 {
		return CastDecimal128AsDecimal128(lv, rv, proc)
	}

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_date {
		return CastVarcharAsDate(lv, rv, proc)
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

	if lv.Typ.Oid == types.T_timestamp && rv.Typ.Oid == types.T_varchar {
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

	if lv.Typ.Oid == types.T_date && isString(rv.Typ.Oid) {
		return CastDateAsString(lv, rv, proc)
	}

	if lv.Typ.Oid == types.T_datetime && isString(rv.Typ.Oid) {
		return CastDatetimeAsString(lv, rv, proc)
	}

	if isInteger(lv.Typ.Oid) && rv.Typ.Oid == types.T_timestamp {
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

	if isDecimal(lv.Typ.Oid) && rv.Typ.Oid == types.T_timestamp {
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
	// if lv.Typ.Oid == types.T_timestamp && rv.Typ.Oid == types.T_time {
	// 	return CastTimestampAsTime(lv, rv, proc)
	// }

	if isNumeric(lv.Typ.Oid) && rv.Typ.Oid == types.T_bool {
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

	if isString(lv.Typ.Oid) && rv.Typ.Oid == types.T_bool {
		return CastStringToBool(lv, rv, proc)
	}

	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("parameter types [%s, %s] of cast function do not match", lv.Typ.Oid, rv.Typ.Oid))
}

func CastTimestampAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Timestamp](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Datetime, 1)
		if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
			return nil, err
		}
		rs2 := make([]types.Date, 1)
		rs2[0] = rs[0].ToDate()
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs2)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDatetimeSlice(vec.Data)
	rs = rs[:len(lvs)]
	rs2 := make([]types.Date, len(lvs), cap(lvs))
	if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
		return nil, err
	}
	for i := 0; i < len(rs2); i++ {
		rs2[i] = rs[i].ToDate()
	}
	vec2, err := proc.AllocVector(rv.Typ, 4*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	nulls.Set(vec2.Nsp, lv.Nsp)
	vector.SetCol(vec2, rs2)
	return vec2, nil
}

// func CastTimestampAsTime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
// 	rtl := 4
// 	lvs := lv.Col.([]types.Timestamp)
// 	if lv.IsScalar() {
// 		vec := proc.AllocScalarVector(rv.Typ)
// 		rs := make([]types.Datetime, 1)
// 		if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
// 			return nil, err
// 		}
// 		rs2 := make([]types.Date, 1)
// 		rs2[0] = rs[0].ToDate()
// 		nulls.Set(vec.Nsp, lv.Nsp)
// 		vector.SetCol(vec, rs2)
// 		return vec, nil
// 	}
// 	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
// 	if err != nil {
// 		return nil, err
// 	}
// 	rs := encoding.DecodeDatetimeSlice(vec.Data)
// 	rs = rs[:len(lvs)]
// 	rs2 := make([]types.Date, len(lvs))
// 	if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
// 		return nil, err
// 	}
// 	for i := 0; i < len(rs2); i++ {
// 		rs2[i] = rs[i].ToDate()
// 	}
// 	nulls.Set(vec.Nsp, lv.Nsp)
// 	vector.SetCol(vec, rs2)
// 	return vec, nil
// }

func CastDecimal64ToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error

	lvs := vector.MustTCols[types.Decimal64](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.Decimal64ToBytes(lvs, col, lv.Typ.Scale); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func CastDecimal128ToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[types.Decimal128](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}

	if col, err = typecast.Decimal128ToBytes(lvs, col, lv.Typ.Scale); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
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
func CastSameType[T constraints.Integer | constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := lv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[T](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T, 1)
		copy(rs, lvs)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
	copy(rs, lvs)
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastSameType2 : Cast handles the same data type and is date series , Contains the following:
// date -> date
// datetime -> datetime
// timestamp -> timestamp
func CastSameType2[T types.Date | types.Datetime | types.Timestamp](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[T](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T, 1)
		copy(rs, lvs)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
	copy(rs, lvs)
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastLeftToRight : Cast handles conversions in the form of cast (left as right), where left and right are different types,
//  and both left and right are numeric types, Contains the following:
// int8 -> (int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
// int16 -> (int8/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
// int32 -> (int8/int16/int64/uint8/uint16/uint32/uint64/float32/float64)
// int64 -> (int8/int16/int32/uint8/uint16/uint32/uint64/float32/float64)
// uint8 -> (int8/int16/int32/int64/uint16/uint32/uint64/float32/float64)
// uint16 -> (int8/int16/int32/int64/uint8/uint32/uint64/float32/float64)
// uint32 -> (int8/int16/int32/int64/uint8/uint16/uint64/float32/float64)
// uint64 -> (int8/int16/int32/int64/uint8/uint16/uint32/float32/float64)
func CastLeftToRight[T1, T2 constraints.Integer | constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[T1](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T2, 1)
		if _, err := typecast.NumericToNumeric(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T2](vec.Data, rtl)
	if _, err := typecast.NumericToNumeric(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastFloatToInt[T1 constraints.Float, T2 constraints.Integer](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[T1](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]T2, 1)
		if _, err := typecast.FloatToIntWithoutError(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T2](vec.Data, rtl)
	if _, err := typecast.FloatToIntWithoutError(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastFloat64ToInt64 : cast float64 to int64
func CastFloat64ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[float64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := typecast.Float64ToInt64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeInt64Slice(vec.Data)
	if _, err := typecast.Float64ToInt64(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastUint64ToInt64 : cast uint64 to int64
func CastUint64ToInt64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[uint64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]int64, 1)
		if _, err := typecast.Uint64ToInt64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeInt64Slice(vec.Data)
	if _, err := typecast.Uint64ToInt64(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastInt64ToUint64 : cast int64 to uint64
func CastInt64ToUint64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	lvs := vector.MustTCols[int64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]uint64, 1)
		if _, err := typecast.Int64ToUint64(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeUint64Slice(vec.Data)
	if _, err := typecast.Int64ToUint64(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastSpecials1Int : Cast converts string to integer,Contains the following:
// (char / varhcar) -> (int8 / int16 / int32/ int64 / uint8 / uint16 / uint32 / uint64)
func CastSpecials1Int[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	col := vector.MustBytesCols(lv)
	var vec *vector.Vector
	var err error
	var rs []T
	if lv.IsScalar() {
		vec = proc.AllocScalarVector(rv.Typ)
		rs = make([]T, 1)
	} else {
		vec, err = proc.AllocVector(rv.Typ, int64(rtl)*int64(len(col.Offsets)))
		if err != nil {
			return nil, err
		}
		rs = encoding.DecodeFixedSlice[T](vec.Data, rtl)
	}
	if _, err = typecast.BytesToInt(col, rs); err != nil {
		return nil, err
	}

	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastSpecials1Uint[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	col := vector.MustBytesCols(lv)
	var vec *vector.Vector
	var err error
	var rs []T
	if lv.IsScalar() {
		vec = proc.AllocScalarVector(rv.Typ)
		rs = make([]T, 1)
	} else {
		vec, err = proc.AllocVector(rv.Typ, int64(rtl)*int64(len(col.Offsets)))
		if err != nil {
			return nil, err
		}
		rs = encoding.DecodeFixedSlice[T](vec.Data, rtl)
	}
	if _, err = typecast.BytesToUint(col, rs); err != nil {
		return nil, err
	}

	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastSpecials1Float : Cast converts string to floating point number,Contains the following:
// (char / varhcar) -> (float32 / float64)
func CastSpecials1Float[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := rv.Typ.Oid.TypeLen()
	col := vector.MustBytesCols(lv)
	var vec *vector.Vector
	var err error
	var rs []T
	if lv.IsScalar() {
		vec = proc.AllocScalarVector(rv.Typ)
		rs = make([]T, 1)
	} else {
		vec, err = proc.AllocVector(rv.Typ, int64(rtl)*int64(len(col.Offsets)))
		if err != nil {
			return nil, err
		}
		rs = encoding.DecodeFixedSlice[T](vec.Data, rtl)
	}
	if _, err = typecast.BytesToFloat(col, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastSpecials2Int : Cast converts integer to string,Contains the following:
// (int8 /int16/int32/int64/uint8/uint16/uint32/uint64) -> (char / varhcar)
func CastSpecials2Int[T constraints.Integer](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[T](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.IntToBytes(lvs, col); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

// CastSpecials2Float : Cast converts floating point number to string ,Contains the following:
// (float32/float64) -> (char / varhcar)
func CastSpecials2Float[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[T](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.FloatToBytes(lvs, col); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
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

//
// CastSpecials3 :  Cast converts string to string ,Contains the following:
// char -> char
// char -> varhcar
// varchar -> char
// varchar -> varhcar
func CastSpecials3(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	source := vector.MustBytesCols(lv)
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocScalarNullVector(rv.Typ), nil
		}
		vec := proc.AllocScalarVector(rv.Typ)
		vec.Col = &types.Bytes{
			Data:    make([]byte, len(source.Data)),
			Offsets: []uint32{source.Offsets[0]},
			Lengths: []uint32{source.Lengths[0]},
		}
		target := vector.MustBytesCols(vec)
		copy(target.Data, source.Data)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(source.Data)))
	if err != nil {
		return nil, err
	}
	b := vec.Col.(*types.Bytes)
	b.Data = vec.Data
	b.Offsets = make([]uint32, len(source.Offsets))
	b.Lengths = make([]uint32, len(source.Lengths))
	copy(b.Data, source.Data)
	copy(b.Offsets, source.Offsets)
	copy(b.Lengths, source.Lengths)
	nulls.Set(vec.Nsp, lv.Nsp)
	return vec, nil
}

func CastSpecialIntToDecimal[T constraints.Integer](
	lv, _ *vector.Vector,
	i2d func(xs []T, rs []types.Decimal128) ([]types.Decimal128, error),
	proc *process.Process) (*vector.Vector, error) {
	resultScale := int32(0)
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		if _, err := i2d(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := i2d(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastSpecialIntToDecimal64[T constraints.Integer](
	lv, rv *vector.Vector,
	i2d func(xs []T, rs []types.Decimal64, scale int64) ([]types.Decimal64, error),
	proc *process.Process) (*vector.Vector, error) {
	resultScale := rv.Typ.Scale
	resultTyp := types.Type{Oid: types.T_decimal64, Size: 8, Width: 38, Scale: resultScale}
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal64, 1)
		if _, err := i2d(lvs, rs, int64(resultScale)); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := i2d(lvs, rs, int64(resultScale)); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastSpecials4 : Cast converts signed integer to decimal128 ,Contains the following:
// (int8/int16/int32/int64) to decimal128
func CastSpecials4[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal(lv, rv, typecast.IntToDecimal128[T], proc)
}

func CastSpecials4_64[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal64(lv, rv, typecast.IntToDecimal64[T], proc)
}

// CastSpecialu4 : Cast converts unsigned integer to decimal128 ,Contains the following:
// (uint8/uint16/uint32/uint64) to decimal128
func CastSpecialu4[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CastSpecialIntToDecimal(lv, rv, typecast.UintToDecimal128[T], proc)
}

func CastFloatAsDecimal128[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	resultType.Size = 16
	vs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		srcStr := fmt.Sprintf("%f", vs[0])
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal128, 1)
		decimal128, err := types.ParseStringToDecimal128(srcStr, resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
		rs[0] = decimal128
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(resultType, int64(resultType.Size)*int64(len(vs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(vs)]
	for i := 0; i < len(vs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		strValue := fmt.Sprintf("%f", vs[i])
		decimal128, err2 := types.ParseStringToDecimal128(strValue, resultType.Width, resultType.Scale)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal128
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastFloatAsDecimal64[T constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	resultType.Size = 8
	vs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		srcStr := fmt.Sprintf("%f", vs[0])
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal64, 1)
		decimal64, err := types.ParseStringToDecimal64(srcStr, resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
		rs[0] = decimal64
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultType, int64(resultType.Size)*int64(len(vs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(vs)]
	for i := 0; i < len(vs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		strValue := fmt.Sprintf("%f", vs[i])
		decimal64, err2 := types.ParseStringToDecimal64(strValue, resultType.Width, resultType.Scale)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal64
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastVarcharAsDate : Cast converts varchar to date type
func CastVarcharAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustBytesCols(lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Date, 1)
		varcharValue := vs.Get(0)
		data, err2 := types.ParseDateCast(string(varcharValue))
		if err2 != nil {
			return nil, err2
		}
		rs[0] = data
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rv.Typ.Oid.TypeLen()*len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDateSlice(vec.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		varcharValue := vs.Get(int64(i))
		data, err2 := types.ParseDateCast(string(varcharValue))
		if err2 != nil {
			return nil, err2
		}
		rs[i] = data
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastVarcharAsDatetime : Cast converts varchar to datetime type
func CastVarcharAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustBytesCols(lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Datetime, 1)
		varcharValue := vs.Get(0)
		data, err2 := types.ParseDatetime(string(varcharValue), rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		rs[0] = data
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rv.Typ.Oid.TypeLen()*len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDatetimeSlice(vec.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		varcharValue := vs.Get(int64(i))
		data, err2 := types.ParseDatetime(string(varcharValue), rv.Typ.Precision)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = data
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastVarcharAsTimestamp : Cast converts varchar to timestamp type
func CastVarcharAsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vs := vector.MustBytesCols(lv)

	if lv.IsScalar() {
		scalarVector := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		strBytes := vs.Get(0)
		data, err := types.ParseTimestamp(string(strBytes), 6)
		if err != nil {
			return nil, err
		}
		rs[0] = data
		nulls.Set(scalarVector.Nsp, lv.Nsp)
		vector.SetCol(scalarVector, rs)
		return scalarVector, nil
	}

	allocVector, err := proc.AllocVector(rv.Typ, int64(rv.Typ.Oid.TypeLen()*len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(allocVector.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		strBytes := vs.Get(int64(i))
		data, err := types.ParseTimestamp(string(strBytes), 6)
		if err != nil {
			return nil, err
		}
		rs[i] = data
	}
	nulls.Set(allocVector.Nsp, lv.Nsp)
	vector.SetCol(allocVector, rs)
	return allocVector, nil
}

// CastDecimal64AsDecimal128 : Cast converts decimal64 to timestamp decimal128
func CastDecimal64AsDecimal128(lv, _ *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvScale := lv.Typ.Scale
	resultScale := lvScale
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	lvs := vector.MustTCols[types.Decimal64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		if _, err := typecast.Decimal64ToDecimal128(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal64ToDecimal128(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastDecimal64AsDecimal64 : Cast converts decimal64 to timestamp decimal64
func CastDecimal64AsDecimal64(lv, _ *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultTyp := lv.Typ
	lvs := vector.MustTCols[types.Decimal64](lv)

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal64, 1)
		copy(rs, lvs)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(lvs)]
	copy(rs, lvs)
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastDecimal128AsDecimal128 : Cast converts decimal128 to timestamp decimal128
func CastDecimal128AsDecimal128(lv, _ *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultTyp := lv.Typ
	lvs := vector.MustTCols[types.Decimal128](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		copy(rs, lvs)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(lvs)]
	copy(rs, lvs)
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

//  castTimeStampAsDatetime : Cast converts timestamp to datetime decimal128
func castTimeStampAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Timestamp](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Datetime, 1)
		if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDatetimeSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.TimestampToDatetime(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

//  castTimestampAsVarchar : Cast converts timestamp to varchar
func castTimestampAsVarchar(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[types.Timestamp](lv)
	resultType := rv.Typ
	resultElementSize := int(resultType.Size)
	precision := lv.Typ.Precision
	if lv.IsScalar() {
		if lv.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vec := proc.AllocScalarVector(resultType)
		rs := &types.Bytes{
			Data:    []byte{},
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		if _, err := typecast.TimestampToVarchar(lvs, rs, precision); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultType, int64(resultElementSize*len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := &types.Bytes{
		Data:    []byte{},
		Offsets: make([]uint32, len(lvs)),
		Lengths: make([]uint32, len(lvs)),
	}
	if _, err := typecast.TimestampToVarchar(lvs, rs, precision); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastStringAsDecimal64 : onverts char/varchar as decimal64
func CastStringAsDecimal64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	resultType.Size = 8
	vs := vector.MustBytesCols(lv)
	if lv.IsScalar() {
		srcStr := vs.Get(0)
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal64, 1)
		decimal64, err := types.ParseStringToDecimal64(string(srcStr), resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
		rs[0] = decimal64
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultType, int64(resultType.Size)*int64(len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		strValue := vs.Get(int64(i))
		decimal64, err2 := types.ParseStringToDecimal64(string(strValue), resultType.Width, resultType.Scale)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal64
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastBoolToString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[bool](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.BoolToBytes(lvs, col); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func CastDateAsString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[types.Date](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.DateToBytes(lvs, col); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func CastDateAsDatetime(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Date](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Datetime, 1)
		if _, err := typecast.DateToDatetime(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDatetimeSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.DateToDatetime(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastStringAsDecimal128 : onverts char/varchar as decimal128
func CastStringAsDecimal128(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	resultType.Size = 16
	vs := vector.MustBytesCols(lv)
	if lv.IsScalar() {
		srcStr := vs.Get(0)
		vec := proc.AllocScalarVector(resultType)
		rs := make([]types.Decimal128, 1)
		decimal128, err := types.ParseStringToDecimal128(string(srcStr), resultType.Width, resultType.Scale)
		if err != nil {
			return nil, err
		}
		rs[0] = decimal128
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(resultType, int64(resultType.Size)*int64(len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		strValue := vs.Get(int64(i))
		decimal128, err2 := types.ParseStringToDecimal128(string(strValue), resultType.Width, resultType.Scale)
		if err2 != nil {
			return nil, err2
		}
		rs[i] = decimal128
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastDatetimeAsTimeStamp : Cast converts datetime to timestamp
func CastDatetimeAsTimeStamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Datetime](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := typecast.DatetimeToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.DatetimeToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

// CastDateAsTimeStamp : Cast converts date to timestamp
func CastDateAsTimeStamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Date](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := typecast.DateToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.DateToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDatetimeAsString(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	lvs := vector.MustTCols[types.Datetime](lv)
	col := &types.Bytes{
		Data:    make([]byte, 0, len(lvs)),
		Offsets: make([]uint32, 0, len(lvs)),
		Lengths: make([]uint32, 0, len(lvs)),
	}
	if col, err = typecast.DateTimeToBytes(lvs, col); err != nil {
		return nil, err
	}
	if err = proc.Mp.Gm.Alloc(int64(cap(col.Data))); err != nil {
		return nil, err
	}
	vec := vector.New(rv.Typ)
	if lv.IsScalar() {
		vec.IsConst = true
	}
	vec.Data = col.Data
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

// CastDatetimeAsDate : convert datetime to date
// DateTime : high 44 bits stands for the seconds passed by, low 20 bits stands for the microseconds passed by
func CastDatetimeAsDate(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Datetime](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Date, 1)
		if _, err := typecast.DateTimeToDate(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDateSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.DateTimeToDate(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastIntAsTimestamp[T constraints.Signed](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if lvs[0] < 0 || int64(lvs[0]) > 32536771199 {
			if lv.Nsp.Np == nil {
				lv.Nsp.Np = &roaring64.Bitmap{}
			}
			lv.Nsp.Np.AddInt(0)
		}
		if _, err := typecast.NumericToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	for i := 0; i < len(lvs); i++ {
		if lv.Nsp.Np == nil {
			lv.Nsp.Np = &roaring64.Bitmap{}
		}
		if lvs[i] < 0 || int64(lvs[i]) > 32536771199 {
			lv.Nsp.Np.AddInt(i)
		}
	}
	if _, err := typecast.NumericToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastUIntAsTimestamp[T constraints.Unsigned](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if lvs[0] < 0 || uint64(lvs[0]) > 32536771199 {
			if lv.Nsp.Np == nil {
				lv.Nsp.Np = &roaring64.Bitmap{}
			}
			lv.Nsp.Np.AddInt(0)
		}
		if _, err := typecast.NumericToTimestamp(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	for i := 0; i < len(lvs); i++ {
		if lv.Nsp.Np == nil {
			lv.Nsp.Np = &roaring64.Bitmap{}
		}
		if lvs[i] < 0 || uint64(lvs[i]) > 32536771199 {
			lv.Nsp.Np.AddInt(i)
		}
	}
	if _, err := typecast.NumericToTimestamp(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal64AsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Decimal64](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := typecast.Decimal64ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal64ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal128AsTimestamp(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[types.Decimal128](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]types.Timestamp, 1)
		if _, err := typecast.Decimal128ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeTimestampSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal128ToTimestamp(lvs, lv.Typ.Precision, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal64ToFloat32(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 4
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float32, 1)
		if _, err := typecast.Decimal64ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat32Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal64ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal128ToFloat32(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 4
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float32, 1)
		if _, err := typecast.Decimal128ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat32Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal128ToFloat32(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal64ToFloat64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := lv.Col.([]types.Decimal64)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float64, 1)
		if _, err := typecast.Decimal64ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal64ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastDecimal128ToFloat64(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := lv.Col.([]types.Decimal128)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]float64, 1)
		if _, err := typecast.Decimal128ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.Decimal128ToFloat64(lvs, lv.Typ.Scale, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastNumValToBool[T constraints.Integer | constraints.Float](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtl := 8
	lvs := vector.MustTCols[T](lv)
	if lv.IsScalar() {
		vec := proc.AllocScalarVector(rv.Typ)
		rs := make([]bool, 1)
		if _, err := typecast.NumericToBool(lvs, rs); err != nil {
			return nil, err
		}
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}
	vec, err := proc.AllocVector(rv.Typ, int64(len(lvs)*rtl))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeBoolSlice(vec.Data)
	rs = rs[:len(lvs)]
	if _, err := typecast.NumericToBool(lvs, rs); err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

func CastStringToBool(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := rv.Typ
	resultType.Size = 8
	vs := vector.MustBytesCols(lv)
	if lv.IsScalar() {
		srcStr := vs.Get(0)
		vec := proc.AllocScalarVector(resultType)
		rs := make([]bool, 1)
		val, err := strconv.ParseFloat(string(srcStr), 64)
		if err != nil {
			return nil, err
		}
		if val != 0 {
			rs[0] = true
		}
		nulls.Reset(vec.Nsp)
		vector.SetCol(vec, rs)
		return vec, nil
	}

	vec, err := proc.AllocVector(resultType, int64(resultType.Size)*int64(len(vs.Lengths)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeBoolSlice(vec.Data)
	rs = rs[:len(vs.Lengths)]
	for i := range vs.Lengths {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			continue
		}
		srcStr := vs.Get(int64(i))
		val, err := strconv.ParseFloat(string(srcStr), 64)
		if err == nil && val != 0 {
			rs[i] = true
		}
	}
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, rs)
	return vec, nil
}

//  isInteger return true if the types.T is integer type
func isInteger(t types.T) bool {
	if t == types.T_int8 || t == types.T_int16 || t == types.T_int32 || t == types.T_int64 ||
		t == types.T_uint8 || t == types.T_uint16 || t == types.T_uint32 || t == types.T_uint64 {
		return true
	}
	return false
}

//  isSignedInteger: return true if the types.T is Signed integer type
func isSignedInteger(t types.T) bool {
	if t == types.T_int8 || t == types.T_int16 || t == types.T_int32 || t == types.T_int64 {
		return true
	}
	return false
}

//  isUnsignedInteger: return true if the types.T is UnSigned integer type
func isUnsignedInteger(t types.T) bool {
	if t == types.T_uint8 || t == types.T_uint16 || t == types.T_uint32 || t == types.T_uint64 {
		return true
	}
	return false
}

//  isFloat: return true if the types.T is floating Point Types
func isFloat(t types.T) bool {
	if t == types.T_float32 || t == types.T_float64 {
		return true
	}
	return false
}

//  isNumeric: return true if the types.T is numbric type
func isNumeric(t types.T) bool {
	if isInteger(t) || isFloat(t) {
		return true
	}
	return false
}

//  isString: return true if the types.T is string type
func isString(t types.T) bool {
	if t == types.T_char || t == types.T_varchar {
		return true
	}
	return false
}

//  isDateSeries: return true if the types.T is date related type
func isDateSeries(t types.T) bool {
	if t == types.T_date || t == types.T_datetime || t == types.T_timestamp {
		return true
	}
	return false
}

// isDecimal: return true if the types.T is decimal64 or decimal128
func isDecimal(t types.T) bool {
	if t == types.T_decimal64 || t == types.T_decimal128 {
		return true
	}
	return false
}
