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
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// XXX need this one to make a pretty function register.
var supportedTypeCast = map[types.T][]types.T{
	types.T_any: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
	},

	types.T_bool: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_int8: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_int16: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_int32: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_int64: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_uint8: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_uint16: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_uint32: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_uint64: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_float32: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_float64: {
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_date: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_datetime: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_timestamp: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_time: {
		types.T_date, types.T_datetime,
		types.T_time,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_decimal64, types.T_decimal128,
	},

	types.T_decimal64: {
		types.T_float32, types.T_float64,
		types.T_int64,
		types.T_uint64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_time, types.T_timestamp,
	},

	types.T_decimal128: {
		types.T_float32, types.T_float64,
		types.T_int32, types.T_int64,
		types.T_uint64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_time, types.T_timestamp,
	},

	types.T_char: {
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_json,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_varchar: {
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_json,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_blob: {
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_json,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_text: {
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_json,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_json: {
		types.T_char, types.T_varchar, types.T_text,
	},

	types.T_uuid: {
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
	},

	types.T_TS: {
		types.T_TS,
	},

	types.T_Rowid: {
		types.T_Rowid,
	},
}

func IfTypeCastSupported(sourceType, targetType types.T) bool {
	supportList, ok := supportedTypeCast[sourceType]
	if ok {
		for _, t := range supportList {
			if t == targetType {
				return true
			}
		}
	}
	return false
}

func NewCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	var err error
	// Cast Parameter1 as Type Parameter2
	fromType := parameters[0].GetType()
	toType := parameters[1].GetType()
	from := parameters[0]
	switch fromType.Oid {
	case types.T_any: // scalar null
		err = scalarNullToOthers(proc.Ctx, toType, result, length)
	case types.T_bool:
		s := vector.GenerateFunctionFixedTypeParameter[bool](from)
		err = boolToOthers(proc.Ctx, s, toType, result, length)
	case types.T_int8:
		s := vector.GenerateFunctionFixedTypeParameter[int8](from)
		err = int8ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_int16:
		s := vector.GenerateFunctionFixedTypeParameter[int16](from)
		err = int16ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_int32:
		s := vector.GenerateFunctionFixedTypeParameter[int32](from)
		err = int32ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_int64:
		s := vector.GenerateFunctionFixedTypeParameter[int64](from)
		err = int64ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_uint8:
		s := vector.GenerateFunctionFixedTypeParameter[uint8](from)
		err = uint8ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_uint16:
		s := vector.GenerateFunctionFixedTypeParameter[uint16](from)
		err = uint16ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_uint32:
		s := vector.GenerateFunctionFixedTypeParameter[uint32](from)
		err = uint32ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_uint64:
		s := vector.GenerateFunctionFixedTypeParameter[uint64](from)
		err = uint64ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_float32:
		s := vector.GenerateFunctionFixedTypeParameter[float32](from)
		err = float32ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_float64:
		s := vector.GenerateFunctionFixedTypeParameter[float64](from)
		err = float64ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_decimal64:
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](from)
		err = decimal64ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_decimal128:
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](from)
		err = decimal128ToOthers(proc.Ctx, s, toType, result, length)
	case types.T_date:
		s := vector.GenerateFunctionFixedTypeParameter[types.Date](from)
		err = dateToOthers(proc, s, toType, result, length)
	case types.T_datetime:
		s := vector.GenerateFunctionFixedTypeParameter[types.Datetime](from)
		err = datetimeToOthers(proc, s, toType, result, length)
	case types.T_time:
		s := vector.GenerateFunctionFixedTypeParameter[types.Time](from)
		err = timeToOthers(proc.Ctx, s, toType, result, length)
	case types.T_timestamp:
		s := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](from)
		err = timestampToOthers(proc, s, toType, result, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		s := vector.GenerateFunctionStrParameter(from)
		err = strTypeToOthers(proc, s, toType, result, length)
	case types.T_uuid:
		s := vector.GenerateFunctionFixedTypeParameter[types.Uuid](from)
		err = uuidToOthers(proc.Ctx, s, toType, result, length)
	case types.T_TS:
		s := vector.GenerateFunctionFixedTypeParameter[types.TS](from)
		err = tsToOthers(proc.Ctx, s, toType, result, length)
	case types.T_Rowid:
		s := vector.GenerateFunctionFixedTypeParameter[types.Rowid](from)
		err = rowidToOthers(proc.Ctx, s, toType, result, length)
	case types.T_json:
		s := vector.GenerateFunctionStrParameter(from)
		err = jsonToOthers(proc.Ctx, s, toType, result, length)
	default:
		// XXX we set the function here to adapt to the BVT cases.
		err = formatCastError(proc.Ctx, from, toType, "")
	}
	return err
}

func scalarNullToOthers(ctx context.Context,
	totype types.Type, result vector.FunctionResultWrapper, length int) error {
	switch totype.Oid {
	case types.T_bool:
		return appendNulls[bool](result, length)
	case types.T_int8:
		return appendNulls[int8](result, length)
	case types.T_int16:
		return appendNulls[int16](result, length)
	case types.T_int32:
		return appendNulls[int32](result, length)
	case types.T_int64:
		return appendNulls[int64](result, length)
	case types.T_uint8:
		return appendNulls[uint8](result, length)
	case types.T_uint16:
		return appendNulls[uint16](result, length)
	case types.T_uint32:
		return appendNulls[uint32](result, length)
	case types.T_uint64:
		return appendNulls[uint64](result, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json:
		return appendNulls[types.Varlena](result, length)
	case types.T_float32:
		return appendNulls[float32](result, length)
	case types.T_float64:
		return appendNulls[float64](result, length)
	case types.T_decimal64:
		return appendNulls[types.Decimal64](result, length)
	case types.T_decimal128:
		return appendNulls[types.Decimal128](result, length)
	case types.T_date:
		return appendNulls[types.Date](result, length)
	case types.T_datetime:
		return appendNulls[types.Datetime](result, length)
	case types.T_time:
		return appendNulls[types.Time](result, length)
	case types.T_timestamp:
		return appendNulls[types.Timestamp](result, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from NULL to %s", totype))
}

func boolToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[bool],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return boolToStr(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return boolToInteger(source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return boolToInteger(source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return boolToInteger(source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return boolToInteger(source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return boolToInteger(source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return boolToInteger(source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return boolToInteger(source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return boolToInteger(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from bool to %s", toType))
}

// although we can merge the int8ToOthers / int16ToOthers ... into intToOthers (use the generic).
// but for extensibility, we didn't do that.
// uint and float are the same.
func int8ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int8],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int8 to %s", toType))
}

func int16ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int16],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int16 to %s", toType))
}

func int32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int32],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int32 to %s", toType))
}

func int64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int64],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int64 to %s", toType))
}

func uint8ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint8],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint8 to %s", toType))
}

func uint16ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint16],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint16 to %s", toType))
}

func uint32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint32],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint32 to %s", toType))
}

func uint64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint64],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint64 to %s", toType))
}

func float32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[float32],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return floatToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return floatToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return floatToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from float32 to %s", toType))
}

func float64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[float64],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return floatToInteger(ctx, source, rs, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return floatToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return floatToDecimal128(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return floatToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from float64 to %s", toType))
}

func dateToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Date],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return dateToSigned(source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return dateToSigned(source, rs, length)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		rs.SetFromParameter(source)
		return nil
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return dateToTime(source, rs, length)
	case types.T_timestamp:
		zone := time.Local
		if proc != nil {
			zone = proc.SessionInfo.TimeZone
		}
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return dateToTimestamp(source, rs, length, zone)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return dateToDatetime(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return dateToStr(source, rs, length)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from date to %s", toType))
}

func datetimeToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Datetime],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return datetimeToInt32(proc.Ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return datetimeToInt64(source, rs, length)
	case types.T_timestamp:
		zone := time.Local
		if proc != nil {
			zone = proc.SessionInfo.TimeZone
		}
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return datetimeToTimestamp(source, rs, length, zone)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return datetimeToDate(source, rs, length)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		v := source.GetSourceVector()
		v.Typ = toType
		rs.SetFromParameter(source)
		return nil
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return datetimeToTime(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return datetimeToStr(source, rs, length)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from datetime to %s", toType))
}

func timestampToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Timestamp],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	zone := time.Local
	if proc != nil {
		zone = proc.SessionInfo.TimeZone
	}

	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return timestampToInt32(proc.Ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return timestampToInt64(source, rs, length)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return timestampToDate(proc.Ctx, source, rs, length, zone)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return timestampToDatetime(proc.Ctx, source, rs, length, zone)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		v := source.GetSourceVector()
		v.Typ = toType
		rs.SetFromParameter(source)
		return nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return timestampToStr(source, rs, length, zone)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from timestamp to %s", toType))
}

func timeToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Time],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return timeToInteger(ctx, source, rs, length)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return timeToDate(source, rs, length)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return timeToDatetime(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		v := source.GetSourceVector()
		v.Typ = toType
		rs.SetFromParameter(source)
		return nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return timeToStr(source, rs, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return timeToDecimal64(ctx, source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return timeToDecimal128(ctx, source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from time to %s", toType))
}

func decimal64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Decimal64],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return decimal64ToFloat(ctx, source, rs, length, 32)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return decimal64ToFloat(ctx, source, rs, length, 64)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return decimal64ToInt64(ctx, source, rs, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal64ToUnsigned(ctx, source, rs, 64, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		v := source.GetSourceVector()
		v.Typ = toType
		rs.SetFromParameter(source)
		return nil
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return decimal64ToDecimal128(source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return decimal64ToTimestamp(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return decimal64ToTime(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return decimal64ToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from decimal64 to %s", toType))
}

func decimal128ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Decimal128],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return decimal128ToSigned(ctx, source, rs, 32, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return decimal128ToSigned(ctx, source, rs, 64, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal128ToUnsigned(ctx, source, rs, 64, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return decimal128ToDecimal64(ctx, source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		v := source.GetSourceVector()
		v.Typ = toType
		rs.SetFromParameter(source)
		return nil
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return decimal128ToFloat(ctx, source, rs, length, 32)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return decimal128ToFloat(ctx, source, rs, length, 64)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return decimal128ToTime(source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return decimal128ToTimestamp(source, rs, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return decimal128ToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from decimal128 to %s", toType))
}

func strTypeToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Varlena],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	ctx := proc.Ctx
	switch toType.Oid {
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return strToSigned(ctx, source, rs, 8, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return strToSigned(ctx, source, rs, 16, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return strToSigned(ctx, source, rs, 32, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return strToSigned(ctx, source, rs, 64, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return strToUnsigned(ctx, source, rs, 8, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return strToUnsigned(ctx, source, rs, 16, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return strToUnsigned(ctx, source, rs, 32, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return strToUnsigned(ctx, source, rs, 64, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return strToFloat(ctx, source, rs, 32, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return strToFloat(ctx, source, rs, 64, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return strToDecimal64(source, rs, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return strToDecimal128(source, rs, length)
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return strToBool(source, rs, length)
	case types.T_json:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToJson(source, rs, length)
	case types.T_uuid:
		rs := vector.MustFunctionResult[types.Uuid](result)
		return strToUuid(source, rs, length)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return strToDate(source, rs, length)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return strToDatetime(source, rs, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return strToTime(source, rs, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		zone := time.Local
		if proc != nil {
			zone = proc.SessionInfo.TimeZone
		}
		return strToTimestamp(source, rs, zone, length)
	case types.T_char, types.T_varchar, types.T_text, types.T_blob:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToStr(proc.Ctx, source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from %s to %s", source.GetType(), toType))
}

func uuidToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Uuid],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return uuidToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uuid to %s", toType))
}

func tsToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.TS],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	if toType.Oid == types.T_TS {
		rs := vector.MustFunctionResult[types.TS](result)
		rs.SetFromParameter(source)
		return nil
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from ts to %s", toType))
}

func rowidToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Rowid],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	if toType.Oid == types.T_Rowid {
		rs := vector.MustFunctionResult[types.Rowid](result)
		rs.SetFromParameter(source)
		return nil
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from rowid to %s", toType))
}

func jsonToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Varlena],
	toType types.Type, result vector.FunctionResultWrapper, length int) error {
	switch toType.Oid {
	case types.T_char, types.T_varchar, types.T_text:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return jsonToStr(source, rs, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from json to %s", toType))
}

// XXX do not use it to cast float to integer, please use floatToInteger
func numericToNumeric[T1, T2 constraints.Integer | constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T1], to *vector.FunctionResult[T2], length int) error {
	var i uint64
	var dftValue T2
	times := uint64(length)
	if err := overflowForNumericToNumeric[T1, T2](ctx, from.UnSafeGetAllValue()); err != nil {
		return err
	}
	for i = 0; i < times; i++ {
		v, isnull := from.GetValue(i)
		if isnull {
			if err := to.Append(dftValue, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(T2(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// XXX do not use it to cast float to integer, please use floatToInteger
func floatToInteger[T1 constraints.Float, T2 constraints.Integer](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T1], to *vector.FunctionResult[T2],
	length int) error {
	var i uint64
	var dftValue T2
	times := uint64(length)
	if err := overflowForNumericToNumeric[T1, T2](ctx, from.UnSafeGetAllValue()); err != nil {
		return err
	}
	for i = 0; i < times; i++ {
		v, isnull := from.GetValue(i)
		if isnull {
			if err := to.Append(dftValue, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(T2(math.Round(float64(v))), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func numericToBool[T constraints.Integer | constraints.Float](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[bool], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		err := to.Append(v != 0, null)
		if err != nil {
			return err
		}
	}
	return nil
}

func boolToStr(
	from vector.FunctionParameterWrapper[bool],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			if v {
				if err := to.AppendStr([]byte("1"), false); err != nil {
					return err
				}
			} else {
				if err := to.AppendStr([]byte("0"), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func boolToInteger[T constraints.Integer](
	from vector.FunctionParameterWrapper[bool],
	to *vector.FunctionResult[T], length int) error {
	var i uint64
	l := uint64(length)
	var dft T
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			if v {
				if err := to.Append(1, false); err != nil {
					return err
				}
			} else {
				if err := to.Append(0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func signedToDecimal64[T1 constraints.Signed](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal64], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := types.Decimal64_FromInt64(int64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func signedToDecimal128[T1 constraints.Signed](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal128], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := types.Decimal128_FromInt64(int64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToDecimal64[T1 constraints.Unsigned](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal64], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := types.Decimal64_FromUint64(uint64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToDecimal128[T1 constraints.Unsigned](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal128], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := types.Decimal128_FromUint64(uint64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToDecimal64[T constraints.Float](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Decimal64], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result64, err := types.Decimal64_FromFloat64(float64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result64, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToDecimal128[T constraints.Float](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Decimal128], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	toType := to.GetType()

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result128, err := types.Decimal128_FromFloat64(float64(v), toType.Width, toType.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result128, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func signedToStr[T constraints.Integer](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(strconv.FormatInt(int64(v), 10))
			if err := to.AppendStr(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToStr[T constraints.Unsigned](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(strconv.FormatUint(uint64(v), 10))
			if err := to.AppendStr(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToStr[T constraints.Float](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	bitSize := int(unsafe.Sizeof(T(0)) * 8)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(strconv.FormatFloat(float64(v), 'G', -1, bitSize))
			if err := to.AppendStr(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func integerToTimestamp[T constraints.Integer](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Timestamp], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Timestamp
	// XXX what is the 32536771199
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null || v < 0 || uint64(v) > 32536771199 {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result := types.UnixToTimestamp(int64(v))
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func integerToTime[T constraints.Integer](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Time
	toType := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		vI64 := int64(v)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			if vI64 < types.MinInputIntTime || vI64 > types.MaxInputIntTime {
				return moerr.NewOutOfRange(ctx, "time", "value %d", v)
			}
			result, err := types.ParseInt64ToTime(vI64, toType.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToSigned[T int32 | int64](
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[T], length int) error {
	var i uint64
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v.DaysSinceUnixEpoch()
			if err := to.Append(T(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToTime(
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToTime(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToTime(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	l := uint64(length)
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToTime(totype.Precision), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToTimestamp(
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[types.Timestamp], length int,
	zone *time.Location) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToTimestamp(zone), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToInt32(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[int32], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v.SecsSinceUnixEpoch()
			if val < math.MinInt32 || val > math.MaxInt32 {
				return moerr.NewOutOfRange(ctx, "int32", "value '%v'", val)
			}
			if err := to.Append(int32(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToInt64(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[int64], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v.SecsSinceUnixEpoch()
			if err := to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToTimestamp(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Timestamp], length int,
	zone *time.Location) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToTimestamp(zone), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToDatetime(
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[types.Datetime], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToDatetime(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToDatetime(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Datetime], length int,
	zone *time.Location) error {
	var i uint64
	l := uint64(length)
	tempR := make([]types.Datetime, 1)
	tempT := make([]types.Timestamp, 1)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			tempT[0] = v
			result, err := binary.TimestampToDatetime(ctx, zone, tempT, tempR)
			if err != nil {
				return err
			}
			if err = to.Append(result[0], false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDatetime(
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Datetime], length int) error {
	var i uint64
	l := uint64(length)
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToDatetime(totype.Precision), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToDate(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Date], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToInt32(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[int32], length int) error {
	var i uint64
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v.Unix()
			if val < math.MinInt32 || val > math.MaxInt32 {
				return moerr.NewOutOfRange(ctx, "int32", "value '%v'", val)
			}
			if err := to.Append(int32(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToInt64(
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[int64], length int) error {
	var i uint64
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v.Unix()
			if err := to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToDate(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Date], length int,
	zone *time.Location) error {
	var i uint64
	l := uint64(length)
	tempR := make([]types.Datetime, 1)
	tempT := make([]types.Timestamp, 1)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			tempT[0] = v
			// XXX I'm not sure if it's a good way to convert it to datetime first.
			// but I just follow the old logic of old code.
			result, err := binary.TimestampToDatetime(ctx, zone, tempT, tempR)
			if err != nil {
				return err
			}
			if err = to.Append(result[0].ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToInteger[T constraints.Integer](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[T], length int) error {
	var i uint64
	l := uint64(length)
	var dft T
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			r := v.ToInt64()
			// XXX we may need an elegant method to do overflow check.
			if err := overflowForNumericToNumeric[int64, T](ctx, []int64{r}); err != nil {
				return err
			}
			if err := to.Append(T(r), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDate(
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Date], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(v.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToStr(
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			if err := to.AppendStr([]byte(v.String()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToStr(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			if err := to.AppendStr([]byte(v.String2(fromType.Precision)), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToStr(
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Varlena], length int,
	zone *time.Location) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			if err := to.AppendStr([]byte(v.String2(zone, fromType.Precision)), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToStr(
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			if err := to.AppendStr([]byte(v.String2(fromType.Precision)), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDecimal64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Decimal64], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	fromType := from.GetType()
	totype := to.GetType()
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.ToDecimal64(ctx, totype.Width, fromType.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDecimal128(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Decimal128], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	fromType := from.GetType()
	totype := to.GetType()
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.ToDecimal128(ctx, totype.Width, fromType.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToInt64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[int64], length int) error {
	var i uint64
	l := uint64(length)
	fromTyp := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(fromTyp.Scale)
			floatRepresentation, err := strconv.ParseFloat(xStr, 64)
			if err != nil {
				return err
			}
			if floatRepresentation > math.MaxInt64 || floatRepresentation < math.MinInt64 {
				return moerr.NewOutOfRange(ctx, "int64", "value '%v'", xStr)
			}
			err = to.Append(int64(math.Round(floatRepresentation)), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToSigned[T constraints.Signed](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[T], bitSize int, length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(0)
			result, err := strconv.ParseInt(xStr, 10, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx,
					fmt.Sprintf("int%d", bitSize),
					"value '%v'", xStr)
			}
			err = to.Append(T(result), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToUnsigned[T constraints.Unsigned](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[T], bitSize int,
	length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(fromType.Scale)
			xStr = strings.Split(xStr, ".")[0]
			result, err := strconv.ParseUint(xStr, 10, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx,
					fmt.Sprintf("uint%d", bitSize),
					"value '%v'", xStr)
			}
			err = to.Append(T(result), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToUnsigned[T constraints.Unsigned](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[T], bitSize int,
	length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(fromType.Scale)
			xStr = strings.Split(xStr, ".")[0]
			result, err := strconv.ParseUint(xStr, 10, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx,
					fmt.Sprintf("uint%d", bitSize),
					"value '%v'", xStr)
			}
			err = to.Append(T(result), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToTime(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	l := uint64(length)
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			result, err := types.ParseDecimal64lToTime(v, totype.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToTime(
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	l := uint64(length)
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			result, err := types.ParseDecimal128lToTime(v, totype.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToTimestamp(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Timestamp], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			ts := types.Timestamp(v.ToInt64())
			if err := to.Append(ts, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToTimestamp(
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Timestamp], length int) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			ts := types.Timestamp(v.ToInt64())
			if err := to.Append(ts, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToFloat[T constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[T], length int, bitSize int) error {
	// IF float32, then bitSize should be 32. IF float64, then 64
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(fromType.Scale)
			result, err := strconv.ParseFloat(xStr, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx, "float32", "value '%v'", xStr)
			}
			if err = to.Append(T(result), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToFloat[T constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[T], length int, bitSize int) error {
	// IF float32, then bitSize should be 32. IF float64, then 64
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			xStr := v.ToStringWithScale(fromType.Scale)
			result, err := strconv.ParseFloat(xStr, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx, "float32", "value '%v'", xStr)
			}
			if err = to.Append(T(result), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToDecimal128(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Decimal128], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	totype := to.GetType()
	{
		v := to.GetResultVector()
		v.Typ.Scale = from.GetType().Scale
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := types.Decimal128_FromDecimal64WithScale(
				v, totype.Width, totype.Scale)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// the scale of decimal128 is guaranteed to be less than 18
// this cast function is too slow, and therefore only temporary, rewrite needed
func decimal128ToDecimal64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Decimal64], length int) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.ToDecimal64(totype.Width, totype.Scale)
			if err != nil {
				// XXX so ...
				return moerr.NewOutOfRange(ctx, "dec64", "value '%v'", v)
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToStr(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.ToStringWithScale(fromType.Scale))
			if err := to.AppendStr(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToStr(
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.ToStringWithScale(fromType.Scale))
			if err := to.AppendStr(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToSigned[T constraints.Signed](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[T], bitSize int,
	length int) error {
	var i uint64
	var l = uint64(length)
	isBinary := from.GetSourceVector().GetIsBin()

	var result T
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if isBinary {
				r, err := strconv.ParseInt(
					hex.EncodeToString(v), 16, 64)
				if err != nil {
					if strings.Contains(err.Error(), "value out of range") {
						// the string maybe non-visible,don't print it
						return moerr.NewOutOfRange(ctx, "int", "")
					}
					return moerr.NewInvalidArg(ctx, "cast to int", r)
				}
				result = T(r)
			} else {
				s := convertByteSliceToString(v)
				r, err := strconv.ParseInt(
					strings.TrimSpace(s), 10, bitSize)
				if err != nil {
					// XXX I'm not sure if we should return the int8 / int16 / int64 info. or
					// just return the int. the old code just return the int. too much bvt result needs to update.
					if strings.Contains(err.Error(), "value out of range") {
						return moerr.NewOutOfRange(ctx, "int", "value '%s'", s)
					}
					return moerr.NewInvalidArg(ctx, "cast to int", s)
				}
				result = T(r)
			}
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToUnsigned[T constraints.Unsigned](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[T], bitSize int,
	length int) error {
	var i uint64
	var l = uint64(length)
	isBinary := from.GetSourceVector().GetIsBin()

	var val uint64
	var tErr error
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			var res *string
			if isBinary {
				s := hex.EncodeToString(v)
				res = &s
				val, tErr = strconv.ParseUint(s, 16, 64)
			} else {
				s := convertByteSliceToString(v)
				res = &s
				val, tErr = strconv.ParseUint(strings.TrimSpace(s), 10, bitSize)
			}
			if tErr != nil {
				if strings.Contains(tErr.Error(), "value out of range") {
					return moerr.NewOutOfRange(ctx, fmt.Sprintf("uint%d", bitSize), "value '%s'", *res)
				}
				return moerr.NewInvalidArg(ctx, fmt.Sprintf("cast to uint%d", bitSize), *res)
			}
			if err := to.Append(T(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToFloat[T constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[T], bitSize int,
	length int) error {
	var i uint64
	var l = uint64(length)
	isBinary := from.GetSourceVector().GetIsBin()

	var result T
	var tErr error
	var r1 uint64
	var r2 float64
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if isBinary {
				s := hex.EncodeToString(v)
				r1, tErr = strconv.ParseUint(s, 16, 64)
				if tErr != nil {
					if strings.Contains(tErr.Error(), "value out of range") {
						return moerr.NewOutOfRange(ctx, "float", "value '%s'", s)
					}
					return moerr.NewInvalidArg(ctx, "cast to float", s)
				}
				result = T(r1)
			} else {
				s := convertByteSliceToString(v)
				r2, tErr = strconv.ParseFloat(s, bitSize)
				if tErr != nil {
					return tErr
				}
				result = T(r2)
			}

			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToDecimal64(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Decimal64], length int,
) error {
	var i uint64
	var l = uint64(length)
	var dft types.Decimal64
	totype := to.GetType()
	isb := from.GetSourceVector().GetIsBin()
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			result, err := types.ParseStringToDecimal64(
				s, totype.Width, totype.Scale, isb)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToDecimal128(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Decimal128], length int,
) error {
	var i uint64
	var l = uint64(length)
	var dft types.Decimal128
	totype := to.GetType()
	isb := from.GetSourceVector().GetIsBin()
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			result, err := types.ParseStringToDecimal128(
				s, totype.Width, totype.Scale, isb)
			if err != nil {
				return err
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToBool(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[bool], length int) error {
	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(false, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseBool(s)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToUuid(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Uuid], length int) error {
	var i uint64
	var l = uint64(length)
	var dft types.Uuid
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseUuid(s)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToJson(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			json, err := types.ParseStringToByteJson(s)
			if err != nil {
				return err
			}
			val, err := types.EncodeJson(json)
			if err != nil {
				return err
			}
			if err = to.AppendStr(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToDate(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Date], length int) error {
	var i uint64
	var l = uint64(length)
	var dft types.Date
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseDateCast(s)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToTime(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Time], length int) error {
	var i uint64
	var l = uint64(length)
	var dft types.Time
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseTime(s, totype.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToDatetime(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Datetime], length int) error {
	var i uint64
	var l = uint64(length)
	var dft types.Datetime
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseDatetime(s, totype.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToTimestamp(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Timestamp],
	zone *time.Location, length int) error {
	var i uint64
	var l = uint64(length)
	var dft types.Timestamp
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			s := convertByteSliceToString(v)
			val, err := types.ParseTimestamp(zone, s, totype.Precision)
			if err != nil {
				return err
			}
			if err = to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int) error {
	totype := to.GetType()
	destLen := int(totype.Width)
	var i uint64
	var l = uint64(length)
	if totype.Oid != types.T_text && destLen != 0 {
		for i = 0; i < l; i++ {
			v, null := from.GetStrValue(i)
			if null {
				if err := to.AppendStr(nil, true); err != nil {
					return err
				}
				continue
			}
			// check the length.
			s := convertByteSliceToString(v)
			if utf8.RuneCountInString(s) > destLen {
				return formatCastError(ctx, from.GetSourceVector(), totype, fmt.Sprintf(
					"Src length %v is larger than Dest length %v", len(s), destLen))
			}
			if err := to.AppendStr(v, false); err != nil {
				return err
			}
		}
	} else {
		for i = 0; i < l; i++ {
			v, null := from.GetStrValue(i)
			if null {
				if err := to.AppendStr(nil, true); err != nil {
					return err
				}
				continue
			}
			// check the length.
			if err := to.AppendStr(v, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func uuidToStr(
	from vector.FunctionParameterWrapper[types.Uuid],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			result := v.ToString()
			if err := to.AppendStr([]byte(result), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func jsonToStr(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int) error {
	var i uint64
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.AppendStr(nil, true); err != nil {
				return err
			}
		} else {
			bj := types.DecodeJson(v)
			val, err := bj.MarshalJSON()
			if err != nil {
				return err
			}
			if err = to.AppendStr(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func overflowForNumericToNumeric[T1, T2 constraints.Integer | constraints.Float](ctx context.Context, xs []T1) error {
	if len(xs) == 0 {
		return nil
	}

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
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}
	case *int16:
		nxs := unsafe.Slice((*int16)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16, *uint32, *uint64:
			for _, x := range nxs {
				if x < 0 {
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}
	case *int32:
		nxs := unsafe.Slice((*int32)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if x < math.MinInt16 || x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32, *uint64:
			for _, x := range nxs {
				if x < 0 {
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}
	case *int64:
		nxs := unsafe.Slice((*int64)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x < math.MinInt8 || x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if x < math.MinInt16 || x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range nxs {
				if x < math.MinInt32 || x > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range nxs {
				if x < 0 || x > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range nxs {
				if x < 0 {
					// XXX for adapt to bvt, but i don't know why we hide the wrong value here.
					return moerr.NewOutOfRange(ctx, "uint64", "")
				}
			}
		}
	case *uint8:
		nxs := unsafe.Slice((*uint8)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		}
	case *uint16:
		nxs := unsafe.Slice((*uint16)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		}
	case *uint32:
		nxs := unsafe.Slice((*uint32)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range nxs {
				if x > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		}
	case *uint64:
		nxs := unsafe.Slice((*uint64)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range nxs {
				if x > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range nxs {
				if x > math.MaxInt64 {
					return moerr.NewOutOfRangeNoCtx("int64", "")
				}
			}
		case *uint8:
			for _, x := range nxs {
				if x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range nxs {
				if x > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		}
	case *float32:
		nxs := unsafe.Slice((*float32)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxInt64 {
					return moerr.NewOutOfRange(ctx, "int64", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range nxs {
				if math.Round(float64(x)) > math.MaxUint64 {
					return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", x)
				}
			}
		}
	case *float64:
		nxs := unsafe.Slice((*float64)(unsafe.Pointer(&xs[0])), len(xs))
		switch ri.(type) {
		case *int8:
			for _, x := range nxs {
				if math.Round(x) > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for _, x := range nxs {
				if math.Round(x) > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for _, x := range nxs {
				if math.Round(x) > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for _, x := range nxs {
				if math.Round(x) > math.MaxInt64 || math.Round(x) < math.MinInt64 {
					return moerr.NewOutOfRange(ctx, "int64", "value '%v'", x)
				}
			}
		case *uint8:
			for _, x := range nxs {
				if math.Round(x) > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for _, x := range nxs {
				if math.Round(x) > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for _, x := range nxs {
				if math.Round(x) > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for _, x := range nxs {
				if math.Round(x) > math.MaxUint64 {
					return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", x)
				}
			}
		case *float32:
			for _, x := range nxs {
				if x > math.MaxFloat32 {
					return moerr.NewOutOfRange(ctx, "float32", "value '%v'", x)
				}
			}
		}
	}
	return nil
}

func appendNulls[T types.FixedSizeT](result vector.FunctionResultWrapper, length int) error {
	if r, ok := result.(*vector.FunctionResult[types.Varlena]); ok {
		var i uint64
		for i = 0; i < uint64(length); i++ {
			if err := r.AppendStr(nil, true); err != nil {
				return err
			}
		}
		return nil
	}
	if r, ok := result.(*vector.FunctionResult[T]); ok {
		var t T
		var i uint64
		for i = 0; i < uint64(length); i++ {
			if err := r.Append(t, true); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// convertByteSliceToString is just a temp method.
func convertByteSliceToString(v []byte) string {
	// s := *(*string)(unsafe.Pointer(&v))
	return string(v)
}
