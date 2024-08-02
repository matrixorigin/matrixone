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

package function

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// XXX need this one to make a pretty function register.
var supportedTypeCast = map[types.T][]types.T{
	types.T_any: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_array_float32, types.T_array_float64,
		types.T_datalink,
	},

	types.T_bool: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_bit: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_int8: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_int16: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_int32: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_int64: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_uint8: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_uint16: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_uint32: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_uint64: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_enum,
		types.T_datalink,
	},

	types.T_float32: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_float64: {
		types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_date: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_datetime: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_timestamp: {
		types.T_int32, types.T_int64,
		types.T_date, types.T_datetime,
		types.T_timestamp,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_time: {
		types.T_date, types.T_datetime,
		types.T_time,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
		types.T_bit,
		types.T_datalink,
	},

	types.T_decimal64: {
		types.T_bit,
		types.T_float32, types.T_float64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_time, types.T_timestamp,
		types.T_datalink,
	},

	types.T_decimal128: {
		types.T_bit,
		types.T_float32, types.T_float64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_char: {
		types.T_bit,
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
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_varchar: {
		types.T_bit,
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
		types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64,
		types.T_datalink,
	},

	types.T_binary: {
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_varbinary, types.T_binary,
		types.T_datalink,
	},

	types.T_varbinary: {
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_uuid,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_datalink,
	},

	types.T_blob: {
		types.T_bit,
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
		types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64,
		types.T_datalink,
	},

	types.T_text: {
		types.T_bit,
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
		types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64,
	},
	types.T_datalink: {
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_json,
		types.T_uuid,
		types.T_text,
		types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64,
		types.T_datalink,
	},

	types.T_json: {
		types.T_char, types.T_varchar, types.T_text, types.T_datalink,
	},

	types.T_uuid: {
		types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink,
	},

	types.T_TS: {
		types.T_TS,
	},

	types.T_Rowid: {
		types.T_Rowid,
	},

	types.T_enum: {
		types.T_enum, types.T_uint16, types.T_uint8, types.T_uint32, types.T_uint64, types.T_uint128,
		types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink,
	},

	types.T_array_float32: {
		types.T_array_float32, types.T_array_float64,
	},
	types.T_array_float64: {
		types.T_array_float32, types.T_array_float64,
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

func NewCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	// Cast Parameter1 as Type Parameter2
	fromType := parameters[0].GetType()
	toType := parameters[1].GetType()
	from := parameters[0]
	switch fromType.Oid {
	case types.T_any: // scalar null
		err = scalarNullToOthers(proc.Ctx, *toType, result, length, selectList)
	case types.T_bool:
		s := vector.GenerateFunctionFixedTypeParameter[bool](from)
		err = boolToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_bit:
		s := vector.GenerateFunctionFixedTypeParameter[uint64](from)
		err = bitToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_int8:
		s := vector.GenerateFunctionFixedTypeParameter[int8](from)
		err = int8ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_int16:
		s := vector.GenerateFunctionFixedTypeParameter[int16](from)
		err = int16ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_int32:
		s := vector.GenerateFunctionFixedTypeParameter[int32](from)
		err = int32ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_int64:
		s := vector.GenerateFunctionFixedTypeParameter[int64](from)
		err = int64ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_uint8:
		s := vector.GenerateFunctionFixedTypeParameter[uint8](from)
		err = uint8ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_uint16:
		s := vector.GenerateFunctionFixedTypeParameter[uint16](from)
		err = uint16ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_uint32:
		s := vector.GenerateFunctionFixedTypeParameter[uint32](from)
		err = uint32ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_uint64:
		s := vector.GenerateFunctionFixedTypeParameter[uint64](from)
		err = uint64ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_float32:
		s := vector.GenerateFunctionFixedTypeParameter[float32](from)
		err = float32ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_float64:
		s := vector.GenerateFunctionFixedTypeParameter[float64](from)
		err = float64ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_decimal64:
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](from)
		err = decimal64ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_decimal128:
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](from)
		err = decimal128ToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_date:
		s := vector.GenerateFunctionFixedTypeParameter[types.Date](from)
		err = dateToOthers(proc, s, *toType, result, length, selectList)
	case types.T_datetime:
		s := vector.GenerateFunctionFixedTypeParameter[types.Datetime](from)
		err = datetimeToOthers(proc, s, *toType, result, length, selectList)
	case types.T_time:
		s := vector.GenerateFunctionFixedTypeParameter[types.Time](from)
		err = timeToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_timestamp:
		s := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](from)
		err = timestampToOthers(proc, s, *toType, result, length, selectList)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		s := vector.GenerateFunctionStrParameter(from)
		err = strTypeToOthers(proc, s, *toType, result, length, selectList)
	case types.T_array_float32, types.T_array_float64:
		//NOTE: Don't mix T_array and T_varchar.
		// T_varchar will have "[1,2,3]" string
		// T_array will have "@@@#@!#@!@#!" binary.
		s := vector.GenerateFunctionStrParameter(from)
		err = arrayTypeToOthers(proc, s, *toType, result, length, selectList)
	case types.T_uuid:
		s := vector.GenerateFunctionFixedTypeParameter[types.Uuid](from)
		err = uuidToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_TS:
		s := vector.GenerateFunctionFixedTypeParameter[types.TS](from)
		err = tsToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_Rowid:
		s := vector.GenerateFunctionFixedTypeParameter[types.Rowid](from)
		err = rowidToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_Blockid:
		s := vector.GenerateFunctionFixedTypeParameter[types.Blockid](from)
		err = blockidToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_json:
		s := vector.GenerateFunctionStrParameter(from)
		err = jsonToOthers(proc.Ctx, s, *toType, result, length, selectList)
	case types.T_enum:
		s := vector.GenerateFunctionFixedTypeParameter[types.Enum](from)
		err = enumToOthers(proc.Ctx, s, *toType, result, length, selectList)
	default:
		// XXX we set the function here to adapt to the BVT cases.
		err = formatCastError(proc.Ctx, from, *toType, "")
	}
	return err
}

func scalarNullToOthers(ctx context.Context,
	totype types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch totype.Oid {
	case types.T_bool:
		return appendNulls[bool](result, length, selectList)
	case types.T_bit:
		return appendNulls[uint64](result, length, selectList)
	case types.T_int8:
		return appendNulls[int8](result, length, selectList)
	case types.T_int16:
		return appendNulls[int16](result, length, selectList)
	case types.T_int32:
		return appendNulls[int32](result, length, selectList)
	case types.T_int64:
		return appendNulls[int64](result, length, selectList)
	case types.T_uint8:
		return appendNulls[uint8](result, length, selectList)
	case types.T_uint16:
		return appendNulls[uint16](result, length, selectList)
	case types.T_uint32:
		return appendNulls[uint32](result, length, selectList)
	case types.T_uint64:
		return appendNulls[uint64](result, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_json,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return appendNulls[types.Varlena](result, length, selectList)
	case types.T_float32:
		return appendNulls[float32](result, length, selectList)
	case types.T_float64:
		return appendNulls[float64](result, length, selectList)
	case types.T_decimal64:
		return appendNulls[types.Decimal64](result, length, selectList)
	case types.T_decimal128:
		return appendNulls[types.Decimal128](result, length, selectList)
	case types.T_date:
		return appendNulls[types.Date](result, length, selectList)
	case types.T_datetime:
		return appendNulls[types.Datetime](result, length, selectList)
	case types.T_time:
		return appendNulls[types.Time](result, length, selectList)
	case types.T_timestamp:
		return appendNulls[types.Timestamp](result, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from NULL to %s", totype))
}

func boolToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[bool],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return rs.DupFromParameter(source, length)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_binary,
		types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return boolToStr(source, rs, length, toType)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return boolToInteger(source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return boolToInteger(source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from bool to %s", toType))
}

func bitToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint64],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return bitToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from %s to %s", source.GetType(), toType))
}

// although we can merge the int8ToOthers / int16ToOthers ... into intToOthers (use the generic).
// but for extensibility, we didn't do that.
// uint and float are the same.
func int8ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int8],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return rs.DupFromParameter(source, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int8 to %s", toType))
}

func int16ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int16],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return rs.DupFromParameter(source, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int16 to %s", toType))
}

func int32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int32],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return rs.DupFromParameter(source, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int32 to %s", toType))
}

func int64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[int64],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return rs.DupFromParameter(source, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return signedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return signedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		// string type.
		rs := vector.MustFunctionResult[types.Varlena](result)
		return signedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from int64 to %s", toType))
}

func uint8ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint8],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return rs.DupFromParameter(source, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint8 to %s", toType))
}

func uint16ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint16],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return rs.DupFromParameter(source, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint16 to %s", toType))
}

func uint32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint32],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return rs.DupFromParameter(source, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint32 to %s", toType))
}

func uint64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[uint64],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return rs.DupFromParameter(source, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return unsignedToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return unsignedToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return unsignedToStr(ctx, source, rs, length, toType)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return integerToTime(ctx, source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return integerToTimestamp(source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return integerToEnum(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uint64 to %s", toType))
}

func float32ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[float32],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		if rs.GetType().Scale >= 0 && rs.GetType().Width > 0 {
			return floatToFixFloat(ctx, source, rs, length, selectList)
		}
		return rs.DupFromParameter(source, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		if rs.GetType().Scale >= 0 && rs.GetType().Width > 0 {
			return floatToFixFloat(ctx, source, rs, length, selectList)
		}
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return floatToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return floatToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return floatToStr(ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from float32 to %s", toType))
}

func float64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[float64],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return numericToBool(source, rs, length, selectList)
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return numericToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return floatToInteger(ctx, source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		if rs.GetType().Scale >= 0 && rs.GetType().Width > 0 {
			return floatToFixFloat(ctx, source, rs, length, selectList)
		}
		return numericToNumeric(ctx, source, rs, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		if rs.GetType().Scale >= 0 && rs.GetType().Width > 0 {
			return floatToFixFloat(ctx, source, rs, length, selectList)
		}
		return rs.DupFromParameter(source, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return floatToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return floatToDecimal128(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_text, types.T_varbinary, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return floatToStr(ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from float64 to %s", toType))
}

func dateToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Date],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return dateToSigned(source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return dateToSigned(source, rs, length, selectList)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return rs.DupFromParameter(source, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return dateToTime(source, rs, length, selectList)
	case types.T_timestamp:
		zone := time.Local
		if proc != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return dateToTimestamp(source, rs, length, zone)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return dateToDatetime(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return dateToStr(proc.Ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from date to %s", toType))
}

func datetimeToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Datetime],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return datetimeToInt32(proc.Ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return datetimeToInt64(source, rs, length, selectList)
	case types.T_timestamp:
		zone := time.Local
		if proc != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return datetimeToTimestamp(source, rs, length, zone)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return datetimeToDate(source, rs, length, selectList)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		v := source.GetSourceVector()
		v.SetType(toType)
		return rs.DupFromParameter(source, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return datetimeToTime(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return datetimeToStr(proc.Ctx, source, rs, length, toType)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return datetimeToDecimal64(proc.Ctx, source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return datetimeToDecimal128(proc.Ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from datetime to %s", toType))
}

func timestampToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Timestamp],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	zone := time.Local
	if proc != nil {
		zone = proc.GetSessionInfo().TimeZone
	}

	switch toType.Oid {
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return timestampToInt32(proc.Ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return timestampToInt64(source, rs, length, selectList)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return timestampToDate(proc.Ctx, source, rs, length, zone)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return timestampToDatetime(proc.Ctx, source, rs, length, zone)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		v := source.GetSourceVector()
		v.SetType(toType)
		return rs.DupFromParameter(source, length)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return timestampToStr(proc.Ctx, source, rs, length, zone, toType)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return timestampToDecimal64(proc.Ctx, source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return timestampToDecimal128(proc.Ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from timestamp to %s", toType))
}

func timeToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Time],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return timeToInteger(ctx, source, rs, length, selectList)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return timeToDate(source, rs, length, selectList)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return timeToDatetime(source, rs, length, selectList)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		v := source.GetSourceVector()
		v.SetType(toType)
		return rs.DupFromParameter(source, length)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return timeToStr(ctx, source, rs, length, toType)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return timeToDecimal64(ctx, source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return timeToDecimal128(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from time to %s", toType))
}

func decimal64ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Decimal64],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal64ToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return decimal64ToFloat(ctx, source, rs, length, 32)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return decimal64ToFloat(ctx, source, rs, length, 64)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return decimal64ToSigned(ctx, source, rs, 8, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return decimal64ToSigned(ctx, source, rs, 16, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return decimal64ToSigned(ctx, source, rs, 32, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return decimal64ToSigned(ctx, source, rs, 64, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return decimal64ToUnsigned(ctx, source, rs, 8, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return decimal64ToUnsigned(ctx, source, rs, 16, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return decimal64ToUnsigned(ctx, source, rs, 32, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal64ToUnsigned(ctx, source, rs, 64, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		if source.GetType().Scale == toType.Scale && source.GetType().Width >= toType.Width {
			if err := rs.DupFromParameter(source, length); err != nil {
				return err
			}
			v := rs.GetResultVector()
			v.SetType(toType)
			return nil
		}
		return decimal64ToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return decimal64ToDecimal128Array(source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return decimal64ToTimestamp(source, rs, length, selectList)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return decimal64ToTime(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return decimal64ToStr(ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from decimal64 to %s", toType))
}

func decimal128ToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Decimal128],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal128ToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return decimal128ToSigned(ctx, source, rs, 8, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return decimal128ToSigned(ctx, source, rs, 16, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return decimal128ToSigned(ctx, source, rs, 32, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return decimal128ToSigned(ctx, source, rs, 64, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return decimal128ToUnsigned(ctx, source, rs, 8, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return decimal128ToUnsigned(ctx, source, rs, 16, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return decimal128ToUnsigned(ctx, source, rs, 32, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return decimal128ToUnsigned(ctx, source, rs, 64, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return decimal128ToDecimal64(ctx, source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		if source.GetType().Scale == toType.Scale && source.GetType().Width >= toType.Width {
			if err := rs.DupFromParameter(source, length); err != nil {
				return err
			}
			v := source.GetSourceVector()
			v.SetType(toType)
			return nil
		}
		return decimal128ToDecimal128(source, rs, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return decimal128ToFloat(ctx, source, rs, length, 32)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return decimal128ToFloat(ctx, source, rs, length, 64)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return decimal128ToTime(source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return decimal128ToTimestamp(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return decimal128ToStr(ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from decimal128 to %s", toType))
}

func strTypeToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Varlena],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	ctx := proc.Ctx

	fromType := source.GetType()
	if fromType.Oid == types.T_blob {
		// For handling BLOB to ARRAY implicit casting.
		// This is used for VECTOR FAST/BINARY IO.
		// SQL: insert into t2 values(2, decode("7e98b23e9e10383b2f41133f", "hex"));
		switch toType.Oid {
		case types.T_array_float32:
			rs := vector.MustFunctionResult[types.Varlena](result)
			return blobToArray[float32](ctx, source, rs, length, toType)
		case types.T_array_float64:
			rs := vector.MustFunctionResult[types.Varlena](result)
			return blobToArray[float64](ctx, source, rs, length, toType)
			// NOTE 1: don't add `switch default` and panic here. If `T_blob` to `ARRAY` is not required,
			// then continue to the `str` to `Other` code.
			// NOTE 2: don't create a switch T_blob case in NewCast() as
			// we only want to handle BLOB-->ARRAY condition separately and
			// rest of the flow is similar to strTypeToOthers.
		}
	}
	switch toType.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return strToBit(ctx, source, rs, int(toType.Width), length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return strToSigned(ctx, source, rs, 8, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return strToSigned(ctx, source, rs, 16, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return strToSigned(ctx, source, rs, 32, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return strToSigned(ctx, source, rs, 64, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return strToUnsigned(ctx, source, rs, 8, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return strToUnsigned(ctx, source, rs, 16, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return strToUnsigned(ctx, source, rs, 32, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return strToUnsigned(ctx, source, rs, 64, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return strToFloat(ctx, source, rs, 32, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return strToFloat(ctx, source, rs, 64, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return strToDecimal64(source, rs, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return strToDecimal128(source, rs, length, selectList)
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return strToBool(source, rs, length, selectList)
	case types.T_json:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToJson(source, rs, length, selectList)
	case types.T_uuid:
		rs := vector.MustFunctionResult[types.Uuid](result)
		return strToUuid(source, rs, length, selectList)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return strToDate(source, rs, length, selectList)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return strToDatetime(source, rs, length, selectList)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return strToTime(source, rs, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		zone := time.Local
		if proc != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		return strToTimestamp(source, rs, zone, length, selectList)
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToStr(ctx, source, rs, length, toType)
	case types.T_array_float32:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToArray[float32](ctx, source, rs, length, toType)
	case types.T_array_float64:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return strToArray[float64](ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from %s to %s", source.GetType(), toType))
}

func arrayTypeToOthers(proc *process.Process,
	source vector.FunctionParameterWrapper[types.Varlena],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	ctx := proc.Ctx
	rs := vector.MustFunctionResult[types.Varlena](result)
	fromType := source.GetType()

	switch fromType.Oid {
	case types.T_array_float32:
		switch toType.Oid {
		case types.T_array_float32:
			return arrayToArray[float32, float32](proc.Ctx, source, rs, length, toType)
		case types.T_array_float64:
			return arrayToArray[float32, float64](proc.Ctx, source, rs, length, toType)
		}
	case types.T_array_float64:
		switch toType.Oid {
		case types.T_array_float32:
			return arrayToArray[float64, float32](proc.Ctx, source, rs, length, toType)
		case types.T_array_float64:
			return arrayToArray[float64, float64](proc.Ctx, source, rs, length, toType)
		}
	}

	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from %s to %s", fromType, toType))
}

func uuidToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Uuid],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return uuidToStr(ctx, source, rs, length, toType)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from uuid to %s", toType))
}

func tsToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.TS],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	if toType.Oid == types.T_TS {
		rs := vector.MustFunctionResult[types.TS](result)
		return rs.DupFromParameter(source, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from ts to %s", toType))
}

func rowidToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Rowid],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	if toType.Oid == types.T_Rowid {
		rs := vector.MustFunctionResult[types.Rowid](result)
		return rs.DupFromParameter(source, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from rowid to %s", toType))
}

func blockidToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Blockid],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	if toType.Oid == types.T_Blockid {
		rs := vector.MustFunctionResult[types.Blockid](result)
		return rs.DupFromParameter(source, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from blockid to %s", toType))
}

func jsonToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Varlena],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return jsonToStr(ctx, source, rs, length, selectList)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from json to %s", toType))
}

func enumToOthers(ctx context.Context,
	source vector.FunctionParameterWrapper[types.Enum],
	toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_uint16, types.T_uint8, types.T_uint32, types.T_uint64, types.T_uint128:
		rs := vector.MustFunctionResult[uint16](result)
		return enumToUint16(source, rs, length, selectList)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return enumToStr(ctx, source, rs, length, selectList)
	case types.T_enum:
		rs := vector.MustFunctionResult[types.Enum](result)
		return rs.DupFromParameter(source, length)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from enum to %s", toType.String()))
}

func integerToFixFloat[T1, T2 constraints.Integer | constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T1], to *vector.FunctionResult[T2], length uint64) error {

	max_value := math.Pow10(int(to.GetType().Width-to.GetType().Scale)) - 1
	var i uint64
	var dftValue T2
	for i = 0; i < length; i++ {
		v, isnull := from.GetValue(i)
		if isnull {
			if err := to.Append(dftValue, true); err != nil {
				return err
			}
		} else {
			if float64(v) < -max_value || float64(v) > max_value {
				return moerr.NewOutOfRange(ctx, "float", "value '%v'", v)
			}
			if err := to.Append(T2(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToFixFloat[T1, T2 constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T1], to *vector.FunctionResult[T2], length int, selectList *FunctionSelectList) error {

	pow := math.Pow10(int(to.GetType().Scale))
	max_value := math.Pow10(int(to.GetType().Width - to.GetType().Scale))
	max_value -= 1.0 / pow
	var i uint64
	var dftValue T2
	for i = 0; i < uint64(length); i++ {
		v, isnull := from.GetValue(i)
		if isnull {
			if err := to.Append(dftValue, true); err != nil {
				return err
			}
		} else {
			v2 := float64(v)
			tmp := math.Round((v2-math.Floor(v2))*pow) / pow
			v2 = math.Floor(v2) + tmp
			if v2 < -max_value || v2 > max_value {
				return moerr.NewOutOfRange(ctx, "float", "value '%v'", v)
			}
			if err := to.Append(T2(v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatNumToFixFloat[T1 constraints.Float](
	ctx context.Context, from float64, to *vector.FunctionResult[T1], originStr string) (T1, error) {

	pow := math.Pow10(int(to.GetType().Scale))
	max_value := math.Pow10(int(to.GetType().Width - to.GetType().Scale))
	max_value -= 1.0 / pow

	tmp := math.Round((from-math.Floor(from))*pow) / pow
	v := math.Floor(from) + tmp
	if v < -max_value || v > max_value {
		if originStr == "" {
			return 0, moerr.NewOutOfRange(ctx, "float", "value '%v'", from)
		} else {
			return 0, moerr.NewOutOfRange(ctx, "float", "value '%s'", originStr)
		}
	}
	return T1(v), nil
}

// XXX do not use it to cast float to integer, please use floatToInteger
func numericToNumeric[T1, T2 constraints.Integer | constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T1], to *vector.FunctionResult[T2], length int, selectList *FunctionSelectList) error {
	var i uint64
	var dftValue T2
	times := uint64(length)

	if to.GetType().Scale >= 0 && to.GetType().Width > 0 {
		return integerToFixFloat(ctx, from, to, times)
	}

	if err := overflowForNumericToNumeric[T1, T2](ctx, from.UnSafeGetAllValue(),
		from.GetSourceVector().GetNulls()); err != nil {
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

func numericToBit[T constraints.Integer | constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[uint64], bitSize int,
	length int, selectList *FunctionSelectList) error {
	for i := 0; i < length; i++ {
		v, null := from.GetValue(uint64(i))
		if null {
			if err := to.Append(uint64(0), true); err != nil {
				return err
			}
		} else {
			var val uint64
			switch any(v).(type) {
			case float32, float64:
				val = uint64(math.Round(float64(v)))
			default:
				val = uint64(v)
			}

			if val > uint64(1<<bitSize-1) {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("int%d", bitSize), "value %d", val)
			}
			if err := to.Append(val, false); err != nil {
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
	length int, selectList *FunctionSelectList) error {
	var i uint64
	var dftValue T2
	times := uint64(length)
	if err := overflowForNumericToNumeric[T1, T2](ctx, from.UnSafeGetAllValue(), from.GetSourceVector().GetNulls()); err != nil {
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
	to *vector.FunctionResult[bool], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			var v1 []byte
			if v {
				v1 = []byte("1")
			} else {
				v1 = []byte("0")
			}
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte("0")
			if v {
				result = []byte("1")
			}
			if toType.Oid == types.T_binary {
				for len(result) < int(toType.Width) {
					result = append(result, 0)
				}
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func boolToInteger[T constraints.Integer](
	from vector.FunctionParameterWrapper[bool],
	to *vector.FunctionResult[T], length int, selectList *FunctionSelectList) error {
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

func bitToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[uint64],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {

	uint64ToBytes := func(v uint64) []byte {
		b := types.EncodeUint64(&v)
		// remove leading zero bytes
		l := len(b)
		for l > 1 && b[l-1] == byte(0) {
			l -= 1
		}
		return b[:l]
	}

	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i := 0; i < length; i++ {
			v, null := from.GetValue(uint64(i))
			b := uint64ToBytes(v)
			if err := explicitCastToBinary(toType, b, null, to); err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < length; i++ {
		v, null := from.GetValue(uint64(i))
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		b := uint64ToBytes(v)
		if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
			if len(b) > int(toType.Width) {
				return moerr.NewDataTruncatedNoCtx("Bit", "truncated for binary/varbinary")
			}
		}

		slices.Reverse(b)
		if toType.Oid == types.T_binary {
			for len(b) < int(toType.Width) {
				b = append(b, byte(0))
			}
		}
		if len(b) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
			return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
				"%v is larger than Dest length %v", v, toType.Width))
		}
		if err := to.AppendBytes(b, false); err != nil {
			return err
		}
	}
	return nil
}

func signedToDecimal64[T1 constraints.Signed](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
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
			result, _ := types.Decimal64(uint64(v)).Scale(totype.Scale)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func signedToDecimal128[T1 constraints.Signed](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result := types.Decimal128{B0_63: uint64(v), B64_127: 0}
			if v < 0 {
				result.B64_127 = ^result.B64_127
			}
			result, _ = result.Scale(totype.Scale)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToDecimal64[T1 constraints.Unsigned](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
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
			result := types.Decimal64(uint64(v))
			result, _ = result.Scale(totype.Scale)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToDecimal128[T1 constraints.Unsigned](
	from vector.FunctionParameterWrapper[T1],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result := types.Decimal128{B0_63: uint64(v), B64_127: 0}
			result, _ = result.Scale(totype.Scale)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToDecimal64[T constraints.Float](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
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
			result64, err := types.Decimal64FromFloat64(float64(v), toType.Width, toType.Scale)
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
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
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
			result128, err := types.Decimal128FromFloat64(float64(v), toType.Width, toType.Scale)
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
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(strconv.FormatInt(int64(v), 10))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}

	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(strconv.FormatInt(int64(v), 10))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Signed", " truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v, toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unsignedToStr[T constraints.Unsigned](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(strconv.FormatUint(uint64(v), 10))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(strconv.FormatUint(uint64(v), 10))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Unsigned", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v, toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatToStr[T constraints.Float](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	bitSize := int(unsafe.Sizeof(T(0)) * 8)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := floatToBytes(float64(v), bitSize)
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			// float to string, [-14,15] convert to exponent.
			result := floatToBytes(float64(v), bitSize)
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Float", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v, toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func integerToTimestamp[T constraints.Integer](
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Timestamp], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
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
			result, err := types.ParseInt64ToTime(vI64, toType.Scale)
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

func integerToEnum[T constraints.Integer](
	ctx context.Context,
	from vector.FunctionParameterWrapper[T],
	to *vector.FunctionResult[types.Enum], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Enum
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		vI64 := int64(v)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			if vI64 < 1 || vI64 > types.MaxEnumLen {
				return moerr.NewOutOfRange(ctx, "enum", "value %d", v)
			}
			result, err := types.ParseIntToEnum(vI64)
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
	to *vector.FunctionResult[T], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
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
			if err := to.Append(v.ToTime(totype.Scale), false); err != nil {
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
	to *vector.FunctionResult[int32], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[int64], length int, selectList *FunctionSelectList) error {
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

func datetimeToDecimal64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.ToDecimal64().Scale(to.GetType().Scale - 6)
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

func datetimeToDecimal128(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.ToDecimal128().Scale(to.GetType().Scale - 6)
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
	to *vector.FunctionResult[types.Datetime], length int, selectList *FunctionSelectList) error {
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
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			result := v.ToDatetime(zone)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDatetime(
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Datetime], length int, selectList *FunctionSelectList) error {
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
			if err := to.Append(v.ToDatetime(totype.Scale), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToDate(
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Date], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[int32], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[int64], length int, selectList *FunctionSelectList) error {
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
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			// XXX I'm not sure if it's a good way to convert it to datetime first.
			// but I just follow the old logic of old code.
			result := v.ToDatetime(zone)
			if err := to.Append(result.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToDecimal64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.UnixToDecimal64()
			if err != nil {
				return err
			}
			result, err = result.Scale(to.GetType().Scale - 6)
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

func timestampToDecimal128(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	for ; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			result, err := v.UnixToDecimal128()
			if err != nil {
				return err
			}
			result, err = result.Scale(to.GetType().Scale - 6)
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

func timeToInteger[T constraints.Integer](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[T], length int, selectList *FunctionSelectList) error {
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
			if err := overflowForNumericToNumeric[int64, T](ctx, []int64{r}, nil); err != nil {
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
	to *vector.FunctionResult[types.Date], length int, selectList *FunctionSelectList) error {
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
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Date],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.String())
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.String())
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Date", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.String(), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Datetime],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.String2(fromType.Scale))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.String2(fromType.Scale))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Datetime", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.String(), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timestampToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Timestamp],
	to *vector.FunctionResult[types.Varlena], length int,
	zone *time.Location, toType types.Type) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.String2(zone, fromType.Scale))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.String2(zone, fromType.Scale))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("TimeStamp", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.String(), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.String2(fromType.Scale))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.String2(fromType.Scale))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Time", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.String(), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeToDecimal64(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Time],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
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
			result, err := v.ToDecimal64(ctx, totype.Width, fromType.Scale)
			if err != nil {
				return err
			}
			result, err = result.Scale(totype.Scale - fromType.Scale)
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
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
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
			result, err := v.ToDecimal128(ctx, totype.Width, fromType.Scale)
			if err != nil {
				return err
			}
			result, err = result.Scale(totype.Scale - fromType.Scale)
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

func decimal64ToSigned[T constraints.Signed](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[T], bitSize int, length int, selectList *FunctionSelectList) error {
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
			x, _ := v.Scale(-fromTyp.Scale)
			xStr := x.Format(0)
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

func decimal128ToSigned[T constraints.Signed](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[T], bitSize int, length int, selectList *FunctionSelectList) error {
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
			x, _ := v.Scale(-fromTyp.Scale)
			xStr := x.Format(0)
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
	length int, selectList *FunctionSelectList) error {
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
			xStr := v.Format(fromType.Scale)
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
	length int, selectList *FunctionSelectList) error {
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
			xStr := v.Format(fromType.Scale)
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	fromtype := from.GetType()
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			result, err := types.ParseDecimal64ToTime(v, fromtype.Scale, totype.Scale)
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	fromtype := from.GetType()
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			result, err := types.ParseDecimal128ToTime(v, fromtype.Scale, totype.Scale)
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
	to *vector.FunctionResult[types.Timestamp], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			ts := types.Timestamp(int64(v))
			if err := to.Append(ts, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToTimestamp(
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Timestamp], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			ts := types.Timestamp(int64(v.B0_63))
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
			xStr := v.Format(fromType.Scale)
			result, err := strconv.ParseFloat(xStr, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx, "float32", "value '%v'", xStr)
			}
			if bitSize == 32 {
				result, _ = strconv.ParseFloat(xStr, 64)
			}
			if to.GetType().Scale < 0 || to.GetType().Width == 0 {
				if err = to.Append(T(result), false); err != nil {
					return err
				}
			} else {
				v2, err := floatNumToFixFloat(ctx, result, to, xStr)
				if err != nil {
					return err
				}
				if err = to.Append(T(v2), false); err != nil {
					return err
				}
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
			xStr := v.Format(fromType.Scale)
			result, err := strconv.ParseFloat(xStr, bitSize)
			if err != nil {
				return moerr.NewOutOfRange(ctx, "float32", "value '%v'", xStr)
			}
			if bitSize == 32 {
				result, _ = strconv.ParseFloat(xStr, 64)
			}
			if to.GetType().Scale < 0 || to.GetType().Width == 0 {
				if err = to.Append(T(result), false); err != nil {
					return err
				}
			} else {
				v2, err := floatNumToFixFloat(ctx, result, to, xStr)
				if err != nil {
					return err
				}
				if err = to.Append(T(v2), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decimal64ToDecimal64(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	fromtype := from.GetType()
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			if totype.Width < fromtype.Width {
				dec := v.Format(fromtype.Scale)
				result, err := types.ParseDecimal64(dec, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			} else {
				result, err := v.Scale(totype.Scale - fromtype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decimal64ToDecimal128Array(
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	fromtype := from.GetType()
	totype := to.GetType()

	if !from.WithAnyNullValue() {
		v := vector.MustFixedCol[types.Decimal64](from.GetSourceVector())
		if totype.Width < fromtype.Width {
			for i = 0; i < l; i++ {
				fromdec := types.Decimal128{B0_63: uint64(v[i]), B64_127: 0}
				if v[i].Sign() {
					fromdec.B64_127 = ^fromdec.B64_127
				}
				dec := fromdec.Format(fromtype.Scale)
				result, err := types.ParseDecimal128(dec, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			}
		} else {
			if totype.Scale == fromtype.Scale {
				for i = 0; i < l; i++ {
					fromdec := types.Decimal128{B0_63: uint64(v[i]), B64_127: 0}
					if v[i].Sign() {
						fromdec.B64_127 = ^fromdec.B64_127
					}
					to.AppendMustValue(fromdec)
				}
			} else {
				for i = 0; i < l; i++ {
					fromdec := types.Decimal128{B0_63: uint64(v[i]), B64_127: 0}
					if v[i].Sign() {
						fromdec.B64_127 = ^fromdec.B64_127
					}
					result, err := fromdec.Scale(totype.Scale - fromtype.Scale)
					if err != nil {
						return err
					}
					if err = to.Append(result, false); err != nil {
						return err
					}
				}
			}
		}
	} else {
		// with any null value
		var dft types.Decimal128
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			if null {
				if err := to.Append(dft, true); err != nil {
					return err
				}
			} else {
				fromdec := types.Decimal128{B0_63: uint64(v), B64_127: 0}
				if v.Sign() {
					fromdec.B64_127 = ^fromdec.B64_127
				}
				if totype.Width < fromtype.Width {
					dec := fromdec.Format(fromtype.Scale)
					result, err := types.ParseDecimal128(dec, totype.Width, totype.Scale)
					if err != nil {
						return err
					}
					if err = to.Append(result, false); err != nil {
						return err
					}
				} else {
					if totype.Scale == fromtype.Scale {
						to.AppendMustValue(fromdec)
					} else {
						result, err := fromdec.Scale(totype.Scale - fromtype.Scale)
						if err != nil {
							return err
						}
						if err = to.Append(result, false); err != nil {
							return err
						}
					}
				}
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
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal64
	fromtype := from.GetType()
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			dec := v.Format(fromtype.Scale)
			result, err := types.ParseDecimal64(dec, totype.Width, totype.Scale)
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

func decimal128ToDecimal128(
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList) error {
	var i uint64
	l := uint64(length)
	var dft types.Decimal128
	fromtype := from.GetType()
	totype := to.GetType()
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(dft, true); err != nil {
				return err
			}
		} else {
			if totype.Width < fromtype.Width {
				dec := v.Format(fromtype.Scale)
				result, err := types.ParseDecimal128(dec, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			} else {
				result, err := v.Scale(totype.Scale - fromtype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decimal64ToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.Format(fromType.Scale))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.Format(fromType.Scale))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Decimal64", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.Format(fromType.Scale), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	l := uint64(length)
	fromType := from.GetType()
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.Format(fromType.Scale))
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.Format(fromType.Scale))
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Decimal128", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.Format(fromType.Scale), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64ToBit(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal64],
	to *vector.FunctionResult[uint64], bitSize int, length int, selectList *FunctionSelectList) error {
	for i := 0; i < length; i++ {
		v, null := from.GetValue(uint64(i))
		if null {
			if err := to.Append(uint64(0), true); err != nil {
				return err
			}
		} else {
			var result uint64
			var err error
			xStr := v.Format(from.GetType().Scale)
			xStr = strings.Split(xStr, ".")[0]
			if result, err = strconv.ParseUint(xStr, 10, bitSize); err != nil {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", bitSize), "value '%v'", xStr)
			}
			if err = to.Append(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128ToBit(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Decimal128],
	to *vector.FunctionResult[uint64], bitSize int, length int, selectList *FunctionSelectList) error {
	for i := 0; i < length; i++ {
		v, null := from.GetValue(uint64(i))
		if null {
			if err := to.Append(uint64(0), true); err != nil {
				return err
			}
		} else {
			var result uint64
			var err error
			xStr := v.Format(from.GetType().Scale)
			xStr = strings.Split(xStr, ".")[0]
			if result, err = strconv.ParseUint(xStr, 10, bitSize); err != nil {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", bitSize), "value '%v'", xStr)
			}
			if err = to.Append(result, false); err != nil {
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
	length int, selectList *FunctionSelectList) error {
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
				s := strings.TrimSpace(convertByteSliceToString(v))
				var r int64
				var err error
				if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
					r, err = strconv.ParseInt(s[2:], 16, bitSize)
				} else {
					r, err = strconv.ParseInt(s, 10, bitSize)
				}
				if err != nil {
					// XXX I'm not sure if we should return the int8 / int16 / int64 info. or
					// just return the int. the old code just return the int. too much bvt result needs to update.
					if strings.Contains(err.Error(), "value out of range") {
						return moerr.NewOutOfRange(ctx, fmt.Sprintf("int%d", bitSize), "value '%s'", s)
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
	length int, selectList *FunctionSelectList) error {
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
				s := strings.TrimSpace(convertByteSliceToString(v))
				res = &s
				if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
					val, tErr = strconv.ParseUint(s[2:], 16, bitSize)
				} else {
					val, tErr = strconv.ParseUint(s, 10, bitSize)
				}
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
	length int, selectList *FunctionSelectList) error {
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
				if to.GetType().Scale < 0 || to.GetType().Width == 0 {
					result = T(r1)
				} else {
					v2, err := floatNumToFixFloat(ctx, float64(r1), to, "")
					if err != nil {
						return err
					}
					result = T(v2)
				}
			} else {
				s := convertByteSliceToString(v)
				r2, tErr = strconv.ParseFloat(s, bitSize)
				if tErr != nil {
					return tErr
				}
				if bitSize == 32 {
					r2, _ = strconv.ParseFloat(s, 64)
				}
				if to.GetType().Scale < 0 || to.GetType().Width == 0 {
					result = T(r2)
				} else {
					v2, err := floatNumToFixFloat(ctx, r2, to, s)
					if err != nil {
						return err
					}
					result = T(v2)
				}
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
	to *vector.FunctionResult[types.Decimal64], length int, selectList *FunctionSelectList,
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
			if !isb {
				result, err := types.ParseDecimal64(s, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			} else {
				result, err := types.ParseDecimal64FromByte(s, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func strToDecimal128(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Decimal128], length int, selectList *FunctionSelectList,
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
			if !isb {
				result, err := types.ParseDecimal128(s, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			} else {
				result, err := types.ParseDecimal128FromByte(s, totype.Width, totype.Scale)
				if err != nil {
					return err
				}
				if err = to.Append(result, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func strToBool(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[bool], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Uuid], length int, selectList *FunctionSelectList) error {
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

func ConvertJsonBytes(inBytes []byte) ([]byte, error) {
	s := convertByteSliceToString(inBytes)
	json, err := types.ParseStringToByteJson(s)
	if err != nil {
		return nil, err
	}
	return types.EncodeJson(json)
}

func strToJson(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := ConvertJsonBytes(v)
			if err != nil {
				return err
			}
			if err = to.AppendBytes(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToDate(
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Date], length int, selectList *FunctionSelectList) error {
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
	to *vector.FunctionResult[types.Time], length int, selectList *FunctionSelectList) error {
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
			val, err := types.ParseTime(s, totype.Scale)
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
	to *vector.FunctionResult[types.Datetime], length int, selectList *FunctionSelectList) error {
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
			val, err := types.ParseDatetime(s, totype.Scale)
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
	zone *time.Location, length int, selectList *FunctionSelectList) error {
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
			val, err := types.ParseTimestamp(zone, s, totype.Scale)
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
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	totype := to.GetType()
	destLen := int(totype.Width)
	var i uint64
	var l = uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetStrValue(i)
			if err := explicitCastToBinary(toType, v, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	if totype.Oid != types.T_text && destLen != 0 {
		for i = 0; i < l; i++ {
			v, null := from.GetStrValue(i)
			if null {
				if err := to.AppendBytes(nil, true); err != nil {
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
			if toType.Oid == types.T_binary && len(v) < int(toType.Width) {
				add0 := int(toType.Width) - len(v)
				for ; add0 != 0; add0-- {
					v = append(v, 0)
				}
			}
			if err := to.AppendBytes(v, false); err != nil {
				return err
			}
		}
	} else {
		for i = 0; i < l; i++ {
			v, null := from.GetStrValue(i)
			if null {
				if err := to.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			if err := to.AppendBytes(v, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToBit(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[uint64], bitSize int, length int, selectList *FunctionSelectList) error {
	for i := 0; i < length; i++ {
		v, null := from.GetStrValue(uint64(i))
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if len(v) > 8 {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", bitSize), "value %s", string(v))
			}

			var val uint64
			for j := range v {
				val = (val << 8) | uint64(v[j])
			}
			if val > uint64(1<<bitSize-1) {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", bitSize), "value %s", string(v))
			}

			if err := to.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strToArray[T types.RealNumbers](
	_ context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int, _ types.Type) error {

	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {

			b, err := types.StringToArrayToBytes[T](convertByteSliceToString(v))
			if err != nil {
				return err
			}
			if err = to.AppendBytes(b, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func blobToArray[T types.RealNumbers](
	_ context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int, _ types.Type) error {

	toType := to.GetType()

	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {

		v, null := from.GetStrValue(i)
		if null || len(v) == 0 {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			arr := types.BytesToArray[T](v)
			if int(toType.Width) != len(arr) {
				return moerr.NewArrayDefMismatchNoCtx(int(toType.Width), len(arr))
			}

			if err := to.AppendBytes(v, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func arrayToArray[I types.RealNumbers, O types.RealNumbers](
	_ context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int, _ types.Type) error {

	var i uint64
	var l = uint64(length)
	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// NOTE: During ARRAY --> ARRAY conversion, if you do width check
		// `to.GetType().Width != from.GetType().Width`
		// cases b/b and b+sqrt(b) fails.

		if from.GetType().Oid == to.GetType().Oid {
			// Eg:- VECF32(3) --> VECF32(3)
			if err := to.AppendBytes(v, false); err != nil {
				return err
			}
		} else {
			// Eg:- VECF32(3) --> VECF64(3)
			_v := types.BytesToArray[I](v)
			cast, err := moarray.Cast[I, O](_v)
			if err != nil {
				return err
			}
			bytes := types.ArrayToBytes[O](cast)
			if err := to.AppendBytes(bytes, false); err != nil {
				return err
			}
		}

	}
	return nil
}

func uuidToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Uuid],
	to *vector.FunctionResult[types.Varlena], length int, toType types.Type) error {
	var i uint64
	var l = uint64(length)
	// Here cast using cast(data_type as binary[(n)]).
	if toType.Oid == types.T_binary && toType.Scale == -1 {
		for i = 0; i < l; i++ {
			v, null := from.GetValue(i)
			v1 := []byte(v.String())
			if err := explicitCastToBinary(toType, v1, null, to); err != nil {
				return err
			}
		}
		return nil
	}
	for i = 0; i < l; i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := []byte(v.String())
			if toType.Oid == types.T_binary || toType.Oid == types.T_varbinary {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Uuid", "truncated for binary/varbinary")
				}
			}
			if toType.Oid == types.T_binary && len(result) < int(toType.Width) {
				add0 := int(toType.Width) - len(result)
				for ; add0 != 0; add0-- {
					result = append(result, 0)
				}
			}
			if toType.Oid == types.T_char || toType.Oid == types.T_varchar {
				if int32(len(result)) > toType.Width {
					return moerr.NewDataTruncatedNoCtx("Uuid", "truncated for char/varchar")
				}
			}
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v.String(), toType.Width))
			}
			if err := to.AppendBytes(result, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func jsonToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	var i uint64
	toType := to.GetType()
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			bj := types.DecodeJson(v)
			val, err := bj.MarshalJSON()
			if err != nil {
				return err
			}
			if len(val) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", val, toType.Width))
			}
			if err = to.AppendBytes(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func enumToUint16(
	from vector.FunctionParameterWrapper[types.Enum],
	to *vector.FunctionResult[uint16], length int, selectList *FunctionSelectList) error {
	var i uint64
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := to.Append(uint16(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func enumToStr(
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Enum],
	to *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	var i uint64
	toType := to.GetType()
	for i = 0; i < uint64(length); i++ {
		v, null := from.GetValue(i)
		if null {
			if err := to.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			result := strconv.FormatUint(uint64(v), 10)
			if len(result) > int(toType.Width) && toType.Oid != types.T_text && toType.Oid != types.T_blob && toType.Oid != types.T_datalink {
				return formatCastError(ctx, from.GetSourceVector(), toType, fmt.Sprintf(
					"%v is larger than Dest length %v", v, toType.Width))
			}
			if err := to.AppendBytes([]byte(result), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func OverflowForNumericToNumeric[T1, T2 constraints.Integer | constraints.Float](ctx context.Context,
	xs []T1, nsp *nulls.Nulls) error {
	return overflowForNumericToNumeric[T1, T2](ctx, xs, nsp)
}

func overflowForNumericToNumeric[T1, T2 constraints.Integer | constraints.Float](ctx context.Context,
	xs []T1, nsp *nulls.Nulls) error {
	if len(xs) == 0 {
		return nil
	}

	var t2 T2
	var ri interface{} = &t2
	switch slice := (any(xs)).(type) {

	case []int8:
		switch ri.(type) {
		case *uint8, *uint16, *uint32, *uint64:
			for i, x := range xs {
				if !nsp.Contains(uint64(i)) && x < 0 {
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}

	case []int16:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt8 || x > math.MaxInt8) {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint8) {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16, *uint32, *uint64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x < 0 {
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}

	case []int32:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt8 || x > math.MaxInt8) {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt16 || x > math.MaxInt16) {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint8) {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint16) {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32, *uint64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x < 0 {
					return moerr.NewOutOfRange(ctx, "uint", "value '%v'", x)
				}
			}
		}

	case []int64:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt8 || x > math.MaxInt8) {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt16 || x > math.MaxInt16) {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < math.MinInt32 || x > math.MaxInt32) {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint8) {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint16) {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && (x < 0 || x > math.MaxUint32) {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x < 0 {
					// XXX for adapt to bvt, but i don't know why we hide the wrong value here.
					return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", x)
				}
			}
		}

	case []uint8:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		}

	case []uint16:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		}

	case []uint32:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		}

	case []uint64:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxInt64 {
					return moerr.NewOutOfRange(ctx, "int64", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		}

	case []float32:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxInt64 {
					return moerr.NewOutOfRange(ctx, "int64", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(float64(x)) > math.MaxUint64 {
					return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", x)
				}
			}
		}

	case []float64:
		switch ri.(type) {
		case *int8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxInt8 {
					return moerr.NewOutOfRange(ctx, "int8", "value '%v'", x)
				}
			}
		case *int16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxInt16 {
					return moerr.NewOutOfRange(ctx, "int16", "value '%v'", x)
				}
			}
		case *int32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxInt32 {
					return moerr.NewOutOfRange(ctx, "int32", "value '%v'", x)
				}
			}
		case *int64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) &&
					(math.Round(x) > math.MaxInt64 || math.Round(x) < math.MinInt64) {
					return moerr.NewOutOfRange(ctx, "int64", "value '%v'", x)
				}
			}
		case *uint8:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxUint8 {
					return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", x)
				}
			}
		case *uint16:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxUint16 {
					return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", x)
				}
			}
		case *uint32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxUint32 {
					return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", x)
				}
			}
		case *uint64:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && math.Round(x) > math.MaxUint64 {
					return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", x)
				}
			}
		case *float32:
			for i, x := range slice {
				if !nsp.Contains(uint64(i)) && x > math.MaxFloat32 {
					return moerr.NewOutOfRange(ctx, "float32", "value '%v'", x)
				}
			}
		}

	}
	return nil
}

func appendNulls[T types.FixedSizeT](result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	if r, ok := result.(*vector.FunctionResult[types.Varlena]); ok {
		var i uint64
		for i = 0; i < uint64(length); i++ {
			if err := r.AppendBytes(nil, true); err != nil {
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
	return util.UnsafeBytesToString(v)
	// return string(v)
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

func FormatCastErrorForInsertValue(ctx context.Context, originStr string, typ types.Type, extraInfo string) error {
	valueStr := strings.TrimRight(strings.TrimLeft(originStr, "["), "]")
	shortenValueStr := shortenValueString(valueStr)
	errStr := fmt.Sprintf("Can't cast '%s' to %v type.", shortenValueStr, typ)
	return moerr.NewInternalError(ctx, errStr+" "+extraInfo)
}

func formatCastError(ctx context.Context, vec *vector.Vector, typ types.Type, extraInfo string) error {
	var errStr string
	if vec.IsConst() {
		if vec.IsConstNull() {
			errStr = fmt.Sprintf("Can't cast 'NULL' as %v type.", typ)
		} else {
			valueStr := strings.TrimRight(strings.TrimLeft(fmt.Sprintf("%v", vec), "["), "]")
			shortenValueStr := shortenValueString(valueStr)
			errStr = fmt.Sprintf("Can't cast '%s' from %v type to %v type.", shortenValueStr, vec.GetType(), typ)
		}
	} else {
		errStr = fmt.Sprintf("Can't cast column from %v type to %v type because of one or more values in that column.", vec.GetType(), typ)
	}
	return moerr.NewInternalError(ctx, errStr+" "+extraInfo)
}

func explicitCastToBinary(toType types.Type, v []byte, null bool, to *vector.FunctionResult[types.Varlena]) error {
	if null {
		if err := to.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}
	// cast(data_type as binary)
	if toType.Width == -1 {
		if err := to.AppendBytes(v, false); err != nil {
			return err
		}
		return nil
	}
	// cast(data_type as binary(n))
	// truncating
	if int32(len(v)) > toType.Width {
		v = v[:toType.Width]
	}
	// right-padding.
	if len(v) < int(toType.Width) {
		add0 := int(toType.Width) - len(v)
		for ; add0 != 0; add0-- {
			v = append(v, 0)
		}
	}
	if err := to.AppendBytes(v, false); err != nil {
		return err
	}
	return nil
}

func floatToBytes(v float64, bitSize int) []byte {
	if v >= float64(1e15) || (v < float64(1e-13) && v > 0) || (v < 0 && v > -1e-13) || v <= -1e15 {
		return []byte(strconv.FormatFloat(float64(v), 'E', -1, bitSize))
	} else {
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, bitSize))
	}
}
