// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/unary"
)

func initBuiltIns() {
	var err error

	for fid, fs := range builtins {
		err = appendFunction(fid, fs)
		if err != nil {
			panic(err)
		}
	}
}

// builtins contains the builtin function indexed by function id.
var builtins = map[int]Functions{
	ABS: {
		Id: ABS,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        unary.AbsInt64,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        unary.AbsUInt64,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.AbsFloat64,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.AbsDecimal128,
			},
		},
	},
	ACOS: {
		Id: ACOS,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Acos,
			},
		},
	},
	BIT_LENGTH: {
		Id: BIT_LENGTH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char}, // todo? check if there is implicit upcast for char/varchar, it not, register another type or add upcast
				ReturnTyp: types.T_int64,
				Fn:        unary.BitLengthFunc,
			},
		},
	},
	CONCAT_WS: {
		Id: CONCAT_WS,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) > 1 {
				ret := make([]types.T, len(inputs))
				convert := false
				for i, t := range inputs {
					if t != types.T_char && t != types.T_varchar && t != types.T_any && t != types.T_blob {
						if castTable[t][types.T_varchar] {
							ret[i] = types.T_varchar
							convert = true
							continue
						}
						return wrongFunctionParameters, nil
					}
					ret[i] = t
				}
				if convert {
					return int32(0), ret
				}
				return int32(0), nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Concat_ws,
			},
		},
	},
	CONCAT: {
		Id: CONCAT,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) > 1 {
				ret := make([]types.T, len(inputs))
				convert := false
				for i, t := range inputs {
					if t != types.T_char && t != types.T_varchar && t != types.T_any && t != types.T_blob {
						if castTable[t][types.T_varchar] {
							ret[i] = types.T_varchar
							convert = true
							continue
						}
						return wrongFunctionParameters, nil
					}
					ret[i] = t
				}
				if convert {
					return int32(0), ret
				}
				return int32(0), nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Concat,
			},
		},
	},
	CURRENT_TIMESTAMP: {
		Id: CURRENT_TIMESTAMP,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) == 0 {
				return int32(0), nil
			}
			if len(inputs) == 1 && inputs[0] == types.T_int64 {
				return int32(0), nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_timestamp,
				Fn:        multi.CurrentTimestamp,
			},
		},
	},
	UUID: {
		// uuid function contains a hidden placeholder parameter
		Id: UUID,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) == 0 {
				return int32(0), nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_STRICT,
				Layout:        STANDARD_FUNCTION,
				Volatile:      true,
				AppendHideArg: true,
				ReturnTyp:     types.T_varchar,
				Fn:            multi.UUID,
			},
		},
	},
	DATE: {
		Id: DATE,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_date,
				Fn:        unary.DateToDate,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_date,
				Fn:        unary.DatetimeToDate,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_date,
				Fn:        unary.DateStringToDate,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_date,
				Fn:        unary.DateStringToDate,
			},
		},
	},
	HOUR: {
		Id: HOUR,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToHour,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToHour,
			},
		},
	},
	MINUTE: {
		Id: MINUTE,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToMinute,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToMinute,
			},
		},
	},
	SECOND: {
		Id: SECOND,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToSecond,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToSecond,
			},
		},
	},
	DAY: {
		Id: DAY,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToDay,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToDay,
			},
		},
	},
	DAYOFYEAR: {
		Id: DAYOFYEAR,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint16,
				Fn:        unary.DayOfYear,
			},
		},
	},
	EMPTY: {
		Id: EMPTY,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_uint8,
				Fn:        unary.Empty,
			},
		},
	},
	EXP: {
		Id: EXP,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Exp,
			},
		},
	},
	EXTRACT: {
		Id: EXTRACT,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_datetime},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ExtractFromDatetime,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_date},
				ReturnTyp: types.T_uint32,
				Fn:        binary.ExtractFromDate,
			},
		},
	},
	LENGTH: {
		Id: LENGTH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_blob},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
		},
	},
	LENGTH_UTF8: {
		Id: LENGTH_UTF8,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_uint64, Fn: unary.LengthUTF8,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_uint64,
				Fn:        unary.LengthUTF8,
			},
		},
	},
	LN: {
		Id: LN,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Ln,
			},
		},
	},
	LOG: {
		Id: LOG,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Log,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Log,
			},
		},
	},
	LTRIM: {
		Id: LTRIM,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Ltrim,
			},
		},
	},
	MONTH: {
		Id: MONTH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToMonth,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToMonth,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateStringToMonth,
			},
		},
	},
	OCT: {
		Id: OCT,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint8],
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint16],
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint32],
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint64],
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int8],
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int16],
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int32],
			},
			{
				Index:     7,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int64],
			},
			{
				Index:     8,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.OctFloat[float32],
			},
			{
				Index:     9,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.OctFloat[float64],
			},
		},
	},
	REVERSE: {
		Id: REVERSE,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Reverse,
			},
		},
	},
	RTRIM: {
		Id: RTRIM,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Rtrim,
			},
		},
	},
	SIN: {
		Id: SIN,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Sin,
			},
		},
	},
	SPACE: {
		Id: SPACE,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.SpaceNumber[uint64],
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.SpaceNumber[int64],
			},
		},
	},
	WEEK: {
		Id: WEEK,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToWeek,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToWeek,
			},
		},
	},
	WEEKDAY: {
		Id: WEEKDAY,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateToWeekday,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        unary.DatetimeToWeekday,
			},
		},
	},
	YEAR: {
		Id: YEAR,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateToYear,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        unary.DatetimeToYear,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateStringToYear,
			},
		},
	},
	// binary functions
	ENDSWITH: {
		Id: ENDSWITH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        binary.Endswith,
			},
		},
	},
	FINDINSET: {
		Id: FINDINSET,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint64,
				Fn:        binary.FindInSet,
			},
		},
	},
	POW: {
		Id: POW,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        binary.Power,
			},
		},
	},
	STARTSWITH: {
		Id: STARTSWITH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        binary.Startswith,
			},
		},
	},
	DATE_FORMAT: {
		Id: DATE_FORMAT,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.DateFormat,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        binary.DateFormat,
			},
		},
	},
	// variadic functions
	CEIL: {
		Id: CEIL,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.CeilUint64,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.CeilUint64,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.CeilInt64,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.CeilInt64,
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.CeilFloat64,
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.CeilFloat64,
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        multi.CeilDecimal128,
			},
		},
	},
	FLOOR: {
		Id: FLOOR,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.FloorUInt64,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.FloorUInt64,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.FloorInt64,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.FloorInt64,
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.FloorFloat64,
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.FloorFloat64,
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        multi.FloorDecimal128,
			},
		},
	},
	LPAD: {
		Id: LPAD,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Lpad,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar, Fn: multi.Lpad,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar, Fn: multi.Lpad,
			},
		},
	},
	PI: {
		Id: PI,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_float64,
				Fn:        multi.Pi,
			},
		},
	},
	ROUND: {
		Id: ROUND,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.RoundUint64,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.RoundUint64,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RoundInt64,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RoundInt64,
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.RoundFloat64,
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.RoundFloat64,
			},
		},
	},
	RPAD: {
		Id: RPAD,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     7,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     8,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     9,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     10,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     11,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     12,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     13,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     14,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     15,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     16,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     17,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     18,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     19,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     20,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     21,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     22,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     23,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     24,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     25,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     26,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     27,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     28,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     29,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     30,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     31,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     32,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     33,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     34,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     35,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     36,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     37,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     38,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     39,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     40,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     41,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     42,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     43,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     44,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     45,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_char, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     46,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_char, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     47,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_char, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     48,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_char, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     49,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_char, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
		},
	},
	SUBSTRING: {
		Id: SUBSTRING,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     7,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     8,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     9,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     10,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     11,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     12,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     13,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     14,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     15,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     16,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     17,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     18,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_blob, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     19,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_blob, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     20,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_blob, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     21,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_blob, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
		},
	},
	FROM_UNIXTIME: {
		Id: FROM_UNIXTIME,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeInt64,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeUint64,
			},
			{
				Index:     2,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeFloat64,
			},
			{
				Index:     3,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeInt64Format,
			},
			{
				Index:     4,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeUint64Format,
			},
			{
				Index:     5,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeFloat64Format,
			},
		},
	},
	UNIX_TIMESTAMP: {
		Id: UNIX_TIMESTAMP,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestamp,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestamp,
			},
			{
				Index:     2,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestampVarchar,
			},
		},
	},
	UTC_TIMESTAMP: {
		Id: UTC_TIMESTAMP,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_datetime,
				Fn:        multi.UTCTimestamp,
			},
		},
	},
	DATE_ADD: {
		Id: DATE_ADD,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date, types.T_int64, types.T_int64},
				ReturnTyp: types.T_date,
				Fn:        multi.DateAdd,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DatetimeAdd,
			},
			{
				Index:     2,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringAdd,
			},
			{
				Index:     3,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringAdd,
			},
			{
				Index:     4,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				ReturnTyp: types.T_timestamp,
				Fn:        multi.TimeStampAdd,
			},
		},
	},
	DATE_SUB: {
		Id: DATE_SUB,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date, types.T_int64, types.T_int64},
				ReturnTyp: types.T_date,
				Fn:        multi.DateSub,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DatetimeSub,
			},
			{
				Index:     2,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringSub,
			},
			{
				Index:     3,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringSub,
			},
			{
				Index:     4,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				ReturnTyp: types.T_timestamp,
				Fn:        multi.TimeStampSub,
			},
		},
	},
	TAN: {
		Id: TAN,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Tan,
			},
		},
	},
	SINH: {
		Id: SINH,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Sinh,
			},
		},
	},
	TO_DATE: {
		Id: TO_DATE,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 2 {
				if inputs[0] == types.T_char || inputs[0] == types.T_varchar {
					if inputs[1] == types.T_char || inputs[1] == types.T_varchar {
						return int32(0), nil
					}
				}
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ToDate,
			},
		},
	},
	ATAN: {
		Id: ATAN,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Atan,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Atan,
			},
		},
	},
	COS: {
		Id: COS,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Cos,
			},
		},
	},
	COT: {
		Id: COT,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Cot,
			},
		},
	},
	TIMESTAMP: {
		Id: TIMESTAMP,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateToTimestamp,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DatetimeToTimestamp,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.TimestampToTimestamp,
			},
			{
				Index:     3,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateStringToTimestamp,
			},
			{
				Index:     4,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateStringToTimestamp,
			},
		},
	},
	DATABASE: {
		Id: DATABASE,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Database,
			},
		},
	},
	USER: {
		Id: USER,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.User,
			},
		},
	},
	CONNECTION_ID: {
		Id: CONNECTION_ID,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.ConnectionID,
			},
		},
	},
	CHARSET: {
		Id: CHARSET,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Charset,
			},
		},
	},
	CURRENT_ROLE: {
		Id: CURRENT_ROLE,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.CurrentRole,
			},
		},
	},
	FOUND_ROWS: {
		Id: FOUND_ROWS,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.FoundRows,
			},
		},
	},
	ICULIBVERSION: {
		Id: ICULIBVERSION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.ICULIBVersion,
			},
		},
	},
	LAST_INSERT_ID: {
		Id: LAST_INSERT_ID,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.LastInsertID,
			},
		},
	},
	ROLES_GRAPHML: {
		Id: ROLES_GRAPHML,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.RolesGraphml,
			},
		},
	},
	ROW_COUNT: {
		Id: ROW_COUNT,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.RowCount,
			},
		},
	},
	VERSION: {
		Id: VERSION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Version,
			},
		},
	},
	COLLATION: {
		Id: COLLATION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Collation,
			},
		},
	},
	JSON_EXTRACT: {
		Id: JSON_EXTRACT,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.JsonExtractByString,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_json, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.JsonExtractByJson,
			},
		},
	},

	ENABLE_FAULT_INJECTION: {
		Id: ENABLE_FAULT_INJECTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_INTERNAL,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_bool,
				Fn:        multi.EnableFaultInjection,
			},
		},
	},
	DISABLE_FAULT_INJECTION: {
		Id: DISABLE_FAULT_INJECTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_INTERNAL,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_bool,
				Fn:        multi.DisableFaultInjection,
			},
		},
	},
	ADD_FAULT_POINT: {
		Id: ADD_FAULT_POINT,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_INTERNAL,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_bool,
				Fn:        multi.AddFaultPoint,
			},
		},
	},
	REMOVE_FAULT_POINT: {
		Id: REMOVE_FAULT_POINT,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_INTERNAL,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_bool,
				Fn:        multi.RemoveFaultPoint,
			},
		},
	},
	TRIGGER_FAULT_POINT: {
		Id: REMOVE_FAULT_POINT,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_INTERNAL,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        multi.TriggerFaultPoint,
			},
		},
	},
	LOAD_FILE: {
		Id: LOAD_FILE,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_blob,
				Fn:        unary.LoadFile,
			},
			{
				Index:     1,
				Volatile:  true,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_blob,
				Fn:        unary.LoadFile,
			},
			// {
			// 	Index:     2,
			// 	Flag:      plan.Function_STRICT,
			// 	Layout:    STANDARD_FUNCTION,
			// 	Args:      []types.T{types.T_text},
			// 	ReturnTyp: types.T_blob,
			// 	Fn:        unary.LoadFile,
			// },
		},
	},
	HEX: {
		Id: HEX,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexString,
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexString,
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexInt64,
			},
		},
	},
	SERIAL: {
		Id: SERIAL,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			return int32(0), nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Serial,
			},
		},
	},
	HASH: {
		Id: HASH,
		TypeCheckFn: func(_ []Function, typs []types.T) (int32, []types.T) {
			return 0, typs
		},
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        multi.Hash,
			},
		},
	},
	BIN: {
		Id: BIN,
		Overloads: []Function{
			{
				Index:     0,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint8],
			},
			{
				Index:     1,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint16],
			},
			{
				Index:     2,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint32],
			},
			{
				Index:     3,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint64],
			},
			{
				Index:     4,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int8],
			},
			{
				Index:     5,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int16],
			},
			{
				Index:     6,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int32],
			},
			{
				Index:     7,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int64],
			},
			{
				Index:     8,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.BinFloat[float32],
			},
			{
				Index:     9,
				Flag:      plan.Function_STRICT,
				Layout:    STANDARD_FUNCTION,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.BinFloat[float64],
			},
		},
	},
}
