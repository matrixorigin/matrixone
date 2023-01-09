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
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/ctl"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/unary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
)

func initBuiltIns() {
	var err error

	for fid, fs := range builtins {
		err = appendFunction(context.Background(), fid, fs)
		if err != nil {
			panic(err)
		}
	}
}

// builtins contains the builtin function indexed by function id.
var builtins = map[int]Functions{
	ABS: {
		Id:     ABS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        unary.AbsInt64,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        unary.AbsUInt64,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.AbsFloat64,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.AbsDecimal128,
			},
		},
	},
	ACOS: {
		Id:     ACOS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Acos,
			},
		},
	},
	BIT_LENGTH: {
		Id:     BIT_LENGTH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_char}, // todo? check if there is implicit upcast for char/varchar, it not, register another type or add upcast
				ReturnTyp: types.T_int64,
				Fn:        unary.BitLengthFunc,
			},
		},
	},
	CONCAT_WS: {
		Id:     CONCAT_WS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) > 1 {
				ret := make([]types.T, len(inputs))
				convert := false
				for i, t := range inputs {
					if t != types.T_char && t != types.T_varchar && t != types.T_any && t != types.T_blob && t != types.T_text {
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
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Concat_ws,
			},
		},
	},
	CONCAT: {
		Id:     CONCAT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) > 1 {
				ret := make([]types.T, len(inputs))
				convert := false
				for i, t := range inputs {
					if t != types.T_char && t != types.T_varchar && t != types.T_any && t != types.T_blob && t != types.T_text {
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
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Concat,
			},
		},
	},
	CURRENT_TIMESTAMP: {
		Id:     CURRENT_TIMESTAMP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
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
				Index:           0,
				Volatile:        false,
				RealTimeRelated: true,
				Args:            []types.T{},
				ReturnTyp:       types.T_timestamp,
				Fn:              multi.CurrentTimestamp,
			},
		},
	},
	UUID: {
		// uuid function contains a hidden placeholder parameter
		Id:     UUID,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) == 0 {
				return int32(0), nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Volatile:      true,
				AppendHideArg: true,
				ReturnTyp:     types.T_varchar,
				Fn:            multi.UUID,
			},
		},
	},
	DATE: {
		Id:     DATE,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_date,
				Fn:        unary.DateToDate,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_time},
				ReturnTyp: types.T_date,
				Fn:        unary.TimeToDate,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_date,
				Fn:        unary.DatetimeToDate,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_date,
				Fn:        unary.DateStringToDate,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_date,
				Fn:        unary.DateStringToDate,
			},
		},
	},
	TIME: {
		Id:     TIME,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_time},
				ReturnTyp: types.T_time,
				Fn:        unary.TimeToTime,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_time,
				Fn:        unary.DateToTime,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_time,
				Fn:        unary.DatetimeToTime,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_time,
				Fn:        unary.Int64ToTime,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_time,
				Fn:        unary.Decimal128ToTime,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_time,
				Fn:        unary.DateStringToTime,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_time,
				Fn:        unary.DateStringToTime,
			},
			{
				Index:     7,
				Args:      []types.T{types.T_text},
				ReturnTyp: types.T_time,
				Fn:        unary.DateStringToTime,
			},
			{
				Index:     8,
				Args:      []types.T{types.T_blob},
				ReturnTyp: types.T_time,
				Fn:        unary.DateStringToTime,
			},
		},
	},
	HOUR: {
		Id:     HOUR,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToHour,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToHour,
			},
		},
	},
	MINUTE: {
		Id:     MINUTE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToMinute,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToMinute,
			},
		},
	},
	SECOND: {
		Id:     SECOND,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_uint8,
				Fn:        unary.TimestampToSecond,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToSecond,
			},
		},
	},
	DAY: {
		Id:     DAY,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToDay,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToDay,
			},
		},
	},
	DAYOFYEAR: {
		Id:     DAYOFYEAR,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint16,
				Fn:        unary.DayOfYear,
			},
		},
	},
	EMPTY: {
		Id:     EMPTY,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_uint8,
				Fn:        unary.Empty,
			},
		},
	},
	EXP: {
		Id:     EXP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Exp,
			},
		},
	},
	EXTRACT: {
		Id:     EXTRACT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_datetime},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ExtractFromDatetime,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_date},
				ReturnTyp: types.T_uint32,
				Fn:        binary.ExtractFromDate,
			},
		},
	},
	LENGTH: {
		Id:     LENGTH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_blob},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_text},
				ReturnTyp: types.T_int64,
				Fn:        unary.Length,
			},
		},
	},
	LENGTH_UTF8: {
		Id:     LENGTH_UTF8,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_uint64, Fn: unary.LengthUTF8,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_uint64,
				Fn:        unary.LengthUTF8,
			},
		},
	},
	LN: {
		Id:     LN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Ln,
			},
		},
	},
	LOG: {
		Id:     LOG,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Log,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Log,
			},
		},
	},
	LTRIM: {
		Id:     LTRIM,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Ltrim,
			},
		},
	},
	MONTH: {
		Id:     MONTH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToMonth,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToMonth,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateStringToMonth,
			},
		},
	},
	OCT: {
		Id:     OCT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.Oct[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.OctFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_decimal128,
				Fn:        unary.OctFloat[float64],
			},
		},
	},
	REVERSE: {
		Id:     REVERSE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Reverse,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Reverse,
			},
		},
	},
	RTRIM: {
		Id:     RTRIM,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Rtrim,
			},
		},
	},
	LEFT: {
		Id:     LEFT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        binary.Left,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_char, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        binary.Left,
			},
		},
	},
	SIN: {
		Id:     SIN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Sin,
			},
		},
	},
	SPACE: {
		Id:     SPACE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.SpaceNumber[uint64],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.SpaceNumber[int64],
			},
		},
	},
	WEEK: {
		Id:     WEEK,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DateToWeek,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_uint8,
				Fn:        unary.DatetimeToWeek,
			},
		},
	},
	WEEKDAY: {
		Id:     WEEKDAY,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateToWeekday,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        unary.DatetimeToWeekday,
			},
		},
	},
	YEAR: {
		Id:     YEAR,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateToYear,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        unary.DatetimeToYear,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        unary.DateStringToYear,
			},
		},
	},
	// binary functions
	ENDSWITH: {
		Id:     ENDSWITH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        binary.Endswith,
			},
		},
	},
	FINDINSET: {
		Id:     FINDINSET,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint64,
				Fn:        binary.FindInSet,
			},
		},
	},
	POW: {
		Id:     POW,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        binary.Power,
			},
		},
	},
	STARTSWITH: {
		Id:     STARTSWITH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        binary.Startswith,
			},
		},
	},
	DATE_FORMAT: {
		Id:     DATE_FORMAT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_datetime, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.DateFormat,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_datetime, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        binary.DateFormat,
			},
		},
	},
	// variadic functions
	CEIL: {
		Id:     CEIL,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.CeilUint64,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.CeilUint64,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.CeilInt64,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.CeilInt64,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.CeilFloat64,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.CeilFloat64,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        multi.CeilDecimal128,
			},
		},
	},
	FLOOR: {
		Id:     FLOOR,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.FloorUInt64,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.FloorUInt64,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.FloorInt64,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.FloorInt64,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.FloorFloat64,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.FloorFloat64,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        multi.FloorDecimal128,
			},
		},
	},
	LPAD: {
		Id:     LPAD,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Lpad,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar, Fn: multi.Lpad,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar, Fn: multi.Lpad,
			},
		},
	},
	PI: {
		Id:     PI,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{},
				ReturnTyp: types.T_float64,
				Fn:        multi.Pi,
			},
		},
	},
	ROUND: {
		Id:     ROUND,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONIC,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.RoundUint64,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        multi.RoundUint64,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RoundInt64,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RoundInt64,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        multi.RoundFloat64,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_float64,
				Fn:        multi.RoundFloat64,
			},
		},
	},
	RPAD: {
		Id:     RPAD,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     7,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     8,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     9,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     10,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     12,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     13,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     14,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     15,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     16,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     17,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     18,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     19,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     20,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     21,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     22,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     23,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     24,
				Args:      []types.T{types.T_varchar, types.T_char, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Rpad,
			},
			{
				Index:     25,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     26,
				Args:      []types.T{types.T_char, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     27,
				Args:      []types.T{types.T_char, types.T_int64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     28,
				Args:      []types.T{types.T_char, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     29,
				Args:      []types.T{types.T_char, types.T_int64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     30,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     31,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     32,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     33,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     34,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     35,
				Args:      []types.T{types.T_char, types.T_float64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     36,
				Args:      []types.T{types.T_char, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     37,
				Args:      []types.T{types.T_char, types.T_float64, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     38,
				Args:      []types.T{types.T_char, types.T_float64, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     39,
				Args:      []types.T{types.T_char, types.T_float64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     40,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     41,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     42,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     43,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     44,
				Args:      []types.T{types.T_char, types.T_varchar, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     45,
				Args:      []types.T{types.T_char, types.T_char, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     46,
				Args:      []types.T{types.T_char, types.T_char, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     47,
				Args:      []types.T{types.T_char, types.T_char, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     48,
				Args:      []types.T{types.T_char, types.T_char, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
			{
				Index:     49,
				Args:      []types.T{types.T_char, types.T_char, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        multi.Rpad,
			},
		},
	},
	SUBSTRING: {
		Id:     SUBSTRING,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_char, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_char, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_char, types.T_float64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     7,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     8,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     9,
				Args:      []types.T{types.T_varchar, types.T_float64, types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     10,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     12,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     13,
				Args:      []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Substring,
			},
			{
				Index:     14,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     15,
				Args:      []types.T{types.T_char, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     16,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     17,
				Args:      []types.T{types.T_char, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     18,
				Args:      []types.T{types.T_blob, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     19,
				Args:      []types.T{types.T_blob, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     20,
				Args:      []types.T{types.T_blob, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     21,
				Args:      []types.T{types.T_blob, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},

			{
				Index:     22,
				Args:      []types.T{types.T_text, types.T_int64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     23,
				Args:      []types.T{types.T_text, types.T_int64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     24,
				Args:      []types.T{types.T_text, types.T_uint64, types.T_int64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
			{
				Index:     25,
				Args:      []types.T{types.T_text, types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_char,
				Fn:        multi.Substring,
			},
		},
	},
	FROM_UNIXTIME: {
		Id:     FROM_UNIXTIME,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeInt64,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeUint64,
			},
			{
				Index:     2,
				Volatile:  true,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.FromUnixTimeFloat64,
			},
			{
				Index:     3,
				Volatile:  true,
				Args:      []types.T{types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeInt64Format,
			},
			{
				Index:     4,
				Volatile:  true,
				Args:      []types.T{types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeUint64Format,
			},
			{
				Index:     5,
				Volatile:  true,
				Args:      []types.T{types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.FromUnixTimeFloat64Format,
			},
		},
	},
	UNIX_TIMESTAMP: {
		Id:     UNIX_TIMESTAMP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestamp,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestamp,
			},
			{
				Index:     2,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.UnixTimestampVarcharToInt64,
			},
			{
				Index:     3,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        multi.UnixTimestampVarcharToDecimal128,
			},
		},
	},
	UTC_TIMESTAMP: {
		Id:     UTC_TIMESTAMP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{},
				ReturnTyp: types.T_datetime,
				Fn:        multi.UTCTimestamp,
			},
		},
	},
	DATE_ADD: {
		Id:     DATE_ADD,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_date, types.T_int64, types.T_int64},
				ReturnTyp: types.T_date,
				Fn:        multi.DateAdd,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_datetime, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DatetimeAdd,
			},
			{
				Index:     2,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringAdd,
			},
			{
				Index:     3,
				Volatile:  true,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringAdd,
			},
			{
				Index:     4,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				ReturnTyp: types.T_timestamp,
				Fn:        multi.TimeStampAdd,
			},
			{
				Index:     5,
				Volatile:  true,
				Args:      []types.T{types.T_time, types.T_int64, types.T_int64},
				ReturnTyp: types.T_time,
				Fn:        multi.TimeAdd,
			},
		},
	},
	DATE_SUB: {
		Id:     DATE_SUB,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_date, types.T_int64, types.T_int64},
				ReturnTyp: types.T_date,
				Fn:        multi.DateSub,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_datetime, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DatetimeSub,
			},
			{
				Index:     2,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringSub,
			},
			{
				Index:     3,
				Volatile:  true,
				Args:      []types.T{types.T_char, types.T_int64, types.T_int64},
				ReturnTyp: types.T_datetime,
				Fn:        multi.DateStringSub,
			},
			{
				Index:     4,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				ReturnTyp: types.T_timestamp,
				Fn:        multi.TimeStampSub,
			},
		},
	},
	TAN: {
		Id:     TAN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Tan,
			},
		},
	},
	SINH: {
		Id:     SINH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Sinh,
			},
		},
	},
	TO_DATE: {
		Id:     TO_DATE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
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
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ToDate,
			},
		},
	},
	STR_TO_DATE: {
		Id:     STR_TO_DATE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        binary.StrToDateTime,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        binary.StrToDate,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        binary.StrToTime,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_char, types.T_char, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        binary.StrToDateTime,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_char, types.T_char, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        binary.StrToDate,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_char, types.T_char, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        binary.StrToTime,
			},
		},
	},
	ATAN: {
		Id:     ATAN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Atan,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Atan,
			},
		},
	},
	COS: {
		Id:     COS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Cos,
			},
		},
	},
	COT: {
		Id:     COT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        unary.Cot,
			},
		},
	},
	TIMESTAMP: {
		Id:     TIMESTAMP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_date},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateToTimestamp,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_datetime},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DatetimeToTimestamp,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.TimestampToTimestamp,
			},
			{
				Index:     3,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateStringToTimestamp,
			},
			{
				Index:     4,
				Volatile:  true,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_timestamp,
				Fn:        unary.DateStringToTimestamp,
			},
		},
	},
	DATABASE: {
		Id:     DATABASE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Database,
			},
		},
	},
	USER: {
		Id:     USER,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.User,
			},
		},
	},
	CONNECTION_ID: {
		Id:     CONNECTION_ID,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.ConnectionID,
			},
		},
	},
	CHARSET: {
		Id:     CHARSET,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Charset,
			},
		},
	},
	CURRENT_ROLE: {
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Id:     CURRENT_ROLE,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.CurrentRole,
			},
		},
	},
	FOUND_ROWS: {
		Id:     FOUND_ROWS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.FoundRows,
			},
		},
	},
	ICULIBVERSION: {
		Id:     ICULIBVERSION,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.ICULIBVersion,
			},
		},
	},
	LAST_INSERT_ID: {
		Id:     LAST_INSERT_ID,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.LastInsertID,
			},
		},
	},
	LAST_QUERY_ID: {
		Id:     LAST_QUERY_ID,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.LastQueryIDWithoutParam,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.LastQueryID,
			},
		},
	},
	ROLES_GRAPHML: {
		Id:     ROLES_GRAPHML,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.RolesGraphml,
			},
		},
	},
	ROW_COUNT: {
		Id:     ROW_COUNT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_uint64,
				Fn:        unary.RowCount,
			},
		},
	},
	VERSION: {
		Id:     VERSION,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Version,
			},
		},
	},
	COLLATION: {
		Id:     COLLATION,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Collation,
			},
		},
	},
	JSON_EXTRACT: {
		Id:     JSON_EXTRACT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  false,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_json,
				Fn:        binary.JsonExtract,
			},
			{
				Index:     1,
				Volatile:  false,
				Args:      []types.T{types.T_json, types.T_varchar},
				ReturnTyp: types.T_json,
				Fn:        binary.JsonExtract,
			},
		},
	},
	JSON_QUOTE: {
		Id:     JSON_QUOTE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  false,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_json,
				Fn:        unary.JsonQuote,
			},
		},
	},
	JSON_UNQUOTE: {
		Id:     JSON_UNQUOTE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_json},
				ReturnTyp: types.T_varchar,
				Fn:        unary.JsonUnquote,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.JsonUnquote,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.JsonUnquote,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_text},
				ReturnTyp: types.T_varchar,
				Fn:        unary.JsonUnquote,
			},
		},
	},

	ENABLE_FAULT_INJECTION: {
		Id:     ENABLE_FAULT_INJECTION,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_bool,
				Fn:        multi.EnableFaultInjection,
			},
		},
	},
	DISABLE_FAULT_INJECTION: {
		Id:     DISABLE_FAULT_INJECTION,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{},
				ReturnTyp: types.T_bool,
				Fn:        multi.DisableFaultInjection,
			},
		},
	},
	ADD_FAULT_POINT: {
		Id:     ADD_FAULT_POINT,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_varchar},
				ReturnTyp: types.T_bool,
				Fn:        multi.AddFaultPoint,
			},
		},
	},
	REMOVE_FAULT_POINT: {
		Id:     REMOVE_FAULT_POINT,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_bool,
				Fn:        multi.RemoveFaultPoint,
			},
		},
	},
	TRIGGER_FAULT_POINT: {
		Id:     REMOVE_FAULT_POINT,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        multi.TriggerFaultPoint,
			},
		},
	},
	LOAD_FILE: {
		Id:     LOAD_FILE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_text,
				Fn:        unary.LoadFile,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_text,
				Fn:        unary.LoadFile,
			},
			// {
			// 	Index:     2,
			// 	Args:      []types.T{types.T_text},
			// 	ReturnTyp: types.T_blob,
			// 	Fn:        unary.LoadFile,
			// },
		},
	},
	HEX: {
		Id:     HEX,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexString,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexString,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.HexInt64,
			},
		},
	},
	SERIAL: {
		Id:     SERIAL,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			return int32(0), nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Serial,
			},
		},
	},
	HASH: {
		Id:     HASH,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(_ []Function, typs []types.T) (int32, []types.T) {
			return 0, typs
		},
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{},
				ReturnTyp: types.T_int64,
				Fn:        multi.Hash,
			},
		},
	},
	BIN: {
		Id:     BIN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.Bin[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_varchar,
				Fn:        unary.BinFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_varchar,
				Fn:        unary.BinFloat[float64],
			},
		},
	},
	REGEXP_INSTR: {
		Id:     REGEXP_INSTR,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_int64,
				Fn:        multi.RegularInstr,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RegularInstr,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        multi.RegularInstr,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64, types.T_uint8},
				ReturnTyp: types.T_int64,
				Fn:        multi.RegularInstr,
			},
		},
	},
	REPLACE: {
		Id:     REPLACE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Replace,
			},
		},
	},
	REGEXP_REPLACE: {
		Id:     REGEXP_REPLACE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularReplace,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularReplace,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularReplace,
			},
		},
	},
	REGEXP_LIKE: {
		Id:     REGEXP_LIKE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_bool,
				Fn:        multi.RegularLike,
			},
		},
	},
	REGEXP_SUBSTR: {
		Id:     REGEXP_SUBSTR,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularSubstr,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularSubstr,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				ReturnTyp: types.T_varchar,
				Fn:        multi.RegularSubstr,
			},
		},
	},

	MO_MEMORY_USAGE: {
		Id:     MO_MEMORY_USAGE,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.MoMemUsage,
			},
		},
	},

	MO_ENABLE_MEMORY_USAGE_DETAIL: {
		Id:     MO_ENABLE_MEMORY_USAGE_DETAIL,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.MoEnableMemUsageDetail,
			},
		},
	},
	MO_DISABLE_MEMORY_USAGE_DETAIL: {
		Id:     MO_DISABLE_MEMORY_USAGE_DETAIL,
		Flag:   plan.Function_INTERNAL,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        unary.MoDisableMemUsageDetail,
			},
		},
	},
	DATEDIFF: {
		Id:     DATEDIFF,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_date, types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        binary.DateDiff,
			},
		},
	},
	TIMESTAMPDIFF: {
		Id:     TIMESTAMPDIFF,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_datetime, types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        multi.TimeStampDiff,
			},
		},
	},
	TIMEDIFF: {
		Id:     TIMEDIFF,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_time, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        binary.TimeDiff[types.Time],
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_datetime, types.T_datetime},
				ReturnTyp: types.T_time,
				Fn:        binary.TimeDiff[types.Datetime],
			},
		},
	},
	MO_CTL: {
		Id:     MO_CTL,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        ctl.Handler,
			},
		},
	},
	MO_SHOW_VISIBLE_BIN: {
		Id:     MO_SHOW_VISIBLE_BIN,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_uint8},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ShowVisibleBin,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_text, types.T_uint8},
				ReturnTyp: types.T_varchar,
				Fn:        binary.ShowVisibleBin,
			},
		},
	},
	FIELD: {
		Id:     FIELD,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			l := len(inputs)
			if l < 2 {
				return wrongFunctionParameters, nil
			}

			//If all arguments are strings, all arguments are compared as strings &&
			//If all arguments are numbers, all arguments are compared as numbers.
			returnType := [...]types.T{types.T_varchar, types.T_char, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64}
			for i := range returnType {
				if operator.CoalesceTypeCheckFn(inputs, nil, returnType[i]) {
					if i < 2 {
						return 0, nil
					} else {
						return int32(i - 1), nil
					}
				}
			}

			//Otherwise, the arguments are compared as double.
			targetTypes := make([]types.T, l)

			for j := 0; j < l; j++ {
				targetTypes[j] = types.T_float64
			}
			if code, _ := tryToMatch(inputs, targetTypes); code == matchedByConvert {
				return 10, targetTypes
			}

			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldString,
			},
			{
				Index:     1,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[int8],
			},
			{
				Index:     2,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[int16],
			},
			{
				Index:     3,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[int32],
			},
			{
				Index:     4,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[int64],
			},
			{
				Index:     5,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[uint8],
			},
			{
				Index:     6,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[uint16],
			},
			{
				Index:     7,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[uint32],
			},
			{
				Index:     8,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[uint64],
			},
			{
				Index:     9,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[float32],
			},
			{
				Index:     10,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        multi.FieldNumber[float64],
			},
		},
	},
	SUBSTRING_INDEX: {
		Id:     SUBSTRING_INDEX,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			l := len(inputs)
			if l != 3 {
				return wrongFunctionParameters, nil
			}

			stringType := [...]types.T{types.T_varchar, types.T_char, types.T_text, types.T_blob}
			numberType := [...]types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64}
			//the first and second arg's types
			stringTypeSet := make(map[types.T]bool)
			for _, v := range stringType {
				stringTypeSet[v] = true
			}
			//the third arg's types
			numberTypeSet := make(map[types.T]bool)
			for _, v := range numberType {
				numberTypeSet[v] = true
			}
			if stringTypeSet[inputs[0]] && stringTypeSet[inputs[1]] && numberTypeSet[inputs[2]] {
				return 0, nil
			}

			//otherwise, try to cast
			minCost, minIndex := math.MaxInt32, -1
			convertTypes := make([]types.T, l)
			thirdArgType := [...]types.T{types.T_float64, types.T_uint64, types.T_int64}

			for _, first := range stringType {
				for _, second := range stringType {
					for _, third := range thirdArgType {
						targetTypes := []types.T{first, second, third}
						if code, c := tryToMatch(inputs, targetTypes); code == matchedByConvert {
							if c < minCost {
								minCost = c
								copy(convertTypes, targetTypes)
								minIndex = 0
							}
						}
					}
				}
			}
			if minIndex != -1 {
				return 0, convertTypes
			}

			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				ReturnTyp: types.T_varchar,
				Fn:        multi.SubStrIndex,
			},
		},
	},
	FORMAT: {
		Id:     FORMAT,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Format,
			},
			{
				Index:     1,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        multi.Format,
			},
		},
	},
	SLEEP: {
		Id:     SLEEP,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint8,
				Volatile:  true,
				Fn:        unary.Sleep[uint64],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_uint8,
				Volatile:  true,
				Fn:        unary.Sleep[float64],
			},
		},
	},
	INSTR: {
		Id:     INSTR,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_int64,
				Volatile:  false,
				Fn:        binary.Instr,
			},
		},
	},
	SPLIT_PART: {
		Id:     SPLIT_PART,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar, types.T_varchar, types.T_uint32},
				ReturnTyp: types.T_varchar,
				Volatile:  false,
				Fn:        multi.SplitPart,
			},
		},
	},
	CURRENT_DATE: {
		Id:     CURRENT_DATE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:           0,
				Args:            []types.T{},
				ReturnTyp:       types.T_date,
				Volatile:        false,
				RealTimeRelated: true,
				Fn:              unary.CurrentDate,
			},
		},
	},
	ASCII: {
		Id:     ASCII,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_varchar},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiString,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_char},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiString,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_text},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiString,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiInt[int8],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiInt[int16],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiInt[int32],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiInt[int64],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiUint[uint8],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiUint[uint16],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiUint[uint32],
			},
			{
				Index:     10,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint8,
				Fn:        unary.AsciiUint[uint64],
			},
		},
	},
	MO_TABLE_ROWS: {
		Id:     MO_TABLE_ROWS,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:           0,
				Args:            []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp:       types.T_int64,
				Volatile:        true,
				RealTimeRelated: true,
				Fn:              ctl.MoTableRows,
			},
		},
	},
	MO_TABLE_SIZE: {
		Id:     MO_TABLE_SIZE,
		Flag:   plan.Function_STRICT,
		Layout: STANDARD_FUNCTION,
		Overloads: []Function{
			{
				Index:           0,
				Args:            []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp:       types.T_int64,
				Volatile:        true,
				RealTimeRelated: true,
				Fn:              ctl.MoTableSize,
			},
		},
	},
}
