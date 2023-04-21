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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/ctl"
)

var _ = tempListForUnaryFunctions1
var _ = tempListForBinaryFunctions2

// tempListForUnaryFunctions1 just set the unary functions still need to be refactored.
// TODO: plz find the entrance to the old execute code from sql/plan/function/builtins.go
//
//	NewOp is new execute code. I set it empty now.
//	if you want to rewrite, and refer to ./builtinSet.go
//	if you want to modify, you should copy code from function directory.
//	but no matter which one, you should see how the old code do.
var tempListForUnaryFunctions1 = []FuncNew{
	// function `abs`
	{
		functionId: ABS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: AbsInt64,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: AbsUInt64, // which need us to refactor.
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: AbsFloat64,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: AbsDecimal128,
			},
		},
	},

	// function `ascii`
	{
		functionId: ASCII,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		// I think we can just set them as one overload but not so much.
		// but if so, we should rewrite a `checkFn` for this function.
		// maybe you can see how I refactor the +, -, cast and so on.
		// but I just do the simple copy here.
		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiString,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiString,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiString,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiInt[int8],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiInt[int16],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiInt[int32],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiInt[int64],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiUint[uint8],
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiUint[uint16],
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiUint[uint32],
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: AsciiUint[uint64],
			},
		},
	},

	// function `bin`
	{
		functionId: BIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[uint8],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[uint16],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[uint32],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[uint64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[int8],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[int16],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[int32],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Bin[int64],
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: BinFloat[float32],
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: BinFloat[float64],
			},
		},
	},

	// function `bit_length`
	{
		functionId: BIT_LENGTH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar}, // old is t_char, I think t_varchar is more suitable.
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: BitLengthFunc,
			},
		},
	},

	// function `current_date`
	{
		functionId: CURRENT_DATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            nil,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: CurrentDate,
			},
		},
	},

	// function `date`
	{
		functionId: DATE,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DateToDate,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: TimeToDate,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DatetimeToDate,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DateStringToDate,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DateStringToDate,
			},
		},
	},

	// function `day`
	{
		functionId: DAY,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DateToDay,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToDay,
			},
		},
	},

	// function `day_of_year`
	{
		functionId: DAYOFYEAR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint16.ToType()
				},
				NewOp: DayOfYear,
			},
		},
	},

	// function `empty`
	{
		functionId: EMPTY,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: Empty,
			},
		},
	},

	// function `hex`
	{
		functionId: HEX,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: HexString,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: HexString,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: HexInt64,
			},
		},
	},

	// function `json_quote`
	{
		functionId: JSON_QUOTE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_json.ToType()
				},
				NewOp: JsonQuote,
			},
		},
	},

	// function `json_unquote`
	{
		functionId: JSON_UNQUOTE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_json},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: JsonUnquote,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: JsonUnquote,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: JsonUnquote,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: JsonUnquote,
			},
		},
	},

	// function `length`
	{
		functionId: LENGTH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: Length,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: Length,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: Length,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: Length,
			},
		},
	},

	// function `length_utf8`
	{
		functionId: LENGTH_UTF8,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: LengthUTF8,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: LengthUTF8,
			},
		},
	},

	// function `load_file`
	// confused.
	{
		functionId: LOAD_FILE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: LoadFile,
			},
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: LoadFile,
			},
		},
	},

	// function `ltrim`
	{
		functionId: LTRIM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Ltrim,
			},
			{
				overloadId: 0,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: Ltrim,
			},
		},
	},

	// function `rtrim`
	{
		functionId: RTRIM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Rtrim,
			},
			{
				overloadId: 0,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: Rtrim,
			},
		},
	},

	// function `sleep`
	{
		functionId: SLEEP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: Sleep[uint64],
			},
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameter []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: Sleep[float64],
			},
		},
	},

	// function `reverse`
	{
		functionId: REVERSE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Reverse,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Reverse,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: Reverse,
			},
		},
	},

	{
		functionId: VERSION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            nil,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Version,
			},
		},
	},

	{
		functionId: NEXTVAL,
		class:      plan.Function_STRICT,
		layout:     UNKNOW_KIND_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Nextval,
			},
		},
	},

	{
		functionId: SETVAL,
		class:      plan.Function_STRICT,
		layout:     UNKNOW_KIND_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Setval,
			},
			{
				overloadId: 1,
				args: []types.T{
					types.T_varchar,
					types.T_varchar,
					types.T_bool,
				},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Setval,
			},
		},
	},

	{
		functionId: CURRVAL,
		class:      plan.Function_STRICT,
		layout:     UNKNOW_KIND_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Currval,
			},
		},
	},

	{
		functionId: LASTVAL,
		class:      plan.Function_STRICT,
		layout:     UNKNOW_KIND_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            nil,
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Lastval,
			},
		},
	},

	{
		functionId: GIT_VERSION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            nil,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: GitVersion,
			},
		},
	},

	{
		functionId: BUILD_VERSION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            nil,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: BuildVersion,
			},
		},
	},

	// function `oct`
	{
		functionId: OCT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[uint8],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[uint16],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[uint32],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[uint64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[int8],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[int16],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[int32],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: Oct[int64],
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: OctFloat[float32],
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				NewOp: OctFloat[float64],
			},
		},
	},

	{
		functionId: MONTH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DateToMonth,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToMonth,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DateStringToMonth,
			},
		},
	},

	{
		functionId: YEAR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: DateToYear,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: DatetimeToYear,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: DateStringToYear,
			},
		},
	},

	{
		functionId: WEEK,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DateToWeek,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToWeek,
			},
		},
	},

	{
		functionId: WEEKDAY,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: DateToWeekday,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: DatetimeToWeekday,
			},
		},
	},

	{
		functionId: MO_MEMORY_USAGE,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: MoMemUsage,
			},
		},
	},

	{
		functionId: MO_ENABLE_MEMORY_USAGE_DETAIL,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: MoEnableMemUsageDetail,
			},
		},
	},

	{
		functionId: MO_CTL,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ctl.MoCtl,
			},
		},
	},

	{
		functionId: MO_TABLE_ROWS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: MoTableRows,
			},
		},
	},

	{
		functionId: MO_TABLE_SIZE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: MoTableSize,
			},
		},
	},

	{
		functionId: MO_TABLE_COL_MAX,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: MoTableColMax,
			},
		},
	},

	{
		functionId: MO_TABLE_COL_MIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: MoTableColMin,
			},
		},
	},

	{
		functionId: SPACE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SpaceNumber[uint64],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SpaceNumber[int64],
			},
		},
	},

	{
		functionId: TIME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: TimeToTime,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DateToTime,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DatetimeToTime,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: Int64ToTime,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: Decimal128ToTime,
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DateStringToTime,
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DateStringToTime,
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DateStringToTime,
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: DateStringToTime,
			},
		},
	},

	{
		functionId: TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: DateToTimestamp,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: DatetimeToTimestamp,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: TimestampToTimestamp,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: DateStringToTimestamp,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: DateStringToTimestamp,
			},
		},
	},

	{
		functionId: VALUES,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: Values,
			},
		},
	},

	{
		functionId: HOUR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: TimestampToHour,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToHour,
			},
		},
	},

	{
		functionId: MINUTE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: TimestampToMinute,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToMinute,
			},
		},
	},

	{
		functionId: SECOND,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: TimestampToSecond,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: DatetimeToSecond,
			},
		},
	},
}

// tempListForBinaryFunctions just set the binary functions still need to be refactored.
// TODO: plz find the entrance to the old execute code from sql/plan/function/builtins.go
//
//	NewOp is new execute code. I set it empty now.
var tempListForBinaryFunctions2 = []FuncNew{

	{
		functionId: ENDSWITH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: EndsWith,
			},
		},
	},
	{
		functionId: EXTRACT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ExtractFromDatetime,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ExtractFromDate,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ExtractFromTime,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ExtractFromVarchar,
			},
		},
	},
	{
		functionId: FINDINSET,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FindInSet,
			},
		},
	},
	{
		functionId: INSTR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: Instr,
			},
		},
	},
	{
		functionId: LEFT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Left,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: Left,
			},
		},
	},
	{
		functionId: STARTSWITH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				NewOp: Startswith,
			},
		},
	},
	{
		functionId: TIMEDIFF,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_time, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: TimeDiff[types.Time],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: TimeDiff[types.Datetime],
			},
		},
	},
	{
		functionId: POW,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: Power,
			},
		},
	},

	// function `date_format`
	// function `show_visible_bin`
	// function `str_to_date`, `to_date`
}
