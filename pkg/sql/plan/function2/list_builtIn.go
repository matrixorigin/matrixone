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

var supportedStringBuiltIns = []FuncNew{
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

	// function `binary`
	{
		functionId: BINARY,
		class:      plan.Function_STRICT,
		layout:     CAST_EXPRESSION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				NewOp: Binary,
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

	// function `concat`
	{
		functionId: CONCAT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    builtInConcatCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					for _, p := range parameters {
						if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
							return types.T_blob.ToType()
						}
					}
					return types.T_varchar.ToType()
				},
				NewOp: builtInConcat,
			},
		},
	},

	// function `concat_ws`
	{
		functionId: CONCAT_WS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    concatWsCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					for _, p := range parameters {
						if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
							return types.T_blob.ToType()
						}
					}
					return types.T_varchar.ToType()
				},
				NewOp: ConcatWs,
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

	// function `endswith`
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

	// function `extract`
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
					return types.T_uint32.ToType()
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

	// function `field`
	{
		functionId: FIELD,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    filedCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldString,
			},
			{
				overloadId: 1,
				volatile:   true,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[int8],
			},
			{
				overloadId: 2,
				volatile:   true,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[int16],
			},
			{
				overloadId: 3,
				volatile:   true,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[int32],
			},
			{
				overloadId: 4,
				volatile:   true,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[int64],
			},
			{
				overloadId: 5,
				volatile:   true,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[uint8],
			},
			{
				overloadId: 6,
				volatile:   true,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[uint16],
			},
			{
				overloadId: 7,
				volatile:   true,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[uint32],
			},
			{
				overloadId: 8,
				volatile:   true,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[uint64],
			},
			{
				overloadId: 9,
				volatile:   true,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[float32],
			},
			{
				overloadId: 10,
				volatile:   true,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FieldNumber[float64],
			},
		},
	},

	// function `findinset`, `find_in_set`
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

	// function `format`
	{
		functionId: FORMAT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    formatCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: FormatWith2Args,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: FormatWith3Args,
			},
		},
	},

	// function `ilike`
	{
		functionId: ILIKE,
		class:      plan.Function_STRICT,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				if inputs[0].Oid.IsMySQLString() && inputs[1].Oid.IsMySQLString() {
					return newCheckResultWithSuccess(0)
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: newOpBuiltInRegexp().iLikeFn,
			},
		},
	},

	// function `instr`
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

	// function `json_extract`
	{
		functionId: JSON_EXTRACT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    jsonExtractCheckFn,
		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_json.ToType()
				},
				NewOp: JsonExtract,
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

	// function `left`
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

	// function `length_utf8`, `char_length`
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

	// function `lpad`
	{
		functionId: LPAD,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInLpad,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_blob, types.T_int64, types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: builtInLpad,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: builtInLpad,
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

	// function `not_reg_match`
	{
		functionId: NOT_REG_MATCH,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInNotRegMatch,
			},
		},
	},

	// function `replace`
	{
		functionId: REPLACE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Replace,
			},
		},
	},

	// function `reg_match`
	{
		functionId: REG_MATCH,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegMatch,
			},
		},
	},

	// function `regexp_instr`
	{
		functionId: REGEXP_INSTR,
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
				NewOp: newOpBuiltInRegexp().builtInRegexpInstr,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpInstr,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpInstr,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64, types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpInstr,
			},
		},
	},

	// function `regexp_like`
	{
		functionId: REGEXP_LIKE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpLike,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpLike,
			},
		},
	},

	// function `regexp_replace`
	{
		functionId: REGEXP_REPLACE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpReplace,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpReplace,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpReplace,
			},
		},
	},

	// function `regexp_substr`
	{
		functionId: REGEXP_SUBSTR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpSubstr,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpSubstr,
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: newOpBuiltInRegexp().builtInRegexpSubstr,
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

	// function `rpad`
	{
		functionId: RPAD,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInRpad,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_blob, types.T_int64, types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: builtInRpad,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: builtInRpad,
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

	// function `serial`
	{
		functionId: SERIAL,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) > 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInSerial,
			},
		},
	},

	// function `space`
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

	// function `split_part`
	{
		functionId: SPLIT_PART,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SplitPart,
			},
		},
	},

	// function `startswith`
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
				NewOp: StartsWith,
			},
		},
	},

	// function `substring`, `substr`, `mid`
	{
		functionId: SUBSTRING,
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
				NewOp: SubStringWith2Args[int64],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith2Args[uint64],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_char, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith2Args[int64],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith2Args[uint64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_char, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith2Args[float64],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_varchar, types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[float64, int64],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_varchar, types.T_float64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[float64, uint64],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[int64, float64],
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_varchar, types.T_uint64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[uint64, float64],
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_varchar, types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[float64, float64],
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[int64, int64],
			},
			{
				overloadId: 11,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[int64, uint64],
			},
			{
				overloadId: 12,
				args:       []types.T{types.T_varchar, types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[uint64, int64],
			},
			{
				overloadId: 13,
				args:       []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStringWith3Args[uint64, uint64],
			},
			{
				overloadId: 14,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith3Args[int64, int64],
			},
			{
				overloadId: 15,
				args:       []types.T{types.T_char, types.T_int64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith3Args[int64, uint64],
			},
			{
				overloadId: 16,
				args:       []types.T{types.T_char, types.T_uint64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith3Args[int64, uint64],
			},
			{
				overloadId: 17,
				args:       []types.T{types.T_char, types.T_uint64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				NewOp: SubStringWith3Args[uint64, uint64],
			},
			{
				overloadId: 18,
				args:       []types.T{types.T_blob, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith3Args[int64, int64],
			},
			{
				overloadId: 19,
				args:       []types.T{types.T_blob, types.T_int64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith3Args[uint64, int64],
			},
			{
				overloadId: 20,
				args:       []types.T{types.T_blob, types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith3Args[uint64, int64],
			},
			{
				overloadId: 21,
				args:       []types.T{types.T_blob, types.T_uint64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith3Args[uint64, uint64],
			},
			{
				overloadId: 22,
				args:       []types.T{types.T_text, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: SubStringWith3Args[int64, int64],
			},
			{
				overloadId: 23,
				args:       []types.T{types.T_text, types.T_int64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: SubStringWith3Args[int64, uint64],
			},
			{
				overloadId: 24,
				args:       []types.T{types.T_text, types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: SubStringWith3Args[uint64, int64],
			},
			{
				overloadId: 25,
				args:       []types.T{types.T_text, types.T_uint64, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				NewOp: SubStringWith3Args[uint64, int64],
			},
			{
				overloadId: 26,
				args:       []types.T{types.T_blob, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith2Args[float64],
			},
			{
				overloadId: 27,
				args:       []types.T{types.T_blob, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith2Args[uint64],
			},
			{
				overloadId: 28,
				args:       []types.T{types.T_blob, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				NewOp: SubStringWith2Args[int64],
			},
		},
	},

	// function `substring_index`
	{
		functionId: SUBSTRING_INDEX,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch, // TODO:

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStrIndex[float64],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStrIndex[uint64],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: SubStrIndex[int64],
			},
		},
	},

	// function `trim`
	{
		functionId: TRIM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: Trim,
			},
		},
	},
}

var supportedMathBuiltIns = []FuncNew{
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

	// function `acos`
	{
		functionId: ACOS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInACos,
			},
		},
	},

	// function `atan`
	{
		functionId: ATAN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInATan,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInATan2,
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

	// function `ceil`, `ceiling`
	{
		functionId: CEIL,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: CeilUint64,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: CeilUint64,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: CeilInt64,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: CeilInt64,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: CeilFloat64,
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: CeilFloat64,
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: CeilDecimal64,
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: CeilDecimal64,
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: CeilDecimal128,
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: CeilDecimal128,
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: CeilStr,
			},
		},
	},

	// function `cos`
	{
		functionId: COS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInCos,
			},
		},
	},

	// function `cot`
	{
		functionId: COT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInCot,
			},
		},
	},

	// function `exp`
	{
		functionId: EXP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInExp,
			},
		},
	},

	// function `floor`
	{
		functionId: FLOOR,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FloorUInt64,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FloorUInt64,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: FloorInt64,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: FloorInt64,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: FloorFloat64,
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: FloorFloat64,
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: FloorDecimal64,
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: FloorDecimal64,
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: FloorDecimal128,
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: FloorDecimal128,
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: FloorStr,
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

	// function `ln`
	{
		functionId: LN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInLn,
			},
		},
	},

	// function `log`
	{
		functionId: LOG,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInLn,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInLog,
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

	// function `PI`
	{
		functionId: PI,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: Pi,
			},
		},
	},

	// function `power`
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

	// function `rand`, `rand(1)`
	{
		functionId: RANDOM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: newOpBuiltInRand().builtInRand,
			},

			{
				overloadId: 1,
				args:       nil,
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInRand,
			},
		},
	},

	// function `round`
	{
		functionId: ROUND,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: RoundUint64,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: RoundUint64,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: RoundInt64,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: RoundInt64,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: RoundFloat64,
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: RoundFloat64,
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: RoundDecimal64,
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: RoundDecimal64,
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: RoundDecimal128,
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: RoundDecimal128,
			},
		},
	},

	// function `sin`
	{
		functionId: SIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInSin,
			},
		},
	},

	// function `sinh`
	{
		functionId: SINH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInSinh,
			},
		},
	},

	// function `tan`
	{
		functionId: TAN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInTan,
			},
		},
	},
}

var supportedDateAndTimeBuiltIns = []FuncNew{
	// function `current_date`, `curdate`
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

	// function `current_timestamp`, `now`
	{
		functionId: CURRENT_TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId:      0,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: builtInCurrentTimestamp,
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

	// function `date_add`
	{
		functionId: DATE_ADD,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DateAdd,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DatetimeAdd,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DateStringAdd,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DateStringAdd,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: TimestampAdd,
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_time, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				NewOp: TimeAdd,
			},
		},
	},

	// function `date_format`
	{
		functionId: DATE_FORMAT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_datetime, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: DateFormat,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: DateFormat,
			},
		},
	},

	// function `date_sub`
	{
		functionId: DATE_SUB,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: DateSub,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DatetimeSub,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DateStringSub,
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: DateStringSub,
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: TimestampSub,
			},
		},
	},

	// function `datediff`
	{
		functionId: DATEDIFF,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true, // TODO: why true.
				args:       []types.T{types.T_date, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInDateDiff,
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

	// function `from_unixtime`
	{
		functionId: FROM_UNIXTIME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: FromUnixTimeInt64,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: FromUnixTimeUint64,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: FromUnixTimeFloat64,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: FromUnixTimeInt64Format,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: FromUnixTimeUint64Format,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: FromUnixTimeFloat64Format,
			},
		},
	},

	// function `hour`
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

	// function `minute`
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

	// function `mo_log_date`
	{
		functionId: MO_LOG_DATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: builtInMoLogDate,
			},
		},
	},

	// function `month`
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

	// function `second`
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

	// function `str_to_date`, `to_date`
	{
		functionId: STR_TO_DATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},

				NewOp: builtInStrToDatetime,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},

				NewOp: builtInStrToDate,
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},

				NewOp: builtInStrToTime,
			},
		},
	},

	// function `time`
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

	// function `timediff`
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

	// function `timestamp`
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

	// function `timestampdiff`
	{
		functionId: TIMESTAMPDIFF,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_datetime, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: TimestampDiff,
			},
		},
	},

	// function `unix_timestamp`
	{
		functionId: UNIX_TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInUnixTimestamp,
			},
			{
				overloadId: 1,
				volatile:   true,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInUnixTimestamp,
			},
			{
				overloadId: 2,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInUnixTimestampVarcharToInt64,
			},
			{
				overloadId: 3,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return types.New(types.T_decimal128, 38, 6)
				},
				NewOp: builtInUnixTimestampVarcharToDecimal128,
			},
		},
	},

	// function `utc_timestamp`
	{
		functionId: UTC_TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				NewOp: UTCTimestamp,
			},
		},
	},

	// function `week`
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

	// function `weekday`
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

	// function `year`
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
}

var supportedControlBuiltIns = []FuncNew{
	// function `add_fault_point`
	{
		functionId: ADD_FAULT_POINT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: AddFaultPoint,
			},
		},
	},

	// function `disable_fault_injection`
	{
		functionId: DISABLE_FAULT_INJECTION,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: DisableFaultInjection,
			},
		},
	},

	// function `enable_fault_injection`
	{
		functionId: ENABLE_FAULT_INJECTION,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: EnableFaultInjection,
			},
		},
	},

	// function `mo_ctl`
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

	// function `mo_enable_memory_usage_detail`
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

	// function `mo_disable_memory_usage_detail`
	{
		functionId: MO_DISABLE_MEMORY_USAGE_DETAIL,
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
				NewOp: MoDisableMemUsageDetail,
			},
		},
	},

	// function `remove_fault_point`
	{
		functionId: REMOVE_FAULT_POINT,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				NewOp: RemoveFaultPoint,
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

	// function `trigger_fault_point`
	{
		functionId: TRIGGER_FAULT_POINT,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: TriggerFaultPoint,
			},
		},
	},
}

var supportedOthersBuiltIns = []FuncNew{
	// function `build_version`
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

	// function `charset`
	{
		functionId: CHARSET,
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
				NewOp: Charset,
			},
		},
	},

	// function `collation`
	{
		functionId: COLLATION,
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
				NewOp: Collation,
			},
		},
	},

	// function `connection_id`
	{
		functionId: CONNECTION_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: ConnectionID,
			},
		},
	},

	// function `current_account_id`
	{
		functionId: CURRENT_ACCOUNT_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				NewOp: builtInCurrentAccountID,
			},
		},
	},

	// function `current_account_name`
	{
		functionId: CURRENT_ACCOUNT_NAME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInCurrentAccountName,
			},
		},
	},

	// function `current_role`
	{
		functionId: CURRENT_ROLE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInCurrentRole,
			},
		},
	},

	// function `current_role_id`
	{
		functionId: CURRENT_ROLE_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				NewOp: builtInCurrentRoleID,
			},
		},
	},

	// function `current_user_id`
	{
		functionId: CURRENT_USER_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				NewOp: builtInCurrentUserID,
			},
		},
	},

	// function `current_role_name`
	{
		functionId: CURRENT_ROLE_NAME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInCurrentRoleName,
			},
		},
	},

	// function `current_user_name`
	{
		functionId: CURRENT_USER_NAME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInCurrentUserName,
			},
		},
	},

	// function `Database`, `schema`
	{
		functionId: DATABASE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       nil,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInDatabase,
			},
		},
	},

	// function `found_rows`
	{
		functionId: FOUND_ROWS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: FoundRows,
			},
		},
	},

	// function `git_version`
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

	// function `hash_value`
	// XXX may be used for hash bucket splitting
	{
		functionId: HASH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) > 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInHash,
			},
		},
	},

	// function `icu_version`
	{
		functionId: ICULIBVERSION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: ICULIBVersion,
			},
		},
	},

	// function `if`, `iff`
	{
		functionId: IFF,
		class:      plan.Function_NONE,
		layout:     STANDARD_FUNCTION,
		checkFn:    iffCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[1]
				},
				NewOp: iffFn,
			},
		},
	},

	// function `internal_auto_increment`
	// 'internal_auto_increment' is used to obtain the current auto_increment column value of the table under the specified database
	{
		functionId: INTERNAL_AUTO_INCREMENT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: builtInInternalAutoIncrement,
			},
		},
	},

	// function `internal_char_length`
	{
		functionId: INTERNAL_CHAR_LENGTH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharLength,
			},
		},
	},

	// function `internal_char_size`
	{
		functionId: INTERNAL_CHAR_SIZE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharSize,
			},
		},
	},

	// function `internal_column_character_set`
	{
		functionId: INTERNAL_COLUMN_CHARACTER_SET,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharacterSet,
			},
		},
	},

	// function `internal_datetime_scale`
	{
		functionId: INTERNAL_DATETIME_SCALE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalDatetimeScale,
			},
		},
	},

	// function `internal_numeric_precision`
	{
		functionId: INTERNAL_NUMERIC_PRECISION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalNumericPrecision,
			},
		},
	},

	// function `internal_numeric_scale`
	{
		functionId: INTERNAL_NUMERIC_SCALE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalNumericScale,
			},
		},
	},

	// function `last_insert_id`
	{
		functionId: LAST_INSERT_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: LastInsertID,
			},
		},
	},

	// function `last_query_id`, `last_uuid`
	{
		functionId: LAST_QUERY_ID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: LastQueryIDWithoutParam,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: LastQueryID,
			},
		},
	},

	// function `load_file`
	// confused function.
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

	// function `mo_memory_usage`
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

	// function `mo_show_visible_bin`
	{
		functionId: MO_SHOW_VISIBLE_BIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInMoShowVisibleBin,
			},
		},
	},

	// function `mo_table_rows`
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

	// function `mo_table_size`
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

	// function `mo_table_col_max`
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

	// function `mo_table_col_min`
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

	// function `roles_graphml`
	{
		functionId: ROLES_GRAPHML,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: RolesGraphml,
			},
		},
	},

	// function `row_count`
	{
		functionId: ROW_COUNT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: RowCount,
			},
		},
	},

	// sequence related functions
	// function `nextval`
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
	// function `setval`
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
	// function `currval`
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
	// function lastval
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

	// function `user`, `system_user`, "current_user", "session_user"
	{
		functionId: USER,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: User,
			},
		},
	},

	// function `uuid`
	{
		functionId: UUID,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uuid.ToType()
				},
				NewOp: builtInUUID,
			},
		},
	},

	// function `values`
	{
		functionId: VALUES,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: Values,
			},
		},
	},

	// function `version`
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
}
