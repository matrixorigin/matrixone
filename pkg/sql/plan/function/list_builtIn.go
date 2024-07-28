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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
				newOp: func() executeLogicOfOverload {
					return AsciiString
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiString
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiString
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiInt[int8]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiInt[int16]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiInt[int32]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiInt[int64]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiUint[uint8]
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiUint[uint16]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiUint[uint32]
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AsciiUint[uint64]
				},
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
				newOp: func() executeLogicOfOverload {
					return Binary
				},
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
				newOp: func() executeLogicOfOverload {
					return BitLengthFunc
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInConcat
				},
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
				newOp: func() executeLogicOfOverload {
					return ConcatWs
				},
			},
		},
	},

	// function `convert`
	{
		functionId: CONVERT,
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
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInConvertFake
				},
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
				newOp: func() executeLogicOfOverload {
					return Empty
				},
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
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return EndsWith
				},
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
				newOp: func() executeLogicOfOverload {
					return ExtractFromDatetime
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ExtractFromDate
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ExtractFromTime
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ExtractFromVarchar
				},
			},
		},
	},

	// function `field`
	{
		functionId: FIELD,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fieldCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldString
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[int8]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[int16]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[int32]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[int64]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[uint8]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[uint16]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[uint32]
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[uint64]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[float32]
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FieldNumber[float64]
				},
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
				newOp: func() executeLogicOfOverload {
					return FindInSet
				},
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
				newOp: func() executeLogicOfOverload {
					return FormatWith2Args
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FormatWith3Args
				},
			},
		},
	},

	// function `ilike`
	{
		functionId: ILIKE,
		class:      plan.Function_STRICT,
		layout:     BINARY_ARITHMETIC_OPERATOR,
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().iLikeFn
				},
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
				newOp: func() executeLogicOfOverload {
					return Instr
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInJsonExtract().jsonExtract
				},
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
				newOp: func() executeLogicOfOverload {
					return JsonQuote
				},
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
				newOp: func() executeLogicOfOverload {
					return JsonUnquote
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return JsonUnquote
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return JsonUnquote
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return JsonUnquote
				},
			},
		},
	},

	// function `json_row`
	{
		functionId: JSON_ROW,
		class:      plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		// typechecking: always success
		checkFn: func(_ []overload, inputs []types.Type) checkResult {
			return newCheckResultWithSuccess(0)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInJsonRow().jsonRow
				},
			},
		},
	},

	// function `jq`
	{
		functionId: JQ,
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInJq().jq
				},
			},
		},
	},

	// function `try_jq`
	{
		functionId: TRY_JQ,
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInJq().tryJq
				},
			},
		},
	},

	// function `wasm`
	{
		functionId: WASM,
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInWasm().wasm
				},
			},
		},
	},

	// function `try_wasm`
	{
		functionId: TRY_WASM,
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInWasm().tryWasm
				},
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
				newOp: func() executeLogicOfOverload {
					return Left
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Left
				},
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
				newOp: func() executeLogicOfOverload {
					return Length
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Length
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Length
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Length
				},
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
				newOp: func() executeLogicOfOverload {
					return LengthUTF8
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LengthUTF8
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInLpad
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_blob, types.T_int64, types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInLpad
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInLpad
				},
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
				newOp: func() executeLogicOfOverload {
					return Ltrim
				},
			},
			{
				overloadId: 0,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Ltrim
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInNotRegMatch
				},
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
				newOp: func() executeLogicOfOverload {
					return Replace
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegMatch
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpInstr
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpInstr
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpInstr
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64, types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpInstr
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpLike
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpLike
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpReplace
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpReplace
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpReplace
				},
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
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpSubstr
				},
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpSubstr
				},
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().builtInRegexpSubstr
				},
			},
		},
	},

	// function `repeat`
	{
		functionId: REPEAT,
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
				newOp: func() executeLogicOfOverload {
					return builtInRepeat
				},
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
				newOp: func() executeLogicOfOverload {
					return Reverse
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Reverse
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Reverse
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInRpad
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_blob, types.T_int64, types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInRpad
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_blob, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInRpad
				},
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
				newOp: func() executeLogicOfOverload {
					return Rtrim
				},
			},
			{
				overloadId: 0,
				args:       []types.T{types.T_blob},
				retType: func(parameter []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Rtrim
				},
			},
		},
	},

	// function `serial`
	{
		functionId: SERIAL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
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
				newOpWithFree: func() (executeLogicOfOverload, executeFreeOfOverload) {
					opSerial := newOpSerial()
					return opSerial.BuiltInSerial, opSerial.Close
				},
			},
		},
	},

	// function `serial_full`
	{
		functionId: SERIAL_FULL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
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
				newOpWithFree: func() (executeLogicOfOverload, executeFreeOfOverload) {
					opSerial := newOpSerial()
					return opSerial.BuiltInSerialFull, opSerial.Close
				},
			},
		},
	},

	// function `serial_extract`
	{
		functionId: SERIAL_EXTRACT,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 3 {
				if inputs[0].Oid == types.T_varchar &&
					inputs[1].Oid == types.T_int64 {
					return newCheckResultWithSuccess(0)
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[2]
				},
				newOp: func() executeLogicOfOverload {
					return builtInSerialExtract
				},
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
				newOp: func() executeLogicOfOverload {
					return SpaceNumber[uint64]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SpaceNumber[int64]
				},
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
				newOp: func() executeLogicOfOverload {
					return SplitPart
				},
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
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return StartsWith
				},
			},
		},
	},

	// function `prefix_eq`
	{
		functionId: PREFIX_EQ,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return PrefixEq
				},
			},
		},
	},

	// function `prefix_in`
	{
		functionId: PREFIX_IN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newImplPrefixIn().doPrefixIn
				},
			},
		},
	},

	// function `prefix_between`
	{
		functionId: PREFIX_BETWEEN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return PrefixBetween
				},
			},
		},
	},
	// to_base64
	{
		functionId: TO_BASE64,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ToBase64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ToBase64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ToBase64
				},
			},
		},
	},

	// from_base64
	{
		functionId: FROM_BASE64,
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
				newOp: func() executeLogicOfOverload {
					return FromBase64
				},
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
				newOp: func() executeLogicOfOverload {
					return SubStringWith2Args
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith2Args
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith3Args
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith3Args
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_blob, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith3Args
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_text, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith3Args
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_blob, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStringWith2Args
				},
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
				newOp: func() executeLogicOfOverload {
					return SubStrIndex[float64]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStrIndex[uint64]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubStrIndex[int64]
				},
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
				newOp: func() executeLogicOfOverload {
					return Trim
				},
			},
		},
	},

	// function `lower`, `to_lower`
	{
		functionId: LOWER,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return builtInToLower
				},
			},
		},
	},

	// function `upper`, `to_upper`
	{
		functionId: UPPER,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return builtInToUpper
				},
			},
		},
	},

	// function `locate`
	{
		functionId: LOCATE,
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
				newOp: func() executeLogicOfOverload {
					return buildInLocate2Args
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char, types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInLocate2Args
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInLocate3Args
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_char, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInLocate3Args
				},
			},
		},
	},

	// function `sha2`
	{
		functionId: SHA2,
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
				newOp: func() executeLogicOfOverload {
					return SHA2Func
				},
			},
		},
	},
}

var supportedArrayOperations = []FuncNew{

	// function `summation`
	{
		functionId: SUMMATION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					// NOTE summation(vecf32) --> float64
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SummationArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SummationArray[float64]
				},
			},
		},
	},

	// function `l1_norm`
	{
		functionId: L1_NORM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L1NormArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L1NormArray[float64]
				},
			},
		},
	},

	// function `l2_norm`
	{
		functionId: L2_NORM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2NormArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2NormArray[float64]
				},
			},
		},
	},

	// function `vector_dims`
	{
		functionId: VECTOR_DIMS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return VectorDimsArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return VectorDimsArray[float64]
				},
			},
		},
	},

	// function `inner_product`
	{
		functionId: INNER_PRODUCT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return InnerProductArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return InnerProductArray[float64]
				},
			},
		},
	},

	// function `cosine_similarity`
	{
		functionId: COSINE_SIMILARITY,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CosineSimilarityArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CosineSimilarityArray[float64]
				},
			},
		},
	},
	// function `l2_distance`
	{
		functionId: L2_DISTANCE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2DistanceArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2DistanceArray[float64]
				},
			},
		},
	},
	// function `l2_distance_sq`
	{
		functionId: L2_DISTANCE_SQ,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2DistanceSqArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return L2DistanceSqArray[float64]
				},
			},
		},
	},
	// function `cosine_distance`
	{
		functionId: COSINE_DISTANCE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CosineDistanceArray[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CosineDistanceArray[float64]
				},
			},
		},
	},
	// function `normalize_l2`
	{
		functionId: NORMALIZE_L2,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return NormalizeL2Array[float32]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return NormalizeL2Array[float64]
				},
			},
		},
	},
	// function `subvector`
	{
		functionId: SUB_VECTOR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_array_float32, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubVectorWith2Args[float32]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubVectorWith2Args[float64]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_array_float32, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubVectorWith3Args[float32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_array_float64, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return SubVectorWith3Args[float64]
				},
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
				newOp: func() executeLogicOfOverload {
					return AbsInt64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AbsUInt64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AbsFloat64
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return AbsDecimal64
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return AbsDecimal128
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return AbsArray[float32]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return AbsArray[float64]
				},
			},
		},
	},
	// function `sqrt`
	{
		functionId: SQRT,
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
				newOp: func() executeLogicOfOverload {
					return builtInSqrt
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInSqrtArray[float32]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInSqrtArray[float64]
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInACos
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInATan
				},
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInATan2
				},
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
				newOp: func() executeLogicOfOverload {
					return Bin[uint8]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[uint16]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[uint32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[uint64]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[int8]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[int16]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[int32]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Bin[int64]
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return BinFloat[float32]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return BinFloat[float64]
				},
			},
		},
	},

	// function `ceil`, `ceiling`
	{
		functionId: CEIL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilUint64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilUint64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilInt64
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilInt64
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilFloat64
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilFloat64
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CeilDecimal64
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CeilDecimal64
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CeilDecimal128
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CeilDecimal128
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CeilStr
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInCos
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInCot
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInExp
				},
			},
		},
	},

	// function `floor`
	{
		functionId: FLOOR,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorUInt64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorUInt64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorInt64
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorInt64
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorFloat64
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorFloat64
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return FloorDecimal64
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return FloorDecimal64
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return FloorDecimal128
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return FloorDecimal128
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FloorStr
				},
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
				newOp: func() executeLogicOfOverload {
					return HexString
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexString
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexInt64
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexUint64
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexFloat32
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexFloat64
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexArray
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return HexArray
				},
			},
		},
	},

	// function `unhex`
	{
		functionId: UNHEX,
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
				newOp: func() executeLogicOfOverload {
					return Unhex
				},
			},
		},
	},

	// function `md5`
	{
		functionId: MD5,
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
				newOp: func() executeLogicOfOverload {
					return Md5
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInLn
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInLn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInLog
				},
			},
		},
	},

	// function `log2`
	{
		functionId: LOG2,
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
				newOp: func() executeLogicOfOverload {
					return builtInLog2
				},
			},
		},
	},

	// function `log10`
	{
		functionId: LOG10,
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
				newOp: func() executeLogicOfOverload {
					return builtInLog10
				},
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
				newOp: func() executeLogicOfOverload {
					return Oct[uint8]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[uint16]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[uint32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[uint64]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[int8]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[int16]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[int32]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Oct[int64]
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return OctFloat[float32]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_decimal128.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return OctFloat[float64]
				},
			},
		},
	},

	// function `PI`
	{
		functionId: PI,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Pi
				},
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
				newOp: func() executeLogicOfOverload {
					return Power
				},
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
			//{
			//	overloadId:     0,
			//	args:           []types.T{types.T_int64},
			//	cannotParallel: true,
			//	volatile:       true,
			//	retType: func(parameters []types.Type) types.Type {
			//		return types.T_float64.ToType()
			//	},
			//	newOp: func() executeLogicOfOverload {
			//		return newOpBuiltInRand().builtInRand
			//	},
			//},

			{
				overloadId: 0,
				args:       nil,
				volatile:   true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInRand
				},
			},
		},
	},

	// function `round`
	{
		functionId: ROUND,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundUint64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundUint64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundInt64
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundInt64
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundFloat64
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RoundFloat64
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return RoundDecimal64
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return RoundDecimal64
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return RoundDecimal128
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_decimal128, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return RoundDecimal128
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInSin
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInSinh
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInTan
				},
			},
		},
	},
}

var supportedDateAndTimeBuiltIns = []FuncNew{
	// function `convert_tz`
	{
		functionId: CONVERT_TZ,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_datetime, types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ConvertTz
				},
			},
		},
	},

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
				newOp: func() executeLogicOfOverload {
					return CurrentDate
				},
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
			if len(inputs) == 1 && inputs[0].Oid == types.T_int64 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId:      0,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					typ := types.T_timestamp.ToType()
					typ.Scale = 6
					return typ
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentTimestamp
				},
			},
		},
	},

	// function `sysdate` (execute timestamp)
	{
		functionId: SYSDATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			if len(inputs) == 1 && inputs[0].Oid == types.T_int64 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId:      0,
				realTimeRelated: true,
				volatile:        true,
				retType: func(parameters []types.Type) types.Type {
					typ := types.T_timestamp.ToType()
					typ.Scale = 6
					return typ
				},
				newOp: func() executeLogicOfOverload {
					return builtInSysdate
				},
			},
		},
	},

	// function `date`
	{
		functionId: DATE,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateToDate
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TimeToDate
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToDate
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToDate
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToDate
				},
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
				newOp: func() executeLogicOfOverload {
					return DateAdd
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeAdd
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringAdd
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringAdd
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TimestampAdd
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_time, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TimeAdd
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_text, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringAdd
				},
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
				newOp: func() executeLogicOfOverload {
					return DateFormat
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateFormat
				},
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
				newOp: func() executeLogicOfOverload {
					return DateSub
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeSub
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringSub
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_char, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringSub
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_timestamp, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TimestampSub
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_text, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringSub
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInDateDiff
				},
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
				newOp: func() executeLogicOfOverload {
					return DateToDay
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToDay
				},
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
				newOp: func() executeLogicOfOverload {
					return DayOfYear
				},
			},
		},
	},

	// function `from_unixtime`
	{
		functionId: FROM_UNIXTIME,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeInt64
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeUint64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeFloat64
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeInt64Format
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeUint64Format
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_float64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FromUnixTimeFloat64Format
				},
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
				newOp: func() executeLogicOfOverload {
					return TimestampToHour
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToHour
				},
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
				newOp: func() executeLogicOfOverload {
					return TimestampToMinute
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToMinute
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInMoLogDate
				},
			},
		},
	},

	// function `purge_log`
	{
		functionId: PURGE_LOG,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInPurgeLog
				},
			},
		},
	},

	// function `mo_admin_name`
	{
		functionId: MO_ADMIN_NAME,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInInternalGetAdminName
				},
			},
		},
	},

	// function `mo_cu`
	{
		functionId: MO_CU,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInMOCU
				},
			},
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInMOCU
				},
			},
		},
	},
	{
		functionId: MO_CU_V1,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInMOCUv1
				},
			},
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return buildInMOCUv1
				},
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
				newOp: func() executeLogicOfOverload {
					return DateToMonth
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToMonth
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToMonth
				},
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
				newOp: func() executeLogicOfOverload {
					return TimestampToSecond
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToSecond
				},
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

				newOp: func() executeLogicOfOverload {
					return builtInStrToDatetime
				},
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},

				newOp: func() executeLogicOfOverload {
					return builtInStrToDate
				},
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},

				newOp: func() executeLogicOfOverload {
					return builtInStrToTime
				},
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
				newOp: func() executeLogicOfOverload {
					return TimeToTime
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateToTime
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToTime
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Int64ToTime
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Decimal128ToTime
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTime
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTime
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTime
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTime
				},
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
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return TimeDiff[types.Time]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					t := types.T_time.ToType()
					t.Scale = parameters[0].Scale
					return t
				},
				newOp: func() executeLogicOfOverload {
					return TimeDiff[types.Datetime]
				},
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
				newOp: func() executeLogicOfOverload {
					return DateToTimestamp
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToTimestamp
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TimestampToTimestamp
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTimestamp
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToTimestamp
				},
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
				newOp: func() executeLogicOfOverload {
					return TimestampDiff
				},
			},
		},
	},

	// function `to_days`
	{
		functionId: TO_DAYS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInToDays
				},
			},
		},
	},

	// function `to_seconds`
	{
		functionId: TO_SECONDS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInToSeconds
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInUnixTimestamp
				},
			},
			{
				overloadId: 1,
				volatile:   true,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInUnixTimestamp
				},
			},
			{
				overloadId: 2,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInUnixTimestampVarcharToInt64
				},
			},
			{
				overloadId: 3,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return types.New(types.T_decimal128, 38, 6)
				},
				newOp: func() executeLogicOfOverload {
					return builtInUnixTimestampVarcharToDecimal128
				},
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
				newOp: func() executeLogicOfOverload {
					return UTCTimestamp
				},
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
				newOp: func() executeLogicOfOverload {
					return DateToWeek
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToWeek
				},
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
				newOp: func() executeLogicOfOverload {
					return DateToWeekday
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToWeekday
				},
			},
		},
	},

	// function `year`
	{
		functionId: YEAR,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateToYear
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DatetimeToYear
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return DateStringToYear
				},
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
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_int64, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return AddFaultPoint
				},
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
				newOp: func() executeLogicOfOverload {
					return DisableFaultInjection
				},
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
				newOp: func() executeLogicOfOverload {
					return EnableFaultInjection
				},
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
				newOp: func() executeLogicOfOverload {
					return MoCtl
				},
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
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoEnableMemUsageDetail
				},
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
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoDisableMemUsageDetail
				},
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
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return RemoveFaultPoint
				},
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
				overloadId:      0,
				args:            []types.T{types.T_uint64},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Sleep[uint64]
				},
			},
			{
				overloadId:      0,
				args:            []types.T{types.T_float64},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameter []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Sleep[float64]
				},
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
				overloadId:      0,
				args:            []types.T{types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return TriggerFaultPoint
				},
			},
		},
	},

	// function `LAST_DAY`
	{
		functionId: LAST_DAY,
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
				newOp: func() executeLogicOfOverload {
					return LastDay
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LastDay
				},
			},
		},
	},

	// function `MAKEDATE`
	{
		functionId: MAKEDATE,
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
				newOp: func() executeLogicOfOverload {
					return MakeDateString
				},
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
				newOp: func() executeLogicOfOverload {
					return BuildVersion
				},
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
				overloadId:      0,
				args:            []types.T{types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Charset
				},
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
				overloadId:      0,
				args:            []types.T{types.T_varchar},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return Collation
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return ConnectionID
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentAccountID
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentAccountName
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentRole
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentRoleID
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentUserID
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentRoleName
				},
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
				overloadId:      0,
				args:            []types.T{},
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInCurrentUserName
				},
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
				overloadId:      0,
				args:            nil,
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInDatabase
				},
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
				volatile:   true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return FoundRows
				},
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
				newOp: func() executeLogicOfOverload {
					return GitVersion
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInHash
				},
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
				newOp: func() executeLogicOfOverload {
					return ICULIBVersion
				},
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
				newOp: func() executeLogicOfOverload {
					return iffFn
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalAutoIncrement
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalCharLength
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalCharSize
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalCharacterSet
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalDatetimeScale
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalNumericPrecision
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInInternalNumericScale
				},
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
				volatile:   true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				realTimeRelated: true,
				newOp: func() executeLogicOfOverload {
					return LastInsertID
				},
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
				overloadId:      0,
				args:            []types.T{},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LastQueryIDWithoutParam
				},
			},
			{
				overloadId:      1,
				args:            []types.T{types.T_int64},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LastQueryID
				},
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
				newOp: func() executeLogicOfOverload {
					return LoadFile
				},
			},
			{
				overloadId: 1,
				volatile:   true,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LoadFile
				},
			},
			{
				overloadId: 2,
				volatile:   true,
				args:       []types.T{types.T_datalink},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return LoadFileDatalink
				},
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
				overloadId:      0,
				volatile:        true,
				realTimeRelated: true,
				args:            []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoMemUsage
				},
			},
		},
	},
	// function `mo_memory`
	{
		functionId: MO_MEMORY,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,
		Overloads: []overload{
			{
				overloadId:      0,
				volatile:        true,
				realTimeRelated: true,
				args:            []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoMemory
				},
			},
		},
	},
	// function `mo_cpu`
	{
		functionId: MO_CPU,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,
		Overloads: []overload{
			{
				overloadId:      0,
				volatile:        true,
				realTimeRelated: true,
				args:            []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoCPU
				},
			},
		},
	},
	// function `mo_cpu_dump`
	{
		functionId: MO_CPU_DUMP,
		class:      plan.Function_INTERNAL,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,
		Overloads: []overload{
			{
				overloadId:      0,
				volatile:        true,
				realTimeRelated: true,
				args:            []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return MoCPUDump
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInMoShowVisibleBin
				},
			},
		},
	},

	// function `mo_show_visible_bin_enum`
	{
		functionId: MO_SHOW_VISIBLE_BIN_ENUM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return builtInMoShowVisibleBinEnum
				},
			},
		},
	},

	// function `cast_index_to_value`
	{
		functionId: CAST_INDEX_TO_VALUE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_enum},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CastIndexToValue
				},
			},
		},
	},

	// function `cast_value_to_index`
	{
		functionId: CAST_VALUE_TO_INDEX,
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
					return types.T_enum.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CastValueToIndex
				},
			},
		},
	},

	// function `cast_index_value_to_index`
	{
		functionId: CAST_INDEX_VALUE_TO_INDEX,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_varchar, types.T_uint16},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_enum.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CastIndexValueToIndex
				},
			},
		},
	},

	// function `cast_nano_to_timestamp`
	{
		functionId: CAST_NANO_TO_TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId:      0,
				args:            []types.T{types.T_int64},
				volatile:        true,
				realTimeRelated: true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CastNanoToTimestamp
				},
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
				newOp: func() executeLogicOfOverload {
					return MoTableRows
				},
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
				newOp: func() executeLogicOfOverload {
					return MoTableSize
				},
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
				newOp: func() executeLogicOfOverload {
					return MoTableColMax
				},
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
				newOp: func() executeLogicOfOverload {
					return MoTableColMin
				},
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
				newOp: func() executeLogicOfOverload {
					return RolesGraphml
				},
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
				newOp: func() executeLogicOfOverload {
					return RowCount
				},
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
				newOp: func() executeLogicOfOverload {
					return Nextval
				},
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
				newOp: func() executeLogicOfOverload {
					return Setval
				},
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
				newOp: func() executeLogicOfOverload {
					return Setval
				},
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
				newOp: func() executeLogicOfOverload {
					return Currval
				},
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
				newOp: func() executeLogicOfOverload {
					return Lastval
				},
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
				volatile:   true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return User
				},
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
				newOp: func() executeLogicOfOverload {
					return builtInUUID
				},
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
				newOp: func() executeLogicOfOverload {
					return Values
				},
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
				newOp: func() executeLogicOfOverload {
					return Version
				},
			},
		},
	},

	// function `mo_check_level`
	{
		functionId: MO_CHECH_LEVEL,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						vs := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
						res := vector.MustFunctionResult[bool](result)
						for i := uint64(0); i < uint64(length); i++ {
							flag, isNull := vs.GetValue(i)
							if isNull || !flag {
								return moerr.NewCheckRecursiveLevel(proc.Ctx)
							}
							res.AppendMustValue(true)
						}
						return nil
					}
				},
			},
		},
	},

	// function `assert`
	{
		functionId: ASSERT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 4 {
				if inputs[0].Oid != types.T_bool || !inputs[1].Oid.IsMySQLString() || !inputs[2].Oid.IsMySQLString() || !inputs[3].Oid.IsMySQLString() {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				return newCheckResultWithSuccess(1)
			}

			if len(inputs) == 2 {
				if inputs[0].Oid != types.T_bool || !inputs[1].Oid.IsMySQLString() {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						checkFlags := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
						errMsgs := vector.GenerateFunctionStrParameter(parameters[1])
						value2, null := errMsgs.GetStrValue(0)
						if null {
							return moerr.NewInternalError(proc.Ctx, "the second parameter of assert() should not be null")
						}
						errMsg := functionUtil.QuickBytesToStr(value2)

						res := vector.MustFunctionResult[bool](result)
						for i := uint64(0); i < uint64(length); i++ {
							flag, isNull := checkFlags.GetValue(i)
							if isNull || !flag {
								return moerr.NewInternalError(proc.Ctx, errMsg)
							}
							res.AppendMustValue(true)
						}
						return nil
					}
				},
			},
			{
				overloadId: 1,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						checkFlags := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
						sourceValues := vector.GenerateFunctionStrParameter(parameters[1])
						columnNames := vector.GenerateFunctionStrParameter(parameters[2])
						columnTypes := vector.GenerateFunctionStrParameter(parameters[3])
						// do a safe check
						if columnNames.WithAnyNullValue() {
							return moerr.NewInternalError(proc.Ctx, "the third parameter of assert() should not be null")
						}
						res := vector.MustFunctionResult[bool](result)
						loopLength := uint64(length)

						// bad design.
						castFlag := parameters[1].GetType().Width == types.MaxVarcharLen
						if castFlag {
							for i := uint64(0); i < loopLength; i++ {
								flag, null1 := checkFlags.GetValue(i)
								if null1 || !flag {
									value, null2 := sourceValues.GetStrValue(i)
									col, _ := columnNames.GetStrValue(i)
									coltypes, _ := columnTypes.GetStrValue(i)
									if !null2 {
										tuples, _, schema, err := types.DecodeTuple(value)
										scales := make([]int32, max(len(coltypes), len(schema)))
										for j := range coltypes {
											scales[j] = int32(coltypes[j])
										}
										if err == nil { // complex key
											return moerr.NewDuplicateEntry(proc.Ctx, tuples.ErrString(scales), string(col))
										}
										return moerr.NewDuplicateEntry(proc.Ctx, string(value), string(col))
									}
									return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("column '%s' cannot be null", string(col)))
								}
								res.AppendMustValue(true)
							}
						} else {
							for i := uint64(0); i < loopLength; i++ {
								flag, null1 := checkFlags.GetValue(i)
								if null1 || !flag {
									value, null2 := sourceValues.GetStrValue(i)
									col, _ := columnNames.GetStrValue(i)
									if !null2 {
										return moerr.NewDuplicateEntry(proc.Ctx, string(value), string(col))
									}
									return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("column '%s' cannot be null", string(col)))
								}
								res.AppendMustValue(true)
							}
						}
						return nil
					}
				},
			},
		},
	},

	// function `isempty`
	{
		functionId: ISEMPTY,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) != 1 {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			return newCheckResultWithSuccess(0)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						isEmpty := parameters[0].Length() == 0
						res := vector.MustFunctionResult[bool](result)
						for i := uint64(0); i < uint64(length); i++ {
							res.AppendMustValue(isEmpty)
						}
						return nil
					}
				},
			},
		},
	},

	// function `not_in_rows`
	{
		functionId: NOT_IN_ROWS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) != 2 {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if inputs[0].Oid != types.T_Rowid || inputs[1].Oid != types.T_Rowid {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			return newCheckResultWithSuccess(0)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						leftRow := vector.GenerateFunctionFixedTypeParameter[types.Rowid](parameters[0])
						rightRow := vector.GenerateFunctionFixedTypeParameter[types.Rowid](parameters[1])
						res := vector.MustFunctionResult[bool](result)

						var rightRowIdMap map[types.Rowid]struct{}
						if rightRow.WithAnyNullValue() {
							rightRowIdMap = make(map[types.Rowid]struct{})
						} else {
							// XXX not sure, but row_id may not be duplicated I think.
							rightRowIdMap = make(map[types.Rowid]struct{}, length)
						}

						loopLength := uint64(length)
						for i := uint64(0); i < loopLength; i++ {
							rightRowId, isNull := rightRow.GetValue(i)
							if !isNull {
								rightRowIdMap[rightRowId] = struct{}{}
							}
						}

						for i := uint64(0); i < loopLength; i++ {
							leftRowId, isNull := leftRow.GetValue(i)
							notInRows := false
							if !isNull {
								if _, ok := rightRowIdMap[leftRowId]; !ok {
									notInRows = true
								}
							}
							res.AppendMustValue(notInRows)
						}
						return nil
					}
				},
			},
		},
	},

	// function `python_user_defined_function`
	{
		functionId: PYTHON_UDF,
		class:      plan.Function_INTERNAL | plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    checkPythonUdf,
		Overloads: []overload{
			{
				overloadId: 0,
				retType:    pythonUdfRetType,
				newOp: func() executeLogicOfOverload {
					return runPythonUdf
				},
			},
		},
	},

	// function `BITMAP_BIT_POSITION`
	{
		functionId: BITMAP_BIT_POSITION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return BitmapBitPosition
				},
			},
		},
	},

	// function `BITMAP_BUCKET_NUMBER`
	{
		functionId: BITMAP_BUCKET_NUMBER,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return BitmapBucketNumber
				},
			},
		},
	},

	// function `BITMAP_COUNT`
	{
		functionId: BITMAP_COUNT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return BitmapCount
				},
			},
		},
	},

	// function `SHA1`
	{
		functionId: SHA1,
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
				newOp: func() executeLogicOfOverload {
					return SHA1Func
				},
			},
		},
	},
}

func MoCtl(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) (err error) {
	return ctl.MoCtl(ivecs, result, proc, length)
}
