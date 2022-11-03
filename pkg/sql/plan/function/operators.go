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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func initOperators() {
	var err error

	for fid, fs := range operators {
		err = appendFunction(fid, fs)
		if err != nil {
			panic(err)
		}
	}
}

// operators contains the operator function indexed by function id.
var operators = map[int]Functions{
	ISTRUE: {
		Id:     ISTRUE,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsTrue,
			},
		},
	},
	ISNOTTRUE: {
		Id:     ISNOTTRUE,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotTrue,
			},
		},
	},
	ISFALSE: {
		Id:     ISFALSE,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsFalse,
			},
		},
	},
	ISNOTFALSE: {
		Id:     ISNOTFALSE,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotFalse,
			},
		},
	},
	// is null operator
	ISNULL: {
		Id:     ISNULL,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_json,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNull,
			},
		},
	},

	ISNOTNULL: {
		Id:     ISNOTNULL,
		Flag:   plan.Function_STRICT,
		Layout: IS_NULL_EXPRESSION,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_json,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNotNull,
			},
		},
	},
	// comparison operator
	IS: {
		Id:     IS,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.Is,
			},
		},
	},
	REG_MATCH: {
		Id:     REG_MATCH,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.RegMatch,
			},
		},
	},
	NOT_REG_MATCH: {
		Id:     NOT_REG_MATCH,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NotRegMatch,
			},
		},
	},
	OP_BIT_XOR: {
		Id:     OP_BIT_XOR,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_int64,
				Fn:        operator.OpBitXorFun[int64],
			},
		},
	},

	OP_BIT_OR: {
		Id:     OP_BIT_OR,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_int64,
				Fn:        operator.OpBitOrFun[int64],
			},
		},
	},

	OP_BIT_AND: {
		Id:     OP_BIT_AND,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_int64,
				Fn:        operator.OpBitAndFun[int64],
			},
		},
	},

	OP_BIT_SHIFT_RIGHT: {
		Id:     OP_BIT_SHIFT_RIGHT,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_int64,
				Fn:        operator.OpBitRightShiftFun[int64],
			},
		},
	},

	OP_BIT_SHIFT_LEFT: {
		Id:     OP_BIT_SHIFT_LEFT,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_int64,
				Fn:        operator.OpBitLeftShiftFun[int64],
			},
		},
	},

	ISNOT: {
		Id:     ISNOT,
		Flag:   plan.Function_STRICT,
		Layout: COMPARISON_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.IsNot,
			},
		},
	},

	EQUAL: {
		Id:          EQUAL,
		Flag:        plan.Function_STRICT,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[uint32],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.EqGeneral[types.Time],
			},
		},
	},

	GREAT_THAN: {
		Id:          GREAT_THAN,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[uint32],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GtGeneral[types.Time],
			},
		},
	},

	GREAT_EQUAL: {
		Id:          GREAT_EQUAL,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[uint32],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.GeGeneral[types.Time],
			},
		},
	},

	LESS_THAN: {
		Id:          LESS_THAN,
		Flag:        plan.Function_STRICT,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[uint32],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LtGeneral[types.Time],
			},
		},
	},

	LESS_EQUAL: {
		Id:          LESS_EQUAL,
		Flag:        plan.Function_STRICT,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[uint32],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LeGeneral[types.Time],
			},
		},
	},

	NOT_EQUAL: {
		Id:          NOT_EQUAL,
		Flag:        plan.Function_STRICT,
		Layout:      COMPARISON_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[uint8],
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[uint16],
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[uint16],
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[uint64],
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[int8],
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[int16],
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[int32],
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[int64],
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[float32],
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[float64],
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeDecimal64,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeDecimal128,
			},
			{
				Index: 12,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeString,
			},
			{
				Index: 13,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeString,
			},
			{
				Index: 14,
				Args: []types.T{
					types.T_date,
					types.T_date,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[types.Date],
			},
			{
				Index: 15,
				Args: []types.T{
					types.T_datetime,
					types.T_datetime,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[types.Datetime],
			},
			{
				Index: 16,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[bool],
			},
			{
				Index: 17,
				Args: []types.T{
					types.T_timestamp,
					types.T_timestamp,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[types.Timestamp],
			},
			{
				Index: 18,
				Args: []types.T{
					types.T_blob,
					types.T_blob,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeString,
			},
			{
				Index: 19,
				Args: []types.T{
					types.T_uuid,
					types.T_uuid,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeUuid,
			},
			{
				Index: 20,
				Args: []types.T{
					types.T_text,
					types.T_text,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeString,
			},
			{
				Index: 21,
				Args: []types.T{
					types.T_time,
					types.T_time,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.NeGeneral[types.Time],
			},
		},
	},

	LIKE: {
		Id:     LIKE,
		Flag:   plan.Function_STRICT,
		Layout: BINARY_LOGICAL_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.Like,
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.Like,
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_char,
					types.T_char,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.Like,
			},
		},
	},

	BETWEEN: {
		Id:          BETWEEN,
		Flag:        plan.Function_STRICT,
		Layout:      BETWEEN_AND_EXPRESSION,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_uint8,
					types.T_uint8,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 1,
				Args: []types.T{
					types.T_uint16,
					types.T_uint16,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 2,
				Args: []types.T{
					types.T_uint32,
					types.T_uint32,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 3,
				Args: []types.T{
					types.T_uint64,
					types.T_uint64,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 4,
				Args: []types.T{
					types.T_int8,
					types.T_int8,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 5,
				Args: []types.T{
					types.T_int16,
					types.T_int16,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 6,
				Args: []types.T{
					types.T_int32,
					types.T_int32,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 7,
				Args: []types.T{
					types.T_int64,
					types.T_int64,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 8,
				Args: []types.T{
					types.T_float32,
					types.T_float32,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 9,
				Args: []types.T{
					types.T_float64,
					types.T_float64,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 10,
				Args: []types.T{
					types.T_decimal64,
					types.T_decimal64,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
			{
				Index: 11,
				Args: []types.T{
					types.T_decimal128,
					types.T_decimal128,
				},
				ReturnTyp: types.T_bool,
				Fn:        nil,
			},
		},
	},

	IN: {
		Id:     IN,
		Flag:   plan.Function_STRICT,
		Layout: IN_PREDICATE,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 2 && inputs[1] == types.T_tuple {
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				ReturnTyp: types.T_bool,
			},
		},
	},

	EXISTS: {
		Id:     EXISTS,
		Flag:   plan.Function_STRICT,
		Layout: EXISTS_ANY_PREDICATE,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				ReturnTyp: types.T_bool,
			},
		},
	},

	// logic operator
	AND: {
		Id:     AND,
		Flag:   plan.Function_STRICT,
		Layout: BINARY_LOGICAL_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LogicAnd,
			},
		},
	},

	OR: {
		Id:     OR,
		Flag:   plan.Function_STRICT,
		Layout: BINARY_LOGICAL_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LogicOr,
			},
		},
	},

	XOR: {
		Id:     XOR,
		Flag:   plan.Function_STRICT,
		Layout: BINARY_LOGICAL_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LogicXor,
			},
		},
	},

	NOT: {
		Id:     NOT,
		Flag:   plan.Function_STRICT,
		Layout: UNARY_LOGICAL_OPERATOR,
		Overloads: []Function{
			{
				Index: 0,
				Args: []types.T{
					types.T_bool,
				},
				ReturnTyp: types.T_bool,
				Fn:        operator.LogicNot,
			},
		},
	},

	// arithmetic operator
	PLUS: {
		Id:          PLUS,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.PlusUint[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.PlusUint[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.PlusUint[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.PlusUint[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.PlusInt[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.PlusInt[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.PlusInt[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.PlusInt[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.PlusFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.PlusFloat[float64],
			},
			{
				Index:     10,
				Args:      []types.T{types.T_decimal64, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.PlusDecimal64,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_decimal128, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.PlusDecimal128,
			},
			{
				Index:     12,
				Args:      []types.T{types.T_date, types.T_interval},
				ReturnTyp: types.T_date,
				Fn:        nil,
			},
		},
	},

	MINUS: {
		Id:          MINUS,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.MinusUint[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.MinusUint[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.MinusUint[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.MinusUint[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.MinusInt[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.MinusInt[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.MinusInt[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.MinusInt[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.MinusFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.MinusFloat[float64],
			},
			{
				Index:     10,
				Args:      []types.T{types.T_decimal64, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.MinusDecimal64,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_decimal128, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.MinusDecimal128,
			},
			{
				Index:     12,
				Args:      []types.T{types.T_date, types.T_date},
				ReturnTyp: types.T_int64,
				Fn:        binary.DateDiff,
			},
			{
				Index:     13,
				Args:      []types.T{types.T_datetime, types.T_datetime},
				ReturnTyp: types.T_int64,
				Fn:        operator.MinusDatetime,
			},
		},
	},

	MULTI: {
		Id:          MULTI,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.MultUint[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.MultUint[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.MultUint[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.MultUint[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.MultInt[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.MultInt[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.MultInt[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.MultInt[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.MultFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.MultFloat[float64],
			},
			{
				Index:     10,
				Args:      []types.T{types.T_decimal64, types.T_decimal64},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.MultDecimal64,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_decimal128, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.MultDecimal128,
			},
		},
	},

	DIV: {
		Id:          DIV,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn2,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.DivFloat[float32],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.DivFloat[float64],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_decimal64, types.T_decimal64},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.DivDecimal64,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_decimal128, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.DivDecimal128,
			},
		},
	},

	INTEGER_DIV: {
		Id:          INTEGER_DIV,
		Flag:        plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn2,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_int64,
				Fn:        operator.IntegerDivFloat[float32],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_int64,
				Fn:        operator.IntegerDivFloat[float64],
			},
		},
	},

	MOD: {
		Id:          MOD,
		Flag:        plan.Function_STRICT,
		Layout:      BINARY_ARITHMETIC_OPERATOR,
		TypeCheckFn: GeneralBinaryOperatorTypeCheckFn1,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.ModUint[uint8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.ModUint[uint16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.ModUint[uint32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.ModUint[uint64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.ModInt[int8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.ModInt[int16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.ModInt[int32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.ModInt[int64],
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.ModFloat[float32],
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.ModFloat[float64],
			},
		},
	},

	UNARY_PLUS: {
		Id:     UNARY_PLUS,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout: UNARY_ARITHMETIC_OPERATOR,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]uint8)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     1,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]uint16)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     2,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]uint32)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     3,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]uint64)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     4,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_int8,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]int8)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     5,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_int16,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]int16)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     6,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_int32,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]int32)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     7,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]int64)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_float32,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]float32)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]float64)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     10,
				Args:      []types.T{types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]types.Decimal64)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
			{
				Index:     11,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
					data := vs[0].Col.([]types.Decimal128)
					vec := vector.NewConstFixed(vs[0].Typ, vs[0].Length(), data[0], proc.Mp())
					return vec, nil
				},
			},
		},
	},

	UNARY_MINUS: {
		Id:     UNARY_MINUS,
		Flag:   plan.Function_STRICT | plan.Function_MONOTONICAL,
		Layout: UNARY_ARITHMETIC_OPERATOR,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.UnaryMinus[int8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.UnaryMinus[int16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.UnaryMinus[int32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.UnaryMinus[int64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.UnaryMinus[float32],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.UnaryMinus[float64],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.UnaryMinusDecimal64,
			},
			{
				Index:     7,
				Args:      []types.T{types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.UnaryMinusDecimal128,
			},
		},
	},

	UNARY_TILDE: {
		Id:     UNARY_TILDE,
		Flag:   plan.Function_STRICT,
		Layout: UNARY_ARITHMETIC_OPERATOR,
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_int8},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[int8],
			},
			{
				Index:     1,
				Args:      []types.T{types.T_int16},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[int16],
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int32},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[int32],
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[int64],
			},
			{
				Index:     4,
				Args:      []types.T{types.T_uint8},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[uint8],
			},
			{
				Index:     5,
				Args:      []types.T{types.T_uint16},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[uint16],
			},
			{
				Index:     6,
				Args:      []types.T{types.T_uint32},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[uint32],
			},
			{
				Index:     7,
				Args:      []types.T{types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.UnaryTilde[uint64],
			},
		},
	},

	// others
	CAST: {
		Id:     CAST,
		Flag:   plan.Function_STRICT,
		Layout: CAST_EXPRESSION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			// cast-operator should check param types strictly
			if len(inputs) == 2 {
				for i, o := range overloads {
					if o.Args[0] == inputs[0] && o.Args[1] == inputs[1] {
						return int32(i), nil
					}
				}
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Args:      []types.T{types.T_int8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     1,
				Args:      []types.T{types.T_int16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     2,
				Args:      []types.T{types.T_int32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     3,
				Args:      []types.T{types.T_int64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     4,
				Args:      []types.T{types.T_uint8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     5,
				Args:      []types.T{types.T_uint16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     6,
				Args:      []types.T{types.T_uint32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     7,
				Args:      []types.T{types.T_uint64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     8,
				Args:      []types.T{types.T_float32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     9,
				Args:      []types.T{types.T_float64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     10,
				Args:      []types.T{types.T_date, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     11,
				Args:      []types.T{types.T_datetime, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     12,
				Args:      []types.T{types.T_timestamp, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     13,
				Args:      []types.T{types.T_int16, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     14,
				Args:      []types.T{types.T_int32, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     15,
				Args:      []types.T{types.T_int64, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     16,
				Args:      []types.T{types.T_uint8, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     17,
				Args:      []types.T{types.T_uint16, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     18,
				Args:      []types.T{types.T_uint32, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     19,
				Args:      []types.T{types.T_uint64, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     20,
				Args:      []types.T{types.T_float32, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     21,
				Args:      []types.T{types.T_float64, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     22,
				Args:      []types.T{types.T_int8, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     23,
				Args:      []types.T{types.T_int32, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     24,
				Args:      []types.T{types.T_int64, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     25,
				Args:      []types.T{types.T_uint8, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     26,
				Args:      []types.T{types.T_uint16, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     27,
				Args:      []types.T{types.T_uint32, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     28,
				Args:      []types.T{types.T_uint64, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     29,
				Args:      []types.T{types.T_float32, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     30,
				Args:      []types.T{types.T_float64, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     31,
				Args:      []types.T{types.T_int8, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     32,
				Args:      []types.T{types.T_int16, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     33,
				Args:      []types.T{types.T_int64, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     34,
				Args:      []types.T{types.T_uint8, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     35,
				Args:      []types.T{types.T_uint16, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     36,
				Args:      []types.T{types.T_uint32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     37,
				Args:      []types.T{types.T_uint64, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     38,
				Args:      []types.T{types.T_float32, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     39,
				Args:      []types.T{types.T_float64, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     40,
				Args:      []types.T{types.T_int8, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     41,
				Args:      []types.T{types.T_int16, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     42,
				Args:      []types.T{types.T_int32, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     43,
				Args:      []types.T{types.T_uint8, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     44,
				Args:      []types.T{types.T_uint16, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     45,
				Args:      []types.T{types.T_uint32, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     46,
				Args:      []types.T{types.T_uint64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     47,
				Args:      []types.T{types.T_float32, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     48,
				Args:      []types.T{types.T_float64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     49,
				Args:      []types.T{types.T_int8, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     50,
				Args:      []types.T{types.T_int16, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     51,
				Args:      []types.T{types.T_int32, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     52,
				Args:      []types.T{types.T_int64, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     53,
				Args:      []types.T{types.T_uint16, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     54,
				Args:      []types.T{types.T_uint32, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     55,
				Args:      []types.T{types.T_uint64, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     56,
				Args:      []types.T{types.T_float32, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     57,
				Args:      []types.T{types.T_float64, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     58,
				Args:      []types.T{types.T_int8, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     59,
				Args:      []types.T{types.T_int16, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     60,
				Args:      []types.T{types.T_int32, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     61,
				Args:      []types.T{types.T_int64, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     62,
				Args:      []types.T{types.T_uint8, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     63,
				Args:      []types.T{types.T_uint32, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     64,
				Args:      []types.T{types.T_uint64, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     65,
				Args:      []types.T{types.T_float32, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     66,
				Args:      []types.T{types.T_float64, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     67,
				Args:      []types.T{types.T_int8, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     68,
				Args:      []types.T{types.T_int16, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     69,
				Args:      []types.T{types.T_int32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     70,
				Args:      []types.T{types.T_int64, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     71,
				Args:      []types.T{types.T_uint8, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     72,
				Args:      []types.T{types.T_uint16, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     73,
				Args:      []types.T{types.T_uint64, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     74,
				Args:      []types.T{types.T_float32, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     75,
				Args:      []types.T{types.T_float64, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     76,
				Args:      []types.T{types.T_int8, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     77,
				Args:      []types.T{types.T_int16, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     78,
				Args:      []types.T{types.T_int32, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     79,
				Args:      []types.T{types.T_int64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     80,
				Args:      []types.T{types.T_uint8, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     81,
				Args:      []types.T{types.T_uint16, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     82,
				Args:      []types.T{types.T_uint32, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     83,
				Args:      []types.T{types.T_float32, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     84,
				Args:      []types.T{types.T_float64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     85,
				Args:      []types.T{types.T_int8, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     86,
				Args:      []types.T{types.T_int16, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     87,
				Args:      []types.T{types.T_int32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     88,
				Args:      []types.T{types.T_int64, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     89,
				Args:      []types.T{types.T_uint8, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     90,
				Args:      []types.T{types.T_uint16, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     91,
				Args:      []types.T{types.T_uint32, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     92,
				Args:      []types.T{types.T_uint64, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     93,
				Args:      []types.T{types.T_float64, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     94,
				Args:      []types.T{types.T_int8, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     95,
				Args:      []types.T{types.T_int16, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     96,
				Args:      []types.T{types.T_int32, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     97,
				Args:      []types.T{types.T_int64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     98,
				Args:      []types.T{types.T_uint8, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     99,
				Args:      []types.T{types.T_uint16, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     100,
				Args:      []types.T{types.T_uint32, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     101,
				Args:      []types.T{types.T_uint64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     102,
				Args:      []types.T{types.T_float32, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     103,
				Args:      []types.T{types.T_char, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     104,
				Args:      []types.T{types.T_varchar, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     105,
				Args:      []types.T{types.T_char, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     106,
				Args:      []types.T{types.T_varchar, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     107,
				Args:      []types.T{types.T_char, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     108,
				Args:      []types.T{types.T_varchar, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     109,
				Args:      []types.T{types.T_char, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     110,
				Args:      []types.T{types.T_varchar, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     111,
				Args:      []types.T{types.T_char, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     112,
				Args:      []types.T{types.T_varchar, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     113,
				Args:      []types.T{types.T_char, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     114,
				Args:      []types.T{types.T_varchar, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     115,
				Args:      []types.T{types.T_char, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     116,
				Args:      []types.T{types.T_varchar, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     117,
				Args:      []types.T{types.T_char, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     118,
				Args:      []types.T{types.T_varchar, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     119,
				Args:      []types.T{types.T_char, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     120,
				Args:      []types.T{types.T_varchar, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     121,
				Args:      []types.T{types.T_char, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     122,
				Args:      []types.T{types.T_varchar, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     123,
				Args:      []types.T{types.T_int8, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     124,
				Args:      []types.T{types.T_int8, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     125,
				Args:      []types.T{types.T_int16, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     126,
				Args:      []types.T{types.T_int16, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     127,
				Args:      []types.T{types.T_int32, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     128,
				Args:      []types.T{types.T_int32, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     129,
				Args:      []types.T{types.T_int64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     130,
				Args:      []types.T{types.T_int64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     131,
				Args:      []types.T{types.T_uint8, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     132,
				Args:      []types.T{types.T_uint8, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     133,
				Args:      []types.T{types.T_uint16, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     134,
				Args:      []types.T{types.T_uint16, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     135,
				Args:      []types.T{types.T_uint32, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     136,
				Args:      []types.T{types.T_uint32, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     137,
				Args:      []types.T{types.T_uint64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     138,
				Args:      []types.T{types.T_uint64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     139,
				Args:      []types.T{types.T_float32, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     140,
				Args:      []types.T{types.T_float32, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     141,
				Args:      []types.T{types.T_float64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     142,
				Args:      []types.T{types.T_float64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     143,
				Args:      []types.T{types.T_char, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     144,
				Args:      []types.T{types.T_varchar, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     145,
				Args:      []types.T{types.T_char, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     146,
				Args:      []types.T{types.T_varchar, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     147,
				Args:      []types.T{types.T_int8, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     148,
				Args:      []types.T{types.T_int16, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     149,
				Args:      []types.T{types.T_int32, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     150,
				Args:      []types.T{types.T_int64, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     151,
				Args:      []types.T{types.T_varchar, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     152,
				Args:      []types.T{types.T_varchar, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     153,
				Volatile:  true,
				Args:      []types.T{types.T_varchar, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     154,
				Args:      []types.T{types.T_decimal64, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     155,
				Args:      []types.T{types.T_decimal64, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     156,
				Args:      []types.T{types.T_decimal128, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     157,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     158,
				Args:      []types.T{types.T_varchar, types.T_interval},
				ReturnTyp: types.T_interval,
				Fn:        nil,
			},
			{
				Index:     159,
				Args:      []types.T{types.T_uint8, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     160,
				Args:      []types.T{types.T_uint16, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     161,
				Args:      []types.T{types.T_uint32, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     162,
				Args:      []types.T{types.T_uint64, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     163,
				Args:      []types.T{types.T_varchar, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     164,
				Args:      []types.T{types.T_char, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     165,
				Args:      []types.T{types.T_varchar, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     166,
				Args:      []types.T{types.T_char, types.T_decimal128},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     167,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_varchar},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     168,
				Args:      []types.T{types.T_date, types.T_char},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     169,
				Args:      []types.T{types.T_date, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     170,
				Args:      []types.T{types.T_datetime, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     171,
				Args:      []types.T{types.T_datetime, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     172,
				Args:      []types.T{types.T_bool, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     173,
				Args:      []types.T{types.T_bool, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     174,
				Args:      []types.T{types.T_date, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     175,
				Args:      []types.T{types.T_datetime, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     176,
				Args:      []types.T{types.T_float32, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     177,
				Args:      []types.T{types.T_float64, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     178,
				Args:      []types.T{types.T_float64, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     179,
				Args:      []types.T{types.T_float32, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     180,
				Volatile:  true,
				Args:      []types.T{types.T_datetime, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     181,
				Volatile:  true,
				Args:      []types.T{types.T_date, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     182,
				Args:      []types.T{types.T_decimal64, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     183,
				Args:      []types.T{types.T_decimal128, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     184,
				Args:      []types.T{types.T_decimal64, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     185,
				Args:      []types.T{types.T_decimal128, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     186,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     187,
				Args:      []types.T{types.T_int8, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     188,
				Args:      []types.T{types.T_int16, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     189,
				Args:      []types.T{types.T_int32, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     190,
				Args:      []types.T{types.T_int64, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     191,
				Args:      []types.T{types.T_uint8, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     192,
				Args:      []types.T{types.T_uint16, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     193,
				Args:      []types.T{types.T_uint32, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     194,
				Args:      []types.T{types.T_uint64, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     195,
				Args:      []types.T{types.T_decimal64, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     196,
				Args:      []types.T{types.T_decimal128, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     197,
				Args:      []types.T{types.T_decimal64, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     198,
				Args:      []types.T{types.T_decimal128, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     199,
				Args:      []types.T{types.T_decimal64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     200,
				Args:      []types.T{types.T_decimal128, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     201,
				Args:      []types.T{types.T_char, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     202,
				Args:      []types.T{types.T_char, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     203,
				Volatile:  true,
				Args:      []types.T{types.T_char, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     204,
				Args:      []types.T{types.T_int8, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     205,
				Args:      []types.T{types.T_int16, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     206,
				Args:      []types.T{types.T_int32, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     207,
				Args:      []types.T{types.T_int64, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     208,
				Args:      []types.T{types.T_int8, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     209,
				Args:      []types.T{types.T_int16, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     210,
				Args:      []types.T{types.T_int32, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     211,
				Args:      []types.T{types.T_int64, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     212,
				Args:      []types.T{types.T_uint8, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     213,
				Args:      []types.T{types.T_uint16, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     214,
				Args:      []types.T{types.T_uint32, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     215,
				Args:      []types.T{types.T_uint64, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     216,
				Args:      []types.T{types.T_float32, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     217,
				Args:      []types.T{types.T_float64, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     218,
				Args:      []types.T{types.T_char, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     219,
				Args:      []types.T{types.T_varchar, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     220,
				Args:      []types.T{types.T_decimal64, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     221,
				Args:      []types.T{types.T_decimal128, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     222,
				Args:      []types.T{types.T_decimal64, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     223,
				Args:      []types.T{types.T_decimal128, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     224,
				Args:      []types.T{types.T_decimal64, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     225,
				Args:      []types.T{types.T_decimal128, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     226,
				Args:      []types.T{types.T_decimal128, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     227,
				Args:      []types.T{types.T_blob, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},

			{
				Index:     228,
				Args:      []types.T{types.T_blob, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     229,
				Args:      []types.T{types.T_blob, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     230,
				Args:      []types.T{types.T_blob, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     231,
				Args:      []types.T{types.T_blob, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     232,
				Args:      []types.T{types.T_blob, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     233,
				Args:      []types.T{types.T_blob, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     234,
				Args:      []types.T{types.T_blob, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     235,
				Args:      []types.T{types.T_blob, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     236,
				Args:      []types.T{types.T_blob, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     237,
				Args:      []types.T{types.T_int8, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     238,
				Args:      []types.T{types.T_int16, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     239,
				Args:      []types.T{types.T_int32, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     240,
				Args:      []types.T{types.T_int64, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     241,
				Args:      []types.T{types.T_uint8, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     242,
				Args:      []types.T{types.T_uint16, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     243,
				Args:      []types.T{types.T_uint32, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     244,
				Args:      []types.T{types.T_uint64, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     245,
				Args:      []types.T{types.T_float32, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     246,
				Args:      []types.T{types.T_float64, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     247,
				Args:      []types.T{types.T_varchar, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     248,
				Args:      []types.T{types.T_blob, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     249,
				Args:      []types.T{types.T_char, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     250,
				Args:      []types.T{types.T_blob, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     251,
				Args:      []types.T{types.T_blob, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     252,
				Args:      []types.T{types.T_blob, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     253,
				Args:      []types.T{types.T_blob, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     254,
				Volatile:  true,
				Args:      []types.T{types.T_blob, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     255,
				Args:      []types.T{types.T_blob, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     256,
				Args:      []types.T{types.T_blob, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     257,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     258,
				Args:      []types.T{types.T_date, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     259,
				Args:      []types.T{types.T_datetime, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     260,
				Args:      []types.T{types.T_bool, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     261,
				Args:      []types.T{types.T_decimal64, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     262,
				Args:      []types.T{types.T_decimal128, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     263,
				Args:      []types.T{types.T_blob, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     264,
				Args:      []types.T{types.T_varchar, types.T_json},
				ReturnTyp: types.T_json,
				Fn:        operator.Cast,
			},
			{
				Index:     265,
				Args:      []types.T{types.T_decimal128, types.T_int32},
				ReturnTyp: types.T_json,
				Fn:        operator.Cast,
			},
			{
				Index:     266,
				Args:      []types.T{types.T_bool, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},
			{
				Index:     267,
				Args:      []types.T{types.T_bool, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     268,
				Args:      []types.T{types.T_bool, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     269,
				Args:      []types.T{types.T_bool, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     270,
				Args:      []types.T{types.T_bool, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     271,
				Args:      []types.T{types.T_bool, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     272,
				Args:      []types.T{types.T_bool, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     273,
				Args:      []types.T{types.T_bool, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     274,
				Args:      []types.T{types.T_uuid, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     275,
				Args:      []types.T{types.T_uuid, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     276,
				Args:      []types.T{types.T_char, types.T_uuid},
				ReturnTyp: types.T_uuid,
				Fn:        operator.Cast,
			},
			{
				Index:     277,
				Args:      []types.T{types.T_varchar, types.T_uuid},
				ReturnTyp: types.T_uuid,
				Fn:        operator.Cast,
			},
			{
				Index:     278,
				Args:      []types.T{types.T_text, types.T_int8},
				ReturnTyp: types.T_int8,
				Fn:        operator.Cast,
			},

			{
				Index:     279,
				Args:      []types.T{types.T_text, types.T_int16},
				ReturnTyp: types.T_int16,
				Fn:        operator.Cast,
			},
			{
				Index:     280,
				Args:      []types.T{types.T_text, types.T_int32},
				ReturnTyp: types.T_int32,
				Fn:        operator.Cast,
			},
			{
				Index:     281,
				Args:      []types.T{types.T_text, types.T_int64},
				ReturnTyp: types.T_int64,
				Fn:        operator.Cast,
			},
			{
				Index:     282,
				Args:      []types.T{types.T_text, types.T_uint8},
				ReturnTyp: types.T_uint8,
				Fn:        operator.Cast,
			},
			{
				Index:     283,
				Args:      []types.T{types.T_text, types.T_uint16},
				ReturnTyp: types.T_uint16,
				Fn:        operator.Cast,
			},
			{
				Index:     284,
				Args:      []types.T{types.T_text, types.T_uint32},
				ReturnTyp: types.T_uint32,
				Fn:        operator.Cast,
			},
			{
				Index:     285,
				Args:      []types.T{types.T_text, types.T_uint64},
				ReturnTyp: types.T_uint64,
				Fn:        operator.Cast,
			},
			{
				Index:     286,
				Args:      []types.T{types.T_text, types.T_float32},
				ReturnTyp: types.T_float32,
				Fn:        operator.Cast,
			},
			{
				Index:     287,
				Args:      []types.T{types.T_text, types.T_float64},
				ReturnTyp: types.T_float64,
				Fn:        operator.Cast,
			},
			{
				Index:     288,
				Args:      []types.T{types.T_int8, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     289,
				Args:      []types.T{types.T_int16, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     290,
				Args:      []types.T{types.T_int32, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     291,
				Args:      []types.T{types.T_int64, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     292,
				Args:      []types.T{types.T_uint8, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     293,
				Args:      []types.T{types.T_uint16, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     294,
				Args:      []types.T{types.T_uint32, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     295,
				Args:      []types.T{types.T_uint64, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     296,
				Args:      []types.T{types.T_float32, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     297,
				Args:      []types.T{types.T_float64, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     298,
				Args:      []types.T{types.T_varchar, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     299,
				Args:      []types.T{types.T_text, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     300,
				Args:      []types.T{types.T_char, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     301,
				Args:      []types.T{types.T_text, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     302,
				Args:      []types.T{types.T_text, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     303,
				Args:      []types.T{types.T_text, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     304,
				Args:      []types.T{types.T_text, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     305,
				Volatile:  true,
				Args:      []types.T{types.T_text, types.T_timestamp},
				ReturnTyp: types.T_timestamp,
				Fn:        operator.Cast,
			},
			{
				Index:     306,
				Args:      []types.T{types.T_text, types.T_decimal64},
				ReturnTyp: types.T_decimal64,
				Fn:        operator.Cast,
			},
			{
				Index:     307,
				Args:      []types.T{types.T_text, types.T_decimal128},
				ReturnTyp: types.T_decimal128,
				Fn:        operator.Cast,
			},
			{
				Index:     308,
				Volatile:  true,
				Args:      []types.T{types.T_timestamp, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     309,
				Args:      []types.T{types.T_date, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     310,
				Args:      []types.T{types.T_datetime, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     311,
				Args:      []types.T{types.T_bool, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     312,
				Args:      []types.T{types.T_decimal64, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     313,
				Args:      []types.T{types.T_decimal128, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     314,
				Args:      []types.T{types.T_text, types.T_bool},
				ReturnTyp: types.T_bool,
				Fn:        operator.Cast,
			},
			{
				Index:     315,
				Args:      []types.T{types.T_text, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
			{
				Index:     316,
				Args:      []types.T{types.T_blob, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     317,
				Args:      []types.T{types.T_time, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     318,
				Args:      []types.T{types.T_date, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     319,
				Args:      []types.T{types.T_time, types.T_date},
				ReturnTyp: types.T_date,
				Fn:        operator.Cast,
			},
			{
				Index:     320,
				Args:      []types.T{types.T_time, types.T_datetime},
				ReturnTyp: types.T_datetime,
				Fn:        operator.Cast,
			},
			{
				Index:     321,
				Args:      []types.T{types.T_datetime, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     322,
				Args:      []types.T{types.T_char, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     323,
				Args:      []types.T{types.T_varchar, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     324,
				Args:      []types.T{types.T_text, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     325,
				Args:      []types.T{types.T_blob, types.T_time},
				ReturnTyp: types.T_time,
				Fn:        operator.Cast,
			},
			{
				Index:     326,
				Args:      []types.T{types.T_time, types.T_char},
				ReturnTyp: types.T_char,
				Fn:        operator.Cast,
			},
			{
				Index:     327,
				Args:      []types.T{types.T_time, types.T_varchar},
				ReturnTyp: types.T_varchar,
				Fn:        operator.Cast,
			},
			{
				Index:     328,
				Args:      []types.T{types.T_time, types.T_text},
				ReturnTyp: types.T_text,
				Fn:        operator.Cast,
			},
			{
				Index:     329,
				Args:      []types.T{types.T_time, types.T_blob},
				ReturnTyp: types.T_blob,
				Fn:        operator.Cast,
			},
		},
	},

	COALESCE: {
		Id:     COALESCE,
		Flag:   plan.Function_NONE,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			l := len(inputs)
			if l == 0 {
				return wrongFunctionParameters, nil
			}

			for i, o := range overloads {
				if operator.CoalesceTypeCheckFn(inputs, nil, o.ReturnTyp) {
					return int32(i), nil
				}
			}

			minCost, minIndex := math.MaxInt32, -1
			convertTypes := make([]types.T, l)
			targetTypes := make([]types.T, l)

			for i, o := range overloads {
				for j := 0; j < l; j++ {
					targetTypes[j] = o.ReturnTyp
				}
				if code, c := tryToMatch(inputs, targetTypes); code == matchedByConvert {
					if c < minCost {
						minCost = c
						copy(convertTypes, targetTypes)
						minIndex = i
					}
				}
			}
			if minIndex != -1 {
				return int32(minIndex), convertTypes
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				ReturnTyp: types.T_varchar,
				Fn:        operator.CoalesceVarchar,
			},
			{
				Index:     1,
				Volatile:  true,
				ReturnTyp: types.T_char,
				Fn:        operator.CoalesceChar,
			},
			{
				Index:     2,
				Volatile:  true,
				ReturnTyp: types.T_int8,
				Fn:        operator.CoalesceInt8,
			},
			{
				Index:     3,
				Volatile:  true,
				ReturnTyp: types.T_int16,
				Fn:        operator.CoalesceInt16,
			},
			{
				Index:     4,
				Volatile:  true,
				ReturnTyp: types.T_int32,
				Fn:        operator.CoalesceInt32,
			},
			{
				Index:     5,
				Volatile:  true,
				ReturnTyp: types.T_int64,
				Fn:        operator.CoalesceInt64,
			},
			{
				Index:     6,
				Volatile:  true,
				ReturnTyp: types.T_uint8,
				Fn:        operator.CoalesceUint8,
			},
			{
				Index:     7,
				Volatile:  true,
				ReturnTyp: types.T_uint16,
				Fn:        operator.CoalesceUint16,
			},
			{
				Index:     8,
				Volatile:  true,
				ReturnTyp: types.T_uint32,
				Fn:        operator.CoalesceUint32,
			},
			{
				Index:     9,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        operator.CoalesceUint64,
			},
			{
				Index:     10,
				Volatile:  true,
				ReturnTyp: types.T_float32,
				Fn:        operator.CoalesceFloat32,
			},
			{
				Index:     11,
				Volatile:  true,
				ReturnTyp: types.T_float64,
				Fn:        operator.CoalesceFloat64,
			},
			{
				Index:     12,
				Volatile:  true,
				ReturnTyp: types.T_bool,
				Fn:        operator.CoalesceBool,
			},
			{
				Index:     13,
				Volatile:  true,
				ReturnTyp: types.T_datetime,
				Fn:        operator.CoalesceDateTime,
			},
			{
				Index:     14,
				Volatile:  true,
				ReturnTyp: types.T_timestamp,
				Fn:        operator.CoalesceTimestamp,
			},
			{
				Index:     15,
				Volatile:  true,
				ReturnTyp: types.T_decimal64,
				Fn:        operator.CoalesceDecimal64,
			},
			{
				Index:     16,
				Volatile:  true,
				ReturnTyp: types.T_decimal128,
				Fn:        operator.CoalesceDecimal128,
			},
			{
				Index:     17,
				Volatile:  true,
				ReturnTyp: types.T_date,
				Fn:        operator.CoalesceDate,
			},
			{
				Index:     18,
				Volatile:  true,
				ReturnTyp: types.T_uuid,
				Fn:        operator.CoalesceUuid,
			},
			{
				Index:     19,
				Volatile:  true,
				ReturnTyp: types.T_time,
				Fn:        operator.CoalesceTime,
			},
			{
				Index:     20,
				Volatile:  true,
				ReturnTyp: types.T_json,
				Fn:        operator.CoalesceJson,
			},
			{
				Index:     21,
				Volatile:  true,
				ReturnTyp: types.T_blob,
				Fn:        operator.CoalesceBlob,
			},
			{
				Index:     22,
				Volatile:  true,
				ReturnTyp: types.T_text,
				Fn:        operator.CoalesceText,
			},
		},
	},

	CASE: {
		Id:     CASE,
		Flag:   plan.Function_NONE,
		Layout: CASE_WHEN_EXPRESSION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			for i, o := range overloads {
				if operator.CwTypeCheckFn(inputs, nil, o.ReturnTyp) {
					return int32(i), nil
				}
			}
			l := len(inputs)
			minCost, minIndex := math.MaxInt32, -1
			convertTypes := make([]types.T, l)
			targetTypes := make([]types.T, l)
			for i, o := range overloads {
				if l >= 2 {
					flag := true
					for j := 0; j < l-1; j += 2 {
						if inputs[j] != types.T_bool && !inputs[0].ToType().IsIntOrUint() {
							flag = false
							break
						}
						targetTypes[j] = types.T_bool
					}
					if l%2 == 1 {
						targetTypes[l-1] = o.ReturnTyp
					}
					for j := 1; j < l; j += 2 {
						targetTypes[j] = o.ReturnTyp
					}
					if flag {
						if code, c := tryToMatch(inputs, targetTypes); code == matchedByConvert {
							if c < minCost {
								minCost = c
								copy(convertTypes, targetTypes)
								minIndex = i
							}
						}
					}
				}
			}
			if minIndex != -1 {
				return int32(minIndex), convertTypes
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				ReturnTyp: types.T_int8,
				Fn:        operator.CaseWhenInt8,
			},
			{
				Index:     1,
				Volatile:  true,
				ReturnTyp: types.T_int16,
				Fn:        operator.CaseWhenInt16,
			},
			{
				Index:     2,
				Volatile:  true,
				ReturnTyp: types.T_int32,
				Fn:        operator.CaseWhenInt32,
			},
			{
				Index:     3,
				Volatile:  true,
				ReturnTyp: types.T_int64,
				Fn:        operator.CaseWhenInt64,
			},
			{
				Index:     4,
				Volatile:  true,
				ReturnTyp: types.T_uint8,
				Fn:        operator.CaseWhenUint8,
			},
			{
				Index:     5,
				Volatile:  true,
				ReturnTyp: types.T_uint16,
				Fn:        operator.CaseWhenUint16,
			},
			{
				Index:     6,
				Volatile:  true,
				ReturnTyp: types.T_uint32,
				Fn:        operator.CaseWhenUint32,
			},
			{
				Index:     7,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        operator.CaseWhenUint64,
			},
			{
				Index:     8,
				Volatile:  true,
				ReturnTyp: types.T_float32,
				Fn:        operator.CaseWhenFloat32,
			},
			{
				Index:     9,
				Volatile:  true,
				ReturnTyp: types.T_float64,
				Fn:        operator.CaseWhenFloat64,
			},
			{
				Index:     10,
				Volatile:  true,
				ReturnTyp: types.T_bool,
				Fn:        operator.CaseWhenBool,
			},
			{
				Index:     11,
				Volatile:  true,
				ReturnTyp: types.T_date,
				Fn:        operator.CaseWhenDate,
			},
			{
				Index:     12,
				Volatile:  true,
				ReturnTyp: types.T_datetime,
				Fn:        operator.CaseWhenDateTime,
			},
			{
				Index:     13,
				Volatile:  true,
				ReturnTyp: types.T_varchar,
				Fn:        operator.CaseWhenVarchar,
			},
			{
				Index:     14,
				Volatile:  true,
				ReturnTyp: types.T_char,
				Fn:        operator.CaseWhenChar,
			},
			{
				Index:     15,
				Volatile:  true,
				ReturnTyp: types.T_decimal64,
				Fn:        operator.CaseWhenDecimal64,
			},
			{
				Index:     16,
				Volatile:  true,
				ReturnTyp: types.T_decimal128,
				Fn:        operator.CaseWhenDecimal128,
			},
			{
				Index:     17,
				Volatile:  true,
				ReturnTyp: types.T_timestamp,
				Fn:        operator.CaseWhenTimestamp,
			},
			{
				Index:     18,
				Volatile:  true,
				ReturnTyp: types.T_blob,
				Fn:        operator.CaseWhenBlob,
			},
			{
				Index:     19,
				Volatile:  true,
				ReturnTyp: types.T_uuid,
				Fn:        operator.CaseWhenUuid,
			},
			{
				Index:     20,
				Volatile:  true,
				ReturnTyp: types.T_text,
				Fn:        operator.CaseWhenText,
			},
			{
				Index:     21,
				Volatile:  true,
				ReturnTyp: types.T_time,
				Fn:        operator.CaseWhenTime,
			},
			{
				Index:     22,
				Volatile:  true,
				ReturnTyp: types.T_json,
				Fn:        operator.CaseWhenJson,
			},
		},
	},

	IFF: {
		Id:     IFF,
		Flag:   plan.Function_NONE,
		Layout: STANDARD_FUNCTION,
		TypeCheckFn: func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			for i, o := range overloads {
				if operator.IfTypeCheckFn(inputs, nil, o.ReturnTyp) {
					return int32(i), nil
				}
			}
			minCost, minIndex := math.MaxInt32, -1
			convertTypes := make([]types.T, 3)
			targetTypes := make([]types.T, 3)
			for i, o := range overloads {
				if len(inputs) == 3 {
					if inputs[0] != types.T_bool && !inputs[0].ToType().IsIntOrUint() {
						continue
					}

					targetTypes[0] = types.T_bool
					targetTypes[1], targetTypes[2] = o.ReturnTyp, o.ReturnTyp
					if code, c := tryToMatch(inputs, targetTypes); code == matchedByConvert {
						if c < minCost {
							minCost = c
							copy(convertTypes, targetTypes)
							minIndex = i
						}
					}
				}
			}
			if minIndex != -1 {
				return int32(minIndex), convertTypes
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:     0,
				Volatile:  true,
				ReturnTyp: types.T_int8,
				Fn:        operator.IfInt8,
			},
			{
				Index:     1,
				Volatile:  true,
				ReturnTyp: types.T_int16,
				Fn:        operator.IfInt16,
			},
			{
				Index:     2,
				Volatile:  true,
				ReturnTyp: types.T_int32,
				Fn:        operator.IfInt32,
			},
			{
				Index:     3,
				Volatile:  true,
				ReturnTyp: types.T_int64,
				Fn:        operator.IfInt64,
			},
			{
				Index:     4,
				Volatile:  true,
				ReturnTyp: types.T_uint8,
				Fn:        operator.IfUint8,
			},
			{
				Index:     5,
				Volatile:  true,
				ReturnTyp: types.T_uint16,
				Fn:        operator.IfUint16,
			},
			{
				Index:     6,
				Volatile:  true,
				ReturnTyp: types.T_uint32,
				Fn:        operator.IfUint32,
			},
			{
				Index:     7,
				Volatile:  true,
				ReturnTyp: types.T_uint64,
				Fn:        operator.IfUint64,
			},
			{
				Index:     8,
				Volatile:  true,
				ReturnTyp: types.T_float32,
				Fn:        operator.IfFloat32,
			},
			{
				Index:     9,
				Volatile:  true,
				ReturnTyp: types.T_float64,
				Fn:        operator.IfFloat64,
			},
			{
				Index:     10,
				Volatile:  true,
				ReturnTyp: types.T_bool,
				Fn:        operator.IfBool,
			},
			{
				Index:     11,
				Volatile:  true,
				ReturnTyp: types.T_date,
				Fn:        operator.IfDate,
			},
			{
				Index:     12,
				Volatile:  true,
				ReturnTyp: types.T_datetime,
				Fn:        operator.IfDateTime,
			},
			{
				Index:     13,
				Volatile:  true,
				ReturnTyp: types.T_varchar,
				Fn:        operator.IfVarchar,
			},
			{
				Index:     14,
				Volatile:  true,
				ReturnTyp: types.T_char,
				Fn:        operator.IfChar,
			},
			{
				Index:     15,
				Volatile:  true,
				ReturnTyp: types.T_decimal64,
				Fn:        operator.IfDecimal64,
			},
			{
				Index:     16,
				Volatile:  true,
				ReturnTyp: types.T_decimal128,
				Fn:        operator.IfDecimal128,
			},
			{
				Index:     17,
				Volatile:  true,
				ReturnTyp: types.T_timestamp,
				Fn:        operator.IfTimestamp,
			},
			{
				Index:     18,
				Volatile:  true,
				ReturnTyp: types.T_blob,
				Fn:        operator.IfBlob,
			},
			{
				Index:     19,
				Volatile:  true,
				ReturnTyp: types.T_text,
				Fn:        operator.IfText,
			},
			{
				Index:     20,
				Volatile:  true,
				ReturnTyp: types.T_time,
				Fn:        operator.IfTime,
			},
			{
				Index:     21,
				Volatile:  true,
				ReturnTyp: types.T_json,
				Fn:        operator.IfJson,
			},
			{
				Index:     22,
				Volatile:  true,
				ReturnTyp: types.T_uuid,
				Fn:        operator.IfUuid,
			},
		},
	},
}
