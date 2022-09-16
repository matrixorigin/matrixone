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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
)

func initAggregateFunction() {
	var err error

	for fid, fs := range aggregates {
		err = appendFunction(fid, fs)
		if err != nil {
			panic(err)
		}
	}
}

// aggregates contains the aggregate function indexed by function id.
var aggregates = map[int]Functions{
	MAX: {
		Id:          MAX,
		TypeCheckFn: generalTypeCheckForUnaryAggregate,
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint8},
				ReturnTyp:     types.T_uint8,
				AggregateInfo: agg.Max,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.Max,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.Max,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Max,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.Max,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.Max,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.Max,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Max,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.Max,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Max,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.Max,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.Max,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.Max,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.Max,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Max,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.Max,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.Max,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.Max,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.Max,
			},
			{
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.Max,
			},
		},
	},
	MIN: {
		Id:          MIN,
		TypeCheckFn: generalTypeCheckForUnaryAggregate,
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint8},
				ReturnTyp:     types.T_uint8,
				AggregateInfo: agg.Min,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.Min,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.Min,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Min,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.Min,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.Min,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.Min,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Min,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.Min,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Min,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.Min,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.Min,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.Min,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.Min,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Min,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.Min,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.Min,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.Min,
			}, {
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.Min,
			}, {
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.Min,
			},
		},
	},
	SUM: {
		Id:          SUM,
		TypeCheckFn: generalTypeCheckForUnaryAggregate,
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint8},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Sum,
			},
			/*
				{
					Index:         10,
					Flag:          plan.Function_AGG,
					Layout:        STANDARD_FUNCTION,
					Args:          []types.T{types.T_date},
					TypeCheckFn:   strictTypeCheck,
					ReturnTyp:     types.T_date,
					AggregateInfo: agg.Sum,
				},
				{
					Index:         11,
					Flag:          plan.Function_AGG,
					Layout:        STANDARD_FUNCTION,
					Args:          []types.T{types.T_datetime},
					TypeCheckFn:   strictTypeCheck,
					ReturnTyp:     types.T_datetime,
					AggregateInfo: agg.Sum,
				},
			*/
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.Sum,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Sum,
			},
		},
	},
	AVG: {
		Id:          AVG,
		TypeCheckFn: generalTypeCheckForUnaryAggregate,
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint8},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Avg,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Avg,
			},
		},
	},
	COUNT: {
		Id: COUNT,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.Count,
			},
		},
	},
	STARCOUNT: {
		Id: STARCOUNT,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.StarCount,
			},
		},
	},
	BIT_AND: {
		Id: BIT_AND,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				if inputs[0] == types.T_any {
					return 0, nil
				}
				if !operator.IsNumeric(inputs[0]) && !operator.IsDecimal(inputs[0]) {
					return wrongFuncParamForAgg, nil
				}
				_, err := agg.ReturnType(agg.BitAnd, types.Type{Oid: inputs[0]})
				if err == nil {
					return 0, nil
				}
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.BitAnd,
			},
		},
	},
	BIT_OR: {
		Id: BIT_OR,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				if inputs[0] == types.T_any {
					return 0, nil
				}
				if !operator.IsNumeric(inputs[0]) && !operator.IsDecimal(inputs[0]) {
					return wrongFuncParamForAgg, nil
				}
				_, err := agg.ReturnType(agg.BitOr, types.Type{Oid: inputs[0]})
				if err == nil {
					return 0, nil
				}
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.BitOr,
			},
		},
	},
	BIT_XOR: {
		Id: BIT_XOR,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				if inputs[0] == types.T_any {
					return 0, nil
				}
				if !operator.IsNumeric(inputs[0]) && !operator.IsDecimal(inputs[0]) {
					return wrongFuncParamForAgg, nil
				}
				_, err := agg.ReturnType(agg.BitXor, types.Type{Oid: inputs[0]})
				if err == nil {
					return 0, nil
				}
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.BitXor,
			},
		},
	},
	VAR_POP: {
		Id: VAR_POP,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				if inputs[0] == types.T_any {
					return 0, nil
				}
				if !operator.IsNumeric(inputs[0]) && !operator.IsDecimal(inputs[0]) {
					return wrongFuncParamForAgg, nil
				}
				t, err := agg.ReturnType(agg.Variance, types.Type{Oid: inputs[0]})
				if err != nil {
					return wrongFunctionParameters, nil
				}
				if t.Oid == types.T_decimal128 {
					return 1, nil
				}
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.Variance,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.Variance,
			},
		},
	},
	STDDEV_POP: {
		Id: STDDEV_POP,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, _ []types.T) {
			if len(inputs) == 1 {
				if inputs[0] == types.T_any {
					return 0, nil
				}
				if !operator.IsNumeric(inputs[0]) && !operator.IsDecimal(inputs[0]) {
					return wrongFuncParamForAgg, nil
				}
				t, err := agg.ReturnType(agg.StdDevPop, types.Type{Oid: inputs[0]})
				if err != nil {
					return wrongFunctionParameters, nil
				}
				if t.Oid == types.T_decimal128 {
					return 1, nil
				}
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.StdDevPop,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.StdDevPop,
			},
		},
	},
	APPROX_COUNT_DISTINCT: {
		Id: APPROX_COUNT_DISTINCT,
		TypeCheckFn: func(_ []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
			if len(inputs) == 1 {
				return 0, nil
			}
			return wrongFunctionParameters, nil
		},
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.ApproxCountDistinct,
			},
		},
	},
	ANY_VALUE: {
		Id:          ANY_VALUE,
		TypeCheckFn: generalTypeCheckForUnaryAggregate,
		Overloads: []Function{
			{
				Index:         0,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint8},
				ReturnTyp:     types.T_uint8,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.AnyValue,
			},
			{
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.AnyValue,
			},
		},
	},
}
