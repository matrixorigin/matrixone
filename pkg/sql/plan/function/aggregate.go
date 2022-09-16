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
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.AggregateMax,
			},
			{
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.AggregateMax,
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
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.AggregateMin,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.AggregateMin,
			}, {
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.AggregateMin,
			}, {
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.AggregateMin,
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
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateSum,
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
				AggregateInfo: agg.AggregateSum,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateSum,
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
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateAvg,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateAvg,
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
				AggregateInfo: agg.AggregateCount,
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
				AggregateInfo: agg.AggregateStarCount,
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
				_, err := agg.ReturnType(agg.AggregateBitAnd, types.Type{Oid: inputs[0]})
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
				AggregateInfo: agg.AggregateBitAnd,
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
				_, err := agg.ReturnType(agg.AggregateBitOr, types.Type{Oid: inputs[0]})
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
				AggregateInfo: agg.AggregateBitOr,
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
				_, err := agg.ReturnType(agg.AggregateBitXor, types.Type{Oid: inputs[0]})
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
				AggregateInfo: agg.AggregateBitXor,
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
				t, err := agg.ReturnType(agg.AggregateVariance, types.Type{Oid: inputs[0]})
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
				AggregateInfo: agg.AggregateVariance,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateVariance,
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
				t, err := agg.ReturnType(agg.AggregateStdDevPop, types.Type{Oid: inputs[0]})
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
				AggregateInfo: agg.AggregateStdDevPop,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateStdDevPop,
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
				AggregateInfo: agg.AggregateApproxCountDistinct,
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
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: agg.AggregateAnyValue,
			},
			{
				Index:         19,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uuid},
				ReturnTyp:     types.T_uuid,
				AggregateInfo: agg.AggregateAnyValue,
			},
		},
	},
}
