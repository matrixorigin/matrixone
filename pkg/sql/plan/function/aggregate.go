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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
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
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: aggregate.Max,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: aggregate.Max,
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
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: aggregate.Min,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: aggregate.Min,
			}, {
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: aggregate.Min,
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
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Sum,
			},
			/*
				{
					Index:         10,
					Flag:          plan.Function_AGG,
					Layout:        STANDARD_FUNCTION,
					Args:          []types.T{types.T_date},
					TypeCheckFn:   strictTypeCheck,
					ReturnTyp:     types.T_date,
					AggregateInfo: aggregate.Sum,
				},
				{
					Index:         11,
					Flag:          plan.Function_AGG,
					Layout:        STANDARD_FUNCTION,
					Args:          []types.T{types.T_datetime},
					TypeCheckFn:   strictTypeCheck,
					ReturnTyp:     types.T_datetime,
					AggregateInfo: aggregate.Sum,
				},
			*/
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: aggregate.Sum,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.Sum,
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
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.Avg,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.Avg,
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
				AggregateInfo: aggregate.Count,
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
				AggregateInfo: aggregate.StarCount,
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
				_, err := aggregate.ReturnType(aggregate.BitAnd, types.Type{Oid: inputs[0]})
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
				AggregateInfo: aggregate.BitAnd,
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
				_, err := aggregate.ReturnType(aggregate.BitOr, types.Type{Oid: inputs[0]})
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
				AggregateInfo: aggregate.BitOr,
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
				_, err := aggregate.ReturnType(aggregate.BitXor, types.Type{Oid: inputs[0]})
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
				AggregateInfo: aggregate.BitXor,
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
				_, err := aggregate.ReturnType(aggregate.Variance, types.Type{Oid: inputs[0]})
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
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.Variance,
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
				_, err := aggregate.ReturnType(aggregate.StdDevPop, types.Type{Oid: inputs[0]})
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
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.StdDevPop,
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
				AggregateInfo: aggregate.ApproxCountDistinct,
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
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         1,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint16},
				ReturnTyp:     types.T_uint16,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         2,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint32},
				ReturnTyp:     types.T_uint32,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         3,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_uint64},
				ReturnTyp:     types.T_uint64,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         4,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int8},
				ReturnTyp:     types.T_int8,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         5,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int16},
				ReturnTyp:     types.T_int16,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         6,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int32},
				ReturnTyp:     types.T_int32,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         7,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_int64},
				ReturnTyp:     types.T_int64,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         8,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float32},
				ReturnTyp:     types.T_float32,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         9,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_float64},
				ReturnTyp:     types.T_float64,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         10,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_date},
				ReturnTyp:     types.T_date,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         11,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_datetime},
				ReturnTyp:     types.T_datetime,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         12,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal64},
				ReturnTyp:     types.T_decimal64,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         13,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_decimal128},
				ReturnTyp:     types.T_decimal128,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         14,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_bool},
				ReturnTyp:     types.T_bool,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         15,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_char},
				ReturnTyp:     types.T_char,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         16,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_varchar},
				ReturnTyp:     types.T_varchar,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         17,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_timestamp},
				ReturnTyp:     types.T_timestamp,
				AggregateInfo: aggregate.AnyValue,
			},
			{
				Index:         18,
				Flag:          plan.Function_AGG,
				Layout:        STANDARD_FUNCTION,
				Args:          []types.T{types.T_blob},
				ReturnTyp:     types.T_blob,
				AggregateInfo: aggregate.AnyValue,
			},
		},
	},
}
