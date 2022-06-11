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
	"github.com/matrixorigin/matrixone/pkg/container/ring/stddevpop"
	"github.com/matrixorigin/matrixone/pkg/container/ring/variance"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/aggregate"
)

func initAggregateFunction() {
	var err error

	for name, fs := range aggregates {
		for _, f := range fs {
			err = appendFunction(name, f)
			if err != nil {
				panic(err)
			}
		}
	}
}

// aggregates contains the aggregate function indexed by function id.
var aggregates = map[int][]Function{
	MAX: {
		{
			Index:         0,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint8,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint16,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint32,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int8,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int16,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int32,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float32,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: aggregate.Max,
		},
		{
			Index:         16,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_bool},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_bool,
			AggregateInfo: aggregate.Max,
		},
	},
	MIN: {
		{
			Index:         0,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint8,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint16,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint32,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int8,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int16,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int32,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float32,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: aggregate.Min,
		},
		{
			Index:         16,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_bool},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_bool,
			AggregateInfo: aggregate.Min,
		},
	},
	SUM: {
		{
			Index:         0,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: aggregate.Sum,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: aggregate.Sum,
		},
	},
	AVG: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_float64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 && isNumberType(inputTypes[0]) {
					return true
				}
				return false
			},
			AggregateInfo: aggregate.Avg,
		},
	},
	COUNT: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_int64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				return len(inputTypes) == 1
			},
			AggregateInfo: aggregate.Count,
		},
	},
	STARCOUNT: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_int64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				return len(inputTypes) == 1
			},
			AggregateInfo: aggregate.StarCount,
		},
	},
	BIT_AND: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_uint64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 {
					_, err := aggregate.NewBitAnd(types.Type{Oid: inputTypes[0]})
					if err == nil {
						return true
					}
				}
				return false
			},
			AggregateInfo: aggregate.BitAnd,
		},
	},
	BIT_OR: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_uint64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 {
					_, err := aggregate.NewBitOr(types.Type{Oid: inputTypes[0]})
					if err == nil {
						return true
					}
				}
				return false
			},
			AggregateInfo: aggregate.BitOr,
		},
	},
	BIT_XOR: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_uint64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 {
					_, err := aggregate.NewBitXor(types.Type{Oid: inputTypes[0]})
					if err == nil {
						return true
					}
				}
				return false
			},
			AggregateInfo: aggregate.BitXor,
		},
	},
	VAR_POP: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_float64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 {
					_, err := variance.NewVarianceRingWithTypeCheck(types.Type{Oid: inputTypes[0]})
					if err == nil {
						return true
					}
				}
				return false
			},
			AggregateInfo: aggregate.Variance,
		},
	},
	STDDEV_POP: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_float64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				if len(inputTypes) == 1 {
					_, err := stddevpop.NewStdDevPopRingWithTypeCheck(types.Type{Oid: inputTypes[0]})
					if err == nil {
						return true
					}
				}
				return false
			},
			AggregateInfo: aggregate.StdDevPop,
		},
	},
	APPROX_COUNT_DISTINCT: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_uint64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) (match bool) {
				return len(inputTypes) == 1
			},
			AggregateInfo: aggregate.ApproxCountDistinct,
		},
	},
}
