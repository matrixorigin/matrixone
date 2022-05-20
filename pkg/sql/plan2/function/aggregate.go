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
			AggregateInfo: nil,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint16,
			AggregateInfo: nil,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint32,
			AggregateInfo: nil,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: nil,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int8,
			AggregateInfo: nil,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int16,
			AggregateInfo: nil,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int32,
			AggregateInfo: nil,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float32,
			AggregateInfo: nil,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: nil,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: nil,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: nil,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: nil,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: nil,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: nil,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: nil,
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
			AggregateInfo: nil,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint16,
			AggregateInfo: nil,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint32,
			AggregateInfo: nil,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: nil,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int8,
			AggregateInfo: nil,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int16,
			AggregateInfo: nil,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int32,
			AggregateInfo: nil,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float32,
			AggregateInfo: nil,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: nil,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: nil,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: nil,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: nil,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: nil,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: nil,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: nil,
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
			AggregateInfo: nil,
		},
		{
			Index:         1,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: nil,
		},
		{
			Index:         2,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: nil,
		},
		{
			Index:         3,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_uint64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_uint64,
			AggregateInfo: nil,
		},
		{
			Index:         4,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int8},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         5,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int16},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         6,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         7,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil,
		},
		{
			Index:         8,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float32},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: nil,
		},
		{
			Index:         9,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_float64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_float64,
			AggregateInfo: nil,
		},
		{
			Index:         10,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_varchar},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_varchar,
			AggregateInfo: nil,
		},
		{
			Index:         11,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_char},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_char,
			AggregateInfo: nil,
		},
		{
			Index:         12,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_date},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_date,
			AggregateInfo: nil,
		},
		{
			Index:         13,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_datetime},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_datetime,
			AggregateInfo: nil,
		},
		{
			Index:         14,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal64,
			AggregateInfo: nil,
		},
		{
			Index:         15,
			Flag:          plan.Function_AGG,
			Layout:        STANDARD_FUNCTION,
			Args:          []types.T{types.T_decimal128},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_decimal128,
			AggregateInfo: nil,
		},
	},
	AVG: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_float64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T) (match bool) {
				if len(inputTypes) == 1 && isNumberType(inputTypes[0]) {
					return true
				}
				return false
			},
			AggregateInfo: nil,
		},
	},
	COUNT: {
		{
			Index:     0,
			Flag:      plan.Function_AGG,
			Layout:    STANDARD_FUNCTION,
			ReturnTyp: types.T_int64,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T) (match bool) {
				if len(inputTypes) == 1 {
					return true
				}
				return false
			},
			AggregateInfo: nil,
		},
	},
}
