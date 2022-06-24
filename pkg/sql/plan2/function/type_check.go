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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

const maxTypeNumber = types.T_tuple + 10

type binaryTargetTypes struct {
	convert     bool
	left, right types.T
}

// castable indicates whether a type can be automatically converted to another type
var castable [][]bool

// binaryTable is a cast rule table for some binary-operators' parameters
// e.g. PLUS, MINUS and so on.
// Format is `binaryTable[LeftInput][RightInput] = {LeftTarget, RightTarget}`
var binaryTable [][]binaryTargetTypes

func init() {
	rules := [][4]types.T{ // left-input, right-input, left-target, right-target
		{types.T_any, types.T_any, types.T_int64, types.T_int64},
		{types.T_any, types.T_int8, types.T_int8, types.T_int8},
		{types.T_int8, types.T_uint8, types.T_int16, types.T_int16},
	}

	binaryTable = make([][]binaryTargetTypes, maxTypeNumber)
	for i := range binaryTable {
		binaryTable[i] = make([]binaryTargetTypes, maxTypeNumber)
	}
	for _, r := range rules {
		binaryTable[r[0]][r[1]] = binaryTargetTypes{
			convert: true,
			left:    r[2],
			right:   r[3],
		}
	}

	all := []types.T{
		types.T_any,
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime, types.T_timestamp,
		types.T_char, types.T_varchar,
		types.T_decimal64, types.T_decimal128,
	}

	numbers := []types.T{ // numbers without decimal
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	}

	floats := []types.T{types.T_float32, types.T_float64}
	strings := []types.T{types.T_char, types.T_varchar}

	maxTypes := 255
	castable = make([][]bool, maxTypes)
	{ // bool
		castable[types.T_bool] = make([]bool, maxTypes)
		castable[types.T_bool][types.T_bool] = true
		for _, typ := range strings {
			castable[types.T_bool][typ] = true
		}
	}
	{ // date
		castable[types.T_date] = make([]bool, maxTypes)
		castable[types.T_date][types.T_date] = true
		castable[types.T_date][types.T_timestamp] = true
		castable[types.T_date][types.T_datetime] = true
		for _, typ := range strings {
			castable[types.T_date][typ] = true
		}
	}
	{ // datetime
		castable[types.T_datetime] = make([]bool, maxTypes)
		castable[types.T_datetime][types.T_datetime] = true
		castable[types.T_datetime][types.T_date] = true
		castable[types.T_datetime][types.T_timestamp] = true
		for _, typ := range strings {
			castable[types.T_datetime][typ] = true
		}
	}
	{ //  float
		for _, t := range floats {
			castable[t] = make([]bool, maxTypes)
			for _, typ := range floats {
				castable[t][typ] = true
			}
			for _, typ := range numbers {
				castable[t][typ] = true
			}
			for _, typ := range strings {
				castable[t][typ] = true
			}
		}
	}
	{ //  number
		for _, t := range numbers {
			castable[t] = make([]bool, maxTypes)
			castable[t][t] = true
			for _, typ := range floats {
				castable[t][typ] = true
			}
			for _, typ := range numbers {
				castable[t][typ] = true
			}
			castable[t][types.T_timestamp] = true
			castable[t][types.T_decimal64] = true
			castable[t][types.T_decimal128] = true
			for _, typ := range strings {
				castable[t][typ] = true
			}
		}
		castable[types.T_decimal64] = make([]bool, maxTypes)
		castable[types.T_decimal64][types.T_decimal64] = true
		castable[types.T_decimal64][types.T_timestamp] = true
		for _, typ := range strings {
			castable[types.T_decimal64][typ] = true
		}
		castable[types.T_decimal128] = make([]bool, maxTypes)
		castable[types.T_decimal128][types.T_decimal128] = true
		castable[types.T_decimal128][types.T_timestamp] = true
		for _, typ := range strings {
			castable[types.T_decimal128][typ] = true
		}
	}
	{ // timestamp
		castable[types.T_timestamp] = make([]bool, maxTypes)
		castable[types.T_timestamp][types.T_timestamp] = true
		castable[types.T_timestamp][types.T_date] = true
		castable[types.T_timestamp][types.T_datetime] = true
		for _, typ := range strings {
			castable[types.T_timestamp][typ] = true
		}
	}
	{ // string
		for _, t := range strings {
			castable[t] = make([]bool, maxTypes)
			for _, typ := range all {
				castable[t][typ] = true
			}
		}
	}
}

func generalBinaryOperatorTypeCheckFn(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T, err error) {
	if len(inputs) == 2 {
		matched := make([]int32, 0, 4)
		inputs[0], inputs[1] = generalBinaryParamsConvert(inputs[0], inputs[1])
		for _, o := range overloads {
			if strictTypeCheck(inputs, o.Args, o.ReturnTyp) {
				matched = append(matched, o.Index)
			}
		}
		if len(matched) == 1 {
			return matched[0], inputs, nil
		} else if len(matched) > 1 {
			return -1, nil, errors.New(errno.AmbiguousFunction, "too many functions matched")
		}
	}
	return -1, nil, errors.New(errno.UndefinedFunction, "type check failed")
}

func generalBinaryParamsConvert(l, r types.T) (types.T, types.T) {
	ts := binaryTable[l][r] // targets
	if ts.convert {
		return ts.left, ts.right
	}
	return l, r
}
