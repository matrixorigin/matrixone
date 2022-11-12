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
)

const (
	maxTypes = 255

	tooManyFunctionsMatched = -2
	wrongFunctionParameters = -1
	wrongFuncParamForAgg    = -3

	matchedDirectly = iota
	matchedByConvert
	matchedFailed
)

type binaryTargetTypes struct {
	convert     bool
	left, right types.T
}

// castTable indicates whether a type can be automatically converted to another type
var castTable [][]bool

// preferredTypeConvert indicates what type conversion we prefer.
var preferredTypeConvert [][]bool

// binaryTable is a cast rule table for some binary-operators' parameters
// e.g. PLUS, MINUS, GT and so on.
// Format is `binaryTable[LeftInput][RightInput] = {LeftTarget, RightTarget}`
var binaryTable [][]binaryTargetTypes

// binaryTable2 is a cast rule table for DIV and INTEGER_DIV
// Format is `binaryTable[LeftInput][RightInput] = {LeftTarget, RightTarget}`
var binaryTable2 [][]binaryTargetTypes

// init binaryTable and castTable
func initTypeCheckRelated() {
	all := []types.T{
		types.T_any,
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_decimal64, types.T_decimal128,
	}
	numbers := []types.T{ // numbers without decimal
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	}
	ints := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64}
	uints := []types.T{types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64}
	floats := []types.T{types.T_float32, types.T_float64}
	strings := []types.T{types.T_char, types.T_varchar, types.T_blob, types.T_text}
	decimals := []types.T{types.T_decimal64, types.T_decimal128}

	// init binaryTable
	var convertRuleForBinaryTable [][4]types.T // left-input, right-input, left-target, right-target
	{
		for _, typ := range all {
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ScalarNull, typ, typ, typ})
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, ScalarNull, typ, typ})
		}
		for _, typ1 := range numbers {
			for _, typ2 := range floats {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ1, typ2, types.T_float64, types.T_float64})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ2, typ1, types.T_float64, types.T_float64})
			}
		}
		for i := 0; i < len(ints); i++ {
			for j := i + 1; j < len(ints); j++ {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], ints[j], ints[j], ints[j]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[j], ints[i], ints[j], ints[j]})
			}
		}
		for i := 0; i < len(uints); i++ {
			for j := i + 1; j < len(uints); j++ {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[i], uints[j], uints[j], uints[j]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[j], uints[i], uints[j], uints[j]})
			}
		}
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_float32, types.T_float64, types.T_float64, types.T_float64})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_float64, types.T_float32, types.T_float64, types.T_float64})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128})
		for _, typ1 := range decimals {
			for _, typ2 := range numbers {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ1, typ2, types.T_decimal128, types.T_decimal128})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ2, typ1, types.T_decimal128, types.T_decimal128})
			}
			for _, typ2 := range floats {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ1, typ2, types.T_float64, types.T_float64})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ2, typ1, types.T_float64, types.T_float64})
			}
		}
		for i := 0; i < len(ints)-1; i++ {
			for j := 0; j < len(uints)-1; j++ {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], uints[j], ints[i+1], ints[i+1]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[j], ints[i], ints[i+1], ints[i+1]})
			}
		}
		for i := range ints {
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], types.T_uint64, types.T_uint64, types.T_uint64})
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_uint64, ints[i], types.T_uint64, types.T_uint64})
		}
		for i := range uints {
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[i], types.T_int64, types.T_int64, types.T_int64})
			convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_int64, uints[i], types.T_int64, types.T_int64})
		}

		{
			typ := types.T_timestamp
			for i := range ints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], typ, types.T_timestamp, types.T_timestamp})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, ints[i], types.T_timestamp, types.T_timestamp})
			}
			for i := range uints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[i], typ, types.T_timestamp, types.T_timestamp})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, uints[i], types.T_timestamp, types.T_timestamp})
			}
			for i := range decimals {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{decimals[i], typ, types.T_decimal128, types.T_decimal128})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, decimals[i], types.T_decimal128, types.T_decimal128})
			}
		}

		{
			typ := types.T_time
			for i := range numbers {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{numbers[i], typ, types.T_time, types.T_time})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, numbers[i], types.T_time, types.T_time})
			}
			for i := range decimals {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{decimals[i], typ, types.T_decimal128, types.T_decimal128})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, decimals[i], types.T_decimal128, types.T_decimal128})
			}
		}

		{
			typ := types.T_bool
			for i := range ints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], typ, ints[i], ints[i]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, ints[i], ints[i], ints[i]})
			}
			for i := range uints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[i], typ, uints[i], uints[i]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, uints[i], uints[i], uints[i]})
			}
		}

		{
			typ := types.T_uuid
			for _, t1 := range strings {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{t1, typ, typ, typ})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, t1, typ, typ})
			}
		}
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_date, types.T_timestamp, types.T_timestamp, types.T_timestamp})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_timestamp, types.T_date, types.T_timestamp, types.T_timestamp})

		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_datetime, types.T_timestamp, types.T_timestamp, types.T_timestamp})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_timestamp, types.T_datetime, types.T_timestamp, types.T_timestamp})

		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_uint64, types.T_int64, types.T_int64, types.T_int64})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_int64, types.T_uint64, types.T_int64, types.T_int64})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_date, types.T_datetime, types.T_datetime, types.T_datetime})
		convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{types.T_datetime, types.T_date, types.T_datetime, types.T_datetime})

		for _, t1 := range strings {
			for _, t2 := range all {
				if t1 == t2 || t2 == types.T_any {
					continue
				}
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{t1, t2, t2, t2})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{t2, t1, t2, t2})
			}
		}
	}

	binaryTable = make([][]binaryTargetTypes, maxTypes)
	for i := range binaryTable {
		binaryTable[i] = make([]binaryTargetTypes, maxTypes)
	}
	for _, r := range convertRuleForBinaryTable {
		binaryTable[r[0]][r[1]] = binaryTargetTypes{
			convert: true,
			left:    r[2],
			right:   r[3],
		}
	}

	// init binaryTable2
	var convertRuleForBinaryTable2 [][4]types.T
	{
		for _, typ := range all {
			convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{ScalarNull, typ, types.T_float64, types.T_float64})
			convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, ScalarNull, types.T_float64, types.T_float64})
		}
		for _, typ := range decimals {
			convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{ScalarNull, typ, typ, typ})
			convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, ScalarNull, typ, typ})
		}
		for i := range numbers {
			for j := range numbers {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], numbers[j], types.T_float64, types.T_float64})
			}
			for j := range decimals {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], decimals[j], types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{decimals[j], numbers[i], types.T_float64, types.T_float64})
			}
			for j := range floats {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], floats[j], types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{floats[j], numbers[i], types.T_float64, types.T_float64})
			}
			for j := range strings {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], strings[j], types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{strings[j], numbers[i], types.T_float64, types.T_float64})
			}
		}
		for i := range floats {
			for j := range decimals {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{floats[i], decimals[j], types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{decimals[j], floats[i], types.T_float64, types.T_float64})
			}
			for j := range strings {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{floats[i], strings[j], types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{strings[j], floats[i], types.T_float64, types.T_float64})
			}
		}
		{
			typ := types.T_timestamp
			for i := range numbers {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], typ, types.T_float64, types.T_float64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, numbers[i], types.T_float64, types.T_float64})
			}
			for i := range decimals {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{decimals[i], typ, types.T_decimal128, types.T_decimal128})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, decimals[i], types.T_decimal128, types.T_decimal128})
			}
		}
		{
			typ := types.T_time
			for i := range numbers {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{numbers[i], typ, types.T_int64, types.T_int64})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, numbers[i], types.T_int64, types.T_int64})
			}
			for i := range decimals {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{decimals[i], typ, types.T_decimal128, types.T_decimal128})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{typ, decimals[i], types.T_decimal128, types.T_decimal128})
			}
		}
		{
			typ := types.T_bool
			for i := range ints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{ints[i], typ, ints[i], ints[i]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, ints[i], ints[i], ints[i]})
			}
			for i := range uints {
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{uints[i], typ, uints[i], uints[i]})
				convertRuleForBinaryTable = append(convertRuleForBinaryTable, [4]types.T{typ, uints[i], uints[i], uints[i]})
			}
		}

		convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128})
		convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128})
		for i := range decimals {
			for j := range strings {
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{strings[j], decimals[i], decimals[i], decimals[i]})
				convertRuleForBinaryTable2 = append(convertRuleForBinaryTable2, [4]types.T{decimals[i], strings[j], decimals[i], decimals[i]})
			}
		}
	}

	binaryTable2 = make([][]binaryTargetTypes, maxTypes)
	for i := range binaryTable2 {
		binaryTable2[i] = make([]binaryTargetTypes, maxTypes)
	}
	for _, r := range convertRuleForBinaryTable2 {
		binaryTable2[r[0]][r[1]] = binaryTargetTypes{
			convert: true,
			left:    r[2],
			right:   r[3],
		}
	}

	// init castTable
	castTable = make([][]bool, maxTypes)
	for i := range castTable {
		castTable[i] = make([]bool, maxTypes)
	}
	{ // bool
		castTable[types.T_bool][types.T_bool] = true
		for _, typ := range strings {
			castTable[types.T_bool][typ] = true
		}
	}
	{ // date
		castTable[types.T_date][types.T_date] = true
		castTable[types.T_date][types.T_timestamp] = true
		castTable[types.T_date][types.T_datetime] = true
		for _, typ := range strings {
			castTable[types.T_date][typ] = true
		}
	}
	{ // time
		castTable[types.T_time][types.T_time] = true
		castTable[types.T_time][types.T_date] = true
		castTable[types.T_time][types.T_datetime] = true
		for _, typ := range strings {
			castTable[types.T_time][typ] = true
		}
	}
	{ // datetime
		castTable[types.T_datetime][types.T_datetime] = true
		castTable[types.T_datetime][types.T_date] = true
		castTable[types.T_datetime][types.T_timestamp] = true
		for _, typ := range strings {
			castTable[types.T_datetime][typ] = true
		}
	}
	{ //  float
		for _, t := range floats {
			castTable[t][types.T_bool] = true
			for _, typ := range floats {
				castTable[t][typ] = true
			}
			for _, typ := range numbers {
				castTable[t][typ] = true
			}
			for _, typ := range decimals {
				castTable[t][typ] = true
			}
			for _, typ := range strings {
				castTable[t][typ] = true
			}
		}
	}
	{ //  number
		for _, t := range numbers {
			castTable[t][t] = true
			castTable[t][types.T_bool] = true
			for _, typ := range floats {
				castTable[t][typ] = true
			}
			for _, typ := range numbers {
				castTable[t][typ] = true
			}
			castTable[t][types.T_timestamp] = true
			castTable[t][types.T_decimal64] = true
			castTable[t][types.T_decimal128] = true
			for _, typ := range strings {
				castTable[t][typ] = true
			}
		}
		for _, t := range decimals {
			castTable[t][t] = true
			castTable[t][types.T_bool] = true
			for _, typ := range floats {
				castTable[t][typ] = true
			}
			for _, typ := range numbers {
				castTable[t][typ] = true
			}
			castTable[t][types.T_timestamp] = true
			castTable[t][types.T_decimal64] = true
			castTable[t][types.T_decimal128] = true
			for _, typ := range strings {
				castTable[t][typ] = true
			}
		}
	}
	{ // timestamp
		castTable[types.T_timestamp][types.T_timestamp] = true
		castTable[types.T_timestamp][types.T_date] = true
		castTable[types.T_timestamp][types.T_datetime] = true
		for _, typ := range strings {
			castTable[types.T_timestamp][typ] = true
		}
	}
	{ // string
		for _, t := range strings {
			for _, typ := range all {
				castTable[t][typ] = true
			}
		}
	}

	// init preferredTypeConvert
	preferredConversion := map[types.T][]types.T{
		types.T_int8:       {types.T_int64, types.T_int32, types.T_int16, types.T_float32, types.T_float64},
		types.T_int16:      {types.T_int64, types.T_int32, types.T_float32, types.T_float64},
		types.T_int32:      {types.T_int64, types.T_float64},
		types.T_int64:      {types.T_float64},
		types.T_uint8:      {types.T_uint64, types.T_uint32, types.T_uint16, types.T_float32, types.T_float64},
		types.T_uint16:     {types.T_uint64, types.T_uint32, types.T_float32, types.T_float64},
		types.T_uint32:     {types.T_uint64, types.T_float64},
		types.T_uint64:     {types.T_int64, types.T_float64},
		types.T_float32:    {types.T_float64},
		types.T_char:       {types.T_varchar, types.T_int64},
		types.T_varchar:    {types.T_char, types.T_int64},
		types.T_blob:       {types.T_blob},
		types.T_text:       {types.T_text},
		types.T_decimal64:  {types.T_decimal128, types.T_float64},
		types.T_decimal128: {types.T_float64},
		types.T_date:       {types.T_datetime},
	}
	preferredTypeConvert = make([][]bool, maxTypes)
	for i := range preferredTypeConvert {
		preferredTypeConvert[i] = make([]bool, maxTypes)
	}
	for t1, v := range preferredConversion {
		for _, t2 := range v {
			preferredTypeConvert[t1][t2] = true
		}
	}
}

var (
	// GeneralBinaryOperatorTypeCheckFn1 will check if params of the binary operators need type convert work
	GeneralBinaryOperatorTypeCheckFn1 = func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
		return generalBinaryOperatorTypeCheckFn(overloads, inputs, generalBinaryParamsConvert)
	}

	// GeneralBinaryOperatorTypeCheckFn2 will check if params of the DIV and INTEGER_DIV need type convert work
	GeneralBinaryOperatorTypeCheckFn2 = func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
		return generalBinaryOperatorTypeCheckFn(overloads, inputs, generalDivParamsConvert)
	}
)

func generalBinaryOperatorTypeCheckFn(overloads []Function, inputs []types.T, convertRule func(types.T, types.T) (types.T, types.T, bool)) (overloadIndex int32, ts []types.T) {
	targets := make([]types.T, 0, 2)
	if len(inputs) == 2 {
		matched := make([]int32, 0, 4)
		t1, t2, convert := convertRule(inputs[0], inputs[1])
		targets = append(targets, t1, t2)
		for _, o := range overloads {
			if ok, _ := tryToMatch(targets, o.Args); ok == matchedDirectly {
				matched = append(matched, o.Index)
			}
		}
		if !convert {
			targets = nil
		}
		if len(matched) == 1 {
			return matched[0], targets
		} else if len(matched) > 1 {
			for j := range inputs {
				if inputs[j] == ScalarNull {
					return matched[0], targets
				}
			}
		}
	}
	return wrongFunctionParameters, targets
}

func generalBinaryParamsConvert(l, r types.T) (types.T, types.T, bool) {
	ts := binaryTable[l][r] // targets
	if ts.convert {
		return ts.left, ts.right, true
	}
	return l, r, false
}

func generalDivParamsConvert(l, r types.T) (types.T, types.T, bool) {
	ts := binaryTable2[l][r]
	if ts.convert {
		return ts.left, ts.right, true
	}
	return l, r, false
}

// a general type check function for unary aggregate functions.
// it will do strict type check for parameters, and return the first overload if all parameters are scalar null.
func generalTypeCheckForUnaryAggregate(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
	if len(inputs) == 1 {
		if inputs[0] == types.T_any {
			return 0, nil
		}
		for i, o := range overloads {
			if o.Args[0] == inputs[0] {
				return int32(i), nil
			}
		}
	}
	return wrongFuncParamForAgg, nil
}

// tryToMatch checks whether the types of the two input parameters match directly
// or can be matched by implicit type conversion.
// If the match is successful,
// it returns the match code and the cost of parameters to be converted.
// if the type conversion we prefer, the cost will be smaller.
func tryToMatch(inputs, requires []types.T) (int, int) {
	cost1, cost2 := 1, len(inputs)*2
	if len(inputs) == len(requires) {
		matchNumber, cost := 0, 0
		for i := 0; i < len(inputs); i++ {
			t1, t2 := inputs[i], requires[i]
			if t1 == t2 || t1 == ScalarNull {
				matchNumber++
			} else if castTable[t1][t2] {
				if preferredTypeConvert[t1][t2] {
					cost += cost1
				} else {
					cost += cost2
				}
			} else {
				return matchedFailed, 0
			}
		}
		if matchNumber == len(inputs) {
			return matchedDirectly, 0
		}
		return matchedByConvert, cost
	}
	return matchedFailed, 0
}
