// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Append-only identities. Their physical signatures require MORPC v98 even
// when the integer operands already have INT64 type and need no private CAST.
const (
	FormatIntegerPrecisionOverload       = 2
	FormatIntegerPrecisionLocaleOverload = 3
	MakeDateIntegerOverload              = 1
	MakeTimeIntegerFloatOverload         = 36
	MakeTimeIntegerExactOverload         = 37
	MakeTimeIntegerUnsignedOverload      = 38
)

func formatIntegerCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) < 2 || len(inputs) > 3 || inputs[0].Oid.IsDateRelate() {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if !inputs[0].IsNumeric() {
		return fixedTypeMatch(overloads, inputs)
	}
	// Only the precision is an integer contract. Preserve the typed number and
	// the existing locale conversion without offering approximate coercion for it.
	targets := append([]types.Type(nil), inputs...)
	if len(inputs) == 3 && !inputs[2].Oid.IsMySQLString() {
		targets[2] = formattedScalarStringType(inputs[2])
		SetTargetScaleFromSource(&inputs[2], &targets[2])
		return newCheckResultWithCast(len(inputs)-2, targets)
	}
	return newCheckResultWithSuccess(len(inputs) - 2)
}

func makeTimeIntegerCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	second := inputs[2]
	target := types.T_float64
	switch {
	case isMakeTimeTextType(second.Oid) || second.Oid.IsDecimal():
		target = types.T_varchar
	case second.Oid.IsUnsignedInt() || second.Oid == types.T_bit:
		target = types.T_uint64
	case second.Oid.IsInteger() || second.Oid == types.T_bool || second.Oid == types.T_any || second.Oid.IsMySQLString():
		target = types.T_int64
	}
	for i, ov := range overloads {
		if ov.args[2] != target {
			continue
		}
		status, _ := tryToMatch(inputs, ov.args)
		if status == matchFailed {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		targets := append([]types.Type(nil), inputs...)
		if second.Oid != target {
			targets[2] = target.ToType()
			SetTargetScaleFromSource(&second, &targets[2])
		}
		if target == types.T_varchar {
			targets[2].Scale = -1
			if second.Oid.IsDecimal() {
				targets[2].Scale = second.Scale
			}
		}
		if status == matchDirectly && targets[2] == second {
			return newCheckResultWithSuccess(i)
		}
		return newCheckResultWithCast(i, targets)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func formatIntegerPrecision(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	precision := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	numeric := ivecs[0].GetType().IsNumeric()
	var text, locale vector.FunctionParameterWrapper[types.Varlena]
	if !numeric {
		text = vector.GenerateFunctionStrParameter(ivecs[0])
	}
	if len(ivecs) == 3 {
		locale = vector.GenerateFunctionStrParameter(ivecs[2])
	}
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		digits, nullDigits := precision.GetValue(i)
		var number string
		var exact, nullNumber bool
		var err error
		if numeric {
			number, exact, nullNumber, err = formatNumericValueAt(ivecs[0], i)
			if err != nil {
				return err
			}
		} else {
			var value []byte
			value, nullNumber = text.GetStrValue(i)
			number = string(value)
		}
		if nullDigits || nullNumber {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		localeValue := "en_US"
		if locale != nil {
			value, isNull := locale.GetStrValue(i)
			if !isNull {
				localeValue = string(value)
			}
		}
		// The formatter's string API is a rendering boundary, not source coercion:
		// conversion and overflow checking have already happened in the binder.
		scale := strconv.FormatInt(max(int64(0), min(digits, int64(30))), 10)
		var formatted string
		if exact {
			formatted, err = format.GetNumberFormatExact(number, scale, localeValue)
		} else {
			formatted, err = format.GetNumberFormat(number, scale, localeValue)
		}
		if err != nil {
			return err
		}
		if err = rs.AppendBytes([]byte(formatted), false); err != nil {
			return err
		}
	}
	return nil
}

func makeDateInteger(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	years := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	days := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	last := types.DateFromCalendar(9999, 12, 31)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		year, nullYear := years.GetValue(i)
		day, nullDay := days.GetValue(i)
		if nullYear || nullDay || year < 0 || year > 9999 || day <= 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if year < 70 {
			year += 2000
		} else if year < 100 {
			year += 1900
		}
		first := types.DateFromCalendar(int32(year), 1, 1)
		if day > int64(last-first)+1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		date := first + types.Date(day-1)
		if err := rs.AppendBytes([]byte(date.String()), false); err != nil {
			return err
		}
	}
	return nil
}
