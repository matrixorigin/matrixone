// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

const (
	IntegerFormat2Overload = 2
	IntegerFormat3Overload = 3
)

func FormatWith2IntegerScale(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	return formatWithIntegerScale(ivecs, result, length, selectList, false)
}
func FormatWith3IntegerScale(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	return formatWithIntegerScale(ivecs, result, length, selectList, true)
}
func formatWithIntegerScale(ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList, withLocale bool) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	scales := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	var strings vector.FunctionParameterWrapper[types.Varlena]
	numeric := ivecs[0].GetType().IsNumeric()
	if !numeric {
		strings = vector.GenerateFunctionStrParameter(ivecs[0])
	}
	var locales vector.FunctionParameterWrapper[types.Varlena]
	if withLocale {
		locales = vector.GenerateFunctionStrParameter(ivecs[2])
	}
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		scale, nullScale := scales.GetValue(i)
		locale := "en_US"
		if withLocale {
			b, n := locales.GetStrValue(i)
			if !n {
				locale = string(b)
			}
		}
		if nullScale {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		scaleText := strconv.FormatInt(scale, 10)
		var formatted string
		var err error
		if numeric {
			number, exact, nullNumber, e := formatNumericValueAt(ivecs[0], i)
			if e != nil {
				return e
			}
			if nullNumber {
				if e = rs.AppendBytes(nil, true); e != nil {
					return e
				}
				continue
			}
			if exact {
				formatted, err = format.GetNumberFormatExact(number, scaleText, locale)
			} else {
				formatted, err = format.GetNumberFormat(number, scaleText, locale)
			}
		} else {
			number, nullNumber := strings.GetStrValue(i)
			if nullNumber {
				if e := rs.AppendBytes(nil, true); e != nil {
					return e
				}
				continue
			}
			formatted, err = format.GetNumberFormat(string(number), scaleText, locale)
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
