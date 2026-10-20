// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	functionUtil "github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A dynamic ADDTIME/SUBTIME result is TIME even for calendar input. Calendar
// arithmetic precedes clock extraction; genuine durations retain hours > 24.
func timeResultArithmetic(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, subtract bool) error {
	results := vector.MustFunctionResult[types.Time](result)
	scale := max(results.GetType().Scale, parameters[1].GetType().Scale)
	var read func(uint64) (types.Datetime, types.Time, temporalStringKind, bool)
	switch parameters[0].GetType().Oid {
	case types.T_time:
		first := vector.GenerateFunctionFixedTypeParameter[types.Time](parameters[0])
		read = func(row uint64) (types.Datetime, types.Time, temporalStringKind, bool) {
			value, isNull := first.GetValue(row)
			return 0, value, temporalStringTime, isNull
		}
	case types.T_date, types.T_datetime, types.T_timestamp:
		first, err := newTimestampPairDateReader(parameters[0], proc)
		if err != nil {
			return err
		}
		read = func(row uint64) (types.Datetime, types.Time, temporalStringKind, bool) {
			value, isNull := first(row)
			return value, 0, temporalStringDateTime, isNull
		}
	case types.T_char, types.T_varchar, types.T_text:
		first := vector.GenerateFunctionStrParameter(parameters[0])
		read = func(row uint64) (types.Datetime, types.Time, temporalStringKind, bool) {
			text, isNull := first.GetStrValue(row)
			if isNull {
				return 0, 0, temporalStringInvalid, true
			}
			date, duration, kind, err := parseTemporalString(functionUtil.QuickBytesToStr(text), scale)
			return date, duration, kind, err != nil || kind == temporalStringDateTime && date == types.ZeroDatetime
		}
	default:
		return moerr.NewInvalidInputf(proc.Ctx, "time arithmetic: unsupported type %v", parameters[0].GetType().Oid)
	}
	second := vector.GenerateFunctionStrParameter(parameters[1])
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		text, isNull := second.GetStrValue(row)
		if isNull {
			if err := results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		date, duration, kind, invalid := read(row)
		delta, err := parseTimeOperand(functionUtil.QuickBytesToStr(text), scale)
		if invalid || err != nil {
			if err := results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		var value types.Time
		var overflow bool
		if kind == temporalStringDateTime {
			var calendar types.Datetime
			calendar, overflow = datetimeArithmeticResult(date, delta, subtract)
			if !overflow {
				value, _ = timestampPairDatetimeToTime(calendar)
			}
		} else {
			value, overflow = timeArithmeticResult(duration, delta, subtract)
		}
		if overflow {
			appendTimeIntervalOverflowWarning(proc)
		}
		if err := results.Append(value, overflow); err != nil {
			return err
		}
	}
	return nil
}
