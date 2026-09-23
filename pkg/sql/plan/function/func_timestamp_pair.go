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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	functionUtil "github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func timestampPairTypeCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		return fixedTypeMatch(overloads, inputs)
	}
	if len(inputs) != 2 || len(overloads) <= 5 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	validDateInput := func(typ types.T) bool {
		return typ == types.T_any || typ == types.T_date || typ == types.T_datetime ||
			typ == types.T_timestamp || isTimestampPairString(typ)
	}
	validTimeInput := func(typ types.T) bool {
		return typ == types.T_any || typ == types.T_time || typ == types.T_date ||
			typ == types.T_datetime || typ == types.T_timestamp || isTimestampPairString(typ)
	}
	if !validDateInput(inputs[0].Oid) || !validTimeInput(inputs[1].Oid) {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	targets := append([]types.Type(nil), inputs...)
	needCast := false
	for i := range targets {
		if targets[i].Oid == types.T_any {
			targets[i] = types.T_varchar.ToType()
			needCast = true
		}
	}
	if needCast {
		return newCheckResultWithCast(5, targets)
	}
	return newCheckResultWithSuccess(5)
}

func isTimestampPairString(typ types.T) bool {
	return typ == types.T_char || typ == types.T_varchar || typ == types.T_text
}

func timestampPairReturnType(parameters []types.Type) types.Type {
	scale := int32(0)
	for _, parameter := range parameters {
		parameterScale := parameter.Scale
		if parameter.Oid == types.T_any || isTimestampPairString(parameter.Oid) {
			parameterScale = 6
		}
		if parameterScale < 0 {
			parameterScale = 0
		} else if parameterScale > 6 {
			parameterScale = 6
		}
		if parameterScale > scale {
			scale = parameterScale
		}
	}
	return types.New(types.T_datetime, scale, scale)
}

type timestampPairDateReader func(row uint64) (types.Datetime, bool)
type timestampPairTimeReader func(row uint64) (types.Time, bool)

func timestampPairDatetimeToTime(value types.Datetime) (types.Time, bool) {
	if value == types.ZeroDatetime {
		return 0, true
	}
	timeOfDay := int64(value) - int64(value.ToDate().ToDatetime())
	return types.Time(timeOfDay).TruncateToScale(6), false
}

func isTimestampPairCompactDatetime(text string) bool {
	if len(text) < 14 {
		return false
	}
	for i := 0; i < 14; i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return len(text) == 14 || text[14] == '.'
}

func newTimestampPairDateReader(vec *vector.Vector, proc *process.Process) (timestampPairDateReader, error) {
	switch vec.GetType().Oid {
	case types.T_date:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Date](vec)
		return func(row uint64) (types.Datetime, bool) {
			value, isNull := parameter.GetValue(row)
			if isNull || value == types.ZeroDate {
				return 0, true
			}
			return value.ToDatetime(), false
		}, nil
	case types.T_datetime:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Datetime](vec)
		return func(row uint64) (types.Datetime, bool) {
			value, isNull := parameter.GetValue(row)
			return value, isNull || value == types.ZeroDatetime
		}, nil
	case types.T_timestamp:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](vec)
		location := proc.GetSessionInfo().TimeZone
		return func(row uint64) (types.Datetime, bool) {
			value, isNull := parameter.GetValue(row)
			if isNull || value == types.ZeroTimestamp {
				return 0, true
			}
			return value.ToDatetime(location), false
		}, nil
	case types.T_char, types.T_varchar, types.T_text:
		parameter := vector.GenerateFunctionStrParameter(vec)
		return func(row uint64) (types.Datetime, bool) {
			value, isNull := parameter.GetStrValue(row)
			if isNull {
				return 0, true
			}
			datetime, err := types.ParseDatetime(functionUtil.QuickBytesToStr(value), 6)
			return datetime, err != nil || datetime == types.ZeroDatetime
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unexpected TIMESTAMP first argument type %s", vec.GetType().Oid)
	}
}

func newTimestampPairTimeReader(vec *vector.Vector, proc *process.Process) (timestampPairTimeReader, error) {
	switch vec.GetType().Oid {
	case types.T_time:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Time](vec)
		return parameter.GetValue, nil
	case types.T_date:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Date](vec)
		return func(row uint64) (types.Time, bool) {
			value, isNull := parameter.GetValue(row)
			return value.ToTime(), isNull
		}, nil
	case types.T_datetime:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Datetime](vec)
		return func(row uint64) (types.Time, bool) {
			value, isNull := parameter.GetValue(row)
			timeValue, invalid := timestampPairDatetimeToTime(value)
			return timeValue, isNull || invalid
		}, nil
	case types.T_timestamp:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](vec)
		location := proc.GetSessionInfo().TimeZone
		return func(row uint64) (types.Time, bool) {
			value, isNull := parameter.GetValue(row)
			return timestampToSessionClockTime(value, location, 6), isNull
		}, nil
	case types.T_char, types.T_varchar, types.T_text:
		parameter := vector.GenerateFunctionStrParameter(vec)
		return func(row uint64) (types.Time, bool) {
			value, isNull := parameter.GetStrValue(row)
			if isNull {
				return 0, true
			}
			text := strings.TrimSpace(functionUtil.QuickBytesToStr(value))
			if text == "" {
				return 0, true
			}
			if isTimestampPairCompactDatetime(text) {
				datetime, err := types.ParseDatetime(text, 6)
				if err == nil {
					return timestampPairDatetimeToTime(datetime)
				}
			}
			if strings.IndexByte(text, 'T') >= 0 {
				datetime, err := types.ParseDatetime(text, 6)
				if err != nil {
					return 0, true
				}
				return timestampPairDatetimeToTime(datetime)
			}
			parseText := text
			negativeDay := false
			if space := strings.IndexByte(text, ' '); space >= 0 {
				day := text[:space]
				if strings.HasPrefix(day, "-") {
					day = day[1:]
					parseText = text[1:]
					negativeDay = true
				}
				if _, err := strconv.ParseUint(day, 10, 64); err != nil {
					datetime, datetimeErr := types.ParseDatetime(text, 6)
					if datetimeErr != nil {
						return 0, true
					}
					return timestampPairDatetimeToTime(datetime)
				}
			}
			timeValue, err := types.ParseTime(parseText, 6)
			if negativeDay {
				timeValue = -timeValue
			}
			return timeValue, err != nil
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unexpected TIMESTAMP second argument type %s", vec.GetType().Oid)
	}
}

func timestampWithTime(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	dateReader, err := newTimestampPairDateReader(parameters[0], proc)
	if err != nil {
		return err
	}
	timeReader, err := newTimestampPairTimeReader(parameters[1], proc)
	if err != nil {
		return err
	}

	results := vector.MustFunctionResult[types.Datetime](result)
	minimum := int64(types.DatetimeEpoch)
	maximum := int64(types.DatetimeFromClock(types.MaxDatetimeYear, 12, 31, 23, 59, 59, 999999))
	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err = results.Append(0, true); err != nil {
				return err
			}
			continue
		}

		base, invalidBase := dateReader(i)
		delta, invalidDelta := timeReader(i)
		baseValue := int64(base)
		if invalidBase || invalidDelta || baseValue < minimum || baseValue > maximum {
			if err = results.Append(0, true); err != nil {
				return err
			}
			continue
		}

		deltaValue := int64(delta)
		if deltaValue < minimum-baseValue || deltaValue > maximum-baseValue {
			if err = results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if err = results.Append(types.Datetime(baseValue+deltaValue), false); err != nil {
			return err
		}
	}
	return nil
}
