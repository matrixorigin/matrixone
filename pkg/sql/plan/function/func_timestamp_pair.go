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
			typ == types.T_timestamp || typ.IsMySQLString()
	}
	validTimeInput := func(typ types.T) bool {
		return typ == types.T_any || typ == types.T_time || typ.IsMySQLString()
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

func timestampPairReturnType(parameters []types.Type) types.Type {
	scale := int32(0)
	for _, parameter := range parameters {
		parameterScale := parameter.Scale
		if parameter.Oid == types.T_any || parameter.Oid.IsMySQLString() {
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

func newTimestampPairTimeReader(vec *vector.Vector) (timestampPairTimeReader, error) {
	switch vec.GetType().Oid {
	case types.T_time:
		parameter := vector.GenerateFunctionFixedTypeParameter[types.Time](vec)
		return parameter.GetValue, nil
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
			if space := strings.IndexByte(text, ' '); space >= 0 {
				if _, err := strconv.ParseUint(text[:space], 10, 64); err != nil {
					return 0, true
				}
			}
			timeValue, err := types.ParseTime(text, 6)
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
	timeReader, err := newTimestampPairTimeReader(parameters[1])
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
