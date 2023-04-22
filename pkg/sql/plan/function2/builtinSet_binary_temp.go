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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/timediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ENDSWITH

func EndsWith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// EXTRACT

func ExtractFromDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func ExtractFromDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func ExtractFromTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func ExtractFromVarchar(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// FINDINSET

func FindInSet(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// INSTR

func Instr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// LEFT

func Left(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// STARTSWITH

func Startswith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

//POW

func Power(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// TIMEDIFF

func TimeDiff[T timediff.DiffT](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {

	p1 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	//TODO: ignoring scale: Original code: https://github.com/m-schen/matrixone/blob/a4b3a641c3daaa10972f17db091c0eb88554c5c2/pkg/sql/plan/function/builtin/binary/timediff.go#L33
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res, _ := timeDiff(v1, v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeDiff[T timediff.DiffT](v1, v2 T) (types.Time, error) {
	tmpTime := int64(v1 - v2)
	// different sign need to check overflow
	if (int64(v1)>>63)^(int64(v2)>>63) != 0 {
		if (tmpTime>>63)^(int64(v1)>>63) != 0 {
			// overflow
			isNeg := int64(v1) < 0
			return types.TimeFromClock(isNeg, types.MaxHourInTime, 59, 59, 0), nil
		}
	}

	// same sign don't need to check overflow
	time := types.Time(tmpTime)
	hour, _, _, _, isNeg := time.ClockFormat()
	if !types.ValidTime(uint64(hour), 0, 0) {
		return types.TimeFromClock(isNeg, types.MaxHourInTime, 59, 59, 0), nil
	}
	return time, nil
}

// TIMESTAMPDIFF

func TimestampDiff(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	return nil
}
