// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Month

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DateStringToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(function2Util.QuickBytesToStr(v))
			if e != nil {
				return e
			}
			if err := rs.Append(d.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Year

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DateStringToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// Week

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// Weekday

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
