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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// IntegerMakeDateOverload is append-only; identity 0 retains string execution.
const IntegerMakeDateOverload = 1

func MakeDateInteger(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	years := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	days := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	lastDate := types.DateFromCalendar(9999, 12, 31)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		year, yearNull := years.GetValue(i)
		day, dayNull := days.GetValue(i)
		if yearNull || dayNull || year < 0 || year > 9999 || day <= 0 {
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
		firstDate := types.DateFromCalendar(int32(year), 1, 1)
		// Check the calendar bound before narrowing the day offset. Values such
		// as 2^32+1 must not wrap to January 1 in the int32 Date representation.
		if day > int64(lastDate-firstDate)+1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		date := firstDate + types.Date(day-1)
		if err := rs.AppendBytes([]byte(date.String()), false); err != nil {
			return err
		}
	}
	return nil
}
