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

package unary

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Seconds in 0000 AD
const ADZeroSeconds = 31622400

// ToSeconds: InMySQL: Given a date date, returns a day number (the number of days since year 0000). Returns NULL if date is NULL.
// note:  but Matrxone think the date of the first year of the year is 0001-01-01, this function selects compatibility with MySQL
// reference linking: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-seconds
func ToSeconds(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	dateParams := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		datetimeValue, isNull := dateParams.GetValue(i)
		if isNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rs.Append(DateTimeDiff(intervalUnitSECOND, types.ZeroDatetime, datetimeValue)+ADZeroSeconds, false)
	}
	return nil
}

// CalcToSeconds: CalcToDays is used to return a day number (the number of days since year 0)
func CalcToSeconds(ctx context.Context, datetimes []types.Datetime, ns *nulls.Nulls) ([]int64, error) {
	res := make([]int64, len(datetimes))
	for idx, datetime := range datetimes {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		res[idx] = DateTimeDiff(intervalUnitSECOND, types.ZeroDatetime, datetime) + ADZeroSeconds
	}
	return res, nil
}
