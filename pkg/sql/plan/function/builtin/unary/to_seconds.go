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

// ToSeconds: InMySQL: Given a date date, returns a day number (the number of days since year 0). Returns NULL if date is NULL.
// note:  but Matrxone think the date of the first year of the year is 0001-01-01, this function selects compatibility with MySQL
// reference linking: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days
func ToSeconds(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	rtyp := types.T_int64.ToType()

	if dateVector.IsConstNull() {
		rvec := vector.NewConstNull(rtyp, dateVector.Length(), proc.Mp())
		return rvec, nil
	}

	if dateVector.IsConst() {
		// XXX Null handling maybe broken.
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcToSeconds(proc.Ctx, datetimes, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, resCol[0], dateVector.Length(), proc.Mp()), nil
	} else {
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcToSeconds(proc.Ctx, datetimes, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		nulls.Set(rvec.GetNulls(), dateVector.GetNulls())
		vector.AppendFixedList(rvec, resCol, nil, proc.Mp())
		return rvec, nil
	}
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
