// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timediff

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type DiffT interface {
	types.Time | types.Datetime
}

func TimeDiffWithTimeFn[T DiffT](v1, v2 []T, rs []types.Time) error {
	if len(v1) == 1 && len(v2) == 1 {
		for i := 0; i < len(rs); i++ {
			res, err := timeDiff(v1[0], v2[0])
			if err != nil {
				return err
			}
			rs[0] = res
		}
	} else if len(v1) == 1 {
		for i := 0; i < len(rs); i++ {
			res, err := timeDiff(v1[0], v2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(v2) == 1 {
		for i := 0; i < len(rs); i++ {
			res, err := timeDiff(v1[i], v2[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else {
		for i := 0; i < len(rs); i++ {
			res, err := timeDiff(v1[i], v2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	}
	return nil
}

func timeDiff[T DiffT](v1, v2 T) (types.Time, error) {
	tmpTime := int64(v1 - v2)
	// different sign need to check over flow
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
