// Copyright 2022 Matrix Origin
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

package time

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func DateToTime(xs []types.Date, rs []types.Time) []types.Time {
	for i, x := range xs {
		rs[i] = x.ToTime()
	}
	return rs
}

func DatetimeToTime(xs []types.Datetime, rs []types.Time, precision int32) []types.Time {
	for i, x := range xs {
		rs[i] = x.ToTime(precision)
	}
	return rs
}

func DateStringToTime(xs []string, rs []types.Time) ([]types.Time, error) {
	for i, str := range xs {
		t, e := types.ParseTime(str, 6)
		if e != nil {
			return rs, moerr.NewOutOfRange("date", "'%s'", str)
		}
		rs[i] = t
	}
	return rs, nil
}
