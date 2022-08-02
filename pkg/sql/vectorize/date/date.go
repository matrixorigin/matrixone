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

package date

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateToDate       func([]types.Date, []types.Date) []types.Date
	DatetimeToDate   func([]types.Datetime, []types.Date) []types.Date
	DateStringToDate func(*types.Bytes, []types.Date) ([]types.Date, error)
)

func init() {
	DateToDate = dateToDate
	DatetimeToDate = datetimeToDate
	DateStringToDate = dateStringToDate
}

func dateToDate(xs []types.Date, rs []types.Date) []types.Date {
	return xs
}

func datetimeToDate(xs []types.Datetime, rs []types.Date) []types.Date {
	for i, x := range xs {
		rs[i] = x.ToDate()
	}
	return rs
}

func dateStringToDate(xs *types.Bytes, rs []types.Date) ([]types.Date, error) {
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		d, e := types.ParseDatetime(str, 6)
		if e != nil {
			return rs, types.ErrIncorrectDateValue
		}
		rs[i] = d.ToDate()
	}
	return rs, nil
}
