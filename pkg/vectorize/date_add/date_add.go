// Copyright 2021 Matrix Origin
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
package date_add

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	DateAdd     func([]types.Date, []int64, []int64, []types.Date) []types.Date
	DatetimeAdd func([]types.Datetime, []int64, []int64, []types.Datetime) []types.Datetime
)

func init() {
	DateAdd = dateAdd
	DatetimeAdd = datetimeAdd
}

func dateAdd(xs []types.Date, ys []int64, zs []int64, rs []types.Date) []types.Date {
	for i, d := range xs {
		rs[i] = d.ToTime().AddInterval(ys[i], types.IntervalType(zs[i])).ToDate()
	}
	return rs
}

func datetimeAdd(xs []types.Datetime, ys []int64, zs []int64, rs []types.Datetime) []types.Datetime {
	for i, d := range xs {
		rs[i] = d.AddInterval(ys[i], types.IntervalType(zs[i]))
	}
	return rs
}
