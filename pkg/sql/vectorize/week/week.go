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

package week

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	DateToWeek     func([]types.Date, []uint8) []uint8
	DatetimeToWeek func([]types.Datetime, []uint8) []uint8
)

func init() {
	DateToWeek = dateToWeek
	DatetimeToWeek = datetimeToWeek
}

func dateToWeek(xs []types.Date, rs []uint8) []uint8 {
	for i, x := range xs {
		_, rs[i] = x.WeekOfYear()
	}
	return rs
}

func datetimeToWeek(xs []types.Datetime, rs []uint8) []uint8 {
	for i, x := range xs {
		_, rs[i] = x.WeekOfYear()
	}
	return rs
}
