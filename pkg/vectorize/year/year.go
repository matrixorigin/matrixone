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

package year

import "github.com/matrixorigin/matrixone/pkg/container/types"

// vectorize year and toYear function
var (
	DateToYear     func([]types.Date, []uint16) []uint16
	DatetimeToYear func([]types.Datetime, []uint16) []uint16
)

func init() {
	DateToYear = dateToYear
	DatetimeToYear = datetimeToYear
}

func dateToYear(xs []types.Date, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func datetimeToYear(xs []types.Datetime, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}
