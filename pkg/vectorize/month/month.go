// Copyright 2022 Matrix Origin
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

package month

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateToMonth       func([]types.Date, []uint8) []uint8
	DatetimeToMonth   func([]types.Datetime, []uint8) []uint8
	DateStringToMonth func(*types.Bytes, *nulls.Nulls, []uint8) []uint8
)

func init() {
	DateToMonth = dataToMonth
	DatetimeToMonth = datetimeToMonth
	DateStringToMonth = dateStringToMonth
}

func dataToMonth(xs []types.Date, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x.Month()
	}
	return rs
}

func datetimeToMonth(xs []types.Datetime, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x.Month()
	}
	return rs
}

func dateStringToMonth(xs *types.Bytes, ns *nulls.Nulls, rs []uint8) []uint8 {
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		d, e := types.ParseDateCast(str)
		if e != nil {
			// set null
			nulls.Add(ns, uint64(i))
			rs[i] = 0
			continue
		}
		rs[i] = d.Month()
	}
	return rs
}
