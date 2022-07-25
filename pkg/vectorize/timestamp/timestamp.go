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

package timestamp

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateToTimestamp       func([]types.Date, *nulls.Nulls, []types.Timestamp) []types.Timestamp
	DatetimeToTimestamp   func([]types.Datetime, *nulls.Nulls, []types.Timestamp) []types.Timestamp
	DateStringToTimestamp func(*types.Bytes, *nulls.Nulls, []types.Timestamp) []types.Timestamp
)

func init() {
	DateToTimestamp = dateToTimestamp
	DatetimeToTimestamp = datetimeToTimestamp
	DateStringToTimestamp = dateStringToTimestamp
}

func dateToTimestamp(xs []types.Date, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i := range xs {
		rs[i] = xs[i].ToTimeUTC()
		if !types.ValidTimestamp(rs[i]) {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func datetimeToTimestamp(xs []types.Datetime, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i, x := range xs {
		rs[i] = types.FromClockUTC(int32(x.Year()), x.Month(), x.Day(), uint8(x.Hour()), uint8(x.Minute()), uint8(x.Sec()), uint32(x.MicroSec()))
		if !types.ValidTimestamp(rs[i]) {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func dateStringToTimestamp(xs *types.Bytes, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i := range xs.Lengths {
		t, err := types.ParseTimestamp(string(xs.Get(int64(i))), 6)
		if err != nil {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
			continue
		}
		rs[i] = t
		if !types.ValidTimestamp(rs[i]) {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}
