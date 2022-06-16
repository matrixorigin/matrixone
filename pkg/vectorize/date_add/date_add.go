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

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateAdd       func([]types.Date, []int64, []int64, *nulls.Nulls, []types.Date) []types.Date
	DatetimeAdd   func([]types.Datetime, []int64, []int64, *nulls.Nulls, []types.Datetime) []types.Datetime
	DateStringAdd func(*types.Bytes, []int64, []int64, *nulls.Nulls, *types.Bytes) *types.Bytes
)

func init() {
	DateAdd = dateAdd
	DatetimeAdd = datetimeAdd
	DateStringAdd = dateStringAdd
}

func dateAdd(xs []types.Date, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Date) []types.Date {
	for i, d := range xs {
		date, success := d.ToTime().AddInterval(ys[0], types.IntervalType(zs[0]), true)
		if success {
			rs[i] = date.ToDate()
		} else {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func datetimeAdd(xs []types.Datetime, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Datetime) []types.Datetime {
	for i, d := range xs {
		date, success := d.AddInterval(ys[0], types.IntervalType(zs[0]), false)
		if success {
			rs[i] = date
		} else {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func dateStringAdd(xs *types.Bytes, ys []int64, zs []int64, ns *nulls.Nulls, rs *types.Bytes) *types.Bytes {
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		if types.UnitIsDayOrLarger(types.IntervalType(zs[0])) {
			d, e := types.ParseDate(str)
			if e == nil {
				date, success := d.ToTime().AddInterval(ys[0], types.IntervalType(zs[0]), true)
				if success {
					rs.AppendOnce([]byte(date.ToDate().String()))
				} else {
					nulls.Add(ns, uint64(i))
					rs.AppendOnce([]byte(""))
				}
				continue
			}
		}
		d, e := types.ParseDatetime(str, 0)
		if e != nil {
			// set null
			nulls.Add(ns, uint64(i))
			rs.AppendOnce([]byte(""))
			continue
		}
		date, success := d.AddInterval(ys[0], types.IntervalType(zs[0]), false)
		if success {
			rs.AppendOnce([]byte(date.String()))
		} else {
			nulls.Add(ns, uint64(i))
			rs.AppendOnce([]byte(""))
		}
	}
	return rs
}
