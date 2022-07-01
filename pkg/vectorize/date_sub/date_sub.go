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
package date_sub

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateSub       func([]types.Date, []int64, []int64, *nulls.Nulls, []types.Date) ([]types.Date, error)
	DatetimeSub   func([]types.Datetime, []int64, []int64, *nulls.Nulls, []types.Datetime) ([]types.Datetime, error)
	DateStringSub func(*types.Bytes, []int64, []int64, *nulls.Nulls, []types.Datetime) ([]types.Datetime, error)
	TimestampSub  func([]types.Timestamp, []int64, []int64, *nulls.Nulls, []types.Timestamp) ([]types.Timestamp, error)
)

func init() {
	DateSub = dateSub
	DatetimeSub = datetimeSub
	DateStringSub = dateStringSub
	TimestampSub = timestampSub
}

func dateSub(xs []types.Date, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Date) ([]types.Date, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(ns, uint64(i))
		}
		return rs, nil
	}
	for i, d := range xs {
		date, success := d.ToTime().AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateType)
		if success {
			rs[i] = date.ToDate()
		} else {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs, nil
}

func datetimeSub(xs []types.Datetime, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Datetime) ([]types.Datetime, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(ns, uint64(i))
		}
		return rs, nil
	}
	for i, d := range xs {
		date, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateTimeType)
		if success {
			rs[i] = date
		} else {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs, nil
}

func dateStringSub(xs *types.Bytes, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Datetime) ([]types.Datetime, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs.Lengths {
			nulls.Add(ns, uint64(i))
			rs[i] = 0
		}
		return rs, nil
	}
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		d, e := types.ParseDatetime(str, 6)
		if e != nil {
			// set null
			nulls.Add(ns, uint64(i))
			rs[i] = 0
			continue
		}
		date, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateTimeType)
		if success {
			rs[i] = date
		} else {
			nulls.Add(ns, uint64(i))
			rs[i] = 0
		}
	}
	return rs, nil
}

func timestampSub(xs []types.Timestamp, ys []int64, zs []int64, ns *nulls.Nulls, rs []types.Timestamp) ([]types.Timestamp, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(ns, uint64(i))
		}
		return rs, nil
	}
	ds := make([]types.Datetime, len(xs))
	ds, err := types.TimestampToDatetime(xs, ds)
	if err != nil {
		return rs, err
	}
	for i, d := range ds {
		d, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.TimeStampType)
		if success {
			rs[i] = types.FromClockUTC(int32(d.Year()), d.Month(), d.Day(), uint8(d.Hour()), uint8(d.Minute()), uint8(d.Sec()), uint32(d.MicroSec()))
		} else {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs, nil
}
