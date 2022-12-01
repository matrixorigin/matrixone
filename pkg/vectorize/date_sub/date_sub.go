// Copyright 2021 Matrix Origin
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
package date_sub

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func DateSub(xs []types.Date, ys []int64, zs []int64, xns *nulls.Nulls, yns *nulls.Nulls, rns *nulls.Nulls, rs []types.Date) ([]types.Date, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(rns, uint64(i))
		}
		return rs, nil
	}

	for _, y := range ys {
		err := types.JudgeIntervalNumOverflow(y, types.IntervalType(zs[0]))
		if err != nil {
			return rs, err
		}
	}
	if xs == nil {
		for i := range ys {
			nulls.Add(rns, uint64(i))
		}
	} else if len(xs) == len(ys) {
		for i, d := range xs {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.ToDatetime().AddInterval(-ys[i], types.IntervalType(zs[0]), types.DateType)
			if success {
				rs[i] = date.ToDate()
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("date", "")
			}
		}
	} else if len(xs) == 1 {
		for i, d := range ys {
			if nulls.Contains(xns, uint64(0)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := xs[0].ToDatetime().AddInterval(-d, types.IntervalType(zs[0]), types.DateType)
			if success {
				rs[i] = date.ToDate()
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("date", "")
			}
		}
	} else if len(ys) == 1 {
		for i, d := range xs {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.ToDatetime().AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateType)
			if success {
				rs[i] = date.ToDate()
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("date", "")
			}
		}
	}
	return rs, nil
}

func DatetimeSub(xs []types.Datetime, ys []int64, zs []int64, xns *nulls.Nulls, yns *nulls.Nulls, rns *nulls.Nulls, rs []types.Datetime) ([]types.Datetime, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(rns, uint64(i))
		}
		return rs, nil
	}

	for _, y := range ys {
		err := types.JudgeIntervalNumOverflow(y, types.IntervalType(zs[0]))
		if err != nil {
			return rs, err
		}
	}
	if xs == nil {
		for i := range ys {
			nulls.Add(rns, uint64(i))
		}
	} else if len(xs) == len(ys) {
		for i, d := range xs {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[i], types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	} else if len(xs) == 1 {
		for i, d := range ys {
			if nulls.Contains(xns, uint64(0)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := xs[0].AddInterval(-d, types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	} else if len(ys) == 1 {
		for i, d := range xs {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	}
	return rs, nil
}

func DateStringSub(xs []string, ys []int64, zs []int64, xns *nulls.Nulls, yns *nulls.Nulls, rns *nulls.Nulls, rs []types.Datetime) ([]types.Datetime, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(rns, uint64(i))
			rs[i] = 0
		}
		return rs, nil
	}
	for _, y := range ys {
		err := types.JudgeIntervalNumOverflow(y, types.IntervalType(zs[0]))
		if err != nil {
			return rs, err
		}
	}
	ds := make([]types.Datetime, len(xs))
	for i, str := range xs {
		if nulls.Contains(xns, uint64(i)) {
			continue
		}
		d, e := types.ParseDatetime(str, 6)
		if e != nil {
			return rs, e
		}
		ds[i] = d
	}
	if xs == nil {
		for i := range ys {
			nulls.Add(rns, uint64(i))
		}
		return rs, nil
	} else if len(ds) == len(ys) {
		for i, d := range ds {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[i], types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	} else if len(ds) == 1 {
		for i, d := range ys {
			if nulls.Contains(xns, uint64(0)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := ds[0].AddInterval(-d, types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	} else if len(ys) == 1 {
		for i, d := range ds {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.DateTimeType)
			if success {
				rs[i] = date
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("datetime", "")
			}
		}
	}
	return rs, nil
}

func TimestampSub(loc *time.Location, xs []types.Timestamp, ys []int64, zs []int64, xns *nulls.Nulls, yns *nulls.Nulls, rns *nulls.Nulls, rs []types.Timestamp) ([]types.Timestamp, error) {
	if len(ys) == 0 || len(zs) == 0 {
		for i := range xs {
			nulls.Add(rns, uint64(i))
		}
		return rs, nil
	}
	for _, y := range ys {
		err := types.JudgeIntervalNumOverflow(y, types.IntervalType(zs[0]))
		if err != nil {
			return rs, err
		}
	}
	ds := make([]types.Datetime, len(xs))
	ds, err := types.TimestampToDatetime(loc, xs, ds)
	if err != nil {
		return rs, err
	}
	if xs == nil {
		for i := range ys {
			nulls.Add(rns, uint64(i))
		}
	} else if len(xs) == len(ys) {
		for i, d := range ds {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[i], types.IntervalType(zs[0]), types.TimeStampType)
			if success {
				ts := date.ToTimestamp(loc)
				if ts < 0 {
					return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
				}
				rs[i] = ts
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
			}
		}
	} else if len(xs) == 1 {
		for i, d := range ys {
			if nulls.Contains(xns, uint64(0)) || nulls.Contains(yns, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := ds[0].AddInterval(-d, types.IntervalType(zs[0]), types.TimeStampType)
			if success {
				ts := date.ToTimestamp(loc)
				if ts < 0 {
					return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
				}
				rs[i] = ts
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
			}
		}
	} else if len(ys) == 1 {
		for i, d := range ds {
			if nulls.Contains(xns, uint64(i)) || nulls.Contains(yns, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			date, success := d.AddInterval(-ys[0], types.IntervalType(zs[0]), types.TimeStampType)
			if success {
				ts := date.ToTimestamp(loc)
				if ts < 0 {
					return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
				}
				rs[i] = ts
			} else {
				return rs, moerr.NewOutOfRangeNoCtx("timestamp", "")
			}
		}
	}
	return rs, nil
}
