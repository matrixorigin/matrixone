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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	DateToTimestamp       = dateToTimestamp
	DatetimeToTimestamp   = datetimeToTimestamp
	DateStringToTimestamp = dateStringToTimestamp
)

func dateToTimestamp(loc *time.Location, xs []types.Date, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i, x := range xs {
		rs[i] = x.ToTimestamp(loc)
		if !types.ValidTimestamp(rs[i]) {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func datetimeToTimestamp(loc *time.Location, xs []types.Datetime, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i, x := range xs {
		rs[i] = x.ToTimestamp(loc)
		if !types.ValidTimestamp(rs[i]) {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
		}
	}
	return rs
}

func dateStringToTimestamp(loc *time.Location, xs []string, ns *nulls.Nulls, rs []types.Timestamp) []types.Timestamp {
	for i, str := range xs {
		t, err := types.ParseTimestamp(loc, str, 6)
		if err != nil {
			rs[i] = 0
			nulls.Add(ns, uint64(i))
			continue
		}
		rs[i] = t
		//for issue5305, do not do this check
		//according to mysql, timestamp function actually return a datetime value
		/*
			if !types.ValidTimestamp(rs[i]) {
				rs[i] = 0
				nulls.Add(ns, uint64(i))
			}
		*/
	}
	return rs
}
