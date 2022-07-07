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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestWeekday(t *testing.T) {
	convey.Convey("WeekDayDateCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "2004-04-03",
				want: 5,
			},
			{
				s:    "2021-10-03",
				want: 6,
			},
			{
				s:    "2020-08-23",
				want: 6,
			},
		}

		var inStrs []string
		var wantInt64 []int64
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantInt64 = append(wantInt64, k.want)
		}

		inVector := testutil.MakeDateVector(inStrs, nil)
		wantVec := testutil.MakeInt64Vector(wantInt64, nil)
		proc := testutil.NewProc()
		res, err := DateToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarWeekDayDateCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "2020-08-23",
			want: 6,
		}

		inVector := testutil.MakeScalarDate(k.s, 10)
		wantVec := testutil.MakeScalarInt64(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("WeekDayDateTimeCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "2004-04-03 13:11:10",
				want: 5,
			},
			{
				s:    "2021-10-03 15:24:18",
				want: 6,
			},
			{
				s:    "2020-08-23 21:53:09",
				want: 6,
			},
		}

		var inStrs []string
		var wantInt64 []int64
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantInt64 = append(wantInt64, k.want)
		}

		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVec := testutil.MakeInt64Vector(wantInt64, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarWeekDayDateTimeCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "2021-10-03 15:24:18",
			want: 6,
		}

		inVector := testutil.MakeScalarDateTime(k.s, 10)
		wantVec := testutil.MakeScalarInt64(k.want, 10)
		proc := testutil.NewProc()
		res, err := DatetimeToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarDateNUllCase", t, func() {
		inVector := testutil.MakeScalarNull(10)
		wantVec := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		res, err := DateToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)

	})

	convey.Convey("ScalarDateTimeNUllCase", t, func() {
		inVector := testutil.MakeScalarNull(10)
		wantVec := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		res, err := DatetimeToWeekday([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)

	})

}
