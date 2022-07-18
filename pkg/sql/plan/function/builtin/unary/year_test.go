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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestYearFunction(t *testing.T) {
	convey.Convey("DateToYearCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "2004-04-03",
				want: 2004,
			},
			{
				s:    "2014-08-03",
				want: 2014,
			},
			{
				s:    "2008-01-03",
				want: 2008,
			},
		}

		var inStrs []string
		var wantint64 []int64
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantint64 = append(wantint64, k.want)
		}

		inVector := testutil.MakeDateVector(inStrs, nil)
		wantVector := testutil.MakeInt64Vector(wantint64, nil)
		proc := testutil.NewProc()
		res, err := DateToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateToYearCaseScalar", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "2004-04-03",
			want: 2004,
		}

		inVector := testutil.MakeScalarDate(k.s, 10)
		wantVector := testutil.MakeScalarInt64(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateToYearCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(10)
		wantVector := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		res, err := DateToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToYearCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "2004-04-03 13:11:10",
				want: 2004,
			},
			{
				s:    "1999-08-05 11:01:02",
				want: 1999,
			},
			{
				s:    "2004-01-03 23:15:08",
				want: 2004,
			},
		}

		var inStrs []string
		var wantInt64 []int64
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantInt64 = append(wantInt64, k.want)
		}
		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVector := testutil.MakeInt64Vector(wantInt64, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToYearCaseScalar", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "2004-01-03 23:15:08",
			want: 2004,
		}

		inVector := testutil.MakeScalarDateTime(k.s, 10)
		wantVector := testutil.MakeScalarInt64(k.want, 10)
		proc := testutil.NewProc()
		res, err := DatetimeToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToYearCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(10)
		wantVector := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		res, err := DatetimeToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToYearCase", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "2014-04-03",
				want: 2014,
			},
			{
				s:    "2009-11-03",
				want: 2009,
			},
			{
				s:    "2012-07-03",
				want: 2012,
			},
			{
				s:    "2012-02-03 18:23:15",
				want: 2012,
			},
		}

		var inStrs []string
		var wantInt64 []int64
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantInt64 = append(wantInt64, k.want)
		}

		inVector := testutil.MakeVarcharVector(inStrs, nil)
		inVector.Length = 4
		wantVector := testutil.MakeInt64Vector(wantInt64, nil)
		proc := testutil.NewProc()
		res, err := DateStringToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToYearCaseScalar", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "2004-01-03 23:15:08",
			want: 2004,
		}

		inVector := testutil.MakeScalarChar(k.s, 10)
		wantVector := testutil.MakeScalarInt64(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateStringToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToYearCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(10)
		wantVector := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		res, err := DateStringToYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}
