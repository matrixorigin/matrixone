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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestMonthFunction(t *testing.T) {
	convey.Convey("DateToMonthCase", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		kases := []kase{
			{
				s:    "2004-04-03",
				want: 4,
			},
			{
				s:    "2004-08-03",
				want: 8,
			},
			{
				s:    "2004-01-03",
				want: 1,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeDateVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DateToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateToMonthCaseScalar", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		k := kase{
			s:    "2004-04-03",
			want: 4,
		}

		inVector := testutil.MakeScalarDate(k.s, 10)
		wantVector := testutil.MakeScalarUint8(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateToMonthCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(types.T_date, 10)
		wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
		proc := testutil.NewProc()
		res, err := DateToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToMonthCase", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		kases := []kase{
			{
				s:    "2004-04-03 13:11:10",
				want: 4,
			},
			{
				s:    "1999-08-05 11:01:02",
				want: 8,
			},
			{
				s:    "2004-01-03 23:15:08",
				want: 1,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}
		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToMonthCaseScalar", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		k := kase{
			s:    "2004-01-03 23:15:08",
			want: 1,
		}

		inVector := testutil.MakeScalarDateTime(k.s, 10)
		wantVector := testutil.MakeScalarUint8(k.want, 10)
		proc := testutil.NewProc()
		res, err := DatetimeToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DatetimeToMonthCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(types.T_datetime, 10)
		wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
		proc := testutil.NewProc()
		res, err := DatetimeToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToMonthCase", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		kases := []kase{
			{
				s:    "2014-04-03",
				want: 4,
			},
			{
				s:    "2009-11-03",
				want: 11,
			},
			{
				s:    "2012-07-03",
				want: 7,
			},
			{
				s:    "2012-02-03 18:23:15",
				want: 2,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeVarcharVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DateStringToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToMonthCaseScalar", t, func() {
		type kase struct {
			s    string
			want uint8
		}

		k := kase{
			s:    "2004-01-03 23:15:08",
			want: 1,
		}

		inVector := testutil.MakeScalarChar(k.s, 10)
		wantVector := testutil.MakeScalarUint8(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateStringToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateStringToMonthCaseScalarNull", t, func() {
		inVector := testutil.MakeScalarNull(types.T_char, 10)
		wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
		proc := testutil.NewProc()
		res, err := DateStringToMonth([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

}
