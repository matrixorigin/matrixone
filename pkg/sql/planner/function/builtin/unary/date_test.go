// Copyright 2021 - 2022 Matrix Origin
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

func TestDate(t *testing.T) {
	convey.Convey("DateCase", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				s:    "2004-04-03",
				want: "2004-04-03",
			},
			{
				s:    "2021-10-03",
				want: "2021-10-03",
			},
			{
				s:    "2020-08-23",
				want: "2020-08-23",
			},
		}

		var inStrs []string
		var wantStr []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStr = append(wantStr, k.want)
		}

		inVector := testutil.MakeDateVector(inStrs, nil)
		wantVec := testutil.MakeDateVector(wantStr, nil)
		proc := testutil.NewProc()
		res, err := DateToDate([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarDateCase", t, func() {
		type kase struct {
			s    string
			want string
		}

		k := kase{
			s:    "2020-08-23",
			want: "2020-08-23",
		}

		inVector := testutil.MakeScalarDate(k.s, 10)
		wantVec := testutil.MakeScalarDate(k.want, 10)
		proc := testutil.NewProc()
		res, err := DateToDate([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("DateTimeCase", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				s:    "2004-04-03 13:11:10",
				want: "2004-04-03",
			},
			{
				s:    "2021-10-03 15:24:18",
				want: "2021-10-03",
			},
			{
				s:    "2020-08-23 21:53:09",
				want: "2020-08-23",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVec := testutil.MakeDateVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToDate([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarDateTimeCase", t, func() {
		type kase struct {
			s    string
			want string
		}

		k := kase{
			s:    "2021-10-03 15:24:18",
			want: "2021-10-03",
		}

		inVector := testutil.MakeScalarDateTime(k.s, 10)
		wantVec := testutil.MakeScalarDate(k.want, 10)
		proc := testutil.NewProc()
		res, err := DatetimeToDate([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("TestDateScalarNull", t, func() {
		vecs := []*vector.Vector{testutil.MakeScalarNull(types.T_date, 10)}
		proc := testutil.NewProc()
		wantVector := testutil.MakeScalarNull(types.T_date, 10)
		vec, err := DateToDate(vecs, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVector, vec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("TestDatetimeScalarNull", t, func() {
		vecs := []*vector.Vector{testutil.MakeScalarNull(types.T_datetime, 10)}
		proc := testutil.NewProc()
		wantVector := testutil.MakeScalarNull(types.T_date, 10)
		vec, err := DatetimeToDate(vecs, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVector, vec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
