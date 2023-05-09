// Copyright 2023 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToSeconds(t *testing.T) {

	convey.Convey("Test01 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  int64
		}{
			{
				datestr: "1986-06-26",
				expect:  62687347200,
			},
			{
				datestr: "1996-08-03",
				expect:  63006249600,
			},
			{
				datestr: "1995-12-03",
				expect:  62985168000,
			},
			{
				datestr: "1986-12-01",
				expect:  62700998400,
			},
			{
				datestr: "1995-12-01",
				expect:  62984995200,
			},
			{
				datestr: "1996-09-12",
				expect:  63009705600,
			},
			{
				datestr: "1989-09-12",
				expect:  62788780800,
			},
			{
				datestr: "1990-08-05",
				expect:  62817033600,
			},
			{
				datestr: "1996-02-11",
				expect:  62991216000,
			},
			{
				datestr: "1989-02-10",
				expect:  62770291200,
			},
			{
				datestr: "1998-03-11",
				expect:  63056793600,
			},
			{
				datestr: "1985-02-18",
				expect:  62644752000,
			},
			{
				datestr: "1990-02-18",
				expect:  62802518400,
			},
		}

		var datestrs []string
		var expects []int64
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		proc := testutil.NewProc()
		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)

		inputs := []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_datetime.ToType(), datetimes, nil),
		}
		expect := testutil.NewFunctionTestResult(types.T_int64.ToType(), false, expects, nil)
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToSeconds)
		succeed, info := caseNow.Run()
		convey.So(succeed, convey.ShouldBeTrue)
		require.True(t, succeed, fmt.Sprintf("err info is '%s'", info))

		//dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		//expectVector := testutil.MakeInt64Vector(expects, nil)
		//
		//proc := testutil.NewProc()
		//result, err := ToSeconds([]*vector.Vector{dateVector}, proc)
		//if err != nil {
		//	t.Fatal(err)
		//}
		//convey.So(err, convey.ShouldBeNil)
		//compare := testutil.CompareVectors(expectVector, result)
		//convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test02 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  int64
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  63430125154,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  63523350754,
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  31622400,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  63640083599,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  63516268800,
			}, {
				datestr: "2009-10-04 22:23:00",
				expect:  63421914180,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  63358755780,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  59982157380,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  63043222980,
			},
		}

		var datestrs []string
		var expects []int64
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		proc := testutil.NewProc()
		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)

		inputs := []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_datetime.ToType(), datetimes, nil),
		}
		expect := testutil.NewFunctionTestResult(types.T_int64.ToType(), false, expects, nil)
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToSeconds)
		succeed, info := caseNow.Run()
		convey.So(succeed, convey.ShouldBeTrue)
		require.True(t, succeed, fmt.Sprintf("err info is '%s'", info))
		//dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		//expectVector := testutil.MakeInt64Vector(expects, nil)
		//
		//proc := testutil.NewProc()
		//result, err := ToSeconds([]*vector.Vector{dateVector}, proc)
		//if err != nil {
		//	t.Fatal(err)
		//}
		//convey.So(err, convey.ShouldBeNil)
		//compare := testutil.CompareVectors(expectVector, result)
		//convey.So(compare, convey.ShouldBeTrue)
	})

}

// Single row constant input parameter test to_days function
func TestToSecondsWithScalar(t *testing.T) {
	//mp := mpool.MustNewZero()
	//makeDateFormatVectors := func(date string) []*vector.Vector {
	//	vec := make([]*vector.Vector, 2)
	//
	//	datetime, err := types.ParseDatetime(date, 6)
	//	if err != nil {
	//		panic(err)
	//	}
	//
	//	vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), datetime, 1, mp)
	//	return vec
	//}

	cases := []struct {
		name    string
		datestr string
		expect  int64
	}{
		{
			name:    "Test01",
			datestr: "2010-01-07 23:12:34.12345",
			expect:  63430125154,
		},
		{
			name:    "Test02",
			datestr: "2012-12-21 23:12:34.123456",
			expect:  63523350754,
		},
		{
			name:    "Test03",
			datestr: "0001-01-01 00:00:00.123456",
			expect:  31622400,
		},
		{
			name:    "Test04",
			datestr: "2016-09-3 00:59:59.123456",
			expect:  63640083599,
		},
		{
			name:    "Test05",
			datestr: "2012-10-01 00:00:00",
			expect:  63516268800,
		},
		{
			name:    "Test06",
			datestr: "2009-10-04 22:23:00",
			expect:  63421914180,
		},
		{
			name:    "Test07",
			datestr: "2007-10-04 22:23:00",
			expect:  63358755780,
		},
		{
			name:    "Test08",
			datestr: "1900-10-04 22:23:00",
			expect:  59982157380,
		},
		{
			name:    "Test09",
			datestr: "1997-10-04 22:23:00",
			expect:  63043222980,
		},
		{
			name:    "Test10",
			datestr: "1999-01-01",
			expect:  63082368000,
		},
		{
			name:    "Test11",
			datestr: "2006-06-01",
			expect:  63316339200,
		},
		{
			name:    "Test12",
			datestr: "1997-10-04 22:23:00",
			expect:  63043222980,
		},
		{
			name:    "Test13",
			datestr: "1997-10-04 22:23:00",
			expect:  63043222980,
		},
		{
			name:    "Test14",
			datestr: "1997-10-04 22:23:00",
			expect:  63043222980,
		},
	}

	//proc := testutil.NewProc()
	//for _, c := range cases {
	//	t.Run(c.name, func(t *testing.T) {
	//		inputVecs := makeDateFormatVectors(c.datestr)
	//		resVec, err := ToSeconds(inputVecs, proc)
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//		require.Equal(t, c.expect, vector.GetFixedAt[int64](resVec, 0))
	//	})
	//}

	proc := testutil.NewProc()
	for idx, kase := range cases {
		d, err := types.ParseDatetime(kase.datestr, 6)
		if err != nil {
			panic(err)
		}

		inputs := []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{d}, nil),
		}
		expect := testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{kase.expect}, nil)
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToSeconds)
		successd, info := caseNow.Run()
		require.True(t, successd, fmt.Sprintf("case %d, err info is '%s'", idx, info))
	}
}
