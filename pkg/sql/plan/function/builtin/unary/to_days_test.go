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

func TestToDays(t *testing.T) {
	convey.Convey("Test01 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  int64
		}{
			{
				datestr: "1986-06-26",
				expect:  725548,
			},
			{
				datestr: "1996-08-03",
				expect:  729239,
			},
			{
				datestr: "1995-12-03",
				expect:  728995,
			},
			{
				datestr: "1986-12-01",
				expect:  725706,
			},
			{
				datestr: "1995-12-01",
				expect:  728993,
			},
			{
				datestr: "1996-09-12",
				expect:  729279,
			},
			{
				datestr: "1989-09-12",
				expect:  726722,
			},
			{
				datestr: "1990-08-05",
				expect:  727049,
			},
			{
				datestr: "1996-02-11",
				expect:  729065,
			},
			{
				datestr: "1989-02-10",
				expect:  726508,
			},
			{
				datestr: "1998-03-11",
				expect:  729824,
			},
			{
				datestr: "1985-02-18",
				expect:  725055,
			},
			{
				datestr: "1990-02-18",
				expect:  726881,
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
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToDays)
		succeed, info := caseNow.Run()
		convey.So(succeed, convey.ShouldBeTrue)
		require.True(t, succeed, fmt.Sprintf("err info is '%s'", info))

	})

	convey.Convey("Test02 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  int64
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  734144,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  735223,
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  366,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  736575,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  735142,
			}, {
				datestr: "2009-10-04 22:23:00",
				expect:  734049,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  733318,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  694237,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  729666,
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
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToDays)
		succeed, info := caseNow.Run()
		convey.So(succeed, convey.ShouldBeTrue)
		require.True(t, succeed, fmt.Sprintf("err info is '%s'", info))
	})
}

// Single row constant input parameter test to_days function
func TestToDaysWithScalar(t *testing.T) {
	cases := []struct {
		name    string
		datestr string
		expect  int64
	}{
		{
			name:    "Test01",
			datestr: "2010-01-07 23:12:34.12345",
			expect:  734144,
		},
		{
			name:    "Test02",
			datestr: "2012-12-21 23:12:34.123456",
			expect:  735223,
		},
		{
			name:    "Test03",
			datestr: "0001-01-01 00:00:00.123456",
			expect:  366,
		},
		{
			name:    "Test04",
			datestr: "2016-09-3 00:59:59.123456",
			expect:  736575,
		},
		{
			name:    "Test05",
			datestr: "2012-10-01 00:00:00",
			expect:  735142,
		},
		{
			name:    "Test06",
			datestr: "2009-10-04 22:23:00",
			expect:  734049,
		},
		{
			name:    "Test07",
			datestr: "2007-10-04 22:23:00",
			expect:  733318,
		},
		{
			name:    "Test08",
			datestr: "1900-10-04 22:23:00",
			expect:  694237,
		},
		{
			name:    "Test09",
			datestr: "1997-10-04 22:23:00",
			expect:  729666,
		},
		{
			name:    "Test10",
			datestr: "1999-01-01",
			expect:  730120,
		},
		{
			name:    "Test11",
			datestr: "2006-06-01",
			expect:  732828,
		},
		{
			name:    "Test12",
			datestr: "1997-10-04 22:23:00",
			expect:  729666,
		},
		{
			name:    "Test13",
			datestr: "1997-10-04 22:23:00",
			expect:  729666,
		},
		{
			name:    "Test14",
			datestr: "1997-10-04 22:23:00",
			expect:  729666,
		},
	}

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
		caseNow := testutil.NewFunctionTestCase(proc, inputs, expect, ToDays)
		successd, info := caseNow.Run()
		require.True(t, successd, fmt.Sprintf("case %d, err info is '%s'", idx, info))
	}
}

func TestDateTimeDiff(t *testing.T) {
	tests := []struct {
		intervalUnit string
		t1           types.Datetime
		t2           types.Datetime
		expect       int64
	}{
		{
			"MONTH",
			types.DatetimeFromClock(2002, 5, 30, 0, 0, 0, 0),
			types.DatetimeFromClock(2001, 1, 1, 0, 0, 0, 0),
			-16,
		},
		{
			"YEAR",
			types.DatetimeFromClock(2002, 5, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2001, 1, 1, 0, 0, 0, 0),
			-1,
		},
		{
			"MINUTE",
			types.DatetimeFromClock(2003, 2, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2003, 5, 1, 12, 5, 55, 0),
			128885,
		},
		{
			"MICROSECOND",
			types.DatetimeFromClock(2002, 5, 30, 0, 0, 0, 0),
			types.DatetimeFromClock(2002, 5, 30, 0, 13, 25, 0),
			805000000,
		},
		{
			"MICROSECOND",
			types.DatetimeFromClock(2000, 1, 1, 0, 0, 0, 12345),
			types.DatetimeFromClock(2000, 1, 1, 0, 0, 45, 32),
			44987687,
		},
		{
			"QUARTER",
			types.DatetimeFromClock(2000, 1, 12, 0, 0, 0, 0),
			types.DatetimeFromClock(2016, 1, 1, 0, 0, 0, 0),
			63,
		},
		{
			"QUARTER",
			types.DatetimeFromClock(2016, 1, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2000, 1, 12, 0, 0, 0, 0),
			-63,
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expect, DateTimeDiff(test.intervalUnit, test.t1, test.t2))
	}
}
