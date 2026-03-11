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

package binary

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestStrToDate(t *testing.T) {
	convey.Convey("Test01 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "04/31/2004",
				expect:  "2004-04-31",
			},
			{
				datestr: "05/31/2012",
				expect:  "2012-05-31",
			},
			{
				datestr: "04/23/2009",
				expect:  "2009-04-23",
			},
			{
				datestr: "01/31/2004",
				expect:  "2004-01-31",
			},
			{
				datestr: "07/03/2018",
				expect:  "2018-07-03",
			},
			{
				datestr: "08/25/2014",
				expect:  "2014-08-25",
			},
			{
				datestr: "06/30/2022",
				expect:  "2022-06-30",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%m/%d/%Y`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeDateVector(expects, []uint64{0})

		proc := testutil.NewProc()
		result, err := StrToDate([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestStrToDate2(t *testing.T) {
	convey.Convey("Test02 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "May 1, 2013",
				expect:  "2013-05-01",
			},
			{
				datestr: "Feb 28, 2022",
				expect:  "2022-02-28",
			},
			{
				datestr: "Jul 20, 2022",
				expect:  "2022-07-20",
			},
			{
				datestr: "Aug 1, 2013",
				expect:  "2013-08-01",
			},
			{
				datestr: "Nov 28, 2022",
				expect:  "2022-11-28",
			},
			{
				datestr: "Dec 20, 2022",
				expect:  "2022-12-20",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%b %d,%Y`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeDateVector(expects, nil)

		proc := testutil.NewProc()
		result, err := StrToDate([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestStrToTime(t *testing.T) {
	convey.Convey("Test03 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "09:30:17",
				expect:  "09:30:17",
			},
			{
				datestr: "11:30:17",
				expect:  "11:30:17",
			},
			{
				datestr: "01:30:17",
				expect:  "01:30:17",
			},
			{
				datestr: "12:30:17",
				expect:  "00:30:17",
			},
			{
				datestr: "05:30:17",
				expect:  "05:30:17",
			},
			{
				datestr: "09:30:17",
				expect:  "09:30:17",
			},
			{
				datestr: "04:30:17",
				expect:  "04:30:17",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%h:%i:%s`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeTimeVector(expects, nil)

		proc := testutil.NewProc()
		result, err := StrToTime([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestStrToTime2(t *testing.T) {
	convey.Convey("Test04 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "11:13:56",
				expect:  "11:13:56",
			},
			{
				datestr: "12:33:51",
				expect:  "00:33:51",
			},
			{
				datestr: "03:23:36",
				expect:  "03:23:36",
			},
			{
				datestr: "01:43:46",
				expect:  "01:43:46",
			},
			{
				datestr: "10:53:41",
				expect:  "10:53:41",
			},
			{
				datestr: "09:23:46",
				expect:  "09:23:46",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%r`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeTimeVector(expects, nil)

		proc := testutil.NewProc()
		result, err := StrToTime([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestStrToDateTime(t *testing.T) {
	convey.Convey("Test05 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "2022-05-27 11:30:00",
				expect:  "2022-05-27 11:30:00",
			},
			{
				datestr: "2012-05-26 12:30:00",
				expect:  "2012-05-26 12:30:00",
			},
			{
				datestr: "2002-07-26 02:30:01",
				expect:  "2002-07-26 02:30:01",
			},
			{
				datestr: "2001-03-26 08:10:01",
				expect:  "2001-03-26 08:10:01",
			},
			{
				datestr: "2011-08-26 07:15:01",
				expect:  "2011-08-26 07:15:01",
			},
			{
				datestr: "2011-11-26 06:15:01",
				expect:  "2011-11-26 06:15:01",
			},
			{
				datestr: "2011-12-26 06:15:01",
				expect:  "2011-12-26 06:15:01",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%Y-%m-%d %H:%i:%s`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeDateTimeVector(expects, nil)

		proc := testutil.NewProc()
		result, err := StrToDateTime([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestStrToDateTime2(t *testing.T) {
	convey.Convey("Test06 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "8:10:2.123456 13-01-02",
				expect:  "2013-01-02 08:10:02.123456",
			},
			{
				datestr: "12:19:2.123456 06-01-02",
				expect:  "2006-01-02 12:19:02.123456",
			},
			{
				datestr: "15:21:2.123456 22-01-02",
				expect:  "2022-01-02 15:21:02.123456",
			},
			{
				datestr: "11:11:2.123456 25-01-02",
				expect:  "2025-01-02 11:11:02.123456",
			},
			{
				datestr: "19:31:2.123456 11-01-02",
				expect:  "2011-01-02 19:31:02.123456",
			},
			{
				datestr: "1:41:2.123456 02-01-02",
				expect:  "2002-01-02 01:41:02.123456",
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		format := `%H:%i:%S.%f %y-%m-%d`
		formatVector := testutil.MakeScalarVarchar(format, 11)
		expectVector := testutil.MakeDateTimeVector(expects, nil)

		proc := testutil.NewProc()
		result, err := StrToDateTime([]*vector.Vector{datestrVector, formatVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func Test_CoreStrToDate(t *testing.T) {
	tests := []struct {
		name   string
		date   string
		format string
		expect GeneralTime
	}{
		{`Test1`, `01,05,2013`, `%d,%m,%Y`, FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`Test2`, `5 12 2021`, `%m%d%Y`, FromDate(2021, 5, 12, 0, 0, 0, 0)},
		{`Test3`, `May 01, 2013`, `%M %d,%Y`, FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`Test4`, `a09:30:17`, `a%h:%i:%s`, FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`Test5`, `09:30:17a`, `%h:%i:%s`, FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`Test6`, `12:43:24`, `%h:%i:%s`, FromDate(0, 0, 0, 0, 43, 24, 0)},
		//{`Test7`, `abc`, `abc`, ZeroCoreTime},
		{`Test8`, `09`, `%m`, FromDate(0, 9, 0, 0, 0, 0, 0)},
		{`Test9`, `09`, `%s`, FromDate(0, 0, 0, 0, 0, 9, 0)},
		{`Test10`, `12:43:24 AM`, `%r`, FromDate(0, 0, 0, 0, 43, 24, 0)},
		{`Test11`, `12:43:24 PM`, `%r`, FromDate(0, 0, 0, 12, 43, 24, 0)},
		{`Test12`, `11:43:24 PM`, `%r`, FromDate(0, 0, 0, 23, 43, 24, 0)},
		{`Test13`, `00:12:13`, `%T`, FromDate(0, 0, 0, 0, 12, 13, 0)},
		{`Test14`, `23:59:59`, `%T`, FromDate(0, 0, 0, 23, 59, 59, 0)},
		//{`Test15`, `00/00/0000`, `%m/%d/%Y`, ZeroCoreTime},
		{`Test16`, `04/30/2004`, `%m/%d/%Y`, FromDate(2004, 4, 30, 0, 0, 0, 0)},
		{`Test17`, `15:35:00`, `%H:%i:%s`, FromDate(0, 0, 0, 15, 35, 0, 0)},
		{`Test18`, `Jul 17 33`, `%b %k %S`, FromDate(0, 7, 0, 17, 0, 33, 0)},
		{`Test19`, `2016-January:7 432101`, `%Y-%M:%l %f`, FromDate(2016, 1, 0, 7, 0, 0, 432101)},
		{`Test20`, `10:13 PM`, `%l:%i %p`, FromDate(0, 0, 0, 22, 13, 0, 0)},
		{`Test21`, `12:00:00 AM`, `%h:%i:%s %p`, FromDate(0, 0, 0, 0, 0, 0, 0)},
		{`Test22`, `12:00:00 PM`, `%h:%i:%s %p`, FromDate(0, 0, 0, 12, 0, 0, 0)},
		{`Test23`, `12:00:00 PM`, `%I:%i:%s %p`, FromDate(0, 0, 0, 12, 0, 0, 0)},
		{`Test24`, `1:00:00 PM`, `%h:%i:%s %p`, FromDate(0, 0, 0, 13, 0, 0, 0)},
		{`Test25`, `18/10/22`, `%y/%m/%d`, FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`Test26`, `8/10/22`, `%y/%m/%d`, FromDate(2008, 10, 22, 0, 0, 0, 0)},
		{`Test27`, `69/10/22`, `%y/%m/%d`, FromDate(2069, 10, 22, 0, 0, 0, 0)},
		{`Test28`, `70/10/22`, `%y/%m/%d`, FromDate(1970, 10, 22, 0, 0, 0, 0)},
		{`Test29`, `18/10/22`, `%Y/%m/%d`, FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`Test30`, `2018/10/22`, `%Y/%m/%d`, FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`Test31`, `8/10/22`, `%Y/%m/%d`, FromDate(2008, 10, 22, 0, 0, 0, 0)},
		{`Test32`, `69/10/22`, `%Y/%m/%d`, FromDate(2069, 10, 22, 0, 0, 0, 0)},
		{`Test33`, `70/10/22`, `%Y/%m/%d`, FromDate(1970, 10, 22, 0, 0, 0, 0)},
		{`Test34`, `18/10/22`, `%Y/%m/%d`, FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`Test35`, `100/10/22`, `%Y/%m/%d`, FromDate(100, 10, 22, 0, 0, 0, 0)},
		{`Test36`, `09/10/1021`, `%d/%m/%y`, FromDate(2010, 10, 9, 0, 0, 0, 0)}, // '%y' only accept up to 2 digits for year
		{`Test37`, `09/10/1021`, `%d/%m/%Y`, FromDate(1021, 10, 9, 0, 0, 0, 0)}, // '%Y' accept up to 4 digits for year
		{`Test38`, `09/10/10`, `%d/%m/%Y`, FromDate(2010, 10, 9, 0, 0, 0, 0)},   // '%Y' will fix the year for only 2 digits
		//'%b'/'%M' should be case insensitive
		{`Test40`, "31/may/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", FromDate(2016, 5, 31, 12, 34, 56, 123400)},
		{`Test41`, "30/april/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", FromDate(2016, 4, 30, 12, 34, 56, 0)},
		{`Test42`, "31/mAy/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", FromDate(2016, 5, 31, 12, 34, 56, 123400)},
		{`Test43`, "30/apRil/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", FromDate(2016, 4, 30, 12, 34, 56, 0)},
		// '%r'
		{`Test45`, " 04 :13:56 AM13/05/2019", "%r %d/%c/%Y", FromDate(2019, 5, 13, 4, 13, 56, 0)},  //
		{`Test46`, "12: 13:56 AM 13/05/2019", "%r%d/%c/%Y", FromDate(2019, 5, 13, 0, 13, 56, 0)},   //
		{`Test47`, "12:13 :56 pm 13/05/2019", "%r %d/%c/%Y", FromDate(2019, 5, 13, 12, 13, 56, 0)}, //
		{`Test48`, "12:3: 56pm  13/05/2019", "%r %d/%c/%Y", FromDate(2019, 5, 13, 12, 3, 56, 0)},   //
		{`Test49`, "11:13:56", "%r", FromDate(0, 0, 0, 11, 13, 56, 0)},                             // EOF before parsing "AM"/"PM"
		{`Test50`, "11:13", "%r", FromDate(0, 0, 0, 11, 13, 0, 0)},                                 // EOF after hh:mm
		{`Test51`, "11:", "%r", FromDate(0, 0, 0, 11, 0, 0, 0)},                                    // EOF after hh:
		{`Test52`, "11", "%r", FromDate(0, 0, 0, 11, 0, 0, 0)},                                     // EOF after hh:
		{`Test53`, "12", "%r", FromDate(0, 0, 0, 0, 0, 0, 0)},                                      // EOF after hh:, and hh=12 -> 0
		// '%T'
		{`Test55`, " 4 :13:56 13/05/2019", "%T %d/%c/%Y", FromDate(2019, 5, 13, 4, 13, 56, 0)},
		{`Test56`, "23: 13:56  13/05/2019", "%T%d/%c/%Y", FromDate(2019, 5, 13, 23, 13, 56, 0)},
		{`Test57`, "12:13 :56 13/05/2019", "%T %d/%c/%Y", FromDate(2019, 5, 13, 12, 13, 56, 0)},
		{`Test58`, "19:3: 56  13/05/2019", "%T %d/%c/%Y", FromDate(2019, 5, 13, 19, 3, 56, 0)},
		{`Test59`, "21:13", "%T", FromDate(0, 0, 0, 21, 13, 0, 0)}, // EOF after hh:mm
		{`Test60`, "21:", "%T", FromDate(0, 0, 0, 21, 0, 0, 0)},    // EOF after hh:
		// More patterns than input string
		{`Test62`, " 2/Jun", "%d/%b/%Y", FromDate(0, 6, 2, 0, 0, 0, 0)},
		//{`Test63`, " liter", "lit era l", ZeroCoreTime},
		// Feb 29 in leap-year
		{`Test65`, "29/Feb/2020 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", FromDate(2020, 2, 29, 12, 34, 56, 0)},
		// When `AllowInvalidDate` is true, check only that the month is in the range from 1 to 12 and the day is in the range from 1 to 31
		{`Test67`, "31/April/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", FromDate(2016, 4, 31, 12, 34, 56, 0)},        // April 31th
		{`Test68`, "29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", FromDate(2021, 2, 29, 12, 34, 56, 0)},          // Feb 29 in non-leap-year
		{`Test69`, "30/Feb/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", FromDate(2016, 2, 30, 12, 34, 56, 123400)}, // Feb 30th

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//ctx := make(map[string]int)
			time := NewGeneralTime()
			gotSuccess := coreStrToDate(context.TODO(), time, tt.date, tt.format)

			require.Truef(t, gotSuccess, "%s failed input=%s format=%s", tt.name, tt.date, tt.format)
			require.Equalf(t, tt.expect, *time, "%s failed input=%s format=%s", tt.name, tt.date, tt.format)
		})
	}
}

func Test_CoreStrToDateErr(t *testing.T) {
	tests := []struct {
		name   string
		date   string
		format string
	}{
		// invalid days when `AllowInvalidDate` is false
		//{"Test01", `04/31/2004`, `%m/%d/%Y`},                        // not exists in the real world
		//{"Test02", "29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f"}, // Feb 29 in non-leap-year

		{"Test03", `512 2021`, `%m%d %Y`}, // MySQL will try to parse '51' for '%m', fail

		{"Test04", `a09:30:17`, `%h:%i:%s`}, // format mismatch
		{"Test05", `12:43:24 a`, `%r`},      // followed by incomplete 'AM'/'PM'
		{"Test06", `23:60:12`, `%T`},        // invalid minute
		{"Test07", `18`, `%l`},
		{"Test08", `00:21:22 AM`, `%h:%i:%s %p`},
		{"Test09", `100/10/22`, `%y/%m/%d`},
		{"Test10", "2010-11-12 11 am", `%Y-%m-%d %H %p`},
		{"Test11", "2010-11-12 13 am", `%Y-%m-%d %h %p`},
		{"Test12", "2010-11-12 0 am", `%Y-%m-%d %h %p`},
		// MySQL accept `SEPTEMB` as `SEPTEMBER`, but we don't want this "feature" in TiDB
		// unless we have to.
		{"Test13", "15 SEPTEMB 2001", "%d %M %Y"},
		// '%r'
		{"Test14", "13:13:56 AM13/5/2019", "%r"},  // hh = 13 with am is invalid
		{"Test15", "00:13:56 AM13/05/2019", "%r"}, // hh = 0 with am is invalid
		{"Test16", "00:13:56 pM13/05/2019", "%r"}, // hh = 0 with pm is invalid
		{"Test17", "11:13:56a", "%r"},             // EOF while parsing "AM"/"PM"
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//ctx := make(map[string]int)
			time := NewGeneralTime()
			gotSuccess := coreStrToDate(context.TODO(), time, tt.date, tt.format)
			require.Falsef(t, gotSuccess, "%s failed input=%s format=%s", tt.name, tt.date, tt.format)
		})
	}

}
