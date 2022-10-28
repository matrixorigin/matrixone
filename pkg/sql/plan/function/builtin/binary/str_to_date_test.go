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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStrToDate(t *testing.T) {
	convey.Convey("Test01 STR_TO_DATE() with multi line", t, func() {
		cases := []struct {
			datestr string
			format  string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 01 53 01 Mon Monday 1 0000 0001 0001 01 %`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `Sep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16 %`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `Oct October 10 10 1st 01 1 275 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 40 40 40 40 Mon Monday 1 2012 2012 2012 12 %`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 40 40 40 Sun Sunday 0 2009 2009 2009 09 %`,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Thu Thursday 4 2007 2007 2007 07 %`,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Thu Thursday 4 1900 1900 1900 00 %`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Sat Saturday 6 1997 1997 1997 97 %`,
			},
			{
				datestr: "1999-01-01",
				expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 00 00 52 53 Fri Friday 5 1998 1998 1999 99 %`,
			},
			{
				datestr: "2006-06-01",
				expect:  `Jun June 06 6 1st 01 1 152 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 22 22 22 22 Thu Thursday 4 2006 2006 2006 06 %`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)

		format := `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`
		formatVector := testutil.MakeScalarVarchar(format, 11)

		expectVector := testutil.MakeVarcharVector(expects, nil)

		proc := testutil.NewProc()
		result, err := DateFormat([]*vector.Vector{dateVector, formatVector}, proc)
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
		expect CoreTime
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
			time := NewCoreTime()
			gotSuccess := CoreStrToDate(time, tt.date, tt.format)

			require.Truef(t, gotSuccess, "%s failed input=%s format=%s", tt.name, tt.date, tt.format)
			require.Equalf(t, tt.expect, *time, "%s failed input=%s format=%s", tt.name, tt.date, tt.format)
		})
	}
}
