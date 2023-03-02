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

package binary

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

// Batch multi line test date_format() function
func TestDateFormat(t *testing.T) {
	convey.Convey("Test01 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
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
			}, {
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

	convey.Convey("Test02 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 2010 2010 10 %`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  `Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 2012 2012 12 %`,
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 01 0001 0001 01 %`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `Sep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 2016 2016 16 %`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `Oct October 10 10 1st 01 1 275 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 2009 2009 09 %`,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 2007 2007 07 %`,
			},
			{
				//SELECT DATE_FORMAT('1900-10-04 22:23:00','%D %y %a %d %m %b %j');
				datestr: "1900-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 1900 1900 00 %`,
			},
			{
				// SELECT DATE_FORMAT('1997-10-04 22:23:00','%H %k %I %r %T %S %w');
				datestr: "1997-10-04 22:23:00",
				expect:  `Oct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 1997 1997 97 %`,
			},
			{
				// SELECT DATE_FORMAT('1999-01-01', '%X %V');
				datestr: "1999-01-01",
				expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 53 1998 1999 99 %`,
			},
			{
				// SELECT DATE_FORMAT('2006-06-00', '%d');
				datestr: "2006-06-01",
				expect:  `Jun June 06 6 1st 01 1 152 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 22 2006 2006 06 %`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%`
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

	convey.Convey("Test03 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `abcJan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10!123 %xyz z`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  `abcDec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12!123 %xyz z`,
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `abcJan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 01 53 01 Mon Monday 1 0000 0001 0001 01!123 %xyz z`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `abcOct October 10 10 1st 01 1 275 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 40 40 40 40 Mon Monday 1 2012 2012 2012 12!123 %xyz z`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `abcOct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 40 40 40 40 Sun Sunday 0 2009 2009 2009 09!123 %xyz z`,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  `abcOct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Thu Thursday 4 2007 2007 2007 07!123 %xyz z`,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  `abcOct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Thu Thursday 4 1900 1900 1900 00!123 %xyz z`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `abcOct October 10 10 4th 04 4 277 22 10 23 PM 10:23:00 PM 22:23:00 00 000000 39 40 39 40 Sat Saturday 6 1997 1997 1997 97!123 %xyz z`,
			},
			{
				datestr: "1999-01-01",
				expect:  `abcJan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 00 00 52 53 Fri Friday 5 1998 1998 1999 99!123 %xyz z`,
			},
			{
				datestr: "2006-06-01",
				expect:  `abcJun June 06 6 1st 01 1 152 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 22 22 22 22 Thu Thursday 4 2006 2006 2006 06!123 %xyz z`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`
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

	convey.Convey("Test04 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `07`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  "21",
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `01`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `03`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `01`,
			},
			{
				//SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
				datestr: "2009-10-04 22:23:00",
				expect:  `04`,
			},
			{
				//SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
				datestr: "2007-10-04 22:23:00",
				expect:  `04`,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  `04`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `04`,
			},
			{
				datestr: "1999-01-01",
				expect:  `01`,
			},
			{
				datestr: "2006-06-01",
				expect:  `01`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `%d`
		formatVector := testutil.MakeScalarVarchar(format, 1)
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

	convey.Convey("Test05 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `2010-01-07 23:12:34`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  "2012-12-21 23:12:34",
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `0001-01-01 00:00:00`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `2016-09-03 00:59:59`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `2012-10-01 00:00:00`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `2009-10-04 22:23:00`,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  `2007-10-04 22:23:00`,
			},
			{
				datestr: "1999-01-01",
				expect:  `1999-01-01 00:00:00`,
			},
			{
				datestr: "2006-06-01",
				expect:  `2006-06-01 00:00:00`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `1997-10-04 22:23:00`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `%Y-%m-%d %H:%i:%s`
		formatVector := testutil.MakeScalarVarchar(format, 10)
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

	convey.Convey("Test06 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `2010-01-07`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  "2012-12-21",
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `0001-01-01`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `2016-09-03`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `2012-10-01`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `2009-10-04`,
			},
			{
				datestr: "2007-10-04 22:23:00",
				expect:  `2007-10-04`,
			},
			{
				datestr: "1900-10-04 22:23:00",
				expect:  `1900-10-04`,
			},
			{
				datestr: "1999-01-01",
				expect:  `1999-01-01`,
			},
			{
				datestr: "2006-06-01",
				expect:  `2006-06-01`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `1997-10-04`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `%Y-%m-%d`
		formatVector := testutil.MakeScalarVarchar(format, 1)
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

	convey.Convey("Test07 Date_Format() with multi line", t, func() {
		cases := []struct {
			datestr string
			expect  string
		}{
			{
				datestr: "2010-01-07 23:12:34.12345",
				expect:  `2010 01`,
			},
			{
				datestr: "2012-12-21 23:12:34.123456",
				expect:  "2012 51",
			},
			{
				datestr: "0001-01-01 00:00:00.123456",
				expect:  `0000 53`,
			},
			{
				datestr: "2016-09-3 00:59:59.123456",
				expect:  `2016 35`,
			},
			{
				datestr: "2012-10-01 00:00:00",
				expect:  `2012 40`,
			},
			{
				datestr: "2009-10-04 22:23:00",
				expect:  `2009 40`,
			},
			{
				datestr: "1999-01-01",
				expect:  `1998 52`,
			},
			{
				datestr: "2006-06-01",
				expect:  `2006 22`,
			},
			{
				datestr: "1997-10-04 22:23:00",
				expect:  `1997 39`,
			},
		}

		var datestrs []string
		var expects []string
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		format := `%X %V`
		formatVector := testutil.MakeScalarVarchar(format, 1)
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

// Single row constant input parameter test date_format function
func TestDateFormatWithScalar(t *testing.T) {
	mp := mpool.MustNewZero()
	// Construct vector parameter of date_format() function
	makeDateFormatVectors := func(date string, format string) []*vector.Vector {
		vec := make([]*vector.Vector, 2)

		datetime, err := types.ParseDatetime(date, 6)
		if err != nil {
			panic(err)
		}

		vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), datetime, 1, mp)
		vec[1] = vector.NewConstBytes(types.T_varchar.ToType(), []byte(format), 1, mp)
		return vec
	}

	cases := []struct {
		name    string
		datestr string
		format  string
		expect  string
	}{
		{
			name:    "Test01",
			datestr: "2010-01-07 23:12:34.12345",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`,
		},
		{
			name:    "Test02",
			datestr: "2012-12-21 23:12:34.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
		},
		{
			name:    "Test03",
			datestr: "0001-01-01 00:00:00.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 01 0001 0001 01 %`,
		},
		{
			name:    "Test04",
			datestr: "2016-09-3 00:59:59.123456",
			format:  `abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`,
			expect:  `abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`,
		},
		{
			name:    "Test05",
			datestr: "2012-10-01 00:00:00",
			format:  `%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`,
		},
		{
			//SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
			name:    "Test06",
			datestr: "2009-10-04 22:23:00",
			format:  `%W %M %Y`,
			expect:  `Sunday October 2009`,
		},
		{
			//SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
			name:    "Test07",
			datestr: "2007-10-04 22:23:00",
			format:  `%H:%i:%s`,
			expect:  `22:23:00`,
		},
		{
			//SELECT DATE_FORMAT('1900-10-04 22:23:00','%D %y %a %d %m %b %j');
			name:    "Test08",
			datestr: "1900-10-04 22:23:00",
			format:  `%D %y %a %d %m %b %j`,
			expect:  `4th 00 Thu 04 10 Oct 277`,
		},
		{
			// SELECT DATE_FORMAT('1997-10-04 22:23:00','%H %k %I %r %T %S %w');
			name:    "Test09",
			datestr: "1997-10-04 22:23:00",
			format:  `%H %k %I %r %T %S %w`,
			expect:  `22 22 10 10:23:00 PM 22:23:00 00 6`,
		},
		{
			// SELECT DATE_FORMAT('1999-01-01', '%X %V');
			name:    "Test10",
			datestr: "1999-01-01",
			format:  `%X %V`,
			expect:  `1998 52`,
		},
		{
			// SELECT DATE_FORMAT('2006-06-00', '%d');
			name:    "Test11",
			datestr: "2006-06-01",
			format:  `%d`,
			expect:  `01`,
		},
		{
			// SELECT DATE_FORMAT('1997-10-04 22:23:00','%Y-%m-%d %H:%i:%s');
			name:    "Test12",
			datestr: "1997-10-04 22:23:00",
			format:  `%Y-%m-%d %H:%i:%s`,
			expect:  `1997-10-04 22:23:00`,
		},
		{
			// SELECT DATE_FORMAT('1997-10-04 22:23:00','%Y-%m');
			name:    "Test13",
			datestr: "1997-10-04 22:23:00",
			format:  `%Y-%m`,
			expect:  `1997-10`,
		},
		{
			// SELECT DATE_FORMAT('1997-10-04 22:23:00','%Y-%m-%d');
			name:    "Test14",
			datestr: "1997-10-04 22:23:00",
			format:  `%Y-%m-%d`,
			expect:  `1997-10-04`,
		},
	}

	proc := testutil.NewProc()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inputVecs := makeDateFormatVectors(c.datestr, c.format)
			formatVec, err := DateFormat(inputVecs, proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, []byte(c.expect), formatVec.GetBytesAt(0))
		})
	}
}

func TestDatetimeFromat(t *testing.T) {
	kases := []struct {
		datestr string
		format  []string
		Expect  []string
	}{
		{
			datestr: "2010-01-07 23:12:34.12345",
			format:  []string{`%b`, `%M`, `%m`, `%c`, `%D`, `%d`, `%e`, `%j`, `%k`, `%h`, `%i`, `%p`, `%r`, `%T`, `%s`, `%f`, `%U`, `%u`, `%V`, `%v`, `%a`, `%W`, `%w`, `%X`, `%x`, `%Y`, `%y`, `%%`},
			Expect:  []string{`Jan`, `January`, `01`, `1`, `7th`, `07`, `7`, `007`, `23`, `11`, `12`, `PM`, `11:12:34 PM`, `23:12:34`, `34`, `123450`, `01`, `01`, `01`, `01`, `Thu`, `Thursday`, `4`, `2010`, `2010`, `2010`, `10`, `%`},
		},
	}

	for _, k := range kases {
		datetime, err := types.ParseDatetime(k.datestr, 5)
		if err != nil {
			t.Fatalf("parse datetime string err %+v", err)
		}

		for i := 0; i < len(k.format); i++ {
			res, err := datetimeFormat(context.TODO(), datetime, k.format[i])
			if err != nil {
				t.Fatalf("dateformat exec error: %+v", err)
			}
			require.Equal(t, k.Expect[i], res)
		}
	}

	cases := []struct {
		name    string
		datestr string
		format  string
		expect  string
	}{
		{
			name:    "Test01",
			datestr: "2010-01-07 23:12:34.12345",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`,
		},
		{
			name:    "Test02",
			datestr: "2012-12-21 23:12:34.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
		},
		{
			name:    "Test03",
			datestr: "0001-01-01 00:00:00.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 01 0001 0001 01 %`,
		},
		{
			name:    "Test04",
			datestr: "2016-09-3 00:59:59.123456",
			format:  `abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`,
			expect:  `abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`,
		},
		{
			datestr: "2012-10-01 00:00:00",
			format:  `%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			datetime, err := types.ParseDatetime(c.datestr, 6)
			if err != nil {
				t.Fatalf("parse datetime string err %+v", err)
			}
			result, err := datetimeFormat(context.TODO(), datetime, c.format)
			if err != nil {
				t.Fatalf("dateformat exec error: %+v", err)
			}
			require.Equal(t, c.expect, result)
		})
	}
}

func TestFormatIntByWidth(t *testing.T) {
	cases := []struct {
		name  string
		num   int
		width int
		want  string
	}{
		{"Test01", 0, 0, "0"},
		{"Test02", 1, 0, "1"},
		{"Test03", 1, 1, "1"},
		{"Test04", 1, 2, "01"},
		{"Test05", 1, 3, "001"},
		{"Test06", 10, 2, "10"},
		{"Test07", 99, 4, "0099"},
		{"Test08", 100, 3, "100"},
		{"Test09", 888, 3, "888"},
		{"Test10", 428, 4, "0428"},
		{"Test11", 100000, 5, "100000"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := FormatIntByWidth(c.num, c.width)
			require.Equal(t, c.want, res)
		})
	}
}
