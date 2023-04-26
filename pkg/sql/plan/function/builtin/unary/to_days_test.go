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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := ToDays([]*vector.Vector{dateVector}, proc)
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

		dateVector := testutil.MakeDateTimeVector(datestrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := ToDays([]*vector.Vector{dateVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// Single row constant input parameter test to_days function
func TestToDaysWithScalar(t *testing.T) {
	mp := mpool.MustNewZero()
	makeDateFormatVectors := func(date string) []*vector.Vector {
		vec := make([]*vector.Vector, 2)

		datetime, err := types.ParseDatetime(date, 6)
		if err != nil {
			panic(err)
		}

		vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), datetime, 1, mp)
		return vec
	}

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
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inputVecs := makeDateFormatVectors(c.datestr)
			resVec, err := ToDays(inputVecs, proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.expect, vector.GetFixedAt[int64](resVec, 0))
		})
	}
}
