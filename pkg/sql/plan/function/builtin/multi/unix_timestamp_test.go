// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi

import (
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestUnixTimestamp(t *testing.T) {
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-01 22:23:00"), 1641075780, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-02 22:23:00"), 1641162180, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-03 22:23:00"), 1641248580, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-02-29 22:23:00"), 0, true)
}

// func FromUnixTime(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error)
func UnixtimeCase(t *testing.T, typ types.T, src types.Timestamp, res int64, isNull bool) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
		wantNull   bool
	}{
		{
			name:       "TEST01",
			vecs:       makeVector2(src, true, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: true,
			wantNull:   isNull,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := UnixTimestamp(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.wantBytes, plus.Col) {
				t.Errorf("unixtimestamp() want %v but got %v", c.wantBytes, plus.Col)
			}
			require.Equal(t, c.wantNull, plus.ConstVectorIsNull())
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

func makeVector2(src types.Timestamp, srcScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 1)
	vectors[0] = vector.NewConstFixed(types.T_timestamp.ToType(), 1, src, testutil.TestUtilMp)
	return vectors
}

func TestUnixTimestampVarcharToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		date   string
		expect float64
	}{
		{`Test1`, `2000-01-01 12:00:00.159`, 946728000.159},
		{`Test2`, `2015-11-13 10:20:19.012`, 1447410019.012},
		{"Test3", `2012-11-13 10:20:19.0123456`, 1352802019.012346},
		{"Test3", `2022-11-13 10:20:19.9999999`, 1668334820},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scalarVec := testutil.MakeScalarVarchar(tt.date, 1)
			newProcess := testutil.NewProcess()
			newProcess.SessionInfo.TimeZone = time.UTC
			resVec, err := UnixTimestampVarcharToFloat64([]*vector.Vector{scalarVec}, newProcess)
			if err != nil {
				t.Fatal(err)
			}
			cols := vector.MustTCols[float64](resVec)
			require.Equal(t, tt.expect, cols[0])
		})
	}
}

func TestUnixTimestampVarchar(t *testing.T) {
	convey.Convey("Test01 unix timestamp", t, func() {
		cases := []struct {
			datestr string
			expect  float64
		}{
			{
				datestr: "2013-01-02 08:10:02.123456",
				expect:  1357114202.123456,
			},
			{
				datestr: "2006-01-02 12:19:02.123456",
				expect:  1136204342.123456,
			},
			{
				datestr: "2022-01-02 15:21:02.123456",
				expect:  1641136862.123456,
			},
			{
				datestr: "2006-01-02 12:19:02.666456",
				expect:  1136204342.666456,
			},
			{
				datestr: "2011-01-02 19:31:02.121111",
				expect:  1293996662.121111,
			},
			{
				datestr: "2002-01-02 01:41:02.123459",
				expect:  1009935662.123459,
			},
		}

		var datestrs []string
		var expects []float64
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		expectVector := testutil.MakeFloat64Vector(expects, nil)

		newProc := testutil.NewProc()
		newProc.SessionInfo.TimeZone = time.UTC
		result, err := UnixTimestampVarcharToFloat64([]*vector.Vector{datestrVector}, newProc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test01 unix timestamp", t, func() {
		cases := []struct {
			datestr string
			expect  int64
		}{
			{
				datestr: "2013-01-02 08:10:02",
				expect:  1357114202,
			},
			{
				datestr: "2006-01-02 12:19:02",
				expect:  1136204342,
			},
			{
				datestr: "2022-01-02 15:21:02",
				expect:  1641136862,
			},
			{
				datestr: "2006-01-02 12:19:02",
				expect:  1136204342,
			},
			{
				datestr: "2011-01-02 19:31:02",
				expect:  1293996662,
			},
			{
				datestr: "2002-01-02 01:41:02",
				expect:  1009935662,
			},
		}

		var datestrs []string
		var expects []int64
		for _, c := range cases {
			datestrs = append(datestrs, c.datestr)
			expects = append(expects, c.expect)
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		newProc := testutil.NewProc()
		newProc.SessionInfo.TimeZone = time.UTC
		result, err := UnixTimestampVarcharToInt64([]*vector.Vector{datestrVector}, newProc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}
