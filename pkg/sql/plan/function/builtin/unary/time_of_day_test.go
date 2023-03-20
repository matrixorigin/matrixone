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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

type testPair struct {
	s    string
	want uint8
}

const (
	Rows = 8000 // default test rows
)

func TestTimeOfDay(t *testing.T) {
	convey.Convey("HourOfDayDatetimeCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 10,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 1,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToHour([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("HourOfDayTimestampCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 10,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 1,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeTimeStampVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := TimestampToHour([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("MinuteOfDayDatetimeCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 20,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 1,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToMinute([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("MinuteOfDayTimestampCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 20,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 1,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeTimeStampVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := TimestampToMinute([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("SecondOfDayDatetimeCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 0,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 37,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
			{
				s:    "2024-04-03 10:20:09",
				want: 9,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeDateTimeVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := DatetimeToSecond([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("SecondOfDayTimestampCase", t, func() {

		pairs := []testPair{
			{
				s:    "2004-04-03 10:20:00",
				want: 0,
			},
			{
				s:    "2004-08-03 01:01:37",
				want: 37,
			},
			{
				s:    "2004-01-03",
				want: 0,
			},
			{
				s:    "2024-04-03 10:20:09",
				want: 9,
			},
		}

		var inStrs []string
		var wantUint8 []uint8
		for _, k := range pairs {
			inStrs = append(inStrs, k.s)
			wantUint8 = append(wantUint8, k.want)
		}

		inVector := testutil.MakeTimeStampVector(inStrs, nil)
		wantVector := testutil.MakeUint8Vector(wantUint8, nil)
		proc := testutil.NewProc()
		res, err := TimestampToSecond([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
	convey.Convey("TimeOfDayCaseScalar", t, func() {

		hourPairs := testPair{
			s:    "2004-01-03 23:15:08",
			want: 23,
		}

		minutePairs := testPair{
			s:    "2004-01-03 23:15:08",
			want: 15,
		}

		secondPairs := testPair{
			s:    "2004-01-03 23:15:08",
			want: 8,
		}

		//Hour Test
		{
			inVectorDT := testutil.MakeScalarDateTime(hourPairs.s, 10)
			inVectorTS := testutil.MakeScalarTimeStamp(hourPairs.s, 10)
			wantVector := testutil.MakeScalarUint8(hourPairs.want, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToHour([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToHour([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

		//Minute Test
		{
			inVectorDT := testutil.MakeScalarDateTime(minutePairs.s, 10)
			inVectorTS := testutil.MakeScalarTimeStamp(minutePairs.s, 10)
			wantVector := testutil.MakeScalarUint8(minutePairs.want, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToMinute([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToMinute([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

		//Second Test
		{
			inVectorDT := testutil.MakeScalarDateTime(secondPairs.s, 10)
			inVectorTS := testutil.MakeScalarTimeStamp(secondPairs.s, 10)
			wantVector := testutil.MakeScalarUint8(secondPairs.want, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToSecond([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToSecond([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

	})
	convey.Convey("TimeOfDayCaseScalarNull", t, func() {

		//Hour Test
		{
			inVectorDT := testutil.MakeScalarNull(types.T_datetime, 10)
			inVectorTS := testutil.MakeScalarNull(types.T_timestamp, 10)
			wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToHour([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToHour([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

		//Minute Test
		{
			inVectorDT := testutil.MakeScalarNull(types.T_datetime, 10)
			inVectorTS := testutil.MakeScalarNull(types.T_timestamp, 10)
			wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToMinute([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToMinute([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

		//Second Test
		{
			inVectorDT := testutil.MakeScalarNull(types.T_datetime, 10)
			inVectorTS := testutil.MakeScalarNull(types.T_timestamp, 10)
			wantVector := testutil.MakeScalarNull(types.T_uint8, 10)
			proc := testutil.NewProc()
			{
				res, err := DatetimeToSecond([]*vector.Vector{inVectorDT}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
			{
				res, err := TimestampToSecond([]*vector.Vector{inVectorTS}, proc)
				convey.So(err, convey.ShouldBeNil)
				compare := testutil.CompareVectors(wantVector, res)
				convey.So(compare, convey.ShouldBeTrue)
			}
		}

	})
}

type benchmarkTestCase struct {
	flags []bool // flags[i] == true: nullable
	types []types.Type
	proc  *process.Process
}

var (
	tcs []benchmarkTestCase
)

func init() {
	tcs = []benchmarkTestCase{
		newTestCase(mpool.MustNewZero(), []bool{false}, []types.Type{types.T_datetime.ToType()}),
	}
}

func newTestCase(m *mpool.MPool, flgs []bool, ts []types.Type) benchmarkTestCase {
	return benchmarkTestCase{
		types: ts,
		flags: flgs,
		proc:  testutil.NewProcessWithMPool(m),
	}
}

// create a new block based on the type information, flags[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func BenchmarkDatetimeToHour(b *testing.B) {
	t := new(testing.T)
	for i := 0; i < b.N; i++ {
		for _, tc := range tcs {
			bat := newBatch(t, tc.flags, tc.types, tc.proc, Rows)
			vec, err := DatetimeToHour(bat.Vecs, tc.proc)
			require.NoError(t, err)
			bat.Clean(tc.proc.Mp())
			vec.Free(tc.proc.Mp())
			require.Equal(t, int64(0), tc.proc.Mp().Stats().NumCurrBytes.Load())
		}
	}
}
