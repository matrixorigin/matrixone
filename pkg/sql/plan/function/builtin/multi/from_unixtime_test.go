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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestFromUnixTimeInt64(t *testing.T) {
	convey.Convey("test FromUnixTime Int64", t, func() {
		kases := []struct {
			num  int64
			want string
		}{
			{
				num:  0,
				want: "1970-01-01 00:00:00",
			},
			{
				num:  1451606400,
				want: "2016-01-01 00:00:00",
			},
			{
				num:  2451606400,
				want: "2047-09-09 01:46:40",
			},
			{
				num:  1451606488,
				want: "2016-01-01 00:01:28",
			},
			{
				num:  32536771199,
				want: "3001-01-18 23:59:59",
			},
			{
				num:  1447430881,
				want: "2015-11-13 16:08:01",
			},
		}

		var nums []int64
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			wants = append(wants, kase.want)
		}

		int64Vector := testutil.MakeInt64Vector(nums, nil)
		wantVector := testutil.MakeDateTimeVector(wants, nil)
		process := newTmpProcess()
		res, err := FromUnixTimeInt64([]*vector.Vector{int64Vector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustTCols[types.Datetime](wantVector)
		cols2 := vector.MustTCols[types.Datetime](res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeUint64(t *testing.T) {
	convey.Convey("test FromUnixTime UInt64", t, func() {
		kases := []struct {
			num  uint64
			want string
		}{
			{
				num:  0,
				want: "1970-01-01 00:00:00",
			},
			{
				num:  1451606400,
				want: "2016-01-01 00:00:00",
			},
			{
				num:  2451606400,
				want: "2047-09-09 01:46:40",
			},
			{
				num:  1451606488,
				want: "2016-01-01 00:01:28",
			},
			{
				num:  32536771199,
				want: "3001-01-18 23:59:59",
			},
			{
				num:  1447430881,
				want: "2015-11-13 16:08:01",
			},
		}

		var nums []uint64
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			wants = append(wants, kase.want)
		}

		uint64Vector := testutil.MakeUint64Vector(nums, nil)
		wantVector := testutil.MakeDateTimeVector(wants, nil)
		process := newTmpProcess()
		res, err := FromUnixTimeUint64([]*vector.Vector{uint64Vector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustTCols[types.Datetime](wantVector)
		cols2 := vector.MustTCols[types.Datetime](res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeFloat64(t *testing.T) {
	convey.Convey("test FromUnixTime Float64", t, func() {
		kases := []struct {
			num  float64
			want string
		}{
			{
				num:  0,
				want: "1970-01-01 00:00:00.000000",
			},
			{
				num:  1451606400.123456,
				want: "2016-01-01 00:00:00.123456",
			},
			{
				num:  1451606400.999999,
				want: "2016-01-01 00:00:00.999999",
			},
			{
				num:  1451606400.1233,
				want: "2016-01-01 00:00:00.123300",
			},
			{
				num:  32536771199.123456789,
				want: "3001-01-18 23:59:59.123455",
			},
			{
				num:  1447430881.4456789,
				want: "2015-11-13 16:08:01.445679",
			},
		}

		var nums []float64
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			wants = append(wants, kase.want)
		}

		float64Vector := testutil.MakeFloat64Vector(nums, nil)
		wantVector := testutil.MakeDateTimeVector(wants, nil)
		process := newTmpProcess()
		res, err := FromUnixTimeFloat64([]*vector.Vector{float64Vector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustTCols[types.Datetime](wantVector)
		cols2 := vector.MustTCols[types.Datetime](res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeInt64Format(t *testing.T) {
	convey.Convey("test FromUnixTime Int64 Fromat", t, func() {
		kases := []struct {
			num    int64
			format string
			want   string
		}{
			{
				num:    0,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 01 1970 1970 70",
			},
			{
				num:    1451606400,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 53 2015 2016 16",
			},
			{
				num:    2451606400,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Sep September 09 9 9th 09 9 252 1 01 46 AM 01:46:40 AM 01:46:40 40 000000 37 2047 2047 47",
			},
			{
				num:    1451606267,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Dec December 12 12 31st 31 31 365 23 11 57 PM 11:57:47 PM 23:57:47 47 000000 53 2015 2015 15",
			},
			{
				num:    32536771199,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 18th 18 18 018 23 11 59 PM 11:59:59 PM 23:59:59 59 000000 03 3001 3001 01",
			},
			{
				num:    2447430881,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jul July 07 7 22nd 22 22 203 17 05 54 PM 05:54:41 PM 17:54:41 41 000000 30 2047 2047 47",
			},
		}

		var nums []int64
		var formats []string
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			formats = append(formats, kase.format)
			wants = append(wants, kase.want)
		}

		int64Vector := testutil.MakeInt64Vector(nums, nil)
		formatVector := testutil.MakeScalarVarchar(formats[0], 5)
		wantVector := testutil.MakeVarcharVector(wants, nil)

		process := newTmpProcess()
		res, err := FromUnixTimeInt64Format([]*vector.Vector{int64Vector, formatVector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustStrCols(wantVector)
		cols2 := vector.MustStrCols(res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeUint64Format(t *testing.T) {
	convey.Convey("test FromUnixTime Int64 Fromat", t, func() {
		kases := []struct {
			num    uint64
			format string
			want   string
		}{
			{
				num:    0,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 01 1970 1970 70",
			},
			{
				num:    1451606400,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 53 2015 2016 16",
			},
			{
				num:    2451606400,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Sep September 09 9 9th 09 9 252 1 01 46 AM 01:46:40 AM 01:46:40 40 000000 37 2047 2047 47",
			},
			{
				num:    1451606267,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Dec December 12 12 31st 31 31 365 23 11 57 PM 11:57:47 PM 23:57:47 47 000000 53 2015 2015 15",
			},
			{
				num:    32536771199,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 18th 18 18 018 23 11 59 PM 11:59:59 PM 23:59:59 59 000000 03 3001 3001 01",
			},
			{
				num:    2447430881,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jul July 07 7 22nd 22 22 203 17 05 54 PM 05:54:41 PM 17:54:41 41 000000 30 2047 2047 47",
			},
		}

		var nums []uint64
		var formats []string
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			formats = append(formats, kase.format)
			wants = append(wants, kase.want)
		}

		uint64Vector := testutil.MakeUint64Vector(nums, nil)
		formatVector := testutil.MakeScalarVarchar(formats[0], 5)
		wantVector := testutil.MakeVarcharVector(wants, nil)

		process := newTmpProcess()
		res, err := FromUnixTimeUint64Format([]*vector.Vector{uint64Vector, formatVector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustStrCols(wantVector)
		cols2 := vector.MustStrCols(res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeFloat64Format(t *testing.T) {
	convey.Convey("test FromUnixTime Float64 Fromat", t, func() {
		kases := []struct {
			num    float64
			format string
			want   string
		}{
			{
				num:    1451606400.123456789,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123457 53 2015 2016 16",
			},
			{
				num:    2451606400.999999,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Sep September 09 9 9th 09 9 252 1 01 46 AM 01:46:40 AM 01:46:40 40 999999 37 2047 2047 47",
			},
			{
				num:    1451606267.99999999,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Dec December 12 12 31st 31 31 365 23 11 57 PM 11:57:48 PM 23:57:48 48 000000 53 2015 2015 15",
			},
			{
				num:    32536771199.123456,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jan January 01 1 18th 18 18 018 23 11 59 PM 11:59:59 PM 23:59:59 59 123455 03 3001 3001 01",
			},
			{
				num:    2447430881.1245,
				format: "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y",
				want:   "Jul July 07 7 22nd 22 22 203 17 05 54 PM 05:54:41 PM 17:54:41 41 124500 30 2047 2047 47",
			},
		}

		var nums []float64
		var formats []string
		var wants []string
		for _, kase := range kases {
			nums = append(nums, kase.num)
			formats = append(formats, kase.format)
			wants = append(wants, kase.want)
		}

		float64Vector := testutil.MakeFloat64Vector(nums, nil)
		formatVector := testutil.MakeScalarVarchar(formats[0], 5)
		wantVector := testutil.MakeVarcharVector(wants, nil)

		process := newTmpProcess()
		res, err := FromUnixTimeFloat64Format([]*vector.Vector{float64Vector, formatVector}, process)
		convey.So(err, convey.ShouldBeNil)
		cols1 := vector.MustStrCols(wantVector)
		cols2 := vector.MustStrCols(res)
		require.Equal(t, cols1, cols2)
	})
}

func TestFromUnixTimeFloat64Null(t *testing.T) {
	convey.Convey("test FromUnixTime Float64 null", t, func() {
		kases := []struct {
			num float64
		}{
			{
				num: -1451606400.123345,
			},
			{
				num: 32536771200.1234566,
			},
			{
				num: 1451606400.9999,
			},
			{
				num: 32536771199.4444,
			},
			{
				num: 1447430881.4545565,
			},
		}

		var nums []float64
		for _, kase := range kases {
			nums = append(nums, kase.num)
		}

		numVector := testutil.MakeFloat64Vector(nums, []uint64{2, 3, 4})
		process := testutil.NewProc()
		res, err := FromUnixTimeFloat64([]*vector.Vector{numVector}, process)
		convey.So(err, convey.ShouldBeNil)
		require.Equal(t, []uint64{0, 1, 2, 3, 4}, res.Nsp.Np.ToArray())
	})
}

func TestFromUnixTimeInt64Null(t *testing.T) {
	convey.Convey("test FromUnixTime Int64 null", t, func() {
		kases := []struct {
			num int64
		}{
			{
				num: -1451606400,
			},
			{
				num: 32536771200,
			},
			{
				num: 1451606400,
			},
			{
				num: 32536771199,
			},
			{
				num: 1447430881,
			},
		}

		var nums []int64
		for _, kase := range kases {
			nums = append(nums, kase.num)
		}

		float64Vector := testutil.MakeInt64Vector(nums, []uint64{2, 3, 4})
		process := testutil.NewProc()
		res, err := FromUnixTimeInt64([]*vector.Vector{float64Vector}, process)
		convey.So(err, convey.ShouldBeNil)
		require.Equal(t, []uint64{0, 1, 2, 3, 4}, res.Nsp.Np.ToArray())
	})
}

func TestFromUnixTimeInt64FormatNull(t *testing.T) {
	convey.Convey("test FromUnixTime Int64 format NUll", t, func() {
		kases := []struct {
			num int64
		}{
			{
				num: -1451606400,
			},
			{
				num: 32536771200,
			},
			{
				num: 1451606400,
			},
			{
				num: 32536771199,
			},
			{
				num: 1447430881,
			},
		}

		var nums []int64
		for _, kase := range kases {
			nums = append(nums, kase.num)
		}

		numVector := testutil.MakeInt64Vector(nums, []uint64{2, 3, 4})
		formatVector := testutil.MakeScalarVarchar("%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y", 5)
		process := testutil.NewProc()
		res, err := FromUnixTimeInt64Format([]*vector.Vector{numVector, formatVector}, process)
		convey.So(err, convey.ShouldBeNil)
		require.Equal(t, []uint64{0, 1, 2, 3, 4}, res.Nsp.Np.ToArray())
	})
}

func TestFromUnixTimeFloat64FormatNull(t *testing.T) {
	convey.Convey("test FromUnixTime float64 NUll", t, func() {
		kases := []struct {
			num float64
		}{
			{
				num: -1451606400.34534,
			},
			{
				num: 32536771200.78678,
			},
			{
				num: 1451606400.56456,
			},
			{
				num: 32536771199.354345,
			},
			{
				num: 1447430881.1232,
			},
		}

		var nums []float64
		for _, kase := range kases {
			nums = append(nums, kase.num)
		}

		numVector := testutil.MakeFloat64Vector(nums, []uint64{2, 3, 4})
		formatVector := testutil.MakeScalarVarchar("%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y", 5)
		process := testutil.NewProc()
		res, err := FromUnixTimeFloat64Format([]*vector.Vector{numVector, formatVector}, process)
		convey.So(err, convey.ShouldBeNil)
		require.Equal(t, []uint64{0, 1, 2, 3, 4}, res.Nsp.Np.ToArray())
	})
}

func newTmpProcess() *process.Process {
	return newProcessWithMPool(mpool.MustNewZero())
}

func newProcessWithMPool(mp *mpool.MPool) *process.Process {
	process := process.New(
		context.Background(),
		mp,
		nil, // no txn client can be set
		nil, // no txn operator can be set
		testutil.NewFS(),
	)
	process.Lim.Size = 1 << 20
	process.Lim.BatchRows = 1 << 20
	process.Lim.BatchSize = 1 << 20
	process.Lim.ReaderSize = 1 << 20
	process.SessionInfo.TimeZone = time.FixedZone("UTC0", 0)
	return process
}
