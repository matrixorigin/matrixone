// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestLeft(t *testing.T) {
	convey.Convey("right cases", t, func() {
		type kase struct {
			s    string
			len  int64
			want string
		}

		kases := []kase{
			{
				"abcde",
				3,
				"abc",
			},
			{
				"abcde",
				0,
				"",
			},
			{
				"abcde",
				-1,
				"",
			},
			{
				"abcde",
				100,
				"abcde",
			},
			{
				"foobarbar",
				5,
				"fooba",
			},
		}
		var inStrs []string
		var lens []int64
		var outStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			lens = append(lens, k.len)
			outStrs = append(outStrs, k.want)
		}

		inVec := testutil.MakeVarcharVector(inStrs, nil)
		lenVec := testutil.MakeInt64Vector(lens, nil)
		wantVec := testutil.MakeVarcharVector(outStrs, nil)
		proc := testutil.NewProc()
		retVec, err := Left([]*vector.Vector{inVec, lenVec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestLeft1(t *testing.T) {
	convey.Convey("left cases1", t, func() {
		type kase struct {
			s    string
			len  int64
			want string
		}

		kases := []kase{
			{
				"是都方式快递费",
				3,
				"是都方",
			},
			{
				"ｱｲｳｴｵ",
				3,
				"ｱｲｳ",
			},
			{
				"ｱｲｳｴｵ ",
				3,
				"ｱｲｳ",
			},
			{
				"ｱｲｳｴｵ  ",
				3,
				"ｱｲｳ",
			},
			{
				"ｱｲｳｴｵ   ",
				3,
				"ｱｲｳ",
			},
			{
				"あいうえお",
				3,
				"あいう",
			},
			{
				"あいうえお ",
				3,
				"あいう",
			},
			{
				"あいうえお  ",
				3,
				"あいう",
			},
			{
				"あいうえお   ",
				3,
				"あいう",
			},
			{
				"龔龖龗龞龡",
				3,
				"龔龖龗",
			},
			{
				"龔龖龗龞龡 ",
				3,
				"龔龖龗",
			},
			{
				"龔龖龗龞龡  ",
				3,
				"龔龖龗",
			},
			{
				"龔龖龗龞龡   ",
				3,
				"龔龖龗",
			},
			{
				"2017-06-15    ",
				8,
				"2017-06-",
			},
			{
				"2019-06-25    ",
				8,
				"2019-06-",
			},
			{
				"    2019-06-25  ",
				8,
				"    2019",
			},
			{
				"   2019-06-25   ",
				8,
				"   2019-",
			},
			{
				"    2012-10-12   ",
				8,
				"    2012",
			},
			{
				"   2004-04-24.   ",
				8,
				"   2004-",
			},
			{
				"   2008-12-04.  ",
				8,
				"   2008-",
			},
			{
				"    2012-03-23.   ",
				8,
				"    2012",
			},
			{
				"    2013-04-30  ",
				8,
				"    2013",
			},
			{
				"  1994-10-04  ",
				8,
				"  1994-1",
			},
			{
				"   2018-06-04  ",
				8,
				"   2018-",
			},
			{
				" 2012-10-12  ",
				8,
				" 2012-10",
			},
			{
				"1241241^&@%#^*^!@#&*(!&    ",
				12,
				"1241241^&@%#",
			},
			{
				" 123 ",
				2,
				" 1",
			},
		}
		var inStrs []string
		var lens []int64
		var outStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			lens = append(lens, k.len)
			outStrs = append(outStrs, k.want)
		}

		inVec := testutil.MakeVarcharVector(inStrs, nil)
		lenVec := testutil.MakeInt64Vector(lens, nil)
		wantVec := testutil.MakeVarcharVector(outStrs, nil)
		proc := testutil.NewProc()
		retVec, err := Left([]*vector.Vector{inVec, lenVec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestLeft2(t *testing.T) {
	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(types.T_char, 10)
		lenVec := testutil.MakeScalarNull(types.T_int64, 10)
		wantvec := testutil.MakeScalarNull(types.T_char, 10)
		proc := testutil.NewProc()
		rvec, err := Left([]*vector.Vector{ivec, lenVec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("scalar", t, func() {
		ivec := testutil.MakeScalarVarchar("abcdefg", 5)
		lenVec := testutil.MakeScalarInt64(3, 5)
		wantvec := testutil.MakeScalarVarchar("abc", 5)
		proc := testutil.NewProc()
		rvec, err := Left([]*vector.Vector{ivec, lenVec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
