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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestRtrim(t *testing.T) {
	convey.Convey("right cases", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				"barbar   ",
				"barbar",
			},
			{
				"MySQL",
				"MySQL",
			},
			{
				"a",
				"a",
			},
			{
				"  20.06 ",
				"  20.06",
			},
			{
				"  right  ",
				"  right",
			},
			{
				"你好  ",
				"你好",
			},
			{
				"2017-06-15   ",
				"2017-06-15",
			},
			{
				"2017-06-15        ",
				"2017-06-15",
			},
		}

		var inStrs []string
		var outStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			outStrs = append(outStrs, k.want)
		}

		extraInStrs := []string{
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ ",
			"ｱｲｳｴｵ  ",
			"ｱｲｳｴｵ   ",
			"ｱｲｳｴｵ ",
			"ｱｲｳｴｵ    ",
			"ｱｲｳｴｵ      ",
			"あいうえお",
			"あいうえお ",
			"あいうえお  ",
			"あいうえお   ",
			"あいうえお",
			"あいうえお ",
			"あいうえお   ",
			"龔龖龗龞龡",
			"龔龖龗龞龡 ",
			"龔龖龗龞龡  ",
			"龔龖龗龞龡   ",
			"龔龖龗龞龡",
			"龔龖龗龞龡 ",
			"龔龖龗龞龡   ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ ",
			"ｱｲｳｴｵ  ",
			"ｱｲｳｴｵ   ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ  ",
			"ｱｲｳｴｵ    ",
			"あいうえお",
			"あいうえお ",
			"あいうえお  ",
			"あいうえお   ",
			"あいうえお",
			"あいうえお  ",
			"あいうえお     ",
			"龔龖龗龞龡",
			"龔龖龗龞龡 ",
			"龔龖龗龞龡  ",
			"龔龖龗龞龡   ",
			"龔龖龗龞龡",
			"龔龖龗龞龡 ",
			"龔龖龗龞龡  ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ ",
			"ｱｲｳｴｵ  ",
			"ｱｲｳｴｵ   ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ ",
			"ｱｲｳｴｵ  ",
			"あいうえお",
			"あいうえお ",
			"あいうえお  ",
			"あいうえお   ",
			"あいうえお",
			"あいうえお  ",
			"あいうえお    ",
			"龔龖龗龞龡",
			"龔龖龗龞龡 ",
			"龔龖龗龞龡  ",
			"龔龖龗龞龡   ",
			"龔龖龗龞龡",
			"龔龖龗龞龡  ",
			"龔龖龗龞龡      ",
			"2017-06-15    ",
			"2019-06-25    ",
			"    2019-06-25  ",
			"   2019-06-25   ",
			"    2012-10-12   ",
			"   2004-04-24.   ",
			"   2008-12-04.  ",
			"    2012-03-23.   ",
			"    2013-04-30  ",
			"  1994-10-04  ",
			"   2018-06-04  ",
			" 2012-10-12  ",
			"1241241^&@%#^*^!@#&*(!&    ",
			" 123 ",
		}
		extraOutStrs := []string{
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"ｱｲｳｴｵ",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"あいうえお",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"龔龖龗龞龡",
			"2017-06-15",
			"2019-06-25",
			"    2019-06-25",
			"   2019-06-25",
			"    2012-10-12",
			"   2004-04-24.",
			"   2008-12-04.",
			"    2012-03-23.",
			"    2013-04-30",
			"  1994-10-04",
			"   2018-06-04",
			" 2012-10-12",
			"1241241^&@%#^*^!@#&*(!&",
			" 123",
		}

		inStrs = append(inStrs, extraInStrs...)
		outStrs = append(outStrs, extraOutStrs...)

		ivec := testutil.MakeVarcharVector(inStrs, nil)
		wantVec := testutil.MakeVarcharVector(outStrs, nil)
		proc := testutil.NewProc()
		retVec, err := Rtrim([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(types.T_char, 10)
		wantvec := testutil.MakeScalarNull(types.T_char, 10)
		proc := testutil.NewProc()
		rvec, err := Rtrim([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)

	})

	convey.Convey("scalar", t, func() {
		ivec := testutil.MakeScalarVarchar("abc   ", 5)
		wantvec := testutil.MakeScalarVarchar("abc", 5)
		proc := testutil.NewProc()
		rvec, err := Rtrim([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
