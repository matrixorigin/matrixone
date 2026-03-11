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

func TestReverse(t *testing.T) {
	convey.Convey("right", t, func() {
		inputStrs := []string{
			"abc",
			"abcd",
			"hello",
			"ｱｲｳｴｵ",
			"あいうえお",
			"龔龖龗龞龡",
			"你好",
			"再 见",
			"bcd",
			"def",
			"xyz",
			"1a1",
			"2012",
			"@($)@($#)_@(#",
			"2023-04-24",
			"10:03:23.021412",
			"sdfad  ",
		}
		wantStrs := []string{
			"cba",
			"dcba",
			"olleh",
			"ｵｴｳｲｱ",
			"おえういあ",
			"龡龞龗龖龔",
			"好你",
			"见 再",
			"dcb",
			"fed",
			"zyx",
			"1a1",
			"2102",
			"#(@_)#$(@)$(@",
			"42-40-3202",
			"214120.32:30:01",
			"  dafds",
		}
		ivec := testutil.MakeVarcharVector(inputStrs, nil)
		wantVec := testutil.MakeVarcharVector(wantStrs, nil)
		proc := testutil.NewProc()
		get, err := Reverse([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, get)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(types.T_char, 10)
		wantvec := testutil.MakeScalarNull(types.T_char, 10)
		proc := testutil.NewProc()
		rvec, err := Reverse([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)

	})
}
