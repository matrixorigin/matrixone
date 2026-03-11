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

func TestLengthUTF8(t *testing.T) {
	convey.Convey("right case", t, func() {
		type kase struct {
			s    string
			want uint64
		}

		kases := []kase{
			{"abc", 3},
			{"", 0},
			{"   ", 3},
			{"中国123", 5},
			{"abc😄", 4},
			{"中国中国中国中国中国中国中国中国中国中国1234", 24},
			{"中国中国中国中国中国中国中国中国中国中国1234😄ggg!", 29},
			{
				"你好",
				2,
			},
			{
				"français",
				8,
			},
			{
				"にほんご",
				4,
			},
			{
				"Español",
				7,
			},
			{
				"123456",
				6,
			},
			{
				"андрей",
				6,
			},
			{
				"\\",
				1,
			},
			{
				string(rune(0x0c)),
				1,
			},
			{
				string('"'),
				1,
			},
			{
				string('\a'),
				1,
			},
			{
				string('\b'),
				1,
			},
			{
				string('\t'),
				1,
			},
			{
				string('\n'),
				1,
			},
			{
				string('\r'),
				1,
			},
			{
				string(rune(0x10)),
				1,
			},
			{
				"你好",
				2,
			},
			{
				"再见",
				2,
			},
			{
				"今天",
				2,
			},
			{
				"日期时间",
				4,
			},
			{
				"明天",
				2,
			},
			{
				"\n\t\r\b" + string(rune(0)) + "\\_\\%\\",
				10,
			},
		}

		var input []string
		var output []uint64
		for _, k := range kases {
			input = append(input, k.s)
			output = append(output, k.want)
		}

		ivec := testutil.MakeVarcharVector(input, nil)
		wantvec := testutil.MakeUint64Vector(output, nil)
		proc := testutil.NewProc()
		rvec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)
	})
	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(types.T_char, 10)
		wantvec := testutil.MakeScalarNull(types.T_uint64, 10)
		proc := testutil.NewProc()
		rvec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)

	})

	convey.Convey("scalar", t, func() {
		ivec := testutil.MakeScalarVarchar("abc", 5)
		wantvec := testutil.MakeScalarUint64(3, 5)
		proc := testutil.NewProc()
		rvec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, rvec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
