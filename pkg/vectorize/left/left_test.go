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

package left

import (
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLeft(t *testing.T) {
	convey.Convey("TestLeft", t, func() {
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

		res := make([]string, len(inStrs))
		Left(inStrs, lens, res)

		require.Equal(t, outStrs, res)
	})
}

func TestLeftAllConst(t *testing.T) {
	convey.Convey("TestLeftAllConst", t, func() {
		type kase struct {
			s    string
			len  int64
			want string
		}
		kases := []kase{
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

		res := make([]string, len(inStrs))
		LeftAllConst(inStrs, lens, res)

		require.Equal(t, outStrs, res)
	})
}

func TestLeftLeftConst(t *testing.T) {
	convey.Convey("TestLeftLeftConst", t, func() {
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
		}
		var inStrs []string
		var lens []int64
		var outStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			lens = append(lens, k.len)
			outStrs = append(outStrs, k.want)
		}

		res := make([]string, len(inStrs))
		LeftLeftConst(inStrs, lens, res)

		require.Equal(t, outStrs, res)
	})
}

func TestLeftRightConst(t *testing.T) {
	convey.Convey("TestLeftRightConst", t, func() {
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
				"あいうえお",
				3,
				"あいう",
			},
			{
				"foobarbar",
				3,
				"foo",
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

		res := make([]string, len(inStrs))
		LeftRightConst(inStrs, lens, res)

		require.Equal(t, outStrs, res)
	})
}
