// Copyright 2021 Matrix Origin
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

package frontend

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	cvey "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func Test_PathExists(t *testing.T) {
	cases := [...]struct {
		path   string
		exist  bool
		isfile bool
		noerr  bool
	}{
		{"test/file", true, true, true},
		{"test/file-no", false, false, false},
		//{"test/dir",true,false,true},
		{"test/dir-no", false, false, false},
		{"testx", false, false, false},
	}

	for _, c := range cases {
		exist, isfile, err := PathExists(c.path)
		require.True(t, (err == nil) == c.noerr)
		require.True(t, exist == c.exist)
		require.True(t, isfile == c.isfile)
	}
}

func Test_closeFlag(t *testing.T) {
	cvey.Convey("closeFlag", t, func() {
		cf := &CloseFlag{}
		cf.setClosed(0)
		cvey.So(cf.IsOpened(), cvey.ShouldBeTrue)

		cf.Open()
		cvey.So(cf.IsOpened(), cvey.ShouldBeTrue)

		cf.Close()
		cvey.So(cf.IsClosed(), cvey.ShouldBeTrue)
	})
}

func Test_MinMax(t *testing.T) {
	cvey.Convey("min", t, func() {
		cvey.So(Min(10, 9), cvey.ShouldEqual, 9)
		cvey.So(Min(9, 10), cvey.ShouldEqual, 9)
	})

	cvey.Convey("minInt64", t, func() {
		cvey.So(MinInt64(10, 9), cvey.ShouldEqual, 9)
		cvey.So(MinInt64(9, 10), cvey.ShouldEqual, 9)
	})

	cvey.Convey("minUint64", t, func() {
		cvey.So(MinUint64(10, 9), cvey.ShouldEqual, 9)
		cvey.So(MinUint64(9, 10), cvey.ShouldEqual, 9)
	})

	cvey.Convey("max", t, func() {
		cvey.So(Max(10, 9), cvey.ShouldEqual, 10)
		cvey.So(Max(9, 10), cvey.ShouldEqual, 10)
	})

	cvey.Convey("maxInt64", t, func() {
		cvey.So(MaxInt64(10, 9), cvey.ShouldEqual, 10)
		cvey.So(MaxInt64(9, 10), cvey.ShouldEqual, 10)
	})

	cvey.Convey("maxUint64", t, func() {
		cvey.So(MaxUint64(10, 9), cvey.ShouldEqual, 10)
		cvey.So(MaxUint64(9, 10), cvey.ShouldEqual, 10)
	})
}

func Test_uint64list(t *testing.T) {
	cvey.Convey("uint64list", t, func() {
		var l = make(Uint64List, 3)
		cvey.So(l.Len(), cvey.ShouldEqual, 3)
		cvey.So(l.Less(0, 1), cvey.ShouldBeFalse)
		a, b := l[0], l[1]
		l.Swap(0, 1)
		cvey.So(a == l[1] && b == l[0], cvey.ShouldBeTrue)
	})
}

func Test_routineid(t *testing.T) {
	cvey.Convey("rtid", t, func() {
		x := GetRoutineId()
		cvey.So(x, cvey.ShouldBeGreaterThanOrEqualTo, 0)
	})
}

func Test_debugcounter(t *testing.T) {
	cvey.Convey("debugCounter", t, func() {
		dc := NewDebugCounter(3)
		dc.Set(0, 1)
		cvey.So(dc.Get(0), cvey.ShouldEqual, 1)
		dc.Add(0, 1)
		cvey.So(dc.Get(0), cvey.ShouldEqual, 2)
		cvey.So(dc.Len(), cvey.ShouldEqual, 3)

		go dc.DCRoutine()

		time.Sleep(6 * time.Second)

		dc.Cf.Close()
	})
}

func Test_timeout(t *testing.T) {
	cvey.Convey("timeout", t, func() {
		to := NewTimeout(5*time.Second, true)
		to.UpdateTime(time.Now())
		cvey.So(to.isTimeout(), cvey.ShouldBeFalse)
	})
}

func Test_substringFromBegin(t *testing.T) {
	cvey.Convey("ssfb", t, func() {
		cvey.So(SubStringFromBegin("abcdef", 3), cvey.ShouldEqual, "abc...")
	})
}

func Test_makedebuginfo(t *testing.T) {
	cvey.Convey("makedebuginfo", t, func() {
		MakeDebugInfo([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			6, 3)
	})
}

func TestWildcardMatch(t *testing.T) {
	//sort by string

	patterns := []string{
		"%",
		"%%%%%%%%a%%%%%%%%b%%%%%%%%b%%%%%%%%",
		"%%%%%%%%a%%%%%%%%b%%%%%%%%c%%%%%%%%",
		"%%%a%b%c%%%",
		"%.%",
		"%.zi%",
		"%.zi_",
		"%.zip",
		"%12%12%",
		"%12%23",
		"%Abac%",
		"%SIP%",
		"%_",
		"%_%_%",
		"%_%_%.zip",
		"%_%_.zip",
		"%_.zip",
		"%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
		"%a%b%ba%ca%a%aa%aaa%fa%ga%b%",
		"%a%b%ba%ca%a%x%aaa%fa%ga%b%",
		"%a%b%ba%ca%aaaa%fa%ga%ggg%b%",
		"%a%b%ba%ca%aaaa%fa%ga%gggg%b%",
		"%aa%",
		"%aa_",
		"%aabbaa%a%",
		"%ab%cd%",
		"%abac%",
		"%ccd",
		"%issip%PI",
		"%issip%ss%",
		"%oWn%",
		"%sip%",
		"%zi%",
		"%zi_",
		"%zip",
		"._",
		"XY%Z%XYz",
		"_",
		"_%",
		"_%%_%&_",
		"_%%_%_",
		"_%%_c_",
		"_%%_d_",
		"_%._%",
		"_%_",
		"_%_.zip",
		"_%b%_%d%_",
		"_.",
		"_._",
		"_.zip",
		"_LaH",
		"_Lah",
		"__",
		"_a",
		"_a%__",
		"_a_",
		"_aa%",
		"_b%__",
		"a%",
		"a%_%_",
		"a%_%_%.zip",
		"a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
		"a%a%a%a%a%a%aa%aaa%a%a%b",
		"a%aar",
		"a%b",
		"a%zz%",
		"a12b",
		"a_",
		"ab%_%xy",
		"ab%cd%xy",
		"abc",
		"abc%abc%abc%abc%abc",
		"abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%",
		"abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%",
		"abc%abc%abc%abc%abc%abc%abc%abc%abc%abc%abcd",
		"bL_h",
		"bLaH",
		"bLa_",
		"bLah",
		"mi%Sip%",
		"mi%sip%",
		"xxx%zzy%f",
		"xxxx%zzy%f",
		"xxxx%zzy%fffff",
		"xy%xyz",
		"xy%z%xyz",
	}

	targets := []string{
		"%",
		"%%%%%%%%a%%%%%%%%b%%%%%%%%c%%%%%%%%",
		"%abc%",
		".a",
		".a.",
		".a.a",
		".a.aa",
		".a.b",
		".a.bcd",
		".aa.",
		".ab",
		".ab.ab.ab.cd.cd.",
		".ab.cd.ab.cd.abcd.",
		".axb.cxd.ab.cd.abcd.",
		".axb.cxd.ab.cd.abcd.xy",
		".axb.cyd.ab.cyd.axbcd.",
		".zip",
		"A12b12",
		"XYXYXYZYXYz",
		"a",
		"a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
		"a%abab",
		"a%ar",
		"a%r",
		"a.",
		"a.a",
		"a.a.zip",
		"a.a.zippo",
		"a.ab.ab.ab.cd.cd.xy",
		"a.b",
		"a.bcd",
		"a.zip",
		"a12B12",
		"a12b12",
		"aAazz",
		"aa",
		"aa.",
		"aa.a",
		"aa.ba.ba",
		"aaa",
		"aaaa.zip",
		"aaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
		"aaabbaabbaab",
		"aaazz",
		"aannn",
		"ab",
		"ab.",
		"ab.ab.cd.ab.cd.abcdxy.",
		"ab.axb.cd.xyab.cyd.axbcd.",
		"ab.axb.cd.xyab.cyd.axbcd.xy",
		"ab.xy",
		"abAbac",
		"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
		"ababac",
		"abanabnabncd",
		"abanabnabncdef",
		"abancda.bnxyabncdefxy",
		"abancdabnxyabncdef",
		"abancdabnxyabncdefxy",
		"abc",
		"abc%abcd%abcd%abc%abcd",
		"abc%abcd%abcd%abc%abcd%abcd%abc%abcd%abc%abc%abcd",
		"abc%abcd%abcde%abcdef%abcdefg%abcdefgh%abcdefghi%abcdefghij%abcdefghijk%abcdefghijkl%abcdefghijklm%abcdefghijklmn",
		"abcccd",
		"abcd",
		"abcde",
		"abcdx_y",
		"abxy",
		"ax",
		"bLaH",
		"bLaaa",
		"bLah",
		"baa.",
		"caa.ba.ba",
		"miSsissippi",
		"missisSIPpi",
		"mississipPI",
		"mississipissippi",
		"mississippi",
		"oWn",
		"xa",
		"xaab",
		"xab",
		"xab_anabnabncd_xy",
		"xxa",
		"xxab",
		"xxxx%zzzzzzzzy%f",
		"xxxxzzzzzzzzyf",
		"xyxyxyxyz",
		"xyxyxyzyxyz",
		"xyz.bcd",
		"zip"}

	want := map[int][]int{
		0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		1:  {11, 12, 13, 14, 15, 21, 28, 38, 44, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 75, 85},
		2:  {1, 2, 8, 11, 12, 13, 14, 15, 28, 30, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 85},
		3:  {1, 2, 8, 11, 12, 13, 14, 15, 28, 30, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 85},
		4:  {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24, 25, 26, 27, 28, 29, 30, 31, 36, 37, 38, 40, 48, 49, 50, 51, 52, 58, 74, 75, 92},
		5:  {16, 26, 27, 31, 40},
		6:  {16, 26, 31, 40},
		7:  {16, 26, 31, 40},
		8:  {17, 32, 33},
		9:  {},
		10: {53},
		11: {77},
		12: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		13: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		14: {26, 40},
		15: {26, 40},
		16: {26, 31, 40},
		17: {20, 42, 43, 54},
		18: {54},
		19: {},
		20: {54},
		21: {},
		22: {6, 9, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 54, 72, 74, 75, 83},
		23: {9, 36, 39, 41, 42, 43, 44, 54, 72, 74, 83},
		24: {44},
		25: {11, 12, 13, 14, 15, 28, 49, 50, 51, 56, 57, 58, 59, 60, 62, 63, 64, 65, 66, 67, 68, 85},
		26: {55},
		27: {65},
		28: {78},
		29: {79},
		30: {81},
		31: {76, 78, 79, 80},
		32: {16, 26, 27, 31, 40, 93},
		33: {16, 26, 31, 40, 93},
		34: {16, 26, 31, 40, 93},
		35: {3},
		36: {18},
		37: {0, 19},
		38: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		39: {},
		40: {1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		41: {2, 8, 30, 56, 62, 63, 65, 66, 92},
		42: {11, 12, 13, 15, 50, 67},
		43: {5, 6, 7, 8, 11, 12, 13, 14, 15, 25, 26, 27, 28, 29, 30, 31, 37, 38, 40, 49, 50, 51, 52, 58, 75, 92},
		44: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93},
		45: {26, 40},
		46: {11, 12, 13, 14, 15, 28, 49, 50, 51, 54, 57, 58, 59, 60, 62, 63, 64, 67, 68, 85},
		47: {24},
		48: {25, 29},
		49: {31},
		50: {71},
		51: {73},
		52: {3, 24, 35, 47, 70, 82},
		53: {3, 35, 82},
		54: {2, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 37, 38, 40, 41, 42, 43, 44, 45, 46, 74, 75, 83, 85},
		55: {4, 10, 36, 39, 84},
		56: {9, 39, 40, 41, 42, 43, 44, 45, 74, 75, 83},
		57: {49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 65, 66, 67, 68, 69},
		58: {19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70},
		59: {20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69},
		60: {26, 40},
		61: {20, 42, 43, 54},
		62: {43, 54},
		63: {},
		64: {21, 29, 43, 44, 47, 54},
		65: {34, 45},
		66: {},
		67: {24, 35, 47, 70},
		68: {51, 52, 58, 60},
		69: {51, 58, 60},
		70: {61},
		71: {},
		72: {64},
		73: {},
		74: {63},
		75: {73},
		76: {71},
		77: {71, 73},
		78: {73},
		79: {},
		80: {76, 78, 79, 80},
		81: {88, 89},
		82: {88, 89},
		83: {},
		84: {90, 91},
		85: {91},
	}

	cvey.Convey("", t, func() {
		for i := 0; i < len(patterns); i++ {
			for j := 0; j < len(targets); j++ {

				//fmt.Println(pat[i], str[j])
				ret := WildcardMatch(patterns[i], targets[j])
				resArr := want[i]
				idx := sort.SearchInts(resArr, j)
				if idx >= len(resArr) || resArr[idx] != j {
					cvey.So(ret, cvey.ShouldBeFalse)
				} else {
					cvey.So(ret, cvey.ShouldBeTrue)
				}
			}
		}
	})
}

func TestGetSimpleExprValue(t *testing.T) {
	cvey.Convey("", t, func() {
		type args struct {
			sql     string
			wantErr bool
			want    interface{}
		}

		kases := []args{
			{"set @@x=1", false, 1},
			{"set @@x=-1", false, -1},
			{"set @@x=1.0", false, 1.0},
			{"set @@x=-1.0", false, -1.0},
			{fmt.Sprintf("set @@x=%d", math.MaxInt64), false, math.MaxInt64},
			{fmt.Sprintf("set @@x=%d", -math.MaxInt64), false, -math.MaxInt64},
			{"set @@x=true", false, true},
			{"set @@x=false", false, false},
			{"set @@x=on", false, "on"},
			{"set @@x=off", false, "off"},
			{"set @@x=abc", false, "abc"},
			{"set @@x=null", false, nil},
			{"set @@x=-null", true, nil},
			{"set @@x=-x", true, nil},
		}

		for _, kase := range kases {
			stmt, err := parsers.ParseOne(dialect.MYSQL, kase.sql)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)

			value, err := GetSimpleExprValue(sv.Assignments[0].Value)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				cvey.So(value, cvey.ShouldEqual, kase.want)
			}
		}

	})
}

func Test_AllocateBatchBasedOnEngineAttributeDefinition(t *testing.T) {
	var attributeDefs []*engine.AttributeDef
	var rowCount = 1
	var colNum = 14
	var tmp = &engine.AttributeDef{}
	var ret *batch.Batch
	cvey.Convey("", t, func() {
		attributeDefs = make([]*engine.AttributeDef, colNum)
		tmp.Attr.Type.Oid = types.T_bool

		for i := 0; i < colNum; i++ {
			attributeDefs[i] = tmp
		}

		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[0].Data, cvey.ShouldResemble, []byte{0})
		cvey.So(ret.Vecs[0].Data, cvey.ShouldResemble, []byte{0})

		tmp.Attr.Type.Oid = types.T_int8
		attributeDefs[1] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[1].Data, cvey.ShouldResemble, []byte{0})
		cvey.So(ret.Vecs[1].Data, cvey.ShouldResemble, []byte{0})

		tmp.Attr.Type.Oid = types.T_int16
		attributeDefs[2] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[2].Data, cvey.ShouldResemble, make([]byte, 2))
		cvey.So(ret.Vecs[2].Data, cvey.ShouldResemble, make([]byte, 2))

		tmp.Attr.Type.Oid = types.T_int32
		attributeDefs[3] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[3].Data, cvey.ShouldResemble, make([]byte, 4))
		cvey.So(ret.Vecs[3].Data, cvey.ShouldResemble, make([]byte, 4))

		tmp.Attr.Type.Oid = types.T_int64
		attributeDefs[4] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[4].Data, cvey.ShouldResemble, make([]byte, 8))
		cvey.So(ret.Vecs[4].Data, cvey.ShouldResemble, make([]byte, 8))

		tmp.Attr.Type.Oid = types.T_uint8
		attributeDefs[5] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[5].Data, cvey.ShouldResemble, []byte{0})
		cvey.So(ret.Vecs[5].Data, cvey.ShouldResemble, []byte{0})

		tmp.Attr.Type.Oid = types.T_uint16
		attributeDefs[6] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[6].Data, cvey.ShouldResemble, make([]byte, 2))
		cvey.So(ret.Vecs[6].Data, cvey.ShouldResemble, make([]byte, 2))

		tmp.Attr.Type.Oid = types.T_uint32
		attributeDefs[7] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[7].Data, cvey.ShouldResemble, make([]byte, 4))
		cvey.So(ret.Vecs[7].Data, cvey.ShouldResemble, make([]byte, 4))

		tmp.Attr.Type.Oid = types.T_uint64
		attributeDefs[8] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[8].Data, cvey.ShouldResemble, make([]byte, 8))
		cvey.So(ret.Vecs[8].Data, cvey.ShouldResemble, make([]byte, 8))

		tmp.Attr.Type.Oid = types.T_float32
		attributeDefs[9] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[9].Data, cvey.ShouldResemble, make([]byte, 4))
		cvey.So(ret.Vecs[9].Data, cvey.ShouldResemble, make([]byte, 4))

		tmp.Attr.Type.Oid = types.T_float64
		attributeDefs[10] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[10].Data, cvey.ShouldResemble, make([]byte, 8))
		cvey.So(ret.Vecs[10].Data, cvey.ShouldResemble, make([]byte, 8))

		tmp.Attr.Type.Oid = types.T_char
		attributeDefs[11] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[11].Data, cvey.ShouldResemble, []byte(nil))
		cvey.So(ret.Vecs[11].Data, cvey.ShouldResemble, []byte(nil))

		tmp.Attr.Type.Oid = types.T_date
		attributeDefs[12] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[12].Data, cvey.ShouldResemble, make([]byte, 4))
		cvey.So(ret.Vecs[12].Data, cvey.ShouldResemble, make([]byte, 4))

		tmp.Attr.Type.Oid = types.T_datetime
		attributeDefs[13] = tmp
		ret = AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs, rowCount)
		cvey.So(ret.Vecs[13].Data, cvey.ShouldResemble, make([]byte, 8))
		cvey.So(ret.Vecs[13].Data, cvey.ShouldResemble, make([]byte, 8))
	})
}

func Test_FillBatchWithData(t *testing.T) {
	var data [][]string
	var batch = &batch.Batch{}
	var colNum = 14
	cvey.Convey("FillBatchWithData succ", t, func() {
		data = make([][]string, 1)
		data[0] = make([]string, 1)
		batch.Vecs = make([]*vector.Vector, colNum)
		batch.Vecs[0] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_bool}}
		batch.Vecs[0].Col = make([]bool, 1)
		FillBatchWithData(data, batch)

		data[0][0] = "true"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[1] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int8}}
		batch.Vecs[1].Col = make([]int8, 1)
		FillBatchWithData(data, batch)

		data[0][1] = "1"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[2] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int16}}
		batch.Vecs[2].Col = make([]int16, 1)
		FillBatchWithData(data, batch)

		data[0][2] = "2"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[3] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int32}}
		batch.Vecs[3].Col = make([]int32, 1)
		FillBatchWithData(data, batch)

		data[0][3] = "3"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[4] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int64}}
		batch.Vecs[4].Col = make([]int64, 1)
		FillBatchWithData(data, batch)

		data[0][4] = "4"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[5] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint8}}
		batch.Vecs[5].Col = make([]uint8, 1)
		FillBatchWithData(data, batch)

		data[0][5] = "5"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[6] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint16}}
		batch.Vecs[6].Col = make([]uint16, 1)
		FillBatchWithData(data, batch)

		data[0][6] = "5"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[7] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint32}}
		batch.Vecs[7].Col = make([]uint32, 1)
		FillBatchWithData(data, batch)

		data[0][7] = "7"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[8] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint64}}
		batch.Vecs[8].Col = make([]uint64, 1)
		FillBatchWithData(data, batch)

		data[0][8] = "5"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[9] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_float32}}
		batch.Vecs[9].Col = make([]float32, 1)
		FillBatchWithData(data, batch)

		data[0][9] = "9"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[10] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_float64}}
		batch.Vecs[10].Col = make([]float64, 1)
		FillBatchWithData(data, batch)

		data[0][10] = "10"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[11] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_date}}
		batch.Vecs[11].Col = make([]types.Date, 1)
		FillBatchWithData(data, batch)

		data[0][11] = "2022-07-13"
		FillBatchWithData(data, batch)

		data[0] = append(data[0], string(""))
		batch.Vecs[12] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_datetime}}
		batch.Vecs[12].Col = make([]types.Datetime, 1)
		FillBatchWithData(data, batch)

		data[0][12] = "2022-07-13 11:11:11.1234"
		FillBatchWithData(data, batch)
	})
}

func Test_FormatLineInBatch(t *testing.T) {
	var bat = &batch.Batch{}
	var rowIndex = 0
	var res []string
	var colNum = 13
	var colName = []string{"false", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0001-01-01", "0001-01-01 00:00:00"}
	cvey.Convey("FormatLineInBatch succ", t, func() {
		bat.Vecs = make([]*vector.Vector, colNum)
		bat.Vecs[0] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_bool}}
		bat.Vecs[0].Col = make([]bool, 1)

		bat.Vecs[1] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int8}}
		bat.Vecs[1].Col = make([]int8, 1)

		bat.Vecs[2] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int16}}
		bat.Vecs[2].Col = make([]int16, 1)

		bat.Vecs[3] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int32}}
		bat.Vecs[3].Col = make([]int32, 1)

		bat.Vecs[4] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_int64}}
		bat.Vecs[4].Col = make([]int64, 1)

		bat.Vecs[5] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint8}}
		bat.Vecs[5].Col = make([]uint8, 1)

		bat.Vecs[6] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint16}}
		bat.Vecs[6].Col = make([]uint16, 1)

		bat.Vecs[7] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint32}}
		bat.Vecs[7].Col = make([]uint32, 1)

		bat.Vecs[8] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_uint64}}
		bat.Vecs[8].Col = make([]uint64, 1)

		bat.Vecs[9] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_float32}}
		bat.Vecs[9].Col = make([]float32, 1)

		bat.Vecs[10] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_float64}}
		bat.Vecs[10].Col = make([]float64, 1)

		bat.Vecs[11] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_date}}
		bat.Vecs[11].Col = make([]types.Date, 1)

		bat.Vecs[12] = &vector.Vector{Nsp: &nulls.Nulls{}, Typ: types.Type{Oid: types.T_datetime}}
		bat.Vecs[12].Col = make([]types.Datetime, 1)

		res = FormatLineInBatch(bat, rowIndex)
		cvey.So(res, cvey.ShouldResemble, colName)

		bat.Vecs[0].Col = make([]bool, 2)
		nulls.Add(bat.Vecs[0].Nsp, 1)

		bat.Vecs[1].Col = make([]int8, 2)
		nulls.Add(bat.Vecs[1].Nsp, 1)

		bat.Vecs[2].Col = make([]int16, 2)
		nulls.Add(bat.Vecs[2].Nsp, 1)

		bat.Vecs[3].Col = make([]int32, 2)
		nulls.Add(bat.Vecs[3].Nsp, 1)

		bat.Vecs[4].Col = make([]int64, 2)
		nulls.Add(bat.Vecs[4].Nsp, 1)

		bat.Vecs[5].Col = make([]uint8, 2)
		nulls.Add(bat.Vecs[5].Nsp, 1)

		bat.Vecs[6].Col = make([]uint16, 2)
		nulls.Add(bat.Vecs[6].Nsp, 1)

		bat.Vecs[7].Col = make([]uint32, 2)
		nulls.Add(bat.Vecs[7].Nsp, 1)

		bat.Vecs[8].Col = make([]uint64, 2)
		nulls.Add(bat.Vecs[8].Nsp, 1)

		bat.Vecs[9].Col = make([]float32, 2)
		nulls.Add(bat.Vecs[9].Nsp, 1)

		bat.Vecs[10].Col = make([]float64, 2)
		nulls.Add(bat.Vecs[10].Nsp, 1)

		bat.Vecs[11].Col = make([]types.Date, 2)
		nulls.Add(bat.Vecs[11].Nsp, 1)

		bat.Vecs[12].Col = make([]types.Datetime, 2)
		nulls.Add(bat.Vecs[12].Nsp, 1)

		res = FormatLineInBatch(bat, rowIndex)
		cvey.So(res, cvey.ShouldResemble, colName)

		res = FormatLineInBatch(bat, 1)
		cvey.So(res, cvey.ShouldResemble, []string{"<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"})
	})
}
