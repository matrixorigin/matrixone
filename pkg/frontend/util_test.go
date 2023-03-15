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
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

	cvey.Convey("max", t, func() {
		cvey.So(Max(10, 9), cvey.ShouldEqual, 10)
		cvey.So(Max(9, 10), cvey.ShouldEqual, 10)
	})
}

func Test_routineid(t *testing.T) {
	cvey.Convey("rtid", t, func() {
		x := GetRoutineId()
		cvey.So(x, cvey.ShouldBeGreaterThanOrEqualTo, 0)
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
	ctx := context.TODO()
	cvey.Convey("", t, func() {
		type args struct {
			sql     string
			wantErr bool
			want    interface{}
		}

		kases := []args{
			{"set @@x=1", false, 1},
			{"set @@x=-1", false, -1},
			{fmt.Sprintf("set @@x=%d", math.MaxInt64), false, math.MaxInt64},
			{fmt.Sprintf("set @@x=%d", -math.MaxInt64), false, -math.MaxInt64},
			{"set @@x=true", false, true},
			{"set @@x=false", false, false},
			{"set @@x=on", false, "on"},
			{"set @@x=off", false, "off"},
			{"set @@x=abc", false, "abc"},
			{"set @@x=null", false, nil},
			{"set @@x=-null", false, nil},
			{"set @@x=-x", true, nil},
		}
		ctrl := gomock.NewController(t)
		ses := NewSession(&FakeProtocol{}, testutil.NewProc().Mp(), config.NewParameterUnit(nil, mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockTxnClient(ctrl), nil), nil, false)
		ses.txnCompileCtx.SetProcess(testutil.NewProc())
		for _, kase := range kases {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := GetSimpleExprValue(sv.Assignments[0].Value, ses)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				cvey.So(value, cvey.ShouldEqual, kase.want)
			}
		}

	})

	cvey.Convey("", t, func() {
		type args struct {
			sql     string
			wantErr bool
			want    interface{}
		}

		dec1, _, _ := types.Parse64("1.0")
		dec2, _, _ := types.Parse64("-1.0")
		dec3, _, _ := types.Parse64("-1.2345670")

		kases := []args{
			{"set @@x=1.0", false, plan.MakePlan2Decimal64ExprWithType(dec1, &plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       18,
				Scale:       1,
				NotNullable: true,
			})},
			{"set @@x=-1.0", false, plan.MakePlan2Decimal64ExprWithType(dec2, &plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       18,
				Scale:       1,
				NotNullable: true,
			})},
			{"set @@x=-1.2345670", false, plan.MakePlan2Decimal64ExprWithType(dec3, &plan.Type{
				Id:          int32(types.T_decimal64),
				Width:       18,
				Scale:       7,
				NotNullable: true,
			})},
		}
		ctrl := gomock.NewController(t)
		ses := NewSession(&FakeProtocol{}, testutil.NewProc().Mp(), config.NewParameterUnit(nil, mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockTxnClient(ctrl), nil), nil, false)
		ses.txnCompileCtx.SetProcess(testutil.NewProc())
		for _, kase := range kases {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := GetSimpleExprValue(sv.Assignments[0].Value, ses)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				cvey.So(value, cvey.ShouldResemble, kase.want)
			}
		}

	})
}

func TestFileExists(t *testing.T) {
	cvey.Convey("test file exists", t, func() {
		exist, err := fileExists("test.txt")
		cvey.So(err, cvey.ShouldBeNil)
		cvey.So(exist, cvey.ShouldBeFalse)
	})
}

func TestGetAttrFromTableDef(t *testing.T) {
	cvey.Convey("test get attr from table def", t, func() {
		want := []string{"id", "a", "b"}
		defs := []engine.TableDef{
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Name: "id",
				},
			},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Name: "a",
				},
			},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Name: "b",
				},
			},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Name:    "c",
					IsRowId: true,
				},
			},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Name:     "d",
					IsHidden: true,
				},
			},
		}
		act, isView, err := getAttrFromTableDef(defs)
		cvey.So(err, cvey.ShouldBeNil)
		cvey.So(isView, cvey.ShouldBeFalse)
		cvey.So(act, cvey.ShouldResemble, want)
	})
}

func TestGetDDL(t *testing.T) {
	ctx := context.TODO()

	cvey.Convey("test get ddl", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		want0 := []byte("create database test;")
		want1 := []byte("create table test.t1 (id int);")
		rs0 := []interface{}{&MysqlResultSet{Data: [][]interface{}{{want0, want0}}}}
		rs1 := []interface{}{&MysqlResultSet{Data: [][]interface{}{{want0, want1}}}}
		bh := mock_frontend.NewMockBackgroundExec(ctrl)

		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Close().Return().AnyTimes()
		cnt := -1
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() ([]interface{}, error) {
			cnt++
			if cnt == 0 {
				return rs0, nil
			}
			return rs1, nil
		}).AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ret, err := getDDL(bh, ctx, "")
		cvey.So(err, cvey.ShouldBeNil)
		cvey.So(ret, cvey.ShouldResemble, string(want0))
		ret, err = getDDL(bh, ctx, "")
		cvey.So(err, cvey.ShouldBeNil)
		cvey.So(ret, cvey.ShouldResemble, string(want1))
	})
}

func TestConvertValueBat2Str(t *testing.T) {
	var (
		typs = []types.Type{
			types.T_bool.ToType(),
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(),
			types.T_uint8.ToType(),
			types.T_uint16.ToType(),
			types.T_uint32.ToType(),
			types.T_uint64.ToType(),
			types.T_float32.ToType(),
			types.T_float64.ToType(),
			types.T_decimal64.ToType(),
			types.T_decimal128.ToType(),
			types.T_date.ToType(),
			types.T_datetime.ToType(),
			types.T_timestamp.ToType(),
			types.T_varchar.ToType(),
			types.T_char.ToType(),
			types.T_json.ToType(),
		}
	)
	before := testutil.TestUtilMp.CurrNB()
	bat := testutil.NewBatch(typs, true, 5, testutil.TestUtilMp)
	rbat, err := convertValueBat2Str(context.TODO(), bat, testutil.TestUtilMp, time.Local)
	require.Nil(t, err)
	require.NotNil(t, rbat)
	bat.Clean(testutil.TestUtilMp)
	rbat.Clean(testutil.TestUtilMp)
	after := testutil.TestUtilMp.CurrNB()
	require.Equal(t, before, after)
}

func TestGenDumpFileName(t *testing.T) {
	base := "test.sql"
	want := "test_1.sql"
	got := genDumpFileName(base, 1)
	require.Equal(t, want, got)
}

func TestCreateDumpFile(t *testing.T) {
	base := "test_dump_" + time.Now().Format("20060102150405") + ".sql"
	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
		os.RemoveAll(base)
	}()
	f, err := createDumpFile(context.TODO(), base)
	require.Nil(t, err)
	require.NotNil(t, f)
}

func TestWriteDump2File(t *testing.T) {
	ctx := context.TODO()
	base := "test_dump_" + time.Now().Format("20060102150405") + ".sql"
	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
		removeFile(base, 1)
		removeFile(base, 2)
	}()
	f, err := createDumpFile(ctx, base)
	require.Nil(t, err)
	require.NotNil(t, f)
	dump := &tree.MoDump{
		OutFile:     base,
		MaxFileSize: 1,
	}
	buf := bytes.NewBufferString("test")
	curFileSize, curFileIdx := int64(0), int64(1)
	_, _, _, err = writeDump2File(ctx, buf, dump, f, curFileIdx, curFileSize)
	require.NotNil(t, err)
	dump.MaxFileSize = 1024
	bufSize := buf.Len()
	f, curFileIdx, curFileSize, err = writeDump2File(ctx, buf, dump, f, curFileIdx, curFileSize)
	require.Nil(t, err)
	require.NotNil(t, f)
	require.Equal(t, int64(1), curFileIdx)
	require.Equal(t, int64(bufSize), curFileSize)
	dump.MaxFileSize = 9
	buf.WriteString("123456")
	bufSize = buf.Len()
	f, curFileIdx, curFileSize, err = writeDump2File(ctx, buf, dump, f, curFileIdx, curFileSize)
	require.Nil(t, err)
	require.NotNil(t, f)
	require.Equal(t, int64(2), curFileIdx)
	require.Equal(t, int64(bufSize), curFileSize)
}

func TestMaybeAppendExtension(t *testing.T) {
	base := "test"
	want := "test.sql"
	got := maybeAppendExtension(base)
	require.Equal(t, want, got)
	got = maybeAppendExtension(want)
	require.Equal(t, want, got)
}

func TestRemovePrefixComment(t *testing.T) {
	require.Equal(t, "abcd", removePrefixComment("abcd"))
	require.Equal(t, "abcd", removePrefixComment("/*11111*/abcd"))
	require.Equal(t, "abcd", removePrefixComment("/**/abcd"))
	require.Equal(t, "/*/abcd", removePrefixComment("/*/abcd"))
	require.Equal(t, "/*abcd", removePrefixComment("/*abcd"))
	require.Equal(t, "*/abcd", removePrefixComment("*/abcd"))
}
