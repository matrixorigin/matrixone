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
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	cvey "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	testutil.SetupAutoIncrService("")
}

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
		ses := newTestSession(t, ctrl)
		//ses := NewSession(&FakeProtocol{}, testutil.NewProc().Mp(), config.NewParameterUnit(nil, mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockTxnClient(ctrl), nil), GSysVariables, false, nil, nil)
		ec := newTestExecCtx(ctx, ctrl)
		ec.proc = testutil.NewProc()
		ec.ses = ses
		ses.txnCompileCtx.execCtx = ec
		for _, kase := range kases {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := GetSimpleExprValue(ctx, sv.Assignments[0].Value, ses)
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
			{"set @@x=1.0", false, fmt.Sprintf("%v", dec1.Format(1))},
			{"set @@x=-1.0", false, fmt.Sprintf("%v", dec2.Format(1))},
			{"set @@x=-1.2345670", false, fmt.Sprintf("%v", dec3.Format(7))},
		}
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		//ses := NewSession(&FakeProtocol{}, testutil.NewProc().Mp(), config.NewParameterUnit(nil, mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockTxnClient(ctrl), nil), GSysVariables, false, nil, nil)
		ec := newTestExecCtx(ctx, ctrl)
		ec.proc = testutil.NewProc()
		ec.ses = ses
		ses.txnCompileCtx.execCtx = ec
		for _, kase := range kases {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := GetSimpleExprValue(ctx, sv.Assignments[0].Value, ses)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				cvey.So(value, cvey.ShouldResemble, kase.want)
			}
		}

	})
}

func TestGetExprValue(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
	catalog.SetupDefines("")
	cvey.Convey("", t, func() {
		type args struct {
			sql     string
			wantErr bool
			want    interface{}
		}

		// dec1280, _, err := types.Parse128("-9223372036854775808")
		// assert.NoError(t, err)

		// dec1281, _, err := types.Parse128("99999999999999999999999999999999999999")
		// assert.NoError(t, err)

		// dec1282, _, err := types.Parse128("-99999999999999999999999999999999999999")
		// assert.NoError(t, err)

		// dec1283, _, err := types.Parse128("9223372036854775807")
		// assert.NoError(t, err)

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
			{"set @@x=(select -t.b from t)", true, nil},
			{"set @@x=(select 1)", false, 1},
			{"set @@x=(select -1)", false, -1},
			{fmt.Sprintf("set @@x=(select %d)", math.MaxInt64), false, math.MaxInt64},
			{fmt.Sprintf("set @@x=(select %d)", -math.MaxInt64), false, -math.MaxInt64},
			{"set @@x=(select true)", false, true},
			{"set @@x=(select false)", false, false},
			{"set @@x=(select 'on')", false, "on"},
			{"set @@x=(select 'off')", false, "off"},
			{"set @@x=(select 'abc')", false, "abc"},
			{"set @@x=(select null)", false, nil},
			{"set @@x=(select -null)", false, nil},
			{"set @@x=(select true != false)", false, true},
			{"set @@x=(select true = false)", false, false},
			{"set @@x=(" +
				"select true = (" +
				"				select 1 = (select 0 + (" +
				"											select (select 1 + (select 2 - 0))" +
				"										)" +
				"							)" +
				"				)" +
				")", false, false},
			{"set @@x=(" +
				"			(select (3 < 4))" +
				" = " +
				"			(select (1 > 4))" +
				"		)", false, false},
			{"set @@x=(select 127)", false, 127},
			{"set @@x=(select -128)", false, -128},
			{"set @@x=(select -2147483648)", false, -2147483648},
			{"set @@x=(select -9223372036854775808)", false, "-9223372036854775808"},
			{"set @@x=(select 18446744073709551615)", false, uint64(math.MaxUint64)},
			{"set @@x=(select 1.1754943508222875e-38)", false, float32(1.1754943508222875e-38)},
			{"set @@x=(select 3.4028234663852886e+38)", false, float32(3.4028234663852886e+38)},
			{"set @@x=(select  2.2250738585072014e-308)", false, float64(2.2250738585072014e-308)},
			{"set @@x=(select  1.7976931348623157e+308)", false, float64(1.7976931348623157e+308)},
			{"set @@x=(select cast(9223372036854775807 as decimal))", false, "9223372036854775807"},
			{"set @@x=(select cast(99999999999999999999999999999999999999 as decimal))", false, "99999999999999999999999999999999999999"},
			{"set @@x=(select cast(-99999999999999999999999999999999999999 as decimal))", false, "-99999999999999999999999999999999999999"},
			{"set @@x=(select cast('{\"a\":1,\"b\":2}' as json))", false, "{\"a\": 1, \"b\": 2}"},
			{"set @@x=(select cast('00000000-0000-0000-0000-000000000000' as uuid))", false, "00000000-0000-0000-0000-000000000000"},
			{"set @@x=(select cast('00:00:00' as time))", false, "00:00:00"},
			{"set @@x=(select cast('1000-01-01 00:00:00' as datetime))", false, "1000-01-01 00:00:00"},
			{"set @@x=(select cast('1970-01-01 00:00:00' as timestamp))", false, "1970-01-01 00:00:00"},
			{"set @@x=(select 1 into outfile './test.csv')", false, 1}, //!!!NOTE: there is no file './test.csv'.
			{"set @@x=(((select true = false)))", false, false},
		}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(ctx).Return([]string{"t"}, nil).AnyTimes()
		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		table.EXPECT().GetTableID(gomock.Any()).Return(uint64(0xABC)).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), "t", nil).Return(table, moerr.NewInternalErrorNoCtx("no such table")).AnyTimes()
		defs := []engine.TableDef{
			&engine.AttributeDef{Attr: engine.Attribute{Name: "a", Type: types.T_char.ToType()}},
			&engine.AttributeDef{Attr: engine.Attribute{Name: "b", Type: types.T_int32.ToType()}},
		}

		table.EXPECT().TableDefs(gomock.Any()).Return(defs, nil).AnyTimes()
		table.EXPECT().GetEngineType().Return(engine.Disttae).AnyTimes()

		var ranges memoryengine.ShardIdSlice
		id := make([]byte, 8)
		binary.LittleEndian.PutUint64(id, 1)
		ranges.Append(id)

		relData := &memoryengine.MemRelationData{
			Shards: ranges,
		}
		table.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any()).Return(relData, nil).AnyTimes()
		//table.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInvalidInputNoCtx("new reader failed")).AnyTimes()

		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		eng.EXPECT().Nodes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		ws.EXPECT().IncrStatementID(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ws.EXPECT().IncrSQLCount().AnyTimes()
		ws.EXPECT().GetSQLCount().AnyTimes()
		ws.EXPECT().StartStatement().AnyTimes()
		ws.EXPECT().EndStatement().AnyTimes()
		ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
		ws.EXPECT().UpdateSnapshotWriteOffset().AnyTimes()
		ws.EXPECT().Adjust(gomock.Any()).AnyTimes()
		ws.EXPECT().CloneSnapshotWS().AnyTimes()
		ws.EXPECT().BindTxnOp(gomock.Any()).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(ws).AnyTimes()
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
		txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
		txnOperator.EXPECT().GetWaitActiveCost().Return(time.Duration(0)).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv := &config.FrontendParameters{
			SessionTimeout: toml.Duration{Duration: 10 * time.Second},
		}

		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		setGlobalPu(pu)
		ses := NewSession(ctx, "", &testMysqlWriter{}, testutil.NewProc().Mp())
		ses.SetDatabaseName("db")
		var c clock.Clock
		err := ses.GetTxnHandler().CreateTempStorage(c)
		assert.Nil(t, err)
		ec := newTestExecCtx(ctx, ctrl)
		ec.proc = testutil.NewProc()
		ec.ses = ses
		ses.txnCompileCtx.execCtx = ec
		for _, kase := range kases {
			fmt.Println("++++>", kase.sql)
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := getExprValue(sv.Assignments[0].Value, ses, ec)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				switch ret := value.(type) {
				case *plan.Expr:
					if types.T(ret.GetTyp().Id) == types.T_decimal64 {
						cvey.So(ret.GetLit().GetDecimal64Val().GetA(), cvey.ShouldEqual, kase.want)
					} else if types.T(ret.GetTyp().Id) == types.T_decimal128 {
						temp := kase.want.(types.Decimal128)
						cvey.So(uint64(ret.GetLit().GetDecimal128Val().GetA()), cvey.ShouldEqual, temp.B0_63)
						cvey.So(uint64(ret.GetLit().GetDecimal128Val().GetB()), cvey.ShouldEqual, temp.B64_127)
					} else {
						panic(fmt.Sprintf("unknown expr type %v", ret.GetTyp()))
					}
				default:
					cvey.So(value, cvey.ShouldEqual, kase.want)
				}
			}
		}

	})

	cvey.Convey("", t, func() {
		type args struct {
			sql     string
			wantErr bool
			want    interface{}
		}

		// dec1, _, _ := types.Parse64("1.0")
		// dec2, _, _ := types.Parse64("-1.0")
		// dec3, _, _ := types.Parse64("-1.2345670")

		kases := []args{
			{"set @@x=1.0", false, "1.0"},
			{"set @@x=-1.0", false, "-1.0"},
			{"set @@x=-1.2345670", false, "-1.2345670"},
		}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(ctx).Return([]string{"t"}, nil).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(ctx, "t", nil).Return(table, nil).AnyTimes()
		defs := []engine.TableDef{
			&engine.AttributeDef{Attr: engine.Attribute{Name: "a", Type: types.T_char.ToType()}},
			&engine.AttributeDef{Attr: engine.Attribute{Name: "b", Type: types.T_int32.ToType()}},
		}

		table.EXPECT().TableDefs(ctx).Return(defs, nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(db, nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		eng.EXPECT().Nodes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		ws.EXPECT().IncrStatementID(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ws.EXPECT().StartStatement().AnyTimes()
		ws.EXPECT().EndStatement().AnyTimes()
		ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
		ws.EXPECT().UpdateSnapshotWriteOffset().AnyTimes()
		ws.EXPECT().Adjust(uint64(0)).AnyTimes()
		ws.EXPECT().IncrSQLCount().AnyTimes()
		ws.EXPECT().GetSQLCount().AnyTimes()
		ws.EXPECT().CloneSnapshotWS().AnyTimes()
		ws.EXPECT().BindTxnOp(gomock.Any()).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(ws).AnyTimes()
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
		txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
		txnOperator.EXPECT().GetWaitActiveCost().Return(time.Duration(0)).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv := &config.FrontendParameters{
			SessionTimeout: toml.Duration{Duration: 5 * time.Minute},
		}

		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		setGlobalPu(pu)
		ses := NewSession(ctx, "", &testMysqlWriter{}, testutil.NewProc().Mp())
		var c clock.Clock
		err := ses.GetTxnHandler().CreateTempStorage(c)
		assert.Nil(t, err)
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ses.txnCompileCtx.execCtx = ec
		for _, kase := range kases {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, kase.sql, 1)
			cvey.So(err, cvey.ShouldBeNil)

			sv, ok := stmt.(*tree.SetVar)
			cvey.So(ok, cvey.ShouldBeTrue)
			value, err := getExprValue(sv.Assignments[0].Value, ses, ec)
			if kase.wantErr {
				cvey.So(err, cvey.ShouldNotBeNil)
			} else {
				cvey.So(err, cvey.ShouldBeNil)
				cvey.So(value, cvey.ShouldResemble, kase.want)
			}
		}

	})
}

var _ error = &testError{}

type testError struct {
	s string
}

func (t testError) Error() string {
	return t.s
}

func TestRewriteError(t *testing.T) {
	type args struct {
		err      error
		username string
	}

	tests := []struct {
		name  string
		args  args
		want  uint16
		want1 string
		want2 string
	}{
		{
			name: "t1",
			args: args{
				err: &testError{s: "non moerr"},
			},
			want:  moerr.ER_INTERNAL_ERROR,
			want1: "HY000",
			want2: "non moerr",
		},
		{
			name:  "t2",
			args:  args{},
			want:  moerr.ER_INTERNAL_ERROR,
			want1: "",
			want2: "",
		},
		{
			name: "t3",
			args: args{
				err:      moerr.NewInternalErrorNoCtx("check password failed"),
				username: "abc",
			},
			want:  moerr.ER_ACCESS_DENIED_ERROR,
			want1: "28000",
			want2: "Access denied for user abc. internal error: check password failed",
		},
		{
			name: "t4",
			args: args{
				err:      moerr.NewInternalErrorNoCtx("suspended"),
				username: "abc",
			},
			want:  moerr.ER_ACCESS_DENIED_ERROR,
			want1: "28000",
			want2: "Access denied for user abc. internal error: suspended",
		},
		{
			name: "t5",
			args: args{
				err:      moerr.NewInternalErrorNoCtx("suspended"),
				username: "abc",
			},
			want:  moerr.ER_ACCESS_DENIED_ERROR,
			want1: "28000",
			want2: "Access denied for user abc. internal error: suspended",
		},
		{
			name: "t6",
			args: args{
				err:      moerr.NewInternalErrorNoCtx("source address     is not authorized"),
				username: "abc",
			},
			want:  moerr.ER_ACCESS_DENIED_ERROR,
			want1: "28000",
			want2: "Access denied for user abc. internal error: source address     is not authorized",
		},
		{
			name: "t7",
			args: args{
				err:      moerr.NewInternalErrorNoCtx("xxxx"),
				username: "abc",
			},
			want:  moerr.ErrInternal,
			want1: "HY000",
			want2: "internal error: xxxx",
		},
		{
			name: "t8",
			args: args{
				err:      moerr.NewBadDBNoCtx("yyy"),
				username: "abc",
			},
			want:  moerr.ER_BAD_DB_ERROR,
			want1: "HY000",
			want2: "invalid database yyy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := RewriteError(tt.args.err, tt.args.username)
			assert.Equalf(t, tt.want, got, "RewriteError(%v, %v)", tt.args.err, tt.args.username)
			assert.Equalf(t, tt.want1, got1, "RewriteError(%v, %v)", tt.args.err, tt.args.username)
			assert.Equalf(t, tt.want2, got2, "RewriteError(%v, %v)", tt.args.err, tt.args.username)
		})
	}
}

func Test_makeExecuteSql(t *testing.T) {
	type args struct {
		ses  *Session
		stmt tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)

	sv := &config.FrontendParameters{
		SessionTimeout: toml.Duration{Duration: 5 * time.Minute},
	}
	ctx := context.TODO()
	pu := config.NewParameterUnit(sv, eng, txnClient, nil)
	setGlobalPu(pu)
	ses1 := NewSession(ctx, "", &testMysqlWriter{}, testutil.NewProc().Mp())

	ses1.SetUserDefinedVar("var2", "val2", "set var2 = val2")
	ses1.SetUserDefinedVar("var3", "val3", "set var3 = val3")
	ses1.SetPrepareStmt(ctx, "st2", &PrepareStmt{
		Name: "st2",
		Sql:  "prepare st2 select * from t where a = ?",
	})
	ses1.SetPrepareStmt(ctx, "st3", &PrepareStmt{
		Name: "st3",
		Sql:  "prepare st3 select * from t where a = ? and b = ?",
	})

	mp, err := mpool.NewMPool("ut_pool", 0, mpool.NoFixed)
	if err != nil {
		assert.NoError(t, err)
	}
	defer mpool.DeleteMPool(mp)

	testProc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil)

	params1 := vector.NewVec(types.T_text.ToType())
	for i := 0; i < 3; i++ {
		err = vector.AppendBytes(params1, []byte{}, false, testProc.GetMPool())
		assert.NoError(t, err)
	}

	util.SetAnyToStringVector(testProc, "aVal", params1, 0)
	util.SetAnyToStringVector(testProc, "NULL", params1, 1)
	util.SetAnyToStringVector(testProc, "bVal", params1, 2)

	ses1.SetPrepareStmt(ctx, "st4", &PrepareStmt{
		Name:   "st4",
		Sql:    "prepare st4 select * from t where a = ? and b = ?",
		params: params1,
	})

	ses1.SetPrepareStmt(ctx, "st5", nil)

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "t1",
			args: args{},
			want: "",
		},
		{
			name: "t2",
			args: args{
				ses:  &Session{},
				stmt: &tree.SetVar{},
			},
			want: "",
		},
		{
			name: "t3",
			args: args{
				ses: ses1,
				stmt: &tree.Execute{
					Name: "st1",
				},
			},
			want: "",
		},
		{
			name: "t4-no variables - no params",
			args: args{
				ses: ses1,
				stmt: &tree.Execute{
					Name: "st2",
				},
			},
			want: "prepare st2 select * from t where a = ? ;",
		},
		{
			name: "t5 - variables",
			args: args{
				ses: ses1,
				stmt: &tree.Execute{
					Name: "st3",
					Variables: []*tree.VarExpr{
						{
							Name: "var2",
						},
						{
							Name: "var-none",
						},
						{
							Name: "var3",
						},
					},
				},
			},
			want: "prepare st3 select * from t where a = ? and b = ? ; set var2 = val2 ;  ; set var3 = val3",
		},
		{
			name: "t6 - params",
			args: args{
				ses: ses1,
				stmt: &tree.Execute{
					Name: "st4",
				},
			},
			want: "prepare st4 select * from t where a = ? and b = ? ; aVal ; NULL ; bVal",
		},
		{
			name: "t7 - params is nil",
			args: args{
				ses: ses1,
				stmt: &tree.Execute{
					Name: "st5",
				},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeExecuteSql(ctx, tt.args.ses, tt.args.stmt); strings.TrimSpace(got) != strings.TrimSpace(tt.want) {
				t.Errorf("makeExecuteSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getVariableValue(t *testing.T) {
	type args struct {
		varDefault interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "0.1", args: args{varDefault: 0.1}, want: "0.100000"},
		{name: "0.000001", args: args{varDefault: 0.000001}, want: "0.000001"},
		{name: "0.0000009", args: args{varDefault: 0.0000009}, want: "9.000000e-07"},
		{name: "7.43e-14", args: args{varDefault: 7.43e-14}, want: "7.430000e-14"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getVariableValue(tt.args.varDefault)
			assert.Equalf(t, tt.want, got, "getVariableValue(%v)", tt.args.varDefault)
		})
	}
}

var _ error = &testErr{}

type testErr struct {
}

func (t testErr) Error() string {
	return "test"
}

func Test_isErrorRollbackWholeTxn(t *testing.T) {
	assert.Equal(t, false, isErrorRollbackWholeTxn(nil))
	assert.Equal(t, false, isErrorRollbackWholeTxn(&testError{}))
	assert.Equal(t, true, isErrorRollbackWholeTxn(moerr.NewDeadLockDetectedNoCtx()))
	assert.Equal(t, true, isErrorRollbackWholeTxn(moerr.NewLockTableBindChangedNoCtx()))
	assert.Equal(t, true, isErrorRollbackWholeTxn(moerr.NewLockTableNotFoundNoCtx()))
	assert.Equal(t, true, isErrorRollbackWholeTxn(moerr.NewDeadlockCheckBusyNoCtx()))
	assert.Equal(t, true, isErrorRollbackWholeTxn(moerr.NewLockConflictNoCtx()))
}

func TestUserInput_getSqlSourceType(t *testing.T) {
	type fields struct {
		sql           string
		stmt          tree.Statement
		sqlSourceType []string
	}
	type args struct {
		i int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "t1",
			fields: fields{
				sql:           "select * from t1",
				sqlSourceType: nil,
			},
			args: args{
				i: 0,
			},
			want: "external_sql",
		},
		{
			name: "t2",
			fields: fields{
				sql:           "select * from t1",
				sqlSourceType: nil,
			},
			args: args{
				i: 1,
			},
			want: "external_sql",
		},
		{
			name: "t3",
			fields: fields{
				sql: "select * from t1",
				sqlSourceType: []string{
					"a",
					"b",
					"c",
				},
			},
			args: args{
				i: 2,
			},
			want: "c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ui := &UserInput{
				sql:           tt.fields.sql,
				stmt:          tt.fields.stmt,
				sqlSourceType: tt.fields.sqlSourceType,
			}
			assert.Equalf(t, tt.want, ui.getSqlSourceType(tt.args.i), "getSqlSourceType(%v)", tt.args.i)
		})
	}
}

func TestTopsort(t *testing.T) {
	cvey.Convey("create graph", t, func() {
		g := toposort{next: make(map[string][]string)}
		g.addVertex("0")
		g.addVertex("1")
		g.addVertex("2")
		g.addVertex("3")
		g.addVertex("4")
		g.addVertex("5")
		g.addEdge("0", "2")
		g.addEdge("1", "2")
		g.addEdge("2", "3")
		g.addEdge("3", "4")
		g.addEdge("3", "5")

		ans, err := g.sort()
		cvey.So(err, cvey.ShouldBeNil)

		sort.StringSlice(ans[:2]).Sort()
		cvey.So(ans[:2], cvey.ShouldResemble, []string{"0", "1"})
		cvey.So(ans[2], cvey.ShouldResemble, "2")
		cvey.So(ans[3], cvey.ShouldResemble, "3")
		sort.StringSlice(ans[4:]).Sort()
		cvey.So(ans[4:], cvey.ShouldResemble, []string{"4", "5"})
	})

	cvey.Convey("create graph", t, func() {
		g := toposort{next: make(map[string][]string)}
		g.addVertex("0")
		g.addVertex("1")
		g.addVertex("2")

		// can be in any order
		_, err := g.sort()
		cvey.So(err, cvey.ShouldBeNil)
	})

	cvey.Convey("create graph", t, func() {
		g := toposort{next: make(map[string][]string)}
		g.addVertex("0")
		g.addVertex("1")
		g.addVertex("2")
		g.addEdge("0", "1")
		g.addEdge("1", "2")
		g.addEdge("2", "0")

		// has a cycle
		_, err := g.sort()
		cvey.So(err, cvey.ShouldNotBeNil)
	})
}

func Test_convertRowsIntoBatch(t *testing.T) {
	colMysqlTyps := []defines.MysqlType{
		defines.MYSQL_TYPE_VAR_STRING,
		defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_FLOAT,
		defines.MYSQL_TYPE_DATE,
		defines.MYSQL_TYPE_TIME,
		defines.MYSQL_TYPE_DATETIME,
		defines.MYSQL_TYPE_TIMESTAMP,
	}
	colNames := make([]string, len(colMysqlTyps))
	mrs := &MysqlResultSet{}
	cnt := 5
	for colIdx, mysqlTyp := range colMysqlTyps {
		col := new(MysqlColumn)
		col.SetColumnType(mysqlTyp)
		col.SetName(colNames[colIdx])
		mrs.AddColumn(col)
	}

	for i := 0; i < cnt; i++ {
		row := make([]any, len(mrs.Columns))
		mrs.AddRow(row)
		for j := 0; j < len(mrs.Columns); j++ {
			switch mrs.Columns[j].ColumnType() {
			case defines.MYSQL_TYPE_VARCHAR:
				row[j] = "def"
			case defines.MYSQL_TYPE_VAR_STRING:
				row[j] = "abc"
			case defines.MYSQL_TYPE_SHORT:
				row[j] = int32(math.MaxInt16)
			case defines.MYSQL_TYPE_LONG:
				row[j] = int32(math.MaxInt32)
			case defines.MYSQL_TYPE_LONGLONG:
				row[j] = int64(math.MaxInt64)
			case defines.MYSQL_TYPE_DOUBLE:
				row[j] = float64(math.MaxFloat64)
			case defines.MYSQL_TYPE_FLOAT:
				row[j] = float32(math.MaxFloat32)
			case defines.MYSQL_TYPE_DATE:
				row[j] = types.Date(0)
			case defines.MYSQL_TYPE_TIME:
				row[j] = types.Time(0)
			case defines.MYSQL_TYPE_DATETIME:
				row[j] = types.Datetime(0)
			case defines.MYSQL_TYPE_TIMESTAMP:
				row[j] = types.Timestamp(0)
			case defines.MYSQL_TYPE_ENUM:
				row[j] = types.Enum(1)
			default:
				assert.True(t, false)
			}
		}
	}

	pool, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	data, pColDefs, err := convertRowsIntoBatch(pool, mrs.Columns, mrs.Data)
	assert.NoError(t, err)
	assert.Equal(t, len(mrs.Columns), len(pColDefs.ResultCols))
	assert.NotNil(t, data)
	assert.Equal(t, len(data.Vecs), len(mrs.Columns))

	ses := &Session{}
	ses.SetTimeZone(time.UTC)
	for i := 0; i < cnt; i++ {
		row := make([]any, len(data.Vecs))
		for j := 0; j < len(data.Vecs); j++ {
			err = extractRowFromVector(context.TODO(), ses, data.Vecs[j], j, row, i)
			assert.NoError(t, err)
			switch data.Vecs[j].GetType().Oid {
			case types.T_varchar:
				row[j] = string(row[j].([]uint8))
			case types.T_date:
				assert.Equal(t, mrs.Data[i][j].(types.Date), row[j])
				continue
			case types.T_time:
				assert.Equal(t, mrs.Data[i][j].(types.Time).String(), row[j])
				continue
			case types.T_datetime:
				assert.Equal(t, mrs.Data[i][j].(types.Datetime).String(), row[j])
				continue
			case types.T_timestamp:
				assert.Equal(t, mrs.Data[i][j].(types.Timestamp).String2(time.UTC, 0), row[j])
				continue
			case types.T_enum:
				assert.Equal(t, mrs.Data[i][j].(types.Enum), row[j])
				continue
			}
			assert.Equal(t, mrs.Data[i][j], row[j])
		}

	}
}

func Test_issue3482(t *testing.T) {
	s := issue3482SqlPrefix + " "
	ui := UserInput{sql: s}
	assert.True(t, ui.isIssue3482Sql())
}

func Test_xxx(t *testing.T) {
	list.New()
}

func Test_isLegal(t *testing.T) {
	type args struct {
		name string
	}
	trueNames := []string{
		"abc",
		"0b",
		"0b0a1fg",
		"123",
		"b'00011011'",
		"b\\\\a9''", //b\\a9''
		"\\0",       //\0
		"\\\\'",     //\\'
		"\\Z",       //\Z
		"/000/",
	}

	type kase struct {
		name string
		args args
		want bool
	}

	tests := []kase{}
	for i, name := range trueNames {
		tests = append(tests, kase{
			name: fmt.Sprintf("t%d", i),
			args: args{
				name: name,
			},
			want: true,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, accountNameIsLegal(tt.args.name), "accountNameIsLegal(%v)", tt.args.name)
			assert.Equalf(t, tt.want, dbNameIsLegal(tt.args.name), "dbNameIsLegal(%v)", tt.args.name)
			assert.Equalf(t, tt.want, tableNameIsLegal(tt.args.name), "tableNameIsLegal(%v)", tt.args.name)
		})
	}
}

func Test_parser(t *testing.T) {
	sql := "select db_name, table_name, constraint_name, column_name, refer_column_name, on_delete, on_update from `mo_catalog`.`mo_foreign_keys` where refer_db_name = `test` and refer_table_name = `b'00011011'`  and (db_name != `test` or db_name = `test` and table_name != `b'00011011'`) order by db_name, table_name, constraint_name;"
	x, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	assert.NoError(t, err)
	fmt.Println(x)

}

func Test_replaceStr(t *testing.T) {
	type args struct {
		s     string
		start int
		end   int
		s2    string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "t1",
			args: args{
				s:     "mysql://root:111@127.0.0.1:6001",
				start: 13,
				end:   16,
				s2:    "******",
			},
			want: "mysql://root:******@127.0.0.1:6001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, replaceStr(tt.args.s, tt.args.start, tt.args.end, tt.args.s2), "replaceStr(%v, %v, %v, %v)", tt.args.s, tt.args.start, tt.args.end, tt.args.s2)
		})
	}
}

func Test_islegal(t *testing.T) {
	assert.False(t, isLegal("", []string{}))
	assert.False(t, isLegal("abc", []string{}))
}

func Test_accountNameIsLegal(t *testing.T) {
	assert.False(t, accountNameIsLegal(",."))
	assert.False(t, dbNameIsLegal(",."))
	assert.False(t, tableNameIsLegal(",."))
}

func Test_compUriInfo(t *testing.T) {
	ret, _ := compositedUriInfo("", "prefix")
	assert.False(t, ret)

	ret, _ = compositedUriInfo("prefix", "prefix")
	assert.False(t, ret)

	ret, _ = compositedUriInfo("prefixroot@3", "prefix")
	assert.False(t, ret)

	ret, _ = compositedUriInfo("prefixroot:111@3", "prefix")
	assert.False(t, ret)

	ret, _ = compositedUriInfo("prefixroot:111@3:65536", "prefix")
	assert.False(t, ret)

	ret, _ = compositedUriInfo("prefixroot:111@3:4", "prefix")
	assert.True(t, ret)
}

func Test_replaceStr2(t *testing.T) {
	assert.Equal(t, replaceStr("", 1, 0, "a"), "")
	assert.Equal(t, replaceStr("abc", 0, 4, "a"), "abc")
}

func Test_uriHasPrefix(t *testing.T) {
	assert.False(t, uriHasPrefix("ab", "abc"))
}

func Test_extractUriInfo(t *testing.T) {
	_, _, err := extractUriInfo(context.Background(), "abc", "t")
	assert.Error(t, err)
}

func Test_BuildTableDefFromMoColumns(t *testing.T) {
	convey.Convey("BuildTableDefFromMoColumns fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

		rm, err := NewRoutineManager(ctx)
		assert.Nil(t, err)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()

		// process.
		ses.proc = testutil.NewProc()
		ses.proc.Base.TxnOperator = txnOperator

		sql, err := getTableColumnDefSql(uint64(tenant.TenantID), "db1", "t1")
		assert.Nil(t, err)

		mrs := newMrsForPasswordOfUser([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		_, err = buildTableDefFromMoColumns(ctx, uint64(tenant.TenantID), "db1", "t1", ses)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_getTableColumnDefSql(t *testing.T) {
	tests := []struct {
		name      string
		accountId uint64
		dbName    string
		tableName string
		want      string
		wantErr   bool
	}{
		{
			name:      "1",
			accountId: 1,
			dbName:    "db1",
			tableName: "tbl1",
			want:      fmt.Sprintf(getTableColumnDefFormat, 1, "db1", "tbl1"),
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getTableColumnDefSql(tt.accountId, tt.dbName, tt.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTableColumnDefSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getTableColumnDefSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractTableDefColumns(t *testing.T) {

	// name type error
	convey.Convey("extractTableDefColumns fail", t, func() {

		var newTestExecResult = func() ExecResult {
			typ := new(types.Type)
			typByte, _ := typ.Marshal()

			def := new(plan2.Default)

			result := newMrsForPasswordOfUser([][]interface{}{{
				def,
				typByte,
				uint64(1),
				uint8(1),
				def.String(),
				uint8(0),
				uint8(0),
				uint8(0),
			}})
			return result
		}

		ctx := context.Background()
		er := newTestExecResult()
		_, err := extractTableDefColumns([]ExecResult{er}, ctx, "test", "test")
		assert.NotNil(t, err)
	})

	// typ error
	convey.Convey("extractTableDefColumns fail", t, func() {

		var newTestExecResult = func() ExecResult {

			def := new(plan2.Default)

			result := newMrsForPasswordOfUser([][]interface{}{{
				"id",
				def,
				uint64(1),
				uint8(1),
				def.String(),
				uint8(0),
				uint8(0),
				uint8(0),
			}})
			return result
		}

		ctx := context.Background()
		er := newTestExecResult()
		_, err := extractTableDefColumns([]ExecResult{er}, ctx, "test", "test")
		assert.NotNil(t, err)
	})

	// colId error
	convey.Convey("extractTableDefColumns fail", t, func() {

		var newTestExecResult = func() ExecResult {
			typ := new(types.Type)
			typByte, _ := typ.Marshal()

			def := new(plan2.Default)

			result := newMrsForPasswordOfUser([][]interface{}{{
				"id",
				typByte,
				def,
				uint8(1),
				def.String(),
				uint8(0),
				uint8(0),
				uint8(0),
			}})
			return result
		}

		ctx := context.Background()
		er := newTestExecResult()
		_, err := extractTableDefColumns([]ExecResult{er}, ctx, "test", "test")
		assert.NotNil(t, err)
	})

	// default error
	convey.Convey("extractTableDefColumns fail", t, func() {

		var newTestExecResult = func() ExecResult {
			typ := new(types.Type)
			typByte, _ := typ.Marshal()

			def := new(plan2.Default)

			result := newMrsForPasswordOfUser([][]interface{}{{
				"id",
				typByte,
				uint64(1),
				uint8(1),
				def.String(),
				uint8(0),
				uint8(0),
				uint8(0),
			}})
			return result
		}

		ctx := context.Background()
		er := newTestExecResult()
		_, err := extractTableDefColumns([]ExecResult{er}, ctx, "test", "test")
		assert.NotNil(t, err)
	})
}
