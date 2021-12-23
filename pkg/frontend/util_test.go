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
	cvey "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_PathExists(t *testing.T) {
	cases := [...]struct {
		path string
		exist bool
		isfile bool
		noerr bool
	}{
		{"test/file",true,true,true},
		{"test/file-no",false,false,false},
		//{"test/dir",true,false,true},
		{"test/dir-no",false,false,false},
		{"testx",false,false,false},
	}

	for _,c := range cases{
		exist,isfile,err := PathExists(c.path)
		require.True(t, (err == nil) == c.noerr)
		require.True(t, exist == c.exist)
		require.True(t, isfile == c.isfile)
	}
}

func Test_closeFlag(t *testing.T) {
	cvey.Convey("closeFlag",t, func() {
		cf := &CloseFlag{}
		cf.setClosed(0)
		cvey.So(cf.IsOpened(),cvey.ShouldBeTrue)

		cf.Open()
		cvey.So(cf.IsOpened(),cvey.ShouldBeTrue)

		cf.Close()
		cvey.So(cf.IsClosed(),cvey.ShouldBeTrue)
	})
}

func Test_MinMax(t *testing.T) {
	cvey.Convey("min",t, func() {
		cvey.So(Min(10,9),cvey.ShouldEqual,9)
		cvey.So(Min(9,10),cvey.ShouldEqual,9)
	})

	cvey.Convey("minInt64",t, func() {
		cvey.So(MinInt64(10,9),cvey.ShouldEqual,9)
		cvey.So(MinInt64(9,10),cvey.ShouldEqual,9)
	})

	cvey.Convey("minUint64",t, func() {
		cvey.So(MinUint64(10,9),cvey.ShouldEqual,9)
		cvey.So(MinUint64(9,10),cvey.ShouldEqual,9)
	})

	cvey.Convey("max",t, func() {
		cvey.So(Max(10,9),cvey.ShouldEqual,10)
		cvey.So(Max(9,10),cvey.ShouldEqual,10)
	})

	cvey.Convey("maxInt64",t, func() {
		cvey.So(MaxInt64(10,9),cvey.ShouldEqual,10)
		cvey.So(MaxInt64(9,10),cvey.ShouldEqual,10)
	})

	cvey.Convey("maxUint64",t, func() {
		cvey.So(MaxUint64(10,9),cvey.ShouldEqual,10)
		cvey.So(MaxUint64(9,10),cvey.ShouldEqual,10)
	})
}

func Test_uint64list(t *testing.T) {
	cvey.Convey("uint64list",t, func() {
		var l Uint64List = make(Uint64List,3)
		cvey.So(l.Len(),cvey.ShouldEqual,3)
		cvey.So(l.Less(0,1),cvey.ShouldBeFalse)
		a,b := l[0],l[1]
		l.Swap(0,1)
		cvey.So(a == l[1] && b == l[0],cvey.ShouldBeTrue)
	})
}

func Test_routineid(t *testing.T) {
	cvey.Convey("rtid",t, func() {
		x := GetRoutineId()
		cvey.So(x,cvey.ShouldBeGreaterThanOrEqualTo,0)
	})
}

func Test_debugcounter(t *testing.T) {
	cvey.Convey("debugCounter",t, func() {
		dc := NewDebugCounter(3)
		dc.Set(0,1)
		cvey.So(dc.Get(0),cvey.ShouldEqual,1)
		dc.Add(0,1)
		cvey.So(dc.Get(0),cvey.ShouldEqual,2)
		cvey.So(dc.Len(),cvey.ShouldEqual,3)

		go dc.DCRoutine()

		time.Sleep(6 * time.Second)

		dc.Cf.Close()
	})
}

func Test_timeout(t *testing.T){
	cvey.Convey("timeout",t, func() {
		to := NewTimeout(5 * time.Second,true)
		to.UpdateTime(time.Now())
		cvey.So(to.isTimeout(),cvey.ShouldBeFalse)
	})
}

func Test_substringFromBegin(t *testing.T) {
	cvey.Convey("ssfb",t, func() {
		cvey.So(SubStringFromBegin("abcdef",3),cvey.ShouldEqual,"abc...")
	})
}

func Test_makedebuginfo(t *testing.T) {
	cvey.Convey("makedebuginfo",t, func() {
		MakeDebugInfo([]byte{0,1,2,3,4,5,6,7,8,9},
			6,3)
	})
}