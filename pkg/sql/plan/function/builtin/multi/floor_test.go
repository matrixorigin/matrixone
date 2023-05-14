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
	"log"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func Test_FloorUint64(t *testing.T) {
	convey.Convey("Test floor for uint64 succ", t, func() {
		var uint64VecBase = []uint64{1, 4, 8, 16, 32, math.MaxUint64, 0}
		var nsp1 = []uint64{6}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeUint64Vector(uint64VecBase, nsp1)
		vec, err := FloorUInt64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.MustFixedCol[uint64](vec)
		ok := (data != nil)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []uint64"))
		}
		compVec := []uint64{1, 4, 8, 16, 32, math.MaxUint64, 0}
		compNsp := []int64{6}

		for i := 0; i < len(compVec); i++ {
			convey.So(data[i], convey.ShouldEqual, compVec[i])
		}
		j := 0
		for i := 0; i < len(compVec); i++ {
			if j < len(compNsp) {
				if compNsp[j] == int64(i) {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
			}
		}

	})
}

func Test_FloorInt64(t *testing.T) {
	convey.Convey("Test floor for int64 succ", t, func() {
		var int64VecBase = []int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0}
		var nsp1 = []uint64{12}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeInt64Vector(int64VecBase, nsp1)
		vec, err := FloorInt64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.MustFixedCol[int64](vec)
		ok := (data != nil)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []int64"))
		}
		compVec := []int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0}
		compNsp := []int64{12}

		for i := 0; i < len(compVec); i++ {
			convey.So(data[i], convey.ShouldEqual, compVec[i])
		}
		j := 0
		for i := 0; i < len(compVec); i++ {
			if j < len(compNsp) {
				if compNsp[j] == int64(i) {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
			}
		}
	})
}

func Test_FloorFloat64(t *testing.T) {
	convey.Convey("Test floor for float64 succ", t, func() {
		var float64VecBase = []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
			1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
		var nsp1 = []uint64{uint64(len(float64VecBase) - 1)}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		vec, err := FloorFloat64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.MustFixedCol[float64](vec)
		ok := (data != nil)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []int64"))
		}
		compVec := []float64{0, -2, -3, math.MinInt64 + 1, math.MinInt64 + 2, -101, -2, 0,
			0, 1, 4, 8, 16, 32, 64, math.MaxInt64, math.MaxFloat64, 0}
		compNsp := []int64{int64(len(float64VecBase) - 1)}

		for i := 0; i < len(compVec); i++ {
			convey.So(data[i], convey.ShouldEqual, compVec[i])
		}
		j := 0
		for i := 0; i < len(compVec); i++ {
			if j < len(compNsp) {
				if compNsp[j] == int64(i) {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.GetNulls().Contains(uint64(i)), convey.ShouldBeFalse)
			}
		}
	})
}
