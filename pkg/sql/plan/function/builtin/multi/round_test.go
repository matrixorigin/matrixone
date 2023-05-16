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

func Test_RoundUint64(t *testing.T) {
	convey.Convey("Test round for uint64 succ", t, func() {
		var uint64VecBase = []uint64{1, 4, 8, 16, 32, math.MaxUint64, 0}
		var nsp1 = []uint64{6}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeUint64Vector(uint64VecBase, nsp1)
		vec, err := RoundUint64(origVecs, proc)
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

func Test_RoundInt64(t *testing.T) {
	convey.Convey("Test round for int64 succ", t, func() {
		var int64VecBase = []int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0}
		var nsp1 = []uint64{12}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeInt64Vector(int64VecBase, nsp1)
		vec, err := RoundInt64(origVecs, proc)
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

func Test_RoundFloat64(t *testing.T) {
	convey.Convey("Test round for float64 succ", t, func() {
		var float64VecBase = []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
			1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
		var nsp1 = []uint64{uint64(len(float64VecBase) - 1)}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		vec, err := RoundFloat64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.MustFixedCol[float64](vec)
		ok := (data != nil)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []int64"))
		}
		compVec := []float64{0, -1, -2, math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 1,
			0, 2, 4, 8, 16, 32, 64, math.MaxInt64, math.MaxFloat64, 0}
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

func Test_RoundFloat64AndInt64(t *testing.T) {
	convey.Convey("Test floor for float64 and int64 succ", t, func() {
		var float64VecBase = []float64{-1.234567, 0, 1.2345678, 0}
		var int64VecBase = []int64{-4, -3, -2, -1, 0, 1, 2, 3, 4, 5}
		var nsp1 = []uint64{3}
		var origVecs = make([]*vector.Vector, 2)
		var proc = testutil.NewProc()
		var compVecVec = [][]float64{{-0, 0, 0, 0}, {-0, 0, 0, 0}, {-0, 0, 0, 0}, {-0, 0, 0, 0}, {-1, 0, 1, 0},
			{-1.2, 0, 1.2, 0}, {-1.23, 0, 1.23, 0}, {-1.235, 0, 1.235, 0}, {-1.2346, 0, 1.2346, 0}, {-1.23457, 0, 1.23457, 0}}

		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		for i := 0; i < len(int64VecBase); i++ {
			origVecs[1] = testutil.MakeScalarInt64(int64VecBase[i], 1)

			vec, err := RoundFloat64(origVecs, proc)
			if err != nil {
				log.Fatal(err)
			}
			data := vector.MustFixedCol[float64](vec)
			ok := (data != nil)
			if !ok {
				log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []int64"))
			}
			compVec := compVecVec[i]
			for j := 0; j < len(compVec); j++ {
				convey.So(data[j], convey.ShouldEqual, compVec[j])
			}
		}
	})
}

func Test_Round2(t *testing.T) {
	convey.Convey("Test_RoundFloat64AndInt64_2", t, func() {
		type kase struct {
			n      float64
			format int64
			want   float64
		}

		kases := []kase{
			{
				n:      999999999.0,
				format: -9,
				want:   1000000000,
			},
			{
				n:      999999999.0,
				format: -8,
				want:   1000000000,
			},
			{
				n:      999999999999999999.0,
				format: -18,
				want:   1000000000000000000,
			},
			{
				n:      999999999999999999.0,
				format: -10,
				want:   1000000000000000000,
			},
			{
				n:      9999999.99999999999,
				format: 10,
				want:   10000000.0000000000,
			},
		}
		for _, k := range kases {
			srcVector := testutil.MakeScalarFloat64(k.n, 1)
			formatVector := testutil.MakeScalarInt64(k.format, 1)

			wantVec := testutil.MakeScalarFloat64(k.want, 1)
			proc := testutil.NewProc()
			res, err := RoundFloat64([]*vector.Vector{srcVector, formatVector}, proc)
			convey.So(err, convey.ShouldBeNil)
			compare := testutil.CompareVectors(wantVec, res)
			convey.So(compare, convey.ShouldBeTrue)

		}

	})

}
