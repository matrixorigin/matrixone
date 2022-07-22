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
	"errors"
	"log"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

func Test_FloorUint64(t *testing.T) {
	convey.Convey("Test floor for uint64 succ", t, func() {
		var uint64VecBase = []uint64{1, 4, 8, 16, 32, math.MaxUint64, 0}
		var nsp1 = []uint64{6}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeUint64Vector(uint64VecBase, nsp1)
		vec, err := FloorUInt64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]uint64)
		if !ok {
			log.Fatal(errors.New("the AbsUint64 function return value type is not []uint64"))
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
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
			}
		}

	})
}

func Test_FloorInt64(t *testing.T) {
	convey.Convey("Test floor for int64 succ", t, func() {
		var int64VecBase = []int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0}
		var nsp1 = []uint64{12}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeInt64Vector(int64VecBase, nsp1)
		vec, err := FloorInt64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]int64)
		if !ok {
			log.Fatal(errors.New("the AbsUint64 function return value type is not []int64"))
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
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
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
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		vec, err := FloorFloat64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]float64)
		if !ok {
			log.Fatal(errors.New("the AbsUint64 function return value type is not []int64"))
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
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeTrue)
					j++
				} else {
					convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
				}
			} else {
				convey.So(vec.Nsp.Np.Contains(uint64(i)), convey.ShouldBeFalse)
			}
		}
	})
}

func Test_FloorFloat64AndInt64(t *testing.T) {
	convey.Convey("Test floor for float64 and int64 succ", t, func() {
		var float64VecBase = []float64{-1.234567, 0, 1.2345678, 0}
		var int64VecBase = []int64{-4, -3, -2, -1, 0, 1, 2, 3, 4, 5}
		var nsp1 = []uint64{3}
		var origVecs = make([]*vector.Vector, 2)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		var compVecVec = [][]float64{{-10000, 0, 0, 0}, {-1000, 0, 0, 0}, {-100, 0, 0, 0}, {-10, 0, 0, 0}, {-2, 0, 1, 0},
			{-1.3, 0, 1.2, 0}, {-1.24, 0, 1.23, 0}, {-1.235, 0, 1.234, 0}, {-1.2346, 0, 1.2345, 0}, {-1.23457, 0, 1.23456, 0}}

		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		for i := 0; i < len(int64VecBase); i++ {
			origVecs[1] = testutil.MakeScalarInt64(int64VecBase[i], 1)

			vec, err := FloorFloat64Int64(origVecs, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := vec.Col.([]float64)
			if !ok {
				log.Fatal(errors.New("the AbsUint64 function return value type is not []int64"))
			}
			compVec := compVecVec[i]
			for j := 0; j < len(compVec); j++ {
				convey.So(data[j], convey.ShouldEqual, compVec[j])
			}
		}
	})
}
