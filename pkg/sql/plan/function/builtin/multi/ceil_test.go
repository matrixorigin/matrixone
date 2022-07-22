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

func Test_CeilUint64(t *testing.T) {
	convey.Convey("Test ceil for uint64 succ", t, func() {
		var uint64VecBase = []uint64{1, 4, 8, 16, 32, math.MaxUint64, 0}
		var nsp1 = []uint64{6}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeUint64Vector(uint64VecBase, nsp1)
		vec, err := CeilUint64(origVecs, proc)
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

func Test_CeilInt64(t *testing.T) {
	convey.Convey("Test ceil for int64 succ", t, func() {
		var int64VecBase = []int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0}
		var nsp1 = []uint64{12}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeInt64Vector(int64VecBase, nsp1)
		vec, err := CeilInt64(origVecs, proc)
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

func Test_CeilFloat64(t *testing.T) {
	convey.Convey("Test ceil for float64 succ", t, func() {
		var float64VecBase = []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
			1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
		var nsp1 = []uint64{uint64(len(float64VecBase) - 1)}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeFloat64Vector(float64VecBase, nsp1)
		vec, err := CeilFloat64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]float64)
		if !ok {
			log.Fatal(errors.New("the AbsUint64 function return value type is not []int64"))
		}
		compVec := []float64{1, -1, -2, math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 1,
			0, 2, 5, 9, 17, 33, 65, math.MaxInt64, math.MaxFloat64, 0}
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
