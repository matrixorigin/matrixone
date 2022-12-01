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
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func Test_AbsUint64(t *testing.T) {
	convey.Convey("Test abs for uint64 succ", t, func() {
		var uint64VecBase = []uint64{1, 0}
		var nsp1 = []uint64{2}
		var origVecs = make([]*vector.Vector, 1)
		var proc = testutil.NewProc()
		origVecs[0] = testutil.MakeUint64Vector(uint64VecBase, nsp1)
		vec, err := AbsUInt64(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]uint64)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the AbsUint64 function return value type is not []uint6"))
		}
		compVec := []uint64{1, 0}
		compNsp := []int64{2}

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
