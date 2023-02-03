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

package binary

import (
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func Test_EndsWith(t *testing.T) {
	convey.Convey("Test EndsWith succ", t, func() {
		var charVecBase = []string{"123-", "321", "123+", "8", ""}
		var charVecBase2 = []string{"-", "+", "1", ""}
		var nsp1, nsp2 []uint64
		var origVecs = make([]*vector.Vector, 2)
		var proc = testutil.NewProc()
		n1, n2 := len(charVecBase), len(charVecBase2)
		inputVec := make([]string, n1*n2)
		inputVec2 := make([]string, len(inputVec))
		for i := 0; i < len(inputVec); i++ {
			inputVec[i] = charVecBase[i/n2]
			inputVec2[i] = charVecBase2[i%n2]
			if (i / n2) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if (i % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(inputVec, nsp1)
		origVecs[1] = testutil.MakeCharVector(inputVec2, nsp2)
		vec, err := Endswith(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.([]uint8)
		if !ok {
			log.Fatal(moerr.NewInternalError(proc.Ctx, "the Endswith function return value type is not []uint8"))
		}
		compVec := []uint8{1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1}
		compNsp := []int64{3, 7, 11, 15, 16, 17, 18, 19}

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

func TestEndswith(t *testing.T) {
	convey.Convey("EndsWith scalar", t, func() {
		firVec := testutil.MakeScalarVarchar("abc", 5)
		secVec := testutil.MakeScalarVarchar("bc", 5)
		wantvec := testutil.MakeScalarUint8(1, 5)

		proc := testutil.NewProc()
		ovec, err := Endswith([]*vector.Vector{firVec, secVec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, ovec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
