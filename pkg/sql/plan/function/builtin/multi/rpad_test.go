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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

var strVecBase = []string{"a", "", ""}
var int64VecBase = []int64{-1, 0, 4, 8, 0}
var int64VecBase2 = []int64{-1, 16, 0}
var uint64VecBase = []uint64{0, 1, 4, 16, 0}
var uint64VecBase2 = []uint64{0, 16, 0}
var float64VecBase = []float64{-1.1, 0.01, 4.2, 8.4, 0}
var float64VecBase2 = []float64{0.01, 2.3, 0}

// var charVecBase = []string{"-1", "0", "4", "8", ""}
var charVecBase2 = []string{"a", "12", ""}
var compVec []string
var compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
var origVecs = make([]*vector.Vector, 3)
var proc = testutil.NewProc()

func Test_varchar1(t *testing.T) {
	convey.Convey("Test rpad varchar int64 and int64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(int64VecBase), len(int64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]int64, len(strVec))
		inputVec2 := make([]int64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = int64VecBase[(i/n3)%n2]
			inputVec2[i] = int64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}

		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeInt64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeInt64Vector(inputVec2, nsp3)

		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a-1-", "a161", "", "a-1-1-1-", "a1616161",
			"", "", "", "", "", "", "", "", "", "", "-1-1", "1616", "", "-1-1-1-1", "16161616", "", "",
			"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data); i++ {
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

func Test_varchar2(t *testing.T) {
	convey.Convey("Test rpad varchar int64 and uint64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(int64VecBase), len(uint64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]int64, len(strVec))
		inputVec2 := make([]uint64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = int64VecBase[(i/n3)%n2]
			inputVec2[i] = uint64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeInt64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeUint64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a000", "a161", "", "a0000000", "a1616161", "", "", "", "", "", "", "", "", "",
			"", "0000", "1616", "", "00000000", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data); i++ {
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

func Test_varchar3(t *testing.T) {
	convey.Convey("Test rpad varchar int64 and float64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(int64VecBase), len(float64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]int64, len(strVec))
		inputVec2 := make([]float64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = int64VecBase[(i/n3)%n2]
			inputVec2[i] = float64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeInt64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeFloat64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a0.0", "a2.3", "", "a0.010.0", "a2.32.32", "", "", "", "", "", "", "", "", "", "",
			"0.01", "2.32", "", "0.010.01", "2.32.32.", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data); i++ {
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

func Test_varchar4(t *testing.T) {
	convey.Convey("Test rpad varchar int64 and T_varchar succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(int64VecBase), len(charVecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]int64, len(strVec))
		inputVec2 := make([]string, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = int64VecBase[(i/n3)%n2]
			inputVec2[i] = charVecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeInt64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeCharVector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "aaaa", "a121", "", "aaaaaaaa", "a1212121", "", "", "", "", "", "", "", "", "", "",
			"aaaa", "1212", "", "aaaaaaaa", "12121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data); i++ {
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

func Test_varchar5(t *testing.T) {
	convey.Convey("Test rpad varchar uint64 and int64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(uint64VecBase), len(int64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]uint64, len(strVec))
		inputVec2 := make([]int64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = uint64VecBase[(i/n3)%n2]
			inputVec2[i] = int64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeUint64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeInt64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "a", "a", "", "a-1-", "a161", "", "a-1-1-1-1-1-1-1-", "a161616161616161", "", "", "", "", "",
			"", "", "-", "1", "", "-1-1", "1616", "", "-1-1-1-1-1-1-1-1", "1616161616161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar6(t *testing.T) {
	convey.Convey("Test rpad varchar uint64 and uint64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(uint64VecBase), len(uint64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]uint64, len(strVec))
		inputVec2 := make([]uint64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = uint64VecBase[(i/n3)%n2]
			inputVec2[i] = uint64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeUint64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeUint64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "a", "a", "", "a000", "a161", "", "a000000000000000", "a161616161616161", "", "", "", "", "", "", "",
			"0", "1", "", "0000", "1616", "", "0000000000000000", "1616161616161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar7(t *testing.T) {
	convey.Convey("Test rpad varchar uint64 and float64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(uint64VecBase), len(float64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]uint64, len(strVec))
		inputVec2 := make([]float64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = uint64VecBase[(i/n3)%n2]
			inputVec2[i] = float64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeUint64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeFloat64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "a", "a", "", "a0.0", "a2.3", "", "a0.010.010.010.0", "a2.32.32.32.32.3", "", "", "", "", "",
			"", "", "0", "2", "", "0.01", "2.32", "", "0.010.010.010.01", "2.32.32.32.32.32", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar8(t *testing.T) {
	convey.Convey("Test rpad varchar uint64 and T_varchar succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(uint64VecBase), len(charVecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]uint64, len(strVec))
		inputVec2 := make([]string, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = uint64VecBase[(i/n3)%n2]
			inputVec2[i] = charVecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeUint64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeCharVector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "a", "a", "", "aaaa", "a121", "", "aaaaaaaaaaaaaaaa", "a121212121212121", "", "", "", "", "", "", "",
			"a", "1", "", "aaaa", "1212", "", "aaaaaaaaaaaaaaaa", "1212121212121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar9(t *testing.T) {
	convey.Convey("Test rpad varchar float64 and int64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(float64VecBase), len(int64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]float64, len(strVec))
		inputVec2 := make([]int64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = float64VecBase[(i/n3)%n2]
			inputVec2[i] = int64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeFloat64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeInt64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a-1-", "a161", "", "a-1-1-1-", "a1616161", "", "", "", "", "", "", "", "", "", "", "-1-1",
			"1616", "", "-1-1-1-1", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar10(t *testing.T) {
	convey.Convey("Test rpad varchar float64 and uint64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(float64VecBase), len(uint64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]float64, len(strVec))
		inputVec2 := make([]uint64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = float64VecBase[(i/n3)%n2]
			inputVec2[i] = uint64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeFloat64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeUint64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a000", "a161", "", "a0000000", "a1616161", "", "", "", "", "", "", "", "", "", "", "0000", "1616", "",
			"00000000", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar11(t *testing.T) {
	convey.Convey("Test rpad varchar float64 and float64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(float64VecBase), len(float64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]float64, len(strVec))
		inputVec2 := make([]float64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = float64VecBase[(i/n3)%n2]
			inputVec2[i] = float64VecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeFloat64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeFloat64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "a0.0", "a2.3", "", "a0.010.0", "a2.32.32", "", "", "", "", "", "", "", "", "", "",
			"0.01", "2.32", "", "0.010.01", "2.32.32.", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

func Test_varchar12(t *testing.T) {
	convey.Convey("Test rpad varchar float64 and T_varchar succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(float64VecBase), len(charVecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]float64, len(strVec))
		inputVec2 := make([]string, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = float64VecBase[(i/n3)%n2]
			inputVec2[i] = charVecBase2[i%n3]
			if (i / (n2 * n3)) == (n1 - 1) {
				nsp1 = append(nsp1, uint64(i))
			}
			if ((i / n3) % n2) == (n2 - 1) {
				nsp2 = append(nsp2, uint64(i))
			}
			if i%n3 == (n3 - 1) {
				nsp3 = append(nsp3, uint64(i))
			}
		}
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeFloat64Vector(inputVec, nsp2)
		origVecs[2] = testutil.MakeCharVector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data := vector.GetStrVectorValues(vec)
		compVec = []string{"", "", "", "", "", "", "aaaa", "a121", "", "aaaaaaaa", "a1212121", "", "", "", "", "", "", "", "", "", "",
			"aaaa", "1212", "", "aaaaaaaa", "12121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data); i++ {
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

// getBytes converts a string slice to a *types.Bytes
func getBytes(s ...string) []string {
	return s
}

func TestRpadInt(t *testing.T) {
	// test: no nulls and all args are attribute names
	isConst := []bool{false, false, false}
	sizes := []int16{3, 5, 6, 0, 1, 6, 9}
	padstrs := getBytes("", "", "", "111", "222", "333", "444")
	strs := getBytes("hello你好", "hello", "hello", "hello", "hello", "hello", "hello你好")
	expectedStrs := getBytes("hel", "hello", "", "", "h", "hello3", "hello你好44")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))
	expectedNsp := new(nulls.Nulls)

	actualStrs, actualNsp, _ := rpad(proc.Ctx, len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: no nulls and the 2nd and 3rd args are constant
	isConst = []bool{false, true, true}
	sizes = []int16{3}
	padstrs = getBytes("111")
	expectedStrs = getBytes("hel", "hel", "hel", "hel", "hel", "hel", "hel")
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(strs), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: nulls
	/**
	Here is a test table like :
	+------+-----+-------+
	|a     |b    |c      |
	+--------------------+
	|NULL  |2    |a      |
	|hello |NULL |a      |
	|hello |2    |NULL   |
	|hello |-2   |a      |
	+------+-----+-------+
	all results of rpad(a,b,c) should be NULLs
	*/
	isConst = []bool{false, false, false}
	strs = getBytes("", "hello", "hello", "hello")
	nulls.Add(oriNsps[0], uint64(0))
	nulls.Add(oriNsps[1], uint64(1))
	nulls.Add(oriNsps[2], uint64(2))
	sizes = []int16{2, 2, 2, -2}
	padstrs = getBytes("a", "a", "", "a")

	expectedStrs = getBytes("", "", "", "")
	expectedNsp = new(nulls.Nulls)
	for i := 0; i < len(strs); i++ {
		nulls.Add(expectedNsp, uint64(i)) // all strings are NULLs

	}
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestRpadFloat(t *testing.T) {
	// test float64
	isConst := []bool{false, false, false}
	sizes := []float64{0.0, 0.1, -0.1, 8, 8.4, 8.6}
	padstrs := getBytes("你好", "你好", "你好", "你好", "你好", "你好")
	strs := getBytes("hello", "hello", "hello", "hello", "hello", "hello")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))

	expectedStrs := getBytes("", "", "", "hello你好你", "hello你好你", "hello你好你好")
	expectedNsp := new(nulls.Nulls)
	actualStrs, actualNsp, _ := rpad(proc.Ctx, len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test float32
	sizes2 := []float32{0.0, 0.1, -0.1, 8, 8.4, 8.6}
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(sizes2), strs, sizes2, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestRpadUint(t *testing.T) {
	// test: no nulls and all args are attribute names
	isConst := []bool{false, false, false}
	sizes := []uint32{3, 5, 6, 0, 1, 6, 9}
	padstrs := getBytes("", "", "", "111", "222", "333", "444")
	strs := getBytes("hello你好", "hello", "hello", "hello", "hello", "hello", "hello你好")
	expectedStrs := getBytes("hel", "hello", "", "", "h", "hello3", "hello你好44")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))
	expectedNsp := new(nulls.Nulls)

	actualStrs, actualNsp, _ := rpad(proc.Ctx, len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: no nulls and the 2nd and 3rd args are constant
	isConst = []bool{false, true, true}
	sizes = []uint32{3}
	padstrs = getBytes("111")
	expectedStrs = getBytes("hel", "hel", "hel", "hel", "hel", "hel", "hel")
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(strs), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: nulls
	/**
	Here is a test table like :
	+------+-----+-------+
	|a     |b    |c      |
	+--------------------+
	|NULL  |2    |a      |
	|hello |NULL |a      |
	|hello |2    |NULL   |
	+------+-----+-------+
	all results of rpad(a,b,c) should be NULLs
	*/
	isConst = []bool{false, false, false}
	strs = getBytes("", "hello", "hello")
	nulls.Add(oriNsps[0], uint64(0))
	nulls.Add(oriNsps[1], uint64(1))
	nulls.Add(oriNsps[2], uint64(2))
	sizes = []uint32{2, 2, 2}
	padstrs = getBytes("a", "a", "")

	expectedStrs = getBytes("", "", "")
	expectedNsp = new(nulls.Nulls)
	for i := 0; i < len(strs); i++ {
		nulls.Add(expectedNsp, uint64(i)) // all strings are NULLs

	}
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestTypes(t *testing.T) {
	isConst := []bool{false, true, false}

	// test sizes with a non-numerical type
	sizes := getBytes("aaasdasdsada")
	padstrs := getBytes("a", "a")
	strs := getBytes("", "test")
	oriNsps := []*nulls.Nulls{new(nulls.Nulls), new(nulls.Nulls), new(nulls.Nulls)}
	// nulls.Add(oriNsps[0], 0) // the first str is NULL

	expectedNsp := new(nulls.Nulls)
	// nulls.Add(expectedNsp, 0)
	expectedStrs := getBytes("", "")

	actualStrs, actualNsp, _ := rpad(proc.Ctx, len(strs), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test padstrs with a numerical type
	isConst = []bool{false, false, false}
	sizes2 := []int64{-1, 2, 10}
	padstrs2 := []int32{8, 9, 10}
	strs2 := getBytes("test", "test", "test")
	oriNsps2 := []*nulls.Nulls{new(nulls.Nulls), new(nulls.Nulls), new(nulls.Nulls)}

	expectedNsp2 := new(nulls.Nulls)
	nulls.Add(expectedNsp2, 0)
	expectedStrs2 := getBytes("", "te", "test101010")
	actualStrs, actualNsp, _ = rpad(proc.Ctx, len(sizes2), strs2, sizes2, padstrs2, isConst, oriNsps2)
	require.Equal(t, expectedStrs2, actualStrs)
	require.Equal(t, expectedNsp2, actualNsp)
}
