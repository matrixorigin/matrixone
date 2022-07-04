package multi

import (
	"errors"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

var strVecBase = []string{"a", "", ""}
var int64VecBase = []int64{-1, 0, 4, 8, 0}
var int64VecBase2 = []int64{-1, 16, 0}
var uint64VecBase = []uint64{0, 1, 4, 16, 0}
var uint64VecBase2 = []uint64{0, 16, 0}
var float64VecBase = []float64{-1.1, 0.01, 4.2, 8.4, 0}
var float64VecBase2 = []float64{0.01, 2.3, 0}
var charVecBase = []string{"-1", "0", "4", "8", ""}
var charVecBase2 = []string{"a", "12", ""}
var compVec []string
var compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
var origVecs = make([]*vector.Vector, 3)
var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))

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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a-1-", "a161", "", "a-1-1-1-", "a1616161",
			"", "", "", "", "", "", "", "", "", "", "-1-1", "1616", "", "-1-1-1-1", "16161616", "", "",
			"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a000", "a161", "", "a0000000", "a1616161", "", "", "", "", "", "", "", "", "",
			"", "0000", "1616", "", "00000000", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a0.0", "a2.3", "", "a0.010.0", "a2.32.32", "", "", "", "", "", "", "", "", "", "",
			"0.01", "2.32", "", "0.010.01", "2.32.32.", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "aaaa", "a121", "", "aaaaaaaa", "a1212121", "", "", "", "", "", "", "", "", "", "",
			"aaaa", "1212", "", "aaaaaaaa", "12121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "a", "a", "", "a-1-", "a161", "", "a-1-1-1-1-1-1-1-", "a161616161616161", "", "", "", "", "",
			"", "", "-", "1", "", "-1-1", "1616", "", "-1-1-1-1-1-1-1-1", "1616161616161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "a", "a", "", "a000", "a161", "", "a000000000000000", "a161616161616161", "", "", "", "", "", "", "",
			"0", "1", "", "0000", "1616", "", "0000000000000000", "1616161616161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "a", "a", "", "a0.0", "a2.3", "", "a0.010.010.010.0", "a2.32.32.32.32.3", "", "", "", "", "",
			"", "", "0", "2", "", "0.01", "2.32", "", "0.010.010.010.01", "2.32.32.32.32.32", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "a", "a", "", "aaaa", "a121", "", "aaaaaaaaaaaaaaaa", "a121212121212121", "", "", "", "", "", "", "",
			"a", "1", "", "aaaa", "1212", "", "aaaaaaaaaaaaaaaa", "1212121212121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{2, 5, 8, 11, 12, 13, 14, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a-1-", "a161", "", "a-1-1-1-", "a1616161", "", "", "", "", "", "", "", "", "", "", "-1-1",
			"1616", "", "-1-1-1-1", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a000", "a161", "", "a0000000", "a1616161", "", "", "", "", "", "", "", "", "", "", "0000", "1616", "",
			"00000000", "16161616", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a0.0", "a2.3", "", "a0.010.0", "a2.32.32", "", "", "", "", "", "", "", "", "", "",
			"0.01", "2.32", "", "0.010.01", "2.32.32.", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "aaaa", "a121", "", "aaaaaaaa", "a1212121", "", "", "", "", "", "", "", "", "", "",
			"aaaa", "1212", "", "aaaaaaaa", "12121212", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}
		compNsp = []int64{0, 1, 2, 5, 8, 11, 12, 13, 14, 15, 16, 17, 20, 23, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			convey.So(str, convey.ShouldEqual, compVec[i])
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

func Test_varchar13(t *testing.T) {
	convey.Convey("Test rpad varchar T_varchar and int64 succ", t, func() {
		n1, n2, n3 := len(strVecBase), len(charVecBase), len(int64VecBase2)
		strVec := make([]string, n1*n2*n3)
		inputVec := make([]string, len(strVec))
		inputVec2 := make([]int64, len(strVec))

		nsp1 := make([]uint64, 0)
		nsp2 := make([]uint64, 0)
		nsp3 := make([]uint64, 0)
		for i := 0; i < len(strVec); i++ {
			strVec[i] = strVecBase[i/(n2*n3)]
			inputVec[i] = charVecBase[(i/n3)%n2]
			inputVec2[i] = int64VecBase2[i%n3]
		}
		// fmt.Printf("strVec: %v\n", strVec)
		// fmt.Printf("inputVec: %v\n", inputVec)
		// fmt.Printf("inputVec2: %v\n", inputVec2)
		origVecs[0] = testutil.MakeCharVector(strVec, nsp1)
		origVecs[1] = testutil.MakeCharVector(inputVec, nsp2)
		origVecs[2] = testutil.MakeInt64Vector(inputVec2, nsp3)
		vec, err := Rpad(origVecs, proc)
		// fmt.Println(vec.Col)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Rpad function return value type is not types.Bytes"))
		}
		compVec = []string{"", "", "", "", "", "", "a-1-", "a161", "a000", "a-1-1-1-",
			"a1616161", "a0000000", "", "", "", "", "", "", "", "",
			"", "-1-1", "1616", "0000", "-1-1-1-1", "16161616", "00000000", "", "", "",
			"", "", "", "", "", "", "-1-1", "1616", "0000", "-1-1-1-1",
			"16161616", "00000000", "", "", ""}
		compNsp = []int64{0, 1, 2, 15, 16, 17, 30, 31, 32}
		for i := 0; i < len(data.Lengths); i++ {
			str := string(data.Data[data.Offsets[i] : data.Offsets[i]+data.Lengths[i]])
			// fmt.Println("abc: ", i)
			convey.So(str, convey.ShouldEqual, compVec[i])
		}
		j := 0
		for i := 0; i < len(compVec); i++ {
			// fmt.Println(i)
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
