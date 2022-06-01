package operator

import (
	"errors"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

func Test_ColXorCol(t *testing.T) {
	convey.Convey("Test col xor col operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		compVec := []bool{false, true, true, true, false, false, true, false, false}
		for i := 0; i < len(data); i++ {
			convey.So(data[i], convey.ShouldEqual, compVec[i])
		}
		NullPos := []int{2, 5, 6, 7, 8}
		NotNullPos := []int{0, 1, 3, 4}
		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}
		for i := 0; i < len(NotNullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NotNullPos[i])), convey.ShouldEqual, false)
		}
	})
}

func Test_ColXorConst(t *testing.T) {
	convey.Convey("Test col xor const operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		vec[1] = InitConstVector()
		vec[1].Col = []bool{true}
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		compVec := []bool{false, false, false, true, true, true, true, true, true}
		for i := 0; i < 9; i++ {
			convey.So(data[i], convey.ShouldEqual, compVec[i])
		}
		NullPos := []int{6, 7, 8}
		NotNullPos := []int{0, 1, 2, 3, 4, 5}
		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}
		for i := 0; i < len(NotNullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NotNullPos[i])), convey.ShouldEqual, false)
		}

		vec[1].Col = []bool{false}
		ret, err = Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok = ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		for i := 0; i < 9; i++ {
			convey.So(data[i], convey.ShouldEqual, !compVec[i])
		}

		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}
		for i := 0; i < len(NotNullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NotNullPos[i])), convey.ShouldEqual, false)
		}

	})
}

func Test_ColXorNull(t *testing.T) {
	convey.Convey("Test col xor null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		vec[1] = InitConstVector()
		nulls.Add(vec[1].Nsp, 0)
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		NullPos := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}

	})
}

func Test_ConstXorConst(t *testing.T) {
	convey.Convey("Test const xor const operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		vec[0].Col = []bool{true}
		vec[1].Col = []bool{true}
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data, convey.ShouldResemble, []bool{false})
		convey.So(ret.Nsp.Np, convey.ShouldBeNil)

		vec[1].Col = []bool{false}
		ret, err = Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok = ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data, convey.ShouldResemble, []bool{true})
		convey.So(ret.Nsp.Np, convey.ShouldBeNil)

		vec[0].Col = []bool{false}
		ret, err = Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok = ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data, convey.ShouldResemble, []bool{false})
		convey.So(ret.Nsp.Np, convey.ShouldBeNil)

	})
}

func Test_ConstXorNull(t *testing.T) {
	convey.Convey("Test const xor null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		vec[0].Col = []bool{true}
		nulls.Add(vec[1].Nsp, 0)
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(ret.Col, convey.ShouldBeNil)
		convey.So(nulls.Contains(ret.Nsp, 0), convey.ShouldEqual, true)
	})
}

func Test_NullXorNull(t *testing.T) {
	convey.Convey("Test null xor null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		nulls.Add(vec[0].Nsp, 0)
		nulls.Add(vec[1].Nsp, 0)
		ret, err := Xor(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(ret.Col, convey.ShouldBeNil)
		convey.So(nulls.Contains(ret.Nsp, 0), convey.ShouldEqual, true)
	})
}
