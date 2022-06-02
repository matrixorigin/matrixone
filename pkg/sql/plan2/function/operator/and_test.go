package operator

import (
	"errors"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

var boolVec = []bool{true, true, true, false, false, false, false, false, false}

func InitColVector() []*vector.Vector {
	InitFuncMap()
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: boolVec,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_bool
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: []bool{true, false, false, true, false, false, true, false, false},
		Nsp: &nulls.Nulls{},
	}
	vec[1].Typ.Oid = types.T_bool
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

func InitConstVector() *vector.Vector {
	InitFuncMap()
	vec := &vector.Vector{Nsp: &nulls.Nulls{}}
	vec.IsConst = true
	return vec
}

func Test_ColAndCol(t *testing.T) {
	convey.Convey("Test col and col operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(data[0], convey.ShouldEqual, true)
		for i := 1; i < len(data); i++ {
			convey.So(data[i], convey.ShouldEqual, false)
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

func Test_ColAndConst(t *testing.T) {
	convey.Convey("Test col and const operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		vec[1] = InitConstVector()
		vec[1].Col = []bool{true}
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		for i := 0; i < 9; i++ {
			convey.So(data[i], convey.ShouldEqual, boolVec[i])
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
		ret, err = And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok = ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		for i := 0; i < 9; i++ {
			convey.So(data[i], convey.ShouldEqual, false)
		}

		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}
		for i := 0; i < len(NotNullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NotNullPos[i])), convey.ShouldEqual, false)
		}

	})
}

func Test_ColAndNull(t *testing.T) {
	convey.Convey("Test col and null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := InitColVector()
		vec[1] = InitConstVector()
		nulls.Add(vec[1].Nsp, 0)
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		NullPos := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		for i := 0; i < len(NullPos); i++ {
			convey.So(nulls.Contains(ret.Nsp, uint64(NullPos[i])), convey.ShouldEqual, true)
		}

	})
}

func Test_ConstAndConst(t *testing.T) {
	convey.Convey("Test const and const operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		vec[0].Col = []bool{true}
		vec[1].Col = []bool{true}
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data, convey.ShouldResemble, []bool{true})
		convey.So(ret.Nsp.Np, convey.ShouldBeNil)

		vec[1].Col = []bool{false}
		ret, err = And(vec, proc)
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

		vec[0].Col = []bool{false}
		ret, err = And(vec, proc)
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

func Test_ConstAndNull(t *testing.T) {
	convey.Convey("Test const and null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		vec[0].Col = []bool{true}
		nulls.Add(vec[1].Nsp, 0)
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(ret.Col, convey.ShouldBeNil)
		convey.So(nulls.Contains(ret.Nsp, 0), convey.ShouldEqual, true)
	})
}

func Test_NullAndNull(t *testing.T) {
	convey.Convey("Test null and null operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 2)
		vec[0] = InitConstVector()
		vec[1] = InitConstVector()
		nulls.Add(vec[0].Nsp, 0)
		nulls.Add(vec[1].Nsp, 0)
		ret, err := And(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(ret.Col, convey.ShouldBeNil)
		convey.So(nulls.Contains(ret.Nsp, 0), convey.ShouldEqual, true)
	})
}
