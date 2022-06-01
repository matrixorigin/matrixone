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

var NotboolVec = []bool{true, false, false}

func Test_NotCol(t *testing.T) {
	convey.Convey("Test not col operator succ", t, func() {
		InitNotFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 1)
		vec[0] = &vector.Vector{
			Col: NotboolVec,
			Nsp: &nulls.Nulls{},
		}
		nulls.Add(vec[0].Nsp, 2)
		ret, err := Not(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		for i := 0; i < len(data); i++ {
			convey.So(data[i], convey.ShouldEqual, !NotboolVec[i])
		}
		convey.So(nulls.Contains(ret.Nsp, 2), convey.ShouldEqual, true)
	})
}

func Test_NotConst(t *testing.T) {
	convey.Convey("Test not const operator succ", t, func() {
		InitNotFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 1)
		vec[0] = InitConstVector()
		vec[0].Col = []bool{true}
		ret, err := Not(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(len(data), convey.ShouldEqual, 1)
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data[0], convey.ShouldEqual, false)

		vec[0].Col = []bool{false}
		ret, err = Not(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok = ret.Col.([]bool)
		if !ok {
			log.Fatal(errors.New("the return vec type is not right"))
		}
		convey.So(len(data), convey.ShouldEqual, 1)
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(data[0], convey.ShouldEqual, true)
	})
}

func Test_NotNull(t *testing.T) {
	convey.Convey("Test not null operator succ", t, func() {
		InitNotFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))
		vec := make([]*vector.Vector, 1)
		vec[0] = InitConstVector()
		nulls.Add(vec[0].Nsp, 0)
		ret, err := Not(vec, proc)
		if err != nil {
			log.Fatal(err)
		}
		convey.So(ret.IsConst, convey.ShouldBeTrue)
		convey.So(ret.Col, convey.ShouldBeNil)
		convey.So(nulls.Contains(ret.Nsp, 0), convey.ShouldEqual, true)
	})
}
