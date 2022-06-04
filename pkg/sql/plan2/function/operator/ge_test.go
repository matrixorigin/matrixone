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

var GeintBool = []bool{true, false, false, false, true, true, false, true, true, true, true, true, true, true, false, true}
var GeintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var GeintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var GeuintBool = []bool{true, false, true, true, true, true, true, false, true}
var GeuintNullPos = []int{2, 5, 6, 7, 8}
var GeuintNotNullPos = []int{0, 1, 3, 4}

var GeboolBool = []bool{true, true, true, false, true, true, false, true, true}

var GestringBool = []bool{true, false, false, true, true, true, false, true, true, true, true, true, false, false, false, true}

type testGeFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testGeFuncVec = []testGeFunc{
	GeDataValue[int8], GeDataValue[int16], GeDataValue[int32], GeDataValue[int64],
	GeDataValue[uint8], GeDataValue[uint16], GeDataValue[uint32], GeDataValue[uint64],
	GeDataValue[float32], GeDataValue[float64], GeDataValue[types.Date],
	GeDataValue[types.Datetime], GeDataValue[types.Decimal64], GeDataValue[bool], GeDataValue[string],
}

var GeretVec = [][]bool{
	GeintBool, GeintBool, GeintBool, GeintBool, GeuintBool, GeuintBool, GeuintBool, GeuintBool,
	GeintBool, GeintBool, GeintBool, GeintBool, GeintBool, GeboolBool, GestringBool,
}

func Test_ColGeCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		InitFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testGeFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, GeretVec[i][j])
			}
			for j := 0; j < len(retNullPosVec[i]); j++ {
				convey.So(nulls.Contains(ret.Nsp, uint64(retNullPosVec[i][j])), convey.ShouldEqual, true)
			}
			for j := 0; j < len(retNotNullPosVec[i]); j++ {
				convey.So(nulls.Contains(ret.Nsp, uint64(retNotNullPosVec[i][j])), convey.ShouldEqual, false)
			}
		}

	})
}
