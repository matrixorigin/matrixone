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

var NeintBool = []bool{false, true, true, false, true, false, true, false, true, true, false, false, false, false, false, false}
var NeintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var NeintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var NeuintBool = []bool{false, true, false, true, false, false, false, false, false}
var NeuintNullPos = []int{2, 5, 6, 7, 8}
var NeuintNotNullPos = []int{0, 1, 3, 4}

var NeboolBool = []bool{false, true, false, true, false, false, false, false, false}

var NestringBool = []bool{false, true, true, false, true, false, true, false, true, true, false, false, false, false, false, false}

type testNeFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testNeFuncVec = []testNeFunc{
	NeGeneral[int8], NeGeneral[int16], NeGeneral[int32], NeGeneral[int64],
	NeGeneral[uint8], NeGeneral[uint16], NeGeneral[uint32], NeGeneral[uint64],
	NeGeneral[float32], NeGeneral[float64], NeGeneral[types.Date],
	NeGeneral[types.Datetime], NeGeneral[types.Decimal64], NeGeneral[bool], NeString,
}

var NeretVec = [][]bool{
	NeintBool, NeintBool, NeintBool, NeintBool, NeuintBool, NeuintBool, NeuintBool, NeuintBool,
	NeintBool, NeintBool, NeintBool, NeintBool, NeintBool, NeboolBool, NestringBool,
}

func Test_ColNeCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testNeFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, NeretVec[i][j])
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
