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

var LeintBool = []bool{true, true, true, true, false, true, true, true, false, false, true, false, false, true, true, true}
var LeintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var LeintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var LeuintBool = []bool{true, true, true, false, true, false, true, true, true}
var LeuintNullPos = []int{2, 5, 6, 7, 8}
var LeuintNotNullPos = []int{0, 1, 3, 4}

var LeboolBool = []bool{true, false, false, true, true, true, true, true, true}

var LestringBool = []bool{true, true, true, false, false, true, true, false, false, false, true, false, true, true, true, true}

type testLeFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testLeFuncVec = []testLeFunc{
	LeDataValue[int8], LeDataValue[int16], LeDataValue[int32], LeDataValue[int64],
	LeDataValue[uint8], LeDataValue[uint16], LeDataValue[uint32], LeDataValue[uint64],
	LeDataValue[float32], LeDataValue[float64], LeDataValue[types.Date],
	LeDataValue[types.Datetime], LeDataValue[types.Decimal64], LeDataValue[bool], LeDataValue[string],
}

var LeretVec = [][]bool{
	LeintBool, LeintBool, LeintBool, LeintBool, LeuintBool, LeuintBool, LeuintBool, LeuintBool,
	LeintBool, LeintBool, LeintBool, LeintBool, LeintBool, LeboolBool, LestringBool,
}

func Test_ColLeCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		InitFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testLeFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, LeretVec[i][j])
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
