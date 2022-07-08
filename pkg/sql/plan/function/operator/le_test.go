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

var LeintBool = []bool{true, true, true, false, false, true, true, false, false, false, true, false, false, false, false, false}
var LeintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var LeintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var LeuintBool = []bool{true, true, false, false, true, false, false, false, false}
var LeuintNullPos = []int{2, 5, 6, 7, 8}
var LeuintNotNullPos = []int{0, 1, 3, 4}

var LeboolBool = []bool{true, false, false, true, true, false, false, false, false}

var LestringBool = []bool{true, true, true, false, false, true, true, false, false, false, true, false, false, false, false, false}

type testLeFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testLeFuncVec = []testLeFunc{
	LeGeneral[int8], LeGeneral[int16], LeGeneral[int32], LeGeneral[int64],
	LeGeneral[uint8], LeGeneral[uint16], LeGeneral[uint32], LeGeneral[uint64],
	LeGeneral[float32], LeGeneral[float64], LeGeneral[types.Date],
	LeGeneral[types.Datetime], LeGeneral[types.Decimal64], LeBool, LeString,
}

var LeretVec = [][]bool{
	LeintBool, LeintBool, LeintBool, LeintBool, LeuintBool, LeuintBool, LeuintBool, LeuintBool,
	LeintBool, LeintBool, LeintBool, LeintBool, LeintBool, LeboolBool, LestringBool,
}

func Test_ColLeCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
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
