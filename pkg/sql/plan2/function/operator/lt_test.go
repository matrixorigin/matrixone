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

var LtintBool = []bool{false, true, true, false, false, false, true, false, false, false, false, false, false, false, false, false}
var LtintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var LtintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var LtuintBool = []bool{false, true, false, false, false, false, false, false, false}
var LtuintNullPos = []int{2, 5, 6, 7, 8}
var LtuintNotNullPos = []int{0, 1, 3, 4}

var LtboolBool = []bool{false, false, false, true, false, false, false, false, false}

var LtstringBool = []bool{false, true, true, false, false, false, true, false, false, false, false, false, false, false, false, false}

type testLtFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testLtFuncVec = []testLtFunc{
	LtGeneral[int8], LtGeneral[int16], LtGeneral[int32], LtGeneral[int64],
	LtGeneral[uint8], LtGeneral[uint16], LtGeneral[uint32], LtGeneral[uint64],
	LtGeneral[float32], LtGeneral[float64], LtGeneral[types.Date],
	LtGeneral[types.Datetime], LtGeneral[types.Decimal64], LtBool, LtString,
}

var LtretVec = [][]bool{
	LtintBool, LtintBool, LtintBool, LtintBool, LtuintBool, LtuintBool, LtuintBool, LtuintBool,
	LtintBool, LtintBool, LtintBool, LtintBool, LtintBool, LtboolBool, LtstringBool,
}

func Test_ColLtCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testLtFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, LtretVec[i][j])
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
