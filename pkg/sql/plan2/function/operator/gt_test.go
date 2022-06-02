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

var GtintBool = []bool{false, false, false, false, true, false, false, false, true, true, false, true, true, false, false, false}
var GtintNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var GtintNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

var GtuintBool = []bool{false, false, false, true, false, true, false, false, false}
var GtuintNullPos = []int{2, 5, 6, 7, 8}
var GtuintNotNullPos = []int{0, 1, 3, 4}

var GtboolBool = []bool{false, true, true, false, false, false, false, false, false}

var GtstringBool = []bool{false, false, false, true, true, false, false, true, true, true, false, true, false, false, false, false}

type testGtFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testGtFuncVec = []testGtFunc{
	GtDataValue[int8], GtDataValue[int16], GtDataValue[int32], GtDataValue[int64],
	GtDataValue[uint8], GtDataValue[uint16], GtDataValue[uint32], GtDataValue[uint64],
	GtDataValue[float32], GtDataValue[float64], GtDataValue[types.Date],
	GtDataValue[types.Datetime], GtDataValue[types.Decimal64], GtDataValue[bool], GtDataValue[string],
}

var GtretVec = [][]bool{
	GtintBool, GtintBool, GtintBool, GtintBool, GtuintBool, GtuintBool, GtuintBool, GtuintBool,
	GtintBool, GtintBool, GtintBool, GtintBool, GtintBool, GtboolBool, GtstringBool,
}

func Test_ColGtCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		InitFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testGtFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, GtretVec[i][j])
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
