package unary

import (
	"errors"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

func Test_Ltrim(t *testing.T) {
	convey.Convey("Test Ltrim succ", t, func() {
		var charVecBase = []string{" 123", "  123", "123 ", " 8 ", " 8 a ", ""}
		var nsp1 = []uint64{5}
		var origVecs = make([]*vector.Vector, 1)
		var proc = process.New(mheap.New(&guest.Mmu{Mmu: host.New(100000), Limit: 100000}))
		origVecs[0] = testutil.MakeCharVector(charVecBase, nsp1)
		vec, err := Ltrim(origVecs, proc)
		if err != nil {
			log.Fatal(err)
		}
		data, ok := vec.Col.(*types.Bytes)
		if !ok {
			log.Fatal(errors.New("the Ltrim function return value type is not types.Bytes"))
		}
		compVec := []string{"123", "123", "123 ", "8 ", "8 a ", ""}
		compNsp := []int64{5}

		for i := 0; i < len(compVec); i++ {
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
