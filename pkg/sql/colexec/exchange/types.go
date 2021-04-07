package exchange

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/process"
)

const (
	UnitLimit = 1024
)

var (
	ZeroBools  []bool
	OneUint64s []uint64
)

type Container struct {
	diffs  []bool
	sizes  []int64 // size of every batch
	matchs []int64
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bats   []*batch.Batch
}

type Argument struct {
	Attrs []string
	Ctr   Container
	Ms    []*guest.Mmu
	Ws    []*process.WaitRegister
}
