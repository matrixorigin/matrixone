package mergededup

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
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
	matchs []int64
	hashs  []uint64
	attrs  []string
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	groups map[uint64][]*hash.DedupGroup // hash code -> group list
}

type Argument struct {
	Attrs []string
	Ctr   Container
}
