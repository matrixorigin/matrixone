package dedup

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
)

const (
	UnitLimit = 1024
)

var (
	ZeroBools  []bool
	OneUint64s []uint64
)

type Container struct {
	n      int
	rows   int64
	diffs  []bool
	matchs []int64
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	vec    *vector.Vector
	groups map[uint64][]*hash.SetGroup // hash code -> group list
}

type Argument struct {
	Attrs []string
	Ctr   Container
}
