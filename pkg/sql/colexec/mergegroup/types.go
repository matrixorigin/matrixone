package mergegroup

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/sql/colexec/aggregation"
)

const (
	Build = iota
	Eval
	End
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
	state  int
	rows   int64
	is     []int // index list
	diffs  []bool
	matchs []int64
	attrs  []string
	rattrs []string
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	refer  map[string]uint64
	groups map[uint64][]*hash.Group // hash code -> group list
}

type Argument struct {
	Flg   bool // is local merge
	Gs    []string
	Ctr   Container
	Refer map[string]uint64
	Es    []aggregation.Extend
}
