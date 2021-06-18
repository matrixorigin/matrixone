package group

import (
	"matrixone/pkg/container/block"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/sql/colexec/aggregation"
)

const (
	Build = iota
	Eval
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
	is     []int // index list
	rows   int64
	diffs  []bool
	matchs []int64
	attrs  []string
	rattrs []string
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *block.Block
	bats   []*block.Block
	refer  map[string]uint64
	groups map[uint64][]*hash.Group // hash code -> group list
}

type Argument struct {
	Gs    []string
	Ctr   Container
	Refer map[string]uint64
	Es    []aggregation.Extend
}
