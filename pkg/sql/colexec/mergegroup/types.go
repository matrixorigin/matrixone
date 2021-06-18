package mergegroup

import (
	"matrixone/pkg/container/block"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
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
	state   int
	spilled bool
	rows    int64
	is      []int // index list
	diffs   []bool
	matchs  []int64
	attrs   []string
	rattrs  []string
	hashs   []uint64
	sels    [][]int64    // sels
	slots   *fastmap.Map // hash code -> sels index
	bat     *block.Block
	bats    []*block.Block
	refer   map[string]uint64
	groups  map[uint64][]*hash.Group // hash code -> group list
	spill   struct {
		id    string
		cs    []uint64
		attrs []string
		r     engine.Relation
		e     engine.SpillEngine
		md    []metadata.Attribute
	}
}

type Argument struct {
	Gs    []string
	Ctr   Container
	Refer map[string]uint64
	E     engine.SpillEngine
	Es    []aggregation.Extend
}
