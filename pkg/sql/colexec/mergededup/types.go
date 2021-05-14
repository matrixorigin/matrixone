package mergededup

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/block"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
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
	state   int
	spilled bool
	diffs   []bool
	matchs  []int64
	hashs   []uint64
	sels    [][]int64    // sels
	slots   *fastmap.Map // hash code -> sels index
	bat     *batch.Batch
	bats    []*block.Block
	groups  map[uint64][]*hash.SetGroup // hash code -> group list
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
	Attrs []string
	Ctr   Container
	E     engine.SpillEngine
}
