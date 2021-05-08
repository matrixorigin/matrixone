package inner

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
	Probe
)

const (
	UnitLimit = 1024
)

var (
	ZeroBools  []bool
	OneUint64s []uint64
)

type Container struct {
	state      int
	spilled    bool
	diffs      []bool
	matchs     []int64
	hashs      []uint64
	attrs      []string
	sels       [][]int64      // sels
	slots      *fastmap.Map   // hash code -> sels index
	bats       []*block.Block // s relation
	probeState struct {
		attrs []string
		bat   *batch.Batch // output relation
		md    []metadata.Attribute
	}
	groups map[uint64][]*hash.BagGroup // hash code -> group list
	spill  struct {
		id    string
		cs    []uint64
		attrs []string
		r     engine.Relation
		e     engine.SpillEngine
		md    []metadata.Attribute
	}
}

type Argument struct {
	R     string
	S     string
	Attrs []string
	Ctr   Container
	E     engine.SpillEngine
}
