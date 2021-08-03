package inner

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
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
	diffs  []bool
	matchs []int64
	hashs  []uint64
	attrs  []string
	rattrs []string
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	Probe  struct {
		attrs []string
		bat   *batch.Batch // output relation
	}
	groups map[uint64][]*hash.BagGroup // hash code -> group list
}

type Argument struct {
	R      string
	S      string
	Rattrs []string
	Sattrs []string
	Ctr    Container
}
