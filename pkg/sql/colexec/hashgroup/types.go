package hashgroup

import (
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/sql/colexec/aggregation"
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
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	vecs   []*vector.Vector
	groups map[uint64][]*hash.Group // hash code -> group list
}

type Argument struct {
	Gs     []string
	Attrs  []string
	Rattrs []string
	Ctr    Container
	Es     []aggregation.Extend
}
