package hashjoin

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/sql/join"
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
	sels   [][]int64                    // sels
	slots  *fastmap.Map                 // hash code -> sels index
	bats   []*batch.Batch               // s relation
	groups map[uint64][]*hash.JoinGroup // hash code -> join list
}

type Argument struct {
	Distinct bool
	Attrs    []string
	Rattrs   []string
	Sattrs   []string
	Ctr      Container
	JoinType join.JoinType
}
