package dedup

import (
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
	diffs      []bool
	matchs     []int64
	hashs      []uint64
	sels       [][]int64    // sels
	slots      *fastmap.Map // hash code -> sels index
	dedupState struct {
		data []byte
		sels []int64
	}
	groups map[uint64][]*hash.DedupGroup // hash code -> group list
}

type Argument struct {
	Attrs []string
	Ctr   Container
}
